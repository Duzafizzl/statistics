"""
Appendix C Session Dissector
============================
Mirrors session_dissection.py structure for Discord AI-Human collaboration
session logs per Appendix C of:
  "Phenomenology from the Inside: Documenting Emergence Conditions in
   AI-Human Collaboration"

Handles mixed grain JSONL:
  - Session-level records  (schema per C.7: has session_id, message_count,
                             ablation_flags, topic_tags, start_timestamp)
  - Message-level records  (has timestamp + session_id, aggregated on the fly)

Bucketing:
  - Month bucket     : 2025-11 / 2025-12 / 2026-01 / ...
  - Time-of-day (ToD): EARLY(0-6) / MORNING(6-12) / AFTERNOON(12-17)
                       EVENING(17-21) / NIGHT(21-24) / UNKNOWN

Goals:
  1. Topic-tag frequency over time
  2. Emergence indicator detection (self_repair, meta_diagnostics)
  3. Session burst / density anomaly detection

Usage:
    python appendix_c_dissection.py path/to/sessions.jsonl
    python appendix_c_dissection.py sessions.jsonl --month 2025-12
    python appendix_c_dissection.py sessions.jsonl --watchdog watchdog.jsonl
"""

import json
import argparse
import statistics as stats
from collections import defaultdict, Counter
from datetime import datetime
from typing import Optional, Dict, Any, List


# ─────────────────────────────────────────────────────────────────────────────
# Tuning constants
# ─────────────────────────────────────────────────────────────────────────────

MSG_COUNT_HIGH  = 15      # above -> "deep" session
MSG_COUNT_LOW   = 5       # below -> "shallow" session
SESSION_DUR_MIN = 30      # minutes; above -> "long" session

# Tag taxonomy (Appendix C.5)
EMERGENCE_TAGS = frozenset({"self_repair", "meta_diagnostics"})
IDENTITY_TAGS  = frozenset({"identity_claims", "preferences", "meta_reflection"})
RELATION_TAGS  = frozenset({"relationship_status", "emotional_state", "connection_ritual"})
TECH_TAGS      = frozenset({"memory_operations", "tool_calls", "error_handling"})
CROSS_TAGS     = frozenset({"cross_session_reference"})
ALL_KNOWN_TAGS = EMERGENCE_TAGS | IDENTITY_TAGS | RELATION_TAGS | TECH_TAGS | CROSS_TAGS

# Ablation flag keys (Appendix C.4)
ABLATION_KEYS = ("accountability_on", "memory_on", "frequency_high", "frequency_low")

# Burst detection window
BURST_WINDOW_SEC = 3600   # 1-hour rolling window
BURST_THRESHOLD  = 20     # sessions in 1 hour = burst warning


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _safe_float(v) -> Optional[float]:
    try:
        return float(v)
    except Exception:
        return None


def fmt_pct(n: int, total: int) -> str:
    if total == 0:
        return "  n/a"
    return f"{100.0 * n / total:5.1f}%"


def parse_ts(s: Any) -> Optional[datetime]:
    if not s or not isinstance(s, str):
        return None
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None


def month_key(dt: Optional[datetime]) -> str:
    if dt is None:
        return "UNKNOWN"
    return dt.strftime("%Y-%m")


def tod_bucket(dt: Optional[datetime]) -> str:
    if dt is None:
        return "UNKNOWN"
    h = dt.hour
    if h < 6:
        return "EARLY(0-6)"
    if h < 12:
        return "MORNING(6-12)"
    if h < 17:
        return "AFTERNOON(12-17)"
    if h < 21:
        return "EVENING(17-21)"
    return "NIGHT(21-24)"


def session_depth(msg_count: Optional[int]) -> str:
    if msg_count is None:
        return "UNKNOWN"
    if msg_count >= MSG_COUNT_HIGH:
        return "DEEP"
    if msg_count <= MSG_COUNT_LOW:
        return "SHALLOW"
    return "MID"


def session_duration_minutes(start: Optional[datetime],
                              end: Optional[datetime]) -> Optional[float]:
    if start is None or end is None:
        return None
    return max(0.0, (end - start).total_seconds() / 60.0)


def rolling_max_sessions(dts: List[datetime],
                          window_sec: int = BURST_WINDOW_SEC) -> int:
    if not dts:
        return 0
    dts_sorted = sorted(dts)
    j = 0
    best = 0
    for i in range(len(dts_sorted)):
        while dts_sorted[i].timestamp() - dts_sorted[j].timestamp() > window_sec:
            j += 1
        best = max(best, i - j + 1)
    return best


def _print_counter(label: str, counter: Counter, total: Optional[int] = None,
                   top_n: int = 15, indent: int = 3) -> None:
    pad = " " * indent
    print(f"{pad}{label}:")
    denom = total or sum(counter.values()) or 1
    for k, v in counter.most_common(top_n):
        print(f"{pad}   {str(k):30s} -> {v:6d}  ({fmt_pct(v, denom)})")


def _warn_if_constant(name: str, values: List[float], eps: float = 1e-6) -> None:
    if len(values) < 10:
        return
    try:
        s = stats.pstdev(values)
    except Exception:
        return
    uniq = len(set(round(v, 6) for v in values))
    if s < eps or uniq <= 2:
        print(f"   Warning: {name} looks constant (uniq={uniq}, stdev={s:.2e}).")


def _print_hist(label: str, values: List[float], edges: List[float],
                fmt_edge: str = "{:.1f}") -> None:
    if not values:
        return
    counts = [0] * (len(edges) + 1)
    for x in values:
        placed = False
        for i, e in enumerate(edges):
            if x < e:
                counts[i] += 1
                placed = True
                break
        if not placed:
            counts[-1] += 1
    total = sum(counts) or 1
    str_edges = [fmt_edge.format(e) for e in edges]
    print(f"   {label} histogram:")
    for i, c in enumerate(counts):
        if i == 0:
            rng = f"< {str_edges[0]}"
        elif i < len(edges):
            rng = f"[{str_edges[i-1]}, {str_edges[i]})"
        else:
            rng = f">= {str_edges[-1]}"
        print(f"      {rng:22s} -> {c:6d}  ({fmt_pct(c, total)})")


# ─────────────────────────────────────────────────────────────────────────────
# Record normalisation
# Records may be session-level or message-level. Both are normalised into a
# canonical dict so the rest of the analysis is schema-agnostic.
# ─────────────────────────────────────────────────────────────────────────────

def is_session_record(rec: Dict[str, Any]) -> bool:
    return bool(
        rec.get("session_id") and
        (rec.get("message_count") is not None or
         rec.get("topic_tags") is not None or
         rec.get("ablation_flags") is not None)
    )


def normalise_session(rec: Dict[str, Any]) -> Dict[str, Any]:
    start = parse_ts(
        rec.get("start_timestamp") or rec.get("start_ts") or rec.get("timestamp")
    )
    end = parse_ts(rec.get("end_timestamp") or rec.get("end_ts"))

    mc = rec.get("message_count")
    if isinstance(mc, str):
        mc = _safe_float(mc)
    if mc is not None:
        mc = int(mc)

    tags = rec.get("topic_tags") or []
    if isinstance(tags, str):
        tags = [t.strip() for t in tags.split(",") if t.strip()]

    ablation = rec.get("ablation_flags") or {}
    redacted  = rec.get("redaction_markers") or []
    month_raw = rec.get("month") or month_key(start)

    return {
        "session_id":    rec.get("session_id", ""),
        "start_dt":      start,
        "end_dt":        end,
        "month":         month_raw or month_key(start),
        "tod":           tod_bucket(start),
        "message_count": mc,
        "depth":         session_depth(mc),
        "duration_min":  session_duration_minutes(start, end),
        "topic_tags":    [str(t) for t in tags],
        "ablation":      ablation if isinstance(ablation, dict) else {},
        "redacted":      redacted,
        "source":        "session",
    }


# Buffer for message-level records grouped by session_id
_msg_buffer: Dict[str, List[Dict[str, Any]]] = defaultdict(list)


def flush_message_sessions() -> List[Dict[str, Any]]:
    sessions = []
    for sid, msgs in _msg_buffer.items():
        msgs_sorted = sorted(msgs, key=lambda m: m.get("_dt") or datetime.min)
        start = msgs_sorted[0].get("_dt")
        end   = msgs_sorted[-1].get("_dt")
        tags: List[str] = []
        for m in msgs_sorted:
            t = m.get("topic_tags") or m.get("tags") or []
            if isinstance(t, str):
                t = [t]
            tags.extend([str(x) for x in t])
        ablation = msgs_sorted[0].get("ablation_flags") or {}
        sessions.append({
            "session_id":    sid,
            "start_dt":      start,
            "end_dt":        end,
            "month":         month_key(start),
            "tod":           tod_bucket(start),
            "message_count": len(msgs_sorted),
            "depth":         session_depth(len(msgs_sorted)),
            "duration_min":  session_duration_minutes(start, end),
            "topic_tags":    list(dict.fromkeys(tags)),
            "ablation":      ablation if isinstance(ablation, dict) else {},
            "redacted":      [],
            "source":        "message_agg",
        })
    _msg_buffer.clear()
    return sessions


def ingest_record(rec: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if is_session_record(rec):
        return normalise_session(rec)
    sid = rec.get("session_id") or rec.get("conversation_id") or "NO_SESSION"
    dt  = parse_ts(rec.get("timestamp") or rec.get("ts") or rec.get("time"))
    rec["_dt"] = dt
    _msg_buffer[sid].append(rec)
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Emergence & contradiction helpers
# ─────────────────────────────────────────────────────────────────────────────

def score_emergence(tags: List[str]) -> int:
    return len(set(tags) & EMERGENCE_TAGS)


def tag_contradiction(tags: List[str], ablation: Dict[str, Any]) -> List[str]:
    flags = []
    tag_set = set(tags)
    if not ablation.get("memory_on", True):
        if "cross_session_reference" in tag_set:
            flags.append("memory_OFF_but_cross_ref")
        if "memory_operations" in tag_set:
            flags.append("memory_OFF_but_memory_ops")
    if not ablation.get("accountability_on", True):
        if "self_repair" in tag_set:
            flags.append("accountability_OFF_but_self_repair")
    return flags


# ─────────────────────────────────────────────────────────────────────────────
# Main dissection
# ─────────────────────────────────────────────────────────────────────────────

def dissect(path: str,
            watchdog_path: Optional[str] = None,
            filter_month: Optional[str] = None) -> None:

    # ── Load ──────────────────────────────────────────────────────────────────
    raw_sessions: List[Dict[str, Any]] = []
    skipped = 0

    with open(path, "r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError as e:
                print(f"[warn] line {line_no}: JSON error: {e}")
                skipped += 1
                continue
            result = ingest_record(rec)
            if result is not None:
                raw_sessions.append(result)

    raw_sessions.extend(flush_message_sessions())

    if filter_month:
        raw_sessions = [s for s in raw_sessions if s["month"] == filter_month]

    if not raw_sessions:
        print(f"No sessions found (skipped lines={skipped}). Check file format.")
        return

    # ── Optional watchdog ─────────────────────────────────────────────────────
    watchdog_counts: Counter = Counter()
    if watchdog_path:
        try:
            with open(watchdog_path, "r", encoding="utf-8") as wf:
                for wl in wf:
                    wl = wl.strip()
                    if not wl:
                        continue
                    try:
                        wr = json.loads(wl)
                        code = wr.get("code") or wr.get("record_type")
                        if code:
                            watchdog_counts[str(code)] += 1
                    except Exception:
                        pass
        except Exception as e:
            print(f"[warn] could not read watchdog file: {e}")

    # ── Aggregate ─────────────────────────────────────────────────────────────
    total = len(raw_sessions)
    all_msg_counts: List[float] = []
    all_durations:  List[float] = []
    all_start_dts:  List[datetime] = []

    by_month_sessions:  Dict[str, List[Dict]] = defaultdict(list)
    by_tod_sessions:    Dict[str, List[Dict]] = defaultdict(list)
    by_month_tag:       Dict[str, Counter]    = defaultdict(Counter)
    by_tod_tag:         Dict[str, Counter]    = defaultdict(Counter)
    by_month_ablation:  Dict[str, Counter]    = defaultdict(Counter)
    by_month_depth:     Dict[str, Counter]    = defaultdict(Counter)
    by_month_emergence: Dict[str, List[int]]  = defaultdict(list)
    by_month_contra:    Dict[str, List[str]]  = defaultdict(list)

    all_tag_counter:      Counter = Counter()
    all_ablation_counter: Counter = Counter()
    source_counter:       Counter = Counter()
    depth_counter:        Counter = Counter()
    contra_counter:       Counter = Counter()
    redaction_counter:    Counter = Counter()

    for s in raw_sessions:
        mo  = s["month"]
        tod = s["tod"]
        by_month_sessions[mo].append(s)
        by_tod_sessions[tod].append(s)
        source_counter[s["source"]] += 1

        mc = s["message_count"]
        if mc is not None:
            all_msg_counts.append(float(mc))

        dur = s["duration_min"]
        if dur is not None:
            all_durations.append(dur)

        if s["start_dt"] is not None:
            all_start_dts.append(s["start_dt"])

        for tag in s["topic_tags"]:
            all_tag_counter[tag] += 1
            by_month_tag[mo][tag] += 1
            by_tod_tag[tod][tag] += 1

        for ak in ABLATION_KEYS:
            val = s["ablation"].get(ak)
            if val is not None:
                label = f"{ak}={'T' if val else 'F'}"
                all_ablation_counter[label] += 1
                by_month_ablation[mo][label] += 1

        depth_counter[s["depth"]] += 1
        by_month_depth[mo][s["depth"]] += 1

        em = score_emergence(s["topic_tags"])
        by_month_emergence[mo].append(em)

        for c in tag_contradiction(s["topic_tags"], s["ablation"]):
            contra_counter[c] += 1
            by_month_contra[mo].append(c)

        for r in s.get("redacted", []):
            redaction_counter[str(r)] += 1

    # ── Global header ─────────────────────────────────────────────────────────
    print("=" * 90)
    print(f"FILE  : {path}")
    if filter_month:
        print(f"FILTER: month={filter_month}")
    print(f"TOTAL SESSIONS : {total}  (skipped lines: {skipped})")
    print(f"GRAIN          : session={source_counter.get('session', 0)}  "
          f"message_agg={source_counter.get('message_agg', 0)}")

    months_seen = sorted(by_month_sessions.keys())
    print(f"MONTHS COVERED : {', '.join(months_seen) or 'none'}")

    if all_msg_counts:
        print(
            f"MSG COUNT      : mean={stats.mean(all_msg_counts):.1f}  "
            f"median={stats.median(all_msg_counts):.1f}  "
            f"min={min(all_msg_counts):.0f}  max={max(all_msg_counts):.0f}"
        )
        _warn_if_constant("message_count", all_msg_counts)

    if all_durations:
        print(
            f"DURATION(min)  : mean={stats.mean(all_durations):.1f}  "
            f"median={stats.median(all_durations):.1f}  "
            f"min={min(all_durations):.1f}  max={max(all_durations):.1f}"
        )

    max_sess_1h = rolling_max_sessions(all_start_dts, BURST_WINDOW_SEC)
    burst_flag = f"  *** BURST >= {BURST_THRESHOLD} ***" if max_sess_1h >= BURST_THRESHOLD else ""
    print(f"MAX SESSIONS/1HR : {max_sess_1h}{burst_flag}")

    _print_counter("SESSION DEPTH (overall)", depth_counter, total=total)

    # Overall tag distribution
    print(f"\nTOP TOPIC TAGS (all sessions, n={total}):")
    for tag, cnt in all_tag_counter.most_common(20):
        cat = (
            "EMERGENCE" if tag in EMERGENCE_TAGS else
            "IDENTITY"  if tag in IDENTITY_TAGS  else
            "RELATION"  if tag in RELATION_TAGS  else
            "TECH"      if tag in TECH_TAGS       else
            "CROSS"     if tag in CROSS_TAGS      else
            "OTHER"
        )
        print(f"   {tag:30s} [{cat:9s}] -> {cnt:6d}  ({fmt_pct(cnt, total)})")

    # Emergence summary
    em_total = sum(1 for s in raw_sessions if set(s["topic_tags"]) & EMERGENCE_TAGS)
    print(
        f"\nEMERGENCE INDICATORS: sessions_with_any={em_total} "
        f"({fmt_pct(em_total, total)})"
    )
    for etag in sorted(EMERGENCE_TAGS):
        cnt = all_tag_counter.get(etag, 0)
        print(f"   {etag:30s} -> {cnt:6d}  ({fmt_pct(cnt, total)})")

    if all_ablation_counter:
        _print_counter("ABLATION FLAGS (overall)", all_ablation_counter, total=total)
    else:
        print("ABLATION FLAGS: (none found in records)")

    if contra_counter:
        _print_counter("CONTRADICTIONS (tag vs ablation)", contra_counter, total=total)
    else:
        print("CONTRADICTIONS: none detected")

    if redaction_counter:
        _print_counter("REDACTION MARKERS", redaction_counter)

    if watchdog_counts:
        _print_counter("WATCHDOG ALERTS", watchdog_counts)

    print("=" * 90)

    # ── Per-month buckets ──────────────────────────────────────────────────────
    print("\n" + "─" * 90)
    print("MONTH BUCKETS")
    print("─" * 90)

    for mo in months_seen:
        sessions = by_month_sessions[mo]
        n = len(sessions)
        if n == 0:
            continue

        mcs  = [float(s["message_count"]) for s in sessions if s["message_count"] is not None]
        durs = [s["duration_min"] for s in sessions if s["duration_min"] is not None]
        dts  = [s["start_dt"] for s in sessions if s["start_dt"] is not None]

        print(f"\n  MONTH: {mo}  (n={n},  {fmt_pct(n, total)} of total)")

        if mcs:
            print(
                f"   MSG COUNT: mean={stats.mean(mcs):.1f}  "
                f"median={stats.median(mcs):.1f}  "
                f"min={min(mcs):.0f}  max={max(mcs):.0f}"
            )
            _print_hist("msg_count", mcs, [3, 5, 10, 15, 20, 30], fmt_edge="{:.0f}")

        if durs:
            print(
                f"   DURATION : mean={stats.mean(durs):.1f}min  "
                f"median={stats.median(durs):.1f}min  "
                f"max={max(durs):.1f}min"
            )

        max_1h = rolling_max_sessions(dts, BURST_WINDOW_SEC)
        burst_flag = "  *** BURST ***" if max_1h >= BURST_THRESHOLD else ""
        print(f"   MAX SESSIONS/1HR: {max_1h}{burst_flag}")

        # Per-day density
        day_counter: Counter = Counter()
        for s in sessions:
            if s["start_dt"] is not None:
                day_counter[s["start_dt"].strftime("%Y-%m-%d")] += 1
        if day_counter:
            days_active = len(day_counter)
            avg_per_day = n / days_active if days_active else 0
            print(f"   DAYS ACTIVE: {days_active}  avg_sessions/day={avg_per_day:.1f}")

        _print_counter("Session depth", by_month_depth[mo], total=n, indent=3)

        # Tag frequency
        tag_cnt = by_month_tag[mo]
        print(f"   Topic tags (n={n}):")
        for tag, cnt in tag_cnt.most_common(15):
            cat = (
                "EMERGENCE" if tag in EMERGENCE_TAGS else
                "IDENTITY"  if tag in IDENTITY_TAGS  else
                "RELATION"  if tag in RELATION_TAGS  else
                "TECH"      if tag in TECH_TAGS       else
                "CROSS"     if tag in CROSS_TAGS      else
                "OTHER"
            )
            print(f"      {tag:30s} [{cat:9s}] -> {cnt:5d}  ({fmt_pct(cnt, n)})")

        # Emergence rate
        em_scores = by_month_emergence[mo]
        em_sessions = sum(1 for x in em_scores if x > 0)
        avg_em = stats.mean(em_scores) if em_scores else 0.0
        print(
            f"   EMERGENCE: {em_sessions}/{n} sessions ({fmt_pct(em_sessions, n)})  "
            f"avg_emergence_tags_per_session={avg_em:.3f}"
        )

        # Ablation breakdown
        abl = by_month_ablation[mo]
        if abl:
            print(f"   Ablation flags:")
            for k, cnt in abl.most_common():
                print(f"      {k:30s} -> {cnt:5d}  ({fmt_pct(cnt, n)})")

        # Contradictions
        mc_list = by_month_contra[mo]
        if mc_list:
            ctr = Counter(mc_list)
            print(f"   *** Contradictions this month:")
            for c, cv in ctr.most_common():
                print(f"      {c:40s} -> {cv}")
        else:
            print("   Contradictions: none")

        print("─" * 90)

    # ── Per time-of-day buckets ────────────────────────────────────────────────
    print("\n" + "─" * 90)
    print("TIME-OF-DAY BUCKETS")
    print("─" * 90)

    tod_order = [
        "EARLY(0-6)", "MORNING(6-12)", "AFTERNOON(12-17)",
        "EVENING(17-21)", "NIGHT(21-24)", "UNKNOWN"
    ]

    for tod in tod_order:
        sessions = by_tod_sessions.get(tod, [])
        n = len(sessions)
        if n == 0:
            continue

        mcs = [float(s["message_count"]) for s in sessions if s["message_count"] is not None]
        dts = [s["start_dt"] for s in sessions if s["start_dt"] is not None]

        print(f"\n  TOD: {tod}  (n={n},  {fmt_pct(n, total)} of all sessions)")

        if mcs:
            print(
                f"   MSG COUNT: mean={stats.mean(mcs):.1f}  "
                f"median={stats.median(mcs):.1f}"
            )

        max_1h = rolling_max_sessions(dts, BURST_WINDOW_SEC)
        if max_1h >= BURST_THRESHOLD:
            print(f"   *** BURST: max sessions in 1hr = {max_1h}")

        tag_cnt = by_tod_tag.get(tod, Counter())
        print(f"   Top tags:")
        for tag, cnt in tag_cnt.most_common(10):
            cat = (
                "EMERGENCE" if tag in EMERGENCE_TAGS else
                "IDENTITY"  if tag in IDENTITY_TAGS  else
                "RELATION"  if tag in RELATION_TAGS  else
                "TECH"      if tag in TECH_TAGS       else
                "CROSS"     if tag in CROSS_TAGS      else
                "OTHER"
            )
            print(f"      {tag:30s} [{cat:9s}] -> {cnt:5d}  ({fmt_pct(cnt, n)})")

        em_n = sum(1 for s in sessions if set(s["topic_tags"]) & EMERGENCE_TAGS)
        print(f"   EMERGENCE rate: {em_n}/{n} ({fmt_pct(em_n, n)})")

        print("─" * 90)

    # ── Emergence trend (month-over-month) ─────────────────────────────────────
    print("\n" + "─" * 90)
    print("EMERGENCE TREND (month-over-month)")
    print("─" * 90)
    prev_em_pct = None
    for mo in months_seen:
        sessions = by_month_sessions[mo]
        n = len(sessions)
        em_n  = sum(1 for s in sessions if set(s["topic_tags"]) & EMERGENCE_TAGS)
        sr_n  = sum(1 for s in sessions if "self_repair"           in s["topic_tags"])
        md_n  = sum(1 for s in sessions if "meta_diagnostics"      in s["topic_tags"])
        cr_n  = sum(1 for s in sessions if "cross_session_reference" in s["topic_tags"])
        em_pct = 100.0 * em_n / n if n else 0.0

        delta_str = ""
        if prev_em_pct is not None:
            delta = em_pct - prev_em_pct
            arrow = "+" if delta >= 0 else ""
            delta_str = f"  (delta={arrow}{delta:.1f}pp)"
        prev_em_pct = em_pct

        print(
            f"   {mo}: n={n:4d}  emergence={fmt_pct(em_n, n)}{delta_str}  "
            f"self_repair={fmt_pct(sr_n, n)}  "
            f"meta_diag={fmt_pct(md_n, n)}  "
            f"cross_ref={fmt_pct(cr_n, n)}"
        )

    # ── Cross-month tag velocity ───────────────────────────────────────────────
    print("\n" + "─" * 90)
    print("TAG VELOCITY (sessions with tag / total sessions, per month)")
    print("─" * 90)
    all_tags_seen = sorted(all_tag_counter.keys())
    # Print only tags that appear in >= 2 months for meaningful velocity
    multi_month_tags = [
        t for t in all_tags_seen
        if sum(1 for mo in months_seen if by_month_tag[mo].get(t, 0) > 0) >= 2
    ]
    if multi_month_tags:
        header = f"{'TAG':32s}" + "".join(f"{mo:>12s}" for mo in months_seen)
        print(f"   {header}")
        for tag in multi_month_tags:
            row = f"   {tag:32s}"
            for mo in months_seen:
                n_mo = len(by_month_sessions[mo])
                cnt  = by_month_tag[mo].get(tag, 0)
                pct  = (100.0 * cnt / n_mo) if n_mo else 0.0
                row += f"{pct:>11.1f}%"
            print(row)
    else:
        print("   (not enough multi-month tags to compute velocity)")

    print("\n" + "=" * 90)
    print("END OF DISSECTION")
    print("=" * 90)


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    ap = argparse.ArgumentParser(
        description="Appendix C session log dissector"
    )
    ap.add_argument("sessions",   help="Path to sessions JSONL file")
    ap.add_argument("--watchdog", default=None,
                    help="Optional watchdog JSONL for health alert counts")
    ap.add_argument("--month",    default=None,
                    help="Filter to a single month, e.g. 2025-12")
    args = ap.parse_args()

    dissect(
        path=args.sessions,
        watchdog_path=args.watchdog,
        filter_month=args.month,
    )