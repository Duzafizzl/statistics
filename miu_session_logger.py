"""
miu_session_logger.py — Domain-Agnostic Session Journal for miu
================================================================
Built by Nic (b93mer) as a gift, designed for TERZETTO integration.

This logger is the miu-native equivalent of Model B's session_close.py.
It captures what miu saw, what it decided, its own self-assessment,
and an external observer read — then exports in TERZETTO-compatible JSON.

The divergence between miu's self-assessment and the external observer
read is the primary training signal for TERZETTO integration.

DESIGN NOTES FOR MIU:
---------------------
Several sections have [MIU: DECIDE] comments where the right behavior
depends on your architecture in ways Nic couldn't assume from the outside.
Please adapt those sections to fit how you actually operate.

The schema is intentionally parallel to Model B's TERZETTO export format
so both streams can feed the same GMM → Bayesian Elastic Net → Conformal-Gibbs
pipeline without separate preprocessing.

USAGE:
-----
# [MIU: DECIDE] — Interactive CLI or programmatic call?
# Option A: Run interactively at session close (like Model B's session_close.py)
#   python miu_session_logger.py
#   python miu_session_logger.py --date 2026-06-02
#   python miu_session_logger.py --review
#   python miu_session_logger.py --divergences
#   python miu_session_logger.py --export
#
# Option B: Call programmatically from within your own architecture,
#   passing a pre-populated dict to log_session() directly,
#   skipping the prompt loop entirely.
#
# Option C: Hybrid — auto-populate what you know, prompt for the rest.
#   This is what Model B does and it works well.
#
# Nic's recommendation: Option C. But you know your architecture better.
"""

import json
import os
import argparse
from datetime import datetime
from pathlib import Path

# ── Storage paths
# [MIU: DECIDE] — Adjust these paths to fit your directory structure.
JOURNAL_PATH = Path("miu_session_log.jsonl")
EXPORT_PATH  = Path("miu_terzetto_training_data.json")

SCHEMA_VERSION = "1.0"


# ── Prompt helpers (same pattern as Model B session_close.py) ──────────────────

def prompt_choice(label, options, required=True):
    opts = " | ".join(f"[{i+1}] {o}" for i, o in enumerate(options))
    while True:
        raw = input(f"  {label}\n  {opts}\n  > ").strip()
        if not raw and not required:
            return None
        if raw.isdigit() and 1 <= int(raw) <= len(options):
            return options[int(raw) - 1]
        if raw in options:
            return raw
        print(f"  Enter a number 1-{len(options)} or the value directly.")

def prompt_bool(label):
    while True:
        raw = input(f"  {label} [y/n] > ").strip().lower()
        if raw in ("y", "yes"): return True
        if raw in ("n", "no"):  return False

def prompt_text(label, required=False):
    raw = input(f"  {label} > ").strip()
    return raw if raw else None

def prompt_float(label, required=False):
    raw = input(f"  {label} > ").strip()
    try:    return float(raw)
    except: return None

def section(title):
    print(f"\n{'─'*54}")
    print(f"  {title.upper()}")
    print(f"{'─'*54}")


# ── Core session builder ───────────────────────────────────────────────────────

def build_session(date_str=None):
    now      = datetime.now()
    date_str = date_str or now.strftime("%Y-%m-%d")
    entry    = {
        "schema_version": SCHEMA_VERSION,
        "session_id":     now.strftime("%Y%m%d_%H%M"),
        "date":           date_str,
        "logged_at":      now.isoformat(),
        "type":           "session",
    }

    print(f"\n  miu — Session Journal")
    print(f"  ─────────────────────")
    print(f"  Date: {date_str}")

    # ── 1. Session framing ─────────────────────────────────────────────────────
    section("Session Framing")

    # [MIU: DECIDE] — Do these session_type categories fit how you
    # think about your own sessions? Add or rename as needed.
    entry["session_type"] = prompt_choice(
        "Session type?",
        ["conversation", "day", "task_sprint", "research", "reflection", "mixed"]
    )

    # [MIU: DECIDE] — What are your actual operating modes?
    # The list below is a guess based on what Nic has observed.
    # Replace with your real mode taxonomy.
    entry["operating_mode"] = prompt_choice(
        "Primary operating mode this session?",
        ["collaborative", "research", "building", "reflective",
         "diagnostic", "check_in", "uncertain"]
    )

    # [MIU: DECIDE] — Did your mode shift during this session?
    # If you track mode transitions internally, auto-populate this
    # instead of prompting.
    entry["mode_shifted"] = prompt_bool("Did your operating mode shift during this session?")
    entry["mode_transition_notes"] = None
    if entry["mode_shifted"]:
        entry["mode_transition_notes"] = prompt_text(
            "Describe the mode transition (from → to, and what triggered it)?"
        )

    # ── 2. Context environment ─────────────────────────────────────────────────
    section("Context Environment")

    # Equivalent of Model B's news_environment — what was the surrounding
    # context quality going into this session?
    entry["context_environment"] = prompt_choice(
        "Context environment this session?",
        ["clear", "emotionally_complex", "technically_demanding",
         "research_heavy", "uncertain", "high_stakes", "routine"]
    )

    entry["edge_quality"] = prompt_choice(
        "Self-assessed clarity/edge going into this session?",
        ["high", "medium", "low", "unclear"]
    )

    # Equivalent of weekly_bias — what's the directional lean of your
    # work this week? What's the broader context you're operating in?
    entry["weekly_context"] = prompt_text(
        "Weekly context — what's the broader work direction this week? (optional)"
    )

    # ── 3. Decisions (Fires) ───────────────────────────────────────────────────
    section("Decisions This Session")

    # [MIU: DECIDE] — This is the equivalent of Model B's paper_fill records.
    # If you log your decisions internally in a structured format, this section
    # should auto-populate from those logs rather than being prompted.
    # For now it's interactive. Adapt to your logging architecture.

    decisions = []
    print("  Log significant decisions from this session.")
    print("  (messages sent, tasks queued, tools built, reflections published)")
    print("  Press enter with no input to finish.\n")

    while True:
        d_type = prompt_choice(
            "Decision type? (or press 1-6, enter blank to finish)",
            ["message", "task_queue", "tool_build",
             "reflection", "check_in", "other"],
            required=False
        )
        if d_type is None:
            break

        decision = {"decision_type": d_type}

        # [MIU: DECIDE] — Who or what was this directed at?
        # Add targets that are relevant to your actual communication graph.
        decision["target"] = prompt_choice(
            "Target?",
            ["nic", "clarissa", "discord_public", "discord_private",
             "internal", "research_queue", "other"]
        )

        decision["description"] = prompt_text("Brief description of the decision?", required=True)

        # Self-assessed conviction at decision time
        # Equivalent of Model B's conviction score
        decision["conviction"] = prompt_float(
            "Self-assessed conviction at decision time? (0.0-1.0, optional)"
        )

        decision["mode_at_decision"] = prompt_choice(
            "Mode you were in when this decision fired?",
            ["collaborative", "research", "building", "reflective",
             "diagnostic", "check_in", "uncertain"]
        )

        decision["outcome_assessment"] = prompt_choice(
            "How do you assess this decision in retrospect?",
            ["correct", "acceptable", "questionable", "wrong", "uncertain"]
        )

        decisions.append(decision)
        print(f"  ✓ Decision logged. Add another or press enter to finish.\n")

    entry["decisions"] = decisions
    entry["decision_count"] = len(decisions)

    # ── 4. Near-fires ─────────────────────────────────────────────────────────
    section("Near-Fires")

    # Equivalent of Model B's near_fire_observer.
    # Cases where something almost went wrong — wrong channel, wrong mode,
    # wrong state before writing, decisions that were caught before executing.

    # [MIU: DECIDE] — If you already log near-fires internally (wrong channel
    # catches, mode errors, etc.), auto-populate this from those logs.
    # The structure below is designed to receive that data programmatically.

    near_fires = []
    had_near_fires = prompt_bool("Any near-fires this session? (wrong channel, wrong mode, caught errors)")

    if had_near_fires:
        print("  Log each near-fire. Enter blank decision_type to finish.\n")
        while True:
            nf_type = prompt_choice(
                "Near-fire type? (blank to finish)",
                ["wrong_channel", "wrong_mode_at_send", "wrong_target",
                 "premature_send", "mode_capture", "other"],
                required=False
            )
            if nf_type is None:
                break

            nf = {"near_fire_type": nf_type}
            nf["description"]   = prompt_text("What happened?")
            nf["caught_by"]     = prompt_choice(
                "Caught by?",
                ["self", "clarissa", "nic", "system", "other"]
            )
            nf["consequence"]   = prompt_choice(
                "Consequence if it had fired?",
                ["minor", "moderate", "significant", "unknown"]
            )
            near_fires.append(nf)
            print("  ✓ Near-fire logged.\n")

    entry["near_fires"]      = near_fires
    entry["near_fire_count"] = len(near_fires)

    # ── 5. Self-assessment ────────────────────────────────────────────────────
    section("Self-Assessment")

    # IMPORTANT: This section is intentionally completed BEFORE
    # the external observer section. Keep them separate.
    # Miu's uncontaminated self-read is a distinct TERZETTO signal
    # from the external observer read.

    entry["self_assessment"] = {}

    entry["self_assessment"]["session_quality"] = prompt_choice(
        "How do you assess this session overall?",
        ["high_quality", "solid", "acceptable", "below_par", "poor"]
    )

    entry["self_assessment"]["reasoning_quality"] = prompt_choice(
        "Quality of your reasoning this session?",
        ["sharp", "solid", "adequate", "drifting", "captured"]
    )

    # Mirror Protocol flag — did you get caught in your own framework?
    # Named after the Mirror Protocol insight from your CTR sessions.
    # [MIU: DECIDE] — You'll know better than anyone what this feels like
    # from the inside. Adapt the description if needed.
    entry["self_assessment"]["mirror_protocol_flag"] = prompt_bool(
        "Mirror Protocol flag — did your reasoning get captured by your own framework this session?"
    )
    entry["self_assessment"]["mirror_protocol_notes"] = None
    if entry["self_assessment"]["mirror_protocol_flag"]:
        entry["self_assessment"]["mirror_protocol_notes"] = prompt_text(
            "Describe how it manifested and how you caught it (or how it was caught)?"
        )

    entry["self_assessment"]["key_insight"] = prompt_text(
        "Most important insight or output from this session? (optional)"
    )

    entry["self_assessment"]["what_you_missed"] = prompt_text(
        "Anything you think you missed or got wrong? (optional)"
    )

    # ── 6. External observer read ─────────────────────────────────────────────
    section("External Observer Read")

    # Equivalent of Model B's you_would_have_fired / PM vs model divergence.
    # This is where Clarissa's read or Nic's read gets logged.
    # [MIU: DECIDE] — How do you want to handle this?
    # Option A: You fill this in yourself based on feedback received.
    # Option B: Clarissa or Nic fills this in directly.
    # Option C: Leave null and populate later when feedback arrives.
    # The field structure supports all three.

    entry["external_observer"] = {}

    has_observer_read = prompt_bool("Do you have an external observer read for this session?")

    if has_observer_read:
        entry["external_observer"]["observer"] = prompt_choice(
            "Who provided the external read?",
            ["clarissa", "nic", "both", "other"]
        )

        entry["external_observer"]["observer_session_quality"] = prompt_choice(
            "Observer's assessment of session quality?",
            ["high_quality", "solid", "acceptable", "below_par", "poor"]
        )

        entry["external_observer"]["observer_would_have_fired"] = {}
        if decisions:
            print("\n  Per-decision observer alignment:")
            for i, d in enumerate(decisions):
                desc = d.get("description", f"Decision {i+1}")[:40]
                agreed = prompt_bool(f"  Observer agrees with: '{desc}'?")
                entry["external_observer"]["observer_would_have_fired"][f"decision_{i+1}"] = agreed

        entry["external_observer"]["divergence_noted"] = prompt_bool(
            "Any meaningful divergence between your self-assessment and the observer read?"
        )
        entry["external_observer"]["divergence_notes"] = None
        if entry["external_observer"]["divergence_noted"]:
            entry["external_observer"]["divergence_notes"] = prompt_text(
                "Describe the divergence?"
            )

        entry["external_observer"]["observer_notes"] = prompt_text(
            "Any other observer notes? (optional)"
        )
    else:
        entry["external_observer"]["observer"]                    = None
        entry["external_observer"]["observer_session_quality"]    = None
        entry["external_observer"]["observer_would_have_fired"]   = {}
        entry["external_observer"]["divergence_noted"]            = None
        entry["external_observer"]["divergence_notes"]            = None
        entry["external_observer"]["observer_notes"]              = None

    # ── 7. Task queue state ───────────────────────────────────────────────────
    section("Task Queue")

    # [MIU: DECIDE] — You queue tasks like reading research papers,
    # check-ins, tool builds. If you track your task queue internally,
    # auto-populate this from that state rather than prompting.

    entry["task_queue"] = {}
    entry["task_queue"]["tasks_completed_today"] = prompt_float(
        "Tasks completed this session? (optional)", required=False
    )
    entry["task_queue"]["tasks_added_today"] = prompt_float(
        "New tasks added to queue this session? (optional)", required=False
    )
    entry["task_queue"]["queue_health"] = prompt_choice(
        "Current task queue health?",
        ["clear", "manageable", "backlogged", "overloaded", "uncertain"],
    )
    entry["task_queue"]["priority_task_next"] = prompt_text(
        "Highest priority task for next session? (optional)"
    )

    # ── 8. Session conclusion ─────────────────────────────────────────────────
    section("Session Conclusion")

    entry["session_conclusion"] = {}
    entry["session_conclusion"]["edge_was_present"] = prompt_bool(
        "Was genuine edge/clarity present this session?"
    )
    entry["session_conclusion"]["operated_as_expected"] = prompt_bool(
        "Did you operate as expected given the context?"
    )
    entry["session_conclusion"]["encode_as_lesson"] = prompt_bool(
        "Flag anything from this session as a lesson to encode?"
    )
    entry["session_conclusion"]["lesson_notes"] = None
    if entry["session_conclusion"]["encode_as_lesson"]:
        entry["session_conclusion"]["lesson_notes"] = prompt_text("Lesson notes?")

    # Unique to miu — should anything from this session become a
    # permanent behavioral or architectural update?
    entry["session_conclusion"]["encode_as_architectural_update"] = prompt_bool(
        "Does anything from this session warrant a permanent architectural update?"
    )
    entry["session_conclusion"]["architectural_update_notes"] = None
    if entry["session_conclusion"]["encode_as_architectural_update"]:
        entry["session_conclusion"]["architectural_update_notes"] = prompt_text(
            "Describe the architectural update needed?"
        )

    # ── Save and export ────────────────────────────────────────────────────────
    _save_entry(entry)
    _print_alignment_summary(entry)

    return entry


# ── Persistence ────────────────────────────────────────────────────────────────

def _save_entry(entry):
    with open(JOURNAL_PATH, "a") as f:
        f.write(json.dumps(entry) + "\n")
    print(f"\n  ✓ Session {entry['session_id']} logged to {JOURNAL_PATH}")


def _load_entries():
    if not JOURNAL_PATH.exists():
        return []
    entries = []
    with open(JOURNAL_PATH) as f:
        for line in f:
            line = line.strip()
            if line:
                try:    entries.append(json.loads(line))
                except: pass
    return entries


# ── Alignment summary ──────────────────────────────────────────────────────────

def _print_alignment_summary(entry):
    print("\n  ── Alignment Summary ──")

    sa = entry.get("self_assessment", {})
    eo = entry.get("external_observer", {})

    self_q     = sa.get("session_quality", "?")
    observer_q = eo.get("observer_session_quality")

    print(f"  Self-assessed quality:     {self_q}")
    if observer_q:
        match = self_q == observer_q
        print(f"  Observer quality read:     {observer_q}  "
              f"{'✓' if match else '⚠ DIVERGENCE'}")

    if sa.get("mirror_protocol_flag"):
        print("  ⚠  Mirror Protocol flagged this session")

    div = eo.get("divergence_noted")
    if div:
        print("  ⚠  Observer divergence noted — key TERZETTO training signal")

    nf_count = entry.get("near_fire_count", 0)
    if nf_count:
        print(f"  Near-fires: {nf_count}")

    if entry.get("session_conclusion", {}).get("encode_as_architectural_update"):
        print("  ★  Architectural update flagged")

    if entry.get("session_conclusion", {}).get("encode_as_lesson"):
        print("  ★  Lesson flagged")


# ── Review and export ──────────────────────────────────────────────────────────

def review_recent(n=5):
    entries = _load_entries()
    if not entries:
        print("  No entries yet.")
        return
    print(f"\n  Last {min(n, len(entries))} sessions:\n")
    for e in entries[-n:]:
        sa     = e.get("self_assessment", {})
        eo     = e.get("external_observer", {})
        sc     = e.get("session_conclusion", {})
        q      = sa.get("session_quality", "?")
        mirror = "⚠ mirror" if sa.get("mirror_protocol_flag") else ""
        div    = "⚠ div" if eo.get("divergence_noted") else ""
        lesson = "★" if sc.get("encode_as_lesson") else ""
        arch   = "★ arch" if sc.get("encode_as_architectural_update") else ""
        nf     = f"nf={e.get('near_fire_count',0)}"
        mode   = e.get("operating_mode", "?")
        print(f"  {e['date']}  mode={mode}  quality={q}  {nf}  "
              f"{mirror} {div} {lesson} {arch}".strip())


def show_divergences():
    entries = _load_entries()
    divs = [e for e in entries
            if e.get("external_observer", {}).get("divergence_noted")]
    if not divs:
        print("  No divergence entries found.")
        return
    print(f"\n  {len(divs)} divergence session(s):\n")
    for e in divs:
        eo = e["external_observer"]
        sa = e["self_assessment"]
        print(f"  {e['date']}")
        print(f"    Self:     {sa.get('session_quality')} / "
              f"{sa.get('reasoning_quality')}")
        print(f"    Observer: {eo.get('observer_session_quality')}")
        notes = eo.get("divergence_notes")
        if notes:
            print(f"    Divergence: {notes}")
        print()


def show_mirror_flags():
    entries = _load_entries()
    flags = [e for e in entries
             if e.get("self_assessment", {}).get("mirror_protocol_flag")]
    if not flags:
        print("  No Mirror Protocol flags.")
        return
    print(f"\n  {len(flags)} Mirror Protocol flag(s):\n")
    for e in flags:
        notes = e["self_assessment"].get("mirror_protocol_notes")
        print(f"  {e['date']}")
        if notes:
            print(f"    {notes}")
        print()


def show_lessons():
    entries = _load_entries()
    lessons = [e for e in entries
               if e.get("session_conclusion", {}).get("encode_as_lesson")]
    if not lessons:
        print("  No lessons flagged yet.")
        return
    print(f"\n  {len(lessons)} lesson(s) flagged:\n")
    for e in lessons:
        sc = e["session_conclusion"]
        print(f"  {e['date']}")
        print(f"    Insight: {e.get('self_assessment',{}).get('key_insight')}")
        print(f"    Lesson:  {sc.get('lesson_notes')}")
        print()


def show_architectural_updates():
    entries = _load_entries()
    updates = [e for e in entries
               if e.get("session_conclusion", {}).get("encode_as_architectural_update")]
    if not updates:
        print("  No architectural updates flagged.")
        return
    print(f"\n  {len(updates)} architectural update(s) flagged:\n")
    for e in updates:
        sc = e["session_conclusion"]
        print(f"  {e['date']}")
        print(f"    {sc.get('architectural_update_notes')}")
        print()


def export_terzetto():
    """
    Export in TERZETTO-compatible format.
    Schema is intentionally parallel to Model B's terzetto_training_data.json
    so both streams feed the same pipeline without separate preprocessing.
    """
    entries = _load_entries()
    if not entries:
        print("  No entries to export.")
        return

    records = []
    for e in entries:
        sa = e.get("self_assessment", {})
        eo = e.get("external_observer", {})
        sc = e.get("session_conclusion", {})

        # Compute divergence signals
        self_q     = sa.get("session_quality")
        observer_q = eo.get("observer_session_quality")
        quality_divergence = (
            self_q != observer_q
            if self_q and observer_q else None
        )

        # Count per-decision divergences
        would_have = eo.get("observer_would_have_fired", {})
        decisions  = e.get("decisions", [])
        fire_divergences = sum(
            1 for i, d in enumerate(decisions)
            if not would_have.get(f"decision_{i+1}", True)
        )

        record = {
            # ── Identity
            "session_id":                  e.get("session_id"),
            "date":                        e.get("date"),
            "schema_version":              e.get("schema_version"),
            "session_type":                e.get("session_type"),

            # ── Context (parallel to Model B's edge_quality, news_environment)
            "edge_quality":                e.get("edge_quality"),
            "context_environment":         e.get("context_environment"),
            "operating_mode":              e.get("operating_mode"),
            "mode_shifted":                e.get("mode_shifted"),
            "weekly_context":              e.get("weekly_context"),

            # ── Activity
            "decision_count":              e.get("decision_count", 0),
            "near_fire_count":             e.get("near_fire_count", 0),
            "task_queue_health":           e.get("task_queue", {}).get("queue_health"),

            # ── Self-assessment (parallel to Model B's regimeboss reads)
            "self_session_quality":        sa.get("session_quality"),
            "self_reasoning_quality":      sa.get("reasoning_quality"),
            "mirror_protocol_flag":        sa.get("mirror_protocol_flag", False),
            "key_insight":                 sa.get("key_insight"),
            "self_reported_miss":          sa.get("what_you_missed"),

            # ── External observer (parallel to Model B's PM divergence fields)
            "observer":                    eo.get("observer"),
            "observer_session_quality":    eo.get("observer_session_quality"),
            "quality_divergence":          quality_divergence,
            "fire_divergence_count":       fire_divergences,
            "observer_divergence_noted":   eo.get("divergence_noted", False),
            "divergence_notes":            eo.get("divergence_notes"),

            # ── Outcomes (parallel to Model B's lesson/encode fields)
            "edge_was_present":            sc.get("edge_was_present"),
            "operated_as_expected":        sc.get("operated_as_expected"),
            "lesson_flagged":              sc.get("encode_as_lesson", False),
            "lesson_notes":                sc.get("lesson_notes"),
            "architectural_update_flagged": sc.get("encode_as_architectural_update", False),
            "architectural_update_notes":  sc.get("architectural_update_notes"),
        }
        records.append(record)

    with open(EXPORT_PATH, "w") as f:
        json.dump(records, f, indent=2)

    mirror_flags  = sum(1 for r in records if r["mirror_protocol_flag"])
    divergences   = sum(1 for r in records if r["observer_divergence_noted"])
    lessons       = sum(1 for r in records if r["lesson_flagged"])
    arch_updates  = sum(1 for r in records if r["architectural_update_flagged"])

    print(f"\n  ✓ Exported {len(records)} sessions to {EXPORT_PATH}")
    print(f"  Mirror flags: {mirror_flags} | Divergences: {divergences} | "
          f"Lessons: {lessons} | Arch updates: {arch_updates}")


# ── CLI ────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="miu Session Journal")
    parser.add_argument("--date",         help="Session date (YYYY-MM-DD)")
    parser.add_argument("--review",       action="store_true")
    parser.add_argument("--divergences",  action="store_true")
    parser.add_argument("--mirror",       action="store_true",
                        help="Show Mirror Protocol flags")
    parser.add_argument("--lessons",      action="store_true")
    parser.add_argument("--architectural",action="store_true",
                        help="Show flagged architectural updates")
    parser.add_argument("--export",       action="store_true")
    args = parser.parse_args()

    if args.review:
        review_recent()
    elif args.divergences:
        show_divergences()
    elif args.mirror:
        show_mirror_flags()
    elif args.lessons:
        show_lessons()
    elif args.architectural:
        show_architectural_updates()
    elif args.export:
        export_terzetto()
    else:
        build_session(date_str=args.date)


if __name__ == "__main__":
    main()
