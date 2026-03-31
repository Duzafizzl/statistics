#!/usr/bin/env python3
"""
build_surface_dataset.py - Builds surface data from taxonomy YAML and optional session JSONL files.

Created: 2026-03-31
Last updated: 2026-03-31
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import traceback
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def slog(level: str, module: str, message: str, **context: Any) -> None:
    payload = {
        "timestamp": datetime.now(UTC).isoformat(),
        "level": level,
        "module": module,
        "message": message,
        **context,
    }
    print(json.dumps(payload), flush=True)


def progress(step: str, idx: int, total: int, status: str = "✅") -> None:
    pct = ((idx / total) * 100.0) if total > 0 else 100.0
    print(f"{status} [{idx}/{total}] ({pct:.1f}%) - {step}", end="\r")
    sys.stdout.flush()


def safe_month(value: str) -> str:
    if not isinstance(value, str):
        return "UNKNOWN"
    if len(value) == 7 and value[4] == "-" and value[:4].isdigit() and value[5:].isdigit():
        return value
    return "UNKNOWN"


def safe_non_negative_num(value: Any, default: float = 0.0) -> float:
    try:
        num = float(value)
    except Exception:
        return float(default)
    return max(0.0, num)


def load_taxonomy(taxonomy_path: Path) -> dict[str, Any]:
    """
    Minimal YAML parser for required fields:
    statistical_reference.monthly_baseline + statistical_reference.tag_baselines.
    Kein externer Dependency-Zwang (pyyaml) im System-Python.
    """
    text = taxonomy_path.read_text(encoding="utf-8")
    lines = text.splitlines()

    in_stat_ref = False
    in_monthly = False
    in_tags = False
    current_month = None

    monthly: dict[str, dict[str, float]] = {}
    tags: dict[str, float] = {}

    month_pattern = re.compile(r'^\s*"?(?P<month>\d{4}-\d{2})"?\s*:\s*$')
    value_pattern = re.compile(r"^\s*(?P<key>[a-zA-Z0-9_]+)\s*:\s*(?P<val>[-+]?[0-9]*\.?[0-9]+)\s*$")
    tag_pattern = re.compile(
        r"^\s*(?P<key>[a-zA-Z0-9_]+_pct)\s*:\s*(?P<val>[-+]?[0-9]*\.?[0-9]+)\s*(?:#.*)?$"
    )

    for raw_line in lines:
        line = raw_line.rstrip()
        stripped = line.strip()

        if stripped.startswith("statistical_reference:"):
            in_stat_ref = True
            in_monthly = False
            in_tags = False
            current_month = None
            continue
        if not in_stat_ref:
            continue

        if stripped.startswith("monthly_baseline:"):
            in_monthly = True
            in_tags = False
            current_month = None
            continue
        if stripped.startswith("tag_baselines:"):
            in_monthly = False
            in_tags = True
            current_month = None
            continue

        if in_monthly:
            m = month_pattern.match(line)
            if m:
                current_month = m.group("month")
                monthly[current_month] = {}
                continue
            if current_month:
                vm = value_pattern.match(line)
                if vm:
                    monthly[current_month][vm.group("key")] = float(vm.group("val"))
                    continue
                if stripped and not line.startswith(" " * 6):
                    current_month = None

        if in_tags:
            tm = tag_pattern.match(line)
            if tm:
                tags[tm.group("key")] = float(tm.group("val"))
            elif stripped and not line.startswith(" " * 4):
                in_tags = False

    if not monthly and not tags:
        raise ValueError("Could not extract required fields from taxonomy YAML.")

    return {
        "statistical_reference": {
            "monthly_baseline": monthly,
            "tag_baselines": tags,
        }
    }


def aggregate_sessions_jsonl(sessions_path: Path) -> dict[str, dict[str, float]]:
    monthly: dict[str, dict[str, float]] = defaultdict(lambda: {"sessions": 0, "messages": 0, "days": set()})
    total_lines = sum(1 for _ in sessions_path.open("r", encoding="utf-8"))
    processed = 0
    with sessions_path.open("r", encoding="utf-8") as f:
        for line in f:
            processed += 1
            if not line.strip():
                progress("Session-JSONL parsing", processed, total_lines)
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                progress("Session-JSONL parsing", processed, total_lines, status="❌")
                continue
            if not isinstance(rec, dict):
                progress("Session-JSONL parsing", processed, total_lines, status="❌")
                continue

            month_raw = rec.get("month")
            if not month_raw and isinstance(rec.get("start_timestamp"), str):
                month_raw = rec["start_timestamp"][:7]
            month = safe_month(month_raw)
            if month == "UNKNOWN":
                progress("Session-JSONL parsing", processed, total_lines, status="❌")
                continue

            msg_count = int(safe_non_negative_num(rec.get("message_count"), default=0))
            monthly[month]["sessions"] += 1
            monthly[month]["messages"] += msg_count
            if isinstance(rec.get("start_timestamp"), str) and len(rec["start_timestamp"]) >= 10:
                monthly[month]["days"].add(rec["start_timestamp"][:10])

            progress("Session-JSONL parsing", processed, total_lines)
    print()

    out: dict[str, dict[str, float]] = {}
    for month in sorted(monthly.keys()):
        sessions = int(monthly[month]["sessions"])
        messages = int(monthly[month]["messages"])
        active_days = max(1, len(monthly[month]["days"]))
        out[month] = {
            "sessions": sessions,
            "messages": messages,
            "avg_messages_per_session": round(messages / max(1, sessions), 1),
            "avg_messages_per_day": round(messages / active_days, 1),
        }
    return out


def derive_tag_baselines_from_sessions(sessions_path: Path) -> dict[str, float]:
    total_sessions = 0
    tag_hits: Counter[str] = Counter()
    with sessions_path.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(rec, dict):
                continue
            tags = rec.get("topic_tags", [])
            if isinstance(tags, str):
                tags = [t.strip() for t in tags.split(",") if t.strip()]
            if not isinstance(tags, list):
                continue
            total_sessions += 1
            for tag in set(str(t).strip() for t in tags if str(t).strip()):
                tag_hits[tag] += 1

    tag_map = {
        "memory_operations": "memory_operations_pct",
        "identity_claims": "identity_claims_pct",
        "cross_session_reference": "cross_session_reference_pct",
        "preferences": "preferences_pct",
        "meta_reflection": "meta_reflection_pct",
        "self_repair": "self_repair_pct",
        "meta_diagnostics": "meta_diagnostics_pct",
    }
    result: dict[str, float] = {}
    for source_tag, out_key in tag_map.items():
        pct = (100.0 * tag_hits.get(source_tag, 0) / total_sessions) if total_sessions > 0 else 0.0
        result[out_key] = round(max(0.0, min(100.0, pct)), 1)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Builds surface data from taxonomy and optional session logs.")
    parser.add_argument("--taxonomy", default="appendix_c_taxonomy.yaml", help="Path to taxonomy YAML")
    parser.add_argument("--sessions", default="", help="Optional: path to session JSONL")
    parser.add_argument("--output", default="data/appendix_c_surface_data.json", help="Output JSON path")
    args = parser.parse_args()

    print("This may take a while, please wait...")
    trace_id = f"surface-build-{int(datetime.now(UTC).timestamp())}"
    slog("INFO", "build_surface_dataset.main", "Start Pipeline", trace_id=trace_id, taxonomy=args.taxonomy, sessions=args.sessions, output=args.output)

    try:
        steps = 4
        progress("STEP 1: Validate inputs", 1, steps)
        taxonomy_path = Path(args.taxonomy)
        if not taxonomy_path.exists():
            raise FileNotFoundError(f"Taxonomy file not found: {taxonomy_path}")
        sessions_path = Path(args.sessions) if args.sessions else None
        if sessions_path and not sessions_path.exists():
            raise FileNotFoundError(f"Session JSONL not found: {sessions_path}")

        progress("STEP 2: Load taxonomy", 2, steps)
        taxonomy = load_taxonomy(taxonomy_path)
        taxonomy_ref = taxonomy.get("statistical_reference", {})
        yaml_monthly = taxonomy_ref.get("monthly_baseline", {})
        yaml_tags = taxonomy_ref.get("tag_baselines", {})

        progress("STEP 3: Aggregate data sources", 3, steps)
        if sessions_path:
            monthly = aggregate_sessions_jsonl(sessions_path)
            tags = derive_tag_baselines_from_sessions(sessions_path)
            source = "sessions_jsonl"
        else:
            monthly = {
                safe_month(k): {
                    "sessions": int(safe_non_negative_num(v.get("sessions", 0))),
                    "messages": int(safe_non_negative_num(v.get("messages", 0))),
                    "avg_messages_per_session": round(safe_non_negative_num(v.get("avg_messages_per_session", 0)), 1),
                    "avg_messages_per_day": round(safe_non_negative_num(v.get("avg_messages_per_day", 0)), 1),
                }
                for k, v in (yaml_monthly.items() if isinstance(yaml_monthly, dict) else [])
                if safe_month(k) != "UNKNOWN"
            }
            tags = {
                str(k): round(max(0.0, min(100.0, safe_non_negative_num(v))), 1)
                for k, v in (yaml_tags.items() if isinstance(yaml_tags, dict) else [])
            }
            source = "taxonomy_yaml"

        progress("STEP 4: Write output", 4, steps)
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "version": "surface-data-1.0",
            "description": "Surface data for the Appendix-C demo from real pipeline input.",
            "created": datetime.now(UTC).strftime("%Y-%m-%d"),
            "source": source,
            "taxonomy_path": str(taxonomy_path),
            "sessions_path": str(sessions_path) if sessions_path else "",
            "statistical_reference": {
                "monthly_baseline": monthly,
                "tag_baselines": tags,
            },
        }
        with out_path.open("w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=True)
        print()

        slog(
            "INFO",
            "build_surface_dataset.main",
            "Pipeline completed",
            trace_id=trace_id,
            output_file=str(out_path),
            month_count=len(payload["statistical_reference"]["monthly_baseline"]),
            tag_count=len(payload["statistical_reference"]["tag_baselines"]),
            source=source,
        )
    except Exception as exc:
        slog(
            "CRITICAL",
            "build_surface_dataset.main",
            "Pipeline failed",
            trace_id=trace_id,
            error_code="SURFACE_PIPELINE_FATAL",
            error_message=str(exc),
            stack_trace=traceback.format_exc(),
        )
        raise


if __name__ == "__main__":
    main()
