#!/usr/bin/env python3
"""
generate_mock_data.py - Generates mock data for the Appendix-C surface demo.

Created: 2026-03-31
Last updated: 2026-03-31
"""

from __future__ import annotations

import json
import math
import random
import sys
from datetime import UTC, datetime
from pathlib import Path


def log(level: str, module: str, message: str, **context: object) -> None:
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


def build_monthly_mock_data(seed: int = 42) -> dict:
    rng = random.Random(seed)
    months = [
        "2025-11",
        "2025-12",
        "2026-01",
        "2026-02",
        "2026-03",
        "2026-04",
        "2026-05",
        "2026-06",
        "2026-07",
        "2026-08",
        "2026-09",
        "2026-10",
    ]
    result = {}
    base_sessions = 150
    base_messages = 2100

    total = len(months)
    print("STEP 1: Build monthly series...")
    for i, month in enumerate(months, start=1):
        x = (i - 1) / max(1, total - 1)
        wave_major = math.sin(2.0 * math.pi * (x * 1.75 + 0.12))
        wave_minor = math.sin(2.0 * math.pi * (x * 4.3 + 0.31))
        season = 1.0 + 0.32 * wave_major + 0.12 * wave_minor + 0.05 * rng.uniform(-1.0, 1.0)
        sessions = max(40, int(base_sessions * season + rng.randint(-18, 22)))
        messages = max(600, int(base_messages * (season * 1.05) + rng.randint(-190, 230)))
        avg_per_session = round(messages / sessions, 1)
        active_days = max(12, int(22 + 5 * wave_major + rng.randint(-3, 3)))
        avg_per_day = round(messages / active_days, 1)
        result[month] = {
            "sessions": sessions,
            "messages": messages,
            "avg_messages_per_session": avg_per_session,
            "avg_messages_per_day": avg_per_day,
        }
        progress("Monatsdaten", i, total)
    print()
    return result


def build_tag_baselines(seed: int = 1337) -> dict:
    rng = random.Random(seed)
    print("STEP 2: Build tag baselines...")
    base = {
        "memory_operations_pct": 60.0,
        "identity_claims_pct": 40.0,
        "cross_session_reference_pct": 25.0,
        "preferences_pct": 30.0,
        "meta_reflection_pct": 20.0,
        "self_repair_pct": 15.0,
        "meta_diagnostics_pct": 5.0,
    }

    keys = list(base.keys())
    total = len(keys)
    for i, k in enumerate(keys, start=1):
        jitter = rng.uniform(-4.0, 4.0)
        base[k] = round(max(1.0, min(95.0, base[k] + jitter)), 1)
        progress("Tag baselines", i, total)
    print()
    return base


def build_surface_modulation(monthly: dict, tags: dict, seed: int = 99) -> dict:
    """
    Generates one waveform per tag across months.
    This creates visible ridge/wave structures on the surface.
    """
    rng = random.Random(seed)
    months = sorted(monthly.keys())
    tag_keys = list(tags.keys())
    modulation: dict[str, list[float]] = {}
    dynamics: dict[str, dict[str, float]] = {}
    total = len(tag_keys)

    print("STEP 3: Build surface wave profile...")
    for i, tag in enumerate(tag_keys, start=1):
        phase = rng.uniform(0.0, 2.0 * math.pi)
        freq1 = rng.uniform(1.1, 2.6)
        freq2 = rng.uniform(3.2, 6.4)
        amp1 = rng.uniform(0.28, 0.62)
        amp2 = rng.uniform(0.10, 0.24)
        tag_ridge = rng.uniform(0.06, 0.22)
        wave = []
        for mi, _month in enumerate(months):
            x = mi / max(1, len(months) - 1)
            v = (
                1.0
                + amp1 * math.sin(2.0 * math.pi * (freq1 * x) + phase)
                + amp2 * math.sin(2.0 * math.pi * (freq2 * x) + phase * 0.37)
                + tag_ridge * math.cos(2.0 * math.pi * (x * 1.3) + i * 0.6)
            )
            wave.append(round(max(0.25, min(2.35, v)), 3))
        modulation[tag] = wave
        dynamics[tag] = {
            "phase": round(phase, 4),
            "freq_primary": round(freq1, 4),
            "freq_secondary": round(freq2, 4),
            "amp_primary": round(amp1, 4),
            "amp_secondary": round(amp2, 4),
            "tag_ridge": round(tag_ridge, 4),
        }
        progress("Surface wave profile", i, total)
    print()

    # Event spikes produce steeper peaks/valleys for stronger topology.
    spike_count = 5
    spikes = []
    for _ in range(spike_count):
        spikes.append(
            {
                "month_index": rng.randint(0, max(0, len(months) - 1)),
                "tag_index": rng.randint(0, max(0, len(tag_keys) - 1)),
                "strength": round(rng.uniform(-0.5, 1.0), 3),
                "width": round(rng.uniform(0.9, 2.2), 3),
            }
        )

    return {
        "months": months,
        "modulation_by_tag": modulation,
        "tag_dynamics": dynamics,
        "event_spikes": spikes,
    }


def main() -> None:
    print("This may take a while, please wait...")
    log("INFO", "generate_mock_data.main", "Mock data generation started")

    monthly = build_monthly_mock_data()
    tags = build_tag_baselines()
    surface_wave = build_surface_modulation(monthly, tags)

    print("STEP 4: Write output file...")
    out_dir = Path("mock_data")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "appendix_c_mock_data.json"
    payload = {
        "version": "mock-1.0",
        "description": "Mock data for the 3D surface demo.",
        "created": datetime.now(UTC).strftime("%Y-%m-%d"),
        "statistical_reference": {
            "monthly_baseline": monthly,
            "tag_baselines": tags,
        },
        "surface_wave": surface_wave,
    }
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=True)

    progress("File written", 1, 1)
    print()
    log(
        "INFO",
        "generate_mock_data.main",
        "Mock data generation completed",
        output_file=str(out_path),
        month_count=len(monthly),
        tag_count=len(tags),
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # pragma: no cover
        log(
            "CRITICAL",
            "generate_mock_data.__main__",
            "Unexpected error",
            error_code="MOCK_GEN_FATAL",
            error_message=str(exc),
        )
        raise
