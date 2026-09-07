#!/usr/bin/env python3
"""
regime_timing_signal.py — Model B Regime Timing Signal
Schema v1 (2026-05-14)

Structural analog to vrp_timing_signal() from the VRP harvesting framework:

    vrp_timing_signal = f(5d_change, 10d_change, percentile) → action_type

This adapts that minimal architecture for Model B's regime + conviction space:

    regime_timing_signal = f(
        conviction_trend_5s,    # mirrors VRP 5d change
        conviction_trend_10s,   # mirrors VRP 10d change
        conviction_pct_rank,    # mirrors VRP percentile rank
        regime_trend_5s,        # no VRP equivalent — regime state has directionality
    ) → ENTER | HOLD | WAIT | EXIT

The signal does not replace the gate or conviction scoring. It answers a
different question: given the session-over-session trajectory of the system,
is the regime currently accumulating edge, holding, fading, or structurally
broken? Useful as a lightweight meta-signal for TERZETTO Layer 0 routing.

Input CSV schema (session-level, one row per session):
    session_date    : YYYY-MM-DD
    regime          : COMPRESS | TRENDING | BASING | POST_SHOCK | NEUTRAL
    conviction_mean : float   — session mean conviction score
    near_fire_count : int     — evaluations in NEAR_FIRE bucket
    evaluated_count : int     — total evaluated records (excl. skip_time etc.)

Output adds columns:
    conviction_trend_5s   : delta conviction_mean over last 5 sessions
    conviction_trend_10s  : delta conviction_mean over last 10 sessions
    conviction_pct_rank   : rolling percentile rank within last 20 sessions
    regime_trend_5s       : delta regime_score over last 5 sessions
    signal                : ENTER | HOLD | WAIT | EXIT

Usage:
    python regime_timing_signal.py --input session_summary.csv
    python regime_timing_signal.py --input session_summary.csv --out signals.csv
    python regime_timing_signal.py --demo          # synthetic data walkthrough
"""

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

# ── Regime encoding ───────────────────────────────────────────────────────────
# Ordered by systematic readiness for entry.
# COMPRESS = most hostile (all gates blocked).
# POST_SHOCK = structurally uncertain, not directionally hostile.
# NEUTRAL → BASING → TRENDING = increasing readiness.
REGIME_SCORE: dict[str, float] = {
    "COMPRESS":   0.00,
    "POST_SHOCK": 0.25,
    "NEUTRAL":    0.50,
    "BASING":     0.75,
    "TRENDING":   1.00,
}

# ── Custom regime mapping (for adapters using different classifiers) ──────────
# If your classifier produces different state names or uses a continuous
# readiness score rather than discrete states, replace REGIME_SCORE above.
#
# Requirements for a valid mapping:
#   1. Keys must match the regime strings in your input CSV exactly.
#   2. Values must be floats in [0.0, 1.0] — 0.0 = most hostile, 1.0 = most ready.
#   3. The scale must be ordinal: higher score = more favorable for entry.
#      The signal uses score differences (regime_trend_5s) so spacing matters.
#
# Example — GARCH-based vol regime classifier with 4 states:
#   REGIME_SCORE = {
#       "HIGH_VOL_CONTRACTION": 0.00,   # vol spiking, structure breaking
#       "ELEVATED_STABLE":      0.33,   # vol elevated but not expanding
#       "NORMAL_RANGE":         0.67,   # vol within expected conformal bands
#       "LOW_VOL_EXPANSION":    1.00,   # vol compressing → trending setup
#   }
#
# Example — continuous readiness score already in [0,1] from your own model:
#   If your classifier outputs a float directly (e.g. a conformal coverage
#   probability), skip REGIME_SCORE entirely and add a "regime_score" column
#   to your input CSV. load_sessions() will use it as-is without mapping.
#   Set the unknown-regime fallback in load_sessions() to your neutral value.
#
# The REGIME_ENTER_MIN and REGIME_EXIT_DELTA thresholds below scale with
# whatever mapping you use — recalibrate them after 10+ sessions.

# ── Rolling windows ───────────────────────────────────────────────────────────
ROLL_SHORT = 5    # sessions (mirrors VRP 5d window)
ROLL_LONG  = 10   # sessions (mirrors VRP 10d window)
ROLL_PCT   = 20   # sessions for percentile rank baseline

# ── Signal thresholds ─────────────────────────────────────────────────────────
# These are calibration-phase defaults — tune once 10+ sessions accumulate.
TREND_ENTER_MIN    = 0.08   # conviction must be rising by this over 5s to ENTER
TREND_EXIT_MAX     = -0.06  # conviction falling by this over 5s (+ regime drop) → EXIT
PCT_RANK_WAIT_MAX  = 0.40   # below this percentile → WAIT regardless
PCT_RANK_ENTER_MIN = 0.55   # must be above this to ENTER
REGIME_ENTER_MIN   = 0.60   # regime score floor for ENTER (BASING or TRENDING)
REGIME_EXIT_DELTA  = -0.25  # regime must drop by this over 5s to trigger EXIT


# ── Data loading ──────────────────────────────────────────────────────────────

def load_sessions(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, parse_dates=["session_date"])
    df = df.sort_values("session_date").reset_index(drop=True)

    required = {"session_date", "regime", "conviction_mean", "near_fire_count", "evaluated_count"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    df["regime_score"] = df["regime"].map(REGIME_SCORE)
    if df["regime_score"].isna().any():
        unknown = df.loc[df["regime_score"].isna(), "regime"].unique().tolist()
        print(f"[warn] Unknown regime values defaulted to NEUTRAL: {unknown}")
        df["regime_score"] = df["regime_score"].fillna(REGIME_SCORE["NEUTRAL"])

    df["near_fire_rate"] = np.where(
        df["evaluated_count"] > 0,
        df["near_fire_count"] / df["evaluated_count"],
        0.0,
    )
    return df


# ── Feature computation ───────────────────────────────────────────────────────

def compute_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Conviction trends — direct structural mirror of VRP 5d/10d change
    df["conviction_trend_5s"]  = df["conviction_mean"].diff(ROLL_SHORT)
    df["conviction_trend_10s"] = df["conviction_mean"].diff(ROLL_LONG)

    # Regime trajectory — is the regime state improving or degrading?
    df["regime_trend_5s"] = df["regime_score"].diff(ROLL_SHORT)

    # Percentile rank of conviction_mean within rolling window
    # Mirrors VRP percentile: where does today's conviction sit historically?
    df["conviction_pct_rank"] = (
        df["conviction_mean"]
        .rolling(ROLL_PCT, min_periods=3)
        .rank(pct=True)
    )

    return df


# ── Signal classification ─────────────────────────────────────────────────────

def classify_signal(row: pd.Series) -> str:
    """
    Four-state signal — same structure as vrp_timing_signal action types.

    Priority order: EXIT > WAIT > ENTER > HOLD

    EXIT  — conviction falling and regime deteriorating. Edge is leaving.
    WAIT  — conviction in low percentile. Not enough signal to act.
    ENTER — conviction rising, percentile confirms, regime is supportive.
    HOLD  — not deteriorating, not ready. Stay present, don't add.
    """
    # Insufficient history — stay neutral
    if pd.isna(row["conviction_trend_5s"]) or pd.isna(row["conviction_pct_rank"]):
        return "WAIT"

    trend_5s     = float(row["conviction_trend_5s"])
    trend_10s    = float(row["conviction_trend_10s"]) if not pd.isna(row.get("conviction_trend_10s")) else 0.0
    pct_rank     = float(row["conviction_pct_rank"])
    regime_score = float(row["regime_score"])
    regime_trend = float(row["regime_trend_5s"]) if not pd.isna(row.get("regime_trend_5s")) else 0.0

    # EXIT: conviction fading AND regime degrading — both conditions required
    # to avoid false exits on single-session conviction dips
    if trend_5s <= TREND_EXIT_MAX and regime_trend <= REGIME_EXIT_DELTA:
        return "EXIT"

    # WAIT: conviction too low in its own historical distribution
    # Regime may be fine but the system isn't accumulating signal
    if pct_rank < PCT_RANK_WAIT_MAX:
        return "WAIT"

    # ENTER: all three dimensions aligned
    if (
        trend_5s >= TREND_ENTER_MIN
        and pct_rank >= PCT_RANK_ENTER_MIN
        and regime_score >= REGIME_ENTER_MIN
    ):
        return "ENTER"

    # HOLD: not broken, not ready — maintain current stance
    return "HOLD"


# ── Reporting ─────────────────────────────────────────────────────────────────

SIGNAL_LABELS = {
    "ENTER": "▲ ENTER",
    "HOLD":  "— HOLD ",
    "WAIT":  "· WAIT ",
    "EXIT":  "▼ EXIT ",
}


def print_report(df: pd.DataFrame) -> None:
    print("\n══ REGIME TIMING SIGNAL ═══════════════════════════════════════════════════")
    print(f"  Sessions analyzed : {len(df)}")
    print(f"  Date range        : {df['session_date'].min().date()} → {df['session_date'].max().date()}")
    print(f"  Windows           : short={ROLL_SHORT}s  long={ROLL_LONG}s  pct_rank_n={ROLL_PCT}s")
    print()

    hdr = f"{'Date':<13} {'Regime':<12} {'Conv':>7} {'Δ5s':>8} {'Δ10s':>8} {'Pct':>6}  Signal"
    print(hdr)
    print("─" * len(hdr))

    for _, row in df.iterrows():
        date_str  = row["session_date"].strftime("%Y-%m-%d")
        regime    = row["regime"]
        conv      = f"{row['conviction_mean']:.4f}"
        t5        = f"{row['conviction_trend_5s']:+.4f}"  if not pd.isna(row["conviction_trend_5s"])  else "    N/A"
        t10       = f"{row['conviction_trend_10s']:+.4f}" if not pd.isna(row["conviction_trend_10s"]) else "    N/A"
        pct       = f"{row['conviction_pct_rank']:.2f}"   if not pd.isna(row["conviction_pct_rank"])  else "  N/A"
        signal    = SIGNAL_LABELS.get(row["signal"], row["signal"])
        print(f"{date_str:<13} {regime:<12} {conv:>7} {t5:>8} {t10:>8} {pct:>6}  {signal}")

    print()
    print("── SIGNAL DISTRIBUTION ────────────────────────────────────────────────────")
    dist = df["signal"].value_counts()
    for sig in ["ENTER", "HOLD", "WAIT", "EXIT"]:
        count = dist.get(sig, 0)
        pct   = 100 * count / len(df)
        bar   = "█" * int(pct / 3)
        print(f"  {SIGNAL_LABELS[sig]}  {count:>3}  {pct:5.1f}%  {bar}")

    print()
    recent = df.tail(ROLL_SHORT)
    recent_dist = recent["signal"].value_counts()
    dominant = recent_dist.index[0] if len(recent_dist) else "N/A"
    latest   = df.iloc[-1]
    print("── CURRENT STATE ──────────────────────────────────────────────────────────")
    print(f"  Latest signal     : {SIGNAL_LABELS.get(latest['signal'], latest['signal'])}")
    print(f"  Dominant (last 5s): {dominant}")
    print(f"  Regime            : {latest['regime']}  (score={latest['regime_score']:.2f})")
    print(f"  Conviction mean   : {latest['conviction_mean']:.4f}")
    if not pd.isna(latest["conviction_pct_rank"]):
        print(f"  Pct rank          : {latest['conviction_pct_rank']:.2f}  (top {100*(1-latest['conviction_pct_rank']):.0f}%)")
    print()


# ── Demo mode ─────────────────────────────────────────────────────────────────

def run_demo() -> None:
    """
    Synthetic session sequence demonstrating all four signal states.
    Intended for sharing with collaborators who don't have live data.
    """
    print("[demo] Generating synthetic session sequence...")

    rng = np.random.default_rng(42)

    regimes = (
        ["COMPRESS"] * 4
        + ["NEUTRAL"] * 3
        + ["BASING"] * 4
        + ["TRENDING"] * 5
        + ["TRENDING"] * 3
        + ["POST_SHOCK"] * 3
        + ["COMPRESS"] * 2
    )

    n = len(regimes)
    base_conviction = np.linspace(0.28, 0.55, n) + rng.normal(0, 0.02, n)
    # Inject a fade at the end (POST_SHOCK → COMPRESS)
    base_conviction[-5:] = np.linspace(0.52, 0.31, 5) + rng.normal(0, 0.015, 5)

    dates = pd.date_range("2026-03-01", periods=n, freq="B")

    near_fire_count = (base_conviction * 12 + rng.integers(0, 3, n)).clip(0).astype(int)
    evaluated_count = rng.integers(8, 20, n)

    df = pd.DataFrame({
        "session_date":    dates,
        "regime":          regimes,
        "conviction_mean": base_conviction.round(4),
        "near_fire_count": near_fire_count,
        "evaluated_count": evaluated_count,
    })

    df["regime_score"] = df["regime"].map(REGIME_SCORE)
    df["near_fire_rate"] = df["near_fire_count"] / df["evaluated_count"]
    df = compute_features(df)
    df["signal"] = df.apply(classify_signal, axis=1)
    print_report(df)

    print("── DESIGN NOTES ────────────────────────────────────────────────────────────")
    print("  Structural mirror of vrp_timing_signal():")
    print("    VRP:     5d_change + 10d_change + percentile → action_type")
    print("    Model B: Δconv_5s  + Δconv_10s  + pct_rank  + regime_trend → action_type")
    print()
    print("  Thresholds are calibration-phase defaults.")
    print("  Tune after 10+ sessions using near_fire_rate as the ground truth signal.")
    print()


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Model B Regime Timing Signal — vrp_timing_signal() analog"
    )
    parser.add_argument("--input", default=None, help="Path to session summary CSV")
    parser.add_argument("--out",   default=None, help="Write signal CSV to this path")
    parser.add_argument("--demo",  action="store_true", help="Run synthetic demo (no input needed)")
    args = parser.parse_args()

    if args.demo:
        run_demo()
        return

    if not args.input:
        parser.print_help()
        sys.exit(1)

    path = Path(args.input)
    if not path.exists():
        print(f"Error: file not found: {path}", file=sys.stderr)
        sys.exit(1)

    df = load_sessions(path)
    df = compute_features(df)
    df["signal"] = df.apply(classify_signal, axis=1)
    print_report(df)

    if args.out:
        out_cols = [
            "session_date", "regime", "conviction_mean", "near_fire_rate",
            "conviction_trend_5s", "conviction_trend_10s",
            "conviction_pct_rank", "regime_trend_5s", "signal",
        ]
        df[out_cols].to_csv(args.out, index=False)
        print(f"Signal CSV written → {args.out}")


if __name__ == "__main__":
    main()