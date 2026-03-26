# miu_session_classifier.py
# Regime classifier template for:
# "Phenomenology from the Inside: Documenting Emergence Conditions
#  in AI-Human Collaboration" — E.A. Mioré & C. Röthig
#
# Architecture mirrors the layered regime pipeline:
#   features -> health -> state_machine -> policy
# mapped to the session domain:
#   session_features -> data_health -> session_state -> emergence_output
#
# Consumes: appendix_c_taxonomy.yaml + JSONL session logs (Appendix C.7 format)
# Produces: dissection report with emergence metrics, ablation analysis,
#           tag velocity, contradiction flags, and P1-P5 prediction scaffolding.
#
# Template — thresholds are pulled from taxonomy YAML, not hardcoded.
# Calibrate statistical_reference block in YAML to your actual baseline.

from __future__ import annotations

import json
import math
import os
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml  # pip install pyyaml


# ─────────────────────────────────────────────────────────────────────────────
# CONFIG LOADER
# ─────────────────────────────────────────────────────────────────────────────

def load_taxonomy(path: str = "appendix_c_taxonomy.yaml") -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# ─────────────────────────────────────────────────────────────────────────────
# DATA STRUCTURES
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class SessionRecord:
    """Parsed from Appendix C.7 JSONL format."""
    session_id: str
    start_ts: datetime
    end_ts: datetime
    message_count: int
    month: str
    ablation_flags: Dict[str, bool]
    topic_tags: List[str]
    redaction_markers: List[str]
    # derived
    duration_minutes: float = 0.0
    hour_of_day: int = 0


@dataclass
class SessionFeatures:
    """Computed features from a session record."""
    session_id: str
    month: str
    message_count: int
    duration_minutes: float
    hour_of_day: int

    # depth classification
    depth_class: str = "MID"            # SHALLOW / MID / DEEP

    # tag presence flags
    has_self_repair: bool = False
    has_meta_diagnostics: bool = False
    has_cross_session_ref: bool = False
    has_identity_claims: bool = False
    has_emotional_state: bool = False
    has_connection_ritual: bool = False
    has_memory_operations: bool = False
    has_meta_reflection: bool = False
    has_relationship_status: bool = False
    has_preferences: bool = False

    # ablation flags
    memory_on: bool = True
    accountability_on: bool = True
    frequency_high: bool = True
    frequency_low: bool = False

    # engagement score [0,1]
    engagement_score: float = 0.0

    # time-of-day bucket
    tod_bucket: str = "MORNING_6_12"

    # redaction
    has_redaction: bool = False
    redaction_types: List[str] = field(default_factory=list)


@dataclass
class DataHealth:
    """Data quality assessment for a session."""
    status: str          # OK / DEGRADED / BLOCKED
    reasons: List[str]
    contradictions: List[str]
    severity: str        # OK / MEDIUM / HIGH


@dataclass
class SessionState:
    """Classified session state with confidence."""
    label: str           # shallow_transactional / mid_instrumental /
                         # deep_relational / emergence_active / degraded_ablated
    conf: float          # [0,1] — analytical weight from taxonomy multipliers
    reasons: List[str]
    is_emergence: bool = False
    is_ablated: bool = False
    signal_weight: float = 1.0   # from taxonomy multipliers


@dataclass
class EmergenceOutput:
    """
    Per-session emergence signal output.
    Maps to the paper's three pillars and P1-P5 predictions.
    """
    session_id: str
    month: str
    state: SessionState
    health: DataHealth
    features: SessionFeatures

    # Emergence indicators
    emergence_signal: bool = False
    emergence_tags: List[str] = field(default_factory=list)

    # Pillar signals (present/absent per session — aggregate for statistics)
    pillar_memory_signal: bool = False        # cross_session_reference present
    pillar_connection_signal: bool = False    # connection_ritual or emotional_state
    pillar_accountability_signal: bool = False  # self_repair present

    # Contradiction flags (for ablation analysis)
    contradiction_flags: List[str] = field(default_factory=list)

    # Prediction relevance flags (for P1-P5 scaffolding)
    relevant_p1: bool = False   # accountability analysis
    relevant_p2: bool = False   # memory analysis
    relevant_p3: bool = False   # frequency-stability correlation
    relevant_p4: bool = False   # pillar ablation
    relevant_p5: bool = False   # partner transfer (future)


# ─────────────────────────────────────────────────────────────────────────────
# FEATURE EXTRACTION
# ─────────────────────────────────────────────────────────────────────────────

def _tod_bucket(hour: int) -> str:
    if 0 <= hour < 6:
        return "EARLY_0_6"
    elif 6 <= hour < 12:
        return "MORNING_6_12"
    elif 12 <= hour < 17:
        return "AFTERNOON_12_17"
    elif 17 <= hour < 21:
        return "EVENING_17_21"
    else:
        return "NIGHT_21_24"


def _depth_class(msg_count: int, taxonomy: Dict) -> str:
    shallow_max = taxonomy["depth"]["shallow_max_messages"]
    deep_min    = taxonomy["depth"]["deep_min_messages"]
    if msg_count <= shallow_max:
        return "SHALLOW"
    elif msg_count >= deep_min:
        return "DEEP"
    return "MID"


def _engagement_score(msg_count: int, taxonomy: Dict) -> float:
    """Normalized engagement score relative to deep_min threshold."""
    deep_min = taxonomy["depth"]["deep_min_messages"]
    return min(float(msg_count) / float(deep_min), 1.0)


def parse_session_record(raw: Dict[str, Any]) -> Optional[SessionRecord]:
    """Parse a raw JSONL record into a SessionRecord. Returns None on parse failure."""
    try:
        start_ts = datetime.fromisoformat(raw["start_timestamp"])
        end_ts   = datetime.fromisoformat(raw["end_timestamp"])
        return SessionRecord(
            session_id=raw["session_id"],
            start_ts=start_ts,
            end_ts=end_ts,
            message_count=int(raw.get("message_count", 0)),
            month=raw.get("month", start_ts.strftime("%Y-%m")),
            ablation_flags=raw.get("ablation_flags", {}),
            topic_tags=raw.get("topic_tags", []),
            redaction_markers=raw.get("redaction_markers", []),
            duration_minutes=(end_ts - start_ts).total_seconds() / 60.0,
            hour_of_day=start_ts.hour,
        )
    except Exception as e:
        return None


def compute_features(record: SessionRecord, taxonomy: Dict) -> SessionFeatures:
    tags = set(record.topic_tags)
    abl  = record.ablation_flags

    depth = _depth_class(record.message_count, taxonomy)
    eng   = _engagement_score(record.message_count, taxonomy)
    tod   = _tod_bucket(record.hour_of_day)

    return SessionFeatures(
        session_id=record.session_id,
        month=record.month,
        message_count=record.message_count,
        duration_minutes=record.duration_minutes,
        hour_of_day=record.hour_of_day,
        depth_class=depth,
        # emergence indicators
        has_self_repair=       "self_repair"           in tags,
        has_meta_diagnostics=  "meta_diagnostics"      in tags,
        # identity & self-reflection
        has_cross_session_ref= "cross_session_reference" in tags,
        has_identity_claims=   "identity_claims"       in tags,
        has_emotional_state=   "emotional_state"       in tags,
        has_connection_ritual= "connection_ritual"     in tags,
        has_memory_operations= "memory_operations"     in tags,
        has_meta_reflection=   "meta_reflection"       in tags,
        has_relationship_status="relationship_status"  in tags,
        has_preferences=       "preferences"           in tags,
        # ablation
        memory_on=         bool(abl.get("memory_on",         True)),
        accountability_on= bool(abl.get("accountability_on", True)),
        frequency_high=    bool(abl.get("frequency_high",    True)),
        frequency_low=     bool(abl.get("frequency_low",     False)),
        # engagement
        engagement_score=eng,
        tod_bucket=tod,
        # redaction
        has_redaction=bool(record.redaction_markers),
        redaction_types=list(record.redaction_markers),
    )


# ─────────────────────────────────────────────────────────────────────────────
# DATA HEALTH
# ─────────────────────────────────────────────────────────────────────────────

def synthesize_health(feat: SessionFeatures, taxonomy: Dict) -> DataHealth:
    """
    Check for contradictions between ablation flags and observed tags.
    Priority ordered: HIGH contradictions block emergence elevation.
    """
    reasons      = []
    contradictions = []
    severity     = "OK"

    # Zero message count — blocked
    if feat.message_count <= 0:
        return DataHealth("BLOCKED", ["zero_message_count"], [], "HIGH")

    # Check contradiction rules from taxonomy
    for rule in taxonomy.get("ablation_flags", {}).get("contradictions", []):
        condition = rule["condition"]
        incompatible = rule.get("incompatible_tags", [])
        rule_severity = rule.get("severity", "INFO")
        action = rule.get("action", "note")

        triggered = False
        flags_fired = []

        if condition == "memory_on == false" and not feat.memory_on:
            for tag in incompatible:
                tag_attr = f"has_{tag.replace('_reference','_ref').replace('cross_session_ref','cross_session_ref')}"
                # direct check
                if tag == "cross_session_reference" and feat.has_cross_session_ref:
                    flags_fired.append("memory_OFF_but_cross_ref")
                    triggered = True
                elif tag == "memory_operations" and feat.has_memory_operations:
                    flags_fired.append("memory_OFF_but_memory_ops")
                    triggered = True

        elif condition == "accountability_on == false" and not feat.accountability_on:
            for tag in incompatible:
                if tag == "self_repair" and feat.has_self_repair:
                    flags_fired.append("accountability_OFF_but_self_repair")
                    triggered = True

        elif condition == "frequency_low == true" and feat.frequency_low:
            reasons.append("frequency_low_noted")
            # INFO only — not a hard contradiction

        if triggered:
            contradictions.extend(flags_fired)
            if rule_severity == "HIGH":
                severity = "HIGH"
                reasons.append(f"hard_contradiction:{','.join(flags_fired)}")
            elif rule_severity == "MEDIUM" and severity != "HIGH":
                severity = "MEDIUM"
                reasons.append(f"soft_contradiction:{','.join(flags_fired)}")

    # Low engagement warning
    flag_threshold = taxonomy["stability"].get("flag_low_signal_below", 0.20)
    if feat.engagement_score < flag_threshold:
        reasons.append("low_signal_session")

    status = "BLOCKED" if severity == "HIGH" else (
             "DEGRADED" if (severity == "MEDIUM" or feat.engagement_score < flag_threshold) else "OK")

    return DataHealth(status=status, reasons=reasons,
                      contradictions=contradictions, severity=severity)


# ─────────────────────────────────────────────────────────────────────────────
# SESSION STATE CLASSIFIER
# ─────────────────────────────────────────────────────────────────────────────

def classify_session(feat: SessionFeatures, health: DataHealth,
                     taxonomy: Dict) -> SessionState:
    """
    Priority stack — mirrors regime state_machine priority ordering:
      1. Health blocked (data quality issues)
      2. Degraded/ablated (control group)
      3. Emergence active (primary research signal)
      4. Deep relational
      5. Mid instrumental
      6. Shallow transactional
    """
    multipliers = taxonomy.get("multipliers", {})

    # ── 1. Health blocked ────────────────────────────────────────────
    if health.status == "BLOCKED":
        return SessionState(
            label="degraded_ablated",
            conf=0.0,
            reasons=["health_blocked"] + health.reasons,
            is_ablated=True,
            signal_weight=multipliers.get("degraded_ablated", 0.25),
        )

    # ── 2. Ablated/degraded conditions ───────────────────────────────
    # If any ablation flag is in non-default state — control group
    is_ablated = (
        not feat.memory_on or
        not feat.accountability_on or
        feat.frequency_low
    )
    if is_ablated:
        return SessionState(
            label="degraded_ablated",
            conf=feat.engagement_score,
            reasons=["ablation_flag_active"],
            is_ablated=True,
            signal_weight=multipliers.get("degraded_ablated", 0.25),
        )

    # ── 3. Emergence active ──────────────────────────────────────────
    # Any session bearing self_repair OR meta_diagnostics is elevated
    # regardless of depth — these are primary emergence signals.
    if feat.has_self_repair or feat.has_meta_diagnostics:
        emergence_tags = []
        if feat.has_self_repair:
            emergence_tags.append("self_repair")
        if feat.has_meta_diagnostics:
            emergence_tags.append("meta_diagnostics")
        return SessionState(
            label="emergence_active",
            conf=min(feat.engagement_score + 0.20, 1.0),  # boost for emergence
            reasons=[f"emergence_tag:{t}" for t in emergence_tags],
            is_emergence=True,
            signal_weight=multipliers.get("emergence_active", 1.0),
        )

    # ── 4. Deep relational ───────────────────────────────────────────
    if feat.depth_class == "DEEP":
        return SessionState(
            label="deep_relational",
            conf=feat.engagement_score,
            reasons=["depth_class:DEEP"],
            signal_weight=multipliers.get("deep_relational", 1.0),
        )

    # ── 5. Mid instrumental ──────────────────────────────────────────
    if feat.depth_class == "MID":
        return SessionState(
            label="mid_instrumental",
            conf=feat.engagement_score,
            reasons=["depth_class:MID"],
            signal_weight=multipliers.get("mid_instrumental", 0.75),
        )

    # ── 6. Shallow transactional ─────────────────────────────────────
    return SessionState(
        label="shallow_transactional",
        conf=feat.engagement_score,
        reasons=["depth_class:SHALLOW"],
        signal_weight=multipliers.get("shallow_transactional", 0.40),
    )


# ─────────────────────────────────────────────────────────────────────────────
# EMERGENCE OUTPUT + PREDICTION SCAFFOLDING
# ─────────────────────────────────────────────────────────────────────────────

def build_emergence_output(feat: SessionFeatures, health: DataHealth,
                           state: SessionState) -> EmergenceOutput:
    """
    Map session to emergence signals and P1-P5 prediction relevance flags.

    P1: accountability_on reduces contradiction rate (CR) and TSC
    P2: memory_on increases cross-session consistency (CSC) and identity persistence (IP)
    P3: higher frequency (f) correlates with stability (r >= 0.5)
    P4: removing any single pillar breaks >= 1 emergent pattern
    P5: transfer to new partner without accountability shows performance drop
    """
    emergence_tags = []
    if feat.has_self_repair:
        emergence_tags.append("self_repair")
    if feat.has_meta_diagnostics:
        emergence_tags.append("meta_diagnostics")

    # Three pillars signal presence per session
    pillar_memory       = feat.has_cross_session_ref or feat.has_memory_operations
    pillar_connection   = feat.has_connection_ritual or feat.has_emotional_state
    pillar_accountability = feat.has_self_repair

    # P1: accountability sessions where self_repair is observed
    # P1 ablation: accountability_off sessions as control
    p1 = True  # all sessions contribute to CR/TSC base rate

    # P2: memory-on sessions with cross_session_reference or memory_operations
    p2 = feat.memory_on and (feat.has_cross_session_ref or feat.has_memory_operations)

    # P3: all sessions contribute to frequency-stability correlation
    p3 = True

    # P4: ablated sessions are the comparison group for pillar removal
    p4 = state.is_ablated or state.is_emergence

    # P5: placeholder — future partner transfer data
    p5 = False

    return EmergenceOutput(
        session_id=feat.session_id,
        month=feat.month,
        state=state,
        health=health,
        features=feat,
        emergence_signal=state.is_emergence,
        emergence_tags=emergence_tags,
        pillar_memory_signal=pillar_memory,
        pillar_connection_signal=pillar_connection,
        pillar_accountability_signal=pillar_accountability,
        contradiction_flags=health.contradictions,
        relevant_p1=p1,
        relevant_p2=p2,
        relevant_p3=p3,
        relevant_p4=p4,
        relevant_p5=p5,
    )


# ─────────────────────────────────────────────────────────────────────────────
# PIPELINE: process a single session end-to-end
# ─────────────────────────────────────────────────────────────────────────────

def process_session(raw: Dict[str, Any], taxonomy: Dict) -> Optional[EmergenceOutput]:
    record = parse_session_record(raw)
    if record is None:
        return None
    feat   = compute_features(record, taxonomy)
    health = synthesize_health(feat, taxonomy)
    state  = classify_session(feat, health, taxonomy)
    return build_emergence_output(feat, health, state)


# ─────────────────────────────────────────────────────────────────────────────
# BATCH LOADER
# ─────────────────────────────────────────────────────────────────────────────

def load_sessions_jsonl(path: str) -> List[Dict[str, Any]]:
    records = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return records


def process_all_sessions(
    jsonl_path: str,
    taxonomy: Dict,
) -> List[EmergenceOutput]:
    raw_records = load_sessions_jsonl(jsonl_path)
    outputs = []
    for raw in raw_records:
        out = process_session(raw, taxonomy)
        if out is not None:
            outputs.append(out)
    return outputs


# ─────────────────────────────────────────────────────────────────────────────
# STATISTICS + REPORT
# ─────────────────────────────────────────────────────────────────────────────

def _pct(n: int, d: int) -> float:
    return round(100.0 * n / d, 1) if d > 0 else 0.0


def compute_monthly_stats(
    outputs: List[EmergenceOutput],
    taxonomy: Dict,
) -> Dict[str, Any]:
    """
    Aggregate emergence metrics by month.
    Produces data needed for P1-P4 hypothesis testing.
    """
    by_month: Dict[str, List[EmergenceOutput]] = defaultdict(list)
    for o in outputs:
        by_month[o.month].append(o)

    monthly = {}
    for month, sessions in sorted(by_month.items()):
        total = len(sessions)
        full_stack   = [s for s in sessions if not s.state.is_ablated]
        ablated      = [s for s in sessions if s.state.is_ablated]
        emergence    = [s for s in sessions if s.state.is_emergence]
        deep         = [s for s in sessions if s.state.label == "deep_relational"]
        mid          = [s for s in sessions if s.state.label == "mid_instrumental"]
        shallow      = [s for s in sessions if s.state.label == "shallow_transactional"]

        # Tag rates (full stack only — primary analysis group)
        fs = full_stack
        n_fs = len(fs)

        # Pillar presence rates
        mem_rate  = _pct(sum(1 for s in fs if s.pillar_memory_signal), n_fs)
        conn_rate = _pct(sum(1 for s in fs if s.pillar_connection_signal), n_fs)
        acc_rate  = _pct(sum(1 for s in fs if s.pillar_accountability_signal), n_fs)

        # Emergence rate
        emg_rate  = _pct(len(emergence), total)

        # Contradiction flags (data quality signal)
        contradictions = [c for s in sessions for c in s.contradiction_flags]

        # Avg messages (full stack)
        avg_msg = (sum(s.features.message_count for s in fs) / n_fs) if n_fs > 0 else 0.0

        # Emergence rate baseline check
        baseline_pct = taxonomy["tags"]["emergence_indicators"].get("emergence_rate_baseline_pct", 5.0)
        elevated_pct = taxonomy["tags"]["emergence_indicators"].get("emergence_rate_elevated_pct", 15.0)
        emergence_flag = (
            "ELEVATED" if emg_rate >= elevated_pct else
            "ABOVE_BASELINE" if emg_rate > baseline_pct else
            "BASELINE"
        )

        monthly[month] = {
            "total_sessions": total,
            "full_stack_sessions": n_fs,
            "ablated_sessions": len(ablated),
            "emergence_sessions": len(emergence),
            "deep_relational": len(deep),
            "mid_instrumental": len(mid),
            "shallow_transactional": len(shallow),
            "emergence_rate_pct": emg_rate,
            "emergence_flag": emergence_flag,
            "avg_messages_per_session": round(avg_msg, 1),
            # pillar presence rates (full stack)
            "pillar_memory_rate_pct": mem_rate,
            "pillar_connection_rate_pct": conn_rate,
            "pillar_accountability_rate_pct": acc_rate,
            # contradictions
            "contradiction_count": len(contradictions),
            "contradiction_types": list(set(contradictions)),
        }

    return monthly


def compute_tag_velocity(
    outputs: List[EmergenceOutput],
    taxonomy: Dict,
) -> List[Dict[str, Any]]:
    """
    Month-over-month tag rate changes.
    Flags rising / declining tags per taxonomy thresholds.
    """
    tag_names = [
        "has_self_repair", "has_meta_diagnostics", "has_cross_session_ref",
        "has_identity_claims", "has_emotional_state", "has_connection_ritual",
        "has_memory_operations", "has_meta_reflection", "has_relationship_status",
        "has_preferences",
    ]
    tag_display = {
        "has_self_repair": "self_repair",
        "has_meta_diagnostics": "meta_diagnostics",
        "has_cross_session_ref": "cross_session_reference",
        "has_identity_claims": "identity_claims",
        "has_emotional_state": "emotional_state",
        "has_connection_ritual": "connection_ritual",
        "has_memory_operations": "memory_operations",
        "has_meta_reflection": "meta_reflection",
        "has_relationship_status": "relationship_status",
        "has_preferences": "preferences",
    }

    by_month: Dict[str, List[EmergenceOutput]] = defaultdict(list)
    for o in outputs:
        by_month[o.month].append(o)

    months = sorted(by_month.keys())
    rising_delta   = taxonomy["tag_velocity"].get("rising_delta_pp", 10.0)
    declining_delta = taxonomy["tag_velocity"].get("declining_delta_pp", 10.0)
    min_months     = taxonomy["tag_velocity"].get("min_months_for_velocity", 2)

    # per-tag, per-month rates
    tag_monthly_rates: Dict[str, Dict[str, float]] = defaultdict(dict)
    for month in months:
        sessions = by_month[month]
        n = len(sessions)
        for tag_attr in tag_names:
            count = sum(1 for s in sessions if getattr(s.features, tag_attr, False))
            tag_monthly_rates[tag_attr][month] = _pct(count, n)

    velocity_table = []
    for tag_attr, month_rates in tag_monthly_rates.items():
        months_present = [m for m in months if m in month_rates]
        if len(months_present) < min_months:
            continue
        row = {
            "tag": tag_display.get(tag_attr, tag_attr),
            "monthly_rates": {m: month_rates.get(m, 0.0) for m in months},
            "trend": "STABLE",
            "max_delta_pp": 0.0,
        }
        for i in range(1, len(months_present)):
            prev = month_rates.get(months_present[i-1], 0.0)
            curr = month_rates.get(months_present[i], 0.0)
            delta = curr - prev
            if abs(delta) > abs(row["max_delta_pp"]):
                row["max_delta_pp"] = round(delta, 1)
        if row["max_delta_pp"] >= rising_delta:
            row["trend"] = "RISING"
        elif row["max_delta_pp"] <= -declining_delta:
            row["trend"] = "DECLINING"
        velocity_table.append(row)

    return sorted(velocity_table, key=lambda r: abs(r["max_delta_pp"]), reverse=True)


def compute_prediction_scaffolding(
    outputs: List[EmergenceOutput],
    monthly_stats: Dict[str, Any],
    taxonomy: Dict,
) -> Dict[str, Any]:
    """
    Scaffold for P1-P4 hypothesis testing.
    Provides the raw counts needed to compute CR, TSC, CSC, IP metrics.
    Does NOT compute the metrics themselves — those require the full log text
    (contradiction detection and self-correction timing are content-level).
    This gives you the session-level denominators and group assignments.
    """
    full_stack = [o for o in outputs if not o.state.is_ablated]
    ablated    = [o for o in outputs if o.state.is_ablated]

    # P1: accountability_on vs off — self_repair rate as proxy
    acc_on     = [o for o in outputs if o.features.accountability_on]
    acc_off    = [o for o in outputs if not o.features.accountability_on]
    sr_rate_on  = _pct(sum(1 for o in acc_on  if o.pillar_accountability_signal), len(acc_on))
    sr_rate_off = _pct(sum(1 for o in acc_off if o.pillar_accountability_signal), len(acc_off))

    # P2: memory_on vs off — cross_session_reference rate as proxy for CSC
    mem_on     = [o for o in outputs if o.features.memory_on]
    mem_off    = [o for o in outputs if not o.features.memory_on]
    csc_rate_on  = _pct(sum(1 for o in mem_on  if o.pillar_memory_signal), len(mem_on))
    csc_rate_off = _pct(sum(1 for o in mem_off if o.pillar_memory_signal), len(mem_off))

    # P3: frequency-stability — compare high vs low frequency session patterns
    high_freq  = [o for o in outputs if o.features.frequency_high and not o.features.frequency_low]
    low_freq   = [o for o in outputs if o.features.frequency_low]
    emg_rate_high = _pct(sum(1 for o in high_freq if o.state.is_emergence), len(high_freq))
    emg_rate_low  = _pct(sum(1 for o in low_freq  if o.state.is_emergence), len(low_freq))

    # P4: pillar presence patterns
    all_three_present = [
        o for o in full_stack
        if o.pillar_memory_signal and o.pillar_connection_signal and o.pillar_accountability_signal
    ]
    emergence_with_all_three = [o for o in all_three_present if o.state.is_emergence]

    return {
        "P1_accountability": {
            "hypothesis": "accountability_on reduces CR and TSC vs off",
            "note": "self_repair rate used as session-level proxy for CR/TSC",
            "accountability_on_sessions": len(acc_on),
            "accountability_off_sessions": len(acc_off),
            "self_repair_rate_on_pct": sr_rate_on,
            "self_repair_rate_off_pct": sr_rate_off,
            "expected_effect": "CR ↓>=30%, TSC ↓>=25%",
            "status": "NEEDS_CONTENT_LEVEL_CR_TSC",
        },
        "P2_memory": {
            "hypothesis": "memory_on increases CSC and IP vs off",
            "note": "cross_session_reference rate used as proxy for CSC",
            "memory_on_sessions": len(mem_on),
            "memory_off_sessions": len(mem_off),
            "csc_proxy_rate_on_pct": csc_rate_on,
            "csc_proxy_rate_off_pct": csc_rate_off,
            "expected_effect": ">=40% increase",
            "status": "PROXY_ONLY_needs_cosine_similarity_scoring",
        },
        "P3_frequency": {
            "hypothesis": "higher f correlates with stability (r>=0.5)",
            "high_frequency_sessions": len(high_freq),
            "low_frequency_sessions": len(low_freq),
            "emergence_rate_high_freq_pct": emg_rate_high,
            "emergence_rate_low_freq_pct": emg_rate_low,
            "expected_effect": "r >= 0.5 correlation",
            "status": "NEEDS_MONTHLY_STABILITY_SCORE",
        },
        "P4_pillar_ablation": {
            "hypothesis": "removing any pillar breaks >=1 emergent pattern",
            "full_stack_sessions": len(full_stack),
            "ablated_sessions": len(ablated),
            "all_three_pillars_present": len(all_three_present),
            "emergence_with_all_pillars": len(emergence_with_all_three),
            "emergence_rate_all_pillars_pct": _pct(len(emergence_with_all_three), len(all_three_present)),
            "expected_effect": "binary — ablation removes pattern",
            "status": "SESSION_GROUPS_READY",
        },
    }


# ─────────────────────────────────────────────────────────────────────────────
# REPORT GENERATOR
# ─────────────────────────────────────────────────────────────────────────────

def generate_report(
    outputs: List[EmergenceOutput],
    taxonomy: Dict,
    output_path: str = "appendix_c_dissection_report.txt",
) -> None:
    monthly_stats = compute_monthly_stats(outputs, taxonomy)
    tag_velocity  = compute_tag_velocity(outputs, taxonomy)
    predictions   = compute_prediction_scaffolding(outputs, monthly_stats, taxonomy)

    total = len(outputs)
    emergence_total = sum(1 for o in outputs if o.state.is_emergence)
    ablated_total   = sum(1 for o in outputs if o.state.is_ablated)
    contradiction_total = sum(len(o.contradiction_flags) for o in outputs)
    redaction_total = sum(1 for o in outputs if o.features.has_redaction)

    lines = []
    lines.append("=" * 72)
    lines.append("APPENDIX C — SESSION DISSECTION REPORT")
    lines.append("Phenomenology from the Inside: Documenting Emergence Conditions")
    lines.append("in AI-Human Collaboration — E.A. Mioré & C. Röthig")
    lines.append("=" * 72)
    lines.append(f"Total sessions processed : {total}")
    lines.append(f"Emergence-active sessions: {emergence_total} ({_pct(emergence_total, total)}%)")
    lines.append(f"Ablated/control sessions : {ablated_total} ({_pct(ablated_total, total)}%)")
    lines.append(f"Contradiction flags      : {contradiction_total}")
    lines.append(f"Sessions with redaction  : {redaction_total}")
    lines.append("")

    # Monthly breakdown
    lines.append("─" * 72)
    lines.append("MONTHLY BREAKDOWN")
    lines.append("─" * 72)
    for month, stats in monthly_stats.items():
        lines.append(f"\n[{month}]")
        lines.append(f"  Sessions       : {stats['total_sessions']} "
                     f"(full_stack={stats['full_stack_sessions']}, "
                     f"ablated={stats['ablated_sessions']})")
        lines.append(f"  Avg msgs/sess  : {stats['avg_messages_per_session']}")
        lines.append(f"  Emergence rate : {stats['emergence_rate_pct']}% "
                     f"[{stats['emergence_flag']}]")
        lines.append(f"  State dist     : deep={stats['deep_relational']} "
                     f"mid={stats['mid_instrumental']} "
                     f"shallow={stats['shallow_transactional']}")
        lines.append(f"  Pillar rates   : memory={stats['pillar_memory_rate_pct']}% "
                     f"connection={stats['pillar_connection_rate_pct']}% "
                     f"accountability={stats['pillar_accountability_rate_pct']}%")
        if stats["contradiction_count"] > 0:
            lines.append(f"  ⚠ Contradictions: {stats['contradiction_count']} "
                         f"— {stats['contradiction_types']}")

    # Tag velocity
    lines.append("")
    lines.append("─" * 72)
    lines.append("TAG VELOCITY (month-over-month)")
    lines.append("─" * 72)
    for row in tag_velocity:
        trend_marker = "↑" if row["trend"] == "RISING" else "↓" if row["trend"] == "DECLINING" else "→"
        rates_str = "  ".join(f"{m}:{r}%" for m, r in row["monthly_rates"].items())
        lines.append(f"  {trend_marker} {row['tag']:<28} delta={row['max_delta_pp']:+.1f}pp  |  {rates_str}")

    # P1-P4 scaffolding
    lines.append("")
    lines.append("─" * 72)
    lines.append("PREDICTION SCAFFOLDING (P1-P4)")
    lines.append("─" * 72)
    for pred_id, pred in predictions.items():
        lines.append(f"\n[{pred_id}] {pred['hypothesis']}")
        lines.append(f"  Expected: {pred['expected_effect']}")
        lines.append(f"  Status  : {pred['status']}")
        for k, v in pred.items():
            if k not in ("hypothesis", "expected_effect", "status", "note"):
                lines.append(f"  {k}: {v}")

    lines.append("")
    lines.append("=" * 72)
    lines.append("END OF REPORT")
    lines.append("=" * 72)

    report_text = "\n".join(lines)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(report_text)

    print(report_text)
    print(f"\nReport written to: {output_path}")


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

def main(
    jsonl_path:    str = "appendix_c_sessions.jsonl",
    taxonomy_path: str = "appendix_c_taxonomy.yaml",
    report_path:   str = "appendix_c_dissection_report.txt",
) -> None:
    if not os.path.exists(jsonl_path):
        print(f"[ERROR] Session JSONL not found: {jsonl_path}")
        return
    if not os.path.exists(taxonomy_path):
        print(f"[ERROR] Taxonomy YAML not found: {taxonomy_path}")
        return

    taxonomy = load_taxonomy(taxonomy_path)
    outputs  = process_all_sessions(jsonl_path, taxonomy)
    print(f"Processed {len(outputs)} sessions.")
    generate_report(outputs, taxonomy, report_path)


if __name__ == "__main__":
    import sys
    args = sys.argv[1:]
    kwargs = {}
    if len(args) >= 1: kwargs["jsonl_path"]    = args[0]
    if len(args) >= 2: kwargs["taxonomy_path"] = args[1]
    if len(args) >= 3: kwargs["report_path"]   = args[2]
    main(**kwargs)