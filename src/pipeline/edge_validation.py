"""Pre-registered, append-only forward validation for Launch selection quality.

The validator deliberately answers a narrow question: did the frozen discovery rule
select a better *paper* distribution than its contemporaneous WATCH control?  Every
eligible row must carry a decision-time price observation, a complete pre-registered
paper-cost ceiling, and an append-only exact-pool 24-hour price observation.  Mutable
outcome JSON is checked only as a redundant materialization; it is never the source of
truth.

A statistical pass is still not real-fill evidence, an execution edge, or permission
to trade automatically.  Those denials are emitted on every result path.
"""
from __future__ import annotations

import math
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

import numpy as np

from src.contract.launch_protocol import (
    COHORT_VERSION, LAUNCH_COST_METHOD, PROTOCOL_ID, PROTOCOL_START_AT,
    SOLANA_NETWORK_FEE_CEILING_USD,
)
from src.contract.launch_selector import MAX_SOURCE_TO_DECISION_SECONDS


PRIMARY_HORIZON = "24h"
PRIMARY_METRIC = "continuous_utc_calendar_daily_mean_cost_adjusted_log_growth_utility"
LOOK_SIZES = (100, 200, 400, 800, 1_600, 3_200)
FAMILY_ALPHA = 0.05
LOOK_ALPHA = FAMILY_ALPHA / len(LOOK_SIZES)
# Outcome attrition is outcome-dependent in this market.  A missing rug/delisting
# cannot be silently discarded, so a positive verdict requires the entire prefix.
MIN_OUTCOME_COVERAGE = 1.0
MAX_COVERAGE_GAP = 0.05
MIN_SHARED_DAYS = 14
MIN_SHARED_EVENT_FRACTION = 0.80
MIN_MEAN_UTILITY_LIFT = 0.02
SPA_REPS = 10_000
SPA_BLOCK_SIZE_DAYS = 3
SPA_SEED = 20_260_720
MAX_LEDGER_COMMIT_DELAY_SECONDS = 300
ENTRY_PROVIDER = "dexscreener_token_pairs_v1"
OUTCOME_PROVIDER = "geckoterminal_public_v2"
ARMS = ("SMALL_PROBE", "WATCH")

SAMPLE_KIND = "forward_paper_selector"
SELECTION_STAGE = "discovery_rule_before_security_and_route"
COST_EVIDENCE_POLICY = (
    "frozen_complete_paper_route_model_plus_preregistered_"
    "solana_2usd_network_ceiling_not_real_fill_v1"
)
PRICE_EVIDENCE_POLICY = (
    "append_only_geckoterminal_public_v2_exact_frozen_pool_"
    "closed_24h_candle_no_lookahead_v1"
)
OUTCOME_TRUTH_POLICY = (
    "recompute_from_append_only_price_and_frozen_entry_cost;"
    "mutable_outcome_is_redundant_only"
)
SOURCE_MEMBERSHIP_POLICY = (
    "exact_live_and_independent_finalized_append_only_observation_recheck_v1"
)


def _aware(value: Any) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError, OverflowError):
        return None
    if parsed.tzinfo is None:
        return None
    try:
        return parsed.astimezone(timezone.utc)
    except (ValueError, OverflowError):
        return None


def _finite(value: Any, *, positive: bool = False) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(number) or (positive and number <= 0):
        return None
    return number


def _same_number(left: Any, right: Any, *, tolerance: float = 1e-6) -> bool:
    left_number, right_number = _finite(left), _finite(right)
    return bool(
        left_number is not None
        and right_number is not None
        and math.isclose(left_number, right_number, rel_tol=0.0, abs_tol=tolerance)
    )


def _candidate_source_proof(row: dict, source_snapshot: dict) -> dict:
    """Re-read immutable source tables; a self-consistent JSON blob is insufficient."""
    from src.pipeline import solana_launch_reconcile as reconcile

    primary = row.get("primary_evidence")
    creator = primary.get("creator") if isinstance(primary, dict) else None
    return reconcile.candidate_reconciliation_proof(
        source_snapshot["signature"], slot=source_snapshot["slot"],
        mint=source_snapshot["mint"], creator=creator,
    )


def _protocol_admission_state() -> dict | None:
    from src.pipeline import launch_protocol_gate

    return launch_protocol_gate.read(protocol_id=PROTOCOL_ID)


def _protocol_snapshot(row: dict) -> tuple[dict | None, dict | None, list[str]]:
    """Validate the frozen entry/cost snapshot and return stable reason codes."""
    reasons: list[str] = []
    if not isinstance(row, dict):
        return None, None, ["row_not_mapping"]
    if not isinstance(row.get("id"), str) or not row.get("id", "").strip():
        reasons.append("event_id_invalid")
    if row.get("lane") != "launch":
        reasons.append("lane_not_launch")
    if row.get("chain") != "solana":
        reasons.append("chain_not_solana")
    if row.get("source") != "Pump.fun standard logs + DEX Screener pool":
        reasons.append("source_universe_mismatch")
    if row.get("decision") not in ARMS:
        reasons.append("decision_not_preregistered_arm")
    if row.get("cohort_version") != COHORT_VERSION:
        reasons.append("cohort_version_mismatch")
    if row.get("state") in {"invalidated", "reorg_removed"}:
        reasons.append("source_event_invalidated")

    detected = _aware(row.get("detected_at"))
    decision = _aware(row.get("decision_at"))
    boundary = datetime.fromisoformat(PROTOCOL_START_AT)
    if detected is None:
        reasons.append("detected_clock_invalid")
    elif detected < boundary:
        reasons.append("detected_before_protocol_start")
    if decision is None:
        reasons.append("decision_clock_invalid")
    if detected is not None and decision is not None and detected > decision:
        reasons.append("detected_after_decision")
    elif detected is not None and decision is not None \
            and (decision - detected).total_seconds() > MAX_SOURCE_TO_DECISION_SECONDS:
        reasons.append("source_to_decision_delay_exceeded")

    ledger_created = _aware(row.get("created_at"))
    if ledger_created is None:
        reasons.append("ledger_created_clock_invalid")
    elif decision is not None:
        commit_delay = (ledger_created - decision).total_seconds()
        if commit_delay < 0:
            reasons.append("ledger_created_before_decision")
        elif commit_delay > MAX_LEDGER_COMMIT_DELAY_SECONDS:
            reasons.append("ledger_commit_delay_exceeded")

    entry = None
    try:
        from src.pipeline.opportunity_ledger import validate_entry_observation

        entry = validate_entry_observation(row)
        if not entry:
            raise ValueError("missing entry observation")
    except (TypeError, ValueError, KeyError, OverflowError):
        reasons.append("entry_observation_invalid")
    if row.get("entry_observation_version") != 1:
        reasons.append("entry_observation_version_mismatch")
    if entry:
        observed = _aware(entry.get("observed_at"))
        if entry.get("provider") != ENTRY_PROVIDER:
            reasons.append("entry_provider_mismatch")
        if observed is None:
            reasons.append("entry_clock_invalid")
        elif decision is None or decision != observed:
            reasons.append("decision_entry_clock_mismatch")
        if observed is not None and observed < boundary:
            reasons.append("entry_before_protocol_start")
        if decision is not None and decision < boundary:
            reasons.append("decision_before_protocol_start")

    selector = None
    source_snapshot = None
    if entry:
        try:
            from src.contract.launch_selector import validate_source_snapshot

            source_snapshot = validate_source_snapshot(
                entry.get("source_snapshot"), token=row.get("token"),
                detected_at=row.get("detected_at"),
                decision_at=row.get("decision_at"),
            )
        except (TypeError, ValueError, KeyError, OverflowError):
            reasons.append("source_snapshot_invalid")
        if source_snapshot is not None:
            try:
                current_source_proof = _candidate_source_proof(row, source_snapshot)
            except Exception:
                reasons.append("source_proof_unverifiable")
            else:
                if current_source_proof != source_snapshot.get("reconciliation_proof"):
                    reasons.append("source_proof_mismatch")
        try:
            from src.contract.launch_selector import evaluate_selector_snapshot

            selector = evaluate_selector_snapshot(
                entry.get("selector_snapshot"),
                event_at=row.get("event_at"),
                decision_at=entry.get("observed_at"),
            )
        except (TypeError, ValueError, KeyError, OverflowError):
            reasons.append("selector_snapshot_invalid")
    if selector and selector.get("decision") != row.get("decision"):
        reasons.append("selector_decision_mismatch")

    contract = None
    if row.get("cost_contract_version") != 1:
        reasons.append("cost_contract_version_mismatch")
    try:
        from src.pipeline.execution_cost import validate

        contract = validate(row.get("cost_contract"))
    except (TypeError, ValueError, KeyError, OverflowError):
        reasons.append("cost_contract_invalid")
    if contract:
        if contract.get("purpose") != "discovery_outcome":
            reasons.append("cost_purpose_mismatch")
        if contract.get("method") != LAUNCH_COST_METHOD:
            reasons.append("cost_method_mismatch")
        if contract.get("measurement_kind") != "paper_model":
            reasons.append("cost_measurement_kind_mismatch")
        if contract.get("cost_policy") != "preregistered_full_paper_ceiling":
            reasons.append("cost_policy_mismatch")
        if contract.get("completeness") != "complete":
            reasons.append("cost_incomplete")
        if contract.get("is_real_fill") is not False:
            reasons.append("cost_claims_real_fill")
        all_in = _finite(contract.get("all_in_total_pct"))
        if all_in is None or all_in < 0:
            reasons.append("all_in_cost_missing")
        if not _same_number(
                contract.get("network_fee_ceiling_usd"),
                SOLANA_NETWORK_FEE_CEILING_USD,
                tolerance=1e-9):
            reasons.append("network_fee_ceiling_mismatch")
        cap = _finite(row.get("max_notional_usd"), positive=True)
        if cap is None:
            reasons.append("max_notional_invalid")
        elif not _same_number(contract.get("notional_usd"), cap):
            reasons.append("cost_notional_cap_mismatch")
        if selector and cap is not None and not _same_number(
                cap, selector.get("max_notional_usd"), tolerance=1e-9):
            reasons.append("position_cap_rule_mismatch")
        route_component = next((
            component for component in contract.get("components", [])
            if isinstance(component, dict)
            and component.get("name") == "modeled_route_dex_and_impact"
        ), None)
        if (selector and (not isinstance(route_component, dict)
                or not _same_number(
                    route_component.get("pct"),
                    selector.get("modeled_route_roundtrip_pct"),
                    tolerance=1e-9,
                ))):
            reasons.append("modeled_route_rule_mismatch")
        if all_in is not None and not _same_number(row.get("cost_pct_est"), all_in):
            reasons.append("row_cost_pct_mismatch")
        if row.get("cost_model") != LAUNCH_COST_METHOD:
            reasons.append("row_cost_model_mismatch")

    # Preserve first-seen ordering while avoiding double counts from dependent gates.
    return entry, contract, list(dict.fromkeys(reasons))


def protocol_exclusion_reasons(row: dict) -> list[str]:
    """Explain why a row is outside the immutable v6 forward cohort."""
    return _protocol_snapshot(row)[2]


def is_protocol_enrollment_candidate(row: dict) -> bool:
    """Identify rows that were meant to enroll after the v6 detection boundary.

    EVM rows and pre-boundary quarantine rows stay descriptive.  A malformed clock
    on a Solana v6 row cannot hide it if another frozen enrollment clock proves it
    belongs after the boundary.
    """
    if (not isinstance(row, dict) or row.get("lane") != "launch"
            or row.get("chain") != "solana"
            or row.get("cohort_version") != COHORT_VERSION):
        return False
    boundary = datetime.fromisoformat(PROTOCOL_START_AT)
    detected = _aware(row.get("detected_at"))
    if detected is not None:
        return detected >= boundary
    entry = row.get("entry_observation")
    fallback = [
        row.get("decision_at"),
        entry.get("observed_at") if isinstance(entry, dict) else None,
    ]
    return any((clock := _aware(value)) is not None and clock >= boundary
               for value in fallback)


def protocol_snapshot(row: dict) -> dict | None:
    """Return the normalized v6 evidence snapshot, or ``None`` when ineligible.

    Outcome settlement and descriptive summaries may reuse this function instead of
    reimplementing the protocol's cost semantics.  The returned anchor is canonical
    UTC and ``all_in_cost_pct`` is the validated complete paper ceiling; neither is a
    real fill.
    """
    entry, contract, reasons = _protocol_snapshot(row)
    if reasons or entry is None or contract is None:
        return None
    return {
        "anchor_at": entry["observed_at"],
        "entry_observation": entry,
        "cost_contract": contract,
        "selector_snapshot": entry["selector_snapshot"],
        "source_snapshot": entry["source_snapshot"],
        "all_in_cost_pct": float(contract["all_in_total_pct"]),
        "ledger_created_at": row["created_at"],
        "cost_is_real_fill": False,
    }


def is_protocol_event(row: dict) -> bool:
    """Return whether a row satisfies the complete frozen v6 snapshot."""
    return not protocol_exclusion_reasons(row)


def _point_state_with_reason(
        row: dict, snapshot: tuple[dict, dict] | None = None,
) -> tuple[str, float | None, str | None]:
    """Recompute one 24h point from append-only evidence, failing closed."""
    if snapshot is None:
        entry, contract, reasons = _protocol_snapshot(row)
        if reasons or entry is None or contract is None:
            return "invalid", None, "protocol_snapshot_invalid"
    else:
        entry, contract = snapshot

    raw_outcome = row.get("outcome")
    if raw_outcome is not None and not isinstance(raw_outcome, dict):
        return "invalid", None, "mutable_outcome_malformed"
    outcome = raw_outcome or {}
    horizons = outcome.get("horizons")
    if horizons is not None and not isinstance(horizons, dict):
        return "invalid", None, "mutable_horizons_malformed"
    horizons = horizons or {}
    unavailable = outcome.get("unavailable_horizons")
    if unavailable is not None and not isinstance(unavailable, list):
        return "invalid", None, "mutable_unavailable_horizons_malformed"
    if isinstance(unavailable, list) and any(
            not isinstance(item, str) for item in unavailable):
        return "invalid", None, "mutable_unavailable_horizons_malformed"
    point_exists = PRIMARY_HORIZON in horizons
    point = horizons.get(PRIMARY_HORIZON)

    raw_observations = row.get("price_observations")
    if raw_observations is not None and not isinstance(raw_observations, dict):
        return "invalid", None, "price_observations_malformed"
    stored = (raw_observations or {}).get(PRIMARY_HORIZON)
    if stored is None:
        if point_exists:
            return "invalid", None, "outcome_without_append_only_price"
        if PRIMARY_HORIZON in set(unavailable or []):
            return "unavailable", None, None
        return "pending", None, None

    try:
        from src.pipeline.opportunity_ledger import validate_stored_price_observation

        proven = validate_stored_price_observation(row, PRIMARY_HORIZON, stored)
    except (TypeError, ValueError, KeyError, OverflowError):
        return "invalid", None, "stored_price_observation_invalid"

    # The ledger validator proves the frozen bindings.  v6 additionally freezes the
    # source and exact Solana-side semantics so another valid provider cannot be
    # substituted after returns are known.
    exact_identity = (
        proven.get("provider") == OUTCOME_PROVIDER
        and proven.get("network") == "solana"
        and proven.get("chain") == "solana"
        and proven.get("token") == entry.get("base_token")
        and proven.get("pool") == entry.get("pair")
        and proven.get("pair") == entry.get("pair")
        # GeckoTerminal may orient the same frozen token as either side of its pool.
        # The append validator already binds the exact token and pool identities.
        and proven.get("token_side") in {"base", "quote"}
        and proven.get("identity_verified") is True
    )
    if proven.get("provider") != OUTCOME_PROVIDER:
        return "invalid", None, "outcome_provider_mismatch"
    if not exact_identity:
        return "invalid", None, "outcome_identity_mismatch"

    entry_price = _finite(entry.get("price"), positive=True)
    observed_price = _finite(proven.get("price"), positive=True)
    cost_pct = _finite(contract.get("all_in_total_pct"))
    if entry_price is None or observed_price is None or cost_pct is None or cost_pct < 0:
        return "invalid", None, "recomputed_point_inputs_invalid"
    gross_unrounded = (observed_price / entry_price - 1.0) * 100.0
    net_unrounded = gross_unrounded - cost_pct
    if not math.isfinite(gross_unrounded) or not math.isfinite(net_unrounded):
        return "invalid", None, "recomputed_point_non_finite"
    gross = round(gross_unrounded, 4)
    net = round(net_unrounded, 4)
    positive = net_unrounded > 0

    # A crash between the append and mutable outcome write is recoverable: append-only
    # evidence is the truth and already suffices to resolve the point.
    if not point_exists:
        return "resolved", net, None
    if not isinstance(point, dict):
        return "invalid", None, "mutable_outcome_point_malformed"

    expected = {
        "price_observation_id": proven.get("observation_id"),
        "price": observed_price,
        "gross_return_pct": gross,
        "net_return_pct_est": net,
        "target_at": proven.get("target_at"),
        "outcome_anchor_at": entry.get("observed_at"),
        "positive_after_cost": positive,
    }
    for field, expected_value in expected.items():
        actual = point.get(field)
        if field in {"price", "gross_return_pct", "net_return_pct_est"}:
            matches = _same_number(actual, expected_value, tolerance=1e-9)
        elif field == "positive_after_cost":
            matches = isinstance(actual, bool) and actual is expected_value
        else:
            matches = actual == expected_value
        if not matches:
            return "invalid", None, f"mutable_outcome_{field}_mismatch"
    return "resolved", net, None


def protocol_point_state(row: dict) -> tuple[str, float | None]:
    """Public strict point state used by every v6 descriptive/statistical view."""
    state, value, _reason = _point_state_with_reason(row)
    return state, value


def _point_state(row: dict) -> tuple[str, float | None]:
    """Backward-compatible alias for callers that used the private helper."""
    return protocol_point_state(row)


def _utility(net_return_pct: float) -> float:
    # A 1% residual-value floor keeps extreme losses finite while log utility stops
    # one moonshot from manufacturing a selector pass.
    if not math.isfinite(net_return_pct):
        raise ValueError("non-finite return cannot enter utility")
    value = math.log1p(max(-99.0, net_return_pct) / 100.0)
    if not math.isfinite(value):
        raise ValueError("non-finite utility")
    return value


def _arm_summary(
        rows: list[dict], states: dict[int, tuple[str, float | None, str | None]],
) -> dict:
    counts = defaultdict(int)
    invalid_by_reason = defaultdict(int)
    values = []
    for row in rows:
        state, value, reason = states[id(row)]
        counts[state] += 1
        if state == "invalid" and reason:
            invalid_by_reason[reason] += 1
        if state == "resolved" and value is not None:
            values.append(value)
    n = len(rows)
    mean = math.fsum(value / len(values) for value in values) if values else None
    ordered = sorted(values)
    if not ordered:
        median = None
    elif len(ordered) % 2:
        median = ordered[len(ordered) // 2]
    else:
        middle = len(ordered) // 2
        median = ordered[middle - 1] / 2.0 + ordered[middle] / 2.0
    if mean is not None and not math.isfinite(mean):
        mean = None
    if median is not None and not math.isfinite(median):
        median = None
    return {
        "eligible_n": n,
        "resolved_n": counts["resolved"],
        "pending_n": counts["pending"],
        "unavailable_n": counts["unavailable"],
        "invalid_n": counts["invalid"],
        "invalid_by_reason": dict(sorted(invalid_by_reason.items())),
        "coverage": counts["resolved"] / n if n else 0.0,
        "positive_rate": (
            sum(value > 0 for value in values) / len(values) if values else None
        ),
        "mean_net_24h": mean,
        "median_net_24h": median,
    }


def _entry_anchor(row: dict) -> datetime:
    observed = _aware((row.get("entry_observation") or {}).get("observed_at"))
    if observed is None:
        raise ValueError("protocol row has no valid entry anchor")
    return observed


def _daily_utility(
        rows: list[dict], states: dict[int, tuple[str, float | None, str | None]],
) -> tuple[dict[str, float], int]:
    by_day: dict[str, list[float]] = defaultdict(list)
    resolved = 0
    for row in rows:
        state, value, _reason = states[id(row)]
        if state != "resolved" or value is None:
            continue
        resolved += 1
        by_day[_entry_anchor(row).date().isoformat()].append(_utility(value))
    return ({day: math.fsum(value / len(values) for value in values)
             for day, values in by_day.items()}, resolved)


def _base_result(
        eligible: dict[str, list[dict]], excluded_n: int,
        excluded_by_reason: dict[str, int], *, integrity_invalid_n: int = 0,
        integrity_invalid_by_reason: dict[str, int] | None = None,
) -> dict:
    counts = {arm: len(rows) for arm, rows in eligible.items()}
    next_look = next(
        (size for size in LOOK_SIZES if min(counts.values(), default=0) < size), None
    )
    return {
        "protocol_id": PROTOCOL_ID,
        "protocol_start_at": PROTOCOL_START_AT,
        "cohort_version": COHORT_VERSION,
        "primary_horizon": PRIMARY_HORIZON,
        "primary_metric": PRIMARY_METRIC,
        "required_outcome_coverage": MIN_OUTCOME_COVERAGE,
        "planned_looks": list(LOOK_SIZES),
        "family_alpha": FAMILY_ALPHA,
        "look_alpha": round(LOOK_ALPHA, 8),
        "spa_reps": SPA_REPS,
        "spa_block_size_days": SPA_BLOCK_SIZE_DAYS,
        "spa_seed": SPA_SEED,
        "max_ledger_commit_delay_seconds": MAX_LEDGER_COMMIT_DELAY_SECONDS,
        "max_source_to_decision_seconds": MAX_SOURCE_TO_DECISION_SECONDS,
        "sample_kind": SAMPLE_KIND,
        "selection_stage": SELECTION_STAGE,
        "cost_evidence_policy": COST_EVIDENCE_POLICY,
        "price_evidence_policy": PRICE_EVIDENCE_POLICY,
        "outcome_truth_policy": OUTCOME_TRUTH_POLICY,
        "source_membership_policy": SOURCE_MEMBERSHIP_POLICY,
        "cost_is_real_fill": False,
        "real_edge_n": 0,
        "real_edge_eligible": False,
        "execution_edge_eligible": False,
        "auto_execution_allowed": False,
        "eligible_n": counts,
        "excluded_n": excluded_n,
        "excluded_by_reason": dict(sorted(excluded_by_reason.items())),
        "integrity_invalid_n": integrity_invalid_n,
        "integrity_invalid_by_reason": dict(sorted(
            (integrity_invalid_by_reason or {}).items()
        )),
        "next_look_n_per_arm": next_look,
        "edge_verdict": "不可判",
    }


def launch_forward_validation(rows: list[dict]) -> dict:
    """Run the current pre-registered look, or explain why it cannot run."""
    eligible = {arm: [] for arm in ARMS}
    snapshots: dict[int, tuple[dict, dict]] = {}
    excluded_by_reason: dict[str, int] = defaultdict(int)
    integrity_invalid_by_reason: dict[str, int] = defaultdict(int)
    excluded_n = 0
    integrity_invalid_n = 0
    seen_event_ids: set[str] = set()
    for row in rows:
        entry, contract, reasons = _protocol_snapshot(row)
        if reasons or entry is None or contract is None:
            excluded_n += 1
            for reason in reasons or ["protocol_snapshot_invalid"]:
                excluded_by_reason[reason] += 1
                if is_protocol_enrollment_candidate(row):
                    integrity_invalid_by_reason[reason] += 1
            if is_protocol_enrollment_candidate(row):
                integrity_invalid_n += 1
            continue
        event_id = row["id"].strip()
        if event_id in seen_event_ids:
            excluded_n += 1
            integrity_invalid_n += 1
            excluded_by_reason["duplicate_event_id"] += 1
            integrity_invalid_by_reason["duplicate_event_id"] += 1
            continue
        seen_event_ids.add(event_id)
        eligible[row["decision"]].append(row)
        snapshots[id(row)] = (entry, contract)
    for arm in ARMS:
        eligible[arm].sort(key=lambda row: (_entry_anchor(row), row.get("id") or ""))
    result = _base_result(
        eligible, excluded_n, excluded_by_reason,
        integrity_invalid_n=integrity_invalid_n,
        integrity_invalid_by_reason=integrity_invalid_by_reason,
    )
    try:
        admission = _protocol_admission_state()
    except Exception as exc:
        admission = {
            "state": "unavailable", "enrollment_open": False,
            "reason_codes": ["protocol_admission_unavailable"],
            "error": f"{type(exc).__name__}: {exc}"[:160],
        }
    result["protocol_admission"] = admission
    if (not isinstance(admission, dict) or admission.get("state") != "open"
            or admission.get("enrollment_open") is not True):
        state = admission.get("state") if isinstance(admission, dict) else "missing"
        result.update({
            "state": "protocol_integrity_blocked",
            "reason": f"Launch v3 来源 admission 未开放或已失效: {state}",
        })
        return result
    if integrity_invalid_n:
        result.update({
            "state": "protocol_integrity_blocked",
            "reason": (
                f"发现 {integrity_invalid_n} 个起点后 v6 候选的冻结证据损坏；"
                "已阻断整个协议判定，不能从分母删除坏行"
            ),
        })
        return result
    min_n = min((len(eligible[arm]) for arm in ARMS), default=0)
    look_n = max((size for size in LOOK_SIZES if size <= min_n), default=None)
    if look_n is None:
        result.update({
            "state": "collecting",
            "reason": (
                f"v6 前向协议每组至少需要 {LOOK_SIZES[0]} 个候选（完整冻结证据）；当前 "
                f"SMALL_PROBE {len(eligible['SMALL_PROBE'])}, "
                f"WATCH {len(eligible['WATCH'])}"
            ),
        })
        return result

    prefix = {arm: eligible[arm][:look_n] for arm in ARMS}
    states = {
        id(row): _point_state_with_reason(row, snapshots[id(row)])
        for arm in ARMS for row in prefix[arm]
    }
    summary = {arm: _arm_summary(prefix[arm], states) for arm in ARMS}
    result.update({"look_n_per_arm": look_n, "arms": summary})
    pending = sum(summary[arm]["pending_n"] for arm in ARMS)
    if pending:
        result.update({
            "state": "awaiting_outcomes",
            "reason": f"第 {look_n} 个固定前缀仍有 {pending} 个 24h 结果待结算",
        })
        return result

    probe_coverage = summary["SMALL_PROBE"]["coverage"]
    watch_coverage = summary["WATCH"]["coverage"]
    if (
        min(probe_coverage, watch_coverage) < MIN_OUTCOME_COVERAGE
        or abs(probe_coverage - watch_coverage) > MAX_COVERAGE_GAP
    ):
        result.update({
            "state": "coverage_blocked",
            "reason": (
                "固定前缀主结果必须 100% 有合规追加证据；缺失可能是 rug/退市等"
                "结局依赖缺失；"
                f"SMALL_PROBE {probe_coverage:.1%}, WATCH {watch_coverage:.1%}"
            ),
        })
        return result

    daily: dict[str, dict[str, float]] = {}
    resolved: dict[str, int] = {}
    try:
        for arm in ARMS:
            daily[arm], resolved[arm] = _daily_utility(prefix[arm], states)
    except (TypeError, ValueError, OverflowError) as exc:
        result.update({
            "state": "invalid_evidence",
            "reason": f"主终点效用不是有限数: {type(exc).__name__}",
        })
        return result
    shared_days = sorted(set(daily["SMALL_PROBE"]) & set(daily["WATCH"]))
    active_days = sorted(set(daily["SMALL_PROBE"]) | set(daily["WATCH"]))
    calendar_days = []
    if active_days:
        cursor = datetime.fromisoformat(active_days[0]).date()
        final_day = datetime.fromisoformat(active_days[-1]).date()
        while cursor <= final_day:
            calendar_days.append(cursor.isoformat())
            cursor += timedelta(days=1)
    shared_events = {
        arm: sum(
            1 for row in prefix[arm]
            if states[id(row)][0] == "resolved"
            and _entry_anchor(row).date().isoformat() in shared_days
        )
        for arm in ARMS
    }
    shared_fraction = {
        arm: shared_events[arm] / resolved[arm] if resolved[arm] else 0.0
        for arm in ARMS
    }
    result.update({
        "shared_days": len(shared_days),
        "calendar_days": len(calendar_days),
        "inactive_arm_daily_utility": 0.0,
        "calendar_policy": "continuous_utc_calendar_absent_arm_is_cash",
        "time_anchor_policy": "entry_observation_utc",
        "shared_event_fraction": shared_fraction,
    })
    if (
        len(shared_days) < MIN_SHARED_DAYS
        or min(shared_fraction.values()) < MIN_SHARED_EVENT_FRACTION
    ):
        result.update({
            "state": "regime_overlap_blocked",
            "reason": (
                f"同期 entry-anchor UTC 日覆盖不足: {len(shared_days)}/{MIN_SHARED_DAYS} 天；"
                f"共享日事件占比 SMALL_PROBE {shared_fraction['SMALL_PROBE']:.1%}, "
                f"WATCH {shared_fraction['WATCH']:.1%}"
            ),
        })
        return result

    # On the union calendar, an inactive arm stayed in cash and receives utility 0.
    probe = np.asarray([daily["SMALL_PROBE"].get(day, 0.0) for day in calendar_days])
    watch = np.asarray([daily["WATCH"].get(day, 0.0) for day in calendar_days])
    if not np.all(np.isfinite(probe)) or not np.all(np.isfinite(watch)):
        result.update({
            "state": "invalid_evidence",
            "reason": "union 日历包含非有限效用，已拒绝统计检验",
        })
        return result
    mean_lift = float(np.mean(probe - watch))
    if not math.isfinite(mean_lift):
        result.update({
            "state": "invalid_evidence",
            "reason": "日均 log 效用差不是有限数，已拒绝统计检验",
        })
        return result
    result["mean_daily_log_utility_lift"] = round(mean_lift, 8)

    event_utility: dict[str, float] = {}
    try:
        for arm in ARMS:
            values = [
                _utility(states[id(row)][1])
                for row in prefix[arm]
                if states[id(row)][0] == "resolved" and states[id(row)][1] is not None
            ]
            event_utility[arm] = math.fsum(value / len(values) for value in values)
    except (TypeError, ValueError, OverflowError) as exc:
        result.update({
            "state": "invalid_evidence",
            "reason": f"全事件效用不是有限数: {type(exc).__name__}",
        })
        return result
    if not all(math.isfinite(value) for value in event_utility.values()):
        result.update({
            "state": "invalid_evidence",
            "reason": "全事件平均效用不是有限数，已拒绝统计检验",
        })
        return result
    event_lift = event_utility["SMALL_PROBE"] - event_utility["WATCH"]
    result.update({
        "mean_event_log_utility": {
            arm: round(value, 8) for arm, value in event_utility.items()
        },
        "mean_event_log_utility_lift": round(event_lift, 8),
    })
    if event_utility["SMALL_PROBE"] <= 0 or event_lift <= 0:
        result.update({
            "state": "no_edge_observed",
            "edge_verdict": "无edge/负",
            "reason": (
                "固定前缀全事件 log 效用未同时优于 WATCH 与现金: "
                f"SMALL_PROBE {event_utility['SMALL_PROBE']:.4f}, "
                f"WATCH {event_utility['WATCH']:.4f}"
            ),
        })
        return result

    try:
        from arch.bootstrap import SPA

        spa = SPA(
            -watch,
            (-probe)[:, None],
            block_size=SPA_BLOCK_SIZE_DAYS,
            reps=SPA_REPS,
            bootstrap="stationary",
            studentize=True,
            seed=SPA_SEED,
        )
        spa.compute()
        pvalues = {name: float(value) for name, value in spa.pvalues.items()}
    except Exception as exc:
        result.update({
            "state": "validator_unavailable",
            "reason": f"SPA validator failed closed: {type(exc).__name__}: {str(exc)[:100]}",
        })
        return result
    if ("upper" not in pvalues
            or not all(math.isfinite(value) and 0.0 <= value <= 1.0
                       for value in pvalues.values())):
        result.update({
            "state": "validator_unavailable",
            "reason": "SPA validator returned invalid p-values and failed closed",
        })
        return result

    result.update({
        "spa_pvalues": {name: round(value, 8) for name, value in pvalues.items()},
        "spa_pvalue_used": "upper",
        "tested_model_count": 1,
    })
    p_upper = pvalues["upper"]
    if p_upper <= LOOK_ALPHA and mean_lift >= MIN_MEAN_UTILITY_LIFT:
        result.update({
            "state": "pass",
            "edge_verdict": "有前向纸面selector edge迹象",
            "reason": (
                f"预注册 {look_n}/组 look 通过: SPA upper p={p_upper:.5f} "
                f"≤ {LOOK_ALPHA:.5f}, 日均log效用差 {mean_lift:.4f}；"
                "仍不是实盘或执行优势"
            ),
        })
    elif mean_lift <= 0:
        result.update({
            "state": "no_edge_observed",
            "edge_verdict": "无edge/负",
            "reason": f"固定 look 的日均log效用差 {mean_lift:.4f} 未优于 WATCH",
        })
    else:
        result.update({
            "state": "inconclusive",
            "reason": (
                f"效应为正但未越过预注册门槛: SPA upper p={p_upper:.5f}, "
                f"日均log效用差 {mean_lift:.4f}"
            ),
        })
    return result
