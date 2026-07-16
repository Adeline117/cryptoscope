"""One fail-closed contract shared by the Launch ledger and public board."""
from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any, Mapping

SUPPORTED_LAUNCH_CHAINS = frozenset({"solana", "base", "bsc", "ethereum"})
ACTIONABLE_LAUNCH_CHAINS = frozenset({"solana"})
MIN_PROBE_NOTIONAL_USD = 25.0
MAX_PROBE_NOTIONAL_USD = 500.0
MAX_POOL_LIQUIDITY_FRACTION = 0.003
# A3 remains intentionally unreachable until one append-only public read-back
# verifier binds a specific assessment payload to what users could actually fetch.
DELIVERY_READBACK_VERIFIER_VERSION = None


def _aware(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError, OverflowError):
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(timezone.utc)


def _positive(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    number = float(value)
    return number if math.isfinite(number) and number > 0 else None


def _nonnegative(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    number = float(value)
    return number if math.isfinite(number) and number >= 0 else None


def launch_manual_probe_failures(
        row: Mapping[str, Any], assessment: Any, evidence: Any,
        *, now: datetime) -> list[str]:
    """Return every reason a candidate cannot be published as a live A3 window."""
    from src.pipeline.edge_validation import (
        LAUNCH_COST_METHOD, LOOK_SIZES, PROTOCOL_ID, is_protocol_event,
    )
    from src.pipeline.launch_execution import (
        MAX_ROUNDTRIP_LOSS_PCT, QUOTE_TTL_SECONDS, SECURITY_TTL_SECONDS,
    )

    now = now.astimezone(timezone.utc)
    failures: list[str] = []

    def fail(code: str) -> None:
        if code not in failures:
            failures.append(code)

    if not is_protocol_event(dict(row)):
        fail("outside_frozen_edge_protocol")
    for field in ("id", "chain", "token", "symbol", "source"):
        if not isinstance(row.get(field), str) or not row.get(field, "").strip():
            fail(f"{field}_missing")
    if str(row.get("source") or "").strip().lower() == "unknown":
        fail("source_missing")
    if row.get("chain") not in SUPPORTED_LAUNCH_CHAINS:
        fail("unsupported_launch_chain")
    if row.get("chain") not in ACTIONABLE_LAUNCH_CHAINS:
        fail("launch_chain_has_no_complete_quote_path")
    if row.get("state") != "live" or row.get("outcome_state") != "open":
        fail("launch_event_not_live_open")
    if row.get("is_expired") is not False:
        fail("launch_event_expiry_state_invalid")
    detected_at = _aware(row.get("detected_at"))
    if detected_at is None or detected_at > now:
        fail("discovery_clock_invalid")
    if row.get("decision") != "SMALL_PROBE" or row.get("recorded_decision") != "SMALL_PROBE":
        fail("discovery_cohort_not_probe")
    if row.get("auto_execution_allowed") is not False:
        fail("automatic_execution_not_disabled")
    if str(row.get("side") or "LONG").upper() != "LONG":
        fail("launch_side_not_long")
    if not isinstance(assessment, Mapping):
        fail("assessment_missing")
        return failures
    if assessment.get("kind") != "read_only_quote":
        fail("assessment_not_read_only_quote")
    if not str(assessment.get("assessment_id") or "").strip():
        fail("assessment_id_missing")
    if assessment.get("opportunity_id") != row.get("id"):
        fail("assessment_opportunity_mismatch")
    if (assessment.get("chain") != row.get("chain")
            or assessment.get("token") != row.get("token")):
        fail("assessment_asset_mismatch")
    assessed_at = _aware(assessment.get("assessed_at"))
    if assessed_at is None or assessed_at > now:
        fail("assessment_clock_invalid")
    if assessment.get("auto_execution_allowed") is not False:
        fail("automatic_execution_not_disabled")
    if assessment.get("is_real_fill") is not False:
        fail("assessment_claims_real_fill")
    if assessment.get("security_state") != "pass":
        fail("security_not_pass")
    if assessment.get("route_state") != "quoted":
        fail("route_not_quoted")
    quote_source = str(assessment.get("quote_source") or "").strip()
    if not quote_source or quote_source.lower() == "unknown":
        fail("quote_source_missing")
    elif (row.get("chain") == "solana"
          and (quote_source != "Jupiter Swap v2 order"
               or assessment.get("quote_mode") != "keyed_v2")):
        fail("quote_not_promotable")

    quote_at = _aware(assessment.get("quote_at"))
    quote_expires_at = _aware(assessment.get("quote_expires_at"))
    expires_at = _aware(assessment.get("expires_at"))
    if (quote_at is None or quote_expires_at is None or expires_at is None
            or quote_expires_at != expires_at or quote_at > now or now >= expires_at
            or not 0 < (expires_at - quote_at).total_seconds() <= QUOTE_TTL_SECONDS
            or (now - quote_at).total_seconds() > QUOTE_TTL_SECONDS):
        fail("quote_clock_invalid")
    if (assessed_at is not None and quote_at is not None
            and abs((assessed_at - quote_at).total_seconds()) > QUOTE_TTL_SECONDS):
        fail("assessment_quote_clock_mismatch")

    security_at = _aware(assessment.get("security_at"))
    security_expires_at = _aware(assessment.get("security_expires_at"))
    if (security_at is None or security_expires_at is None
            or security_at > now or now >= security_expires_at
            or not 0 < (security_expires_at - security_at).total_seconds()
            <= SECURITY_TTL_SECONDS
            or (now - security_at).total_seconds() > SECURITY_TTL_SECONDS):
        fail("security_clock_invalid")

    discovery_entry = _positive(row.get("entry_price"))
    discovery_invalidation = _positive(row.get("invalidation_price"))
    public_limit = _positive(row.get("max_notional_usd"))
    liquidity = _positive(row.get("liquidity_usd"))
    entry = _positive(assessment.get("entry_reference_price"))
    invalidation = _positive(assessment.get("invalidation_reference_price"))
    notional = _positive(assessment.get("notional_usd"))
    if discovery_entry is None:
        fail("discovery_entry_invalid")
    if discovery_invalidation is None:
        fail("discovery_invalidation_invalid")
    if public_limit is None:
        fail("position_cap_invalid")
    elif public_limit > MAX_PROBE_NOTIONAL_USD:
        fail("position_cap_above_launch_limit")
    if liquidity is None:
        fail("liquidity_invalid")
    elif public_limit is not None:
        expected_limit = round(min(
            MAX_PROBE_NOTIONAL_USD,
            max(MIN_PROBE_NOTIONAL_USD, liquidity * MAX_POOL_LIQUIDITY_FRACTION),
        ), 2)
        if not math.isclose(public_limit, expected_limit, abs_tol=1e-6):
            fail("position_cap_disagrees_with_frozen_liquidity_rule")
    if entry is None:
        fail("entry_reference_price_invalid")
    if invalidation is None:
        fail("invalidation_reference_price_invalid")
    if notional is None:
        fail("quote_notional_invalid")
    if (discovery_entry is not None and discovery_invalidation is not None
            and discovery_invalidation >= discovery_entry):
        fail("discovery_invalidation_not_below_long_entry")
    if entry is not None and invalidation is not None and invalidation >= entry:
        fail("invalidation_not_below_long_entry")
    if (notional is not None and public_limit is not None
            and not math.isclose(notional, public_limit, abs_tol=1e-9)):
        fail("quote_notional_mismatches_position_cap")

    try:
        from src.pipeline.execution_cost import validate

        discovery_contract = validate(row.get("cost_contract"))
    except (TypeError, ValueError):
        discovery_contract = None
        fail("discovery_cost_contract_invalid")
    if discovery_contract is not None:
        discovery_notional = _positive(discovery_contract.get("notional_usd"))
        frozen_cost = _nonnegative(row.get("cost_pct_est"))
        all_in_cost = _nonnegative(discovery_contract.get("all_in_total_pct"))
        if (public_limit is None or discovery_notional is None
                or not math.isclose(discovery_notional, public_limit, abs_tol=1e-9)):
            fail("discovery_cost_notional_mismatch")
        if (frozen_cost is None or all_in_cost is None
                or not math.isclose(frozen_cost, all_in_cost, abs_tol=1e-9)):
            fail("discovery_cost_total_mismatch")
        if (discovery_contract.get("purpose") != "discovery_outcome"
                or discovery_contract.get("method") != LAUNCH_COST_METHOD
                or discovery_contract.get("measurement_kind") != "paper_model"
                or discovery_contract.get("cost_policy")
                != "preregistered_full_paper_ceiling"
                or discovery_contract.get("completeness") != "complete"
                or discovery_contract.get("is_real_fill") is not False):
            fail("discovery_cost_semantics_invalid")

    contract = assessment.get("cost_contract")
    route_loss = network_fee = None
    try:
        contract = validate(contract)
    except (TypeError, ValueError):
        contract = None
        fail("current_cost_contract_invalid")
    if contract is not None:
        all_in = contract.get("all_in_total_pct")
        if (contract.get("purpose") != "current_action"
                or contract.get("completeness") != "complete"
                or isinstance(all_in, bool) or not isinstance(all_in, (int, float))
                or not math.isfinite(float(all_in)) or float(all_in) < 0
                or contract.get("is_real_fill") is not False):
            fail("all_in_cost_incomplete")
        if not isinstance(contract.get("method"), str) or not contract.get("method", "").strip():
            fail("current_cost_method_missing")
        components = {
            component.get("name"): component
            for component in contract.get("components", [])
            if isinstance(component, Mapping)
        }
        route_component = components.get("route_loss")
        network_component = components.get("network_fee")
        route_loss = (_nonnegative(route_component.get("pct"))
                      if isinstance(route_component, Mapping)
                      and route_component.get("status") == "included" else None)
        network_fee = (_nonnegative(network_component.get("pct"))
                       if isinstance(network_component, Mapping)
                       and network_component.get("status") == "included" else None)
        if route_loss is None:
            fail("route_loss_cost_missing")
        if network_fee is None:
            fail("network_fee_cost_missing")
        if (_nonnegative(all_in) is None or float(all_in) > MAX_ROUNDTRIP_LOSS_PCT
                or route_loss is not None and route_loss > MAX_ROUNDTRIP_LOSS_PCT):
            fail("current_all_in_cost_above_limit")
        contract_notional = _positive(contract.get("notional_usd"))
        if (notional is None or contract_notional is None
                or not math.isclose(contract_notional, notional, abs_tol=1e-9)):
            fail("cost_notional_mismatch")

    look_n = (evidence.get("look_n_per_arm")
              if isinstance(evidence, Mapping) else None)
    measured_n = evidence.get("measured_n") if isinstance(evidence, Mapping) else None
    if (not isinstance(evidence, Mapping) or evidence.get("state") != "pass"
            or evidence.get("lane") != "launch"
            or evidence.get("protocol_id") != PROTOCOL_ID
            or evidence.get("protocol_state") != "pass"
            or evidence.get("cost_is_real_fill") is not False
            or evidence.get("edge_verdict") != "有前向纸面selector edge迹象"
            or evidence.get("sample_kind") != "forward_paper_selector"
            or evidence.get("selection_stage")
            != "discovery_rule_before_security_and_route"
            or evidence.get("real_edge_n") != 0
            or evidence.get("real_edge_eligible") is not False
            or evidence.get("execution_edge_eligible") is not False
            or evidence.get("auto_execution_allowed") is not False
            or evidence.get("minimum_n") != LOOK_SIZES[0]
            or isinstance(look_n, bool) or look_n not in LOOK_SIZES
            or isinstance(measured_n, bool) or not isinstance(measured_n, int)
            or not isinstance(look_n, int) or measured_n < look_n * 2
            or not isinstance(evidence.get("reason"), str)
            or not evidence.get("reason", "").strip()):
        fail("evidence_gate_not_pass")
    if (isinstance(evidence, Mapping)
            and evidence.get("protocol_state") == "protocol_integrity_blocked"):
        fail("protocol_integrity_blocked")
    if assessment.get("delivery_sla_state") != "pass":
        fail("delivery_sla_unverified")
    if DELIVERY_READBACK_VERIFIER_VERSION is None:
        fail("delivery_readback_verifier_unavailable")
    security_gate = assessment.get("security_gate")
    if (not isinstance(security_gate, Mapping) or security_gate.get("state") != "pass"
            or security_gate.get("chain") != row.get("chain")
            or security_gate.get("token") != row.get("token")
            or security_gate.get("source") != "GoPlus Solana + finalized Solana RPC"
            or _aware(security_gate.get("checked_at")) != security_at
            or _aware(security_gate.get("expires_at")) != security_expires_at
            or any(not isinstance(security_gate.get(field), list)
                   or bool(security_gate.get(field))
                   for field in ("hard_flags", "cautions", "unknown_fields"))
            or bool(str(security_gate.get("reason") or "").strip())):
        fail("public_security_evidence_missing")
    providers = (security_gate.get("providers")
                 if isinstance(security_gate, Mapping) else None)
    if (not isinstance(providers, Mapping)
            or not all(isinstance(providers.get(name), Mapping)
                       and providers[name].get("state") == "pass"
                       for name in ("goplus", "solana_rpc"))):
        fail("public_security_provider_evidence_missing")
    execution_probe = assessment.get("execution_probe")
    back_usd = _positive(assessment.get("roundtrip_back_usd"))
    nested_loss = (_nonnegative(execution_probe.get("roundtrip_loss_pct"))
                   if isinstance(execution_probe, Mapping) else None)
    nested_back = (_positive(execution_probe.get("roundtrip_back_usd"))
                   if isinstance(execution_probe, Mapping) else None)
    nested_entry = (_positive(execution_probe.get("entry_reference_price"))
                    if isinstance(execution_probe, Mapping) else None)
    nested_invalidation = (_positive(execution_probe.get("invalidation_reference_price"))
                           if isinstance(execution_probe, Mapping) else None)
    expected_route_loss = (max(0.0, (notional - back_usd) / notional * 100)
                           if notional is not None and back_usd is not None else None)
    provider_contract = (execution_probe.get("provider_contract")
                         if isinstance(execution_probe, Mapping) else None)
    provider_contract_valid = (
        isinstance(provider_contract, Mapping)
        and provider_contract.get("version") == 1
        and provider_contract.get("provider") == "jupiter"
        and provider_contract.get("api_version") == "v2"
        and provider_contract.get("operation") == "order"
        and provider_contract.get("endpoint")
        == "https://api.jup.ag/swap/v2/order"
        and provider_contract.get("auth_mode") == "x_api_key"
        and provider_contract.get("slippage_bps") == 100
        and provider_contract.get("swap_mode") == "ExactIn"
        and provider_contract.get("read_only") is True
        and provider_contract.get("taker_supplied") is False
        and provider_contract.get("transaction_built") is False
    )
    if (not isinstance(execution_probe, Mapping)
            or execution_probe.get("state") != "quoted"
            or execution_probe.get("chain") != row.get("chain")
            or execution_probe.get("token") != row.get("token")
            or execution_probe.get("source") != assessment.get("quote_source")
            or execution_probe.get("api_mode") != "keyed_v2"
            or execution_probe.get("promotion_eligible") is not True
            or execution_probe.get("quote_contract_verified") is not True
            or not provider_contract_valid
            or _aware(execution_probe.get("checked_at")) != quote_at
            or execution_probe.get("read_only") is not True
            or execution_probe.get("is_real_fill") is not False
            or execution_probe.get("network_fees_included") is not True
            or bool(str(execution_probe.get("reason") or "").strip())
            or _positive(execution_probe.get("notional_usd")) is None
            or notional is None
            or not math.isclose(
                float(execution_probe.get("notional_usd")), notional, abs_tol=1e-9
            )
            or route_loss is None or nested_loss is None
            or not math.isclose(nested_loss, route_loss, abs_tol=1e-6)
            or expected_route_loss is None
            or not math.isclose(expected_route_loss, route_loss, abs_tol=1e-4)
            or back_usd is None or nested_back is None
            or not math.isclose(nested_back, back_usd, abs_tol=1e-9)
            or entry is None or nested_entry is None
            or not math.isclose(nested_entry, entry, abs_tol=1e-12)
            or invalidation is None or nested_invalidation is None
            or not math.isclose(nested_invalidation, invalidation, abs_tol=1e-12)):
        fail("public_route_evidence_missing")
    return failures
