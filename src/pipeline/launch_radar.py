"""Launch Radar — the low-float / early repricing opportunity lane.

This is deliberately an event pipeline, not a generic meme score.  A launch is
captured once with its first observable pool price, liquidity and risk facts;
the UI can then show whether the user is early enough to *consider* a tiny probe
or should only watch.  No automatic execution and no claim that a new token will
rise.
"""
from __future__ import annotations

import json
from copy import deepcopy
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Callable

from src.pipeline.opportunity_ledger import (
    active, event_id_readback_matches, record, record_if_absent,
)
from src.contract import dexscreener as dex
from src.contract.launch_probe import SUPPORTED_LAUNCH_CHAINS
from src.contract.launch_protocol import (
    COHORT_VERSION, LAUNCH_COST_METHOD, PROTOCOL_ID, PROTOCOL_START_AT,
)
from src.contract.launch_selector import (
    MAX_FDV_USD, MAX_LIQUIDITY_USD, MAX_POOL_AGE_MIN,
    MAX_SOURCE_TO_DECISION_SECONDS, MIN_FDV_USD,
    MIN_LIQUIDITY_USD, evaluate_selector_snapshot, freeze_selector_snapshot,
    freeze_source_snapshot,
)

PROFILES_URL = "https://api.dexscreener.com/token-profiles/latest/v1"
PAIRS_URL = dex.TOKEN_PAIRS_URL
TOKENS_URL = dex.TOKENS_URL
DEX_BATCH_SIZE = dex.BATCH_SIZE
SUPPORTED_CHAINS = set(SUPPORTED_LAUNCH_CHAINS)
MAX_CANDIDATES = 30
MAX_EXECUTION_ASSESSMENTS = 5
ENTRY_EVIDENCE_COHORT_VERSION = COHORT_VERSION
Clock = Callable[[], datetime]
AdmissionProbe = Callable[..., dict]


MarketDataSchemaError = dex.MarketDataSchemaError


def _wall_clock() -> datetime:
    return datetime.now(timezone.utc)


def _clock_value(clock: Clock) -> datetime:
    value = clock()
    return (value if value.tzinfo else value.replace(tzinfo=timezone.utc)).astimezone(
        timezone.utc
    )


def _protocol_admission(
        *, now: datetime, admission_probe: AdmissionProbe | None = None) -> dict:
    if admission_probe is None:
        from src.pipeline import launch_protocol_gate

        admission_probe = launch_protocol_gate.admit
    value = admission_probe(
        protocol_id=PROTOCOL_ID, cohort_version=COHORT_VERSION,
        start_at=PROTOCOL_START_AT, now=now,
    )
    if not isinstance(value, dict):
        raise ValueError("launch protocol admission did not return a mapping")
    return value


def _public_research_protocol(readiness: object, admission: object) -> dict:
    """Derive an effective public enrollment gate from the same view snapshot."""
    readiness = readiness if isinstance(readiness, dict) else {}
    admission = admission if isinstance(admission, dict) else {}
    reasons: list[str] = []

    def add(value: object) -> None:
        if isinstance(value, str) and value.strip() and value not in reasons:
            reasons.append(value)

    for value in readiness.get("reason_codes") or []:
        add(value)
    for value in admission.get("reason_codes") or []:
        add(value)
    identity_ok = True
    for field, expected in (
        ("protocol_id", PROTOCOL_ID),
        ("cohort_version", COHORT_VERSION),
        ("protocol_start_at", PROTOCOL_START_AT),
    ):
        if admission.get(field) != expected:
            identity_ok = False
            add(f"protocol_admission_{field}_mismatch")
    source_ready = (
        readiness.get("state") == "ready" and readiness.get("ready") is True
    )
    admission_open = (
        identity_ok and admission.get("state") == "open"
        and admission.get("enrollment_open") is True
    )
    enrollment_open = source_ready and admission_open
    persistent_state = str(admission.get("state") or "unavailable")
    if not source_ready:
        add("source_readiness_not_ready")
    if not admission_open:
        add("protocol_admission_not_open")
    if enrollment_open:
        state = "open"
        reasons = []
    elif persistent_state == "breached":
        state = "breached"
    elif persistent_state in {"scheduled", "armed"}:
        state = persistent_state
    else:
        state = "blocked"
    return {
        "protocol_id": PROTOCOL_ID,
        "cohort_version": COHORT_VERSION,
        "protocol_start_at": PROTOCOL_START_AT,
        "enrollment_state": state,
        "persistent_admission_state": persistent_state,
        "enrollment_open": enrollment_open,
        "reason_codes": reasons,
        "source_readiness_state": readiness.get("state") or "blocked",
        "sample_kind": "forward_paper_selector",
        "selection_stage": "discovery_rule_before_security_and_route",
        "real_edge_n": 0,
        "real_edge_eligible": False,
        "execution_edge_eligible": False,
        "auto_execution_allowed": False,
    }


def _num(value, default=0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _json(url: str) -> object:
    req = urllib.request.Request(url, headers={"User-Agent": "CryptoScope/LaunchRadar/1.0"})
    with urllib.request.urlopen(req, timeout=12) as response:
        return json.loads(response.read().decode())


def _pair_for(profile: dict, fetch=_json) -> dict | None:
    chain, token = profile.get("chainId"), profile.get("tokenAddress")
    if chain not in SUPPORTED_CHAINS or not token:
        return None
    return _pair_for_token(chain, token, fetch=fetch)


def _pair_for_token(chain: str, token: str, *, fetch=_json) -> dict | None:
    """Select the deepest observable pool for one identity-proven token."""
    return _pair_for_token_evidence(chain, token, fetch=fetch)[0]


def _pair_for_token_evidence(
        chain: str, token: str, *, fetch=_json) -> tuple[dict | None, str]:
    """Return an exact-pool selection plus the complete response hash."""
    evidence = dex.exact_base_pair(chain, token, fetch=fetch)
    return evidence["pair"], evidence["response_hash"]


def _batch_candidate_tokens(chain: str, tokens: list[str], *, fetch=_json) -> dict:
    """Prefilter up to 30 tokens without using this response as entry evidence."""
    return dex.batch_prefilter(chain, tokens, fetch=fetch)


def qualify(pair: dict, *, now: datetime | None = None, source: str = "dexscreener") -> dict | None:
    """Turn one observable DEX pool into a conservative executable event card.

    The thresholds only establish that a token is tradeable enough to observe;
    they do not predict a pump. A high sniper/boost/volume signal never overrides
    inadequate liquidity or a late launch.
    """
    now = now or datetime.now(timezone.utc)
    base = pair.get("baseToken") or {}
    chain, token = pair.get("chainId"), base.get("address")
    pair_address = pair.get("pairAddress")
    price = _num(pair.get("priceUsd"))
    liq = _num((pair.get("liquidity") or {}).get("usd"))
    fdv = _num(pair.get("fdv") or pair.get("marketCap"))
    created_ms = pair.get("pairCreatedAt")
    if (chain not in SUPPORTED_CHAINS or not token or not pair_address
            or price <= 0 or not created_ms):
        return None
    try:
        event_at = datetime.fromtimestamp(float(created_ms) / 1000, tz=timezone.utc)
    except (TypeError, ValueError, OSError):
        return None
    age_min = (now - event_at).total_seconds() / 60
    tx5 = (pair.get("txns") or {}).get("m5") or {}
    buys, sells = int(tx5.get("buys") or 0), int(tx5.get("sells") or 0)
    vol5 = _num((pair.get("volume") or {}).get("m5"))
    boost = _num((pair.get("boosts") or {}).get("active"))
    # A pool outside these bounds is either not executable, not the early-error
    # regime, or already too old for this specific lane.
    if not (MIN_LIQUIDITY_USD <= liq <= MAX_LIQUIDITY_USD
            and MIN_FDV_USD <= fdv <= MAX_FDV_USD
            and 0 <= age_min <= MAX_POOL_AGE_MIN):
        return None
    selector_snapshot = freeze_selector_snapshot(
        pool_created_at=event_at.isoformat(), liquidity_usd=liq, fdv_usd=fdv,
        volume_m5_usd=vol5, buys_m5=buys, sells_m5=sells,
    )
    selector = evaluate_selector_snapshot(
        selector_snapshot, event_at=event_at.isoformat(), decision_at=now.isoformat(),
    )
    flow_ratio = selector["flow_ratio"]
    # Both the size and route model are recomputed from the immutable selector facts.
    max_notional = selector["max_notional_usd"]
    roundtrip_cost = selector["modeled_route_roundtrip_pct"]
    from src.pipeline.execution_cost import (
        discovery_contract,
        solana_launch_full_paper_contract,
    )
    if chain == "solana":
        cost_contract = solana_launch_full_paper_contract(
            notional_usd=max_notional,
            modeled_route_roundtrip_pct=roundtrip_cost,
            method=LAUNCH_COST_METHOD,
        )
        frozen_cost = cost_contract["all_in_total_pct"]
        cost_model = LAUNCH_COST_METHOD
    else:
        # EVM pools remain visible, but their unknown network fee keeps the contract
        # descriptive and excludes them from the Solana-only v6 edge protocol.
        cost_model = "constant_product_roundtrip_plus_0.60pct_buffer_v1"
        cost_contract = discovery_contract(
            notional_usd=max_notional,
            modeled_roundtrip_pct=roundtrip_cost,
            method=cost_model,
        )
        frozen_cost = roundtrip_cost
    decision = selector["decision"]
    reasons = [f"首池 {age_min:.0f}m", f"FDV ${fdv:,.0f}", f"流动性 ${liq:,.0f}"]
    if buys or sells:
        reasons.append(f"5m 买/卖 {buys}/{sells}")
    if boost:
        reasons.append(f"推广 {boost:.0f}(仅作注意力，不是买入理由)")
    observed_at = now.astimezone(timezone.utc).isoformat()
    return {
        "lane": "launch", "chain": chain, "token": token,
        "event_key": f"dex_pair:{pair_address}",
        "symbol": base.get("symbol") or "?", "name": base.get("name") or "",
        "source": source, "event_at": event_at.isoformat(), "detected_at": observed_at,
        "decision_at": observed_at, "state": "live",
        "decision": decision, "entry_price": price,
        "entry_observation": {
            "version": 1, "provider": "dexscreener_token_pairs_v1",
            "observed_at": observed_at, "chain": chain,
            "base_token": token,
            "quote_token": (pair.get("quoteToken") or {}).get("address"),
            "pair": pair_address, "price": price, "currency": "usd",
            "field": "priceUsd", "identity_verified": True,
            "selector_snapshot": selector_snapshot,
        },
        "invalidation_price": round(price * 0.70, 12),
        "max_notional_usd": max_notional, "age_min": round(age_min, 1),
        "modeled_route_cost_pct_est": roundtrip_cost,
        "roundtrip_cost_pct_est": frozen_cost,
        "cost_model": cost_model,
        # Generic DEX profiles and EVM factory rows are a useful discovery surface but
        # not the pre-registered Pump.fun source universe.  Only the primary bridge may
        # promote a row to v6 after freezing its exact chain-log evidence.
        "cost_contract": cost_contract,
        "cohort_version": 0,
        "fdv": fdv, "liquidity_usd": liq, "volume_m5": vol5,
        "buys_m5": buys, "sells_m5": sells, "flow_ratio": round(flow_ratio, 2),
        "boost_active": boost, "pair": pair_address, "url": pair.get("url"),
        "reasons": reasons,
    }


def _assess_candidate(event: dict, assessor, *, assessed: int,
                      max_assessments: int) -> tuple[dict, int]:
    if event.get("decision") != "SMALL_PROBE":
        return event, assessed
    if assessed < max(0, max_assessments):
        assessed += 1
        try:
            return assessor(event), assessed
        except Exception as exc:
            event["decision"] = "WATCH"
            event["security_gate"] = {
                "state": "unknown", "reason": f"assessment failed: {str(exc)[:60]}",
            }
            event["execution_probe"] = {"state": "skipped", "reason": "assessment failed"}
            return event, assessed
    event["decision"] = "WATCH"
    event["security_gate"] = {
        "state": "unknown", "reason": "per-scan assessment budget exhausted",
    }
    event["execution_probe"] = {"state": "skipped", "reason": "assessment budget exhausted"}
    event["reasons"].append("执行门降级:本轮安全/路由检查预算已用完")
    return event, assessed


def _execution_assessment(event: dict, *, assessed_at: datetime) -> dict:
    """Convert a mutable gate result into one append-only current measurement."""
    chain, token = event.get("chain"), event.get("token")
    security = {**(event.get("security_gate") or {}), "chain": chain, "token": token}
    route = {**(event.get("execution_probe") or {}), "chain": chain, "token": token}
    notional = float(event.get("max_notional_usd") or 0)
    from src.pipeline.execution_cost import route_contract, unknown_route_contract
    try:
        route_loss = float(route["roundtrip_loss_pct"])
        cost = route_contract(
            notional_usd=notional, route_loss_pct=max(0, route_loss),
            method=str(route.get("api_mode") or route.get("source") or "read_only_route"))
    except (KeyError, TypeError, ValueError):
        cost = unknown_route_contract(
            notional_usd=notional, method=str(route.get("source") or "route_unavailable"))
    quote_at = route.get("checked_at") or event.get("quote_at")
    expires_at = event.get("expires_at")
    security_at = security.get("checked_at") or assessed_at.isoformat()
    security_expires_at = None
    if security.get("state") == "pass":
        try:
            from src.pipeline.launch_execution import SECURITY_TTL_SECONDS
            security_expires_at = (datetime.fromisoformat(security_at).astimezone(timezone.utc)
                                   + timedelta(seconds=SECURITY_TTL_SECONDS)).isoformat()
        except (TypeError, ValueError):
            security_expires_at = None
    sec_state, route_state = security.get("state") or "unknown", route.get("state") or "unknown"
    reasons = []
    if sec_state != "pass":
        reasons.append(f"security_{sec_state}")
    if route_state != "quoted":
        reasons.append(f"route_{route_state}")
    if cost["completeness"] != "complete":
        reasons.append("cost_incomplete")
    if route.get("entry_reference_price") is None:
        reasons.append("entry_reference_price_unknown")
    reasons.append("delivery_sla_unverified")
    return {
        "kind": "read_only_quote", "chain": chain, "token": token,
        "assessed_at": assessed_at.isoformat(),
        "security_state": sec_state,
        "security_at": security_at, "security_expires_at": security_expires_at,
        "route_state": route_state, "quote_source": route.get("source"),
        "quote_mode": route.get("api_mode"), "quote_at": quote_at,
        "quote_expires_at": expires_at, "expires_at": expires_at,
        "notional_usd": notional,
        "entry_reference_price": route.get("entry_reference_price"),
        "invalidation_reference_price": route.get("invalidation_reference_price"),
        "roundtrip_back_usd": route.get("roundtrip_back_usd"),
        "cost_contract": cost, "is_real_fill": False,
        "reason_code": reasons[0] if reasons else None,
        "action_reason_codes": reasons, "delivery_sla_state": "unverified",
        "security_gate": security, "execution_probe": route,
        "auto_execution_allowed": False,
    }


def _append_assessment(ident: str, event: dict, assessor, *, assessed: int,
                       max_assessments: int, clock: Clock = _wall_clock) -> tuple[dict, int]:
    """Assess a copy so current quotes can never mutate the discovery snapshot."""
    assessed_event, assessed = _assess_candidate(
        deepcopy(event), assessor, assessed=assessed, max_assessments=max_assessments)
    if event.get("decision") == "SMALL_PROBE":
        from src.pipeline.opportunity_ledger import append_execution_assessment
        append_execution_assessment(
            ident,
            _execution_assessment(assessed_event, assessed_at=_clock_value(clock)),
        )
    return assessed_event, assessed


def _scan_primary_solana_batch(fetch, *, now: datetime, assessor, assessed: int,
                               max_assessments: int, max_candidates: int,
                               clock: Clock = _wall_clock,
                               admission_probe: AdmissionProbe | None = None,
                               ) -> tuple[dict, int]:
    """Claim and process at most one provider-sized Solana batch."""
    from src.pipeline import solana_launch_stream as stream
    result = {"available": True, "attempted": 0, "recorded": 0, "inserted": 0,
              "pending": 0, "errors": 0, "screened_out": 0, "orphaned": 0}
    admission = _protocol_admission(now=now, admission_probe=admission_probe)
    if admission.get("state") != "open" or admission.get("enrollment_open") is not True:
        result.update({
            "available": False, "reason": "launch protocol enrollment is blocked",
            "source_admission": admission,
        })
        return result, assessed
    rows = stream.claim_forward_protocol_batch(
        now=now, limit=min(DEX_BATCH_SIZE, max(0, max_candidates)),
        protocol_start_at=PROTOCOL_START_AT,
        max_source_to_decision_seconds=MAX_SOURCE_TO_DECISION_SECONDS,
    )
    result["attempted"] = len(rows)
    settled: set[str] = set()

    def settle(raw: dict, state: str, *, error: str | None = None,
               ledger_event_id: str | None = None, at: datetime,
               outcome_kind: str, response_hash: str | None = None,
               pair_address: str | None = None,
               error_kind: str | None = None) -> None:
        if not stream.set_qualification(
            raw["signature"], state, error=error,
            ledger_event_id=ledger_event_id,
            lease_token=raw["qualification_lease_token"],
            outcome_kind=outcome_kind, response_hash=response_hash,
            pair_address=pair_address, error_kind=error_kind, at=at,
        ):
            raise RuntimeError("Solana qualification lease was lost")
        settled.add(raw["signature"])

    def release_unsettled() -> None:
        for candidate in rows:
            if candidate["signature"] not in settled:
                stream.release_qualification_lease(
                    candidate["signature"], candidate["qualification_lease_token"]
                )

    for offset in range(0, len(rows), DEX_BATCH_SIZE):
        batch = rows[offset:offset + DEX_BATCH_SIZE]
        try:
            batch_evidence = _batch_candidate_tokens(
                "solana", [raw["mint"] for raw in batch], fetch=fetch,
            )
            batch_observed_at = _clock_value(clock)
            stream.report_qualification_provider(
                "ok", response_hash=batch_evidence["response_hash"],
                at=batch_observed_at,
            )
            candidate_tokens = batch_evidence["base_tokens"]
        except Exception as exc:
            # A provider-wide/schema failure is one health failure, not evidence
            # that every token lacks a pool. Preserve each token state for retry.
            release_unsettled()
            stream.report_qualification_provider(
                "error", error=f"{type(exc).__name__}: {exc}", at=now,
            )
            result["errors"] += 1
            result["reason"] = f"{type(exc).__name__}: {exc}"[:160]
            break
        batch_failed = False
        for raw in batch:
            try:
                from src.pipeline import solana_launch_reconcile as reconcile

                frozen_reconciliation = reconcile.candidate_reconciliation_proof(
                    raw["signature"], slot=raw["slot"], mint=raw["mint"],
                )
                normalized_mint = raw["mint"]
                if normalized_mint not in candidate_tokens:
                    settle(
                        raw, "market_pending", error="DEX pool not indexed yet",
                        at=batch_observed_at, outcome_kind="valid_empty",
                        response_hash=batch_evidence["response_hash"],
                    )
                    result["pending"] += 1
                    continue
                try:
                    pair, exact_response_hash = _pair_for_token_evidence(
                        "solana", raw["mint"], fetch=fetch,
                    )
                except Exception as exc:
                    stream.report_qualification_provider(
                        "error", error=f"{type(exc).__name__}: {exc}", at=now,
                    )
                    raise
                observed_at = _clock_value(clock)
                detected_at = datetime.fromisoformat(raw["detected_at"]).astimezone(
                    timezone.utc
                )
                if (observed_at - detected_at).total_seconds() \
                        > MAX_SOURCE_TO_DECISION_SECONDS:
                    settle(
                        raw, "qualification_expired",
                        error="source-to-decision deadline exceeded", at=observed_at,
                        outcome_kind="deadline_exceeded",
                        response_hash=exact_response_hash,
                        pair_address=(pair or {}).get("pairAddress"),
                    )
                    result["screened_out"] += 1
                    continue
                if not pair:
                    settle(
                        raw, "market_pending", error="exact DEX pool not indexed yet",
                        at=observed_at, outcome_kind="exact_pool_pending",
                        response_hash=exact_response_hash,
                    )
                    result["pending"] += 1
                    continue
                event = qualify(
                    pair, now=observed_at,
                    source="Pump.fun standard logs + DEX Screener pool",
                )
                if event is None:
                    created_ms = pair.get("pairCreatedAt")
                    try:
                        age_hours = ((observed_at - datetime.fromtimestamp(
                            float(created_ms) / 1000,
                            tz=timezone.utc)).total_seconds() / 3600)
                    except (TypeError, ValueError, OSError):
                        age_hours = 0
                    terminal = age_hours > 24
                    state = "screened_out" if terminal else "market_pending"
                    reason = ("pool is older than the 24h launch window" if terminal
                              else "pool not yet within liquidity/FDV launch bounds")
                    settle(
                        raw, state, error=reason, at=observed_at,
                        outcome_kind="screened_out" if terminal else "below_threshold",
                        response_hash=exact_response_hash,
                        pair_address=pair.get("pairAddress"),
                    )
                    result["screened_out" if terminal else "pending"] += 1
                    continue
                event["detected_at"] = raw["detected_at"]
                event["decision_at"] = observed_at.isoformat()
                current_admission = _protocol_admission(
                    now=observed_at, admission_probe=admission_probe,
                )
                if (current_admission.get("state") != "open"
                        or current_admission.get("enrollment_open") is not True):
                    release_unsettled()
                    result.update({
                        "available": False,
                        "reason": "launch protocol enrollment breached during batch",
                        "source_admission": current_admission,
                    })
                    batch_failed = True
                    break
                current_reconciliation = reconcile.candidate_reconciliation_proof(
                    raw["signature"], slot=raw["slot"], mint=raw["mint"],
                )
                if current_reconciliation != frozen_reconciliation:
                    raise RuntimeError("candidate reconciliation proof changed during scan")
                source_snapshot = freeze_source_snapshot(
                    signature=raw["signature"], slot=raw["slot"],
                    event_type=raw["event_type"], detected_at=raw["detected_at"],
                    captured_at=raw["captured_at"],
                    decision_at=observed_at.isoformat(),
                    mint=raw["mint"], raw_payload_hash=raw["raw_payload_hash"],
                    hydration_payload_hash=raw["hydration_payload_hash"],
                    capture_mode=raw["capture_mode"],
                    source_provider=raw["source_provider"],
                    reconciliation_state=raw["reconciliation_state"],
                    reconciled_at=raw["reconciled_at"],
                    reconciliation_proof=current_reconciliation,
                )
                event["entry_observation"]["source_snapshot"] = source_snapshot
                event["cohort_version"] = ENTRY_EVIDENCE_COHORT_VERSION
                event["event_key"] = f"pump_fun:{raw['signature']}"
                event["primary_evidence"] = {
                    "source": "Solana live websocket + independent finalized archive",
                    "program": stream.PUMP_FUN_PROGRAM,
                    "signature": raw["signature"],
                    "creator": raw["creator"],
                    "slot": raw["slot"],
                    "event_type": raw["event_type"],
                    "reconciliation_epoch_id": current_reconciliation["epoch_id"],
                    "evidence_state": "complete",
                    "explorer_url": f"https://solscan.io/tx/{raw['signature']}",
                }
                ident, new = record_if_absent(event)
                if not event_id_readback_matches(
                        ident, lane="launch", chain="solana", token=raw["mint"],
                        cohort_version=ENTRY_EVIDENCE_COHORT_VERSION,
                        source_snapshot=source_snapshot):
                    settle(
                        raw, "ledger_orphan",
                        error="opportunity ledger ID failed exact read-back",
                        ledger_event_id=ident, at=observed_at,
                        outcome_kind="ledger_orphan",
                        response_hash=exact_response_hash,
                        pair_address=pair.get("pairAddress"),
                    )
                    result["orphaned"] += 1
                    continue
                _, assessed = _append_assessment(
                    ident, event, assessor, assessed=assessed,
                    max_assessments=max_assessments, clock=clock)
                settle(
                    raw, "qualified_recorded", ledger_event_id=ident,
                    at=observed_at, outcome_kind="qualified",
                    response_hash=exact_response_hash,
                    pair_address=pair.get("pairAddress"),
                )
                result["recorded"] += 1
                result["inserted"] += int(new)
            except Exception as exc:
                release_unsettled()
                result["errors"] += 1
                result["reason"] = f"{type(exc).__name__}: {exc}"[:160]
                batch_failed = True
                break
        if batch_failed:
            break
    return result, assessed


def _scan_primary_solana(fetch, *, now: datetime, assessor, assessed: int,
                         max_assessments: int, max_candidates: int,
                         clock: Clock = _wall_clock,
                         admission_probe: AdmissionProbe | None = None,
                         ) -> tuple[dict, int]:
    """Bridge live Pump.fun evidence in short, crash-safe provider batches."""
    total = {"available": True, "attempted": 0, "recorded": 0, "inserted": 0,
             "pending": 0, "errors": 0, "screened_out": 0, "orphaned": 0}
    from src.pipeline import solana_launch_stream as stream
    remaining = max(0, int(max_candidates))
    if remaining == 0:
        return total, assessed
    admission = _protocol_admission(now=now, admission_probe=admission_probe)
    if admission.get("state") != "open" or admission.get("enrollment_open") is not True:
        total.update({
            "available": False, "reason": "launch protocol enrollment is blocked",
            "source_admission": admission,
        })
        return total, assessed
    provider_health = stream.qualification_provider_health(now=now)
    if not provider_health["ready"]:
        total.update({
            "available": False, "errors": 1,
            "reason": "DEX Screener qualification circuit is open",
        })
        return total, assessed
    while remaining:
        batch, assessed = _scan_primary_solana_batch(
            fetch, now=now, assessor=assessor, assessed=assessed,
            max_assessments=max_assessments,
            max_candidates=min(DEX_BATCH_SIZE, remaining), clock=clock,
            admission_probe=admission_probe,
        )
        for key in (
            "attempted", "recorded", "inserted", "pending", "errors",
            "screened_out", "orphaned",
        ):
            total[key] += int(batch.get(key) or 0)
        claimed = int(batch.get("attempted") or 0)
        remaining -= claimed
        if batch.get("available") is False:
            total["available"] = False
            if batch.get("source_admission"):
                total["source_admission"] = batch["source_admission"]
        if claimed == 0 or batch.get("errors") or batch.get("available") is False:
            if batch.get("reason"):
                total["reason"] = batch["reason"]
            break
    return total, assessed


def _scan_primary_evm(fetch, *, now: datetime, assessor, assessed: int,
                      max_assessments: int, max_candidates: int,
                      clock: Clock = _wall_clock) -> tuple[dict, int]:
    """Bridge exact official factory pools without guessing token identity."""
    from src.pipeline import evm_factory_stream as stream
    from src.pipeline.evm_launch_bridge import exact_pair, identify_target

    rows = stream.qualification_batch(now=now, limit=max_candidates)
    result = {"available": True, "attempted": 0, "recorded": 0, "inserted": 0,
              "pending": 0, "errors": 0, "screened_out": 0, "duplicates": 0,
              "orphaned": 0}
    backoffs = (180, 360, 900, 1800)
    for raw in rows:
        result["attempted"] += 1
        target, terminal = identify_target(raw)
        if terminal:
            stream.set_qualification(raw, terminal, reason="quote-side identity is not unique",
                                     at=now)
            result["screened_out"] += 1
            continue
        retry_after = backoffs[min(int(raw.get("qualification_attempts") or 0),
                                   len(backoffs) - 1)]
        try:
            pairs = fetch(PAIRS_URL.format(chain=raw["chain"], token=target))
            observed_at = _clock_value(clock)
            pair = exact_pair(raw, target, pairs)
        except Exception as exc:
            stream.set_qualification(raw, "market_error", reason=str(exc),
                                     target_token=target,
                                     retry_after_seconds=retry_after, at=now)
            result["errors"] += 1
            break
        if pair is None:
            stream.set_qualification(raw, "market_pending",
                                     reason="exact factory pool not indexed yet",
                                     target_token=target,
                                     retry_after_seconds=retry_after, at=now)
            result["pending"] += 1
            continue
        try:
            chain_time = datetime.fromisoformat(raw["block_at"]).astimezone(timezone.utc)
            market_time = datetime.fromtimestamp(
                float(pair["pairCreatedAt"]) / 1000, tz=timezone.utc)
        except (KeyError, TypeError, ValueError, OSError) as exc:
            stream.set_qualification(raw, "market_error",
                                     reason=f"invalid pool creation clock: {exc}",
                                     target_token=target,
                                     retry_after_seconds=retry_after, at=now)
            result["errors"] += 1
            continue
        if abs((market_time - chain_time).total_seconds()) > 600:
            stream.set_qualification(raw, "market_error",
                                     reason="market pool clock differs from factory by >10m",
                                     target_token=target,
                                     retry_after_seconds=retry_after, at=now)
            result["errors"] += 1
            continue
        event = qualify(pair, now=observed_at,
                        source=f"{raw['chain']} {raw['venue']} factory + DEX Screener exact pool")
        if event is None:
            stream.set_qualification(raw, "below_threshold",
                                     reason="exact pool below current liquidity/FDV bounds",
                                     target_token=target,
                                     retry_after_seconds=retry_after, at=now)
            result["pending"] += 1
            continue
        # The factory row is the identity authority. DEX APIs may return a
        # checksum-cased address; store the canonical raw chain/token so the
        # cross-database read-back is exact rather than merely case-insensitive.
        event["chain"] = raw["chain"]
        event["token"] = target
        # The factory clock is the primary event time. The market decision is made
        # now; never backdate it to the raw socket receipt.
        event["event_at"] = chain_time.isoformat()
        event["detected_at"] = observed_at.isoformat()
        event["decision_at"] = observed_at.isoformat()
        event["event_key"] = (
            f"factory:{raw['chain']}:{raw['transaction_hash']}:{raw['log_index']}"
        )
        event["primary_evidence"] = {
            "source": "official EVM factory log",
            "factory": raw["factory"], "venue": raw["venue"],
            "transaction_hash": raw["transaction_hash"],
            "log_index": raw["log_index"], "block_number": raw["block_number"],
            "block_hash": raw["block_hash"], "pool": raw["pool"],
            "raw_detected_at": raw["detected_at"], "evidence_state": "complete",
            "explorer_url": (f"https://bscscan.com/tx/{raw['transaction_hash']}"
                             if raw["chain"] == "bsc" else
                             f"https://basescan.org/tx/{raw['transaction_hash']}"
                             if raw["chain"] == "base" else
                             f"https://etherscan.io/tx/{raw['transaction_hash']}"),
        }
        ident, new = record_if_absent(event)
        if not new:
            stream.set_qualification(
                raw, "duplicate_token_existing",
                reason="token already has a first launch event",
                target_token=target, at=now,
            )
            result["duplicates"] += 1
            continue
        try:
            ledger_verified = event_id_readback_matches(
                ident, lane="launch", chain=raw["chain"], token=target)
            orphan_reason = "opportunity ledger ID failed exact read-back"
        except Exception as exc:
            ledger_verified = False
            orphan_reason = f"opportunity ledger exact read-back unavailable: {exc}"
        if not ledger_verified:
            stream.set_qualification(
                raw, "ledger_orphan",
                reason=orphan_reason,
                target_token=target, ledger_event_id=ident, at=now,
            )
            result["orphaned"] += 1
            continue
        _, assessed = _append_assessment(
            ident, event, assessor, assessed=assessed,
            max_assessments=max_assessments, clock=clock)
        stream.set_qualification(raw, "qualified_recorded", target_token=target,
                                 ledger_event_id=ident, at=now)
        result["recorded"] += 1
        result["inserted"] += 1
    return result, assessed


def scan(fetch=_json, *, now: datetime | None = None, max_profiles: int = MAX_CANDIDATES,
         assessor=None, max_assessments: int = MAX_EXECUTION_ASSESSMENTS,
         max_primary: int = 120, max_evm: int = 10,
         evidence_clock: Clock | None = None,
         protocol_admission_probe: AdmissionProbe | None = None) -> dict:
    """Discover pools, then safety/round-trip gate only raw actionable candidates.

    The hard assessment budget bounds GoPlus/router calls. Anything beyond the budget
    is WATCH, never an unchecked SMALL_PROBE.
    """
    now = now or datetime.now(timezone.utc)
    evidence_clock = evidence_clock or _wall_clock
    if assessor is None:
        from src.pipeline.launch_execution import assess as assessor
    inserted = assessed = 0
    try:
        primary, assessed = _scan_primary_solana(
            fetch, now=now, assessor=assessor, assessed=assessed,
            max_assessments=max_assessments, max_candidates=max(0, max_primary),
            clock=evidence_clock, admission_probe=protocol_admission_probe)
        inserted += primary["inserted"]
    except Exception as exc:
        primary = {"available": False, "attempted": 0, "recorded": 0, "inserted": 0,
                   "pending": 0, "errors": 1, "screened_out": 0,
                   "reason": str(exc)[:120]}
    try:
        primary_evm, assessed = _scan_primary_evm(
            fetch, now=now, assessor=assessor, assessed=assessed,
            max_assessments=max_assessments, max_candidates=max(0, max_evm),
            clock=evidence_clock)
        inserted += primary_evm["inserted"]
    except Exception as exc:
        primary_evm = {"available": False, "attempted": 0, "recorded": 0,
                       "inserted": 0, "pending": 0, "errors": 1,
                       "screened_out": 0, "duplicates": 0,
                       "reason": str(exc)[:120]}
    profiles = fetch(PROFILES_URL)
    profiles = profiles if isinstance(profiles, list) else []
    for profile in profiles[:max_profiles]:
        try:
            pair = _pair_for(profile, fetch)
            observed_at = _clock_value(evidence_clock)
            event = qualify(pair, now=observed_at) if pair else None
            if event:
                ident, new = record(event)
                _, assessed = _append_assessment(
                    ident, event, assessor, assessed=assessed,
                    max_assessments=max_assessments, clock=evidence_clock)
                inserted += int(new)
        except Exception:
            continue
    return {"scanned": (len(profiles[:max_profiles]) + primary["attempted"]
                        + primary_evm["attempted"]),
            "profile_scanned": len(profiles[:max_profiles]), "primary": primary,
            "primary_evm": primary_evm,
            "assessed": assessed, "inserted": inserted,
            "events": active("launch"),
            "source": "Primary chain launch evidence + DEX Screener pools/profiles"}


def refresh_quotes(*, now: datetime | None = None, assessor=None,
                   max_candidates: int = 1, refresh_before_seconds: int = 30,
                   retry_after_seconds: int = 60,
                   evidence_clock: Clock | None = None) -> dict:
    """Refresh a bounded set of evidence-bound quotes without rediscovery.

    Discovery remains a three-minute event job. This fast path only appends current
    read-only measurements and never changes the frozen cohort, price, or cost.
    """
    from src.pipeline import opportunity_ledger as ledger

    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    evidence_clock = evidence_clock or _wall_clock
    from src.pipeline.edge_validation import is_protocol_event

    rows = [
        row for row in ledger.outcome_rows(open_only=True)
        if row.get("decision") == "SMALL_PROBE"
        and (
            is_protocol_event(row)
            or (
                row.get("cohort_version") == ENTRY_EVIDENCE_COHORT_VERSION
                and isinstance(row.get("entry_observation"), dict)
            )
        )
    ]
    recent = []
    for row in rows:
        try:
            if now - datetime.fromisoformat(row["detected_at"]).astimezone(timezone.utc) \
                    <= timedelta(hours=3):
                recent.append(row)
        except (TypeError, ValueError, KeyError):
            continue
    recent.sort(key=lambda row: row["detected_at"], reverse=True)
    result = {"eligible": len(recent), "attempted": 0, "refreshed": 0,
              "skipped_fresh": 0, "skipped_backoff": 0, "errors": 0}
    for row in recent:
        if result["attempted"] >= max(0, int(max_candidates)):
            break
        latest = ledger.latest_execution_assessment(row["id"])
        if latest:
            try:
                assessed_age = (now - datetime.fromisoformat(latest["assessed_at"])
                                 .astimezone(timezone.utc)).total_seconds()
            except (TypeError, ValueError):
                assessed_age = retry_after_seconds + 1
            try:
                remaining = (datetime.fromisoformat(latest["expires_at"])
                             .astimezone(timezone.utc) - now).total_seconds()
            except (TypeError, ValueError):
                remaining = None
            if latest.get("route_state") == "quoted" and remaining is not None \
                    and remaining > refresh_before_seconds:
                result["skipped_fresh"] += 1
                continue
            if latest.get("route_state") != "quoted" and assessed_age < retry_after_seconds:
                result["skipped_backoff"] += 1
                continue
        event = dict(row.get("payload") or {})
        for key in ("lane", "chain", "token", "symbol", "entry_price",
                    "invalidation_price", "max_notional_usd", "decision",
                    "cost_pct_est", "cost_model", "cost_contract", "cohort_version"):
            if row.get(key) is not None:
                event[key] = row[key]
        event.setdefault("reasons", [])
        current_assessor = assessor
        if current_assessor is None:
            from src.pipeline.launch_execution import gate, route_probe, security_probe

            cached_security = ((latest or {}).get("payload") or {}).get("security_gate")
            try:
                security_age = (now - datetime.fromisoformat(latest["security_at"])
                                .astimezone(timezone.utc)).total_seconds()
            except (TypeError, ValueError, KeyError):
                security_age = 301
            from src.pipeline.launch_execution import SECURITY_TTL_SECONDS
            security = (cached_security if isinstance(cached_security, dict)
                        and security_age <= SECURITY_TTL_SECONDS else security_probe(event))
            route = (route_probe(event) if security.get("state") == "pass"
                     else {"state": "skipped", "reason": "security gate did not pass",
                           "read_only": True})
            gate_at = _clock_value(evidence_clock)
            current_assessor = lambda candidate, s=security, r=route, at=gate_at: gate(
                candidate, s, r, now=at
            )
        result["attempted"] += 1
        try:
            _append_assessment(row["id"], event, current_assessor, assessed=0,
                               max_assessments=1, clock=evidence_clock)
            result["refreshed"] += 1
        except Exception:
            result["errors"] += 1
    return result


def view() -> dict:
    """Read-only board payload; scanning belongs to a scheduled ingestion path."""
    source_readiness = {
        "state": "blocked", "ready": False,
        "reason_codes": ["source_readiness_unavailable"],
    }
    protocol_admission = {
        "protocol_id": PROTOCOL_ID,
        "cohort_version": COHORT_VERSION,
        "protocol_start_at": PROTOCOL_START_AT,
        "state": "scheduled", "enrollment_open": False,
        "reason_codes": ["protocol_gate_not_initialized"],
        "auto_execution_allowed": False,
    }
    try:
        from src.pipeline import (
            launch_protocol_gate, solana_launch_reconcile,
            solana_launch_stream, stream_health,
        )
        health = [item for item in stream_health.snapshot()
                  if item["source"] == "solana"]
        streams = [item for item in health
                   if item["stream"] == "pump_fun_launches"]
        maintenance = next(
            (item for item in health
             if item["stream"] == solana_launch_stream.MAINTENANCE_STREAM),
            None,
        )
        protocol_admission = (
            launch_protocol_gate.read(protocol_id=PROTOCOL_ID) or protocol_admission
        )
        source_readiness = solana_launch_reconcile.source_readiness()
        reconciliation = (source_readiness.get("runtime") or {}).get(
            "reconciliation"
        )
        primary = {"available": True,
                   "qualification": solana_launch_stream.qualification_summary(
                       ledger_readback=lambda ident, mint: event_id_readback_matches(
                           ident, lane="launch", chain="solana", token=mint)),
                   "streams": streams,
                   "maintenance": maintenance,
                   "reconciliation": reconciliation,
                   "market_provider": solana_launch_stream.qualification_provider_health(),
                   "source_readiness": source_readiness,
                   "protocol_admission": protocol_admission,
                   }
    except Exception as exc:
        primary = {"available": False, "reason": str(exc)[:120],
                   "streams": [], "maintenance": None, "reconciliation": None,
                   "source_readiness": source_readiness,
                   "protocol_admission": protocol_admission}
    try:
        from src.pipeline import evm_factory_stream
        from src.pipeline.evm_launch_bridge import configured_stream_health
        evm_primary = {"available": True,
                       "qualification": evm_factory_stream.qualification_summary(
                           ledger_readback=lambda ident, chain, token:
                           event_id_readback_matches(
                               ident, lane="launch", chain=chain, token=token)),
                       "streams": configured_stream_health()}
    except Exception as exc:
        evm_primary = {"available": False, "reason": str(exc)[:120], "streams": []}
    return {"events": active("launch"),
            "research_protocol": _public_research_protocol(
                source_readiness, protocol_admission,
            ),
            "primary_sources": {"solana": primary, "evm": evm_primary},
            "source": "Launch event ledger + primary chain stream health"}


if __name__ == "__main__":
    print(json.dumps(scan(), ensure_ascii=False, indent=2))
