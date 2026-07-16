"""Frozen, reproducible Launch discovery selector contract."""
from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any, Mapping


SELECTOR_VERSION = 1
SELECTOR_RULE_ID = "launch-solana-discovery-rule-v1"
SELECTOR_PROVIDER = "dexscreener_token_pairs_v1"
MIN_LIQUIDITY_USD = 5_000.0
MAX_LIQUIDITY_USD = 2_000_000.0
MIN_FDV_USD = 10_000.0
MAX_FDV_USD = 10_000_000.0
MAX_POOL_AGE_MIN = 24 * 60.0
PROBE_MAX_AGE_MIN = 180.0
MAX_SOURCE_TO_DECISION_SECONDS = 600
MIN_BUYS_M5 = 3
MIN_FLOW_RATIO = 1.15
MIN_VOLUME_LIQUIDITY_FRACTION = 0.015
ROUTE_BUFFER_PCT = 0.60
MIN_PROBE_NOTIONAL_USD = 25.0
MAX_PROBE_NOTIONAL_USD = 500.0
MAX_POOL_LIQUIDITY_FRACTION = 0.003
IMPACT_MODEL = "constant_product_total_liquidity_half_reserve_round_2dp_v1"
SNAPSHOT_FIELDS = {
    "version", "rule_id", "provider", "pool_created_at", "liquidity_usd",
    "fdv_usd", "volume_m5_usd", "buys_m5", "sells_m5",
}
SOURCE_SNAPSHOT_VERSION = 2
SOURCE_UNIVERSE_ID = "solana-pump-fun-standard-create-logs-v1"
SOURCE_PROVIDER = (
    "solana_rpc_live_ws_plus_independent_finalized_reconciliation_v2"
)
PUMP_FUN_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
SOURCE_CAPTURE_MODE = "live_ws"
SOURCE_RECONCILIATION_STATE = "verified_live"
SOURCE_EVENT_TYPES = {"pump_fun_create", "pump_fun_createv2"}
RECONCILIATION_PROOF_FIELDS = {
    "version", "epoch_id", "from_slot", "to_slot", "status", "checked_at",
    "live_provider", "archive_provider", "genesis_hash", "evidence_hash",
    "finalized_head", "live_captured_at", "live_observation_hash",
    "archive_observation_hash", "hydration_identity_hash",
}
SOURCE_SNAPSHOT_FIELDS = {
    "version", "universe_id", "provider", "program", "signature", "slot",
    "event_type", "detected_at", "captured_at", "mint", "raw_payload_hash",
    "hydration_payload_hash", "evidence_state", "capture_mode",
    "source_provider", "reconciliation_state", "reconciled_at",
    "reconciliation_proof",
}


def _aware(value: Any, *, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{field} must be a timezone-aware timestamp") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{field} must be a timezone-aware timestamp")
    return parsed.astimezone(timezone.utc)


def _nonnegative(value: Any, *, field: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{field} must be finite and nonnegative")
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{field} must be finite and nonnegative") from exc
    if not math.isfinite(number) or number < 0:
        raise ValueError(f"{field} must be finite and nonnegative")
    return number


def _count(value: Any, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field} must be a nonnegative integer")
    return value


def _required_text(value: Any, *, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} is required")
    return value.strip()


def _lowercase_hex(value: Any, *, field: str, length: int) -> str:
    normalized = _required_text(value, field=field)
    if (len(normalized) != length
            or any(character not in "0123456789abcdef" for character in normalized)):
        raise ValueError(f"{field} must be {length} lowercase hex characters")
    return normalized


def _provider_id(value: Any, *, field: str) -> str:
    normalized = _required_text(value, field=field)
    if not normalized.startswith("solana_rpc:") or normalized == "solana_rpc:unknown":
        raise ValueError(f"{field} must identify a Solana RPC host")
    return normalized


def _provider_host(value: str) -> str:
    host = value[len("solana_rpc:"):]
    name, separator, port = host.rpartition(":")
    if separator and port.isdigit() and name:
        host = name
    if not host or host in {"unknown", ":"}:
        raise ValueError("provider host is unavailable")
    return host.lower()


def _freeze_reconciliation_proof(
        value: Any, *, slot: int, captured_at: datetime,
        raw_payload_hash: str, hydration_payload_hash: str) -> dict:
    if not isinstance(value, Mapping) or set(value) != RECONCILIATION_PROOF_FIELDS:
        raise ValueError("reconciliation proof fields mismatch")
    if value.get("version") != 1:
        raise ValueError("reconciliation proof version mismatch")
    epoch_id = _lowercase_hex(value.get("epoch_id"), field="epoch_id", length=32)
    first = _count(value.get("from_slot"), field="from_slot")
    last = _count(value.get("to_slot"), field="to_slot")
    if first > last or not first <= slot <= last:
        raise ValueError("source slot is outside reconciliation epoch")
    if value.get("status") != "sealed_clean":
        raise ValueError("reconciliation epoch is not clean")
    checked = _aware(value.get("checked_at"), field="reconciliation checked_at")
    live = _provider_id(value.get("live_provider"), field="live_provider")
    archive = _provider_id(value.get("archive_provider"), field="archive_provider")
    if _provider_host(live) == _provider_host(archive):
        raise ValueError("reconciliation providers must be independent")
    genesis = _required_text(value.get("genesis_hash"), field="genesis_hash")
    evidence = _lowercase_hex(
        value.get("evidence_hash"), field="evidence_hash", length=64,
    )
    finalized_head = _count(value.get("finalized_head"), field="finalized_head")
    if finalized_head < last:
        raise ValueError("reconciliation finalized head is before epoch end")
    live_captured = _aware(
        value.get("live_captured_at"), field="proof live_captured_at",
    )
    if live_captured != captured_at:
        raise ValueError("proof live capture clock disagrees with source")
    live_hash = _lowercase_hex(
        value.get("live_observation_hash"),
        field="live_observation_hash", length=64,
    )
    archive_hash = _lowercase_hex(
        value.get("archive_observation_hash"),
        field="archive_observation_hash", length=64,
    )
    identity_hash = _lowercase_hex(
        value.get("hydration_identity_hash"),
        field="hydration_identity_hash", length=64,
    )
    if live_hash != raw_payload_hash or archive_hash != raw_payload_hash:
        raise ValueError("source observation hashes disagree with frozen raw payload")
    if identity_hash != hydration_payload_hash:
        raise ValueError("hydration identity hash disagrees with frozen payload")
    return {
        "version": 1, "epoch_id": epoch_id, "from_slot": first,
        "to_slot": last, "status": "sealed_clean",
        "checked_at": checked.isoformat(), "live_provider": live,
        "archive_provider": archive, "genesis_hash": genesis,
        "evidence_hash": evidence, "finalized_head": finalized_head,
        "live_captured_at": live_captured.isoformat(),
        "live_observation_hash": live_hash,
        "archive_observation_hash": archive_hash,
        "hydration_identity_hash": identity_hash,
    }


def freeze_selector_snapshot(
        *, pool_created_at: str, liquidity_usd: float, fdv_usd: float,
        volume_m5_usd: float, buys_m5: int, sells_m5: int) -> dict:
    """Create the immutable source facts needed to reproduce the arm and cost."""
    created = _aware(pool_created_at, field="pool_created_at")
    return {
        "version": SELECTOR_VERSION,
        "rule_id": SELECTOR_RULE_ID,
        "provider": SELECTOR_PROVIDER,
        "pool_created_at": created.isoformat(),
        "liquidity_usd": _nonnegative(liquidity_usd, field="liquidity_usd"),
        "fdv_usd": _nonnegative(fdv_usd, field="fdv_usd"),
        "volume_m5_usd": _nonnegative(volume_m5_usd, field="volume_m5_usd"),
        "buys_m5": _count(buys_m5, field="buys_m5"),
        "sells_m5": _count(sells_m5, field="sells_m5"),
    }


def evaluate_selector_snapshot(
        value: Any, *, event_at: str, decision_at: str) -> dict:
    """Validate frozen facts and deterministically reproduce decision/cost inputs."""
    if not isinstance(value, Mapping):
        raise ValueError("selector snapshot must be a mapping")
    if set(value) != SNAPSHOT_FIELDS:
        raise ValueError("selector snapshot fields mismatch")
    if value.get("version") != SELECTOR_VERSION:
        raise ValueError("selector snapshot version mismatch")
    if value.get("rule_id") != SELECTOR_RULE_ID:
        raise ValueError("selector rule mismatch")
    if value.get("provider") != SELECTOR_PROVIDER:
        raise ValueError("selector provider mismatch")
    frozen_event = _aware(value.get("pool_created_at"), field="pool_created_at")
    row_event = _aware(event_at, field="event_at")
    decision = _aware(decision_at, field="decision_at")
    if frozen_event != row_event:
        raise ValueError("selector pool clock disagrees with event_at")
    age_min = (decision - row_event).total_seconds() / 60.0
    if not math.isfinite(age_min) or not 0 <= age_min <= MAX_POOL_AGE_MIN:
        raise ValueError("selector pool age is outside the discovery window")

    liquidity = _nonnegative(value.get("liquidity_usd"), field="liquidity_usd")
    fdv = _nonnegative(value.get("fdv_usd"), field="fdv_usd")
    volume = _nonnegative(value.get("volume_m5_usd"), field="volume_m5_usd")
    buys = _count(value.get("buys_m5"), field="buys_m5")
    sells = _count(value.get("sells_m5"), field="sells_m5")
    if not MIN_LIQUIDITY_USD <= liquidity <= MAX_LIQUIDITY_USD:
        raise ValueError("selector liquidity is outside the frozen universe")
    if not MIN_FDV_USD <= fdv <= MAX_FDV_USD:
        raise ValueError("selector FDV is outside the frozen universe")

    flow_ratio = buys / max(sells, 1)
    max_notional = round(min(
        MAX_PROBE_NOTIONAL_USD,
        max(MIN_PROBE_NOTIONAL_USD, liquidity * MAX_POOL_LIQUIDITY_FRACTION),
    ), 2)
    reserve = liquidity / 2.0
    impact_pct = round(max_notional / (reserve + max_notional) * 100.0, 2)
    modeled_route_pct = round(2 * impact_pct + ROUTE_BUFFER_PCT, 3)
    ready = (
        age_min <= PROBE_MAX_AGE_MIN
        and buys >= MIN_BUYS_M5
        and flow_ratio >= MIN_FLOW_RATIO
        and volume >= liquidity * MIN_VOLUME_LIQUIDITY_FRACTION
    )
    return {
        "decision": "SMALL_PROBE" if ready else "WATCH",
        "age_min": age_min,
        "liquidity_usd": liquidity,
        "fdv_usd": fdv,
        "volume_m5_usd": volume,
        "buys_m5": buys,
        "sells_m5": sells,
        "flow_ratio": flow_ratio,
        "max_notional_usd": max_notional,
        "modeled_route_roundtrip_pct": modeled_route_pct,
        "impact_model": IMPACT_MODEL,
    }


def freeze_source_snapshot(
        *, signature: str, slot: int, event_type: str, detected_at: str,
        captured_at: str, decision_at: str, mint: str, raw_payload_hash: str,
        hydration_payload_hash: str, capture_mode: str, source_provider: str,
        reconciliation_state: str, reconciled_at: str,
        reconciliation_proof: Mapping[str, Any]) -> dict:
    """Freeze the primary Pump.fun universe evidence inside the entry snapshot."""
    signature = _required_text(signature, field="source signature")
    if isinstance(slot, bool) or not isinstance(slot, int) or slot < 0:
        raise ValueError("source slot must be a nonnegative integer")
    if not isinstance(event_type, str) or event_type not in SOURCE_EVENT_TYPES:
        raise ValueError("source event type is not a Pump.fun create event")
    mint = _required_text(mint, field="source mint")
    raw_payload_hash = _lowercase_hex(
        raw_payload_hash, field="raw_payload_hash", length=64,
    )
    hydration_payload_hash = _lowercase_hex(
        hydration_payload_hash, field="hydration_payload_hash", length=64,
    )
    if capture_mode != SOURCE_CAPTURE_MODE:
        raise ValueError("source capture mode is not live websocket")
    if reconciliation_state != SOURCE_RECONCILIATION_STATE:
        raise ValueError("source was not verified against independent finality")
    source_provider = _provider_id(source_provider, field="source_provider")
    detected = _aware(detected_at, field="source detected_at")
    captured = _aware(captured_at, field="source captured_at")
    decision = _aware(decision_at, field="source decision_at")
    reconciled = _aware(reconciled_at, field="source reconciled_at")
    if detected != captured:
        raise ValueError("source detection clock disagrees with live capture")
    proof = _freeze_reconciliation_proof(
        reconciliation_proof, slot=slot, captured_at=captured,
        raw_payload_hash=raw_payload_hash,
        hydration_payload_hash=hydration_payload_hash,
    )
    if source_provider != proof["live_provider"]:
        raise ValueError("source provider disagrees with reconciliation proof")
    if reconciled != _aware(proof["checked_at"], field="proof checked_at"):
        raise ValueError("source reconciliation clock disagrees with proof")
    if not detected <= reconciled <= decision:
        raise ValueError("source proof must exist between detection and decision")
    if (decision - detected).total_seconds() > MAX_SOURCE_TO_DECISION_SECONDS:
        raise ValueError("source-to-decision deadline exceeded")
    return {
        "version": SOURCE_SNAPSHOT_VERSION,
        "universe_id": SOURCE_UNIVERSE_ID,
        "provider": SOURCE_PROVIDER,
        "program": PUMP_FUN_PROGRAM,
        "signature": signature,
        "slot": slot,
        "event_type": event_type,
        "detected_at": detected.isoformat(),
        "captured_at": captured.isoformat(),
        "mint": mint,
        "raw_payload_hash": raw_payload_hash,
        "hydration_payload_hash": hydration_payload_hash,
        "evidence_state": "complete",
        "capture_mode": SOURCE_CAPTURE_MODE,
        "source_provider": source_provider,
        "reconciliation_state": SOURCE_RECONCILIATION_STATE,
        "reconciled_at": reconciled.isoformat(),
        "reconciliation_proof": proof,
    }


def validate_source_snapshot(
        value: Any, *, token: str, detected_at: str, decision_at: str) -> dict:
    """Revalidate exact frozen source facts without trusting a cohort label."""
    if not isinstance(value, Mapping) or set(value) != SOURCE_SNAPSHOT_FIELDS:
        raise ValueError("source snapshot fields mismatch")
    normalized = freeze_source_snapshot(
        signature=value.get("signature"), slot=value.get("slot"),
        event_type=value.get("event_type"), detected_at=value.get("detected_at"),
        captured_at=value.get("captured_at"),
        decision_at=decision_at,
        mint=value.get("mint"), raw_payload_hash=value.get("raw_payload_hash"),
        hydration_payload_hash=value.get("hydration_payload_hash"),
        capture_mode=value.get("capture_mode"),
        source_provider=value.get("source_provider"),
        reconciliation_state=value.get("reconciliation_state"),
        reconciled_at=value.get("reconciled_at"),
        reconciliation_proof=value.get("reconciliation_proof"),
    )
    if value != normalized:
        raise ValueError("source snapshot constants mismatch")
    if normalized["mint"] != token:
        raise ValueError("source snapshot mint mismatch")
    if _aware(detected_at, field="row detected_at") != _aware(
            normalized["detected_at"], field="source detected_at"):
        raise ValueError("source snapshot detection clock mismatch")
    return normalized
