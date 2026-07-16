"""Conservative identity rules for EVM factory events entering Launch Radar."""
from __future__ import annotations

import re
from datetime import datetime, timezone

from src.pipeline import evm_factory_stream, stream_health


QUOTE_ASSETS = {
    "bsc": {
        "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c",  # WBNB
        "0x55d398326f99059ff775485246999027b3197955",  # USDT
        "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d",  # USDC
        "0xe9e7cea3dedca5984780bafc599bd69add087d56",  # BUSD
        "0x2170ed0880ac9a755fd29b2688956bd959f933f8",  # ETH
        "0x7130d2a12b9bcbfae4f2634d864a1ee1ce3ead9c",  # BTCB
    },
    "base": {
        "0x4200000000000000000000000000000000000006",  # WETH
        "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",  # USDC
        "0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca",  # USDbC
    },
    "ethereum": {
        "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",  # WETH
        "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",  # USDC
        "0xdac17f958d2ee523a2206206994597c13d831ec7",  # USDT
        "0x6b175474e89094c44da98b954eedeac495271d0f",  # DAI
    },
}


def identify_target(row: dict) -> tuple[str | None, str | None]:
    """Return the non-quote token only when factory identity is unambiguous."""
    quotes = QUOTE_ASSETS.get(row.get("chain"), set())
    token0, token1 = str(row.get("token0", "")).lower(), str(row.get("token1", "")).lower()
    known = (token0 in quotes, token1 in quotes)
    if known == (True, True):
        return None, "unsupported_quote_pair"
    if known == (False, False):
        return None, "ambiguous_target"
    return (token1 if known[0] else token0), None


def exact_pair(row: dict, target: str, payload: object) -> dict | None:
    """Match the factory-emitted pool, never a deeper pre-existing token pool."""
    if not isinstance(payload, list):
        raise ValueError("DEX pool response is not a list")
    chain, pool = row["chain"], row["pool"].lower()
    quotes = QUOTE_ASSETS.get(chain, set())
    for pair in payload:
        if not isinstance(pair, dict):
            continue
        base = (pair.get("baseToken") or {}).get("address")
        quote = (pair.get("quoteToken") or {}).get("address")
        if (str(pair.get("chainId", "")).lower() == chain
                and str(pair.get("pairAddress", "")).lower() == pool
                and str(base or "").lower() == target.lower()
                and str(quote or "").lower() in quotes):
            return pair
    return None


def _utc(value: object) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed.astimezone(timezone.utc) if parsed.tzinfo is not None else None


def _safe_code(value: object, fallback: str) -> str:
    return (value if isinstance(value, str)
            and re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,79}", value)
            else fallback)


def configured_stream_health(*, now: datetime | None = None) -> list[dict]:
    """Return transport plus independently proved finalized factory coverage.

    Continuous block heads only prove that the websocket is moving.  A configured
    stream is never reported live unless its exact factory/topic range is also
    current through an independent HTTP provider.
    """
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        raise ValueError("stream-health clock must include a timezone")
    current = current.astimezone(timezone.utc)
    observed = {(item["source"], item["stream"]): item
                for item in stream_health.snapshot(now=current)}
    out = []
    for spec in evm_factory_stream.configured_specs():
        item = observed.get((spec.chain, spec.stream))
        if item is None:
            item = {"source": spec.chain, "stream": spec.stream, "status": "missing",
                    "stale": True, "open_gaps": 0, "last_received_at": None,
                    "last_error": "no stream observation"}
        coverage = evm_factory_stream.coverage_snapshot(spec)
        coverage_health = observed.get(
            (spec.chain, evm_factory_stream.coverage_stream(spec))
        )
        details = (
            coverage_health.get("details")
            if isinstance(coverage_health, dict) else None
        )
        details = dict(details) if isinstance(details, dict) else {}
        for identity_field in ("ws_provider_id", "http_provider_id"):
            identity = details.get(identity_field)
            if (not isinstance(identity, str)
                    or re.fullmatch(r"provider:[0-9a-f]{64}", identity) is None):
                details[identity_field] = None
        if coverage_health is not None:
            allowed_detail_fields = {
                "schema_version", "state", "chain", "venue", "factory",
                "coverage_started_block", "verified_through_block",
                "verified_through_hash", "safe_head_block", "safe_head_hash",
                "safe_head_at", "audit_duration_ms", "lag_blocks", "verified_at",
                "ws_provider_id", "http_provider_id", "provider_independent",
                "connection_generation", "consecutive_failures", "next_retry_at",
                "last_error_kind",
            }
            details = {key: value for key, value in details.items()
                       if key in allowed_detail_fields}
            details["state"] = _safe_code(
                details.get("state"), "invalid_coverage_state",
            )
            for context_field, expected in (
                ("chain", spec.chain), ("venue", spec.venue),
                ("factory", spec.address),
            ):
                if details.get(context_field) != expected:
                    details[context_field] = None
            for numeric_field in (
                "schema_version", "coverage_started_block",
                "verified_through_block", "safe_head_block",
                "audit_duration_ms", "lag_blocks", "consecutive_failures",
            ):
                if type(details.get(numeric_field)) is not int:
                    details[numeric_field] = None
            for hash_field in ("verified_through_hash", "safe_head_hash"):
                value = details.get(hash_field)
                if (not isinstance(value, str)
                        or re.fullmatch(r"0x[0-9a-f]{64}", value) is None):
                    details[hash_field] = None
            for time_field in ("safe_head_at", "verified_at", "next_retry_at"):
                value = details.get(time_field)
                if value is not None:
                    parsed = _utc(value)
                    details[time_field] = parsed.isoformat() if parsed else None
            generation = details.get("connection_generation")
            if (not isinstance(generation, str)
                    or re.fullmatch(r"[0-9a-f]{32}", generation) is None):
                details["connection_generation"] = None
            if type(details.get("provider_independent")) is not bool:
                details["provider_independent"] = False
            details["last_error_kind"] = _safe_code(
                details.get("last_error_kind"), "coverage_worker_error",
            ) if details.get("last_error_kind") is not None else None
            coverage_health = {
                key: coverage_health.get(key) for key in (
                    "source", "stream", "last_received_at", "updated_at",
                    "status", "age_seconds", "stale",
                )
            } | {
                "last_error": _safe_code(
                    details.get("last_error_kind"), "coverage_worker_error",
                ) if coverage_health.get("last_error") else None,
                "details": details,
            }
        max_age = evm_factory_stream.FINALIZED_HEAD_MAX_AGE_SECONDS.get(spec.chain)
        max_lag = evm_factory_stream.FINALIZED_HEAD_MAX_LAG_BLOCKS.get(spec.chain)
        safe_at = _utc(coverage.get("safe_head_at"))
        verified_at = _utc(coverage.get("verified_at"))
        event_at = _utc(item.get("last_event_at"))
        safe_head_age = (
            (current - safe_at).total_seconds() if safe_at is not None else None
        )
        event_time_lag = (
            (event_at - safe_at).total_seconds()
            if event_at is not None and safe_at is not None else None
        )
        try:
            if (type(item["cursor"]) is not int
                    or type(coverage["safe_head_block"]) is not int):
                raise ValueError("non-integer finality cursor")
            transport_cursor = item["cursor"]
            safe_head = coverage["safe_head_block"]
            tip_lag = transport_cursor - safe_head
        except (KeyError, TypeError, ValueError, OverflowError):
            tip_lag = None
        try:
            if type(coverage["audit_duration_ms"]) is not int:
                raise ValueError("non-integer audit duration")
            audit_duration = coverage["audit_duration_ms"]
        except (KeyError, TypeError, ValueError, OverflowError):
            audit_duration = None
        health_matches_proof = bool(
            details.get("schema_version") == 2
            and details.get("state") == "verified"
            and details.get("chain") == spec.chain
            and details.get("venue") == spec.venue
            and details.get("factory") == spec.address
            and details.get("provider_independent") is True
            and isinstance(details.get("connection_generation"), str)
            and re.fullmatch(
                r"[0-9a-f]{32}", details.get("connection_generation", ""),
            ) is not None
            and details.get("ws_provider_id") == coverage.get("ws_provider_id")
            and details.get("http_provider_id") == coverage.get("http_provider_id")
            and details.get("safe_head_hash") == coverage.get("safe_head_hash")
            and _utc(details.get("safe_head_at")) == safe_at
            and details.get("coverage_started_block")
                == coverage.get("coverage_started_block")
            and details.get("verified_through_block")
                == coverage.get("verified_through_block")
            and details.get("verified_through_hash")
                == coverage.get("verified_through_hash")
            and details.get("safe_head_block") == coverage.get("safe_head_block")
            and details.get("audit_duration_ms") == coverage.get("audit_duration_ms")
            and details.get("lag_blocks") == coverage.get("lag_blocks")
            and details.get("verified_at") == coverage.get("verified_at")
        )
        freshness_ready = bool(
            max_age is not None and safe_head_age is not None
            and 0 <= safe_head_age <= max_age
            and event_time_lag is not None
            and 0 <= event_time_lag <= max_age
            and verified_at is not None and safe_at is not None
            and safe_at <= verified_at <= current
            and audit_duration is not None
            and 0 <= audit_duration
                <= evm_factory_stream.MAX_COVERAGE_AUDIT_DURATION_MS
        )
        alignment_ready = bool(
            max_lag is not None and tip_lag is not None
            and 0 <= tip_lag <= max_lag
        )
        coverage_ready = bool(
            coverage.get("state") == "verified"
            and coverage.get("provider_independent") is True
            and coverage.get("lag_blocks") == 0
            and coverage_health is not None
            and coverage_health.get("status") == "live"
            and coverage_health.get("stale") is not True
            and health_matches_proof
            and freshness_ready
            and alignment_ready
        )
        coverage_gate_error = None
        if not coverage_ready:
            if coverage.get("state") != "verified":
                coverage_gate_error = _safe_code(
                    coverage.get("last_error_kind"), "invalid_coverage_proof",
                )
            elif not health_matches_proof:
                coverage_gate_error = "coverage_generation_or_proof_mismatch"
            elif (safe_head_age is None or max_age is None
                  or not 0 <= safe_head_age <= max_age
                  or event_time_lag is None
                  or not 0 <= event_time_lag <= max_age
                  or verified_at is None or safe_at is None
                  or not safe_at <= verified_at <= current):
                coverage_gate_error = "finalized_head_stale"
            elif (audit_duration is None or audit_duration < 0
                  or audit_duration
                  > evm_factory_stream.MAX_COVERAGE_AUDIT_DURATION_MS):
                coverage_gate_error = "coverage_audit_slow"
            elif not alignment_ready:
                coverage_gate_error = "finality_block_lag_exceeded"
            elif coverage_health is None or coverage_health.get("status") != "live":
                coverage_gate_error = "coverage_worker_not_live"
            else:
                coverage_gate_error = "finalized_coverage_unverified"
        transport_status = item.get("status") or "missing"
        if transport_status not in {
            "live", "degraded", "disconnected", "stale", "missing",
        }:
            transport_status = "unknown"
        combined_status = transport_status
        combined_error = (
            None if transport_status == "live"
            else _safe_code(
                f"transport_{transport_status}", "transport_unavailable",
            )
        )
        if transport_status == "live" and not coverage_ready:
            combined_status = "degraded"
            combined_error = coverage_gate_error
        out.append({
            **{key: item.get(key) for key in (
                "source", "stream", "cursor", "last_event_at",
                "last_received_at", "latency_ms", "updated_at", "age_seconds",
                "open_gaps", "deferred_gaps", "next_gap_retry_at", "stale",
            )},
            "status": combined_status,
            "last_error": combined_error,
            "transport_status": transport_status,
            "chain": spec.chain,
            "venue": spec.venue,
            "factory": spec.address,
            "coverage_verified": coverage_ready,
            "coverage": coverage,
            "coverage_health": coverage_health,
            "coverage_gate_error": coverage_gate_error,
            "safe_head_age_seconds": (
                round(safe_head_age, 1) if safe_head_age is not None else None
            ),
            "max_safe_head_age_seconds": max_age,
            "tip_to_finalized_lag_blocks": tip_lag,
            "max_tip_to_finalized_lag_blocks": max_lag,
            "finalized_event_time_lag_seconds": (
                round(event_time_lag, 1) if event_time_lag is not None else None
            ),
            "audit_duration_ms": audit_duration,
            "max_audit_duration_ms": (
                evm_factory_stream.MAX_COVERAGE_AUDIT_DURATION_MS
            ),
        })
    return out
