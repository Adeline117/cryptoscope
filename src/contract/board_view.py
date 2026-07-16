"""Fail-closed contract for public board JSON views.

The ledger and lane read models remain the source of truth for actionability.  This
module guards the final serialization boundary: contradictory or non-JSON data must
never overwrite the last known-good public view.
"""
from __future__ import annotations

import json
import math
from datetime import datetime, timedelta, timezone
from typing import Any, Literal, Mapping
from urllib.parse import urlparse

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator


class BoardViewContractError(ValueError):
    """Raised before any public view is written when its contract is invalid."""


class BoardEnvelope(BaseModel):
    """Common clock and schema fields shared by every board view."""

    model_config = ConfigDict(extra="allow")

    schema_version: Literal[1]
    view: str = Field(min_length=1)
    generated_at: AwareDatetime
    refresh_cadence_min: float = Field(gt=0)
    freshness_grace_min: float = Field(ge=0)
    next_expected_at: AwareDatetime
    stale_after_at: AwareDatetime

    @model_validator(mode="after")
    def clocks_are_ordered(self) -> "BoardEnvelope":
        if not self.generated_at < self.next_expected_at <= self.stale_after_at:
            raise ValueError("board clocks must satisfy generated < next <= stale")
        return self


_CANONICAL_EVENT_COLLECTIONS = {
    "launch": ("events", "launch", True),
    "structure": ("events", "structure", True),
    "airdrop": ("events", "airdrop", True),
    "perps": ("cascade_events", "cascade", True),
}
_ACTION_LEVELS = {
    "A0_BLOCKED", "A1_WATCH", "A2_PAPER_READY",
    "A3_MANUAL_PROBE", "A4_REAL_FILL_VALIDATED",
}
_ENROLLMENT_STATES = {"scheduled", "armed", "open", "breached", "blocked"}
_PROTOCOL_IDENTITY_FIELDS = ("protocol_id", "cohort_version", "protocol_start_at")
_ADMISSION_SAFETY_FIELDS = (
    "state", "enrollment_open", "armed_at", "opened_at", "breached_at",
    "auto_execution_allowed",
)
_STRUCTURE_EVENT_TYPES = {
    "instrument_inventory_addition", "legacy_inventory_delta",
}
_STRUCTURE_INSTRUMENT_CLASSES = {
    "crypto_asset", "tokenized_equity", "tokenized_etf",
    "tokenized_equity_or_etf", "tokenized_commodity", "tokenized_forex",
    "tokenized_bond", "unclassified_spot", "mixed",
}
_STRUCTURE_TAXONOMY_FIELDS = {
    "okx": frozenset({"instCategory"}),
    "coinbase": frozenset({"product_type"}),
}
_STRUCTURE_SCHEDULE_FIELDS = {
    "okx": frozenset({"contTdSwTime", "listTime"}),
    "bybit": frozenset({"launchTime"}),
}
_RUNTIME_SAFETY_REASON_CODES = frozenset({
    "storage_pressure_warn", "storage_pressure_critical",
    "solana_streams_unhealthy", "solana_maintenance_unhealthy",
    "evm_streams_unhealthy",
    "hyperliquid_raw_trade_retention_shed",
    "runtime_health_unavailable",
})
_PERP_IDENTITY_CACHE_TTL_SECONDS = 26 * 60 * 60
_PERP_IDENTITY_STATUSES = frozenset({
    "verified", "research_only", "blocked", "invalid", "stale", "unavailable",
})
_PERP_IDENTITY_REASON_CODES = frozenset({
    "heuristic_mapping_not_actionable",
    "identity_collection_blocked",
    "identity_cache_invalid",
    "identity_cache_stale",
    "identity_cache_unavailable",
    "identity_projection_invalid",
    "identity_load_failed",
})
_PERP_IDENTITY_MAX_ROWS = 20_000
_PERP_IDENTITY_MAX_MARKETS = 100_000
_PERP_IDENTITY_MAX_SOURCES = 64


def _reason_codes(value: Any, *, path: str, required: bool) -> list[str]:
    if (not isinstance(value, list)
            or any(not isinstance(item, str) or not item.strip() for item in value)):
        raise BoardViewContractError(f"{path} must be a string array")
    if required and not value:
        raise BoardViewContractError(f"{path} must explain the blocked state")
    return value


def _required_text(value: Any, *, path: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise BoardViewContractError(f"{path} must be a non-empty string")
    return value.strip()


def _launch_protocol_join_member(view: str, payload: Any) -> dict | None:
    """Project only immutable identity and admission safety for cross-view joins."""
    if not isinstance(payload, Mapping):
        return None
    try:
        if view == "launch":
            identity_source = payload["research_protocol"]
            admission = payload["primary_sources"]["solana"]["protocol_admission"]
        elif view == "stats":
            validation = payload["lanes"]["launch"]["edge_validation"]
            identity_source = validation
            admission = validation["protocol_admission"]
        else:
            raise ValueError(f"unsupported protocol join view: {view}")
        if not isinstance(identity_source, Mapping) or not isinstance(admission, Mapping):
            return None
        identity = {field: identity_source.get(field) for field in _PROTOCOL_IDENTITY_FIELDS}
        safety = {field: admission.get(field) for field in _ADMISSION_SAFETY_FIELDS}
        if (not isinstance(identity["protocol_id"], str)
                or isinstance(identity["cohort_version"], bool)
                or not isinstance(identity["cohort_version"], int)
                or not isinstance(identity["protocol_start_at"], str)
                or safety["state"] not in {"scheduled", "armed", "open", "breached"}
                or not isinstance(safety["enrollment_open"], bool)
                or safety["auto_execution_allowed"] is not False
                or any(admission.get(field) != value
                       or type(admission.get(field)) is not type(value)
                       for field, value in identity.items())):
            return None
        return {
            "view": view,
            "generated_at": payload.get("generated_at"),
            "identity": identity,
            "admission_updated_at": admission.get("updated_at"),
            "admission": safety,
        }
    except (KeyError, TypeError):
        return None


def _legal_admission_transition(older: Mapping[str, Any],
                                newer: Mapping[str, Any]) -> bool:
    """Match the persistent gate, including its pre-start readiness reset."""
    rank = {"scheduled": 0, "armed": 1, "open": 2, "breached": 3}
    older_state = older["admission"]["state"]
    newer_state = newer["admission"]["state"]
    if rank[newer_state] >= rank[older_state]:
        return True
    if older_state != "armed" or newer_state != "scheduled":
        return False
    try:
        transition_at = _aware_clock(
            newer["admission_updated_at"], path="newer admission updated_at",
        )
        protocol_start_at = _aware_clock(
            newer["identity"]["protocol_start_at"], path="protocol start_at",
        )
    except BoardViewContractError:
        return False
    return transition_at < protocol_start_at


def launch_protocol_join(launch: Any, stats: Any) -> dict:
    """Build a fail-closed certificate for independently cached Launch views."""
    members = {
        "launch": _launch_protocol_join_member("launch", launch),
        "stats": _launch_protocol_join_member("stats", stats),
    }
    missing = [name for name, member in members.items() if member is None]
    if missing:
        state, reasons = "incomplete", [f"{name}_protocol_projection_missing" for name in missing]
    elif members["launch"]["identity"] != members["stats"]["identity"]:
        state, reasons = "identity_mismatch", ["launch_stats_protocol_identity_mismatch"]
    elif members["launch"]["admission"] == members["stats"]["admission"]:
        state, reasons = "consistent", []
    else:
        left, right = members["launch"], members["stats"]
        left_state, right_state = left["admission"]["state"], right["admission"]["state"]
        if left_state == right_state:
            state, reasons = "contradiction", ["same_state_safety_projection_mismatch"]
        else:
            try:
                left_clock = _aware_clock(
                    left["admission_updated_at"], path="launch admission updated_at",
                )
                right_clock = _aware_clock(
                    right["admission_updated_at"], path="stats admission updated_at",
                )
            except BoardViewContractError:
                left_clock = right_clock = None
            if left_clock is None or right_clock is None or left_clock == right_clock:
                state, reasons = "contradiction", ["admission_state_clock_ambiguous"]
            else:
                older, newer = ((left, right) if left_clock < right_clock else (right, left))
                if not _legal_admission_transition(older, newer):
                    state, reasons = "contradiction", ["admission_state_regressed"]
                else:
                    state, reasons = "sync_pending", ["admission_state_not_yet_joined"]
    return {
        "version": 1,
        "state": state,
        "cross_view_edge_usable": state == "consistent",
        "reason_codes": reasons,
        "members": members,
    }


def _finite_nonnegative(value: Any, *, path: str, optional: bool = False) -> float | None:
    if value is None and optional:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise BoardViewContractError(f"{path} must be a nonnegative number")
    number = float(value)
    if not math.isfinite(number) or number < 0:
        raise BoardViewContractError(f"{path} must be a nonnegative number")
    return number


def _provider_host(value: Any, *, path: str) -> str:
    if not isinstance(value, str) or not value.startswith("solana_rpc:"):
        raise BoardViewContractError(f"{path} must identify a Solana RPC provider")
    host = value[len("solana_rpc:"):].strip().lower()
    name, separator, port = host.rpartition(":")
    if separator and name and port.isdigit():
        host = name
    if not host:
        raise BoardViewContractError(f"{path} has no provider host")
    return host


def _validate_source_readiness(value: Any, *, path: str) -> dict:
    if not isinstance(value, Mapping):
        raise BoardViewContractError(f"{path} must be an object")
    state, ready = value.get("state"), value.get("ready")
    if state not in {"ready", "blocked"} or not isinstance(ready, bool):
        raise BoardViewContractError(f"{path} has an invalid readiness state")
    if ready is not (state == "ready"):
        raise BoardViewContractError(f"{path}.ready contradicts state")
    readiness_reasons = _reason_codes(
        value.get("reason_codes"), path=f"{path}.reason_codes", required=not ready,
    )
    if ready and readiness_reasons:
        raise BoardViewContractError(f"{path}.reason_codes must be empty while ready")
    required = _exact_nonnegative_int(
        value.get("required_clean_epochs"), path=f"{path}.required_clean_epochs",
    )
    observed = _exact_nonnegative_int(
        value.get("observed_epochs"), path=f"{path}.observed_epochs",
    )
    if required < 1:
        raise BoardViewContractError(f"{path}.required_clean_epochs must be positive")
    max_age = _finite_nonnegative(value.get("max_age_seconds"),
                                  path=f"{path}.max_age_seconds")
    if max_age == 0:
        raise BoardViewContractError(f"{path}.max_age_seconds must be positive")
    age = _finite_nonnegative(value.get("latest_age_seconds"),
                              path=f"{path}.latest_age_seconds", optional=True)
    max_lag = _exact_nonnegative_int(
        value.get("max_finalized_lag_slots"),
        path=f"{path}.max_finalized_lag_slots",
    )
    sealed_lag = value.get("latest_sealed_lag_slots")
    runtime_lag = value.get("latest_runtime_lag_slots")
    if sealed_lag is not None:
        sealed_lag = _exact_nonnegative_int(
            sealed_lag, path=f"{path}.latest_sealed_lag_slots",
        )
    if runtime_lag is not None:
        runtime_lag = _exact_nonnegative_int(
            runtime_lag, path=f"{path}.latest_runtime_lag_slots",
        )
    runtime = value.get("runtime")
    if not isinstance(runtime, Mapping) or "live" not in runtime or "maintenance" not in runtime:
        raise BoardViewContractError(f"{path}.runtime must expose live and maintenance")
    epoch = value.get("latest_epoch")
    if epoch is not None:
        if not isinstance(epoch, Mapping):
            raise BoardViewContractError(f"{path}.latest_epoch must be an object or null")
        for field in ("epoch_id", "live_provider", "archive_provider", "checked_at"):
            if not isinstance(epoch.get(field), str) or not epoch.get(field, "").strip():
                raise BoardViewContractError(f"{path}.latest_epoch.{field} is required")
        first = _exact_nonnegative_int(epoch.get("from_slot"),
                                       path=f"{path}.latest_epoch.from_slot")
        last = _exact_nonnegative_int(epoch.get("to_slot"),
                                      path=f"{path}.latest_epoch.to_slot")
        if first > last:
            raise BoardViewContractError(f"{path}.latest_epoch slot range is invalid")
        _exact_nonnegative_int(epoch.get("missing_live"),
                               path=f"{path}.latest_epoch.missing_live")
        _exact_nonnegative_int(epoch.get("extra_live"),
                               path=f"{path}.latest_epoch.extra_live")
        _exact_nonnegative_int(epoch.get("finalized_head"),
                               path=f"{path}.latest_epoch.finalized_head")
        if epoch.get("status") not in {"sealed_clean", "sealed_breached"}:
            raise BoardViewContractError(f"{path}.latest_epoch.status is invalid")
        _aware_clock(epoch.get("checked_at"), path=f"{path}.latest_epoch.checked_at")
    if ready:
        if observed < required or age is None or age > max_age:
            raise BoardViewContractError(f"{path} lacks a fresh complete burn-in")
        if sealed_lag is None or runtime_lag is None \
                or sealed_lag > max_lag or runtime_lag > max_lag:
            raise BoardViewContractError(f"{path} exceeds the finalized slot lag policy")
        live_provider = value.get("live_provider")
        archive_provider = value.get("archive_provider")
        if (_provider_host(live_provider, path=f"{path}.live_provider")
                == _provider_host(archive_provider, path=f"{path}.archive_provider")):
            raise BoardViewContractError(f"{path} providers are not independent")
        if (not isinstance(epoch, Mapping) or epoch.get("status") != "sealed_clean"
                or epoch.get("missing_live") != 0 or epoch.get("extra_live") != 0
                or epoch.get("live_provider") != live_provider
                or epoch.get("archive_provider") != archive_provider):
            raise BoardViewContractError(f"{path}.latest_epoch is not clean and bound")
        for stream_name in ("live", "maintenance"):
            stream = runtime.get(stream_name)
            if (not isinstance(stream, Mapping) or stream.get("status") != "live"
                    or stream.get("open_gaps") != 0):
                raise BoardViewContractError(
                    f"{path}.runtime.{stream_name} is not live and gap-free"
                )
    return dict(value)


def _validate_protocol_admission(value: Any, *, path: str) -> dict:
    if not isinstance(value, Mapping):
        raise BoardViewContractError(f"{path} must be an object")
    from src.contract.launch_protocol import (
        COHORT_VERSION, PROTOCOL_ID, PROTOCOL_START_AT,
    )

    for field, expected in (
        ("protocol_id", PROTOCOL_ID), ("cohort_version", COHORT_VERSION),
        ("protocol_start_at", PROTOCOL_START_AT),
    ):
        if value.get(field) != expected or type(value.get(field)) is not type(expected):
            raise BoardViewContractError(f"{path}.{field} violates protocol identity")
    state = value.get("state")
    if state not in {"scheduled", "armed", "open", "breached"}:
        raise BoardViewContractError(f"{path}.state is invalid")
    if value.get("enrollment_open") is not (state == "open"):
        raise BoardViewContractError(f"{path}.enrollment_open contradicts state")
    if value.get("auto_execution_allowed") is not False:
        raise BoardViewContractError(f"{path}.auto_execution_allowed must be false")
    reasons = _reason_codes(
        value.get("reason_codes"), path=f"{path}.reason_codes", required=state != "open",
    )
    if state == "open" and reasons:
        raise BoardViewContractError(f"{path}.reason_codes must be empty while open")
    required_clock = {"armed": "armed_at", "open": "opened_at", "breached": "breached_at"}
    if state in required_clock:
        _aware_clock(value.get(required_clock[state]), path=f"{path}.{required_clock[state]}")
    for field in ("created_at", "updated_at"):
        if value.get(field) is not None:
            _aware_clock(value.get(field), path=f"{path}.{field}")
    return dict(value)


def _validate_research_protocol(value: Any, *, readiness: Mapping[str, Any],
                                admission: Mapping[str, Any], generated_at: datetime,
                                path: str) -> dict:
    if not isinstance(value, Mapping):
        raise BoardViewContractError(f"{path} must be an object")
    from src.contract.launch_protocol import (
        COHORT_VERSION, PROTOCOL_ID, PROTOCOL_START_AT,
    )

    exact = {
        "protocol_id": PROTOCOL_ID, "cohort_version": COHORT_VERSION,
        "protocol_start_at": PROTOCOL_START_AT,
        "persistent_admission_state": admission["state"],
        "source_readiness_state": readiness["state"],
        "sample_kind": "forward_paper_selector",
        "selection_stage": "discovery_rule_before_security_and_route",
        "real_edge_n": 0, "real_edge_eligible": False,
        "execution_edge_eligible": False, "auto_execution_allowed": False,
    }
    for field, expected in exact.items():
        if value.get(field) != expected or type(value.get(field)) is not type(expected):
            raise BoardViewContractError(f"{path}.{field} violates public protocol truth")
    state = value.get("enrollment_state")
    if state not in _ENROLLMENT_STATES:
        raise BoardViewContractError(f"{path}.enrollment_state is invalid")
    effective_open = readiness["ready"] is True and admission["state"] == "open"
    if value.get("enrollment_open") is not effective_open:
        raise BoardViewContractError(f"{path}.enrollment_open contradicts source/admission")
    expected_state = (
        "open" if effective_open else
        "breached" if admission["state"] == "breached" else
        admission["state"] if admission["state"] in {"scheduled", "armed"} else
        "blocked"
    )
    if state != expected_state:
        raise BoardViewContractError(
            f"{path}.enrollment_state must be {expected_state!r}"
        )
    if effective_open and generated_at < datetime.fromisoformat(PROTOCOL_START_AT):
        raise BoardViewContractError(f"{path} cannot open before protocol_start_at")
    research_reasons = _reason_codes(
        value.get("reason_codes"), path=f"{path}.reason_codes",
        required=not effective_open,
    )
    if effective_open and value.get("reason_codes"):
        raise BoardViewContractError(f"{path}.reason_codes must be empty while open")
    for reason in [
        *(readiness.get("reason_codes") or []),
        *(admission.get("reason_codes") or []),
    ]:
        if reason not in research_reasons:
            raise BoardViewContractError(f"{path}.reason_codes hides {reason!r}")
    return dict(value)


def _validate_current_launch_source(row: Mapping[str, Any], *, path: str) -> None:
    from src.pipeline.edge_validation import (
        COHORT_VERSION, is_protocol_enrollment_candidate,
    )

    candidate = dict(row)
    if not is_protocol_enrollment_candidate(candidate):
        return
    if candidate.get("cohort_version") != COHORT_VERSION:
        raise BoardViewContractError(f"{path}.cohort_version escaped current protocol")
    entry = candidate.get("entry_observation")
    if not isinstance(entry, Mapping):
        raise BoardViewContractError(f"{path}.entry_observation is required for v6")
    try:
        from src.contract.launch_selector import validate_source_snapshot

        validate_source_snapshot(
            entry.get("source_snapshot"), token=candidate.get("token"),
            detected_at=candidate.get("detected_at"),
            decision_at=candidate.get("decision_at"),
        )
    except (TypeError, ValueError, KeyError, OverflowError) as exc:
        raise BoardViewContractError(
            f"{path}.entry_observation.source_snapshot is invalid: {exc}"
        ) from exc


def _validate_launch_view(payload: Mapping[str, Any], *, generated_at: datetime) -> None:
    primary_sources = payload.get("primary_sources")
    if not isinstance(primary_sources, Mapping):
        raise BoardViewContractError("launch.primary_sources must be an object")
    solana = primary_sources.get("solana")
    if not isinstance(solana, Mapping):
        raise BoardViewContractError("launch.primary_sources.solana must be an object")
    readiness = _validate_source_readiness(
        solana.get("source_readiness"), path="launch.primary_sources.solana.source_readiness",
    )
    admission = _validate_protocol_admission(
        solana.get("protocol_admission"), path="launch.primary_sources.solana.protocol_admission",
    )
    research = _validate_research_protocol(
        payload.get("research_protocol"), readiness=readiness, admission=admission,
        generated_at=generated_at, path="launch.research_protocol",
    )
    rows = payload.get("events")
    if not isinstance(rows, list):
        raise BoardViewContractError("launch.events must be a list")
    for index, row in enumerate(rows):
        if not isinstance(row, Mapping):
            continue
        path = f"launch.events[{index}]"
        _validate_current_launch_source(row, path=path)
        if research["enrollment_open"] is not True and (
            row.get("action_level") == "A3_MANUAL_PROBE"
            or row.get("actionable_now") is True
            or row.get("effective_decision") == "SMALL_PROBE"
        ):
            raise BoardViewContractError(
                f"{path} cannot be actionable while protocol enrollment is blocked"
            )


def _exact_nonnegative_int(value: Any, *, path: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise BoardViewContractError(f"{path} must be a nonnegative integer")
    return value


def _validate_launch_stats(value: Any, *, path: str) -> None:
    """Prevent descriptive or real-execution claims from entering the edge card."""
    if not isinstance(value, Mapping):
        raise BoardViewContractError(f"{path} must be an object")
    from src.pipeline.edge_validation import (
        COHORT_VERSION, LOOK_ALPHA, LOOK_SIZES, MIN_MEAN_UTILITY_LIFT,
        MIN_OUTCOME_COVERAGE, PROTOCOL_ID, PROTOCOL_START_AT,
    )

    exact = {
        "metric": "append_only_exact_pool_24h_positive_after_frozen_full_paper_cost",
        "sample_kind": "forward_paper_selector",
        "selection_stage": "discovery_rule_before_security_and_route",
        "cost_is_real_fill": False,
        "real_edge_n": 0,
        "real_edge_eligible": False,
        "execution_edge_eligible": False,
        "auto_execution_allowed": False,
        "source_membership_policy": (
            "exact_live_and_independent_finalized_append_only_observation_recheck_v1"
        ),
    }
    for field, expected in exact.items():
        if value.get(field) != expected or type(value.get(field)) is not type(expected):
            raise BoardViewContractError(f"{path}.{field} violates the Launch evidence contract")
    n = _exact_nonnegative_int(value.get("n"), path=f"{path}.n")
    probe, control = value.get("probe"), value.get("control")
    if not isinstance(probe, Mapping) or not isinstance(control, Mapping):
        raise BoardViewContractError(f"{path} must expose both frozen protocol arms")
    strict_resolved = {
        name: _exact_nonnegative_int(
            arm.get("resolved_n"), path=f"{path}.{name}.resolved_n",
        )
        for name, arm in (("probe", probe), ("control", control))
    }
    strict_n = sum(strict_resolved.values())
    if n != strict_n:
        raise BoardViewContractError(f"{path}.n must equal strict resolved arm truth")

    validation = value.get("edge_validation")
    if not isinstance(validation, Mapping):
        raise BoardViewContractError(f"{path}.edge_validation must be an object")
    validation_exact = {
        "protocol_id": PROTOCOL_ID, "cohort_version": COHORT_VERSION,
        "protocol_start_at": PROTOCOL_START_AT,
        "sample_kind": exact["sample_kind"],
        "selection_stage": exact["selection_stage"],
        "cost_is_real_fill": False, "real_edge_n": 0,
        "real_edge_eligible": False, "execution_edge_eligible": False,
        "auto_execution_allowed": False,
    }
    for field, expected in validation_exact.items():
        if (validation.get(field) != expected
                or type(validation.get(field)) is not type(expected)):
            raise BoardViewContractError(
                f"{path}.edge_validation.{field} violates the frozen protocol"
            )
    if value.get("edge_verdict") != validation.get("edge_verdict"):
        raise BoardViewContractError(f"{path}.edge_verdict disagrees with its validator")
    state = validation.get("state")
    if not isinstance(state, str) or not state.strip():
        raise BoardViewContractError(f"{path}.edge_validation.state is required")
    allowed_states = {
        "protocol_integrity_blocked", "collecting", "awaiting_outcomes",
        "coverage_blocked", "regime_overlap_blocked", "invalid_evidence",
        "no_edge_observed", "validator_unavailable", "pass", "inconclusive",
    }
    if state not in allowed_states:
        raise BoardViewContractError(f"{path}.edge_validation.state is invalid")
    if not isinstance(validation.get("reason"), str) or not validation.get("reason", "").strip():
        raise BoardViewContractError(f"{path}.edge_validation.reason is required")
    if validation.get("source_membership_policy") != exact["source_membership_policy"]:
        raise BoardViewContractError(
            f"{path}.edge_validation.source_membership_policy drifted"
        )
    admission = _validate_protocol_admission(
        validation.get("protocol_admission"),
        path=f"{path}.edge_validation.protocol_admission",
    )
    if admission["state"] != "open" and (
            state != "protocol_integrity_blocked"
            or validation.get("edge_verdict") != "不可判"):
        raise BoardViewContractError(
            f"{path}.edge_validation hides a blocked protocol admission"
        )

    positive_verdict = "有前向纸面selector edge迹象"
    if state != "pass" and validation.get("edge_verdict") == positive_verdict:
        raise BoardViewContractError(
            f"{path}.edge_validation cannot claim positive edge outside pass"
        )
    if state == "pass":
        if validation.get("edge_verdict") != positive_verdict:
            raise BoardViewContractError(
                f"{path}.edge_validation.pass must carry the frozen paper verdict"
            )
        look_n = _exact_nonnegative_int(
            validation.get("look_n_per_arm"),
            path=f"{path}.edge_validation.look_n_per_arm",
        )
        if look_n not in LOOK_SIZES:
            raise BoardViewContractError(
                f"{path}.edge_validation.pass is not a frozen look"
            )
        eligible_n = validation.get("eligible_n")
        prefix_arms = validation.get("arms")
        if not isinstance(eligible_n, Mapping) or not isinstance(prefix_arms, Mapping):
            raise BoardViewContractError(
                f"{path}.edge_validation.pass must expose eligible and prefix arms"
            )
        for public_name, protocol_name in (
            ("probe", "SMALL_PROBE"), ("control", "WATCH"),
        ):
            if strict_resolved[public_name] < look_n:
                raise BoardViewContractError(
                    f"{path}.{public_name} has fewer resolved rows than the frozen look"
                )
            eligible_count = _exact_nonnegative_int(
                eligible_n.get(protocol_name),
                path=f"{path}.edge_validation.eligible_n.{protocol_name}",
            )
            if eligible_count < look_n:
                raise BoardViewContractError(
                    f"{path}.edge_validation.eligible_n.{protocol_name} is below the look"
                )
            prefix = prefix_arms.get(protocol_name)
            if not isinstance(prefix, Mapping):
                raise BoardViewContractError(
                    f"{path}.edge_validation.arms.{protocol_name} is required for pass"
                )
            for field, expected in (
                ("eligible_n", look_n), ("resolved_n", look_n),
                ("pending_n", 0), ("unavailable_n", 0), ("invalid_n", 0),
            ):
                actual = _exact_nonnegative_int(
                    prefix.get(field),
                    path=f"{path}.edge_validation.arms.{protocol_name}.{field}",
                )
                if actual != expected:
                    raise BoardViewContractError(
                        f"{path}.edge_validation.arms.{protocol_name}.{field} "
                        "contradicts a complete frozen look"
                    )
            coverage = _finite_nonnegative(
                prefix.get("coverage"),
                path=f"{path}.edge_validation.arms.{protocol_name}.coverage",
            )
            if coverage != MIN_OUTCOME_COVERAGE:
                raise BoardViewContractError(
                    f"{path}.edge_validation.arms.{protocol_name} lacks full coverage"
                )
        pvalues = validation.get("spa_pvalues")
        if (validation.get("spa_pvalue_used") != "upper"
                or not isinstance(pvalues, Mapping)):
            raise BoardViewContractError(
                f"{path}.edge_validation.pass must use the SPA upper p-value"
            )
        p_upper = _finite_nonnegative(
            pvalues.get("upper"), path=f"{path}.edge_validation.spa_pvalues.upper",
        )
        if p_upper > LOOK_ALPHA:
            raise BoardViewContractError(
                f"{path}.edge_validation SPA upper p-value misses the frozen alpha"
            )
        mean_lift = _finite_nonnegative(
            validation.get("mean_daily_log_utility_lift"),
            path=f"{path}.edge_validation.mean_daily_log_utility_lift",
        )
        if mean_lift < MIN_MEAN_UTILITY_LIFT:
            raise BoardViewContractError(
                f"{path}.edge_validation mean lift misses the frozen threshold"
            )

    current = value.get("current_protocol")
    if not isinstance(current, Mapping):
        raise BoardViewContractError(f"{path}.current_protocol must be an object")
    for field in ("protocol_id", "cohort_version", "protocol_start_at"):
        if current.get(field) != validation.get(field):
            raise BoardViewContractError(f"{path}.current_protocol.{field} drifted")
    _exact_nonnegative_int(
        current.get("integrity_invalid_n"),
        path=f"{path}.current_protocol.integrity_invalid_n",
    )
    if current.get("protocol_admission") != validation.get("protocol_admission"):
        raise BoardViewContractError(f"{path}.current_protocol admission drifted")

    legacy = value.get("legacy_distribution")
    if (not isinstance(legacy, Mapping)
            or legacy.get("sample_kind") != "legacy_mutable_outcome_descriptive_only"
            or legacy.get("edge_eligible") is not False
            or legacy.get("real_edge_n") != 0
            or legacy.get("real_edge_eligible") is not False
            or legacy.get("execution_edge_eligible") is not False
            or legacy.get("auto_execution_allowed") is not False):
        raise BoardViewContractError(f"{path}.legacy_distribution is not quarantined")


def _validate_stats_view(payload: Mapping[str, Any]) -> None:
    lanes = payload.get("lanes")
    if not isinstance(lanes, Mapping):
        raise BoardViewContractError("stats.lanes must be an object")
    if "launch" not in lanes:
        raise BoardViewContractError("stats.lanes.launch is required")
    _validate_launch_stats(lanes["launch"], path="stats.lanes.launch")
    if "carry" in lanes:
        _validate_carry_stats(lanes["carry"], path="stats.lanes.carry")


def _aware_clock(value: Any, *, path: str) -> datetime:
    try:
        clock = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise BoardViewContractError(f"{path} must be an ISO timestamp") from exc
    if clock.tzinfo is None:
        raise BoardViewContractError(f"{path} must include a timezone")
    return clock


def _epoch_millis_clock(value: Any, *, path: str) -> datetime:
    """Parse an exchange epoch-millisecond field without lossy float rounding."""
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise BoardViewContractError(f"{path} must be a positive epoch-ms integer")
    text = str(value).strip()
    if not text.isdecimal():
        raise BoardViewContractError(f"{path} must be a positive epoch-ms integer")
    try:
        millis = int(text)
    except ValueError as exc:
        raise BoardViewContractError(
            f"{path} must be a representable epoch-ms integer"
        ) from exc
    if millis <= 0:
        raise BoardViewContractError(f"{path} must be a positive epoch-ms integer")
    try:
        return datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(
            milliseconds=millis,
        )
    except OverflowError as exc:
        raise BoardViewContractError(
            f"{path} must be a representable epoch-ms integer"
        ) from exc


def _validate_cost_contract(value: Any, *, path: str) -> dict | None:
    if value is None:
        return None
    try:
        from src.pipeline.execution_cost import validate

        return validate(value)
    except (TypeError, ValueError) as exc:
        raise BoardViewContractError(f"{path} is invalid: {exc}") from exc


def _validate_launch_a3(row: Mapping[str, Any], *, assessment: Any,
                        generated_at: datetime, path: str) -> None:
    """Recheck every public manual-probe gate instead of trusting its label."""
    from src.contract.launch_probe import launch_manual_probe_failures
    from src.pipeline.launch_execution import QUOTE_TTL_SECONDS

    wall_now = datetime.now(timezone.utc)
    if abs((wall_now - generated_at).total_seconds()) > QUOTE_TTL_SECONDS:
        raise BoardViewContractError(f"{path} A3 envelope is outside the live quote window")
    failures = launch_manual_probe_failures(
        row, assessment, row.get("evidence_gate"), now=wall_now
    )
    if failures:
        raise BoardViewContractError(f"{path} invalid A3 manual probe: {', '.join(failures)}")
    from src.pipeline.opportunity_ledger import launch_delivery_readback_matches

    if not launch_delivery_readback_matches(str(row.get("id") or ""), dict(assessment)):
        raise BoardViewContractError(
            f"{path} A3 delivery proof is absent from the append-only ledger"
        )


def _validate_nonlaunch_identity(row: Mapping[str, Any], *, generated_at: datetime,
                                 path: str) -> None:
    """Bind a public event to one source, asset and timezone-aware observation."""
    for field in ("chain", "token", "symbol", "source"):
        _required_text(row.get(field), path=f"{path}.{field}")
    for field in ("detected_at", "decision_at"):
        clock = _aware_clock(row.get(field), path=f"{path}.{field}")
        if clock > generated_at + timedelta(seconds=5):
            raise BoardViewContractError(f"{path}.{field} is ahead of the board clock")
    # Event/executable clocks may legitimately describe a scheduled future listing
    # or claim window. Observation and quote clocks may not claim future knowledge.
    for field in ("event_at", "executable_at"):
        if row.get(field) is None:
            continue
        _aware_clock(row[field], path=f"{path}.{field}")
    if row.get("quote_at") is not None:
        clock = _aware_clock(row["quote_at"], path=f"{path}.quote_at")
        if clock > generated_at + timedelta(seconds=5):
            raise BoardViewContractError(f"{path}.quote_at is ahead of the board clock")
    if row.get("expires_at") is not None:
        _aware_clock(row["expires_at"], path=f"{path}.expires_at")


def _validate_nested_manual_only(row: Mapping[str, Any], *, path: str) -> None:
    """Do not let a nested assessment or evidence object opt into automation."""
    assessment = row.get("current_assessment")
    if assessment is not None and assessment.get("auto_execution_allowed") is not False:
        raise BoardViewContractError(
            f"{path}.current_assessment.auto_execution_allowed must be exactly false"
        )
    evidence = row.get("evidence_gate")
    if evidence is not None:
        if not isinstance(evidence, Mapping):
            raise BoardViewContractError(f"{path}.evidence_gate must be an object")
        if evidence.get("auto_execution_allowed") is not False:
            raise BoardViewContractError(
                f"{path}.evidence_gate.auto_execution_allowed must be exactly false"
            )


def _same_clock(left: Any, right: Any, *, path: str) -> bool:
    return _aware_clock(left, path=f"{path}.left") == _aware_clock(
        right, path=f"{path}.right",
    )


def _validate_structure_products(row: Mapping[str, Any], *, path: str) -> None:
    markets = row.get("markets")
    if (not isinstance(markets, list) or not markets
            or any(not isinstance(item, str) or not item.strip() for item in markets)
            or len(set(markets)) != len(markets)):
        raise BoardViewContractError(f"{path}.markets must be unique non-empty strings")
    products = row.get("products")
    if not isinstance(products, list) or len(products) != len(markets):
        raise BoardViewContractError(f"{path}.products must bind every market exactly once")
    product_markets: list[str] = []
    categories: set[str] = set()
    for index, product in enumerate(products):
        product_path = f"{path}.products[{index}]"
        if not isinstance(product, Mapping):
            raise BoardViewContractError(f"{product_path} must be an object")
        market = _required_text(product.get("market"), path=f"{product_path}.market")
        product_markets.append(market)
        metadata = product.get("metadata")
        if not isinstance(metadata, Mapping) or metadata.get("version") != 1:
            raise BoardViewContractError(f"{product_path}.metadata must be version 1")
        if (metadata.get("source") != row.get("source")
                or metadata.get("instrument_id") != market
                or metadata.get("market_type") != "spot"
                or not isinstance(metadata.get("source_fields"), Mapping)):
            raise BoardViewContractError(f"{product_path}.metadata identity is unbound")
        classification = product.get("classification")
        if not isinstance(classification, Mapping):
            raise BoardViewContractError(f"{product_path}.classification must be an object")
        category = classification.get("category")
        if category not in _STRUCTURE_INSTRUMENT_CLASSES - {"mixed"}:
            raise BoardViewContractError(f"{product_path}.classification category is invalid")
        _required_text(
            classification.get("basis"), path=f"{product_path}.classification.basis",
        )
        if category == "unclassified_spot":
            if classification.get("basis") not in {
                    "no_explicit_product_taxonomy", "legacy_row_has_no_product_taxonomy",
                    "current_inventory_metadata_unavailable"}:
                raise BoardViewContractError(
                    f"{product_path}.classification has an invalid unknown basis"
                )
        else:
            if classification.get("basis") != "official_instrument_metadata":
                raise BoardViewContractError(
                    f"{product_path}.classification lacks official metadata basis"
                )
            source_field = classification.get("source_field")
            source_value = classification.get("source_value")
            source = metadata.get("source")
            if (not isinstance(source_field, str)
                    or source_field not in _STRUCTURE_TAXONOMY_FIELDS.get(source, ())):
                raise BoardViewContractError(
                    f"{product_path}.classification source taxonomy field is not allowed"
                )
            raw_value = metadata["source_fields"].get(source_field)
            if (not isinstance(source_value, str) or not isinstance(raw_value, str)
                    or raw_value.strip().lower() != source_value):
                raise BoardViewContractError(
                    f"{product_path}.classification is not bound to source_fields"
                )
            if source_field == "instCategory":
                expected_category = {
                    "1": "crypto_asset", "3": "tokenized_equity_or_etf",
                    "4": "tokenized_commodity", "5": "tokenized_forex",
                    "6": "tokenized_bond",
                }.get(source_value) if metadata.get("source") == "okx" else None
            else:
                value = source_value
                if "etf" in value or "exchange traded fund" in value:
                    expected_category = "tokenized_etf"
                elif any(word in value for word in ("equity", "stock", "share")):
                    expected_category = "tokenized_equity"
                elif "commodit" in value:
                    expected_category = "tokenized_commodity"
                elif value in {"fx", "forex", "foreign_exchange"}:
                    expected_category = "tokenized_forex"
                elif any(word in value for word in ("bond", "fixed_income")):
                    expected_category = "tokenized_bond"
                elif value in {"crypto", "cryptocurrency", "digital_asset"}:
                    expected_category = "crypto_asset"
                else:
                    expected_category = None
            if expected_category != category:
                raise BoardViewContractError(
                    f"{product_path}.classification contradicts source taxonomy"
                )
        categories.add(category)
        schedule = product.get("source_reported_schedule")
        if schedule is not None:
            if (not isinstance(schedule, Mapping)
                    or schedule.get("basis") != "instrument_metadata_only"
                    or schedule.get("official_announcement_verified") is not False):
                raise BoardViewContractError(
                    f"{product_path}.source_reported_schedule must remain unverified"
                )
            schedule_path = f"{product_path}.source_reported_schedule"
            source_field = _required_text(
                schedule.get("source_field"), path=f"{schedule_path}.source_field",
            )
            source = metadata.get("source")
            if source_field not in _STRUCTURE_SCHEDULE_FIELDS.get(source, ()):
                raise BoardViewContractError(
                    f"{schedule_path}.source_field is not allowed for {source}"
                )
            source_fields = metadata["source_fields"]
            if source_field not in source_fields:
                raise BoardViewContractError(
                    f"{schedule_path}.source_field is absent from source_fields"
                )
            reported_open = _aware_clock(
                schedule.get("reported_open_at"),
                path=f"{schedule_path}.reported_open_at",
            )
            source_open = _epoch_millis_clock(
                source_fields[source_field],
                path=f"{product_path}.metadata.source_fields.{source_field}",
            )
            if source_open != reported_open:
                raise BoardViewContractError(
                    f"{schedule_path}.reported_open_at contradicts source_fields"
                )
    if set(product_markets) != set(markets) or len(set(product_markets)) != len(markets):
        raise BoardViewContractError(f"{path}.products do not match markets")
    declared_classes = row.get("instrument_classes")
    if (not isinstance(declared_classes, list)
            or set(declared_classes) != categories
            or len(declared_classes) != len(categories)):
        raise BoardViewContractError(f"{path}.instrument_classes contradict products")
    expected = next(iter(categories)) if len(categories) == 1 else "mixed"
    if row.get("instrument_class") != expected:
        raise BoardViewContractError(f"{path}.instrument_class contradicts products")


def _validate_structure_event(
    row: Mapping[str, Any], *, generated_at: datetime, path: str,
) -> None:
    event_type = row.get("event_type")
    if event_type not in _STRUCTURE_EVENT_TYPES:
        raise BoardViewContractError(f"{path}.event_type is invalid")
    if not _same_clock(
        row.get("inventory_detected_at"), row.get("detected_at"),
        path=f"{path}.inventory_detection_binding",
    ):
        raise BoardViewContractError(f"{path}.inventory_detected_at is unbound")
    _validate_structure_products(row, path=path)
    classification = row.get("instrument_classification")
    if not isinstance(classification, Mapping):
        raise BoardViewContractError(
            f"{path}.instrument_classification must expose metadata time semantics"
        )
    if (classification.get("time_semantics")
            != "current_inventory_metadata_not_event_time_evidence"
            or classification.get("event_time_evidence") is not False):
        raise BoardViewContractError(
            f"{path}.instrument_classification cannot claim event-time evidence"
        )
    classification_state = classification.get("state")
    metadata_observed_at = classification.get("metadata_observed_at")
    products = row.get("products")
    if classification_state == "current_metadata_observed":
        observed_clock = _aware_clock(
            metadata_observed_at,
            path=f"{path}.instrument_classification.metadata_observed_at",
        )
        if observed_clock > generated_at + timedelta(seconds=5):
            raise BoardViewContractError(
                f"{path}.instrument_classification is ahead of the board clock"
            )
        if any(
            (product.get("classification") or {}).get("basis")
            == "current_inventory_metadata_unavailable"
            for product in products
        ):
            raise BoardViewContractError(
                f"{path}.instrument_classification claims unavailable metadata"
            )
        for index, product in enumerate(products):
            schedule = product.get("source_reported_schedule")
            if schedule is not None and (
                    schedule.get("metadata_observed_at") != metadata_observed_at
                    or schedule.get("time_semantics")
                    != "current_inventory_metadata_not_event_time_evidence"):
                raise BoardViewContractError(
                    f"{path}.products[{index}].source_reported_schedule has "
                    "ambiguous time semantics"
                )
    elif classification_state == "unclassified_metadata_unavailable":
        if metadata_observed_at is not None or row.get("instrument_class") != "unclassified_spot":
            raise BoardViewContractError(
                f"{path}.instrument_classification must fail closed"
            )
        if any(
            (product.get("classification") or {}).get("basis")
            != "current_inventory_metadata_unavailable"
            for product in products
        ):
            raise BoardViewContractError(
                f"{path}.instrument_classification hides unavailable metadata"
            )
        if any(product.get("source_reported_schedule") is not None for product in products):
            raise BoardViewContractError(
                f"{path}.instrument_classification cannot publish a schedule "
                "when metadata is unavailable"
            )
        if any(bool(product["metadata"]["source_fields"]) for product in products):
            raise BoardViewContractError(
                f"{path}.instrument_classification cannot retain source_fields "
                "when metadata is unavailable"
            )
    else:
        raise BoardViewContractError(
            f"{path}.instrument_classification.state is invalid"
        )
    verification = row.get("listing_verification")
    if not isinstance(verification, Mapping):
        raise BoardViewContractError(f"{path}.listing_verification must be an object")

    if (row.get("time_semantics") != "inventory_detection_not_listing_open"
            or row.get("scheduled_open_at") is not None
            or verification.get("state") != "unverified"):
        raise BoardViewContractError(f"{path} inventory observation claims a listing time")
    if not _same_clock(
        row.get("event_at"), row.get("detected_at"),
        path=f"{path}.inventory_event_clock",
    ):
        raise BoardViewContractError(
            f"{path}.event_at must remain the inventory detection time"
        )
    _required_text(
        verification.get("reason_code"),
        path=f"{path}.listing_verification.reason_code",
    )


def _trusted_https_url(value: Any, *, trust_root: str, path: str) -> str:
    url = _required_text(value, path=path)
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower().rstrip(".")
    if (parsed.scheme != "https" or not host or parsed.username is not None
            or parsed.password is not None
            or not (host == trust_root or host.endswith("." + trust_root))):
        raise BoardViewContractError(f"{path} must be HTTPS under {trust_root}")
    return url


def _validate_airdrop_event(row: Mapping[str, Any], *, generated_at: datetime,
                            path: str) -> None:
    trust_root = _required_text(row.get("trust_root"), path=f"{path}.trust_root")
    if trust_root != trust_root.lower().rstrip(".") or "." not in trust_root:
        raise BoardViewContractError(f"{path}.trust_root is not canonical")
    _trusted_https_url(
        row.get("official_url"), trust_root=trust_root, path=f"{path}.official_url",
    )
    _trusted_https_url(
        row.get("source_evidence_url"), trust_root=trust_root,
        path=f"{path}.source_evidence_url",
    )
    state = row.get("source_state")
    if state not in {"source_verified", "source_unverified"}:
        raise BoardViewContractError(f"{path}.source_state is invalid")
    if row.get("official_state") != state:
        raise BoardViewContractError(f"{path}.official_state contradicts source_state")
    verification = row.get("source_verification")
    if not isinstance(verification, Mapping):
        raise BoardViewContractError(f"{path}.source_verification must be an object")
    if verification.get("trust_root") != trust_root:
        raise BoardViewContractError(f"{path}.source_verification trust root drifted")
    checked_at = _aware_clock(
        verification.get("checked_at"), path=f"{path}.source_verification.checked_at",
    )
    if checked_at > generated_at + timedelta(seconds=5):
        raise BoardViewContractError(
            f"{path}.source_verification.checked_at is ahead of the board clock"
        )
    source_pass = (
        verification.get("official_page_verified") is True
        and verification.get("evidence_page_verified") is True
    )
    if (state == "source_verified") is not source_pass:
        raise BoardViewContractError(f"{path}.source_verification contradicts source_state")
    if row.get("deadline") != row.get("expires_at"):
        raise BoardViewContractError(f"{path}.deadline must equal expires_at")

    if row.get("actionable_now") is True:
        if (row.get("effective_decision") != "CLAIM_CHECK"
                or row.get("state") != "claimable"
                or state != "source_verified"
                or row.get("evidence_state") != "recorded"):
            raise BoardViewContractError(f"{path} has no verified claim-check basis")
        wallet_count = row.get("wallet_count")
        if isinstance(wallet_count, bool) or not isinstance(wallet_count, int) \
                or wallet_count < 1:
            raise BoardViewContractError(f"{path}.wallet_count must prove an owned wallet")


def _validate_cascade_event(row: Mapping[str, Any], *, path: str) -> None:
    direction, side = row.get("direction"), row.get("side")
    expected_side = {
        "longs_crowded": "SHORT", "down": "SHORT",
        "shorts_crowded": "LONG", "up": "LONG",
    }.get(direction)
    if expected_side is None or side != expected_side:
        raise BoardViewContractError(f"{path}.side contradicts cascade direction")
    if row.get("event_at") is None:
        raise BoardViewContractError(f"{path}.event_at is required")
    if row.get("actionable_now") is not True:
        return
    evidence = row.get("evidence_gate")
    probe = row.get("execution_probe")
    if not isinstance(evidence, Mapping) or evidence.get("state") != "pass":
        raise BoardViewContractError(f"{path} lacks a passing cascade evidence gate")
    if (not isinstance(probe, Mapping) or probe.get("state") != "quoted"
            or probe.get("read_only") is not True
            or probe.get("is_real_fill") is not False
            or probe.get("side") != side
            or probe.get("quote_at") != row.get("quote_at")):
        raise BoardViewContractError(f"{path} lacks a bound read-only cascade quote")


def _validate_nonlaunch_action(row: Mapping[str, Any], *, view: str,
                               generated_at: datetime, path: str) -> None:
    allowed = {
        "structure": {"WATCH"},
        "airdrop": {"WATCH", "CLAIM_CHECK", "CLAIMED", "EXPIRED", "AVOID"},
        "perps": {"WATCH", "SMALL_PROBE", "EXPIRED", "AVOID"},
    }[view]
    effective = row.get("effective_decision")
    if effective not in allowed:
        raise BoardViewContractError(f"{path}.effective_decision is invalid for {view}")
    action_decisions = {"SMALL_PROBE", "CLAIM_CHECK"}
    if row.get("actionable_now") is not (effective in action_decisions):
        raise BoardViewContractError(
            f"{path}.actionable_now contradicts effective_decision"
        )
    _validate_nested_manual_only(row, path=path)
    if view == "structure":
        if row.get("event_at") is None:
            raise BoardViewContractError(f"{path}.event_at is required")
        _validate_structure_event(row, generated_at=generated_at, path=path)
    elif view == "airdrop":
        _validate_airdrop_event(row, generated_at=generated_at, path=path)
    else:
        _validate_cascade_event(row, path=path)


def _validate_observation_rows(value: Any, *, path: str) -> None:
    """Validate non-event Perps observations without inventing execution fields."""
    if not isinstance(value, list):
        raise BoardViewContractError(f"{path} must be a list")
    for index, row in enumerate(value):
        row_path = f"{path}[{index}]"
        if not isinstance(row, Mapping):
            raise BoardViewContractError(f"{row_path} must be an object")
        _required_text(row.get("symbol"), path=f"{row_path}.symbol")
        if row.get("actionable_now") is True:
            raise BoardViewContractError(f"{row_path} is an observation, not an action")
        if ("auto_execution_allowed" in row
                and row.get("auto_execution_allowed") is not False):
            raise BoardViewContractError(
                f"{row_path}.auto_execution_allowed must be exactly false"
            )
        if (row.get("effective_decision") in {"SMALL_PROBE", "CLAIM_CHECK"}
                or row.get("decision") in {"SMALL_PROBE", "CLAIM_CHECK"}):
            raise BoardViewContractError(
                f"{row_path} cannot make an observation actionable"
            )


def _validate_carry_proxy_semantics(value: Any, *, path: str,
                                    real_fill_field: str) -> Mapping[str, Any]:
    """Freeze Carry's public claim as a partial quote proxy, never real edge."""
    if not isinstance(value, Mapping):
        raise BoardViewContractError(f"{path} must be an object")
    exact = {
        "cohort_kind": "descriptive_quote_proxy",
        "cost_completeness": "partial",
        "all_in_total_pct": None,
        real_fill_field: False,
        "real_edge_eligible": False,
    }
    for field, expected in exact.items():
        if value.get(field) != expected or type(value.get(field)) is not type(expected):
            raise BoardViewContractError(
                f"{path}.{field} violates the Carry proxy contract"
            )
    if _exact_nonnegative_int(
            value.get("real_edge_n"), path=f"{path}.real_edge_n") != 0:
        raise BoardViewContractError(
            f"{path}.real_edge_n must remain zero until real-fill validation exists"
        )
    if ("auto_execution_allowed" in value
            and value.get("auto_execution_allowed") is not False):
        raise BoardViewContractError(
            f"{path}.auto_execution_allowed must be exactly false"
        )
    return value


def _validate_exclusion_breakdown(value: Any, *, excluded_n: int,
                                  path: str) -> None:
    if not isinstance(value, Mapping):
        raise BoardViewContractError(f"{path} must be an object")
    total = 0
    for reason, count in value.items():
        if not isinstance(reason, str) or not reason.strip():
            raise BoardViewContractError(f"{path} keys must be non-empty strings")
        total += _exact_nonnegative_int(count, path=f"{path}.{reason}")
    # One excluded episode may carry several independent reason codes, so the
    # reason total can exceed the row total but can never under-explain it.
    if (excluded_n == 0 and total != 0) or (excluded_n > 0 and total < excluded_n):
        raise BoardViewContractError(
            f"{path} does not account for the excluded Carry episodes"
        )


def _validate_carry_stats(value: Any, *, path: str) -> None:
    value = _validate_carry_proxy_semantics(
        value, path=path, real_fill_field="cost_is_real_fill",
    )
    exact = {
        "metric": "quote_rate_integral_minus_book_quotes_and_modeled_fee_proxy",
        "execution_mode": "paper_orderbook_measurement",
        "edge_verdict": "不可判",
    }
    for field, expected in exact.items():
        if value.get(field) != expected or type(value.get(field)) is not type(expected):
            raise BoardViewContractError(
                f"{path}.{field} violates the Carry evidence contract"
            )
    n = _exact_nonnegative_int(value.get("n"), path=f"{path}.n")
    n_proxy = _exact_nonnegative_int(
        value.get("n_proxy"), path=f"{path}.n_proxy",
    )
    hits = _exact_nonnegative_int(value.get("hits"), path=f"{path}.hits")
    total = _exact_nonnegative_int(
        value.get("total_closed"), path=f"{path}.total_closed",
    )
    excluded = _exact_nonnegative_int(
        value.get("excluded_closed"), path=f"{path}.excluded_closed",
    )
    _exact_nonnegative_int(value.get("pending"), path=f"{path}.pending")
    if n_proxy != n or hits > n or total != n + excluded:
        raise BoardViewContractError(
            f"{path} proxy/closed/excluded counts are inconsistent"
        )
    _validate_exclusion_breakdown(
        value.get("excluded_by_reason"), excluded_n=excluded,
        path=f"{path}.excluded_by_reason",
    )
    from src.pipeline.opportunity_outcomes import MIN_N as CARRY_MIN_N

    verdict = value.get("verdict")
    expected_verdict = "measured" if n >= CARRY_MIN_N else "不可判"
    if verdict != expected_verdict:
        raise BoardViewContractError(f"{path}.verdict contradicts its proxy sample")


def _validate_carry_episode_rows(value: Any, *, expected_n: int,
                                 path: str) -> None:
    if not isinstance(value, list) or len(value) != expected_n:
        raise BoardViewContractError(f"{path} must contain exactly {expected_n} rows")
    for index, row in enumerate(value):
        row_path = f"{path}[{index}]"
        if not isinstance(row, Mapping):
            raise BoardViewContractError(f"{row_path} must be an object")
        contract = _validate_cost_contract(
            row.get("cost_contract"), path=f"{row_path}.cost_contract",
        )
        if contract is None:
            raise BoardViewContractError(f"{row_path}.cost_contract is required")
        contract_exact = {
            "purpose": "paper_measurement",
            "method": "cross_perp_paper_quote_proxy_v1",
            "completeness": "partial",
            "all_in_total_pct": None,
            "is_real_fill": False,
        }
        for field, expected in contract_exact.items():
            if (contract.get(field) != expected
                    or type(contract.get(field)) is not type(expected)):
                raise BoardViewContractError(
                    f"{row_path}.cost_contract.{field} violates the Carry proxy contract"
                )
        row_exact = {
            "settled_funding_pct": None,
            "basis_pnl_pct": None,
            "realized_net_return_pct": None,
            "real_edge_eligible": False,
        }
        for field, expected in row_exact.items():
            if row.get(field) != expected or type(row.get(field)) is not type(expected):
                raise BoardViewContractError(
                    f"{row_path}.{field} cannot claim real Carry execution"
                )
        if "is_real_fill" in row and row.get("is_real_fill") is not False:
            raise BoardViewContractError(f"{row_path}.is_real_fill must be exactly false")


def _validate_carry_paper(value: Any, *, health: Any, path: str) -> None:
    if not isinstance(value, Mapping):
        raise BoardViewContractError(f"{path} must be an object")
    if not value:
        paper_health = health.get("paper") if isinstance(health, Mapping) else None
        if (not isinstance(paper_health, Mapping)
                or paper_health.get("state") not in {"partial", "error"}):
            raise BoardViewContractError(
                f"{path} may be empty only when the paper tracker failed explicitly"
            )
        return
    value = _validate_carry_proxy_semantics(
        value, path=path, real_fill_field="is_real_fill",
    )
    n_proxy = _exact_nonnegative_int(
        value.get("n_proxy_closed"), path=f"{path}.n_proxy_closed",
    )
    n_closed = _exact_nonnegative_int(
        value.get("n_closed"), path=f"{path}.n_closed",
    )
    total = _exact_nonnegative_int(
        value.get("n_closed_total"), path=f"{path}.n_closed_total",
    )
    excluded = _exact_nonnegative_int(
        value.get("n_closed_excluded"), path=f"{path}.n_closed_excluded",
    )
    n_open = _exact_nonnegative_int(
        value.get("n_open"), path=f"{path}.n_open",
    )
    n_exit_pending = _exact_nonnegative_int(
        value.get("n_exit_pending"), path=f"{path}.n_exit_pending",
    )
    _exact_nonnegative_int(
        value.get("n_quarantined_total"), path=f"{path}.n_quarantined_total",
    )
    if n_closed != n_proxy or total != n_closed + excluded:
        raise BoardViewContractError(
            f"{path} proxy/closed/excluded counts are inconsistent"
        )
    if n_exit_pending > n_open:
        raise BoardViewContractError(f"{path}.n_exit_pending exceeds open episodes")
    open_positions = value.get("open_positions")
    _validate_carry_episode_rows(
        open_positions, expected_n=n_open, path=f"{path}.open_positions",
    )
    recent = value.get("recent", [])
    _validate_carry_episode_rows(
        recent, expected_n=min(n_proxy, 8), path=f"{path}.recent",
    )
    _validate_exclusion_breakdown(
        value.get("excluded_by_reason"), excluded_n=excluded,
        path=f"{path}.excluded_by_reason",
    )


def _validate_perps_view(payload: Mapping[str, Any]) -> None:
    for key in ("perps", "carry"):
        if key not in payload:
            raise BoardViewContractError(f"perps.{key} is required")
        _validate_observation_rows(payload[key], path=f"perps.{key}")
    _validate_carry_paper(
        payload.get("carry_paper"), health=payload.get("carry_source_health"),
        path="perps.carry_paper",
    )


def _exact_keys(value: Mapping[str, Any], expected: set[str], *, path: str) -> None:
    if set(value) != expected:
        raise BoardViewContractError(f"{path} has fields outside its exact contract")


def _runtime_count(value: Any, *, path: str) -> int | None:
    if value is None:
        return None
    return _exact_nonnegative_int(value, path=path)


def _validate_runtime_safety(value: Any, *, path: str) -> None:
    """Validate the exact fail-closed projection used by the public meta view."""
    if not isinstance(value, Mapping):
        raise BoardViewContractError(f"{path} must be an object")
    _exact_keys(value, {
        "version", "state", "blocks_actionability", "auto_execution_allowed",
        "storage_pressure", "reason_codes", "streams",
        "hyperliquid_raw_trade_retention",
    }, path=path)
    if value.get("version") != 1 or type(value.get("version")) is not int:
        raise BoardViewContractError(f"{path}.version must be exactly 1")
    state = value.get("state")
    if state not in {"healthy", "degraded", "blocked", "unknown"}:
        raise BoardViewContractError(f"{path}.state is invalid")
    blocks = value.get("blocks_actionability")
    if not isinstance(blocks, bool):
        raise BoardViewContractError(f"{path}.blocks_actionability must be boolean")
    if value.get("auto_execution_allowed") is not False:
        raise BoardViewContractError(
            f"{path}.auto_execution_allowed must be exactly false"
        )
    storage = value.get("storage_pressure")
    if storage not in {"ok", "warn", "critical", "unknown"}:
        raise BoardViewContractError(f"{path}.storage_pressure is invalid")

    streams = value.get("streams")
    if not isinstance(streams, Mapping):
        raise BoardViewContractError(f"{path}.streams must be an object")
    _exact_keys(streams, {"solana", "evm"}, path=f"{path}.streams")
    solana = streams.get("solana")
    if not isinstance(solana, Mapping):
        raise BoardViewContractError(f"{path}.streams.solana must be an object")
    _exact_keys(
        solana, {"state", "live", "configured", "maintenance"},
        path=f"{path}.streams.solana",
    )
    solana_state = solana.get("state")
    if solana_state not in {"healthy", "blocked", "unknown"}:
        raise BoardViewContractError(f"{path}.streams.solana.state is invalid")
    solana_live = _runtime_count(
        solana.get("live"), path=f"{path}.streams.solana.live",
    )
    solana_configured = _runtime_count(
        solana.get("configured"), path=f"{path}.streams.solana.configured",
    )
    if (solana_live is not None and solana_configured is not None
            and solana_live > solana_configured):
        raise BoardViewContractError(f"{path}.streams.solana live exceeds configured")
    expected_solana = (
        "unknown"
        if solana_live is None or solana_configured in {None, 0}
        else "healthy" if solana_live == solana_configured else "blocked"
    )
    if solana_state != expected_solana:
        raise BoardViewContractError(f"{path}.streams.solana counts contradict state")
    maintenance = solana.get("maintenance")
    if maintenance not in {"healthy", "blocked", "unknown"}:
        raise BoardViewContractError(f"{path}.streams.solana.maintenance is invalid")

    evm = streams.get("evm")
    if not isinstance(evm, Mapping):
        raise BoardViewContractError(f"{path}.streams.evm must be an object")
    _exact_keys(evm, {"state", "live", "configured"}, path=f"{path}.streams.evm")
    evm_state = evm.get("state")
    if evm_state not in {"healthy", "degraded", "blocked", "unknown"}:
        raise BoardViewContractError(f"{path}.streams.evm.state is invalid")
    evm_live = _runtime_count(evm.get("live"), path=f"{path}.streams.evm.live")
    evm_configured = _runtime_count(
        evm.get("configured"), path=f"{path}.streams.evm.configured",
    )
    if (evm_live is not None and evm_configured is not None
            and evm_live > evm_configured):
        raise BoardViewContractError(f"{path}.streams.evm live exceeds configured")
    expected_evm = (
        "unknown"
        if evm_live is None or evm_configured in {None, 0}
        else "healthy" if evm_live == evm_configured
        else "blocked" if evm_live == 0 else "degraded"
    )
    if evm_state != expected_evm:
        raise BoardViewContractError(f"{path}.streams.evm counts contradict state")

    retention = value.get("hyperliquid_raw_trade_retention")
    if retention not in {"retained", "shed", "unknown"}:
        raise BoardViewContractError(
            f"{path}.hyperliquid_raw_trade_retention is invalid"
        )
    reasons = value.get("reason_codes")
    if (not isinstance(reasons, list)
            or any(not isinstance(reason, str) for reason in reasons)
            or len(reasons) != len(set(reasons))
            or any(reason not in _RUNTIME_SAFETY_REASON_CODES for reason in reasons)):
        raise BoardViewContractError(f"{path}.reason_codes violates its allowlist")
    unavailable = (storage == "unknown" or solana_state == "unknown"
                   or maintenance == "unknown" or evm_state == "unknown"
                   or retention == "unknown")
    expected_reasons = ["runtime_health_unavailable"] if unavailable else []
    if storage == "warn":
        expected_reasons.append("storage_pressure_warn")
    elif storage == "critical":
        expected_reasons.append("storage_pressure_critical")
    if solana_state == "blocked":
        expected_reasons.append("solana_streams_unhealthy")
    if maintenance == "blocked":
        expected_reasons.append("solana_maintenance_unhealthy")
    if evm_state in {"degraded", "blocked"}:
        expected_reasons.append("evm_streams_unhealthy")
    if retention == "shed":
        expected_reasons.append("hyperliquid_raw_trade_retention_shed")
    if reasons != expected_reasons:
        raise BoardViewContractError(f"{path}.reason_codes contradict runtime state")

    if unavailable:
        expected_state, expected_blocks = "unknown", True
    elif (storage == "critical" or solana_state == "blocked"
          or maintenance == "blocked"):
        expected_state, expected_blocks = "blocked", True
    elif storage == "warn" or evm_state != "healthy" or retention == "shed":
        expected_state, expected_blocks = "degraded", False
    else:
        expected_state, expected_blocks = "healthy", False
    if state != expected_state or blocks is not expected_blocks:
        raise BoardViewContractError(
            f"{path} top-level state contradicts its projected components"
        )


def _validate_perp_identity_policy(value: Any, *, path: str) -> None:
    """Validate the exact public gate for identity-dependent perp scans only."""
    if not isinstance(value, Mapping):
        raise BoardViewContractError(f"{path} must be an object")
    _exact_keys(value, {
        "version", "status", "blocks_identity_dependent_scans",
        "auto_execution_allowed", "reason_codes", "market_count",
        "research_mapped", "actionable_identity_count",
        "independent_source_count", "observed_path_count",
        "cache_age_seconds", "cache_ttl_seconds",
    }, path=path)
    if value.get("version") != 1 or type(value.get("version")) is not int:
        raise BoardViewContractError(f"{path}.version must be exactly 1")
    status = value.get("status")
    if not isinstance(status, str) or status not in _PERP_IDENTITY_STATUSES:
        raise BoardViewContractError(f"{path}.status is invalid")
    blocks = value.get("blocks_identity_dependent_scans")
    if not isinstance(blocks, bool):
        raise BoardViewContractError(
            f"{path}.blocks_identity_dependent_scans must be boolean"
        )
    if value.get("auto_execution_allowed") is not False:
        raise BoardViewContractError(
            f"{path}.auto_execution_allowed must be exactly false"
        )
    reasons = value.get("reason_codes")
    if (
        not isinstance(reasons, list)
        or any(
            not isinstance(reason, str)
            or reason not in _PERP_IDENTITY_REASON_CODES
            for reason in reasons
        )
        or len(reasons) != len(set(reasons))
    ):
        raise BoardViewContractError(f"{path}.reason_codes violates its allowlist")

    market_count = _exact_nonnegative_int(
        value.get("market_count"), path=f"{path}.market_count",
    )
    research_mapped = _exact_nonnegative_int(
        value.get("research_mapped"), path=f"{path}.research_mapped",
    )
    actionable = _exact_nonnegative_int(
        value.get("actionable_identity_count"),
        path=f"{path}.actionable_identity_count",
    )
    independent = _exact_nonnegative_int(
        value.get("independent_source_count"),
        path=f"{path}.independent_source_count",
    )
    observed_paths = _exact_nonnegative_int(
        value.get("observed_path_count"), path=f"{path}.observed_path_count",
    )
    age = _runtime_count(
        value.get("cache_age_seconds"), path=f"{path}.cache_age_seconds",
    )
    ttl = value.get("cache_ttl_seconds")
    if type(ttl) is not int or ttl != _PERP_IDENTITY_CACHE_TTL_SECONDS:
        raise BoardViewContractError(f"{path}.cache_ttl_seconds is invalid")
    if (
        market_count > _PERP_IDENTITY_MAX_MARKETS
        or research_mapped > _PERP_IDENTITY_MAX_ROWS
        or actionable > _PERP_IDENTITY_MAX_ROWS
        or independent > _PERP_IDENTITY_MAX_SOURCES
        or observed_paths > _PERP_IDENTITY_MAX_SOURCES
    ):
        raise BoardViewContractError(f"{path} count exceeds its public bound")

    if status in {"verified", "research_only"}:
        if (
            market_count == 0
            or research_mapped + actionable > market_count
            or independent == 0
            or observed_paths < independent
            or age is None
            or age >= ttl
        ):
            raise BoardViewContractError(
                f"{path} usable cache counts or age are inconsistent"
            )
        if status == "research_only":
            expected_reasons = ["heuristic_mapping_not_actionable"]
            consistent = research_mapped > 0 and actionable == 0 and blocks is True
        else:
            expected_reasons = []
            consistent = (
                research_mapped == 0 and actionable > 0 and blocks is False
            )
    else:
        expected_reasons_by_status = {
            "blocked": {"identity_collection_blocked"},
            "invalid": {"identity_cache_invalid", "identity_projection_invalid"},
            "stale": {"identity_cache_stale"},
            "unavailable": {"identity_cache_unavailable", "identity_load_failed"},
        }
        expected_reasons = reasons
        consistent = (
            len(reasons) == 1
            and reasons[0] in expected_reasons_by_status[status]
            and blocks is True
            and market_count == 0
            and research_mapped == 0
            and actionable == 0
            and independent == 0
            and observed_paths == 0
            and age is None
        )
    if reasons != expected_reasons or not consistent:
        raise BoardViewContractError(
            f"{path} status, reasons, counts, or gate are inconsistent"
        )


def _validate_event(row: Mapping[str, Any], *, view: str, lane: str,
                    generated_at: datetime, path: str) -> None:
    _required_text(row.get("id"), path=f"{path}.id")
    if row.get("lane") != lane:
        raise BoardViewContractError(
            f"{path}.lane must be {lane!r}, got {row.get('lane')!r}"
        )
    if not isinstance(row.get("actionable_now"), bool):
        raise BoardViewContractError(f"{path}.actionable_now must be boolean")
    if row.get("auto_execution_allowed") is not False:
        raise BoardViewContractError(
            f"{path}.auto_execution_allowed must be exactly false"
        )

    _validate_cost_contract(row.get("cost_contract"), path=f"{path}.cost_contract")
    assessment = row.get("current_assessment")
    if assessment is not None:
        if not isinstance(assessment, Mapping):
            raise BoardViewContractError(f"{path}.current_assessment must be an object")
        _validate_cost_contract(
            assessment.get("cost_contract"),
            path=f"{path}.current_assessment.cost_contract",
        )

    level = row.get("action_level")
    effective = row.get("effective_decision")
    if view == "launch":
        if level not in _ACTION_LEVELS:
            raise BoardViewContractError(f"{path}.action_level is invalid")
        if level == "A4_REAL_FILL_VALIDATED":
            raise BoardViewContractError(
                f"{path} cannot publish A4 until the real-fill verifier is available"
            )
        expected = {
            "A0_BLOCKED": (False, "AVOID"),
            "A1_WATCH": (False, "WATCH"),
            "A2_PAPER_READY": (False, "WATCH"),
            "A3_MANUAL_PROBE": (True, "SMALL_PROBE"),
            "A4_REAL_FILL_VALIDATED": (False, "WATCH"),
        }[level]
        if (row.get("actionable_now"), effective) != expected:
            raise BoardViewContractError(
                f"{path} contradicts {level}: expected actionable/effective {expected}"
            )
        if level == "A3_MANUAL_PROBE":
            _validate_launch_a3(
                row, assessment=assessment, generated_at=generated_at, path=path
            )
    else:
        _validate_nonlaunch_identity(row, generated_at=generated_at, path=path)
        _validate_nonlaunch_action(
            row, view=view, generated_at=generated_at, path=path,
        )

    if row.get("actionable_now"):
        if effective not in {"SMALL_PROBE", "CLAIM_CHECK"}:
            raise BoardViewContractError(
                f"{path}.effective_decision cannot be actionable: {effective!r}"
            )
        expiry_value = ((assessment or {}).get("expires_at")
                        if isinstance(assessment, Mapping) else None)
        expiry_value = expiry_value or row.get("expires_at")
        expiry = _aware_clock(expiry_value, path=f"{path}.expires_at")
        if expiry <= generated_at:
            raise BoardViewContractError(f"{path} is actionable with an expired quote")


def validate_board_view(name: str, payload: Any, *, cadence_min: float,
                        grace_min: float) -> dict:
    """Validate one view without mutating or normalizing the caller's payload."""
    if not isinstance(payload, dict):
        raise BoardViewContractError(f"{name} board payload must be an object")
    try:
        # Pydantic validates the modeled envelope. Strict JSON serialization catches
        # NaN/Infinity and non-serializable values hidden in extra nested fields.
        envelope = BoardEnvelope.model_validate(payload)
        json.dumps(payload, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
    except (TypeError, ValueError) as exc:
        raise BoardViewContractError(f"{name} board envelope invalid: {exc}") from exc

    if envelope.view != name:
        raise BoardViewContractError(
            f"view name mismatch: output {name!r}, payload {envelope.view!r}"
        )
    if envelope.generated_at > datetime.now(timezone.utc) + timedelta(seconds=5):
        raise BoardViewContractError(f"{name} generated_at is ahead of the wall clock")
    if not math.isclose(envelope.refresh_cadence_min, cadence_min, abs_tol=1e-9):
        raise BoardViewContractError(f"{name} refresh cadence does not match policy")
    if not math.isclose(envelope.freshness_grace_min, grace_min, abs_tol=1e-9):
        raise BoardViewContractError(f"{name} freshness grace does not match policy")
    cadence_seconds = (envelope.next_expected_at - envelope.generated_at).total_seconds()
    grace_seconds = (envelope.stale_after_at - envelope.next_expected_at).total_seconds()
    if not math.isclose(cadence_seconds, cadence_min * 60, abs_tol=1e-6):
        raise BoardViewContractError(f"{name} next_expected_at does not match cadence")
    if not math.isclose(grace_seconds, grace_min * 60, abs_tol=1e-6):
        raise BoardViewContractError(f"{name} stale_after_at does not match grace")

    if name == "launch":
        _validate_launch_view(payload, generated_at=envelope.generated_at)
    if name == "stats":
        _validate_stats_view(payload)
    if name == "perps":
        _validate_perps_view(payload)
    if name == "meta":
        _validate_runtime_safety(payload.get("runtime_safety"), path="meta.runtime_safety")
        _validate_perp_identity_policy(
            payload.get("perp_identity_policy"), path="meta.perp_identity_policy",
        )
    if name == "structure":
        if (payload.get("product_metadata_time_semantics")
                != "current_inventory_metadata_not_event_time_evidence"):
            raise BoardViewContractError(
                "structure product metadata must disclose current-not-event-time semantics"
            )
        metadata_at = payload.get("product_metadata_at")
        if metadata_at is not None:
            if _aware_clock(metadata_at, path="structure.product_metadata_at") \
                    > envelope.generated_at + timedelta(seconds=5):
                raise BoardViewContractError(
                    "structure.product_metadata_at is ahead of the board clock"
                )

    collection = _CANONICAL_EVENT_COLLECTIONS.get(name)
    if collection:
        key, lane, required = collection
        rows = payload.get(key)
        if rows is None and not required:
            rows = []
        if not isinstance(rows, list):
            raise BoardViewContractError(f"{name}.{key} must be a list")
        seen: set[str] = set()
        for index, row in enumerate(rows):
            path = f"{name}.{key}[{index}]"
            if not isinstance(row, Mapping):
                raise BoardViewContractError(f"{path} must be an object")
            ident = _required_text(row.get("id"), path=f"{path}.id")
            if ident in seen:
                raise BoardViewContractError(f"{name}.{key} contains duplicate id {ident!r}")
            seen.add(ident)
            _validate_event(row, view=name, lane=lane,
                            generated_at=envelope.generated_at, path=path)
        if name == "structure":
            metadata_at = payload.get("product_metadata_at")
            metadata_clock = (
                _aware_clock(metadata_at, path="structure.product_metadata_at")
                if metadata_at is not None else None
            )
            for index, row in enumerate(rows):
                classification = row.get("instrument_classification")
                if (isinstance(classification, Mapping)
                        and classification.get("state") == "current_metadata_observed"):
                    if metadata_clock is None:
                        raise BoardViewContractError(
                            "structure.product_metadata_at cannot be null while "
                            "current metadata is published"
                        )
                    if metadata_clock < envelope.generated_at - timedelta(minutes=5):
                        raise BoardViewContractError(
                            "structure.product_metadata_at is too old for current metadata"
                        )
                    observed = _aware_clock(
                        classification.get("metadata_observed_at"),
                        path=(f"structure.events[{index}].instrument_classification."
                              "metadata_observed_at"),
                    )
                    if observed > metadata_clock:
                        raise BoardViewContractError(
                            f"structure.events[{index}] metadata observation is newer "
                            "than its sidecar"
                        )
    return payload
