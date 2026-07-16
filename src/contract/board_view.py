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
                rank = {"scheduled": 0, "armed": 1, "open": 2, "breached": 3}
                if rank[newer["admission"]["state"]] < rank[older["admission"]["state"]]:
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
    from src.pipeline.edge_validation import COHORT_VERSION, PROTOCOL_ID, PROTOCOL_START_AT

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
    strict_n = sum(
        _exact_nonnegative_int(arm.get("resolved_n"), path=f"{path}.{name}.resolved_n")
        for name, arm in (("probe", probe), ("control", control))
    )
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


def _aware_clock(value: Any, *, path: str) -> datetime:
    try:
        clock = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise BoardViewContractError(f"{path} must be an ISO timestamp") from exc
    if clock.tzinfo is None:
        raise BoardViewContractError(f"{path} must include a timezone")
    return clock


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


def _validate_perps_view(payload: Mapping[str, Any]) -> None:
    for key in ("perps", "carry"):
        if key not in payload:
            raise BoardViewContractError(f"perps.{key} is required")
        _validate_observation_rows(payload[key], path=f"perps.{key}")


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
    return payload
