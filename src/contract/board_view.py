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
    "perps": ("cascade_events", "cascade", False),
}
_ACTION_LEVELS = {
    "A0_BLOCKED", "A1_WATCH", "A2_PAPER_READY",
    "A3_MANUAL_PROBE", "A4_REAL_FILL_VALIDATED",
}


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


def _validate_event(row: Mapping[str, Any], *, view: str, lane: str,
                    generated_at: datetime, path: str) -> None:
    if not str(row.get("id") or "").strip():
        raise BoardViewContractError(f"{path}.id must be non-empty")
    if row.get("lane") != lane:
        raise BoardViewContractError(
            f"{path}.lane must be {lane!r}, got {row.get('lane')!r}"
        )
    if not isinstance(row.get("actionable_now"), bool):
        raise BoardViewContractError(f"{path}.actionable_now must be boolean")
    if row.get("auto_execution_allowed") is True:
        raise BoardViewContractError(f"{path} cannot allow automatic execution")

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
            ident = str(row.get("id") or "").strip()
            if ident in seen:
                raise BoardViewContractError(f"{name}.{key} contains duplicate id {ident!r}")
            seen.add(ident)
            _validate_event(row, view=name, lane=lane,
                            generated_at=envelope.generated_at, path=path)
    return payload
