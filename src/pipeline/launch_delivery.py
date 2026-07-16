"""Publish and independently read back immutable Launch assessment snapshots.

The stable ``launch.json`` object is overwritten and may remain cached by Vercel
Blob.  It is therefore only a discovery surface.  A3 delivery authority comes from
one unique, never-overwritten snapshot URL whose exact bytes are fetched back before
an append-only ledger proof is written.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import re
import secrets
import urllib.request
from datetime import datetime, timezone
from typing import Callable
from urllib.parse import quote, urlparse

from src.contract.launch_probe import (
    DELIVERY_READBACK_PROOF_VERSION,
    DELIVERY_READBACK_VERIFIER_VERSION,
    DELIVERY_SLA_SECONDS,
    launch_manual_probe_failures,
    launch_delivery_subject_hash,
)

_MAX_SNAPSHOT_BYTES = 2_000_000
_SAFE_ID = re.compile(r"^[A-Za-z0-9_-]{8,128}$")


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, allow_nan=False, sort_keys=True,
        separators=(",", ":"),
    ).encode()


def _aware(value: object, *, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be an ISO timestamp") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{field} must include a timezone")
    return parsed.astimezone(timezone.utc)


def _status(response) -> int:
    value = getattr(response, "status", None)
    if value is None and hasattr(response, "getcode"):
        value = response.getcode()
    return int(value or 0)


def _content_type(response) -> str:
    headers = getattr(response, "headers", None)
    if headers is None:
        return ""
    if hasattr(headers, "get_content_type"):
        return str(headers.get_content_type() or "").lower()
    return str(headers.get("Content-Type", "")).split(";", 1)[0].strip().lower()


def _read_limited(response, *, limit: int = _MAX_SNAPSHOT_BYTES) -> bytes:
    body = response.read(limit + 1)
    if len(body) > limit:
        raise ValueError("public launch snapshot exceeds size limit")
    return body


def _validate_public_snapshot_url(value: object, *, snapshot_path: str) -> str:
    if not isinstance(value, str):
        raise ValueError("Blob upload did not return a public snapshot URL")
    parsed = urlparse(value)
    hostname = (parsed.hostname.lower().rstrip(".")
                if parsed.hostname else "")
    public_blob_host = (
        hostname == "public.blob.vercel-storage.com"
        or hostname.endswith(".public.blob.vercel-storage.com")
    )
    if (parsed.scheme != "https" or not parsed.hostname or parsed.username is not None
            or parsed.password is not None or not public_blob_host
            or parsed.query or parsed.fragment
            or parsed.path.lstrip("/") != snapshot_path):
        raise ValueError("Blob returned an invalid public snapshot URL")
    return value


def _candidate_rows(
        launch: dict, *, generated_at: datetime) -> list[tuple[str, dict]]:
    if not isinstance(launch, dict) or launch.get("view") != "launch":
        raise ValueError("launch delivery requires a launch board envelope")
    if not isinstance(launch.get("events"), list):
        raise ValueError("launch delivery events must be a list")
    candidates: list[tuple[str, dict]] = []
    seen: set[str] = set()
    for row in launch["events"]:
        if not isinstance(row, dict):
            continue
        assessment = row.get("current_assessment")
        if not isinstance(assessment, dict):
            continue
        assessment_id = assessment.get("assessment_id")
        opportunity_id = row.get("id")
        if (not isinstance(assessment_id, str) or not _SAFE_ID.fullmatch(assessment_id)
                or not isinstance(opportunity_id, str) or not opportunity_id):
            continue
        if assessment_id in seen:
            raise ValueError("launch delivery assessment_id is duplicated")
        seen.add(assessment_id)
        if (row.get("action_level") != "A2_PAPER_READY"
                or row.get("actionable_now") is not False
                or row.get("auto_execution_allowed") is not False
                or assessment.get("delivery_sla_state") != "unverified"
                or "delivery_readback" in assessment
                or assessment.get("auto_execution_allowed") is not False
                or assessment.get("kind") != "read_only_quote"):
            continue
        try:
            failures = set(launch_manual_probe_failures(
                row, assessment, row.get("evidence_gate"), now=generated_at,
            ))
        except Exception:
            continue
        if failures != {"delivery_sla_unverified", "delivery_readback_missing"}:
            continue
        candidates.append((opportunity_id, assessment))
    candidates.sort(
        key=lambda item: _aware(
            item[1].get("assessed_at"), field="assessment.assessed_at",
        ),
        reverse=True,
    )
    return candidates


def publish_and_verify_launch_snapshots(
        launch: dict, *, token: str | None = None,
        opener=urllib.request.urlopen,
        clock: Callable[[], datetime] | None = None,
        nonce_factory: Callable[[], str] | None = None,
        max_candidates: int = 5,
        timeout: float = 15.0) -> dict:
    """Upload unique A2 snapshots, exact-byte read them back, and append proofs.

    A fresh nonce is part of every pathname.  If the process crashes after PUT, the
    next run never treats a 409 or an unknown prior object as successful delivery;
    it publishes another immutable object and proves that exact URL instead.
    """
    token = token if token is not None else os.environ.get("BLOB_READ_WRITE_TOKEN", "")
    if not token:
        return {"eligible": 0, "attempted": 0, "uploaded": 0,
                "read_back": 0, "inserted": 0, "errors": 0,
                "deferred": 0,
                "state": "disabled", "reason": "missing Blob write token"}
    clock = clock or (lambda: datetime.now(timezone.utc))
    nonce_factory = nonce_factory or (lambda: secrets.token_hex(8))
    launch_generated_at = _aware(launch.get("generated_at"), field="launch.generated_at")
    candidates = _candidate_rows(launch, generated_at=launch_generated_at)
    try:
        candidate_limit = max(1, min(5, int(max_candidates)))
    except (TypeError, ValueError):
        candidate_limit = 5
    result = {"eligible": len(candidates), "attempted": 0, "uploaded": 0,
              "read_back": 0, "inserted": 0, "errors": 0,
              "deferred": max(0, len(candidates) - candidate_limit), "state": "ok"}

    from src.pipeline import opportunity_ledger as ledger

    for opportunity_id, assessment in candidates[:candidate_limit]:
        result["attempted"] += 1
        try:
            assessment_id = assessment["assessment_id"]
            assessment_sha = launch_delivery_subject_hash(assessment)
            ledger_sha = ledger.execution_assessment_payload_hash(
                assessment_id, opportunity_id,
            )
            if ledger_sha is None:
                raise ValueError("assessment is absent from the immutable ledger")
            snapshot = {
                "schema_version": 1,
                "kind": "cryptoscope_launch_assessment_snapshot",
                "verifier_version": DELIVERY_READBACK_VERIFIER_VERSION,
                "opportunity_id": opportunity_id,
                "assessment_id": assessment_id,
                "launch_generated_at": launch_generated_at.isoformat(),
                "public_assessment_sha256": assessment_sha,
                "ledger_assessment_sha256": ledger_sha,
                "auto_execution_allowed": False,
                "assessment": assessment,
            }
            body = _canonical_bytes(snapshot)
            if len(body) > _MAX_SNAPSHOT_BYTES:
                raise ValueError("launch assessment snapshot exceeds size limit")
            snapshot_sha = hashlib.sha256(body).hexdigest()
            nonce = str(nonce_factory())
            if not re.fullmatch(r"[A-Za-z0-9_-]{8,64}", nonce):
                raise ValueError("launch snapshot nonce is invalid")
            snapshot_path = (
                f"launch-snapshots/v1/{assessment_id}-{assessment_sha[:16]}-"
                f"{snapshot_sha[:16]}-{nonce}.json"
            )
            upload_url = "https://blob.vercel-storage.com/" + quote(
                snapshot_path, safe="/",
            )
            request = urllib.request.Request(
                upload_url, data=body, method="PUT", headers={
                    "Authorization": f"Bearer {token}",
                    "x-api-version": "7",
                    "x-content-type": "application/json",
                    "x-add-random-suffix": "0",
                    # Immutable objects can be cached permanently. No overwrite
                    # header is ever sent on this evidence path.
                    "x-cache-control-max-age": "31536000",
                },
            )
            with opener(request, timeout=timeout) as response:
                if _status(response) not in {200, 201}:
                    raise ValueError("immutable Blob upload failed")
                upload_response = json.loads(_read_limited(response).decode("utf-8"))
            public_url = _validate_public_snapshot_url(
                upload_response.get("url"), snapshot_path=snapshot_path,
            )
            result["uploaded"] += 1

            read_request = urllib.request.Request(
                public_url, method="GET", headers={
                    "Accept": "application/json",
                    "Cache-Control": "no-cache",
                    "User-Agent": "cryptoscope-delivery-readback/1",
                },
            )
            with opener(read_request, timeout=timeout) as response:
                if _status(response) != 200:
                    raise ValueError("public launch snapshot readback failed")
                if _content_type(response) != "application/json":
                    raise ValueError("public launch snapshot content type is not JSON")
                final_url = getattr(response, "geturl", lambda: public_url)()
                _validate_public_snapshot_url(final_url, snapshot_path=snapshot_path)
                if final_url != public_url:
                    raise ValueError("public launch snapshot redirected")
                fetched_body = _read_limited(response)
            fetched_at = _aware(clock(), field="delivery.fetched_at")
            if fetched_body != body:
                raise ValueError("public launch snapshot bytes disagree with upload")
            latency_ms = (fetched_at - launch_generated_at).total_seconds() * 1000
            if (not math.isfinite(latency_ms) or latency_ms < 0
                    or latency_ms > DELIVERY_SLA_SECONDS * 1000):
                raise ValueError("public launch snapshot missed delivery SLA")
            expires_at = _aware(assessment.get("expires_at"), field="assessment.expires_at")
            if fetched_at >= expires_at:
                raise ValueError("public launch snapshot arrived after quote expiry")
            result["read_back"] += 1

            proof = {
                "version": DELIVERY_READBACK_PROOF_VERSION,
                "verifier_version": DELIVERY_READBACK_VERIFIER_VERSION,
                "state": "pass",
                "assessment_id": assessment_id,
                "opportunity_id": opportunity_id,
                "public_url": public_url,
                "snapshot_path": snapshot_path,
                "fetched_at": fetched_at.isoformat(),
                "launch_generated_at": launch_generated_at.isoformat(),
                "delivery_latency_ms": latency_ms,
                "public_snapshot_sha256": snapshot_sha,
                "public_assessment_sha256": assessment_sha,
                "ledger_assessment_sha256": ledger_sha,
            }
            _, inserted = ledger._append_launch_delivery_readback(
                proof, assessment, fetched_body,
            )
            result["inserted"] += int(inserted)
        except Exception:
            # Detailed exception text can include provider internals; the scheduler
            # emits only aggregate state while the quote stays fail-closed at A2.
            result["errors"] += 1
    if result["errors"]:
        result["state"] = "partial" if result["inserted"] else "failed"
    return result
