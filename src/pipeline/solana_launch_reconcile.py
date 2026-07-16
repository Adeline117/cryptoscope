"""Independent finalized-epoch reconciliation for the Solana launch universe."""
from __future__ import annotations

import hashlib
import json
import math
import os
import time
import urllib.parse
from datetime import datetime, timezone
from typing import Any

from src.pipeline import solana_launch_stream as stream
from src.pipeline import stream_health


DEFAULT_EPOCH_SLOTS = 128
DEFAULT_SAFETY_SLOTS = 64
DEFAULT_REQUIRED_CLEAN_EPOCHS = 1_440
DEFAULT_MAX_READINESS_AGE_SECONDS = 300
DEFAULT_MAX_FINALIZED_LAG_SLOTS = 256
MAX_SIGNATURE_PAGES = 50
SIGNATURE_PAGE_SIZE = 1_000


class ReconciliationError(RuntimeError):
    """An epoch cannot be sealed from complete independent evidence."""


RECONCILIATION_RPC_ALGORITHM = "signature_pagination_transaction_hydration_v1"


def new_rpc_run_telemetry() -> dict:
    """Return a bounded, public-safe accumulator for one reconciliation attempt."""
    return {
        "version": 1,
        "algorithm": RECONCILIATION_RPC_ALGORITHM,
        "rpc_calls_total": 0,
        "rpc_failures_total": 0,
        "rpc_calls_by_method": {},
        "rpc_failures_by_method": {},
        "rpc_calls_by_role": {"live": 0, "archive": 0},
        "rpc_failures_by_role": {"live": 0, "archive": 0},
        "approx_success_response_bytes": 0,
        "run_elapsed_ms": 0,
    }


def _start_rpc_run_telemetry(telemetry: dict | None) -> dict:
    target = telemetry if telemetry is not None else {}
    if not isinstance(target, dict):
        raise TypeError("reconciliation telemetry must be a mutable object")
    target.clear()
    target.update(new_rpc_run_telemetry())
    return target


def _finish_rpc_run_telemetry(telemetry: dict, started: float) -> None:
    telemetry["run_elapsed_ms"] = max(
        0, round((time.monotonic() - started) * 1_000),
    )


def reconciliation_worker_details(
    telemetry: dict, *, outcome: str, error_kind: str | None = None,
) -> dict:
    """Bind metrics to a categorical outcome without publishing exception text."""
    rpc = new_rpc_run_telemetry()
    for key in rpc:
        if key in telemetry:
            rpc[key] = telemetry[key]
    details = {"schema_version": 1, "outcome": str(outcome), "rpc": rpc}
    if error_kind:
        details["error_kind"] = str(error_kind)[:80]
    return details


_PUBLIC_RECONCILIATION_OUTCOMES = {
    "unconfigured", "sealed_clean", "sealed_breached", "waiting_finality",
    "rpc_pressure", "failed",
}
_PUBLIC_RPC_METHODS = (
    "getGenesisHash", "getSlot", "getFirstAvailableBlock", "getBlocks",
    "getSignaturesForAddress", "getTransaction",
)
_PUBLIC_RPC_ROLES = ("live", "archive")


def _public_counter(value: object, *, maximum: int = 1_000_000_000) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value if 0 <= value <= maximum else None


def _public_counter_map(value: object, keys: tuple[str, ...]) -> dict | None:
    if not isinstance(value, dict):
        return None
    projected = {}
    for key in keys:
        count = _public_counter(value.get(key, 0))
        if count is None:
            return None
        if count:
            projected[key] = count
    return projected


def _public_category(value: object) -> str | None:
    if not isinstance(value, str) or not 0 < len(value) <= 80:
        return None
    if not all(character.isalnum() or character in "_.-" for character in value):
        return None
    return value


def _public_reconciliation_details(value: object) -> dict | None:
    if not isinstance(value, dict) or value.get("schema_version") != 1:
        return None
    outcome = value.get("outcome")
    rpc = value.get("rpc")
    if outcome not in _PUBLIC_RECONCILIATION_OUTCOMES or not isinstance(rpc, dict):
        return None
    calls = _public_counter(rpc.get("rpc_calls_total"))
    failures = _public_counter(rpc.get("rpc_failures_total"))
    response_bytes = _public_counter(
        rpc.get("approx_success_response_bytes"), maximum=10**15,
    )
    elapsed = rpc.get("run_elapsed_ms")
    by_method = _public_counter_map(
        rpc.get("rpc_calls_by_method"), _PUBLIC_RPC_METHODS,
    )
    failures_by_method = _public_counter_map(
        rpc.get("rpc_failures_by_method"), _PUBLIC_RPC_METHODS,
    )
    by_role = _public_counter_map(rpc.get("rpc_calls_by_role"), _PUBLIC_RPC_ROLES)
    failures_by_role = _public_counter_map(
        rpc.get("rpc_failures_by_role"), _PUBLIC_RPC_ROLES,
    )
    valid = (
        rpc.get("version") == 1
        and rpc.get("algorithm") == RECONCILIATION_RPC_ALGORITHM
        and calls is not None and failures is not None and failures <= calls
        and response_bytes is not None
        and not isinstance(elapsed, bool) and isinstance(elapsed, (int, float))
        and math.isfinite(elapsed) and 0 <= elapsed <= 86_400_000
        and by_method is not None and sum(by_method.values()) == calls
        and failures_by_method is not None
        and sum(failures_by_method.values()) == failures
        and by_role is not None and sum(by_role.values()) == calls
        and failures_by_role is not None
        and sum(failures_by_role.values()) == failures
    )
    if not valid:
        return None
    public = {
        "schema_version": 1,
        "outcome": outcome,
        "rpc": {
            "version": 1,
            "algorithm": RECONCILIATION_RPC_ALGORITHM,
            "rpc_calls_total": calls,
            "rpc_failures_total": failures,
            "rpc_calls_by_method": by_method,
            "rpc_failures_by_method": failures_by_method,
            "rpc_calls_by_role": by_role,
            "rpc_failures_by_role": failures_by_role,
            "approx_success_response_bytes": response_bytes,
            "run_elapsed_ms": elapsed,
        },
    }
    error_kind = _public_category(value.get("error_kind"))
    if error_kind:
        public["error_kind"] = error_kind
    return public


def public_reconciliation_health(row: object) -> dict | None:
    """Project one worker row without leaking endpoint-bearing error text."""
    if not isinstance(row, dict):
        return None
    status = row.get("status")
    if status not in {"live", "degraded", "stale"}:
        status = "unavailable"
    updated = row.get("updated_at")
    if not isinstance(updated, str):
        updated = None
    age = row.get("age_seconds")
    if (isinstance(age, bool) or not isinstance(age, (int, float))
            or not math.isfinite(age) or age < 0):
        age = None
    gaps = _public_counter(row.get("open_gaps"))
    return {
        "status": status,
        "updated_at": updated,
        "age_seconds": age,
        "stale": row.get("stale") is True,
        "open_gaps": gaps if gaps is not None else 0,
        "details": _public_reconciliation_details(row.get("details")),
    }


def configured_archive_endpoint() -> str | None:
    return os.getenv("SOLANA_RECONCILIATION_RPC_URL", "").strip() or None


def _provider(rpc: object) -> str:
    return stream.rpc_provider_id(getattr(rpc, "endpoint", None))


def _endpoint_host(endpoint: str | None) -> str:
    host = (urllib.parse.urlsplit(str(endpoint or "")).hostname or "").lower()
    if not host or host == "unknown":
        raise ReconciliationError("Solana provider host is unavailable")
    return host


def _provider_host(rpc: object) -> str:
    return _endpoint_host(getattr(rpc, "endpoint", None))


def _stored_provider_host(provider: object) -> str:
    value = str(provider or "")
    if not value.startswith("solana_rpc:"):
        raise ReconciliationError("stored Solana provider identity is invalid")
    host = value[len("solana_rpc:"):]
    if not host or host == "unknown":
        raise ReconciliationError("stored Solana provider host is unavailable")
    name, separator, port = host.rpartition(":")
    if separator and port.isdigit() and name:
        host = name
    return host.lower()


def _canonical_hash(value: object) -> str:
    return hashlib.sha256(json.dumps(
        value, sort_keys=True, separators=(",", ":"),
    ).encode()).hexdigest()


def _ensure_schema(connection) -> None:
    connection.execute("""CREATE TABLE IF NOT EXISTS reconciliation_epochs(
        epoch_id TEXT PRIMARY KEY,
        from_slot INTEGER NOT NULL,
        to_slot INTEGER NOT NULL,
        live_provider TEXT NOT NULL,
        archive_provider TEXT NOT NULL,
        genesis_hash TEXT NOT NULL,
        status TEXT NOT NULL,
        checked_at TEXT NOT NULL,
        produced_slots INTEGER NOT NULL,
        canonical_launches INTEGER NOT NULL,
        live_launches INTEGER NOT NULL,
        missing_live INTEGER NOT NULL,
        extra_live INTEGER NOT NULL,
        evidence_hash TEXT NOT NULL,
        finalized_head INTEGER NOT NULL,
        error TEXT,
        UNIQUE(from_slot,to_slot,archive_provider))""")
    connection.execute("""CREATE TABLE IF NOT EXISTS reconciliation_cursor(
        archive_provider TEXT PRIMARY KEY,
        next_slot INTEGER NOT NULL,
        updated_at TEXT NOT NULL)""")
    connection.execute("""CREATE TRIGGER IF NOT EXISTS
        trg_reconciliation_epoch_no_update
        BEFORE UPDATE ON reconciliation_epochs BEGIN
          SELECT RAISE(ABORT, 'sealed reconciliation epoch is immutable');
        END""")
    connection.execute("""CREATE TRIGGER IF NOT EXISTS
        trg_reconciliation_epoch_no_delete
        BEFORE DELETE ON reconciliation_epochs BEGIN
          SELECT RAISE(ABORT, 'sealed reconciliation epoch is immutable');
        END""")
    connection.commit()
    epoch_columns = {
        row[1] for row in connection.execute("PRAGMA table_info(reconciliation_epochs)")
    }
    if "finalized_head" not in epoch_columns:
        connection.execute("BEGIN IMMEDIATE")
        epoch_columns = {
            row[1]
            for row in connection.execute("PRAGMA table_info(reconciliation_epochs)")
        }
        if "finalized_head" not in epoch_columns:
            connection.execute(
                "ALTER TABLE reconciliation_epochs "
                "ADD COLUMN finalized_head INTEGER NOT NULL DEFAULT -1"
            )
        connection.commit()


def _rpc(
    rpc: object, method: str, params: list, *, telemetry: dict | None = None,
    role: str,
) -> Any:
    if telemetry is not None:
        telemetry["rpc_calls_total"] += 1
        by_method = telemetry["rpc_calls_by_method"]
        by_method[method] = int(by_method.get(method, 0)) + 1
        by_role = telemetry["rpc_calls_by_role"]
        by_role[role] = int(by_role.get(role, 0)) + 1
    try:
        result = rpc.call(method, params)
    except Exception:
        if telemetry is not None:
            telemetry["rpc_failures_total"] += 1
            failed_methods = telemetry["rpc_failures_by_method"]
            failed_methods[method] = int(failed_methods.get(method, 0)) + 1
            failed_roles = telemetry["rpc_failures_by_role"]
            failed_roles[role] = int(failed_roles.get(role, 0)) + 1
        raise
    if telemetry is not None:
        try:
            encoded = json.dumps(
                result, sort_keys=True, separators=(",", ":"),
                ensure_ascii=False, default=str,
            ).encode("utf-8")
            telemetry["approx_success_response_bytes"] += len(encoded)
        except Exception:
            # Telemetry must never make a valid reconciliation fail. RPC values
            # are JSON in production; exotic test doubles simply contribute 0.
            pass
    return result


def _integer(value: object, *, field: str) -> int:
    if isinstance(value, bool):
        raise ReconciliationError(f"{field} is not an integer")
    try:
        result = int(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ReconciliationError(f"{field} is not an integer") from exc
    if result < 0:
        raise ReconciliationError(f"{field} is negative")
    return result


def _produced_slots(
    rpc: object, start: int, end: int, *, telemetry: dict | None = None,
    role: str,
) -> list[int]:
    raw = _rpc(rpc, "getBlocks", [
        start, end, {"commitment": "finalized"},
    ], telemetry=telemetry, role=role)
    if not isinstance(raw, list):
        raise ReconciliationError("getBlocks returned a non-list")
    values = [_integer(value, field="produced slot") for value in raw]
    if values != sorted(set(values)) or any(value < start or value > end for value in values):
        raise ReconciliationError("getBlocks returned invalid coverage")
    return values


def _program_signatures(
    rpc: object, start: int, end: int, *, telemetry: dict | None = None,
) -> dict[str, int]:
    """Page newest-to-oldest until the archive proves it crossed epoch start."""
    before = None
    seen_cursors: set[str] = set()
    selected: dict[str, int] = {}
    crossed_start = False
    previous_slot = None
    for _page in range(MAX_SIGNATURE_PAGES):
        options: dict[str, object] = {
            "commitment": "finalized", "limit": SIGNATURE_PAGE_SIZE,
        }
        if before:
            options["before"] = before
        payload = _rpc(
            rpc, "getSignaturesForAddress", [stream.PUMP_FUN_PROGRAM, options],
            telemetry=telemetry, role="archive",
        )
        if not isinstance(payload, list):
            raise ReconciliationError("getSignaturesForAddress returned a non-list")
        if not payload:
            crossed_start = True
            break
        last_signature = None
        for item in payload:
            if not isinstance(item, dict) or not item.get("signature"):
                raise ReconciliationError("signature page contains malformed identity")
            signature = str(item["signature"])
            slot = _integer(item.get("slot"), field="signature slot")
            if previous_slot is not None and slot > previous_slot:
                raise ReconciliationError("signature page is not newest-to-oldest")
            previous_slot = slot
            last_signature = signature
            if slot < start:
                crossed_start = True
            elif slot <= end and item.get("err") is None:
                previous = selected.setdefault(signature, slot)
                if previous != slot:
                    raise ReconciliationError("signature changed slot within archive page")
        if crossed_start:
            break
        if not last_signature or last_signature in seen_cursors:
            raise ReconciliationError("signature pagination cursor did not advance")
        seen_cursors.add(last_signature)
        before = last_signature
    if not crossed_start:
        raise ReconciliationError("signature pagination did not cross epoch start")
    return selected


def _canonical_launch(
    rpc: object, signature: str, expected_slot: int, *,
    telemetry: dict | None = None,
) -> tuple[dict, dict] | None:
    tx = _rpc(rpc, "getTransaction", [signature, {
        "commitment": "finalized", "encoding": "jsonParsed",
        "maxSupportedTransactionVersion": 0,
    }], telemetry=telemetry, role="archive")
    if not isinstance(tx, dict):
        raise ReconciliationError(f"finalized transaction unavailable: {signature[:12]}")
    signatures = ((tx.get("transaction") or {}).get("signatures") or [])
    if not signatures or str(signatures[0]) != signature:
        raise ReconciliationError("hydrated signature does not match archive request")
    slot = _integer(tx.get("slot"), field="transaction slot")
    if slot != expected_slot:
        raise ReconciliationError("hydrated signature changed archive slot")
    meta = tx.get("meta") or {}
    if meta.get("err") is not None:
        return None
    logs = [str(line) for line in meta.get("logMessages") or []]
    creation = stream._creation_type(logs)
    if not creation:
        return None
    payload = {
        "kind": "launch", "signature": signature, "slot": slot,
        "transaction_index": None, "program": stream.PUMP_FUN_PROGRAM,
        "event_type": f"pump_fun_{creation.lower()}", "logs": logs,
    }
    block_time = None
    if tx.get("blockTime") is not None:
        try:
            block_time = datetime.fromtimestamp(
                float(tx["blockTime"]), tz=timezone.utc,
            )
        except (TypeError, ValueError, OverflowError, OSError) as exc:
            raise ReconciliationError("transaction blockTime is invalid") from exc
    return payload, {"transaction": tx["transaction"], "meta": meta, "slot": slot,
                     "block_time": block_time}


def _live_signatures(
    start: int, end: int, *, connection=None, live_provider: str | None = None,
) -> set[str]:
    owned = connection is None
    connection = connection or stream._conn()
    try:
        if owned:
            _ensure_schema(connection)
        return {str(row[0]) for row in connection.execute(
            """SELECT DISTINCT signature FROM raw_launch_observations
               WHERE capture_mode='live_ws' AND canonical_match=1
                 AND source_provider=? AND slot BETWEEN ? AND ?""",
            (live_provider, start, end),
        ).fetchall()}
    finally:
        if owned:
            connection.close()


def _mark_comparison(
    connection, *, epoch_id: str, checked_at: str, verified: set[str],
    missing: set[str], extra: set[str],
) -> None:
    """Attach comparisons inside the same transaction that seals the epoch."""
    for signature in verified:
        changed = connection.execute(
            """UPDATE raw_launches SET reconciliation_state='verified_live',
                 reconciliation_epoch_id=?,reconciled_at=? WHERE signature=?""",
            (epoch_id, checked_at, signature),
        ).rowcount
        if changed != 1:
            raise ReconciliationError("verified candidate disappeared before epoch seal")
    for signature, state, reason in (
        *((signature, "reconciled_backfill", "independent archive found no live capture")
          for signature in missing),
        *((signature, "extra_live", "live capture absent from finalized archive set")
          for signature in extra),
    ):
        changed = connection.execute(
            """UPDATE raw_launches SET reconciliation_state=?,
                 reconciliation_epoch_id=?,reconciled_at=?,
                 source_conflict_at=?,source_conflict_reason=?
               WHERE signature=?""",
            (state, epoch_id, checked_at, checked_at, reason, signature),
        ).rowcount
        if changed != 1:
            raise ReconciliationError("breached candidate disappeared before epoch seal")
        connection.execute(
            """UPDATE raw_launches SET qualification_state='provenance_conflict',
                 qualification_error=? WHERE signature=?
               AND qualification_state IN
                   ('raw_unqualified','market_pending','market_error')""",
            (reason, signature),
        )


def _reconcile_next_epoch(
    live_rpc: object, archive_rpc: object, *,
    now: datetime | None = None, epoch_slots: int = DEFAULT_EPOCH_SLOTS,
    safety_slots: int = DEFAULT_SAFETY_SLOTS, start_slot: int | None = None,
    telemetry: dict | None = None,
) -> dict:
    """Seal one complete epoch; incomplete reads never advance the cursor."""
    checked = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    size = max(1, int(epoch_slots))
    safety = max(0, int(safety_slots))
    live_provider, archive_provider = _provider(live_rpc), _provider(archive_rpc)
    if _provider_host(live_rpc) == _provider_host(archive_rpc):
        raise ReconciliationError("live and archive providers must use different hosts")
    live_genesis = _rpc(
        live_rpc, "getGenesisHash", [], telemetry=telemetry, role="live",
    )
    archive_genesis = _rpc(
        archive_rpc, "getGenesisHash", [], telemetry=telemetry, role="archive",
    )
    if not isinstance(live_genesis, str) or live_genesis != archive_genesis:
        raise ReconciliationError("live and archive providers disagree on genesis")
    finalized_head = min(
        _integer(_rpc(
            live_rpc, "getSlot", [{"commitment": "finalized"}],
            telemetry=telemetry, role="live",
        ), field="live head"),
        _integer(_rpc(
            archive_rpc, "getSlot", [{"commitment": "finalized"}],
            telemetry=telemetry, role="archive",
        ), field="archive head"),
    ) - safety
    if finalized_head < 0:
        return {"state": "waiting_finality", "finalized_head": finalized_head}
    connection = stream._conn()
    try:
        _ensure_schema(connection)
        cursor = connection.execute(
            "SELECT next_slot FROM reconciliation_cursor WHERE archive_provider=?",
            (archive_provider,),
        ).fetchone()
        if cursor:
            first = int(cursor[0])
        elif start_slot is not None:
            first = max(0, int(start_slot))
        else:
            first = max(0, ((finalized_head + 1) // size - 1) * size)
    finally:
        connection.close()
    last = first + size - 1
    if last > finalized_head:
        return {
            "state": "waiting_finality", "from_slot": first, "to_slot": last,
            "finalized_head": finalized_head,
        }
    first_available = _integer(
        _rpc(
            archive_rpc, "getFirstAvailableBlock", [], telemetry=telemetry,
            role="archive",
        ), field="first available block",
    )
    if first_available > first:
        raise ReconciliationError("archive provider cannot serve epoch start")
    live_blocks = _produced_slots(
        live_rpc, first, last, telemetry=telemetry, role="live",
    )
    archive_blocks = _produced_slots(
        archive_rpc, first, last, telemetry=telemetry, role="archive",
    )
    if live_blocks != archive_blocks:
        raise ReconciliationError("live and archive providers disagree on produced slots")
    signatures = _program_signatures(
        archive_rpc, first, last, telemetry=telemetry,
    )
    archive_launches: set[str] = set()
    archive_provider_id = _provider(archive_rpc)
    for signature, slot in sorted(signatures.items(), key=lambda item: (item[1], item[0])):
        launch = _canonical_launch(
            archive_rpc, signature, slot, telemetry=telemetry,
        )
        if launch is None:
            continue
        payload, transaction = launch
        stream.persist(
            payload, transaction={
                "transaction": transaction["transaction"],
                "meta": transaction["meta"], "slot": transaction["slot"],
            },
            capture_mode="finalized_reconciliation", captured_at=checked,
            block_time=transaction["block_time"], source_provider=archive_provider_id,
        )
        archive_launches.add(signature)
    epoch_id = _canonical_hash({
        "genesis": live_genesis, "from_slot": first, "to_slot": last,
        "live_provider": live_provider, "archive_provider": archive_provider,
    })[:32]
    connection = stream._conn()
    try:
        _ensure_schema(connection)
        connection.execute("BEGIN IMMEDIATE")
        # The write lock freezes the live observation set until the epoch row,
        # candidate comparisons, and cursor all commit together.
        live_launches = _live_signatures(
            first, last, connection=connection, live_provider=live_provider,
        )
        missing = archive_launches - live_launches
        extra = live_launches - archive_launches
        evidence = {
            "produced_slots_hash": _canonical_hash(archive_blocks),
            "archive_launches_hash": _canonical_hash(sorted(archive_launches)),
            "live_launches_hash": _canonical_hash(sorted(live_launches)),
        }
        status = "sealed_clean" if not missing and not extra else "sealed_breached"
        evidence_hash = _canonical_hash(evidence)
        connection.execute(
            """INSERT INTO reconciliation_epochs(
                 epoch_id,from_slot,to_slot,live_provider,archive_provider,
                 genesis_hash,status,checked_at,produced_slots,canonical_launches,
                 live_launches,missing_live,extra_live,evidence_hash,finalized_head,error)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NULL)""",
            (epoch_id, first, last, live_provider, archive_provider, live_genesis,
             status, checked.isoformat(), len(archive_blocks), len(archive_launches),
             len(live_launches), len(missing), len(extra), evidence_hash,
             finalized_head),
        )
        _mark_comparison(
            connection, epoch_id=epoch_id, checked_at=checked.isoformat(),
            verified=archive_launches & live_launches,
            missing=missing, extra=extra,
        )
        connection.execute(
            """INSERT INTO reconciliation_cursor(archive_provider,next_slot,updated_at)
               VALUES (?,?,?) ON CONFLICT(archive_provider) DO UPDATE SET
                 next_slot=excluded.next_slot,updated_at=excluded.updated_at""",
            (archive_provider, last + 1, checked.isoformat()),
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()
    return {
        "state": status, "epoch_id": epoch_id, "from_slot": first, "to_slot": last,
        "produced_slots": len(archive_blocks),
        "canonical_launches": len(archive_launches),
        "live_launches": len(live_launches),
        "missing_live": len(missing), "extra_live": len(extra),
        "live_provider": live_provider, "archive_provider": archive_provider,
        "checked_at": checked.isoformat(), "evidence_hash": evidence_hash,
        "finalized_head": finalized_head,
    }


def reconcile_next_epoch(
    live_rpc: object, archive_rpc: object, *,
    now: datetime | None = None, epoch_slots: int = DEFAULT_EPOCH_SLOTS,
    safety_slots: int = DEFAULT_SAFETY_SLOTS, start_slot: int | None = None,
    telemetry: dict | None = None,
) -> dict:
    """Run one epoch and always finalize bounded RPC cost telemetry."""
    metrics = _start_rpc_run_telemetry(telemetry)
    started = time.monotonic()
    try:
        result = _reconcile_next_epoch(
            live_rpc, archive_rpc, now=now, epoch_slots=epoch_slots,
            safety_slots=safety_slots, start_slot=start_slot, telemetry=metrics,
        )
    finally:
        _finish_rpc_run_telemetry(metrics, started)
    return {**result, "rpc_telemetry": dict(metrics)}


def reconciliation_epoch_proof(
    epoch_id: str, *, slot: int, reconciled_at: str | None = None,
) -> dict:
    """Return the immutable clean-epoch proof covering one candidate slot."""
    if not isinstance(epoch_id, str) or not epoch_id:
        raise ReconciliationError("candidate has no reconciliation epoch")
    candidate_slot = _integer(slot, field="candidate slot")
    connection = stream._conn()
    try:
        _ensure_schema(connection)
        row = connection.execute(
            """SELECT epoch_id,from_slot,to_slot,status,checked_at,live_provider,
                      archive_provider,genesis_hash,evidence_hash,finalized_head,
                      missing_live,extra_live
               FROM reconciliation_epochs WHERE epoch_id=?""",
            (epoch_id,),
        ).fetchone()
    finally:
        connection.close()
    if row is None:
        raise ReconciliationError("candidate reconciliation epoch is unavailable")
    (stored_id, first, last, status, checked_at, live_provider,
     archive_provider, genesis_hash, evidence_hash, finalized_head,
     missing_live, extra_live) = row
    if status != "sealed_clean":
        raise ReconciliationError("candidate reconciliation epoch is not clean")
    if int(missing_live) or int(extra_live):
        raise ReconciliationError("candidate reconciliation epoch has source differences")
    if not int(first) <= candidate_slot <= int(last):
        raise ReconciliationError("candidate slot is outside reconciliation epoch")
    if _stored_provider_host(live_provider) == _stored_provider_host(archive_provider):
        raise ReconciliationError("candidate reconciliation providers are not independent")
    if reconciled_at is not None and str(reconciled_at) != str(checked_at):
        raise ReconciliationError("candidate reconciliation clock changed")
    return {
        "version": 1, "epoch_id": stored_id,
        "from_slot": int(first), "to_slot": int(last), "status": status,
        "checked_at": checked_at, "live_provider": live_provider,
        "archive_provider": archive_provider, "genesis_hash": genesis_hash,
        "evidence_hash": evidence_hash, "finalized_head": int(finalized_head),
    }


def candidate_reconciliation_proof(
    signature: str, *, slot: int, mint: str,
) -> dict:
    """Prove membership without trusting a mutable ledger creator field.

    Creator is read from the append-only raw launch and matched against both
    hydration observations.  The frozen ledger source snapshot intentionally binds
    signature, slot and mint, so callers must not supply creator from payload JSON.
    """
    candidate_slot = _integer(slot, field="candidate slot")
    expected = {"signature": str(signature), "mint": str(mint)}
    if not all(value for value in expected.values()):
        raise ReconciliationError("candidate source identity is incomplete")
    connection = stream._conn()
    try:
        _ensure_schema(connection)
        row = connection.execute(
            """SELECT r.signature,r.slot,r.creator,r.mint,r.detected_at,r.captured_at,
                      r.source_provider,r.raw_payload_hash,r.hydration_payload_hash,
                      r.capture_mode,r.evidence_state,r.source_conflict_at,
                      r.reconciliation_state,r.reconciliation_epoch_id,r.reconciled_at,
                      e.from_slot,e.to_slot,e.status,e.checked_at,e.live_provider,
                      e.archive_provider,e.missing_live,e.extra_live,e.genesis_hash,
                      e.evidence_hash,e.finalized_head
                 FROM raw_launches AS r
                 JOIN reconciliation_epochs AS e
                   ON e.epoch_id=r.reconciliation_epoch_id
                WHERE r.signature=?""",
            (expected["signature"],),
        ).fetchone()
        if row is None:
            raise ReconciliationError("candidate has no sealed reconciliation membership")
        keys = (
            "signature", "slot", "creator", "mint", "detected_at", "captured_at",
            "source_provider", "raw_payload_hash", "hydration_payload_hash",
            "capture_mode", "evidence_state", "source_conflict_at",
            "reconciliation_state", "reconciliation_epoch_id", "reconciled_at",
            "from_slot", "to_slot", "status", "checked_at", "live_provider",
            "archive_provider", "missing_live", "extra_live",
            "genesis_hash", "evidence_hash", "finalized_head",
        )
        candidate = dict(zip(keys, row))
        valid = (
            candidate["signature"] == expected["signature"]
            and int(candidate["slot"]) == candidate_slot
            and candidate["mint"] == expected["mint"]
            and isinstance(candidate["creator"], str)
            and bool(candidate["creator"].strip())
            and candidate["capture_mode"] == "live_ws"
            and candidate["evidence_state"] == "complete"
            and candidate["source_conflict_at"] is None
            and candidate["reconciliation_state"] == "verified_live"
            and candidate["source_provider"] == candidate["live_provider"]
            and candidate["reconciled_at"] == candidate["checked_at"]
            and candidate["detected_at"] == candidate["captured_at"]
            and candidate["status"] == "sealed_clean"
            and not int(candidate["missing_live"])
            and not int(candidate["extra_live"])
            and int(candidate["from_slot"]) <= candidate_slot
            <= int(candidate["to_slot"])
            and _stored_provider_host(candidate["live_provider"])
            != _stored_provider_host(candidate["archive_provider"])
        )
        if not valid:
            raise ReconciliationError("candidate reconciliation membership changed")
        raw_observation = (
            candidate["signature"], candidate_slot, candidate["captured_at"],
            candidate["live_provider"], candidate["raw_payload_hash"],
        )
        live_count = connection.execute(
            """SELECT COUNT(*) FROM raw_launch_observations
                WHERE signature=? AND slot=? AND capture_mode='live_ws'
                  AND captured_at=? AND source_provider=? AND payload_hash=?
                  AND canonical_match=1""",
            raw_observation,
        ).fetchone()[0]
        archive_count = connection.execute(
            """SELECT COUNT(*) FROM raw_launch_observations
                WHERE signature=? AND slot=?
                  AND capture_mode='finalized_reconciliation'
                  AND captured_at=? AND source_provider=? AND payload_hash=?
                  AND canonical_match=1""",
            (candidate["signature"], candidate_slot, candidate["checked_at"],
             candidate["archive_provider"], candidate["raw_payload_hash"]),
        ).fetchone()[0]
        hydration_counts = []
        for provider in (candidate["live_provider"], candidate["archive_provider"]):
            hydration_counts.append(connection.execute(
                """SELECT COUNT(*) FROM hydration_observations
                    WHERE signature=? AND source_provider=? AND identity_hash=?
                      AND creator=? AND mint=? AND evidence_state='complete'""",
                (candidate["signature"], provider,
                 candidate["hydration_payload_hash"], candidate["creator"],
                 candidate["mint"]),
            ).fetchone()[0])
        if live_count != 1 or archive_count != 1 or not all(hydration_counts):
            raise ReconciliationError(
                "candidate lacks exact immutable live/archive observations"
            )
        proof = {
            "version": 1,
            "epoch_id": candidate["reconciliation_epoch_id"],
            "from_slot": int(candidate["from_slot"]),
            "to_slot": int(candidate["to_slot"]),
            "status": candidate["status"],
            "checked_at": candidate["checked_at"],
            "live_provider": candidate["live_provider"],
            "archive_provider": candidate["archive_provider"],
            "genesis_hash": candidate["genesis_hash"],
            "evidence_hash": candidate["evidence_hash"],
            "finalized_head": int(candidate["finalized_head"]),
            "live_captured_at": candidate["captured_at"],
            "live_observation_hash": candidate["raw_payload_hash"],
            "archive_observation_hash": candidate["raw_payload_hash"],
            "hydration_identity_hash": candidate["hydration_payload_hash"],
        }
        return proof
    finally:
        connection.close()


def source_readiness(
    *, now: datetime | None = None,
    required_clean_epochs: int = DEFAULT_REQUIRED_CLEAN_EPOCHS,
    require_runtime_health: bool = True,
    max_age_seconds: float = DEFAULT_MAX_READINESS_AGE_SECONDS,
    max_finalized_lag_slots: int = DEFAULT_MAX_FINALIZED_LAG_SLOTS,
) -> dict:
    """Fail closed until a contiguous clean burn-in and live health both exist."""
    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    required = max(1, int(required_clean_epochs))
    live_provider = stream.rpc_provider_id(stream.configured_rpc_endpoint())
    archive_endpoint = configured_archive_endpoint()
    archive_provider = (
        stream.rpc_provider_id(archive_endpoint) if archive_endpoint else None
    )
    try:
        max_age = float(max_age_seconds)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("max reconciliation age must be positive") from exc
    if not math.isfinite(max_age) or max_age <= 0:
        raise ValueError("max reconciliation age must be positive")
    if (isinstance(max_finalized_lag_slots, bool)
            or not isinstance(max_finalized_lag_slots, int)
            or max_finalized_lag_slots < 0):
        raise ValueError("max finalized lag slots must be a nonnegative integer")
    connection = stream._conn()
    try:
        _ensure_schema(connection)
        epochs = (connection.execute(
            """SELECT epoch_id,from_slot,to_slot,status,live_provider,
                      archive_provider,checked_at,missing_live,extra_live,
                      finalized_head
                 FROM reconciliation_epochs
                WHERE archive_provider=? AND live_provider=?
                ORDER BY to_slot DESC LIMIT ?""",
            (archive_provider, live_provider, required),
        ).fetchall() if archive_provider else [])
    finally:
        connection.close()
    contiguous = len(epochs) == required
    if contiguous:
        for newer, older in zip(epochs, epochs[1:]):
            if int(older[2]) + 1 != int(newer[1]):
                contiguous = False
                break
    clean = contiguous and all(row[3] == "sealed_clean" for row in epochs)
    latest_checked = None
    if epochs:
        try:
            latest_checked = datetime.fromisoformat(
                str(epochs[0][6]).replace("Z", "+00:00")
            )
            if latest_checked.tzinfo is None:
                raise ValueError("naive reconciliation clock")
            latest_checked = latest_checked.astimezone(timezone.utc)
        except (TypeError, ValueError, OverflowError):
            latest_checked = None
    age = ((current - latest_checked).total_seconds()
           if latest_checked is not None else None)
    fresh = bool(age is not None and 0 <= age <= max_age)
    health = [row for row in stream_health.snapshot(now=current)
              if row.get("source") == "solana"] if require_runtime_health else []
    live = next((row for row in health if row.get("stream") == "pump_fun_launches"), None)
    maintenance = next((row for row in health
                        if row.get("stream") == stream.MAINTENANCE_STREAM), None)
    reconciliation = public_reconciliation_health(next(
        (row for row in health if row.get("stream") == "pump_fun_reconciliation"),
        None,
    ))
    runtime_ok = (not require_runtime_health or bool(
        live and live.get("status") == "live" and not live.get("open_gaps")
        and maintenance and maintenance.get("status") == "live"
    ))
    sealed_lag = None
    if epochs:
        try:
            sealed_lag = int(epochs[0][9]) - int(epochs[0][2])
        except (TypeError, ValueError, OverflowError):
            sealed_lag = None
    runtime_lag = None
    if live and live.get("cursor") is not None and epochs:
        try:
            runtime_lag = max(0, int(live["cursor"]) - int(epochs[0][2]))
        except (TypeError, ValueError, OverflowError):
            runtime_lag = None
    lag_ok = bool(
        sealed_lag is not None and 0 <= sealed_lag <= max_finalized_lag_slots
        and (not require_runtime_health or (
            runtime_lag is not None and runtime_lag <= max_finalized_lag_slots
        ))
    )
    try:
        independent = bool(
            archive_endpoint
            and _endpoint_host(stream.configured_rpc_endpoint())
            != _endpoint_host(archive_endpoint)
        )
    except ReconciliationError:
        independent = False
    reasons = []
    if archive_provider is None:
        reasons.append("archive_provider_not_configured")
    elif not independent:
        reasons.append("archive_provider_not_independent")
    elif len(epochs) < required:
        reasons.append("clean_epoch_burn_in_incomplete")
    elif not contiguous:
        reasons.append("reconciliation_epochs_not_contiguous")
    elif not clean:
        reasons.append("reconciliation_epoch_breached")
    if epochs and not fresh:
        reasons.append("reconciliation_evidence_stale")
    if epochs and not lag_ok:
        reasons.append("reconciliation_slot_lag_exceeded")
    if not runtime_ok:
        reasons.append("live_stream_health_not_ready")
    ready = bool(independent and clean and fresh and lag_ok and runtime_ok)
    return {
        "state": "ready" if ready else "blocked", "ready": ready,
        "live_provider": live_provider, "archive_provider": archive_provider,
        "required_clean_epochs": required,
        "observed_epochs": len(epochs),
        "max_age_seconds": max_age, "latest_age_seconds": age,
        "max_finalized_lag_slots": max_finalized_lag_slots,
        "latest_sealed_lag_slots": sealed_lag,
        "latest_runtime_lag_slots": runtime_lag,
        "latest_epoch": ({
            "epoch_id": epochs[0][0],
            "from_slot": epochs[0][1], "to_slot": epochs[0][2],
            "status": epochs[0][3], "live_provider": epochs[0][4],
            "archive_provider": epochs[0][5], "checked_at": epochs[0][6],
            "missing_live": epochs[0][7], "extra_live": epochs[0][8],
            "finalized_head": epochs[0][9],
        } if epochs else None),
        "runtime": {
            "live": live, "maintenance": maintenance,
            "reconciliation": reconciliation,
        },
        "reason_codes": reasons,
    }
