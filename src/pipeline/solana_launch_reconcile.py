"""Independent finalized-epoch reconciliation for the Solana launch universe."""
from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from typing import Any

from src.pipeline import solana_launch_stream as stream
from src.pipeline import stream_health


DEFAULT_EPOCH_SLOTS = 128
DEFAULT_SAFETY_SLOTS = 64
DEFAULT_REQUIRED_CLEAN_EPOCHS = 1_440
MAX_SIGNATURE_PAGES = 50
SIGNATURE_PAGE_SIZE = 1_000


class ReconciliationError(RuntimeError):
    """An epoch cannot be sealed from complete independent evidence."""


def configured_archive_endpoint() -> str | None:
    return os.getenv("SOLANA_RECONCILIATION_RPC_URL", "").strip() or None


def _provider(rpc: object) -> str:
    return stream.rpc_provider_id(getattr(rpc, "endpoint", None))


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
    connection.commit()
    connection.execute("""CREATE TRIGGER IF NOT EXISTS
        trg_reconciliation_epoch_no_delete
        BEFORE DELETE ON reconciliation_epochs BEGIN
          SELECT RAISE(ABORT, 'sealed reconciliation epoch is immutable');
        END""")


def _rpc(rpc: object, method: str, params: list) -> Any:
    result = rpc.call(method, params)
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


def _produced_slots(rpc: object, start: int, end: int) -> list[int]:
    raw = _rpc(rpc, "getBlocks", [
        start, end, {"commitment": "finalized"},
    ])
    if not isinstance(raw, list):
        raise ReconciliationError("getBlocks returned a non-list")
    values = [_integer(value, field="produced slot") for value in raw]
    if values != sorted(set(values)) or any(value < start or value > end for value in values):
        raise ReconciliationError("getBlocks returned invalid coverage")
    return values


def _program_signatures(rpc: object, start: int, end: int) -> dict[str, int]:
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


def _canonical_launch(rpc: object, signature: str, expected_slot: int) -> tuple[dict, dict] | None:
    tx = _rpc(rpc, "getTransaction", [signature, {
        "commitment": "finalized", "encoding": "jsonParsed",
        "maxSupportedTransactionVersion": 0,
    }])
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


def _live_signatures(start: int, end: int) -> set[str]:
    connection = stream._conn()
    try:
        _ensure_schema(connection)
        return {str(row[0]) for row in connection.execute(
            """SELECT DISTINCT signature FROM raw_launch_observations
               WHERE capture_mode='live_ws' AND canonical_match=1
                 AND slot BETWEEN ? AND ?""",
            (start, end),
        ).fetchall()}
    finally:
        connection.close()


def _mark_comparison(
    *, epoch_id: str, checked_at: str, verified: set[str],
    missing: set[str], extra: set[str],
) -> None:
    connection = stream._conn()
    try:
        connection.execute("BEGIN IMMEDIATE")
        for signature in verified:
            connection.execute(
                """UPDATE raw_launches SET reconciliation_state='verified_live',
                     reconciliation_epoch_id=?,reconciled_at=? WHERE signature=?""",
                (epoch_id, checked_at, signature),
            )
        for signature, state, reason in (
            *((signature, "reconciled_backfill", "independent archive found no live capture")
              for signature in missing),
            *((signature, "extra_live", "live capture absent from finalized archive set")
              for signature in extra),
        ):
            connection.execute(
                """UPDATE raw_launches SET reconciliation_state=?,
                     reconciliation_epoch_id=?,reconciled_at=?,
                     source_conflict_at=?,source_conflict_reason=?
                   WHERE signature=?""",
                (state, epoch_id, checked_at, checked_at, reason, signature),
            )
            connection.execute(
                """UPDATE raw_launches SET qualification_state='provenance_conflict',
                     qualification_error=? WHERE signature=?
                   AND qualification_state IN
                       ('raw_unqualified','market_pending','market_error')""",
                (reason, signature),
            )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def reconcile_next_epoch(
    live_rpc: object, archive_rpc: object, *,
    now: datetime | None = None, epoch_slots: int = DEFAULT_EPOCH_SLOTS,
    safety_slots: int = DEFAULT_SAFETY_SLOTS, start_slot: int | None = None,
) -> dict:
    """Seal one complete epoch; incomplete reads never advance the cursor."""
    checked = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    size = max(1, int(epoch_slots))
    safety = max(0, int(safety_slots))
    live_provider, archive_provider = _provider(live_rpc), _provider(archive_rpc)
    if live_provider == archive_provider:
        raise ReconciliationError("live and archive providers must use different hosts")
    live_genesis = _rpc(live_rpc, "getGenesisHash", [])
    archive_genesis = _rpc(archive_rpc, "getGenesisHash", [])
    if not isinstance(live_genesis, str) or live_genesis != archive_genesis:
        raise ReconciliationError("live and archive providers disagree on genesis")
    finalized_head = min(
        _integer(_rpc(live_rpc, "getSlot", [{"commitment": "finalized"}]), field="live head"),
        _integer(_rpc(archive_rpc, "getSlot", [{"commitment": "finalized"}]), field="archive head"),
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
        _rpc(archive_rpc, "getFirstAvailableBlock", []), field="first available block",
    )
    if first_available > first:
        raise ReconciliationError("archive provider cannot serve epoch start")
    live_blocks = _produced_slots(live_rpc, first, last)
    archive_blocks = _produced_slots(archive_rpc, first, last)
    if live_blocks != archive_blocks:
        raise ReconciliationError("live and archive providers disagree on produced slots")
    signatures = _program_signatures(archive_rpc, first, last)
    archive_launches: set[str] = set()
    archive_provider_id = _provider(archive_rpc)
    for signature, slot in sorted(signatures.items(), key=lambda item: (item[1], item[0])):
        launch = _canonical_launch(archive_rpc, signature, slot)
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
    live_launches = _live_signatures(first, last)
    missing = archive_launches - live_launches
    extra = live_launches - archive_launches
    epoch_id = _canonical_hash({
        "genesis": live_genesis, "from_slot": first, "to_slot": last,
        "live_provider": live_provider, "archive_provider": archive_provider,
    })[:32]
    _mark_comparison(
        epoch_id=epoch_id, checked_at=checked.isoformat(),
        verified=archive_launches & live_launches, missing=missing, extra=extra,
    )
    evidence = {
        "produced_slots_hash": _canonical_hash(archive_blocks),
        "archive_launches_hash": _canonical_hash(sorted(archive_launches)),
        "live_launches_hash": _canonical_hash(sorted(live_launches)),
    }
    status = "sealed_clean" if not missing and not extra else "sealed_breached"
    connection = stream._conn()
    try:
        _ensure_schema(connection)
        connection.execute("BEGIN IMMEDIATE")
        connection.execute(
            """INSERT INTO reconciliation_epochs(
                 epoch_id,from_slot,to_slot,live_provider,archive_provider,
                 genesis_hash,status,checked_at,produced_slots,canonical_launches,
                 live_launches,missing_live,extra_live,evidence_hash,error)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,NULL)""",
            (epoch_id, first, last, live_provider, archive_provider, live_genesis,
             status, checked.isoformat(), len(archive_blocks), len(archive_launches),
             len(live_launches), len(missing), len(extra), _canonical_hash(evidence)),
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
    }


def source_readiness(
    *, now: datetime | None = None,
    required_clean_epochs: int = DEFAULT_REQUIRED_CLEAN_EPOCHS,
    require_runtime_health: bool = True,
) -> dict:
    """Fail closed until a contiguous clean burn-in and live health both exist."""
    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    required = max(1, int(required_clean_epochs))
    connection = stream._conn()
    try:
        _ensure_schema(connection)
        epochs = connection.execute(
            """SELECT from_slot,to_slot,status,archive_provider,checked_at,
                      missing_live,extra_live FROM reconciliation_epochs
               ORDER BY to_slot DESC LIMIT ?""",
            (required,),
        ).fetchall()
    finally:
        connection.close()
    contiguous = len(epochs) == required
    if contiguous:
        for newer, older in zip(epochs, epochs[1:]):
            if int(older[1]) + 1 != int(newer[0]):
                contiguous = False
                break
    clean = contiguous and all(row[2] == "sealed_clean" for row in epochs)
    health = [row for row in stream_health.snapshot(now=current)
              if row.get("source") == "solana"] if require_runtime_health else []
    live = next((row for row in health if row.get("stream") == "pump_fun_launches"), None)
    maintenance = next((row for row in health
                        if row.get("stream") == stream.MAINTENANCE_STREAM), None)
    runtime_ok = (not require_runtime_health or bool(
        live and live.get("status") == "live" and not live.get("open_gaps")
        and maintenance and maintenance.get("status") == "live"
    ))
    reasons = []
    if len(epochs) < required:
        reasons.append("clean_epoch_burn_in_incomplete")
    elif not contiguous:
        reasons.append("reconciliation_epochs_not_contiguous")
    elif not clean:
        reasons.append("reconciliation_epoch_breached")
    if not runtime_ok:
        reasons.append("live_stream_health_not_ready")
    return {
        "state": "ready" if clean and runtime_ok else "blocked",
        "ready": bool(clean and runtime_ok),
        "required_clean_epochs": required,
        "observed_epochs": len(epochs),
        "latest_epoch": ({
            "from_slot": epochs[0][0], "to_slot": epochs[0][1],
            "status": epochs[0][2], "archive_provider": epochs[0][3],
            "checked_at": epochs[0][4], "missing_live": epochs[0][5],
            "extra_live": epochs[0][6],
        } if epochs else None),
        "runtime": {"live": live, "maintenance": maintenance},
        "reason_codes": reasons,
    }
