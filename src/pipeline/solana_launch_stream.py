"""Standard Solana stream for raw Pump.fun launch evidence.

This intentionally uses the standard ``logsSubscribe`` API instead of Helius'
paid ``transactionSubscribe`` extension. A launch remains raw evidence until
the corresponding transaction proves both its mint and creator signer.
"""
from __future__ import annotations

import hashlib
import http.client
import json
import math
import os
import socket
import sqlite3
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Callable

import structlog

from src.config import DATA_DIR
from src.pipeline import stream_health
from src.pipeline.stream_runner import StreamEvent, StreamRunner

logger = structlog.get_logger()

PUMP_FUN_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
PUBLIC_SOLANA_RPC = "https://api.mainnet-beta.solana.com"
PUBLIC_SOLANA_WS = "wss://api.mainnet-beta.solana.com/"
MAX_BACKFILL_SLOTS = 16
GAP_RETRY_SLOT_BUDGET = 1
SKIPPED_SLOT_PROOF_LOOKAHEAD = 512
HYDRATION_RETRY_BASE_SECONDS = 60
HYDRATION_RETRY_MAX_SECONDS = 3600
RPC_PRESSURE_DEFAULT_COOLDOWN_SECONDS = 60
RPC_PRESSURE_MAX_COOLDOWN_SECONDS = 3600
MAINTENANCE_STREAM = "pump_fun_maintenance"
DB = DATA_DIR / "solana_launch_events.db"


class RpcPressureError(RuntimeError):
    """Transport/rate pressure that must stop all RPC work for this cycle."""

    def __init__(self, message: str, *, kind: str,
                 retry_after_seconds: int | None = None):
        super().__init__(message)
        self.kind = kind
        self.retry_after_seconds = retry_after_seconds


class TransactionUnavailableError(RuntimeError):
    """One confirmed transaction is unavailable; this is not capacity pressure."""


def _retry_after_seconds(value: object, *, now: datetime | None = None) -> int | None:
    if value is None:
        return None
    try:
        seconds = math.ceil(float(str(value).strip()))
    except (TypeError, ValueError, OverflowError):
        try:
            until = parsedate_to_datetime(str(value))
            if until.tzinfo is None:
                return None
            current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
            seconds = math.ceil((until.astimezone(timezone.utc) - current).total_seconds())
        except (TypeError, ValueError, OverflowError):
            return None
    return min(RPC_PRESSURE_MAX_COOLDOWN_SECONDS, max(0, int(seconds)))


def _as_rpc_pressure(error: Exception) -> RpcPressureError | None:
    if isinstance(error, RpcPressureError):
        return error
    if isinstance(error, urllib.error.HTTPError):
        if int(error.code) == 429:
            return RpcPressureError(str(error), kind="rate_limited")
        if int(error.code) in {502, 503, 504}:
            return RpcPressureError(str(error), kind="transport")
        return None
    if isinstance(error, (TimeoutError, socket.timeout)):
        return RpcPressureError(
            str(error) or type(error).__name__, kind="timeout")
    if isinstance(error, http.client.IncompleteRead):
        return RpcPressureError(str(error), kind="truncated")
    if isinstance(error, urllib.error.URLError):
        reason = getattr(error, "reason", None)
        if isinstance(reason, (TimeoutError, socket.timeout)):
            return RpcPressureError(str(error), kind="timeout")
        return RpcPressureError(str(error), kind="transport")
    if isinstance(error, ConnectionError):
        return RpcPressureError(str(error), kind="transport")
    return None


def _circuit_cooldown_seconds(retry_after_seconds: int | None,
                              pressure_count: int) -> int:
    exponential = RPC_PRESSURE_DEFAULT_COOLDOWN_SECONDS * (
        2 ** min(6, max(0, int(pressure_count) - 1))
    )
    return min(
        RPC_PRESSURE_MAX_COOLDOWN_SECONDS,
        max(exponential, max(0, int(retry_after_seconds or 0))),
    )


def _report_maintenance(status: str, error: str | None = None) -> None:
    """Health reporting is fail-visible but can never kill the worker it reports."""
    try:
        stream_health.report_worker(
            "solana", MAINTENANCE_STREAM, status=status, error=error,
        )
    except Exception as exc:
        logger.exception(
            "solana_launch_maintenance_health_failed", error=str(exc)[:240],
        )


def _enable_wal(c: sqlite3.Connection) -> None:
    """Enable WAL without losing a simultaneous scheduler/worker startup race."""
    deadline = time.monotonic() + 8.0
    while True:
        try:
            current = c.execute("PRAGMA journal_mode").fetchone()
            if current and str(current[0]).lower() == "wal":
                return
            changed = c.execute("PRAGMA journal_mode=WAL").fetchone()
            if changed and str(changed[0]).lower() == "wal":
                return
        except sqlite3.OperationalError as exc:
            if "locked" not in str(exc).lower() or time.monotonic() >= deadline:
                raise
        if time.monotonic() >= deadline:
            raise sqlite3.OperationalError("timed out enabling WAL journal mode")
        time.sleep(0.05)


def _conn() -> sqlite3.Connection:
    DB.parent.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(str(DB), timeout=10)
    try:
        c.execute("PRAGMA busy_timeout=8000")
        _enable_wal(c)
        c.execute("PRAGMA synchronous=NORMAL")
        c.execute("""CREATE TABLE IF NOT EXISTS raw_launches(
            signature TEXT PRIMARY KEY, slot INTEGER NOT NULL, transaction_index INTEGER,
            program TEXT NOT NULL, event_type TEXT NOT NULL, creator TEXT, mint TEXT,
            detected_at TEXT NOT NULL, hydrated_at TEXT, raw_payload_hash TEXT NOT NULL,
            hydration_payload_hash TEXT, logs TEXT NOT NULL,
            evidence_state TEXT NOT NULL DEFAULT 'raw_only', hydration_error TEXT,
            qualification_state TEXT NOT NULL DEFAULT 'raw_unqualified')""")
        migrations = (
            ("qualification_attempted_at", "TEXT"),
            ("qualification_error", "TEXT"),
            ("qualified_at", "TEXT"),
            ("ledger_event_id", "TEXT"),
            ("hydration_retry_count", "INTEGER NOT NULL DEFAULT 0"),
            ("hydration_next_retry_at", "TEXT"),
            ("hydration_attempted_at", "TEXT"),
            ("hydration_last_rpc_error", "TEXT"),
        )
        columns = {row[1] for row in c.execute("PRAGMA table_info(raw_launches)")}
        if any(name not in columns for name, _kind in migrations):
            # Scheduler and the stream process can open the same legacy evidence
            # DB together. Re-read the schema while holding the write lock so a
            # stale PRAGMA result can never trigger a duplicate ALTER TABLE.
            c.execute("BEGIN IMMEDIATE")
            columns = {row[1] for row in c.execute("PRAGMA table_info(raw_launches)")}
            for name, kind in migrations:
                if name not in columns:
                    c.execute(f"ALTER TABLE raw_launches ADD COLUMN {name} {kind}")
                    columns.add(name)
            c.commit()
        c.execute("CREATE INDEX IF NOT EXISTS idx_solana_launch_slot ON raw_launches(slot)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_solana_launch_qualification "
                  "ON raw_launches(evidence_state,qualification_state,detected_at DESC)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_solana_launch_hydration_queue "
                  "ON raw_launches(evidence_state,hydration_next_retry_at,detected_at,slot)")
        c.commit()
        return c
    except Exception:
        c.rollback()
        c.close()
        raise


QUALIFICATION_STATES = {
    "raw_unqualified", "market_pending", "market_error",
    "screened_out", "qualified_recorded", "ledger_orphan",
}
RETRYABLE_QUALIFICATION_STATES = {
    "raw_unqualified", "market_pending", "market_error",
}


def qualification_batch(*, now: datetime | None = None, limit: int = 20,
                        max_age_hours: float = 24,
                        retry_after_seconds: float = 300) -> list[dict]:
    """Return recent, identity-proven launches due for market qualification.

    Reading a row never consumes it. The caller must explicitly record an attempt,
    so a crash between selection and hydration cannot silently lose evidence.
    """
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    cutoff = now - timedelta(hours=max(0, float(max_age_hours)))
    retry_cutoff = now - timedelta(seconds=max(0, float(retry_after_seconds)))
    c = _conn()
    try:
        rows = c.execute(
            """SELECT signature,slot,event_type,creator,mint,detected_at,
                      qualification_state,qualification_attempted_at
               FROM raw_launches
               WHERE evidence_state='complete' AND mint IS NOT NULL
                 AND qualification_state IN ('raw_unqualified','market_pending','market_error')
               ORDER BY detected_at DESC LIMIT ?""",
            (max(0, int(limit)) * 5,),
        ).fetchall()
    finally:
        c.close()
    keys = ("signature", "slot", "event_type", "creator", "mint", "detected_at",
            "qualification_state", "qualification_attempted_at")
    due = []
    for row in rows:
        item = dict(zip(keys, row))
        try:
            detected = datetime.fromisoformat(item["detected_at"]).astimezone(timezone.utc)
            attempted = (datetime.fromisoformat(item["qualification_attempted_at"])
                         .astimezone(timezone.utc)
                         if item.get("qualification_attempted_at") else None)
        except (TypeError, ValueError):
            continue
        if detected < cutoff or (attempted is not None and attempted > retry_cutoff):
            continue
        due.append(item)
        if len(due) >= max(0, int(limit)):
            break
    return due


def set_qualification(signature: str, state: str, *, error: str | None = None,
                      ledger_event_id: str | None = None,
                      at: datetime | None = None) -> bool:
    """Persist one explicit qualification result without deleting raw evidence."""
    if state not in QUALIFICATION_STATES:
        raise ValueError(f"unknown qualification state: {state}")
    now = (at or datetime.now(timezone.utc)).astimezone(timezone.utc).isoformat()
    qualified_at = now if state == "qualified_recorded" else None
    c = _conn()
    try:
        changed = c.execute(
            """UPDATE raw_launches SET qualification_state=?,
                      qualification_attempted_at=?,qualification_error=?,
                      qualified_at=COALESCE(?,qualified_at),
                      ledger_event_id=COALESCE(?,ledger_event_id)
               WHERE signature=?""",
            (state, now, str(error)[:240] if error else None,
             qualified_at, ledger_event_id, signature),
        ).rowcount
        c.commit()
        return bool(changed)
    finally:
        c.close()


def qualification_summary(
    *, now: datetime | None = None, recent_hours: float = 24,
    ledger_readback: Callable[[str, str], bool] | None = None,
) -> dict:
    """Expose coverage, counting only uniquely readable ledger IDs as recorded.

    Raw stream rows are immutable evidence.  A historical row whose claimed
    ``ledger_event_id`` no longer resolves is exported as quarantined/orphaned,
    never silently included in the recorded-opportunity count.
    """
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    cutoff = (now - timedelta(hours=max(0, float(recent_hours)))).isoformat()
    c = _conn()
    try:
        evidence = dict(c.execute(
            "SELECT evidence_state,COUNT(*) FROM raw_launches GROUP BY evidence_state"
        ).fetchall())
        raw_qualification = dict(c.execute(
            "SELECT qualification_state,COUNT(*) FROM raw_launches GROUP BY qualification_state"
        ).fetchall())
        marked_recorded = c.execute(
            "SELECT ledger_event_id,mint FROM raw_launches "
            "WHERE qualification_state='qualified_recorded'"
        ).fetchall()
        recent_complete = c.execute(
            "SELECT COUNT(*) FROM raw_launches WHERE evidence_state='complete' "
            "AND detected_at>=?", (cutoff,),
        ).fetchone()[0]
        pending_total, due_pending, deferred_pending, oldest_pending_at, max_retry = \
            c.execute(
                """SELECT COUNT(*),
                          SUM(CASE WHEN hydration_next_retry_at IS NULL
                                        OR hydration_next_retry_at<=? THEN 1 ELSE 0 END),
                          SUM(CASE WHEN hydration_next_retry_at>? THEN 1 ELSE 0 END),
                          MIN(detected_at),MAX(hydration_retry_count)
                   FROM raw_launches
                   WHERE evidence_state IN ('raw_only','rpc_unavailable')""",
                (now.isoformat(), now.isoformat()),
            ).fetchone()
    finally:
        c.close()
    traceable_ids: set[str] = set()
    orphan_ids: set[str] = set()
    traceable_rows = 0
    orphan_rows = 0
    missing_id_rows = 0
    readback_error_rows = 0
    for ledger_id, mint in marked_recorded:
        valid = False
        if ledger_id and mint and ledger_readback is not None:
            try:
                valid = bool(ledger_readback(str(ledger_id), str(mint)))
            except Exception:
                readback_error_rows += 1
                continue
        elif ledger_id and mint:
            readback_error_rows += 1
            continue
        if valid:
            traceable_rows += 1
            traceable_ids.add(str(ledger_id))
        else:
            orphan_rows += 1
            if ledger_id:
                orphan_ids.add(str(ledger_id))
            else:
                missing_id_rows += 1

    qualification = dict(raw_qualification)
    qualification["qualified_recorded"] = len(traceable_ids)
    quarantined_state_rows = int(raw_qualification.get("ledger_orphan") or 0)
    if orphan_rows or quarantined_state_rows:
        qualification["ledger_orphan"] = orphan_rows + quarantined_state_rows
    oldest_pending_age_seconds = None
    if oldest_pending_at:
        try:
            oldest = datetime.fromisoformat(str(oldest_pending_at)).astimezone(timezone.utc)
            oldest_pending_age_seconds = max(0, round((now - oldest).total_seconds()))
        except (TypeError, ValueError):
            pass
    return {
        "raw_total": sum(evidence.values()),
        "evidence": evidence,
        "hydration": {
            "state": "backlogged" if pending_total else "ok",
            "pending_total": int(pending_total or 0),
            "by_state": {
                state: int(evidence.get(state) or 0)
                for state in ("raw_only", "rpc_unavailable")
            },
            "due": int(due_pending or 0),
            "deferred": int(deferred_pending or 0),
            "oldest_pending_at": oldest_pending_at,
            "oldest_pending_age_seconds": oldest_pending_age_seconds,
            "max_retry_count": int(max_retry or 0),
        },
        "qualification": qualification,
        "raw_qualification_states": raw_qualification,
        "traceability": {
            "state": ("unavailable" if readback_error_rows else
                      "ok" if not orphan_rows and not quarantined_state_rows else "partial"),
            "raw_marked_recorded_rows": len(marked_recorded),
            "traceable_rows": traceable_rows,
            "traceable_unique_ledger_events": len(traceable_ids),
            "orphan_rows": orphan_rows + quarantined_state_rows,
            "orphan_unique_ledger_ids": len(orphan_ids),
            "missing_ledger_id_rows": missing_id_rows,
            "quarantined_state_rows": quarantined_state_rows,
            "readback_error_rows": readback_error_rows,
        },
        "recent_hours": recent_hours,
        "recent_complete": recent_complete,
    }


def subscribe_requests() -> list[dict]:
    """Subscribe to Pump logs plus slots used to detect reconnect gaps."""
    return [
        {"jsonrpc": "2.0", "id": 1, "method": "logsSubscribe", "params": [
            {"mentions": [PUMP_FUN_PROGRAM]}, {"commitment": "confirmed"},
        ]},
        {"jsonrpc": "2.0", "id": 2, "method": "slotSubscribe"},
    ]


def _invoked_program(line: str) -> tuple[str, int] | None:
    prefix, marker = "Program ", " invoke ["
    if not line.startswith(prefix) or marker not in line or not line.endswith("]"):
        return None
    program, raw_depth = line[len(prefix):].split(marker, 1)
    try:
        depth = int(raw_depth[:-1])
    except ValueError:
        return None
    return (program, depth) if program and depth > 0 else None


def _exited_program(line: str) -> str | None:
    prefix = "Program "
    if not line.startswith(prefix):
        return None
    body = line[len(prefix):]
    if body.endswith(" success"):
        return body[:-len(" success")]
    if " failed:" in body:
        return body.split(" failed:", 1)[0]
    return None


def _creation_type(logs: list[str]) -> str | None:
    """Return only Create logs emitted by an active Pump invocation.

    A Pump swap commonly invokes the Associated Token Account program, which
    emits its own ``Instruction: Create`` log.  Scanning the transaction-wide
    log strings without following invocation depth would misclassify that CPI as
    a Pump launch and could make historical gap recovery permanently fail.
    """
    active: dict[int, str] = {}
    for raw_line in logs:
        line = str(raw_line).strip()
        invoked = _invoked_program(line)
        if invoked:
            program, depth = invoked
            for current_depth in tuple(active):
                if current_depth >= depth:
                    active.pop(current_depth, None)
            active[depth] = program
            continue
        exited = _exited_program(line)
        if exited:
            matching = [depth for depth, program in active.items()
                        if program == exited]
            if matching:
                depth = max(matching)
                for current_depth in tuple(active):
                    if current_depth >= depth:
                        active.pop(current_depth, None)
            continue
        if not active or active[max(active)] != PUMP_FUN_PROGRAM:
            continue
        for name in ("CreateV2", "Create"):
            if line == f"Program log: Instruction: {name}":
                return name
    return None


def parse_message(raw: object) -> StreamEvent | None:
    msg = json.loads(raw) if isinstance(raw, (str, bytes, bytearray)) else raw
    if not isinstance(msg, dict):
        raise ValueError("Solana websocket message must be an object")
    if msg.get("error"):
        raise PermissionError(f"Solana subscription rejected: {msg['error']}")
    if msg.get("id") in {1, 2} and "result" in msg:
        return None
    if msg.get("method") == "slotNotification":
        result = (msg.get("params") or {}).get("result") or {}
        if result.get("slot") is None:
            raise ValueError("Solana slot notification lacks slot")
        slot = int(result["slot"])
        return StreamEvent({"kind": "slot", "slot": slot}, cursor=slot)
    if msg.get("method") != "logsNotification":
        return None
    result = (msg.get("params") or {}).get("result") or {}
    value = result.get("value") or {}
    context = result.get("context") or {}
    signature, slot = value.get("signature"), context.get("slot")
    if not signature or slot is None:
        raise ValueError("Solana log notification lacks signature or slot")
    if value.get("err") is not None:
        return None
    logs = [str(line) for line in value.get("logs") or []]
    create_name = _creation_type(logs)
    if not create_name:
        return None
    payload = {
        "kind": "launch", "signature": str(signature), "slot": int(slot),
        "transaction_index": None, "program": PUMP_FUN_PROGRAM,
        "event_type": f"pump_fun_{create_name.lower()}", "logs": logs,
    }
    return StreamEvent(payload)


def _hash(value: object) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _insert_raw(payload: dict) -> None:
    detected_at = datetime.now(timezone.utc).isoformat()
    evidence = {key: payload.get(key) for key in (
        "signature", "slot", "transaction_index", "program", "event_type", "logs")}
    c = _conn()
    try:
        c.execute("""INSERT INTO raw_launches(
            signature,slot,transaction_index,program,event_type,detected_at,
            raw_payload_hash,logs,evidence_state,qualification_state
        ) VALUES (?,?,?,?,?,?,?,?,?,'raw_unqualified')
        ON CONFLICT(signature) DO UPDATE SET
          slot=excluded.slot,
          transaction_index=COALESCE(raw_launches.transaction_index,
                                     excluded.transaction_index)""",
                  (payload["signature"], payload["slot"],
                   payload.get("transaction_index"), payload["program"],
                   payload["event_type"], detected_at, _hash(evidence),
                   json.dumps(payload["logs"], separators=(",", ":")), "raw_only"))
        c.commit()
    finally:
        c.close()


def _message(tx: dict) -> dict:
    return ((tx.get("transaction") or {}).get("message") or {})


def _account_keys(tx: dict) -> tuple[list[str], set[str]]:
    """Resolve raw/parsed static and loaded keys without inventing signers.

    Raw JSON transactions encode signer status in the message header and encode
    instruction accounts as indexes.  Address-table keys are appended after the
    static keys, but can never be transaction signers.
    """
    message = _message(tx)
    header = message.get("header") or {}
    try:
        required_signatures = max(0, int(header.get("numRequiredSignatures") or 0))
    except (TypeError, ValueError):
        required_signatures = 0
    keys, signers = [], set()
    for index, key in enumerate(message.get("accountKeys") or []):
        value = key.get("pubkey") if isinstance(key, dict) else key
        value = str(value) if value else ""
        # Preserve the on-chain index even for malformed values.  Compressing
        # the list could make a later instruction index resolve to the wrong key.
        keys.append(value)
        if value and ((isinstance(key, dict) and key.get("signer") is True)
                      or (not isinstance(key, dict)
                          and index < required_signatures)):
            signers.add(value)
    loaded = (tx.get("meta") or {}).get("loadedAddresses") or {}
    for kind in ("writable", "readonly"):
        for key in loaded.get(kind) or []:
            value = key.get("pubkey") if isinstance(key, dict) else key
            keys.append(str(value) if value else "")
    return keys, signers


def _instructions(tx: dict):
    yield from _message(tx).get("instructions") or []
    for group in (tx.get("meta") or {}).get("innerInstructions") or []:
        yield from group.get("instructions") or []


def _indexed_value(value: object, keys: list[str]) -> str | None:
    if isinstance(value, int) and not isinstance(value, bool):
        return keys[value] if 0 <= value < len(keys) else None
    return str(value) if value else None


def _instruction_program(instruction: dict, keys: list[str]) -> str | None:
    program = instruction.get("programId")
    if program:
        return str(program)
    return _indexed_value(instruction.get("programIdIndex"), keys)


def _instruction_accounts(instruction: dict, keys: list[str]) -> list[str]:
    accounts = []
    for value in instruction.get("accounts") or []:
        resolved = _indexed_value(value, keys)
        if not resolved:
            return []
        accounts.append(resolved)
    return accounts


def _extract_identity(tx: dict) -> tuple[str | None, str | None, str | None]:
    """Cross-check the creation instruction with transaction signer metadata."""
    keys, signers = _account_keys(tx)
    for instruction in _instructions(tx):
        if not isinstance(instruction, dict):
            continue
        if _instruction_program(instruction, keys) != PUMP_FUN_PROGRAM:
            continue
        accounts = _instruction_accounts(instruction, keys)
        if not accounts:
            continue
        mint = accounts[0]
        creator_candidates = [value for value in accounts
                              if value != mint and value in signers]
        if mint in signers and len(creator_candidates) == 1:
            return creator_candidates[0], mint, None
    return None, None, "creation instruction did not prove one creator signer and mint signer"


def _hydration_retry_delay(retry_count: int) -> int:
    exponent = min(10, max(0, int(retry_count) - 1))
    return min(HYDRATION_RETRY_MAX_SECONDS,
               HYDRATION_RETRY_BASE_SECONDS * (2 ** exponent))


def _set_hydration(signature: str, tx: dict | None, error: str | None,
                   *, at: datetime | None = None,
                   retry_after_seconds: int | None = None) -> str:
    now_dt = (at or datetime.now(timezone.utc)).astimezone(timezone.utc)
    now = now_dt.isoformat()
    creator = mint = None
    if tx is not None and error is None:
        creator, mint, error = _extract_identity(tx)
    state = "complete" if creator and mint else ("rpc_unavailable" if tx is None
                                                   else "incomplete")
    c = _conn()
    try:
        c.execute("BEGIN IMMEDIATE")
        row = c.execute(
            "SELECT hydration_retry_count,evidence_state FROM raw_launches "
            "WHERE signature=?",
            (signature,),
        ).fetchone()
        if row is None:
            raise KeyError(f"unknown Solana launch signature: {signature}")
        current_retry = int(row[0] or 0)
        previous_state = str(row[1])
        rpc_failed = tx is None
        if previous_state == "complete" and state != "complete":
            # Two workers may have selected the same raw row before either wrote.
            # A slower ambiguous response must never downgrade complete identity
            # evidence committed by the winner.
            c.execute(
                "UPDATE raw_launches SET hydration_attempted_at=? WHERE signature=?",
                (now, signature),
            )
            c.commit()
            return "complete"
        if rpc_failed and previous_state in {"complete", "incomplete"}:
            # A later audit outage cannot erase a payload already captured.
            state = previous_state
        retry_count = current_retry + 1 if rpc_failed else 0
        retry_delay = min(
            HYDRATION_RETRY_MAX_SECONDS,
            max(
                _hydration_retry_delay(retry_count),
                max(0, int(retry_after_seconds or 0)),
            ),
        )
        next_retry_at = (
            now_dt + timedelta(seconds=retry_delay)
        ).isoformat() if rpc_failed else None
        if rpc_failed:
            c.execute("""UPDATE raw_launches SET hydration_attempted_at=?,
                         evidence_state=?,
                         hydration_error=CASE WHEN evidence_state IN
                           ('complete','incomplete') THEN hydration_error ELSE ? END,
                         hydration_last_rpc_error=?,hydration_retry_count=?,
                         hydration_next_retry_at=? WHERE signature=?""",
                      (now, state, str(error)[:240] if error else None,
                       str(error)[:240] if error else None, retry_count,
                       next_retry_at, signature))
        else:
            c.execute("""UPDATE raw_launches SET creator=?,mint=?,hydrated_at=?,
                         hydration_attempted_at=?,hydration_payload_hash=?,
                         evidence_state=?,hydration_error=?,
                         hydration_last_rpc_error=NULL,hydration_retry_count=0,
                         hydration_next_retry_at=NULL WHERE signature=?""",
                      (creator, mint, now, now, _hash(tx), state,
                       str(error)[:240] if error else None, signature))
        c.commit()
        return state
    except Exception:
        c.rollback()
        raise
    finally:
        c.close()


class JsonRpc:
    def __init__(self, endpoint: str, *, timeout: float = 15):
        self.endpoint = endpoint
        self.timeout = timeout

    def call(self, method: str, params: list, *, timeout: float | None = None) -> object:
        body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method,
                           "params": params}).encode()
        request = urllib.request.Request(
            self.endpoint, data=body,
            headers={"Content-Type": "application/json", "User-Agent": "CryptoScope/1.0"})
        effective_timeout = self.timeout if timeout is None else min(
            self.timeout, max(0.001, float(timeout)),
        )
        try:
            with urllib.request.urlopen(request, timeout=effective_timeout) as response:
                result = json.load(response)
        except urllib.error.HTTPError as error:
            # The exception is also the HTTP response and owns its socket.
            try:
                retry_after = _retry_after_seconds(
                    (error.headers or {}).get("Retry-After"),
                )
                code = int(error.code)
                if code == 429:
                    raise RpcPressureError(
                        f"Solana RPC {method} returned HTTP 429",
                        kind="rate_limited", retry_after_seconds=retry_after,
                    ) from error
                if code in {502, 503, 504}:
                    raise RpcPressureError(
                        f"Solana RPC {method} returned HTTP {code}",
                        kind="transport", retry_after_seconds=retry_after,
                    ) from error
                raise
            finally:
                error.close()
        except Exception as error:
            pressure = _as_rpc_pressure(error)
            if pressure is not None:
                raise pressure from error
            raise
        if result.get("error"):
            rpc_error = result["error"]
            if isinstance(rpc_error, dict) and rpc_error.get("code") == 429:
                raise RpcPressureError(
                    f"Solana RPC {method} returned JSON-RPC 429",
                    kind="rate_limited",
                )
            error = RuntimeError(f"Solana RPC {method} failed: {rpc_error}")
            pressure = _as_rpc_pressure(error)
            if pressure is not None:
                raise pressure
            raise error
        return result.get("result")


def _transaction(rpc: JsonRpc, signature: str) -> dict:
    result = rpc.call("getTransaction", [signature, {
        "commitment": "confirmed", "encoding": "jsonParsed",
        "maxSupportedTransactionVersion": 0,
    }])
    if not isinstance(result, dict):
        raise TransactionUnavailableError("confirmed transaction is not available")
    signatures = ((result.get("transaction") or {}).get("signatures") or [])
    if not signatures or str(signatures[0]) != str(signature):
        raise RuntimeError("RPC transaction signature does not match request")
    return result


def persist(payload: object, *, rpc: JsonRpc | None = None,
            transaction: dict | None = None) -> None:
    if not isinstance(payload, dict) or payload.get("kind") != "launch":
        return
    _insert_raw(payload)
    if transaction is not None:
        # A database/evidence failure must propagate to gap recovery. It is not
        # an RPC outage and must never be relabelled as one by the handler below.
        _set_hydration(payload["signature"], transaction, None)
        return
    if rpc is None:
        return
    try:
        tx = _transaction(rpc, payload["signature"])
    except Exception as exc:
        pressure = _as_rpc_pressure(exc)
        _set_hydration(
            payload["signature"], None, str(exc),
            retry_after_seconds=(pressure.retry_after_seconds if pressure else None),
        )
        logger.warning("solana_launch_hydration_failed",
                       signature=payload["signature"][:12], error=str(exc)[:120])
        return
    _set_hydration(payload["signature"], tx, None)


def rehydrate_pending(rpc: JsonRpc, *, limit: int = 100,
                      include_incomplete: bool = False,
                      now: datetime | None = None) -> dict:
    """Retry due evidence oldest-first without repeatedly guessing ambiguous rows."""
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    states = ["raw_only", "rpc_unavailable"]
    if include_incomplete:
        states.append("incomplete")
    placeholders = ",".join("?" for _ in states)
    c = _conn()
    try:
        rows = c.execute(
            f"SELECT signature FROM raw_launches WHERE evidence_state IN ({placeholders}) "
            "AND (hydration_next_retry_at IS NULL OR hydration_next_retry_at<=?) "
            "ORDER BY detected_at ASC,slot ASC,signature ASC LIMIT ?",
            (*states, now.isoformat(), max(0, int(limit))),
        ).fetchall()
    finally:
        c.close()
    attempted = completed = incomplete = unavailable = rpc_failed = pressure_failed = 0
    pressure_kind = None
    retry_after = None
    for (signature,) in rows:
        attempted += 1
        try:
            tx = _transaction(rpc, signature)
        except TransactionUnavailableError as exc:
            _set_hydration(signature, None, str(exc), at=now)
            unavailable += 1
            continue
        except Exception as exc:
            pressure = _as_rpc_pressure(exc)
            _set_hydration(
                signature, None, str(exc), at=now,
                retry_after_seconds=(pressure.retry_after_seconds if pressure else None),
            )
            if pressure is not None:
                pressure_failed += 1
                pressure_kind = pressure.kind
                retry_after = pressure.retry_after_seconds
                break
            rpc_failed += 1
            continue
        state = _set_hydration(signature, tx, None, at=now)
        if state == "complete":
            completed += 1
        else:
            incomplete += 1
    return {
        "attempted": attempted, "completed": completed,
        "incomplete": incomplete, "unavailable": unavailable,
        "rpc_failed": rpc_failed, "pressure_failed": pressure_failed,
        "pressure_kind": pressure_kind,
        "retry_after_seconds": retry_after,
    }


def retry_open_gaps(rpc: JsonRpc, *, limit: int = 10,
                    slot_budget: int = GAP_RETRY_SLOT_BUDGET) -> dict:
    gaps = stream_health.open_gaps("solana", "pump_fun_launches", limit=limit)
    attempted = recovered = progressed = failed = 0
    pressure_kind = None
    retry_after = None
    # A public Solana block can be several MB.  Verify and checkpoint at most one
    # slot per maintenance tick so historical recovery cannot starve live data.
    remaining_budget = min(GAP_RETRY_SLOT_BUDGET, max(0, int(slot_budget)))
    for gap in gaps:
        if remaining_budget <= 0:
            break
        start = int(gap["from_cursor"])
        attempted += 1
        remaining_budget -= 1
        try:
            _backfill_finalized_slot(start, rpc=rpc)
            state = stream_health.advance_gap(
                gap["id"], start,
                details={"backfilled": True, "retry": True,
                         "slot": start,
                         "gap_to": gap["to_cursor"]},
            )
            if state == "resolved":
                recovered += 1
            elif state == "advanced":
                progressed += 1
        except Exception as exc:
            failed += 1
            pressure = _as_rpc_pressure(exc)
            pressure_kind = pressure.kind if pressure else None
            retry_after = pressure.retry_after_seconds if pressure else None
            deferred = stream_health.defer_gap(
                gap["id"], str(exc),
                base_delay_seconds=max(
                    60, int(math.ceil(retry_after or 0)),
                ),
            )
            logger.warning(
                "solana_launch_backfill_failed", slot=start,
                error=str(exc)[:120],
                next_retry_at=(deferred or {}).get("next_retry_at"),
            )
    return {
        "attempted": attempted, "recovered": recovered,
        "progressed": progressed, "failed": failed,
        "pressure_kind": pressure_kind,
        "retry_after_seconds": retry_after,
    }


def _rehydrate_loop(stop: threading.Event, rpc: JsonRpc,
                    *, interval_seconds: float = 60,
                    monotonic: Callable[[], float] = time.monotonic) -> None:
    circuit_open_until = 0.0
    circuit_kind = None
    pressure_count = 0
    while not stop.is_set():
        cycle_started = monotonic()
        maintenance_status = "live"
        maintenance_error = None
        try:
            cycle_now = monotonic()
            if circuit_open_until and cycle_now < circuit_open_until:
                maintenance_status = "degraded"
                remaining = max(0, round(circuit_open_until - cycle_now))
                maintenance_error = (
                    f"RPC circuit open: {circuit_kind}; retry in {remaining}s"
                )
                logger.warning(
                    "solana_launch_rpc_circuit_open",
                    pressure_kind=circuit_kind,
                    remaining_seconds=remaining,
                )
            else:
                half_open = circuit_open_until > 0
                # Recover the oldest missing chain evidence before spending RPC
                # budget on transactions already durably present as raw evidence.
                gaps = retry_open_gaps(rpc)
                if gaps["attempted"]:
                    logger.info("solana_launch_gap_retry", **gaps)
                result = None
                if gaps["pressure_kind"] is None and not (
                    half_open and gaps["attempted"]
                ):
                    result = rehydrate_pending(rpc, limit=1 if half_open else 5)
                    if result["attempted"]:
                        logger.info("solana_launch_rehydrated", **result)

                pressure_lane = None
                pressure_kind = gaps["pressure_kind"]
                retry_after = gaps["retry_after_seconds"]
                if pressure_kind is not None:
                    pressure_lane = "gap"
                elif result is not None and result["pressure_kind"] is not None:
                    pressure_lane = "hydration"
                    pressure_kind = result["pressure_kind"]
                    retry_after = result["retry_after_seconds"]

                if pressure_lane is not None:
                    pressure_count += 1
                    cooldown = _circuit_cooldown_seconds(
                        retry_after, pressure_count,
                    )
                    circuit_open_until = monotonic() + cooldown
                    circuit_kind = pressure_kind
                    maintenance_status = "degraded"
                    maintenance_error = (
                        f"{pressure_lane} RPC pressure: {pressure_kind}; "
                        f"cooldown {cooldown}s"
                    )
                    logger.warning(
                        "solana_launch_rpc_pressure",
                        lane=pressure_lane, pressure_kind=pressure_kind,
                        retry_after_seconds=retry_after,
                        circuit_seconds=cooldown,
                    )
                elif half_open:
                    if (gaps["attempted"]
                            or (result is not None and result["attempted"])):
                        circuit_open_until = 0.0
                        circuit_kind = None
                        pressure_count = 0
                        logger.info("solana_launch_rpc_circuit_recovered")
                    else:
                        maintenance_status = "degraded"
                        maintenance_error = "RPC circuit half-open; no due probe"
                elif gaps["failed"] or (
                    result is not None and result["rpc_failed"]
                ):
                    maintenance_status = "degraded"
                    maintenance_error = "maintenance evidence recovery failed"
        except Exception as exc:
            # One transient database/provider defect must never silently kill the
            # only maintenance thread.
            logger.exception(
                "solana_launch_maintenance_failed", error=str(exc)[:240],
            )
            maintenance_status = "degraded"
            maintenance_error = f"maintenance exception: {str(exc)[:180]}"
        _report_maintenance(maintenance_status, maintenance_error)
        elapsed = monotonic() - cycle_started
        if stop.wait(max(1.0, float(interval_seconds) - elapsed)):
            break


def _launch_from_block_transaction(item: dict, slot: int, index: int) -> tuple[dict, dict] | None:
    meta = item.get("meta") or {}
    if meta.get("err") is not None:
        return None
    logs = [str(line) for line in meta.get("logMessages") or []]
    create_name = _creation_type(logs)
    tx = item.get("transaction") or {}
    if not create_name:
        return None
    normalized = {"transaction": tx, "meta": meta, "slot": int(slot)}
    signatures = tx.get("signatures") or []
    if not signatures:
        raise RuntimeError(f"slot {slot} transaction {index} has no signature")
    payload = {
        "kind": "launch", "signature": str(signatures[0]), "slot": int(slot),
        "transaction_index": int(index), "program": PUMP_FUN_PROGRAM,
        "event_type": f"pump_fun_{create_name.lower()}", "logs": logs,
    }
    return payload, normalized


def _backfill_finalized_slot(slot: int, *, rpc: JsonRpc) -> None:
    """Verify one finalized produced/skipped slot, raising on partial evidence."""
    slot = int(slot)
    finalized = int(rpc.call("getSlot", [{"commitment": "finalized"}]))
    if finalized < slot:
        raise RuntimeError(f"slot {slot} is not finalized (tip {finalized})")
    first_available = int(rpc.call("getFirstAvailableBlock", []))
    if slot < first_available:
        raise RuntimeError(
            f"slot {slot} predates first available block {first_available}")
    proof_end = min(finalized, slot + SKIPPED_SLOT_PROOF_LOOKAHEAD)
    produced = rpc.call("getBlocks", [
        slot, proof_end, {"commitment": "finalized"},
    ])
    if not isinstance(produced, list):
        raise RuntimeError("getBlocks returned a non-list result")
    if any(isinstance(value, bool) or not isinstance(value, int) for value in produced):
        raise RuntimeError("getBlocks returned invalid slot coverage")
    produced_slots = [int(value) for value in produced]
    if (produced_slots != sorted(set(produced_slots))
            or any(value < slot or value > proof_end for value in produced_slots)):
        raise RuntimeError("getBlocks returned invalid slot coverage")
    if not produced_slots:
        raise RuntimeError(
            f"slot {slot} has no bounded finalized skipped-slot proof")
    first_produced = produced_slots[0]
    if first_produced > slot:
        successor = rpc.call("getBlock", [first_produced, {
            "commitment": "finalized", "encoding": "json",
            "transactionDetails": "none", "rewards": False,
            "maxSupportedTransactionVersion": 0,
        }])
        if not isinstance(successor, dict):
            raise RuntimeError(
                f"finalized successor block {first_produced} is unavailable")
        parent = successor.get("parentSlot")
        if (isinstance(parent, bool) or not isinstance(parent, int)
                or int(parent) < 0):
            raise RuntimeError(
                f"finalized successor block {first_produced} lacks parentSlot")
        if int(parent) < slot:
            return
        raise RuntimeError(
            f"provider omitted produced slot {slot}; successor parent is {parent}")
    block = rpc.call("getBlock", [slot, {
        "commitment": "finalized", "encoding": "json",
        "transactionDetails": "full", "rewards": False,
        "maxSupportedTransactionVersion": 0,
    }])
    if not isinstance(block, dict):
        raise RuntimeError(f"finalized block {slot} is unavailable")
    transactions = block.get("transactions")
    if not isinstance(transactions, list):
        raise RuntimeError(f"finalized block {slot} has no transaction list")
    for index, item in enumerate(transactions):
        if not isinstance(item, dict):
            raise RuntimeError(f"slot {slot} transaction {index} is malformed")
        launch = _launch_from_block_transaction(item, slot, index)
        if not launch:
            continue
        payload, tx = launch
        # Gap completeness means the matching raw event was durably captured.
        # Identity qualification is a separate, retriable evidence state; a new
        # Pump account layout must not permanently pin the entire stream gap.
        persist(payload, transaction=tx)


def backfill_slots(from_slot: int, to_slot: int, *, rpc: JsonRpc) -> bool:
    """Compatibility wrapper for bounded manual recovery outside the live reader."""
    if to_slot < from_slot:
        return True
    if to_slot - from_slot + 1 > MAX_BACKFILL_SLOTS:
        return False
    try:
        for slot in range(int(from_slot), int(to_slot) + 1):
            _backfill_finalized_slot(slot, rpc=rpc)
        return True
    except Exception as exc:
        logger.warning("solana_launch_backfill_failed", from_slot=from_slot,
                       to_slot=to_slot, error=str(exc)[:120])
        return False


class _SolanaSocket:
    def __init__(self, socket):
        self.socket = socket

    def recv(self):
        return self.socket.recv()

    def ping(self):
        self.socket.ping()

    def send_json(self, payload: dict):
        self.socket.send(json.dumps(payload, separators=(",", ":")))

    def close(self):
        self.socket.close()

    def shutdown(self):
        shutdown = getattr(self.socket, "shutdown", None)
        if callable(shutdown):
            shutdown()


def build_runner(*, rpc: JsonRpc | None = None,
                 socket_factory: Callable[[], object] | None = None) -> StreamRunner:
    if rpc is None:
        rpc = JsonRpc(os.getenv("SOLANA_STREAM_RPC_URL", PUBLIC_SOLANA_RPC))
    if socket_factory is None:
        from websocket import create_connection

        endpoint = os.getenv("SOLANA_STREAM_WS_URL", PUBLIC_SOLANA_WS)
        socket_factory = lambda: create_connection(endpoint, timeout=10)

    def connect():
        return _SolanaSocket(socket_factory())

    def subscribe(ws):
        for request in subscribe_requests():
            ws.send_json(request)

    return StreamRunner(
        source="solana", stream="pump_fun_launches", connect=connect,
        subscribe=subscribe, parse=parse_message,
        # The websocket reader only records immutable raw evidence.  RPC
        # hydration and gap recovery belong to the bounded maintenance worker.
        on_event=persist,
        heartbeat_seconds=30, health_interval_seconds=1,
        expect_contiguous=True,
        backfill=None,
    )


def main() -> None:
    from dotenv import load_dotenv
    from src.config import PROJECT_ROOT

    load_dotenv(PROJECT_ROOT / ".env")
    _conn().close()
    rpc = JsonRpc(os.getenv("SOLANA_STREAM_RPC_URL", PUBLIC_SOLANA_RPC))
    stop = threading.Event()
    # Never hold up the live subscription on historical multi-MB RPC reads.
    worker = threading.Thread(target=_rehydrate_loop, args=(stop, rpc), daemon=True)
    worker.start()
    try:
        build_runner(rpc=rpc).run_forever(stop)
    finally:
        stop.set()
        worker.join(timeout=2)


if __name__ == "__main__":
    main()
