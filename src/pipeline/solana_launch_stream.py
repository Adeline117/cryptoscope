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
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Callable

import structlog

from src.config import DATA_DIR
from src.ops import stream_disk_guard
from src.pipeline import stream_health
from src.pipeline.stream_runner import StreamEvent, StreamRunner

logger = structlog.get_logger()

PUMP_FUN_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
PUBLIC_SOLANA_RPC = "https://api.mainnet-beta.solana.com"
PUBLIC_SOLANA_WS = "wss://api.mainnet-beta.solana.com/"
MAX_BACKFILL_SLOTS = 16
GAP_RETRY_SLOT_BUDGET = 1
# 16 proof units rarely fit inside GAP_WORK_BUDGET_SECONDS when blocks are
# produced (multi-MB getBlock each); the wall-clock deadline is the real
# limiter and _adjust_gap_budget backs off gently when it is hit.
GAP_RETRY_MAX_SLOT_BUDGET = 16
GAP_RETRY_RAMP_CLEAN_CYCLES = 2
SKIPPED_SLOT_PROOF_LOOKAHEAD = 512
HYDRATION_RETRY_BASE_SECONDS = 60
HYDRATION_RETRY_MAX_SECONDS = 3600
HYDRATION_BATCH_LIMIT = 5
HYDRATION_MAX_BATCH_LIMIT = 20
HYDRATION_BATCH_STEP = 5
HYDRATION_RAMP_CLEAN_CYCLES = 2
# Soft wall-clock budgets: no new RPC starts after its deadline and production
# socket timeouts are clamped to remaining time. Synchronous JSON decoding cannot
# be pre-empted safely, so these are not advertised as hard real-time limits.
MAINTENANCE_WORK_BUDGET_SECONDS = 30.0
GAP_WORK_BUDGET_SECONDS = 20.0
RPC_PRESSURE_DEFAULT_COOLDOWN_SECONDS = 60
RPC_PRESSURE_MAX_COOLDOWN_SECONDS = 3600
MAINTENANCE_STREAM = "pump_fun_maintenance"
QUALIFICATION_LEASE_SECONDS = 120
QUALIFICATION_RETRY_SECONDS = 300
QUALIFICATION_VIRGIN_FRACTION = 0.75
DB = DATA_DIR / "solana_launch_events.db"


def configured_rpc_endpoint() -> str:
    """Prefer a stream override, then the project's configured provider RPC."""
    return (os.getenv("SOLANA_STREAM_RPC_URL", "").strip()
            or os.getenv("SOLANA_RPC_URL", "").strip()
            or PUBLIC_SOLANA_RPC)


def configured_ws_endpoint() -> str:
    """Derive the matching provider websocket unless explicitly overridden."""
    explicit = os.getenv("SOLANA_STREAM_WS_URL", "").strip()
    if explicit:
        return explicit
    parsed = urllib.parse.urlsplit(configured_rpc_endpoint())
    if parsed.scheme in {"http", "https"} and parsed.netloc:
        return urllib.parse.urlunsplit((
            "wss" if parsed.scheme == "https" else "ws",
            parsed.netloc, parsed.path, parsed.query, parsed.fragment,
        ))
    return PUBLIC_SOLANA_WS


def rpc_provider_id(endpoint: str | None = None) -> str:
    """Name a provider without persisting API keys or URL query parameters."""
    parsed = urllib.parse.urlsplit(endpoint or configured_rpc_endpoint())
    host = (parsed.hostname or "unknown").lower()
    port = f":{parsed.port}" if parsed.port else ""
    return f"solana_rpc:{host}{port}"


def _capture_clock(value: datetime | str | None) -> str:
    if value is None:
        parsed = datetime.now(timezone.utc)
    elif isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("capture timestamps must be timezone-aware")
    return parsed.astimezone(timezone.utc).isoformat()


class RpcPressureError(RuntimeError):
    """Transport/rate pressure that must stop all RPC work for this cycle."""

    def __init__(self, message: str, *, kind: str,
                 retry_after_seconds: int | None = None):
        super().__init__(message)
        self.kind = kind
        self.retry_after_seconds = retry_after_seconds


class TransactionUnavailableError(RuntimeError):
    """One confirmed transaction is unavailable; this is not capacity pressure."""


class MaintenanceDeadlineExceeded(TimeoutError):
    """The bounded maintenance cycle cannot start another RPC call."""

    def __init__(self, message: str, *, work_started: bool = False):
        super().__init__(message)
        self.work_started = work_started


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
    if isinstance(error, MaintenanceDeadlineExceeded):
        return None
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


def _adjust_gap_budget(current: int, clean_cycles: int,
                       result: dict) -> tuple[int, int]:
    current = min(GAP_RETRY_MAX_SLOT_BUDGET,
                  max(GAP_RETRY_SLOT_BUDGET, int(current)))
    if result.get("pressure_kind") or int(result.get("failed") or 0):
        return GAP_RETRY_SLOT_BUDGET, 0
    if result.get("deadline_exhausted"):
        # Running out of lane wall-clock is not provider pressure: the budget
        # simply outgrew the time slice. Halve instead of restarting the ramp
        # from the floor, or a raised ceiling would sawtooth at 1.
        return max(GAP_RETRY_SLOT_BUDGET, current // 2), 0
    attempted = int(result.get("attempted") or 0)
    completed = int(result.get("recovered") or 0) + int(
        result.get("progressed") or 0)
    if attempted == 0 or attempted < current:
        return current, clean_cycles
    if completed != attempted:
        return GAP_RETRY_SLOT_BUDGET, 0
    clean_cycles = max(0, int(clean_cycles)) + 1
    if (clean_cycles >= GAP_RETRY_RAMP_CLEAN_CYCLES
            and current < GAP_RETRY_MAX_SLOT_BUDGET):
        return min(GAP_RETRY_MAX_SLOT_BUDGET, current * 2), 0
    return current, clean_cycles


def _adjust_hydration_limit(current: int, clean_cycles: int,
                            result: dict | None) -> tuple[int, int]:
    current = min(HYDRATION_MAX_BATCH_LIMIT,
                  max(HYDRATION_BATCH_LIMIT, int(current)))
    if result is None:
        return current, clean_cycles
    if (result.get("pressure_kind") or result.get("deadline_exhausted")
            or int(result.get("rpc_failed") or 0)
            or int(result.get("persistence_failed") or 0)):
        return HYDRATION_BATCH_LIMIT, 0
    attempted = int(result.get("attempted") or 0)
    if attempted == 0 or attempted < current:
        return current, clean_cycles
    classified = sum(int(result.get(key) or 0) for key in (
        "completed", "incomplete", "unavailable", "rpc_failed", "pressure_failed",
    ))
    if classified != attempted:
        return HYDRATION_BATCH_LIMIT, 0
    clean_cycles = max(0, int(clean_cycles)) + 1
    if (clean_cycles >= HYDRATION_RAMP_CLEAN_CYCLES
            and current < HYDRATION_MAX_BATCH_LIMIT):
        return min(HYDRATION_MAX_BATCH_LIMIT,
                   current + HYDRATION_BATCH_STEP), 0
    return current, clean_cycles


def _gap_probe_succeeded(result: dict) -> bool:
    """A half-open gap probe succeeds only with closed, persisted evidence."""
    attempted = int(result.get("attempted") or 0)
    completed = int(result.get("recovered") or 0) + int(
        result.get("progressed") or 0)
    return bool(
        attempted > 0
        and result.get("pressure_kind") is None
        and not result.get("deadline_exhausted")
        and not int(result.get("deadline_stopped") or 0)
        and not int(result.get("failed") or 0)
        and completed == attempted
    )


def _hydration_probe_succeeded(result: dict | None) -> bool:
    """A half-open hydration probe succeeds only with one classified outcome."""
    if result is None:
        return False
    attempted = int(result.get("attempted") or 0)
    classified = sum(int(result.get(key) or 0) for key in (
        "completed", "incomplete", "unavailable",
    ))
    return bool(
        attempted > 0
        and result.get("pressure_kind") is None
        and not result.get("deadline_exhausted")
        and not int(result.get("deadline_stopped") or 0)
        and not int(result.get("rpc_failed") or 0)
        and not int(result.get("pressure_failed") or 0)
        and not int(result.get("persistence_failed") or 0)
        and classified == attempted
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


def _rpc_call(rpc: object, method: str, params: list, *,
              deadline: float | None = None,
              monotonic: Callable[[], float] = time.monotonic) -> object:
    if deadline is None:
        return rpc.call(method, params)
    remaining = float(deadline) - monotonic()
    if remaining <= 0:
        raise MaintenanceDeadlineExceeded(
            f"maintenance deadline reached before Solana RPC {method}",
            work_started=False,
        )
    if isinstance(rpc, JsonRpc):
        try:
            return rpc.call(method, params, timeout=remaining)
        except RpcPressureError as exc:
            if exc.kind == "timeout" and monotonic() >= deadline:
                raise MaintenanceDeadlineExceeded(
                    f"maintenance deadline expired during Solana RPC {method}",
                    work_started=True,
                ) from exc
            raise
    # Test/custom clients retain their established two-argument contract. The
    # deadline is still checked before every call; production network timeouts
    # are additionally clamped by JsonRpc above.
    return rpc.call(method, params)


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
            ("qualification_attempt_count", "INTEGER NOT NULL DEFAULT 0"),
            ("qualification_lease_token", "TEXT"),
            ("qualification_lease_started_at", "TEXT"),
            ("qualification_lease_expires_at", "TEXT"),
            ("qualification_next_retry_at", "TEXT"),
            ("qualification_last_outcome_kind", "TEXT"),
            ("capture_mode", "TEXT NOT NULL DEFAULT 'legacy_unknown'"),
            ("captured_at", "TEXT"),
            ("block_time", "TEXT"),
            ("source_provider", "TEXT"),
            ("source_conflict_at", "TEXT"),
            ("source_conflict_reason", "TEXT"),
            ("reconciliation_state", "TEXT NOT NULL DEFAULT 'unverified'"),
            ("reconciliation_epoch_id", "TEXT"),
            ("reconciled_at", "TEXT"),
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
        c.execute("""CREATE INDEX IF NOT EXISTS idx_solana_launch_qualification_due
                     ON raw_launches(
                       qualification_state,qualification_next_retry_at,
                       qualification_lease_expires_at,detected_at,slot,signature
                     ) WHERE evidence_state='complete' AND mint IS NOT NULL""")
        c.execute("""CREATE TABLE IF NOT EXISTS qualification_observations(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            signature TEXT NOT NULL,
            attempt_id TEXT NOT NULL,
            observed_at TEXT NOT NULL,
            provider TEXT NOT NULL,
            endpoint_contract TEXT NOT NULL,
            outcome_kind TEXT NOT NULL,
            response_hash TEXT,
            pair_address TEXT,
            error_kind TEXT,
            error TEXT,
            UNIQUE(signature,attempt_id),
            FOREIGN KEY(signature) REFERENCES raw_launches(signature))""")
        c.execute("""CREATE TRIGGER IF NOT EXISTS trg_qualification_observation_no_update
                     BEFORE UPDATE ON qualification_observations BEGIN
                       SELECT RAISE(ABORT, 'qualification observation is append-only');
                     END""")
        c.execute("""CREATE TRIGGER IF NOT EXISTS trg_qualification_observation_no_delete
                     BEFORE DELETE ON qualification_observations BEGIN
                       SELECT RAISE(ABORT, 'qualification observation is append-only');
                     END""")
        c.execute("""CREATE TABLE IF NOT EXISTS qualification_provider_health(
            provider TEXT PRIMARY KEY,
            status TEXT NOT NULL,
            checked_at TEXT NOT NULL,
            last_error TEXT,
            failure_count INTEGER NOT NULL DEFAULT 0,
            circuit_open_until TEXT,
            response_hash TEXT)""")
        c.execute("""CREATE TABLE IF NOT EXISTS raw_launch_observations(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            signature TEXT NOT NULL,
            slot INTEGER NOT NULL,
            transaction_index INTEGER,
            capture_mode TEXT NOT NULL,
            captured_at TEXT NOT NULL,
            block_time TEXT,
            source_provider TEXT NOT NULL,
            payload_hash TEXT NOT NULL,
            canonical_match INTEGER NOT NULL,
            UNIQUE(signature,capture_mode,source_provider,payload_hash),
            FOREIGN KEY(signature) REFERENCES raw_launches(signature))""")
        c.execute("""CREATE TABLE IF NOT EXISTS hydration_observations(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            signature TEXT NOT NULL,
            observed_at TEXT NOT NULL,
            source_provider TEXT NOT NULL,
            response_hash TEXT,
            identity_hash TEXT,
            creator TEXT,
            mint TEXT,
            evidence_state TEXT NOT NULL,
            error TEXT,
            UNIQUE(signature,source_provider,response_hash,identity_hash,evidence_state),
            FOREIGN KEY(signature) REFERENCES raw_launches(signature))""")
        for trigger, table in (
            ("trg_raw_launch_observation_no_update", "raw_launch_observations"),
            ("trg_hydration_observation_no_update", "hydration_observations"),
        ):
            c.execute(f"""CREATE TRIGGER IF NOT EXISTS {trigger}
                         BEFORE UPDATE ON {table} BEGIN
                           SELECT RAISE(ABORT, 'source observation is append-only');
                         END""")
        for trigger, table in (
            ("trg_raw_launch_observation_no_delete", "raw_launch_observations"),
            ("trg_hydration_observation_no_delete", "hydration_observations"),
        ):
            c.execute(f"""CREATE TRIGGER IF NOT EXISTS {trigger}
                         BEFORE DELETE ON {table} BEGIN
                           SELECT RAISE(ABORT, 'source observation is append-only');
                         END""")
        c.execute("""CREATE TRIGGER IF NOT EXISTS trg_solana_terminal_qualification_immutable
                     BEFORE UPDATE OF qualification_state,qualification_error,
                                      ledger_event_id,qualified_at ON raw_launches
                     WHEN OLD.qualification_state IN (
                       'screened_out','qualified_recorded','ledger_orphan',
                       'historical_raw_only','qualification_expired',
                       'provenance_conflict'
                     ) AND (
                       NEW.qualification_state IS NOT OLD.qualification_state OR
                       NEW.qualification_error IS NOT OLD.qualification_error OR
                       NEW.ledger_event_id IS NOT OLD.ledger_event_id OR
                       NEW.qualified_at IS NOT OLD.qualified_at
                     )
                     BEGIN
                       SELECT RAISE(ABORT, 'terminal qualification is immutable');
                     END""")
        c.commit()
        return c
    except Exception:
        c.rollback()
        c.close()
        raise


QUALIFICATION_STATES = {
    "raw_unqualified", "market_pending", "market_error",
    "screened_out", "qualified_recorded", "ledger_orphan",
    "historical_raw_only", "qualification_expired", "provenance_conflict",
}
RETRYABLE_QUALIFICATION_STATES = {
    "raw_unqualified", "market_pending", "market_error",
}
QUALIFICATION_OUTCOME_KINDS = {
    "valid_empty", "exact_pool_pending", "below_threshold",
    "screened_out", "qualified", "ledger_orphan", "deadline_exceeded",
}
QUALIFICATION_PROVIDER = "dexscreener"
QUALIFICATION_ENDPOINT_CONTRACT = (
    "tokens_v1_batch_prefilter_then_token_pairs_v1_exact_entry_v1"
)
CAPTURE_MODES = {"live_ws", "gap_backfill", "finalized_reconciliation"}


class SourceEvidenceConflict(RuntimeError):
    """Two observations disagree on immutable on-chain launch identity."""


def _sha256_or_none(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = str(value)
    if (len(normalized) != 64
            or any(character not in "0123456789abcdef" for character in normalized)):
        raise ValueError("response_hash must be a lowercase sha256")
    return normalized


def report_qualification_provider(
    status: str, *, error: str | None = None, response_hash: str | None = None,
    at: datetime | None = None,
) -> dict:
    """Persist one batch-level provider result and an exponential circuit."""
    if status not in {"ok", "error"}:
        raise ValueError("provider status must be ok or error")
    now = (at or datetime.now(timezone.utc)).astimezone(timezone.utc)
    response_hash = _sha256_or_none(response_hash)
    c = _conn()
    try:
        c.execute("BEGIN IMMEDIATE")
        previous = c.execute(
            "SELECT failure_count FROM qualification_provider_health WHERE provider=?",
            (QUALIFICATION_PROVIDER,),
        ).fetchone()
        failures = 0 if status == "ok" else int((previous or (0,))[0] or 0) + 1
        open_until = None
        if status == "error":
            open_until = (
                now + timedelta(seconds=_circuit_cooldown_seconds(None, failures))
            ).isoformat()
        c.execute(
            """INSERT INTO qualification_provider_health(
                   provider,status,checked_at,last_error,failure_count,
                   circuit_open_until,response_hash)
               VALUES (?,?,?,?,?,?,?)
               ON CONFLICT(provider) DO UPDATE SET
                 status=excluded.status,checked_at=excluded.checked_at,
                 last_error=excluded.last_error,
                 failure_count=excluded.failure_count,
                 circuit_open_until=excluded.circuit_open_until,
                 response_hash=excluded.response_hash""",
            (QUALIFICATION_PROVIDER, status, now.isoformat(),
             str(error)[:240] if error else None, failures, open_until,
             response_hash),
        )
        c.commit()
    except Exception:
        c.rollback()
        raise
    finally:
        c.close()
    return qualification_provider_health(now=now)


def qualification_provider_health(*, now: datetime | None = None) -> dict:
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    c = _conn()
    try:
        row = c.execute(
            """SELECT status,checked_at,last_error,failure_count,
                      circuit_open_until,response_hash
               FROM qualification_provider_health WHERE provider=?""",
            (QUALIFICATION_PROVIDER,),
        ).fetchone()
    finally:
        c.close()
    if row is None:
        return {
            "provider": QUALIFICATION_PROVIDER, "status": "unknown",
            "ready": True, "circuit_state": "closed", "checked_at": None,
            "last_error": None, "failure_count": 0,
            "circuit_open_until": None, "response_hash": None,
        }
    status, checked_at, last_error, failures, open_until, response_hash = row
    open_clock = None
    try:
        if open_until:
            open_clock = datetime.fromisoformat(str(open_until)).astimezone(timezone.utc)
    except (TypeError, ValueError, OverflowError):
        open_clock = datetime.max.replace(tzinfo=timezone.utc)
    circuit_open = open_clock is not None and open_clock > now
    return {
        "provider": QUALIFICATION_PROVIDER, "status": status,
        "ready": not circuit_open,
        "circuit_state": "open" if circuit_open else (
            "half_open" if status == "error" else "closed"
        ),
        "checked_at": checked_at, "last_error": last_error,
        "failure_count": int(failures or 0),
        "circuit_open_until": open_until, "response_hash": response_hash,
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


def claim_qualification_batch(
    *, now: datetime | None = None, limit: int = 20,
    protocol_start_at: str | None = None,
    max_source_to_decision_seconds: float | None = None,
    retry_after_seconds: float = QUALIFICATION_RETRY_SECONDS,
    lease_seconds: float = QUALIFICATION_LEASE_SECONDS,
    virgin_fraction: float = QUALIFICATION_VIRGIN_FRACTION,
    require_reconciled_live: bool = False,
) -> list[dict]:
    """Atomically lease a fair mix of virgin and retryable launch evidence.

    Historical and latency-breached rows are retained as explicit terminal
    denominator states. They are never hidden behind a SQL LIMIT or allowed to
    consume the forward protocol's live work capacity.
    """
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    requested = max(0, int(limit))
    if requested == 0:
        return []
    try:
        fraction = float(virgin_fraction)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("virgin_fraction must be between zero and one") from exc
    if not math.isfinite(fraction) or not 0 <= fraction <= 1:
        raise ValueError("virgin_fraction must be between zero and one")
    boundary = None
    if protocol_start_at is not None:
        try:
            boundary = datetime.fromisoformat(
                str(protocol_start_at).replace("Z", "+00:00")
            )
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("protocol_start_at must be timezone-aware") from exc
        if boundary.tzinfo is None:
            raise ValueError("protocol_start_at must be timezone-aware")
        boundary = boundary.astimezone(timezone.utc)
    deadline_cutoff = None
    if max_source_to_decision_seconds is not None:
        try:
            seconds = float(max_source_to_decision_seconds)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("max source-to-decision seconds must be positive") from exc
        if not math.isfinite(seconds) or seconds <= 0:
            raise ValueError("max source-to-decision seconds must be positive")
        deadline_cutoff = now - timedelta(seconds=seconds)
    retry_cutoff = now - timedelta(seconds=max(0, float(retry_after_seconds)))
    lease_expires = now + timedelta(seconds=max(1, float(lease_seconds)))
    retryable_sql = "'raw_unqualified','market_pending','market_error'"
    c = _conn()
    try:
        c.execute("BEGIN IMMEDIATE")
        available_lease = (
            "(qualification_lease_expires_at IS NULL "
            "OR qualification_lease_expires_at<=?)"
        )
        if boundary is not None:
            c.execute(
                f"""UPDATE raw_launches
                    SET qualification_state='historical_raw_only',
                        qualification_error='detected before active protocol boundary',
                        qualification_lease_token=NULL,
                        qualification_lease_started_at=NULL,
                        qualification_lease_expires_at=NULL,
                        qualification_next_retry_at=NULL,
                        qualification_last_outcome_kind='protocol_preboundary'
                    WHERE qualification_state IN ({retryable_sql})
                      AND detected_at<? AND {available_lease}""",
                (boundary.isoformat(), now.isoformat()),
            )
        if deadline_cutoff is not None:
            c.execute(
                f"""UPDATE raw_launches
                    SET qualification_state='qualification_expired',
                        qualification_error='source-to-decision deadline exceeded',
                        qualification_lease_token=NULL,
                        qualification_lease_started_at=NULL,
                        qualification_lease_expires_at=NULL,
                        qualification_next_retry_at=NULL,
                        qualification_last_outcome_kind='deadline_exceeded'
                    WHERE qualification_state IN ({retryable_sql})
                      AND detected_at<? AND {available_lease}""",
                (deadline_cutoff.isoformat(), now.isoformat()),
            )
        where = f"""evidence_state='complete' AND mint IS NOT NULL
                     AND qualification_state IN ({retryable_sql})
                     AND (
                       qualification_next_retry_at<=? OR
                       (qualification_next_retry_at IS NULL AND (
                         qualification_attempted_at IS NULL OR
                         qualification_attempted_at<=?
                       ))
                     )
                     AND {available_lease}"""
        if require_reconciled_live:
            where += """
                AND capture_mode='live_ws'
                AND captured_at=detected_at
                AND source_provider IS NOT NULL
                AND source_conflict_at IS NULL
                AND reconciliation_state='verified_live'
                AND reconciliation_epoch_id IS NOT NULL
                AND reconciled_at IS NOT NULL
                AND EXISTS (
                  SELECT 1 FROM reconciliation_epochs AS epoch
                   WHERE epoch.epoch_id=raw_launches.reconciliation_epoch_id
                     AND epoch.status='sealed_clean'
                     AND epoch.missing_live=0 AND epoch.extra_live=0
                     AND raw_launches.slot BETWEEN epoch.from_slot AND epoch.to_slot
                     AND raw_launches.source_provider=epoch.live_provider
                     AND raw_launches.reconciled_at=epoch.checked_at
                     AND EXISTS (
                       SELECT 1 FROM raw_launch_observations AS live_observation
                        WHERE live_observation.signature=raw_launches.signature
                          AND live_observation.slot=raw_launches.slot
                          AND live_observation.capture_mode='live_ws'
                          AND live_observation.captured_at=raw_launches.captured_at
                          AND live_observation.source_provider=epoch.live_provider
                          AND live_observation.payload_hash=raw_launches.raw_payload_hash
                          AND live_observation.canonical_match=1
                     )
                     AND EXISTS (
                       SELECT 1 FROM raw_launch_observations AS archive_observation
                        WHERE archive_observation.signature=raw_launches.signature
                          AND archive_observation.slot=raw_launches.slot
                          AND archive_observation.capture_mode='finalized_reconciliation'
                          AND archive_observation.captured_at=epoch.checked_at
                          AND archive_observation.source_provider=epoch.archive_provider
                          AND archive_observation.payload_hash=raw_launches.raw_payload_hash
                          AND archive_observation.canonical_match=1
                     )
                     AND EXISTS (
                       SELECT 1 FROM hydration_observations AS live_hydration
                        WHERE live_hydration.signature=raw_launches.signature
                          AND live_hydration.source_provider=epoch.live_provider
                          AND live_hydration.identity_hash=
                              raw_launches.hydration_payload_hash
                          AND live_hydration.creator=raw_launches.creator
                          AND live_hydration.mint=raw_launches.mint
                          AND live_hydration.evidence_state='complete'
                     )
                     AND EXISTS (
                       SELECT 1 FROM hydration_observations AS archive_hydration
                        WHERE archive_hydration.signature=raw_launches.signature
                          AND archive_hydration.source_provider=epoch.archive_provider
                          AND archive_hydration.identity_hash=
                              raw_launches.hydration_payload_hash
                          AND archive_hydration.creator=raw_launches.creator
                          AND archive_hydration.mint=raw_launches.mint
                          AND archive_hydration.evidence_state='complete'
                     )
                )"""
        fields = """signature,slot,event_type,creator,mint,detected_at,
                    raw_payload_hash,hydration_payload_hash,
                    qualification_state,qualification_attempted_at,
                    capture_mode,captured_at,source_provider,
                    reconciliation_state,reconciliation_epoch_id,reconciled_at"""
        params = (now.isoformat(), retry_cutoff.isoformat(), now.isoformat())
        virgin_rows = c.execute(
            f"""SELECT {fields} FROM raw_launches WHERE {where}
                  AND qualification_state='raw_unqualified'
                  AND qualification_attempted_at IS NULL
                ORDER BY detected_at ASC,slot ASC,signature ASC LIMIT ?""",
            (*params, requested),
        ).fetchall()
        retry_rows = c.execute(
            f"""SELECT {fields} FROM raw_launches WHERE {where}
                  AND NOT (qualification_state='raw_unqualified'
                           AND qualification_attempted_at IS NULL)
                ORDER BY qualification_attempted_at ASC,detected_at ASC,
                         slot ASC,signature ASC LIMIT ?""",
            (*params, requested),
        ).fetchall()
        virgin_quota = min(requested, math.ceil(requested * fraction))
        retry_quota = requested - virgin_quota
        chosen = list(virgin_rows[:virgin_quota]) + list(retry_rows[:retry_quota])
        chosen_signatures = {str(row[0]) for row in chosen}
        remainder = list(virgin_rows[virgin_quota:]) + list(retry_rows[retry_quota:])
        remainder.sort(key=lambda row: (str(row[5]), int(row[1]), str(row[0])))
        for row in remainder:
            if len(chosen) >= requested:
                break
            if str(row[0]) not in chosen_signatures:
                chosen.append(row)
                chosen_signatures.add(str(row[0]))

        keys = (
            "signature", "slot", "event_type", "creator", "mint", "detected_at",
            "raw_payload_hash", "hydration_payload_hash",
            "qualification_state", "qualification_attempted_at",
            "capture_mode", "captured_at", "source_provider",
            "reconciliation_state", "reconciliation_epoch_id", "reconciled_at",
        )
        claimed = []
        for row in chosen:
            token = uuid.uuid4().hex
            changed = c.execute(
                f"""UPDATE raw_launches
                    SET qualification_lease_token=?,qualification_lease_started_at=?,
                        qualification_lease_expires_at=?
                    WHERE signature=? AND qualification_state=?
                      AND {available_lease}""",
                (token, now.isoformat(), lease_expires.isoformat(),
                 row[0], row[8], now.isoformat()),
            ).rowcount
            if changed:
                item = dict(zip(keys, row))
                item["qualification_lease_token"] = token
                item["qualification_lease_expires_at"] = lease_expires.isoformat()
                claimed.append(item)
        c.commit()
        return claimed
    except Exception:
        c.rollback()
        raise
    finally:
        c.close()


def claim_forward_protocol_batch(
    *, now: datetime | None = None, limit: int = 20,
    protocol_start_at: str, max_source_to_decision_seconds: float,
    retry_after_seconds: float = QUALIFICATION_RETRY_SECONDS,
    lease_seconds: float = QUALIFICATION_LEASE_SECONDS,
    virgin_fraction: float = QUALIFICATION_VIRGIN_FRACTION,
) -> list[dict]:
    """Lease only candidates with exact immutable clean-epoch membership."""
    return claim_qualification_batch(
        now=now, limit=limit, protocol_start_at=protocol_start_at,
        max_source_to_decision_seconds=max_source_to_decision_seconds,
        retry_after_seconds=retry_after_seconds, lease_seconds=lease_seconds,
        virgin_fraction=virgin_fraction, require_reconciled_live=True,
    )


def set_qualification(signature: str, state: str, *, error: str | None = None,
                      ledger_event_id: str | None = None,
                      lease_token: str | None = None,
                      outcome_kind: str | None = None,
                      response_hash: str | None = None,
                      pair_address: str | None = None,
                      error_kind: str | None = None,
                      retry_after_seconds: float = QUALIFICATION_RETRY_SECONDS,
                      at: datetime | None = None) -> bool:
    """Persist one CAS-protected result without deleting raw evidence.

    A terminal result is immutable. An exact replay is idempotent, while a stale
    worker or a mismatched replay is rejected instead of regressing the row.
    """
    if state not in QUALIFICATION_STATES:
        raise ValueError(f"unknown qualification state: {state}")
    if outcome_kind is not None and outcome_kind not in QUALIFICATION_OUTCOME_KINDS:
        raise ValueError(f"unknown qualification outcome: {outcome_kind}")
    response_hash = _sha256_or_none(response_hash)
    now_dt = (at or datetime.now(timezone.utc)).astimezone(timezone.utc)
    now = now_dt.isoformat()
    qualified_at = now if state == "qualified_recorded" else None
    normalized_error = str(error)[:240] if error else None
    normalized_ledger = str(ledger_event_id) if ledger_event_id is not None else None
    next_retry_at = None
    if state in RETRYABLE_QUALIFICATION_STATES:
        try:
            retry_seconds = max(0.0, float(retry_after_seconds))
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("retry_after_seconds must be nonnegative") from exc
        if not math.isfinite(retry_seconds):
            raise ValueError("retry_after_seconds must be nonnegative")
        next_retry_at = (now_dt + timedelta(seconds=retry_seconds)).isoformat()
    c = _conn()
    try:
        c.execute("BEGIN IMMEDIATE")
        row = c.execute(
            """SELECT qualification_state,qualification_error,ledger_event_id,
                      qualification_lease_token,qualification_lease_expires_at
               FROM raw_launches WHERE signature=?""",
            (signature,),
        ).fetchone()
        if row is None:
            c.rollback()
            return False
        (current_state, current_error, current_ledger, current_lease,
         current_lease_expires_at) = row
        if current_state not in RETRYABLE_QUALIFICATION_STATES:
            exact_replay = (
                current_state == state
                and current_error == normalized_error
                and current_ledger == normalized_ledger
            )
            c.commit()
            return exact_replay
        if current_lease is not None:
            if not lease_token or str(lease_token) != str(current_lease):
                c.rollback()
                return False
            try:
                lease_expires_at = datetime.fromisoformat(
                    str(current_lease_expires_at).replace("Z", "+00:00")
                ).astimezone(timezone.utc)
            except (TypeError, ValueError, OverflowError):
                c.rollback()
                return False
            if lease_expires_at <= now_dt:
                c.rollback()
                return False
        elif lease_token is not None:
            c.rollback()
            return False
        changed = c.execute(
            """UPDATE raw_launches SET qualification_state=?,
                      qualification_attempted_at=?,
                      qualification_attempt_count=qualification_attempt_count+1,
                      qualification_next_retry_at=?,
                      qualification_last_outcome_kind=COALESCE(?,qualification_last_outcome_kind),
                      qualification_error=?,
                      qualified_at=COALESCE(?,qualified_at),
                      ledger_event_id=COALESCE(?,ledger_event_id),
                      qualification_lease_token=NULL,
                      qualification_lease_started_at=NULL,
                      qualification_lease_expires_at=NULL
               WHERE signature=? AND qualification_state=?""",
            (state, now, next_retry_at, outcome_kind, normalized_error,
             qualified_at, normalized_ledger, signature, current_state),
        ).rowcount
        if changed and current_lease is not None and outcome_kind is not None:
            c.execute(
                """INSERT INTO qualification_observations(
                       signature,attempt_id,observed_at,provider,endpoint_contract,
                       outcome_kind,response_hash,pair_address,error_kind,error)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (signature, str(current_lease), now, QUALIFICATION_PROVIDER,
                 QUALIFICATION_ENDPOINT_CONTRACT, outcome_kind, response_hash,
                 str(pair_address) if pair_address else None,
                 str(error_kind)[:80] if error_kind else None, normalized_error),
            )
        c.commit()
        return bool(changed)
    except Exception:
        c.rollback()
        raise
    finally:
        c.close()


def release_qualification_lease(signature: str, lease_token: str) -> bool:
    """Release one exact claim after a batch-level provider failure."""
    if not lease_token:
        return False
    c = _conn()
    try:
        changed = c.execute(
            """UPDATE raw_launches SET qualification_lease_token=NULL,
                      qualification_lease_started_at=NULL,
                      qualification_lease_expires_at=NULL
               WHERE signature=? AND qualification_lease_token=?
                 AND qualification_state IN
                     ('raw_unqualified','market_pending','market_error')""",
            (signature, str(lease_token)),
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
                   WHERE evidence_state IN ('raw_only','rpc_unavailable','incomplete')""",
                (now.isoformat(), now.isoformat()),
            ).fetchone()
        (qualification_pending, qualification_virgin, qualification_retry,
         qualification_leased, qualification_due, oldest_qualification_at,
         max_qualification_attempts) = c.execute(
            """SELECT COUNT(*),
                      SUM(CASE WHEN qualification_attempted_at IS NULL THEN 1 ELSE 0 END),
                      SUM(CASE WHEN qualification_attempted_at IS NOT NULL THEN 1 ELSE 0 END),
                      SUM(CASE WHEN qualification_lease_expires_at>? THEN 1 ELSE 0 END),
                      SUM(CASE WHEN (qualification_lease_expires_at IS NULL
                                         OR qualification_lease_expires_at<=?)
                                    AND (qualification_next_retry_at IS NULL
                                         OR qualification_next_retry_at<=?)
                               THEN 1 ELSE 0 END),
                      MIN(detected_at),MAX(qualification_attempt_count)
               FROM raw_launches
               WHERE evidence_state='complete' AND mint IS NOT NULL
                 AND qualification_state IN
                     ('raw_unqualified','market_pending','market_error')""",
            (now.isoformat(), now.isoformat(), now.isoformat()),
        ).fetchone()
        qualification_outcomes = dict(c.execute(
            """SELECT outcome_kind,COUNT(*) FROM qualification_observations
               GROUP BY outcome_kind"""
        ).fetchall())
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
                for state in ("raw_only", "rpc_unavailable", "incomplete")
            },
            "due": int(due_pending or 0),
            "deferred": int(deferred_pending or 0),
            "oldest_pending_at": oldest_pending_at,
            "oldest_pending_age_seconds": oldest_pending_age_seconds,
            "max_retry_count": int(max_retry or 0),
        },
        "qualification": qualification,
        "raw_qualification_states": raw_qualification,
        "qualification_queue": {
            "state": "backlogged" if qualification_pending else "ok",
            "pending_total": int(qualification_pending or 0),
            "virgin": int(qualification_virgin or 0),
            "retry": int(qualification_retry or 0),
            "leased": int(qualification_leased or 0),
            "due": int(qualification_due or 0),
            "oldest_at": oldest_qualification_at,
            "max_attempt_count": int(max_qualification_attempts or 0),
            "outcomes": qualification_outcomes,
            "provider": qualification_provider_health(now=now),
        },
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


def _insert_raw(
    payload: dict, *, capture_mode: str = "live_ws",
    captured_at: datetime | str | None = None,
    block_time: datetime | str | None = None,
    source_provider: str | None = None,
) -> None:
    if capture_mode not in CAPTURE_MODES:
        raise ValueError(f"unknown Solana capture mode: {capture_mode}")
    captured = _capture_clock(captured_at)
    normalized_block_time = _capture_clock(block_time) if block_time is not None else None
    provider = str(source_provider or rpc_provider_id())
    logs_json = json.dumps(payload["logs"], separators=(",", ":"))
    # transaction_index is an enrichment unavailable to logsSubscribe. Excluding
    # its value from the canonical hash lets finalized reconciliation prove the
    # same event without rewriting the first live payload.
    evidence = {key: payload.get(key) for key in (
        "signature", "slot", "program", "event_type", "logs")}
    evidence["transaction_index"] = None
    payload_hash = _hash(evidence)
    conflict_reason = None
    c = _conn()
    try:
        c.execute("BEGIN IMMEDIATE")
        existing = c.execute(
            """SELECT slot,transaction_index,program,event_type,logs,
                      qualification_state
               FROM raw_launches WHERE signature=?""",
            (payload["signature"],),
        ).fetchone()
        canonical_match = True
        if existing is None:
            c.execute("""INSERT INTO raw_launches(
                signature,slot,transaction_index,program,event_type,detected_at,
                raw_payload_hash,logs,evidence_state,qualification_state,
                capture_mode,captured_at,block_time,source_provider
            ) VALUES (?,?,?,?,?,?,?,?,?,'raw_unqualified',?,?,?,?)""",
                      (payload["signature"], payload["slot"],
                       payload.get("transaction_index"), payload["program"],
                       payload["event_type"], captured, payload_hash, logs_json,
                       "raw_only", capture_mode, captured, normalized_block_time,
                       provider))
        else:
            canonical_match = (
                int(existing[0]) == int(payload["slot"])
                and str(existing[2]) == str(payload["program"])
                and str(existing[3]) == str(payload["event_type"])
                and str(existing[4]) == logs_json
            )
            if canonical_match:
                c.execute(
                    """UPDATE raw_launches SET
                         transaction_index=COALESCE(transaction_index,?),
                         block_time=COALESCE(block_time,?)
                       WHERE signature=?""",
                    (payload.get("transaction_index"), normalized_block_time,
                     payload["signature"]),
                )
            else:
                conflict_reason = "same signature has conflicting slot/program/event/logs"
                c.execute(
                    """UPDATE raw_launches SET evidence_state='source_conflict',
                         source_conflict_at=?,source_conflict_reason=?
                       WHERE signature=?""",
                    (captured, conflict_reason, payload["signature"]),
                )
                if existing[5] in RETRYABLE_QUALIFICATION_STATES:
                    c.execute(
                        """UPDATE raw_launches SET
                             qualification_state='provenance_conflict',
                             qualification_error=?
                           WHERE signature=?""",
                        (conflict_reason, payload["signature"]),
                    )
        c.execute(
            """INSERT OR IGNORE INTO raw_launch_observations(
                 signature,slot,transaction_index,capture_mode,captured_at,
                 block_time,source_provider,payload_hash,canonical_match)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (payload["signature"], int(payload["slot"]),
             payload.get("transaction_index"), capture_mode, captured,
             normalized_block_time, provider, payload_hash,
             1 if canonical_match else 0),
        )
        c.commit()
        if conflict_reason:
            raise SourceEvidenceConflict(conflict_reason)
    except SourceEvidenceConflict:
        raise
    except Exception:
        c.rollback()
        raise
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
                   retry_after_seconds: int | None = None,
                   source_provider: str | None = None) -> str:
    now_dt = (at or datetime.now(timezone.utc)).astimezone(timezone.utc)
    now = now_dt.isoformat()
    provider = str(source_provider or rpc_provider_id())
    creator = mint = None
    response_hash = _hash(tx) if tx is not None else None
    identity_error = None
    if tx is not None and error is None:
        signatures = ((tx.get("transaction") or {}).get("signatures") or [])
        if not signatures:
            error = "transaction did not prove its requested signature"
        elif str(signatures[0]) != str(signature):
            identity_error = "hydration transaction signature conflicts with raw launch"
        else:
            creator, mint, error = _extract_identity(tx)
    state = "complete" if creator and mint else ("rpc_unavailable" if tx is None
                                                   else "incomplete")
    c = _conn()
    try:
        c.execute("BEGIN IMMEDIATE")
        row = c.execute(
            """SELECT hydration_retry_count,evidence_state,creator,mint,
                      slot,qualification_state
               FROM raw_launches WHERE signature=?""",
            (signature,),
        ).fetchone()
        if row is None:
            raise KeyError(f"unknown Solana launch signature: {signature}")
        current_retry = int(row[0] or 0)
        previous_state = str(row[1])
        previous_creator, previous_mint = row[2:4]
        raw_slot, qualification_state = int(row[4]), str(row[5])
        tx_slot = tx.get("slot") if isinstance(tx, dict) else None
        if (tx_slot is not None
                and (isinstance(tx_slot, bool) or int(tx_slot) != raw_slot)):
            identity_error = "hydration transaction slot conflicts with raw launch"
        identity_hash = (
            _hash({
                "signature": signature, "slot": raw_slot,
                "program": PUMP_FUN_PROGRAM, "creator": creator, "mint": mint,
            }) if creator and mint else None
        )
        if previous_state == "complete" and state == "complete" \
                and (str(previous_creator) != str(creator)
                     or str(previous_mint) != str(mint)):
            identity_error = "complete hydration identity conflicts with first observation"
        if identity_error:
            state = "source_conflict"
            c.execute(
                """UPDATE raw_launches SET evidence_state='source_conflict',
                     source_conflict_at=?,source_conflict_reason=?,
                     hydration_attempted_at=? WHERE signature=?""",
                (now, identity_error, now, signature),
            )
            if qualification_state in RETRYABLE_QUALIFICATION_STATES:
                c.execute(
                    """UPDATE raw_launches SET
                         qualification_state='provenance_conflict',
                         qualification_error=? WHERE signature=?""",
                    (identity_error, signature),
                )
            c.execute(
                """INSERT OR IGNORE INTO hydration_observations(
                     signature,observed_at,source_provider,response_hash,
                     identity_hash,creator,mint,evidence_state,error)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                (signature, now, provider, response_hash, identity_hash,
                 creator, mint, state, identity_error),
            )
            c.commit()
            raise SourceEvidenceConflict(identity_error)
        rpc_failed = tx is None
        preserve_complete = previous_state == "complete" and state != "complete"
        exact_complete_replay = previous_state == "complete" and state == "complete"
        if preserve_complete or exact_complete_replay:
            # Two workers may have selected the same raw row before either wrote.
            # Neither a slower ambiguous response nor an equivalent provider
            # replay may overwrite first-complete identity/hash evidence.
            c.execute(
                "UPDATE raw_launches SET hydration_attempted_at=? WHERE signature=?",
                (now, signature),
            )
            c.execute(
                """INSERT OR IGNORE INTO hydration_observations(
                     signature,observed_at,source_provider,response_hash,
                     identity_hash,creator,mint,evidence_state,error)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                (signature, now, provider, response_hash, identity_hash,
                 creator, mint, state, str(error)[:240] if error else None),
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
                      (creator, mint, now, now, identity_hash, state,
                       str(error)[:240] if error else None, signature))
        c.execute(
            """INSERT OR IGNORE INTO hydration_observations(
                 signature,observed_at,source_provider,response_hash,
                 identity_hash,creator,mint,evidence_state,error)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (signature, now, provider, response_hash, identity_hash,
             creator, mint, state, str(error)[:240] if error else None),
        )
        c.commit()
        return state
    except SourceEvidenceConflict:
        raise
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


def _transaction(rpc: JsonRpc, signature: str, *,
                 deadline: float | None = None,
                 monotonic: Callable[[], float] = time.monotonic) -> dict:
    result = _rpc_call(rpc, "getTransaction", [signature, {
        "commitment": "confirmed", "encoding": "jsonParsed",
        "maxSupportedTransactionVersion": 0,
    }], deadline=deadline, monotonic=monotonic)
    if not isinstance(result, dict):
        raise TransactionUnavailableError("confirmed transaction is not available")
    signatures = ((result.get("transaction") or {}).get("signatures") or [])
    if not signatures or str(signatures[0]) != str(signature):
        raise RuntimeError("RPC transaction signature does not match request")
    return result


def persist(payload: object, *, rpc: JsonRpc | None = None,
            transaction: dict | None = None,
            capture_mode: str = "live_ws",
            captured_at: datetime | str | None = None,
            block_time: datetime | str | None = None,
            source_provider: str | None = None) -> None:
    if not isinstance(payload, dict) or payload.get("kind") != "launch":
        return
    provider = source_provider or rpc_provider_id(
        getattr(rpc, "endpoint", None) if rpc is not None else None
    )
    _insert_raw(
        payload, capture_mode=capture_mode, captured_at=captured_at,
        block_time=block_time, source_provider=provider,
    )
    if transaction is not None:
        # A database/evidence failure must propagate to gap recovery. It is not
        # an RPC outage and must never be relabelled as one by the handler below.
        _set_hydration(
            payload["signature"], transaction, None, source_provider=provider,
        )
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
            source_provider=provider,
        )
        logger.warning("solana_launch_hydration_failed",
                       signature=payload["signature"][:12], error=str(exc)[:120])
        return
    _set_hydration(payload["signature"], tx, None, source_provider=provider)


def _persist_stream_event(payload: object) -> None:
    """Block before the writer so StreamRunner cannot advance its health cursor."""
    stream_disk_guard.GUARD.require_evidence_write("solana")
    persist(payload)


def rehydrate_pending(rpc: JsonRpc, *, limit: int = 100,
                      include_incomplete: bool = False,
                      now: datetime | None = None,
                      deadline: float | None = None,
                      monotonic: Callable[[], float] = time.monotonic) -> dict:
    """Retry due evidence oldest-first without repeatedly guessing ambiguous rows."""
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    provider = rpc_provider_id(getattr(rpc, "endpoint", None))
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
    persistence_failed = 0
    persistence_error = None
    deadline_stopped = 0
    pressure_kind = None
    retry_after = None
    deadline_exhausted = False
    for (signature,) in rows:
        if deadline is not None and monotonic() >= deadline:
            deadline_exhausted = True
            break
        attempted += 1
        try:
            tx = _transaction(
                rpc, signature, deadline=deadline, monotonic=monotonic,
            )
        except MaintenanceDeadlineExceeded as exc:
            if not exc.work_started:
                attempted -= 1
            else:
                deadline_stopped += 1
            deadline_exhausted = True
            break
        except TransactionUnavailableError as exc:
            try:
                _set_hydration(
                    signature, None, str(exc), at=now, source_provider=provider,
                )
            except Exception as persist_exc:
                persistence_failed += 1
                persistence_error = str(persist_exc)[:240]
                logger.exception(
                    "solana_launch_hydration_persist_failed",
                    signature=signature[:12], error=persistence_error,
                    original_error=str(exc)[:120],
                )
                break
            unavailable += 1
        except Exception as exc:
            pressure = _as_rpc_pressure(exc)
            try:
                _set_hydration(
                    signature, None, str(exc), at=now,
                    retry_after_seconds=(
                        pressure.retry_after_seconds if pressure else None
                    ),
                    source_provider=provider,
                )
            except Exception as persist_exc:
                persistence_failed += 1
                persistence_error = str(persist_exc)[:240]
                logger.exception(
                    "solana_launch_hydration_persist_failed",
                    signature=signature[:12], error=persistence_error,
                    original_error=str(exc)[:120],
                    pressure_kind=pressure.kind if pressure else None,
                )
            if pressure is not None:
                pressure_failed += 1
                pressure_kind = pressure.kind
                retry_after = pressure.retry_after_seconds
                break
            rpc_failed += 1
            if persistence_failed:
                break
        else:
            try:
                state = _set_hydration(
                    signature, tx, None, at=now, source_provider=provider,
                )
            except Exception as persist_exc:
                persistence_failed += 1
                persistence_error = str(persist_exc)[:240]
                logger.exception(
                    "solana_launch_hydration_persist_failed",
                    signature=signature[:12], error=persistence_error,
                )
                break
            if state == "complete":
                completed += 1
            else:
                incomplete += 1
        if deadline is not None and monotonic() >= deadline:
            deadline_exhausted = True
            break
    return {
        "attempted": attempted, "completed": completed,
        "incomplete": incomplete, "unavailable": unavailable,
        "rpc_failed": rpc_failed, "pressure_failed": pressure_failed,
        "persistence_failed": persistence_failed,
        "persistence_error": persistence_error,
        "pressure_kind": pressure_kind,
        "retry_after_seconds": retry_after,
        "deadline_stopped": deadline_stopped,
        "deadline_exhausted": deadline_exhausted,
    }


def retry_open_gaps(rpc: JsonRpc, *, limit: int = 10,
                    slot_budget: int = GAP_RETRY_SLOT_BUDGET,
                    deadline: float | None = None,
                    monotonic: Callable[[], float] = time.monotonic) -> dict:
    attempted = recovered = progressed = failed = 0
    pressure_kind = None
    retry_after = None
    deadline_stopped = 0
    deadline_exhausted = False
    # Blocks can be several MB. Recovery stays sequential and bounded per
    # maintenance cycle. One unit may prove a leading skipped run, but a
    # produced block is still handled one slot at a time so partial evidence
    # can never advance the gap cursor.
    remaining_budget = min(
        GAP_RETRY_MAX_SLOT_BUDGET, max(0, int(slot_budget)),
    )
    # Smallest-remaining-first: the whole budget goes to the queue head, so
    # oldest-first would let one multi-thousand-slot gap starve hundreds of
    # one-slot skipped-run gaps that each need a single proof unit. The fetch
    # window must cover at least one gap per budget unit or a ramped budget
    # goes unused against a backlog of one-slot gaps.
    gaps = stream_health.open_gaps(
        "solana", "pump_fun_launches",
        limit=max(int(limit), remaining_budget), order="smallest",
    )
    stop_lane = False
    for gap in gaps:
        if remaining_budget <= 0 or stop_lane:
            break
        cursor = int(gap["from_cursor"])
        end = int(gap["to_cursor"])
        while cursor <= end and remaining_budget > 0:
            if deadline is not None and monotonic() >= deadline:
                deadline_exhausted = True
                stop_lane = True
                break
            attempted += 1
            remaining_budget -= 1
            try:
                evidence = _backfill_finalized_slot(
                    cursor, rpc=rpc, deadline=deadline, monotonic=monotonic,
                )
                if not isinstance(evidence, dict):
                    raise RuntimeError(
                        "Solana gap proof returned invalid evidence")
                evidence_state = evidence.get("state")
                evidence_slot = evidence.get("slot")
                verified_through = evidence.get("verified_through")
                if (isinstance(evidence_slot, bool)
                        or not isinstance(evidence_slot, int)
                        or evidence_slot != cursor
                        or isinstance(verified_through, bool)
                        or not isinstance(verified_through, int)
                        or verified_through < cursor):
                    raise RuntimeError(
                        "Solana gap proof returned invalid verified_through")
                if evidence_state == "produced":
                    if verified_through != cursor:
                        raise RuntimeError(
                            "produced Solana gap proof crossed its slot")
                elif evidence_state == "skipped_proven":
                    next_slot = evidence.get("proof_next_slot")
                    parent_slot = evidence.get("proof_parent_slot")
                    if (isinstance(next_slot, bool)
                            or not isinstance(next_slot, int)
                            or isinstance(parent_slot, bool)
                            or not isinstance(parent_slot, int)
                            or parent_slot < 0 or parent_slot >= cursor
                            or next_slot <= cursor
                            or verified_through != next_slot - 1):
                        raise RuntimeError(
                            "skipped Solana gap proof contract is inconsistent")
                else:
                    raise RuntimeError("Solana gap proof state is invalid")
                checkpoint = min(end, verified_through)
                state = stream_health.advance_gap(
                    gap["id"], checkpoint,
                    details={
                        "backfilled": True, "retry": True,
                        "slot": cursor, "gap_to": end, **evidence,
                    },
                )
                if state == "resolved":
                    recovered += 1
                    break
                if state != "advanced":
                    raise RuntimeError(
                        "verified Solana gap checkpoint was not persisted")
                progressed += 1
                cursor = checkpoint + 1
                # No post-advance deadline probe: the while-top pre-check
                # already stops remaining work, and probing here would brand a
                # cycle that spent its full budget cleanly as deadline
                # exhausted, so the budget ramp could never park at a budget
                # that exactly fits the lane's wall clock.
            except MaintenanceDeadlineExceeded:
                # The outer pre-check guarantees this slot was selected within
                # budget. A slot proof spans several RPCs, so work_started only
                # describes the final call; earlier calls may already have run.
                # Keep the slot classified as an attempted, unfinished probe.
                deadline_stopped += 1
                deadline_exhausted = True
                stop_lane = True
                break
            except Exception as exc:
                failed += 1
                pressure = _as_rpc_pressure(exc)
                pressure_kind = pressure.kind if pressure else None
                retry_after = pressure.retry_after_seconds if pressure else None
                try:
                    deferred = stream_health.defer_gap(
                        gap["id"], str(exc),
                        base_delay_seconds=max(
                            60, int(math.ceil(retry_after or 0)),
                        ),
                    )
                except Exception as defer_exc:
                    # Preserve the original provider-pressure classification even
                    # when the independent health store cannot persist backoff.
                    deferred = None
                    logger.exception(
                        "solana_launch_gap_defer_failed",
                        slot=cursor, error=str(defer_exc)[:120],
                        original_error=str(exc)[:120],
                        pressure_kind=pressure_kind,
                    )
                logger.warning(
                    "solana_launch_backfill_failed", slot=cursor,
                    error=str(exc)[:120],
                    next_retry_at=(deferred or {}).get("next_retry_at"),
                )
                stop_lane = True
                break
    return {
        "attempted": attempted, "recovered": recovered,
        "progressed": progressed, "failed": failed,
        "pressure_kind": pressure_kind,
        "retry_after_seconds": retry_after,
        "deadline_stopped": deadline_stopped,
        "deadline_exhausted": deadline_exhausted,
    }


def _rehydrate_loop(stop: threading.Event, rpc: JsonRpc,
                    *, interval_seconds: float = 60,
                    monotonic: Callable[[], float] = time.monotonic) -> None:
    circuit_open_until = 0.0
    circuit_kind = None
    pressure_count = 0
    gap_budget = GAP_RETRY_SLOT_BUDGET
    gap_clean_cycles = 0
    hydration_limit = HYDRATION_BATCH_LIMIT
    hydration_clean_cycles = 0
    while not stop.is_set():
        cycle_started = monotonic()
        total_deadline = cycle_started + MAINTENANCE_WORK_BUDGET_SECONDS
        gap_deadline = min(
            total_deadline, cycle_started + GAP_WORK_BUDGET_SECONDS,
        )
        maintenance_status = "live"
        maintenance_error = None
        try:
            stream_disk_guard.GUARD.require_evidence_write("solana")
        except stream_disk_guard.StreamDiskCritical:
            # Do not start gap-store, launch DB, or RPC work while the evidence
            # volume is critical.  Keep the worker alive and retry next cycle.
            _report_maintenance("degraded", "workspace disk critical; maintenance paused")
            elapsed = monotonic() - cycle_started
            if stop.wait(max(1.0, float(interval_seconds) - elapsed)):
                break
            continue
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
                gap_lane_error = None
                # A local gap-store failure must be fail-visible without starving
                # hydration work in the independent launch-evidence database.
                try:
                    gaps = retry_open_gaps(
                        rpc, slot_budget=(GAP_RETRY_SLOT_BUDGET
                                          if half_open else gap_budget),
                        deadline=gap_deadline, monotonic=monotonic,
                    )
                except Exception as exc:
                    pressure = _as_rpc_pressure(exc)
                    gap_lane_error = str(exc)[:160]
                    gaps = {
                        "attempted": 0, "recovered": 0, "progressed": 0,
                        "failed": 1,
                        "pressure_kind": pressure.kind if pressure else None,
                        "retry_after_seconds": (
                            pressure.retry_after_seconds if pressure else None
                        ),
                        "deadline_stopped": 0,
                        "deadline_exhausted": False,
                    }
                    logger.exception(
                        "solana_launch_gap_lane_failed", error=gap_lane_error,
                    )
                if gaps["attempted"]:
                    logger.info("solana_launch_gap_retry", **gaps)
                gap_deadline_hit = bool(gaps["deadline_exhausted"])
                total_deadline_hit = monotonic() >= total_deadline
                result = None
                if (gaps["pressure_kind"] is None
                        and not total_deadline_hit
                        and not (half_open and gaps["attempted"])):
                    result = rehydrate_pending(
                        rpc, limit=1 if half_open else hydration_limit,
                        deadline=total_deadline, monotonic=monotonic,
                    )
                    if result["attempted"]:
                        logger.info("solana_launch_rehydrated", **result)
                    total_deadline_hit = bool(
                        result["deadline_exhausted"]
                        or monotonic() >= total_deadline
                    )

                pressure_lane = None
                hydration_persist_failed = bool(
                    result is not None
                    and int(result.get("persistence_failed") or 0)
                )
                hydration_persist_error = (
                    str(result.get("persistence_error") or "unknown error")[:120]
                    if result is not None else "unknown error"
                )
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
                    gap_budget = GAP_RETRY_SLOT_BUDGET
                    gap_clean_cycles = 0
                    hydration_limit = HYDRATION_BATCH_LIMIT
                    hydration_clean_cycles = 0
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
                elif total_deadline_hit:
                    gap_budget = GAP_RETRY_SLOT_BUDGET
                    gap_clean_cycles = 0
                    hydration_limit = HYDRATION_BATCH_LIMIT
                    hydration_clean_cycles = 0
                    maintenance_status = "degraded"
                    maintenance_error = "maintenance work budget exhausted"
                    if hydration_persist_failed:
                        maintenance_error += "; hydration persistence failed"
                elif half_open:
                    gap_probe_attempted = int(gaps["attempted"] or 0) > 0
                    hydration_probe_attempted = bool(
                        result is not None and int(result["attempted"] or 0) > 0
                    )
                    probe_succeeded = (
                        _gap_probe_succeeded(gaps) if gap_probe_attempted
                        else _hydration_probe_succeeded(result)
                    )
                    if probe_succeeded:
                        circuit_open_until = 0.0
                        circuit_kind = None
                        pressure_count = 0
                        logger.info("solana_launch_rpc_circuit_recovered")
                        if gap_lane_error is not None:
                            maintenance_status = "degraded"
                            maintenance_error = (
                                f"gap lane local failure: {gap_lane_error}"
                            )
                    elif gap_probe_attempted or hydration_probe_attempted:
                        maintenance_status = "degraded"
                        maintenance_error = "RPC circuit half-open; probe failed"
                    else:
                        maintenance_status = "degraded"
                        maintenance_error = "RPC circuit half-open; no due probe"
                elif gap_deadline_hit:
                    maintenance_status = "degraded"
                    maintenance_error = "gap lane work budget exhausted"
                    if result is not None and result["rpc_failed"]:
                        maintenance_error += "; hydration recovery failed"
                    if hydration_persist_failed:
                        maintenance_error += "; hydration persistence failed"
                elif gap_lane_error is not None:
                    maintenance_status = "degraded"
                    maintenance_error = f"gap lane local failure: {gap_lane_error}"
                    if hydration_persist_failed:
                        maintenance_error += "; hydration persistence failed"
                elif hydration_persist_failed:
                    maintenance_status = "degraded"
                    maintenance_error = (
                        f"hydration persistence failed: {hydration_persist_error}"
                    )
                elif gaps["failed"] or (
                    result is not None and result["rpc_failed"]
                ):
                    maintenance_status = "degraded"
                    maintenance_error = "maintenance evidence recovery failed"

                if (pressure_lane is None and not total_deadline_hit
                        and not half_open):
                    previous_gap_budget = gap_budget
                    previous_hydration_limit = hydration_limit
                    gap_budget, gap_clean_cycles = _adjust_gap_budget(
                        gap_budget, gap_clean_cycles, gaps,
                    )
                    hydration_limit, hydration_clean_cycles = \
                        _adjust_hydration_limit(
                            hydration_limit, hydration_clean_cycles, result,
                        )
                    if (gap_budget != previous_gap_budget
                            or hydration_limit != previous_hydration_limit):
                        logger.info(
                            "solana_launch_maintenance_budget_adjusted",
                            gap_budget=gap_budget,
                            hydration_limit=hydration_limit,
                        )
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


def _backfill_finalized_slot(
    slot: int, *, rpc: JsonRpc, deadline: float | None = None,
    monotonic: Callable[[], float] = time.monotonic,
    capture_mode: str = "gap_backfill",
) -> dict:
    """Verify one produced slot or a leading skipped run, failing on partial proof."""
    slot = int(slot)
    finalized = int(_rpc_call(
        rpc, "getSlot", [{"commitment": "finalized"}],
        deadline=deadline, monotonic=monotonic,
    ))
    if finalized < slot:
        raise RuntimeError(f"slot {slot} is not finalized (tip {finalized})")
    first_available = int(_rpc_call(
        rpc, "getFirstAvailableBlock", [],
        deadline=deadline, monotonic=monotonic,
    ))
    if slot < first_available:
        raise RuntimeError(
            f"slot {slot} predates first available block {first_available}")
    proof_end = min(finalized, slot + SKIPPED_SLOT_PROOF_LOOKAHEAD)
    produced = _rpc_call(rpc, "getBlocks", [
        slot, proof_end, {"commitment": "finalized"},
    ], deadline=deadline, monotonic=monotonic)
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
        successor = _rpc_call(rpc, "getBlock", [first_produced, {
            "commitment": "finalized", "encoding": "json",
            "transactionDetails": "none", "rewards": False,
            "maxSupportedTransactionVersion": 0,
        }], deadline=deadline, monotonic=monotonic)
        if not isinstance(successor, dict):
            raise RuntimeError(
                f"finalized successor block {first_produced} is unavailable")
        parent = successor.get("parentSlot")
        if (isinstance(parent, bool) or not isinstance(parent, int)
                or int(parent) < 0):
            raise RuntimeError(
                f"finalized successor block {first_produced} lacks parentSlot")
        if int(parent) < slot:
            return {
                "state": "skipped_proven", "slot": slot,
                "verified_through": first_produced - 1,
                "proof_next_slot": first_produced,
                "proof_parent_slot": int(parent),
            }
        raise RuntimeError(
            f"provider omitted produced slot {slot}; successor parent is {parent}")
    block = _rpc_call(rpc, "getBlock", [slot, {
        "commitment": "finalized", "encoding": "json",
        "transactionDetails": "full", "rewards": False,
        "maxSupportedTransactionVersion": 0,
    }], deadline=deadline, monotonic=monotonic)
    if not isinstance(block, dict):
        raise RuntimeError(f"finalized block {slot} is unavailable")
    transactions = block.get("transactions")
    if not isinstance(transactions, list):
        raise RuntimeError(f"finalized block {slot} has no transaction list")
    launches = 0
    raw_block_time = block.get("blockTime")
    block_time = None
    if raw_block_time is not None:
        try:
            block_time = datetime.fromtimestamp(
                float(raw_block_time), tz=timezone.utc,
            )
        except (TypeError, ValueError, OverflowError, OSError) as exc:
            raise RuntimeError(f"finalized block {slot} has invalid blockTime") from exc
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
        persist(
            payload, transaction=tx, capture_mode=capture_mode,
            block_time=block_time,
            source_provider=rpc_provider_id(getattr(rpc, "endpoint", None)),
        )
        launches += 1
    return {
        "state": "produced", "slot": slot,
        "verified_through": slot, "launches": launches,
    }


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
        rpc = JsonRpc(configured_rpc_endpoint())
    if socket_factory is None:
        from websocket import create_connection

        endpoint = configured_ws_endpoint()
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
        on_event=_persist_stream_event,
        heartbeat_seconds=30, health_interval_seconds=1,
        expect_contiguous=True,
        backfill=None,
    )


def main() -> None:
    from dotenv import load_dotenv
    from src.config import PROJECT_ROOT

    load_dotenv(PROJECT_ROOT / ".env")
    _conn().close()
    rpc = JsonRpc(configured_rpc_endpoint())
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
