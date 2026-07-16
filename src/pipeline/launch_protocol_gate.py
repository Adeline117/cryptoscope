"""Persistent source-admission latch for one pre-registered Launch cohort."""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Callable

from src.pipeline import solana_launch_reconcile as reconcile
from src.pipeline import solana_launch_stream as stream


MAX_ACTIVATION_DELAY_SECONDS = 180
ReadinessProbe = Callable[..., dict]


def _aware(value: datetime | str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("protocol clocks must be timezone-aware")
    return parsed.astimezone(timezone.utc)


def _canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _hash(value: object) -> str:
    return hashlib.sha256(_canonical(value).encode()).hexdigest()


def _ensure_schema(connection) -> None:
    connection.execute("""CREATE TABLE IF NOT EXISTS launch_protocol_admission(
        protocol_id TEXT PRIMARY KEY,
        cohort_version INTEGER NOT NULL,
        start_at TEXT NOT NULL,
        state TEXT NOT NULL,
        armed_at TEXT,
        opened_at TEXT,
        breached_at TEXT,
        reason_codes TEXT NOT NULL,
        readiness_hash TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL)""")
    connection.execute("""CREATE TABLE IF NOT EXISTS
        launch_protocol_admission_observations(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          protocol_id TEXT NOT NULL,
          checked_at TEXT NOT NULL,
          ready INTEGER NOT NULL,
          readiness_hash TEXT NOT NULL,
          reason_codes TEXT NOT NULL,
          UNIQUE(protocol_id,checked_at,readiness_hash),
          FOREIGN KEY(protocol_id) REFERENCES launch_protocol_admission(protocol_id))""")
    connection.execute("""CREATE TRIGGER IF NOT EXISTS
        trg_launch_protocol_identity_immutable
        BEFORE UPDATE OF protocol_id,cohort_version,start_at,created_at
        ON launch_protocol_admission BEGIN
          SELECT RAISE(ABORT, 'launch protocol identity is immutable');
        END""")
    connection.execute("""CREATE TRIGGER IF NOT EXISTS
        trg_launch_protocol_breach_terminal
        BEFORE UPDATE ON launch_protocol_admission
        WHEN OLD.state='breached' BEGIN
          SELECT RAISE(ABORT, 'breached launch protocol is terminal');
        END""")
    for name, action in (("no_update", "UPDATE"), ("no_delete", "DELETE")):
        connection.execute(f"""CREATE TRIGGER IF NOT EXISTS
            trg_launch_protocol_observation_{name}
            BEFORE {action} ON launch_protocol_admission_observations BEGIN
              SELECT RAISE(ABORT, 'protocol admission observation is append-only');
            END""")
    connection.commit()


def _probe(readiness_probe: ReadinessProbe, *, now: datetime) -> dict:
    try:
        value = readiness_probe(now=now)
    except Exception as exc:
        return {
            "state": "blocked", "ready": False,
            "reason_codes": ["source_readiness_unavailable"],
            "error": f"{type(exc).__name__}: {exc}"[:160],
        }
    if not isinstance(value, dict) or value.get("ready") is not True \
            or value.get("state") != "ready":
        reasons = (value.get("reason_codes") if isinstance(value, dict) else None)
        return {
            **(value if isinstance(value, dict) else {}),
            "state": "blocked", "ready": False,
            "reason_codes": list(reasons) if isinstance(reasons, list) and reasons
            else ["source_readiness_blocked"],
        }
    return {**value, "state": "ready", "ready": True,
            "reason_codes": list(value.get("reason_codes") or [])}


def _result(row: tuple, readiness: dict) -> dict:
    keys = (
        "protocol_id", "cohort_version", "start_at", "state", "armed_at",
        "opened_at", "breached_at", "reason_codes", "readiness_hash",
        "created_at", "updated_at",
    )
    item = dict(zip(keys, row))
    try:
        reasons = json.loads(item["reason_codes"])
    except (TypeError, json.JSONDecodeError):
        reasons = ["protocol_gate_state_invalid"]
    return {
        "protocol_id": item["protocol_id"],
        "cohort_version": item["cohort_version"],
        "protocol_start_at": item["start_at"],
        "state": item["state"],
        "enrollment_open": item["state"] == "open",
        "armed_at": item["armed_at"], "opened_at": item["opened_at"],
        "breached_at": item["breached_at"], "reason_codes": reasons,
        "readiness_hash": item["readiness_hash"],
        "created_at": item["created_at"], "updated_at": item["updated_at"],
        "source_readiness": readiness,
        "auto_execution_allowed": False,
    }


def admit(
    *, protocol_id: str, cohort_version: int, start_at: str,
    now: datetime | None = None,
    readiness_probe: ReadinessProbe = reconcile.source_readiness,
    max_activation_delay_seconds: int = MAX_ACTIVATION_DELAY_SECONDS,
) -> dict:
    """Arm before the boundary, open once, and permanently latch any later breach."""
    current = _aware(now or datetime.now(timezone.utc))
    boundary = _aware(start_at)
    if not isinstance(protocol_id, str) or not protocol_id.strip():
        raise ValueError("protocol_id is required")
    if isinstance(cohort_version, bool) or not isinstance(cohort_version, int):
        raise ValueError("cohort_version must be an integer")
    if (isinstance(max_activation_delay_seconds, bool)
            or not isinstance(max_activation_delay_seconds, int)
            or max_activation_delay_seconds < 0):
        raise ValueError("max activation delay must be a nonnegative integer")
    readiness = _probe(readiness_probe, now=current)
    readiness_hash = _hash(readiness)
    readiness_reasons = list(readiness.get("reason_codes") or [])
    connection = stream._conn()
    try:
        _ensure_schema(connection)
        connection.execute("BEGIN IMMEDIATE")
        row = connection.execute(
            "SELECT * FROM launch_protocol_admission WHERE protocol_id=?",
            (protocol_id,),
        ).fetchone()
        if row is None:
            connection.execute(
                """INSERT INTO launch_protocol_admission(
                     protocol_id,cohort_version,start_at,state,armed_at,opened_at,
                     breached_at,reason_codes,readiness_hash,created_at,updated_at)
                   VALUES (?,?,?,'scheduled',NULL,NULL,NULL,?,?,?,?)""",
                (protocol_id, cohort_version, boundary.isoformat(),
                 _canonical(["protocol_start_not_reached"]), readiness_hash,
                 current.isoformat(), current.isoformat()),
            )
            row = connection.execute(
                "SELECT * FROM launch_protocol_admission WHERE protocol_id=?",
                (protocol_id,),
            ).fetchone()
        if row[1] != cohort_version or row[2] != boundary.isoformat():
            raise ValueError("protocol identity disagrees with persistent admission gate")
        connection.execute(
            """INSERT OR IGNORE INTO launch_protocol_admission_observations(
                 protocol_id,checked_at,ready,readiness_hash,reason_codes)
               VALUES (?,?,?,?,?)""",
            (protocol_id, current.isoformat(), int(readiness["ready"]),
             readiness_hash, _canonical(readiness_reasons)),
        )
        state = str(row[3])
        if state != "breached":
            armed_at, opened_at, breached_at = row[4], row[5], row[6]
            reasons: list[str]
            if state == "open":
                if readiness["ready"]:
                    next_state, reasons = "open", []
                else:
                    next_state, breached_at = "breached", current.isoformat()
                    reasons = ["source_readiness_breached_after_open", *readiness_reasons]
            elif current < boundary:
                if readiness["ready"]:
                    next_state, reasons = "armed", ["protocol_start_not_reached"]
                    armed_at = armed_at or current.isoformat()
                else:
                    next_state, reasons, armed_at = "scheduled", [
                        "protocol_start_not_reached", *readiness_reasons,
                    ], None
            else:
                activation_delay = (current - boundary).total_seconds()
                if (state == "armed" and readiness["ready"]
                        and activation_delay <= max_activation_delay_seconds):
                    next_state, reasons = "open", []
                    opened_at = current.isoformat()
                else:
                    next_state, breached_at = "breached", current.isoformat()
                    reason = ("protocol_activation_late" if activation_delay
                              > max_activation_delay_seconds
                              else "protocol_not_armed_and_ready_at_boundary")
                    reasons = [reason, *readiness_reasons]
            connection.execute(
                """UPDATE launch_protocol_admission
                      SET state=?,armed_at=?,opened_at=?,breached_at=?,
                          reason_codes=?,readiness_hash=?,updated_at=?
                    WHERE protocol_id=?""",
                (next_state, armed_at, opened_at, breached_at,
                 _canonical(list(dict.fromkeys(reasons))), readiness_hash,
                 current.isoformat(), protocol_id),
            )
        connection.commit()
        final = connection.execute(
            "SELECT * FROM launch_protocol_admission WHERE protocol_id=?",
            (protocol_id,),
        ).fetchone()
        return _result(final, readiness)
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def read(*, protocol_id: str) -> dict | None:
    """Read the durable latch without changing source or activation state."""
    connection = stream._conn()
    try:
        _ensure_schema(connection)
        row = connection.execute(
            "SELECT * FROM launch_protocol_admission WHERE protocol_id=?",
            (protocol_id,),
        ).fetchone()
        if row is None:
            return None
        return _result(row, {"state": "unobserved", "ready": False,
                             "reason_codes": ["source_readiness_not_rechecked"]})
    finally:
        connection.close()
