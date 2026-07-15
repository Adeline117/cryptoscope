"""Persistent truth layer for real-time stream freshness and sequence gaps."""
from __future__ import annotations

import json
import sqlite3
import time
from datetime import datetime, timedelta, timezone

from src.config import DATA_DIR

DB = DATA_DIR / "stream_health.db"


def _enable_wal(c: sqlite3.Connection) -> None:
    """Enable WAL without losing a simultaneous multi-process startup race."""
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


def _iso(value: datetime | str | None) -> str:
    if value is None:
        dt = datetime.now(timezone.utc)
    elif isinstance(value, datetime):
        dt = value
    else:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if dt.tzinfo is None:
        raise ValueError("stream timestamps must include a timezone")
    return dt.astimezone(timezone.utc).isoformat()


def _conn() -> sqlite3.Connection:
    DB.parent.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(str(DB), timeout=10)
    try:
        c.execute("PRAGMA busy_timeout=8000")
        _enable_wal(c)
        c.execute("PRAGMA synchronous=NORMAL")
        c.execute("""CREATE TABLE IF NOT EXISTS streams(
            source TEXT NOT NULL, stream TEXT NOT NULL, cursor INTEGER,
            last_event_at TEXT, last_received_at TEXT, latency_ms INTEGER,
            status TEXT NOT NULL, last_error TEXT, updated_at TEXT NOT NULL,
            PRIMARY KEY(source,stream))""")
        c.execute("""CREATE TABLE IF NOT EXISTS gaps(
            id INTEGER PRIMARY KEY AUTOINCREMENT, source TEXT NOT NULL, stream TEXT NOT NULL,
            from_cursor INTEGER NOT NULL, to_cursor INTEGER NOT NULL,
            detected_at TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'open',
            resolved_at TEXT, details TEXT, retry_count INTEGER NOT NULL DEFAULT 0,
            next_retry_at TEXT, last_error TEXT,
            UNIQUE(source,stream,from_cursor,to_cursor))""")
        migrations = (
            ("retry_count", "INTEGER NOT NULL DEFAULT 0"),
            ("next_retry_at", "TEXT"),
            ("last_error", "TEXT"),
        )
        gap_columns = {row[1] for row in c.execute("PRAGMA table_info(gaps)")}
        if any(name not in gap_columns for name, _kind in migrations):
            # Scheduler and both stream workers can open the same legacy DB at
            # startup. Serialize only the migration, then re-read the schema while
            # holding the write lock so a second process never acts on a stale
            # PRAGMA result and attempts the same ALTER TABLE.
            c.execute("BEGIN IMMEDIATE")
            gap_columns = {row[1] for row in c.execute("PRAGMA table_info(gaps)")}
            for name, kind in migrations:
                if name not in gap_columns:
                    c.execute(f"ALTER TABLE gaps ADD COLUMN {name} {kind}")
                    gap_columns.add(name)
            c.commit()
        c.execute("CREATE INDEX IF NOT EXISTS idx_stream_gaps_open "
                  "ON gaps(source,stream,status,detected_at)")
        c.commit()
        return c
    except Exception:
        c.rollback()
        c.close()
        raise


def observe(source: str, stream: str, *, cursor: int | None = None,
            event_at: datetime | str | None = None,
            received_at: datetime | str | None = None,
            expect_contiguous: bool = False) -> dict:
    """Record one message without regressing a cursor or hiding sequence gaps."""
    if not source or not stream:
        raise ValueError("source and stream are required")
    received = _iso(received_at)
    event = _iso(event_at or received)
    event_dt, received_dt = datetime.fromisoformat(event), datetime.fromisoformat(received)
    latency_ms = max(0, round((received_dt - event_dt).total_seconds() * 1000))
    if cursor is not None:
        cursor = int(cursor)

    c = _conn()
    try:
        c.execute("BEGIN IMMEDIATE")
        previous = c.execute(
            "SELECT cursor FROM streams WHERE source=? AND stream=?",
            (source, stream),
        ).fetchone()
        previous_cursor = previous[0] if previous else None
        classification = "event"
        next_cursor = cursor if previous_cursor is None else previous_cursor
        gap_info = None
        if cursor is not None and previous_cursor is not None:
            if cursor < previous_cursor:
                classification = "out_of_order"
            elif cursor == previous_cursor:
                classification = "duplicate"
            else:
                next_cursor = cursor
                if expect_contiguous and cursor > previous_cursor + 1:
                    classification = "gap_detected"
                    c.execute("""INSERT OR IGNORE INTO gaps(
                        source,stream,from_cursor,to_cursor,detected_at,status,details
                    ) VALUES (?,?,?,?,?,'open',?)""",
                              (source, stream, previous_cursor + 1, cursor - 1, received,
                               json.dumps({"observed_after": cursor}, separators=(",", ":"))))
                    gap_id = c.execute(
                        "SELECT id FROM gaps WHERE source=? AND stream=? AND from_cursor=? "
                        "AND to_cursor=?",
                        (source, stream, previous_cursor + 1, cursor - 1),
                    ).fetchone()[0]
                    gap_info = {"id": gap_id, "from_cursor": previous_cursor + 1,
                                "to_cursor": cursor - 1}
        elif cursor is not None:
            next_cursor = cursor
        open_gaps = c.execute(
            "SELECT COUNT(*) FROM gaps WHERE source=? AND stream=? AND status='open'",
            (source, stream),
        ).fetchone()[0]
        status = "degraded" if open_gaps else "live"
        c.execute("""INSERT INTO streams(
            source,stream,cursor,last_event_at,last_received_at,latency_ms,status,last_error,updated_at
        ) VALUES (?,?,?,?,?,?,?,NULL,?)
        ON CONFLICT(source,stream) DO UPDATE SET
            cursor=excluded.cursor,last_event_at=excluded.last_event_at,
            last_received_at=excluded.last_received_at,latency_ms=excluded.latency_ms,
            status=excluded.status,last_error=NULL,updated_at=excluded.updated_at""",
                  (source, stream, next_cursor, event, received, latency_ms, status, received))
        c.commit()
        return {"classification": classification, "cursor": next_cursor,
                "latency_ms": latency_ms, "status": status, "open_gaps": open_gaps,
                "gap": gap_info}
    finally:
        c.close()


def mark_disconnected(source: str, stream: str, error: str,
                      *, at: datetime | str | None = None) -> None:
    now = _iso(at)
    c = _conn()
    try:
        c.execute("""INSERT INTO streams(source,stream,status,last_error,updated_at)
                     VALUES (?,?,'disconnected',?,?)
                     ON CONFLICT(source,stream) DO UPDATE SET
                       status='disconnected',last_error=excluded.last_error,
                       updated_at=excluded.updated_at""",
                  (source, stream, str(error)[:240], now))
        c.commit()
    finally:
        c.close()


def report_worker(source: str, stream: str, *, status: str,
                  error: str | None = None,
                  at: datetime | str | None = None) -> None:
    """Heartbeat a background worker without pretending it emitted market data."""
    if not source or not stream:
        raise ValueError("source and stream are required")
    if status not in {"live", "degraded"}:
        raise ValueError("worker status must be live or degraded")
    now = _iso(at)
    c = _conn()
    try:
        c.execute("""INSERT INTO streams(
                         source,stream,last_received_at,status,last_error,updated_at
                     ) VALUES (?,?,?,?,?,?)
                     ON CONFLICT(source,stream) DO UPDATE SET
                       last_received_at=excluded.last_received_at,
                       status=excluded.status,last_error=excluded.last_error,
                       updated_at=excluded.updated_at""",
                  (source, stream, now, status,
                   str(error)[:240] if error else None, now))
        c.commit()
    finally:
        c.close()


def resolve_gap(gap_id: int, *, details: dict | None = None,
                at: datetime | str | None = None) -> bool:
    now = _iso(at)
    c = _conn()
    try:
        row = c.execute("SELECT source,stream FROM gaps WHERE id=? AND status='open'",
                        (gap_id,)).fetchone()
        if not row:
            return False
        c.execute("UPDATE gaps SET status='resolved',resolved_at=?,details=?,"
                  "retry_count=0,next_retry_at=NULL,last_error=NULL WHERE id=?",
                  (now, json.dumps(details or {}, separators=(",", ":")), gap_id))
        remaining = c.execute(
            "SELECT COUNT(*) FROM gaps WHERE source=? AND stream=? AND status='open'", row
        ).fetchone()[0]
        c.execute("UPDATE streams SET status=?,updated_at=? WHERE source=? AND stream=?",
                  ("degraded" if remaining else "live", now, *row))
        c.commit()
        return True
    finally:
        c.close()


def advance_gap(gap_id: int, through_cursor: int, *, details: dict | None = None,
                at: datetime | str | None = None) -> str | None:
    """Checkpoint a verified prefix without pretending the whole gap recovered."""
    now = _iso(at)
    through = int(through_cursor)
    c = _conn()
    try:
        c.execute("BEGIN IMMEDIATE")
        row = c.execute(
            "SELECT source,stream,from_cursor,to_cursor FROM gaps "
            "WHERE id=? AND status='open'", (gap_id,),
        ).fetchone()
        if not row:
            c.rollback()
            return None
        source, stream, start, end = row
        if through < start:
            c.rollback()
            return None
        payload = json.dumps(details or {}, separators=(",", ":"))
        if through >= end:
            c.execute(
                "UPDATE gaps SET status='resolved',resolved_at=?,details=?,"
                "retry_count=0,next_retry_at=NULL,last_error=NULL WHERE id=?",
                (now, payload, gap_id),
            )
            result = "resolved"
        else:
            c.execute(
                "UPDATE gaps SET from_cursor=?,details=?,retry_count=0,"
                "next_retry_at=NULL,last_error=NULL WHERE id=?",
                (through + 1, payload, gap_id),
            )
            result = "advanced"
        remaining = c.execute(
            "SELECT COUNT(*) FROM gaps WHERE source=? AND stream=? AND status='open'",
            (source, stream),
        ).fetchone()[0]
        c.execute(
            "UPDATE streams SET status=?,updated_at=? WHERE source=? AND stream=?",
            ("degraded" if remaining else "live", now, source, stream),
        )
        c.commit()
        return result
    finally:
        c.close()


def defer_gap(gap_id: int, error: str, *, at: datetime | str | None = None,
              base_delay_seconds: int = 60,
              max_delay_seconds: int = 6 * 60 * 60) -> dict | None:
    """Persist exponential retry backoff while keeping the gap fail-visible."""
    now = _iso(at)
    current = datetime.fromisoformat(now)
    base = max(1, int(base_delay_seconds))
    maximum = max(base, int(max_delay_seconds))
    c = _conn()
    try:
        c.execute("BEGIN IMMEDIATE")
        row = c.execute(
            "SELECT retry_count FROM gaps WHERE id=? AND status='open'", (gap_id,),
        ).fetchone()
        if not row:
            c.rollback()
            return None
        retry_count = int(row[0] or 0) + 1
        delay = min(maximum, base * (2 ** min(retry_count - 1, 20)))
        next_retry = (current + timedelta(seconds=delay)).isoformat()
        last_error = str(error)[:240]
        c.execute(
            "UPDATE gaps SET retry_count=?,next_retry_at=?,last_error=? WHERE id=?",
            (retry_count, next_retry, last_error, gap_id),
        )
        c.commit()
        return {"retry_count": retry_count, "delay_seconds": delay,
                "next_retry_at": next_retry, "last_error": last_error}
    finally:
        c.close()


def open_gaps(source: str, stream: str, *, limit: int = 100,
              now: datetime | str | None = None) -> list[dict]:
    current = _iso(now)
    c = _conn()
    try:
        rows = c.execute(
            """SELECT id,from_cursor,to_cursor,detected_at,details,
                      retry_count,next_retry_at,last_error
               FROM gaps WHERE source=? AND stream=? AND status='open'
                 AND (next_retry_at IS NULL OR next_retry_at<=?)
               ORDER BY detected_at,id LIMIT ?""",
            (source, stream, current, max(0, int(limit))),
        ).fetchall()
    finally:
        c.close()
    keys = ("id", "from_cursor", "to_cursor", "detected_at", "details",
            "retry_count", "next_retry_at", "last_error")
    return [dict(zip(keys, row)) for row in rows]


def snapshot(*, now: datetime | str | None = None,
             stale_after_seconds: int = 120) -> list[dict]:
    current = datetime.fromisoformat(_iso(now))
    c = _conn()
    try:
        rows = c.execute("""SELECT source,stream,cursor,last_event_at,last_received_at,
                                   latency_ms,status,last_error,updated_at
                            FROM streams ORDER BY source,stream""").fetchall()
        gaps = {(s, st): (n, deferred, next_retry) for s, st, n, deferred, next_retry
                in c.execute(
                    """SELECT source,stream,COUNT(*),
                              SUM(CASE WHEN next_retry_at>? THEN 1 ELSE 0 END),
                              MIN(CASE WHEN next_retry_at>? THEN next_retry_at END)
                       FROM gaps WHERE status='open' GROUP BY source,stream""",
                    (current.isoformat(), current.isoformat()),
                )}
    finally:
        c.close()
    out = []
    keys = ("source", "stream", "cursor", "last_event_at", "last_received_at",
            "latency_ms", "status", "last_error", "updated_at")
    for row in rows:
        item = dict(zip(keys, row))
        received = item.get("last_received_at")
        age = ((current - datetime.fromisoformat(received)).total_seconds()
               if received else None)
        item["age_seconds"] = round(age, 1) if age is not None else None
        gap_count, deferred, next_retry = gaps.get(
            (item["source"], item["stream"]), (0, 0, None))
        item["open_gaps"] = gap_count
        item["deferred_gaps"] = deferred
        item["next_gap_retry_at"] = next_retry
        item["stale"] = age is None or age > stale_after_seconds
        if item["stale"] and item["status"] == "live":
            item["status"] = "stale"
        out.append(item)
    return out
