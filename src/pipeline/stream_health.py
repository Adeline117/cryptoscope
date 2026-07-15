"""Persistent truth layer for real-time stream freshness and sequence gaps."""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone

from src.config import DATA_DIR

DB = DATA_DIR / "stream_health.db"


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
    c.execute("PRAGMA busy_timeout=8000")
    c.execute("""CREATE TABLE IF NOT EXISTS streams(
        source TEXT NOT NULL, stream TEXT NOT NULL, cursor INTEGER,
        last_event_at TEXT, last_received_at TEXT, latency_ms INTEGER,
        status TEXT NOT NULL, last_error TEXT, updated_at TEXT NOT NULL,
        PRIMARY KEY(source,stream))""")
    c.execute("""CREATE TABLE IF NOT EXISTS gaps(
        id INTEGER PRIMARY KEY AUTOINCREMENT, source TEXT NOT NULL, stream TEXT NOT NULL,
        from_cursor INTEGER NOT NULL, to_cursor INTEGER NOT NULL,
        detected_at TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'open',
        resolved_at TEXT, details TEXT,
        UNIQUE(source,stream,from_cursor,to_cursor))""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_stream_gaps_open "
              "ON gaps(source,stream,status,detected_at)")
    return c


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


def resolve_gap(gap_id: int, *, details: dict | None = None,
                at: datetime | str | None = None) -> bool:
    now = _iso(at)
    c = _conn()
    try:
        row = c.execute("SELECT source,stream FROM gaps WHERE id=? AND status='open'",
                        (gap_id,)).fetchone()
        if not row:
            return False
        c.execute("UPDATE gaps SET status='resolved',resolved_at=?,details=? WHERE id=?",
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


def snapshot(*, now: datetime | str | None = None,
             stale_after_seconds: int = 120) -> list[dict]:
    current = datetime.fromisoformat(_iso(now))
    c = _conn()
    try:
        rows = c.execute("""SELECT source,stream,cursor,last_event_at,last_received_at,
                                   latency_ms,status,last_error,updated_at
                            FROM streams ORDER BY source,stream""").fetchall()
        gaps = {(s, st): n for s, st, n in c.execute(
            "SELECT source,stream,COUNT(*) FROM gaps WHERE status='open' GROUP BY source,stream"
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
        item["open_gaps"] = gaps.get((item["source"], item["stream"]), 0)
        item["stale"] = age is None or age > stale_after_seconds
        if item["stale"] and item["status"] == "live":
            item["status"] = "stale"
        out.append(item)
    return out
