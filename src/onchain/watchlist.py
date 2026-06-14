"""Near-saturation watchlist — the bridge from Stage 1 (state) to Stage 2 (event).

Stage 1 (the accumulation pipeline) is a cheap batch scan over the whole market.
When a token's effective concentration is *near saturation* (the whale is nearly
done accumulating), it earns a place on this narrow watchlist. Stage 2 then
watches ONLY this list closely for the launch-prep event — that is how the
expensive real-time work stays cheap (a handful of tokens, not the whole market).

Entries expire after a TTL so the list stays small and current.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import structlog

from src.config import DATA_DIR

logger = structlog.get_logger()

DB_PATH = DATA_DIR / "watchlist.db"
TTL_DAYS = 10  # drop stale entries — saturation that never launched


def _connect(db_path: Path = DB_PATH) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), timeout=10)
    conn.execute("PRAGMA busy_timeout=10000")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS watchlist (
            token TEXT NOT NULL,
            chain TEXT NOT NULL,
            added_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            effective_top_pct REAL,
            symbol TEXT,
            status TEXT DEFAULT 'watching',
            PRIMARY KEY (token, chain)
        )
    """)
    return conn


def add_to_watchlist(
    token: str, chain: str, effective_top_pct: float,
    symbol: str = "", db_path: Path = DB_PATH,
) -> None:
    """Add or refresh a near-saturation token (idempotent upsert)."""
    now = datetime.now(timezone.utc).isoformat()
    conn = _connect(db_path)
    try:
        conn.execute(
            """INSERT INTO watchlist (token, chain, added_at, updated_at,
                                      effective_top_pct, symbol, status)
               VALUES (?, ?, ?, ?, ?, ?, 'watching')
               ON CONFLICT(token, chain) DO UPDATE SET
                   updated_at = excluded.updated_at,
                   effective_top_pct = excluded.effective_top_pct,
                   symbol = excluded.symbol""",
            (token, chain, now, now, effective_top_pct, symbol),
        )
        conn.commit()
    finally:
        conn.close()
    logger.info("watchlist_add", token=token, chain=chain, eff=effective_top_pct)


def get_active(db_path: Path = DB_PATH) -> list[dict]:
    """Return non-expired watching entries (newest first)."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=TTL_DAYS)).isoformat()
    conn = _connect(db_path)
    try:
        rows = conn.execute(
            """SELECT token, chain, symbol, effective_top_pct, updated_at
               FROM watchlist
               WHERE status = 'watching' AND updated_at >= ?
               ORDER BY updated_at DESC""",
            (cutoff,),
        ).fetchall()
    finally:
        conn.close()
    cols = ["token", "chain", "symbol", "effective_top_pct", "updated_at"]
    return [dict(zip(cols, r)) for r in rows]


def set_status(token: str, chain: str, status: str, db_path: Path = DB_PATH) -> None:
    """Update an entry's status (e.g. 'launched', 'dropped')."""
    conn = _connect(db_path)
    try:
        conn.execute(
            "UPDATE watchlist SET status = ?, updated_at = ? WHERE token = ? AND chain = ?",
            (status, datetime.now(timezone.utc).isoformat(), token, chain),
        )
        conn.commit()
    finally:
        conn.close()


def prune(db_path: Path = DB_PATH) -> int:
    """Delete expired entries. Returns the number removed."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=TTL_DAYS)).isoformat()
    conn = _connect(db_path)
    try:
        cur = conn.execute("DELETE FROM watchlist WHERE updated_at < ?", (cutoff,))
        conn.commit()
        return cur.rowcount
    finally:
        conn.close()
