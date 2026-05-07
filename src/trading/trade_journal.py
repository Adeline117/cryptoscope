"""SQLite-backed trade journal for recording and querying trade history."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

from src.config import DATA_DIR

DB_PATH = DATA_DIR / "trade_journal.db"


def _get_conn(db_path: Path = DB_PATH) -> sqlite3.Connection:
    """Return a connection with row_factory set to sqlite3.Row."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def create_tables(db_path: Path = DB_PATH) -> None:
    """Create the trades table if it does not exist."""
    conn = _get_conn(db_path)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS trades (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            opened_at       TEXT NOT NULL,
            closed_at       TEXT,
            symbol          TEXT NOT NULL,
            direction       TEXT NOT NULL,
            leverage        REAL NOT NULL DEFAULT 1,
            entry_price     REAL NOT NULL,
            exit_price      REAL,
            size_usd        REAL NOT NULL,
            stop_loss       REAL,
            take_profit     REAL,
            pnl_usd         REAL,
            pnl_pct         REAL,
            signals_triggered TEXT,
            signal_confidence REAL,
            consensus_strength REAL,
            entry_reason    TEXT,
            exit_reason     TEXT,
            market_snapshot TEXT,
            notes           TEXT
        )
        """
    )
    conn.commit()
    conn.close()


def record_open(trade_data: dict[str, Any], db_path: Path = DB_PATH) -> int:
    """Insert an open trade and return its row id.

    Args:
        trade_data: Dict with keys matching the trades table columns.
        db_path: Override database path (useful for tests).

    Returns:
        The integer id of the newly inserted row.
    """
    create_tables(db_path)
    conn = _get_conn(db_path)

    signals = trade_data.get("signals_triggered")
    if isinstance(signals, (list, dict)):
        signals = json.dumps(signals, ensure_ascii=False)

    snapshot = trade_data.get("market_snapshot")
    if isinstance(snapshot, dict):
        snapshot = json.dumps(snapshot, ensure_ascii=False)

    now = datetime.now(timezone.utc).isoformat()

    cur = conn.execute(
        """
        INSERT INTO trades (
            opened_at, symbol, direction, leverage, entry_price,
            size_usd, stop_loss, take_profit,
            signals_triggered, signal_confidence, consensus_strength,
            entry_reason, market_snapshot, notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            trade_data.get("opened_at", now),
            trade_data["symbol"],
            trade_data["direction"],
            trade_data.get("leverage", 1),
            trade_data["entry_price"],
            trade_data["size_usd"],
            trade_data.get("stop_loss"),
            trade_data.get("take_profit"),
            signals,
            trade_data.get("signal_confidence"),
            trade_data.get("consensus_strength"),
            trade_data.get("entry_reason"),
            snapshot,
            trade_data.get("notes"),
        ),
    )
    conn.commit()
    trade_id = cur.lastrowid
    conn.close()
    return trade_id  # type: ignore[return-value]


def record_close(
    trade_id: int,
    exit_data: dict[str, Any],
    db_path: Path = DB_PATH,
) -> None:
    """Update a trade with closing information.

    Args:
        trade_id: The trade row id.
        exit_data: Dict with exit_price, pnl_usd, pnl_pct, exit_reason, notes.
        db_path: Override database path.
    """
    conn = _get_conn(db_path)
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """
        UPDATE trades
        SET closed_at   = ?,
            exit_price  = ?,
            pnl_usd     = ?,
            pnl_pct     = ?,
            exit_reason  = ?,
            notes        = COALESCE(notes || ' | ', '') || ?
        WHERE id = ?
        """,
        (
            exit_data.get("closed_at", now),
            exit_data["exit_price"],
            exit_data.get("pnl_usd"),
            exit_data.get("pnl_pct"),
            exit_data.get("exit_reason"),
            exit_data.get("notes", ""),
            trade_id,
        ),
    )
    conn.commit()
    conn.close()


def get_today_trades(db_path: Path = DB_PATH) -> list[dict[str, Any]]:
    """Return all trades opened today (UTC)."""
    create_tables(db_path)
    conn = _get_conn(db_path)
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    rows = conn.execute(
        "SELECT * FROM trades WHERE opened_at LIKE ? ORDER BY opened_at DESC",
        (f"{today_str}%",),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_win_rate_by_signal(db_path: Path = DB_PATH) -> list[dict[str, Any]]:
    """Return win-rate statistics grouped by signal type.

    Only considers closed trades (pnl_usd IS NOT NULL).
    """
    create_tables(db_path)
    conn = _get_conn(db_path)
    rows = conn.execute(
        """
        SELECT
            signals_triggered                         AS signal,
            COUNT(*)                                   AS total_trades,
            SUM(CASE WHEN pnl_usd > 0 THEN 1 ELSE 0 END) AS wins,
            SUM(CASE WHEN pnl_usd <= 0 THEN 1 ELSE 0 END) AS losses,
            ROUND(
                SUM(CASE WHEN pnl_usd > 0 THEN 1 ELSE 0 END) * 100.0
                / COUNT(*), 1
            )                                          AS win_rate_pct,
            ROUND(SUM(pnl_usd), 2)                    AS total_pnl_usd,
            ROUND(AVG(pnl_pct), 2)                     AS avg_pnl_pct
        FROM trades
        WHERE pnl_usd IS NOT NULL
        GROUP BY signals_triggered
        ORDER BY win_rate_pct DESC
        """
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_weekly_summary(db_path: Path = DB_PATH) -> dict[str, Any]:
    """Return a summary of this week's (Mon-Sun UTC) trading activity."""
    create_tables(db_path)
    conn = _get_conn(db_path)

    now = datetime.now(timezone.utc)
    monday = now - timedelta(days=now.weekday())
    week_start = monday.strftime("%Y-%m-%d")

    row = conn.execute(
        """
        SELECT
            COUNT(*)                                    AS total_trades,
            SUM(CASE WHEN pnl_usd > 0 THEN 1 ELSE 0 END) AS wins,
            SUM(CASE WHEN pnl_usd <= 0 THEN 1 ELSE 0 END) AS losses,
            ROUND(SUM(pnl_usd), 2)                     AS total_pnl_usd,
            ROUND(AVG(pnl_pct), 2)                      AS avg_pnl_pct,
            ROUND(MAX(pnl_usd), 2)                      AS best_trade_usd,
            ROUND(MIN(pnl_usd), 2)                      AS worst_trade_usd
        FROM trades
        WHERE opened_at >= ?
        """,
        (week_start,),
    ).fetchone()
    conn.close()

    if row is None:
        return {"week_start": week_start, "total_trades": 0}

    result = dict(row)
    result["week_start"] = week_start
    total = result.get("total_trades") or 0
    wins = result.get("wins") or 0
    result["win_rate_pct"] = round(wins / total * 100, 1) if total > 0 else 0.0
    return result
