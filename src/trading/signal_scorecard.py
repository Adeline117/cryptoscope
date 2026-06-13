"""Signal Scorecard — tracks every signal and measures if it was right.

Records signal price at alert time, then checks 1h/4h/24h later.
This is the ONLY way to know which signals actually make money.

Schema:
  signal_id | signal_type | asset | chain | direction | confidence
  price_at_signal | price_1h | price_4h | price_24h
  pnl_1h_pct | pnl_4h_pct | pnl_24h_pct
  was_profitable_1h | was_profitable_4h | was_profitable_24h
  created_at
"""

from __future__ import annotations

import json
import os
import sqlite3
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

import structlog

logger = structlog.get_logger()

DB_PATH = Path("data/signal_scorecard.db")


def _ensure_db() -> sqlite3.Connection:
    """Create DB and table if not exists."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    conn.execute("PRAGMA busy_timeout=10000")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_id TEXT UNIQUE,
            signal_type TEXT,
            asset TEXT,
            chain TEXT,
            direction TEXT,
            confidence INTEGER,
            entry_price REAL,
            price_1h REAL,
            price_4h REAL,
            price_24h REAL,
            pnl_1h_pct REAL,
            pnl_4h_pct REAL,
            pnl_24h_pct REAL,
            was_profitable_1h INTEGER,
            was_profitable_4h INTEGER,
            was_profitable_24h INTEGER,
            metadata TEXT,
            created_at TEXT,
            checked_1h INTEGER DEFAULT 0,
            checked_4h INTEGER DEFAULT 0,
            checked_24h INTEGER DEFAULT 0
        )
    """)
    conn.commit()
    return conn


def record_signal(
    signal_type: str,
    asset: str,
    chain: str,
    direction: str,
    confidence: int,
    entry_price: float,
    metadata: dict | None = None,
) -> str:
    """Record a new signal with its entry price. Returns signal_id."""
    now = datetime.now(timezone.utc)
    signal_id = f"{signal_type}_{asset}_{now.strftime('%Y%m%d_%H%M%S')}"

    conn = _ensure_db()
    try:
        conn.execute(
            """INSERT OR IGNORE INTO signals
               (signal_id, signal_type, asset, chain, direction, confidence,
                entry_price, metadata, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (signal_id, signal_type, asset, chain, direction, confidence,
             entry_price, json.dumps(metadata or {}), now.isoformat()),
        )
        conn.commit()
        logger.info("signal_recorded", signal_id=signal_id, asset=asset, price=entry_price)
    finally:
        conn.close()

    return signal_id


def _get_price(asset: str, chain: str = "") -> float | None:
    """Get current price from DexScreener or CoinGecko."""
    # For known major coins, use CoinGecko
    coin_map = {
        "BTC": "bitcoin", "ETH": "ethereum", "SOL": "solana",
        "ARB": "arbitrum", "OP": "optimism", "APT": "aptos",
        "TIA": "celestia", "SUI": "sui", "STRK": "starknet",
    }

    cg_id = coin_map.get(asset.upper())
    if cg_id:
        try:
            url = f"https://api.coingecko.com/api/v3/simple/price?ids={cg_id}&vs_currencies=usd"
            req = urllib.request.Request(url, headers={"User-Agent": "CryptoScope/1.0"})
            with urllib.request.urlopen(req, timeout=8) as resp:
                data = json.loads(resp.read().decode())
            return data.get(cg_id, {}).get("usd")
        except Exception:
            pass

    # For meme coins, use DexScreener
    if chain in ("sol", "solana") and len(asset) > 10:  # Looks like an address
        try:
            url = f"https://api.dexscreener.com/token-pairs/v1/solana/{asset}"
            req = urllib.request.Request(url, headers={"User-Agent": "CryptoScope/1.0"})
            with urllib.request.urlopen(req, timeout=8) as resp:
                pairs = json.loads(resp.read().decode())
            if isinstance(pairs, list) and pairs:
                return float(pairs[0].get("priceUsd", 0))
        except Exception:
            pass

    return None


def check_pending_signals():
    """Check signals that need 1h/4h/24h price updates. Run this every 30 min."""
    conn = _ensure_db()
    now = datetime.now(timezone.utc)

    try:
        rows = conn.execute(
            "SELECT id, signal_id, asset, chain, direction, entry_price, created_at, "
            "checked_1h, checked_4h, checked_24h FROM signals "
            "WHERE checked_24h = 0"
        ).fetchall()

        # Price cache — fetch once per asset, reuse for all checkpoints
        price_cache: dict[str, float | None] = {}

        for row in rows:
            (row_id, signal_id, asset, chain, direction, entry_price,
             created_at_str, checked_1h, checked_4h, checked_24h) = row

            created_at = datetime.fromisoformat(created_at_str)
            age_hours = (now - created_at).total_seconds() / 3600

            updates = {}

            # Fetch price once per asset (cached)
            if asset not in price_cache:
                price_cache[asset] = _get_price(asset, chain)
            cached_price = price_cache[asset]

            # Check 1h
            if not checked_1h and age_hours >= 1:
                price = cached_price
                if price and entry_price:
                    pnl = ((price - entry_price) / entry_price) * 100
                    if direction == "SHORT":
                        pnl = -pnl
                    updates["price_1h"] = price
                    updates["pnl_1h_pct"] = round(pnl, 2)
                    updates["was_profitable_1h"] = 1 if pnl > 0 else 0
                    updates["checked_1h"] = 1

            # Check 4h
            if not checked_4h and age_hours >= 4:
                price = cached_price
                if price and entry_price:
                    pnl = ((price - entry_price) / entry_price) * 100
                    if direction == "SHORT":
                        pnl = -pnl
                    updates["price_4h"] = price
                    updates["pnl_4h_pct"] = round(pnl, 2)
                    updates["was_profitable_4h"] = 1 if pnl > 0 else 0
                    updates["checked_4h"] = 1

            # Check 24h
            if not checked_24h and age_hours >= 24:
                price = cached_price
                if price and entry_price:
                    pnl = ((price - entry_price) / entry_price) * 100
                    if direction == "SHORT":
                        pnl = -pnl
                    updates["price_24h"] = price
                    updates["pnl_24h_pct"] = round(pnl, 2)
                    updates["was_profitable_24h"] = 1 if pnl > 0 else 0
                    updates["checked_24h"] = 1

            if updates:
                set_clause = ", ".join(f"{k} = ?" for k in updates)
                values = list(updates.values()) + [row_id]
                conn.execute(f"UPDATE signals SET {set_clause} WHERE id = ?", values)
                logger.info("signal_updated", signal_id=signal_id, updates=updates)

        conn.commit()
    finally:
        conn.close()


def get_scorecard_summary() -> dict:
    """Get overall performance summary by signal type."""
    conn = _ensure_db()
    try:
        rows = conn.execute("""
            SELECT signal_type,
                   COUNT(*) as total,
                   SUM(CASE WHEN was_profitable_1h = 1 THEN 1 ELSE 0 END) as wins_1h,
                   SUM(CASE WHEN was_profitable_4h = 1 THEN 1 ELSE 0 END) as wins_4h,
                   SUM(CASE WHEN was_profitable_24h = 1 THEN 1 ELSE 0 END) as wins_24h,
                   AVG(pnl_1h_pct) as avg_pnl_1h,
                   AVG(pnl_4h_pct) as avg_pnl_4h,
                   AVG(pnl_24h_pct) as avg_pnl_24h,
                   SUM(checked_24h) as completed
            FROM signals
            GROUP BY signal_type
        """).fetchall()

        summary = {}
        for row in rows:
            (sig_type, total, wins_1h, wins_4h, wins_24h,
             avg_1h, avg_4h, avg_24h, completed) = row
            summary[sig_type] = {
                "total": total,
                "completed": completed or 0,
                "win_rate_1h": f"{(wins_1h or 0) / max(total, 1) * 100:.0f}%",
                "win_rate_4h": f"{(wins_4h or 0) / max(total, 1) * 100:.0f}%",
                "win_rate_24h": f"{(wins_24h or 0) / max(total, 1) * 100:.0f}%",
                "avg_pnl_1h": f"{avg_1h or 0:+.2f}%",
                "avg_pnl_4h": f"{avg_4h or 0:+.2f}%",
                "avg_pnl_24h": f"{avg_24h or 0:+.2f}%",
            }
        return summary
    finally:
        conn.close()


def format_scorecard_message() -> str:
    """Format scorecard as Telegram HTML."""
    summary = get_scorecard_summary()
    if not summary:
        return "📊 <b>信号记分卡</b>\n\n暂无数据。信号记录后1h/4h/24h自动验证。"

    lines = ["📊 <b>信号记分卡 — 哪个信号能赚钱？</b>", ""]

    for sig_type, data in summary.items():
        type_labels = {
            "token_unlock": "解锁做空",
            "funding_reversion": "费率回归",
            "boost_detection": "Boost检测",
            "smart_money_cluster": "聪明钱聚集",
            "meme_alert": "Meme扫描",
        }
        label = type_labels.get(sig_type, sig_type)
        total = data["total"]
        completed = data["completed"]

        lines.append(f"<b>{label}</b> ({completed}/{total}笔已验证)")
        lines.append(
            f"  胜率: 1h {data['win_rate_1h']} · 4h {data['win_rate_4h']} · 24h {data['win_rate_24h']}"
        )
        lines.append(
            f"  均PnL: 1h {data['avg_pnl_1h']} · 4h {data['avg_pnl_4h']} · 24h {data['avg_pnl_24h']}"
        )
        lines.append("")

    return "\n".join(lines)
