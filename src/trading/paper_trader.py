"""Paper Trading Engine — simulates real trading with virtual capital.

Starts with virtual 10 SOL. When signals fire, auto-"buys" and tracks P&L.
Strict rules: sell 50% at +100%, sell all at -30%, time stop at 24h.

This proves whether the system makes money BEFORE risking real capital.
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

DB_PATH = Path("data/paper_trades.db")
INITIAL_BALANCE_SOL = 10.0
MAX_POSITION_PCT = 0.05  # 5% of balance per trade
MAX_CONCURRENT = 5
TP_PCT = 100.0          # Take profit: +100% sell half
SL_PCT = -30.0          # Stop loss: -30% sell all
TRAILING_STOP_PCT = 25  # Trailing stop: if drops 25% from peak
TIME_STOP_HOURS = 24


def _ensure_db() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            asset TEXT,
            chain TEXT,
            direction TEXT,
            entry_price REAL,
            current_price REAL,
            peak_price REAL,
            amount_sol REAL,
            amount_tokens REAL,
            pnl_pct REAL DEFAULT 0,
            pnl_sol REAL DEFAULT 0,
            status TEXT DEFAULT 'open',
            signal_type TEXT,
            entry_time TEXT,
            exit_time TEXT,
            exit_price REAL,
            exit_reason TEXT,
            metadata TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS balance_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            balance_sol REAL,
            open_positions INTEGER,
            total_pnl_sol REAL,
            timestamp TEXT
        )
    """)
    conn.commit()
    return conn


def get_balance() -> float:
    """Get current virtual balance."""
    conn = _ensure_db()
    try:
        # Start with initial balance, subtract open positions, add closed P&L
        allocated = conn.execute(
            "SELECT COALESCE(SUM(amount_sol), 0) FROM positions WHERE status = 'open'"
        ).fetchone()[0]
        realized = conn.execute(
            "SELECT COALESCE(SUM(pnl_sol), 0) FROM positions WHERE status = 'closed'"
        ).fetchone()[0]
        return INITIAL_BALANCE_SOL - allocated + realized
    finally:
        conn.close()


def get_open_positions() -> list[dict]:
    conn = _ensure_db()
    try:
        rows = conn.execute(
            "SELECT id, asset, chain, direction, entry_price, amount_sol, "
            "amount_tokens, signal_type, entry_time FROM positions WHERE status = 'open'"
        ).fetchall()
        return [
            {
                "id": r[0], "asset": r[1], "chain": r[2], "direction": r[3],
                "entry_price": r[4], "amount_sol": r[5], "amount_tokens": r[6],
                "signal_type": r[7], "entry_time": r[8],
            }
            for r in rows
        ]
    finally:
        conn.close()


def open_position(
    asset: str,
    chain: str,
    direction: str,
    price: float,
    signal_type: str,
    metadata: dict | None = None,
) -> dict | None:
    """Open a paper position. Returns position dict or None if rejected."""
    balance = get_balance()
    open_count = len(get_open_positions())

    # Checks
    if open_count >= MAX_CONCURRENT:
        logger.info("paper_trade_rejected", reason="max_concurrent", open=open_count)
        return None

    if balance < 0.05:  # Less than 0.05 SOL available
        logger.info("paper_trade_rejected", reason="insufficient_balance", balance=balance)
        return None

    # Position size: 5% of balance, min 0.02 SOL
    size_sol = max(balance * MAX_POSITION_PCT, 0.02)
    size_sol = min(size_sol, balance * 0.2)  # Never more than 20%
    amount_tokens = size_sol / price if price > 0 else 0

    now = datetime.now(timezone.utc)

    conn = _ensure_db()
    try:
        conn.execute(
            """INSERT INTO positions
               (asset, chain, direction, entry_price, current_price,
                amount_sol, amount_tokens, signal_type, entry_time, status, metadata)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'open', ?)""",
            (asset, chain, direction, price, price,
             round(size_sol, 4), amount_tokens, signal_type, now.isoformat(),
             json.dumps(metadata or {})),
        )
        conn.commit()
        logger.info(
            "paper_position_opened",
            asset=asset, direction=direction, price=price,
            size_sol=round(size_sol, 4), signal_type=signal_type,
        )
        return {
            "asset": asset, "direction": direction, "price": price,
            "size_sol": round(size_sol, 4), "signal_type": signal_type,
        }
    finally:
        conn.close()


def _get_price(asset: str, chain: str = "") -> float | None:
    """Get current price."""
    # Major coins via CoinGecko
    coin_map = {
        "BTC": "bitcoin", "ETH": "ethereum", "SOL": "solana",
        "ARB": "arbitrum", "OP": "optimism", "TIA": "celestia",
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

    # Meme coins via DexScreener (asset = contract address)
    if chain in ("sol", "solana") and len(asset) > 20:
        try:
            url = f"https://api.dexscreener.com/token-pairs/v1/solana/{asset}"
            req = urllib.request.Request(url, headers={"User-Agent": "CryptoScope/1.0"})
            with urllib.request.urlopen(req, timeout=8) as resp:
                pairs = json.loads(resp.read().decode())
            if isinstance(pairs, list) and pairs:
                best = max(pairs, key=lambda p: (p.get("liquidity", {}) or {}).get("usd", 0) or 0)
                return float(best.get("priceUsd", 0))
        except Exception:
            pass
    return None


def check_exits():
    """Check all open positions for TP/SL/time exits. Run every 30 seconds."""
    positions = get_open_positions()
    now = datetime.now(timezone.utc)

    for pos in positions:
        price = _get_price(pos["asset"], pos["chain"])
        if not price or not pos["entry_price"]:
            continue

        entry = pos["entry_price"]
        pnl_pct = ((price - entry) / entry) * 100
        if pos["direction"] == "SHORT":
            pnl_pct = -pnl_pct

        pnl_sol = pos["amount_sol"] * (pnl_pct / 100)

        # Time stop
        entry_time = datetime.fromisoformat(pos["entry_time"])
        age_hours = (now - entry_time).total_seconds() / 3600

        exit_reason = None

        # Track peak price for trailing stop — default to entry, NOT current price
        peak = entry  # Safe default; updated from DB below if available

        if pnl_pct >= TP_PCT:
            exit_reason = f"TP +{pnl_pct:.0f}%"
        elif pnl_pct <= SL_PCT:
            exit_reason = f"SL {pnl_pct:.0f}%"
        elif age_hours >= TIME_STOP_HOURS:
            exit_reason = f"时间止损 {age_hours:.0f}h"

        # Trailing stop check
        if not exit_reason and pnl_pct > 20:  # Only activate after +20%
            # Get peak from DB
            conn_peek = _ensure_db()
            try:
                row = conn_peek.execute(
                    "SELECT peak_price FROM positions WHERE id = ?", (pos["id"],)
                ).fetchone()
                db_peak = row[0] if row and row[0] else entry
                peak = max(db_peak, price) if pos["direction"] == "LONG" else min(db_peak, price) if db_peak > 0 else price

                # Check if dropped TRAILING_STOP_PCT from peak
                if pos["direction"] == "LONG" and peak > 0:
                    drop_from_peak = ((peak - price) / peak) * 100
                    if drop_from_peak >= TRAILING_STOP_PCT:
                        exit_reason = f"尾随止盈 (从峰值跌{drop_from_peak:.0f}%)"
                elif pos["direction"] == "SHORT" and peak > 0:
                    rise_from_peak = ((price - peak) / peak) * 100
                    if rise_from_peak >= TRAILING_STOP_PCT:
                        exit_reason = f"尾随止盈 (从低点涨{rise_from_peak:.0f}%)"
            finally:
                conn_peek.close()

        # Update current price and peak
        conn = _ensure_db()
        try:
            conn.execute(
                "UPDATE positions SET current_price = ?, peak_price = ?, pnl_pct = ?, pnl_sol = ? WHERE id = ?",
                (price, peak, round(pnl_pct, 2), round(pnl_sol, 4), pos["id"]),
            )

            if exit_reason:
                conn.execute(
                    """UPDATE positions SET status = 'closed', exit_time = ?,
                       exit_price = ?, exit_reason = ?, pnl_pct = ?, pnl_sol = ?
                       WHERE id = ?""",
                    (now.isoformat(), price, exit_reason,
                     round(pnl_pct, 2), round(pnl_sol, 4), pos["id"]),
                )
                logger.info(
                    "paper_position_closed",
                    asset=pos["asset"], reason=exit_reason,
                    pnl_pct=round(pnl_pct, 2), pnl_sol=round(pnl_sol, 4),
                )

            conn.commit()
        finally:
            conn.close()


def get_performance_summary() -> dict:
    """Get overall paper trading performance."""
    conn = _ensure_db()
    try:
        total = conn.execute("SELECT COUNT(*) FROM positions").fetchone()[0]
        closed = conn.execute("SELECT COUNT(*) FROM positions WHERE status = 'closed'").fetchone()[0]
        open_count = conn.execute("SELECT COUNT(*) FROM positions WHERE status = 'open'").fetchone()[0]

        winners = conn.execute(
            "SELECT COUNT(*) FROM positions WHERE status = 'closed' AND pnl_sol > 0"
        ).fetchone()[0]

        total_pnl = conn.execute(
            "SELECT COALESCE(SUM(pnl_sol), 0) FROM positions WHERE status = 'closed'"
        ).fetchone()[0]

        avg_win = conn.execute(
            "SELECT COALESCE(AVG(pnl_pct), 0) FROM positions WHERE status = 'closed' AND pnl_sol > 0"
        ).fetchone()[0]

        avg_loss = conn.execute(
            "SELECT COALESCE(AVG(pnl_pct), 0) FROM positions WHERE status = 'closed' AND pnl_sol <= 0"
        ).fetchone()[0]

        balance = get_balance()
        win_rate = (winners / closed * 100) if closed > 0 else 0

        return {
            "balance_sol": round(balance, 4),
            "initial_sol": INITIAL_BALANCE_SOL,
            "total_pnl_sol": round(total_pnl, 4),
            "total_pnl_pct": round((balance - INITIAL_BALANCE_SOL) / INITIAL_BALANCE_SOL * 100, 2),
            "total_trades": total,
            "open_positions": open_count,
            "closed_trades": closed,
            "winners": winners,
            "losers": closed - winners,
            "win_rate": round(win_rate, 1),
            "avg_win_pct": round(avg_win, 2),
            "avg_loss_pct": round(avg_loss, 2),
        }
    finally:
        conn.close()


def format_performance_message() -> str:
    """Format paper trading results as Telegram HTML."""
    p = get_performance_summary()

    if p["total_trades"] == 0:
        return "📋 <b>模拟交易</b>\n\n虚拟资金: 10 SOL\n暂无交易。等待信号..."

    pnl_emoji = "📈" if p["total_pnl_sol"] > 0 else "📉"
    wr_emoji = "🟢" if p["win_rate"] > 50 else "🔴" if p["win_rate"] < 40 else "🟡"

    lines = [
        f"📋 <b>模拟交易成绩单</b>",
        f"",
        f"💰 余额: {p['balance_sol']:.4f} SOL ({p['total_pnl_pct']:+.1f}%)",
        f"{pnl_emoji} 总盈亏: {p['total_pnl_sol']:+.4f} SOL",
        f"",
        f"📊 交易: {p['closed_trades']}笔完成 · {p['open_positions']}笔持仓中",
        f"{wr_emoji} 胜率: {p['win_rate']:.0f}% ({p['winners']}胜/{p['losers']}负)",
        f"📈 均赢: {p['avg_win_pct']:+.1f}% | 📉 均亏: {p['avg_loss_pct']:+.1f}%",
    ]

    # Verdict. A paper PnL/20-trade win rate is not proof of an edge: it can repeat
    # one market episode, omit executable slippage/funding, and has no matched base
    # rate. Keep this dashboard useful without granting a false "go live" licence.
    if p["closed_trades"] >= 20:
        if p["win_rate"] > 50 and p["total_pnl_sol"] > 0:
            lines.append("\n🟡 <b>纸面结果为正，仍不可据此授权实盘</b>"
                         " — 还需独立事件、净成本和基准率验证")
        elif p["win_rate"] > 40:
            lines.append("\n🟡 <b>纸面结果边缘，继续积累独立样本</b>")
        else:
            lines.append("\n🔴 <b>纸面结果为负，停止扩大风险</b>")
    else:
        remaining = 20 - p["closed_trades"]
        lines.append(f"\n⏳ 还需 {remaining} 笔交易才能判断")

    return "\n".join(lines)
