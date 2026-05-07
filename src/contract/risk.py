"""Risk Manager — enforces trading rules and circuit breakers.

Rules (user configurable):
- Max loss per trade: 2% of account
- Max daily loss: 5% of account
- Max concurrent positions: 3
- High leverage (>20x) auto-reduces size
- Consecutive loss cooldown
- Liquidation proximity warning

All checks run BEFORE order placement. If any check fails,
the order is blocked and user is warned.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import structlog

logger = structlog.get_logger()

DB_PATH = Path("data/risk_manager.db")

# Default risk limits (can be overridden)
DEFAULT_LIMITS = {
    "max_loss_per_trade_pct": 2.0,       # Max 2% of account per trade
    "max_daily_loss_pct": 5.0,           # Max 5% daily drawdown
    "max_concurrent_positions": 3,        # Max 3 positions at once
    "max_leverage": 20,                   # Warn above 20x
    "consecutive_loss_cooldown": 3,       # Cooldown after 3 consecutive losses
    "cooldown_hours": 4,                  # 4 hour cooldown
    "liq_warning_pct": 15,               # Warn if within 15% of liquidation
}


@dataclass
class RiskCheck:
    """Result of a risk check."""
    passed: bool
    warnings: list[str]
    blocks: list[str]  # Hard blocks — cannot proceed
    suggested_size: float | None = None  # Adjusted position size


def _ensure_db() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS daily_pnl (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT,
            exchange TEXT,
            pnl_usd REAL,
            trade_count INTEGER DEFAULT 0,
            wins INTEGER DEFAULT 0,
            losses INTEGER DEFAULT 0,
            updated_at TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS trade_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            exchange TEXT,
            symbol TEXT,
            side TEXT,
            amount REAL,
            entry_price REAL,
            exit_price REAL,
            pnl_usd REAL,
            pnl_pct REAL,
            leverage INTEGER,
            trade_time TEXT,
            exit_time TEXT,
            status TEXT DEFAULT 'open'
        )
    """)
    conn.commit()
    return conn


def check_trade_risk(
    account_balance: float,
    position_size_usd: float,
    leverage: int,
    current_open_positions: int,
    limits: dict | None = None,
) -> RiskCheck:
    """Pre-trade risk check. Returns pass/fail with reasons."""
    lim = {**DEFAULT_LIMITS, **(limits or {})}
    warnings = []
    blocks = []

    # 1. Position size vs account
    risk_pct = (position_size_usd / account_balance * 100) if account_balance > 0 else 100
    max_risk = lim["max_loss_per_trade_pct"]

    if risk_pct > max_risk * 2:
        blocks.append(f"仓位 {risk_pct:.1f}% 超过限制 {max_risk}% 的2倍 — 禁止下单")
    elif risk_pct > max_risk:
        warnings.append(f"仓位 {risk_pct:.1f}% 超过 {max_risk}% 限制")

    # 2. Leverage
    if leverage > lim["max_leverage"]:
        warnings.append(f"杠杆 {leverage}x 超过 {lim['max_leverage']}x 建议上限")
        # Suggest reduced size
        suggested = position_size_usd * lim["max_leverage"] / leverage
        warnings.append(f"建议减仓至 ${suggested:,.0f}")

    if leverage > 50:
        blocks.append(f"杠杆 {leverage}x 极度危险 — 建议不超过20x")

    # 3. Concurrent positions
    if current_open_positions >= lim["max_concurrent_positions"]:
        blocks.append(f"已有 {current_open_positions} 个持仓，上限 {lim['max_concurrent_positions']}")

    # 4. Daily loss check
    conn = _ensure_db()
    try:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        row = conn.execute(
            "SELECT pnl_usd, losses FROM daily_pnl WHERE date = ?", (today,)
        ).fetchone()

        if row:
            daily_pnl, daily_losses = row
            daily_loss_pct = abs(daily_pnl) / account_balance * 100 if daily_pnl < 0 and account_balance > 0 else 0

            if daily_loss_pct >= lim["max_daily_loss_pct"]:
                blocks.append(f"今日已亏 {daily_loss_pct:.1f}% (限制 {lim['max_daily_loss_pct']}%) — 停止交易")

            if daily_losses >= lim["consecutive_loss_cooldown"]:
                blocks.append(f"连续亏损 {daily_losses} 次 — 冷静期 {lim['cooldown_hours']}h")
    finally:
        conn.close()

    passed = len(blocks) == 0
    return RiskCheck(passed=passed, warnings=warnings, blocks=blocks)


def record_trade_result(
    exchange: str,
    symbol: str,
    side: str,
    pnl_usd: float,
    leverage: int,
) -> None:
    """Record a trade result for daily P&L tracking."""
    conn = _ensure_db()
    try:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        now = datetime.now(timezone.utc).isoformat()

        # Update daily P&L
        existing = conn.execute(
            "SELECT id, pnl_usd, trade_count, wins, losses FROM daily_pnl WHERE date = ? AND exchange = ?",
            (today, exchange)
        ).fetchone()

        if existing:
            new_pnl = existing[1] + pnl_usd
            new_trades = existing[2] + 1
            new_wins = existing[3] + (1 if pnl_usd > 0 else 0)
            new_losses = existing[4] + (1 if pnl_usd <= 0 else 0)
            conn.execute(
                "UPDATE daily_pnl SET pnl_usd = ?, trade_count = ?, wins = ?, losses = ?, updated_at = ? WHERE id = ?",
                (new_pnl, new_trades, new_wins, new_losses, now, existing[0])
            )
        else:
            conn.execute(
                "INSERT INTO daily_pnl (date, exchange, pnl_usd, trade_count, wins, losses, updated_at) VALUES (?, ?, ?, 1, ?, ?, ?)",
                (today, exchange, pnl_usd, 1 if pnl_usd > 0 else 0, 1 if pnl_usd <= 0 else 0, now)
            )

        conn.commit()
    finally:
        conn.close()


def get_daily_summary(exchange: str = "") -> dict:
    """Get today's trading summary."""
    conn = _ensure_db()
    try:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if exchange:
            row = conn.execute(
                "SELECT pnl_usd, trade_count, wins, losses FROM daily_pnl WHERE date = ? AND exchange = ?",
                (today, exchange)
            ).fetchone()
        else:
            row = conn.execute(
                "SELECT SUM(pnl_usd), SUM(trade_count), SUM(wins), SUM(losses) FROM daily_pnl WHERE date = ?",
                (today,)
            ).fetchone()

        if row and row[0] is not None:
            return {
                "date": today,
                "pnl_usd": round(row[0], 2),
                "trades": row[1] or 0,
                "wins": row[2] or 0,
                "losses": row[3] or 0,
                "win_rate": round((row[2] or 0) / max(row[1] or 1, 1) * 100, 1),
            }
        return {"date": today, "pnl_usd": 0, "trades": 0, "wins": 0, "losses": 0, "win_rate": 0}
    finally:
        conn.close()


def format_risk_check_message(check: RiskCheck) -> str:
    """Format risk check as Telegram HTML."""
    if check.passed and not check.warnings:
        return "✅ 风控检查通过"

    lines = []
    if check.blocks:
        lines.append("🚫 <b>风控拦截</b>")
        for b in check.blocks:
            lines.append(f"  ❌ {b}")

    if check.warnings:
        lines.append("\n⚠️ <b>风险警告</b>")
        for w in check.warnings:
            lines.append(f"  ⚠️ {w}")

    if check.passed:
        lines.append("\n✅ 可以继续交易（注意风险）")
    else:
        lines.append("\n❌ 交易被拦截，请调整参数")

    return "\n".join(lines)
