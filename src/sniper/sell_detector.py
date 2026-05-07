"""Sell Detector — alerts when smart money starts selling tokens you hold.

Compares your paper trading positions against smart money wallet holdings.
If a tracked wallet reduces position in a token you hold → URGENT alert.

This is the "聪明钱出货预警" from the design spec.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import structlog

from src.sniper.smart_money_monitor import load_wallets, get_wallet_new_tokens

logger = structlog.get_logger()

# Store previous holdings snapshots to detect changes
_previous_holdings: dict[str, dict[str, float]] = {}  # wallet -> {mint: amount}


def detect_sells(my_positions: list[dict]) -> list[dict]:
    """Compare smart money holdings vs previous snapshot to detect sells.

    Args:
        my_positions: List of paper/real positions [{asset, direction, ...}]

    Returns:
        List of sell alerts: [{wallet, token, sold_pct, your_pnl}]
    """
    global _previous_holdings
    alerts = []

    # Get my held token mints
    my_mints = set()
    for p in my_positions:
        asset = p.get("asset", "")
        if len(asset) > 20:  # Looks like a mint address
            my_mints.add(asset)

    if not my_mints:
        return []

    wallets = load_wallets()
    t1_wallets = [w for w in wallets if w.get("tier") == 1]

    for w in t1_wallets[:15]:  # Check top 15 T1 wallets
        addr = w["address"]
        label = w.get("label", addr[:12])

        # Get current holdings
        current_tokens = get_wallet_new_tokens(addr)
        current_map = {t["mint"]: t["amount"] for t in current_tokens}

        # Compare with previous snapshot
        prev_map = _previous_holdings.get(addr, {})

        if prev_map:  # Only detect changes after first snapshot
            for mint in my_mints:
                prev_amount = prev_map.get(mint, 0)
                curr_amount = current_map.get(mint, 0)

                if prev_amount > 0 and curr_amount < prev_amount:
                    sold_pct = ((prev_amount - curr_amount) / prev_amount) * 100

                    if sold_pct >= 10:  # At least 10% reduction
                        alerts.append({
                            "wallet_address": addr,
                            "wallet_label": label,
                            "wallet_tier": w.get("tier", 3),
                            "token_mint": mint,
                            "prev_amount": prev_amount,
                            "curr_amount": curr_amount,
                            "sold_pct": round(sold_pct, 1),
                            "action": "全部卖出" if curr_amount == 0 else f"减仓{sold_pct:.0f}%",
                        })

        # Update snapshot
        _previous_holdings[addr] = current_map

    if alerts:
        logger.warning("smart_money_sells_detected", count=len(alerts))

    return alerts


def format_sell_alert(alerts: list[dict], my_position: dict | None = None) -> str:
    """Format sell alerts as Telegram HTML — emergency warning."""
    if not alerts:
        return ""

    token_mint = alerts[0].get("token_mint", "?")
    mint_short = f"{token_mint[:8]}...{token_mint[-6:]}" if len(token_mint) > 14 else token_mint

    total_sellers = len(alerts)
    total_tracked = 15  # T1 wallets checked

    lines = [
        f"🚨 <b>聪明钱出货预警</b>",
        f"━━━━━━━━━━━━━━━━━━━━",
        f"Token: <code>{mint_short}</code>",
        f"━━━━━━━━━━━━━━━━━━━━",
    ]

    for a in alerts[:5]:
        action_emoji = "🔴" if a["sold_pct"] >= 50 else "🟡"
        lines.append(
            f"{action_emoji} {a['wallet_label']} {a['action']} "
            f"({a['prev_amount']:.2f} → {a['curr_amount']:.2f})"
        )

    lines.append(f"━━━━━━━━━━━━━━━━━━━━")
    lines.append(f"⚡ 过去扫描周期内 {total_sellers}/{total_tracked} 聪明钱已减仓/离场")

    if my_position:
        lines.append(f"\n你的持仓: {my_position.get('direction', '?')} · "
                     f"PnL: {my_position.get('pnl_pct', 0):+.1f}%")

    return "\n".join(lines)


async def run_sell_detector() -> dict:
    """Check for smart money sells on your positions."""
    # Get paper positions
    try:
        from src.trading.paper_trader import get_open_positions
        positions = get_open_positions()
    except Exception:
        positions = []

    if not positions:
        return {"status": "no_positions"}

    alerts = detect_sells(positions)

    sent = 0
    if alerts:
        msg = format_sell_alert(alerts)
        try:
            from src.distribution.telegram_sender import send_critical_alert
            await send_critical_alert(msg)
            sent += 1
        except Exception as e:
            logger.error("sell_alert_send_failed", error=str(e))

    return {"alerts": len(alerts), "sent": sent}
