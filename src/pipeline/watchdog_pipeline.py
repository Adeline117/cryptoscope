"""5-minute watchdog pipeline: lightweight critical checks with immediate P0 alerts.

Checks:
1. Stablecoin depeg (USDT/USDC price deviation > 1% from $1.00)
2. BTC/ETH 1-hour crash (> 5% drop)
3. Market-wide liquidation surge (if data available)
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone

import aiohttp
import structlog

logger = structlog.get_logger()

COINGECKO_BASE = "https://api.coingecko.com/api/v3"

# Thresholds
DEPEG_THRESHOLD_PCT = 1.0  # 1% deviation from $1.00
CRASH_THRESHOLD_PCT = 5.0  # 5% drop in 1 hour


async def run_watchdog_pipeline() -> dict:
    """Run all watchdog checks and fire P0 alerts for any triggers.

    Returns a summary dict of what was checked and any alerts fired.
    """
    logger.info("watchdog_pipeline_started")
    alerts_fired: list[str] = []

    timeout = aiohttp.ClientTimeout(total=15)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        # Run checks concurrently
        results = await asyncio.gather(
            _check_stablecoin_depeg(session),
            _check_btc_eth_crash(session),
            _check_liquidation_surge(session),
            return_exceptions=True,
        )

    for result in results:
        if isinstance(result, Exception):
            logger.warning("watchdog_check_error", error=str(result))
            continue
        if isinstance(result, list):
            alerts_fired.extend(result)

    # Fire P0 alerts via Telegram
    if alerts_fired:
        logger.warning("watchdog_alerts_triggered", count=len(alerts_fired))
        try:
            from src.distribution.telegram_sender import send_critical_alert

            for alert_msg in alerts_fired:
                await send_critical_alert(alert_msg)
                await asyncio.sleep(0.3)
        except Exception as e:
            logger.error("watchdog_telegram_failed", error=str(e))

    summary = {
        "status": "alerts_fired" if alerts_fired else "all_clear",
        "alerts_count": len(alerts_fired),
        "alerts": alerts_fired,
        "checked_at": datetime.now(timezone.utc).isoformat(),
    }
    logger.info("watchdog_pipeline_complete", **summary)
    return summary


async def _check_stablecoin_depeg(session: aiohttp.ClientSession) -> list[str]:
    """Check USDT and USDC prices for deviation from $1.00."""
    alerts = []
    try:
        url = f"{COINGECKO_BASE}/simple/price"
        params = {
            "ids": "tether,usd-coin",
            "vs_currencies": "usd",
            "include_24hr_change": "true",
        }
        async with session.get(url, params=params) as resp:
            resp.raise_for_status()
            data = await resp.json()

        stablecoins = {
            "tether": ("USDT", data.get("tether", {})),
            "usd-coin": ("USDC", data.get("usd-coin", {})),
        }

        for coin_id, (symbol, price_data) in stablecoins.items():
            price = price_data.get("usd")
            if price is None:
                continue

            deviation_pct = abs(price - 1.0) * 100
            if deviation_pct >= DEPEG_THRESHOLD_PCT:
                direction = "above" if price > 1.0 else "below"
                alerts.append(
                    f"[P0 STABLECOIN DEPEG] {symbol} is trading at ${price:.4f} "
                    f"({deviation_pct:.2f}% {direction} peg)\n\n"
                    f"This is a CRITICAL event. Stablecoin depeg may indicate:\n"
                    f"- Exchange insolvency risk\n"
                    f"- Bank run / redemption crisis\n"
                    f"- Smart contract exploit\n\n"
                    f"Immediate action recommended."
                )
                logger.critical(
                    "stablecoin_depeg_detected",
                    symbol=symbol,
                    price=price,
                    deviation_pct=deviation_pct,
                )

    except Exception as e:
        logger.warning("stablecoin_check_failed", error=str(e))

    return alerts


async def _check_btc_eth_crash(session: aiohttp.ClientSession) -> list[str]:
    """Check BTC and ETH for > 5% drop in the last 1 hour."""
    alerts = []
    try:
        url = f"{COINGECKO_BASE}/coins/markets"
        params = {
            "vs_currency": "usd",
            "ids": "bitcoin,ethereum",
            "price_change_percentage": "1h",
            "sparkline": "false",
        }
        async with session.get(url, params=params) as resp:
            resp.raise_for_status()
            data = await resp.json()

        for coin in data:
            name = coin.get("name", "Unknown")
            symbol = coin.get("symbol", "?").upper()
            price = coin.get("current_price", 0)
            change_1h = coin.get("price_change_percentage_1h_in_currency") or 0

            if change_1h <= -CRASH_THRESHOLD_PCT:
                alerts.append(
                    f"[P0 FLASH CRASH] {name} ({symbol}) dropped {abs(change_1h):.1f}% "
                    f"in the last hour\n\n"
                    f"Current price: ${price:,.2f}\n"
                    f"1h change: {change_1h:+.2f}%\n\n"
                    f"Possible causes:\n"
                    f"- Large liquidation cascade\n"
                    f"- Exchange outage / hack\n"
                    f"- Major negative news event\n\n"
                    f"Monitor closely."
                )
                logger.critical(
                    "btc_eth_crash_detected",
                    coin=symbol,
                    price=price,
                    change_1h=change_1h,
                )

    except Exception as e:
        logger.warning("btc_eth_crash_check_failed", error=str(e))

    return alerts


async def _check_liquidation_surge(session: aiohttp.ClientSession) -> list[str]:
    """Check for market-wide liquidation surges via CoinGlass public data.

    Note: CoinGlass API requires a key for full access. We attempt the public
    endpoint; if unavailable, we skip gracefully.
    """
    alerts = []
    try:
        # Try CoinGlass public liquidation endpoint
        url = "https://open-api.coinglass.com/public/v2/liquidation_history"
        params = {"time_type": 1, "symbol": "all"}
        async with session.get(url, params=params) as resp:
            if resp.status != 200:
                logger.debug("liquidation_data_unavailable", status=resp.status)
                return alerts
            data = await resp.json()

        # Parse liquidation data if available
        liq_data = data.get("data", [])
        if not liq_data:
            return alerts

        # Check for total liquidations in the last hour
        # CoinGlass returns liquidation amounts; we check for spikes
        latest = liq_data[0] if liq_data else {}
        total_liq = latest.get("longLiquidationUsd", 0) + latest.get("shortLiquidationUsd", 0)

        # Alert if total liquidations exceed $500M in one period
        LIQUIDATION_THRESHOLD = 500_000_000
        if total_liq >= LIQUIDATION_THRESHOLD:
            long_liq = latest.get("longLiquidationUsd", 0)
            short_liq = latest.get("shortLiquidationUsd", 0)
            alerts.append(
                f"[P0 LIQUIDATION SURGE] ${total_liq / 1e6:,.0f}M liquidated\n\n"
                f"Long liquidations: ${long_liq / 1e6:,.0f}M\n"
                f"Short liquidations: ${short_liq / 1e6:,.0f}M\n\n"
                f"Massive liquidation event in progress. "
                f"Expect increased volatility and potential cascading liquidations."
            )
            logger.critical(
                "liquidation_surge_detected",
                total_usd=total_liq,
                long_usd=long_liq,
                short_usd=short_liq,
            )

    except Exception as e:
        # This is expected to fail without API key — not an error
        logger.debug("liquidation_check_skipped", error=str(e))

    return alerts
