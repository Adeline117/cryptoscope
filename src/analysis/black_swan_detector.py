"""Multi-signal convergence black swan detection.

Scans across all collected data to detect rare, high-impact events
using both single-trigger and compound-trigger signals.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timezone

import structlog

from src.collectors.base import CollectedItem

logger = structlog.get_logger()


@dataclass
class BlackSwanAlert:
    """A detected black swan signal."""

    signal_type: str
    severity: str  # "critical", "high", "warning"
    title: str
    description: str
    triggered_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    contributing_signals: list[str] = field(default_factory=list)
    evidence: dict = field(default_factory=dict)


# --- Single-trigger thresholds (immediate critical) ---

STABLECOIN_DEPEG_THRESHOLD = 0.01  # 1% deviation from $1
EXCHANGE_RESERVE_COLLAPSE_THRESHOLD = 15.0  # 15% drop in 24h
GOVERNANCE_EMERGENCY_KEYWORDS = ["emergency", "pause", "exploit", "hack"]
GOVERNANCE_EMERGENCY_MIN_AMOUNT = 10_000_000  # $10M

# --- Compound-trigger thresholds (2+ signals = critical) ---

MARKET_PANIC_FG_THRESHOLD = 15  # Fear & Greed below 15
MARKET_PANIC_BTC_DROP_THRESHOLD = 10.0  # BTC 24h drop > 10%
MARKET_PANIC_VOLUME_MULTIPLIER = 3.0  # Volume > 3x average

LIQUIDITY_CRISIS_NL_DROP_THRESHOLD = 5.0  # Net Liquidity weekly drop > 5%
LIQUIDITY_CRISIS_VIX_THRESHOLD = 30  # VIX > 30
LIQUIDITY_CRISIS_BTC_DROP_THRESHOLD = 8.0  # BTC drop > 8%


async def scan_black_swan(
    all_items: list[CollectedItem],
    fear_greed_value: int | None = None,
    net_liquidity_change: float | None = None,
) -> list[BlackSwanAlert]:
    """Scan all collected items for black swan events.

    Args:
        all_items: All collected items from every source.
        fear_greed_value: Current Fear & Greed index value (0-100).
        net_liquidity_change: Weekly net liquidity change percentage.

    Returns:
        List of BlackSwanAlert objects, sorted by severity.
    """
    alerts: list[BlackSwanAlert] = []

    # Run all single-trigger checks
    alerts.extend(_check_stablecoin_depeg(all_items))
    alerts.extend(_check_exchange_reserve_collapse(all_items))
    alerts.extend(_check_governance_emergency(all_items))

    # Run compound-trigger checks
    alerts.extend(
        _check_market_panic(all_items, fear_greed_value)
    )
    alerts.extend(
        _check_liquidity_crisis(all_items, net_liquidity_change)
    )

    # Sort by severity
    severity_order = {"critical": 0, "high": 1, "warning": 2}
    alerts.sort(key=lambda a: severity_order.get(a.severity, 3))

    if alerts:
        logger.warning(
            "black_swan_signals_detected",
            count=len(alerts),
            critical=sum(1 for a in alerts if a.severity == "critical"),
        )

    return alerts


# --- Single-trigger detectors ---


def _check_stablecoin_depeg(items: list[CollectedItem]) -> list[BlackSwanAlert]:
    """Check if USDT/USDC price deviates > 1% from $1."""
    alerts: list[BlackSwanAlert] = []
    target_stables = {"usdt", "usdc"}

    for item in items:
        meta = item.metadata
        if meta.get("data_type") != "market_price":
            continue

        symbol = (meta.get("symbol") or "").lower()
        if symbol not in target_stables:
            continue

        price = meta.get("price")
        if price is None:
            continue

        try:
            price = float(price)
        except (ValueError, TypeError):
            continue

        deviation = abs(price - 1.0)
        if deviation > STABLECOIN_DEPEG_THRESHOLD:
            direction = "above" if price > 1.0 else "below"
            alerts.append(
                BlackSwanAlert(
                    signal_type="stablecoin_depeg",
                    severity="critical",
                    title=f"STABLECOIN DEPEG: {symbol.upper()} at ${price:.4f}",
                    description=(
                        f"{symbol.upper()} has depegged to ${price:.4f} "
                        f"({deviation * 100:.2f}% {direction} $1.00). "
                        f"This may indicate systemic risk."
                    ),
                    contributing_signals=[f"{symbol.upper()} price = ${price:.4f}"],
                    evidence={
                        "symbol": symbol.upper(),
                        "price": price,
                        "deviation_pct": deviation * 100,
                        "direction": direction,
                    },
                )
            )

    return alerts


def _check_exchange_reserve_collapse(
    items: list[CollectedItem],
) -> list[BlackSwanAlert]:
    """Check if any single exchange reserves drop > 15% in 24h."""
    alerts: list[BlackSwanAlert] = []

    for item in items:
        meta = item.metadata
        if meta.get("data_type") != "exchange_reserve":
            continue

        change_24h = meta.get("change_24h_pct")
        if change_24h is None:
            continue

        try:
            change_val = float(change_24h)
        except (ValueError, TypeError):
            continue

        if change_val <= -EXCHANGE_RESERVE_COLLAPSE_THRESHOLD:
            exchange = meta.get("exchange", "Unknown")
            reserves = meta.get("reserves_usd", 0)
            alerts.append(
                BlackSwanAlert(
                    signal_type="exchange_reserve_collapse",
                    severity="critical",
                    title=(
                        f"EXCHANGE RESERVE COLLAPSE: {exchange.upper()} "
                        f"down {abs(change_val):.1f}% in 24h"
                    ),
                    description=(
                        f"{exchange.upper()} reserves dropped {abs(change_val):.1f}% "
                        f"in 24 hours (${reserves:,.0f} remaining). "
                        f"Potential bank run or insolvency risk."
                    ),
                    contributing_signals=[
                        f"{exchange} 24h change: {change_val:+.1f}%"
                    ],
                    evidence={
                        "exchange": exchange,
                        "change_24h_pct": change_val,
                        "reserves_usd": reserves,
                    },
                )
            )

    return alerts


def _check_governance_emergency(
    items: list[CollectedItem],
) -> list[BlackSwanAlert]:
    """Check for governance emergency actions involving large dollar amounts."""
    alerts: list[BlackSwanAlert] = []

    for item in items:
        meta = item.metadata
        text = f"{item.title} {item.content}".lower()

        # Check for emergency keywords
        keyword_matches = [
            kw for kw in GOVERNANCE_EMERGENCY_KEYWORDS if kw in text
        ]
        if not keyword_matches:
            continue

        # Try to extract dollar amounts from the text
        amount = _extract_dollar_amount(text)
        if amount is None:
            # Also check metadata for amounts
            for key in ("amount", "tvl", "value", "total_value"):
                val = meta.get(key)
                if val is not None:
                    try:
                        amount = float(val)
                        break
                    except (ValueError, TypeError):
                        continue

        if amount is not None and amount >= GOVERNANCE_EMERGENCY_MIN_AMOUNT:
            alerts.append(
                BlackSwanAlert(
                    signal_type="governance_emergency",
                    severity="critical",
                    title=f"GOVERNANCE EMERGENCY: {item.title[:80]}",
                    description=(
                        f"Emergency governance action detected involving "
                        f"${amount:,.0f}. Keywords: {', '.join(keyword_matches)}. "
                        f"Source: {item.title}"
                    ),
                    contributing_signals=[
                        f"Keywords: {', '.join(keyword_matches)}",
                        f"Amount: ${amount:,.0f}",
                    ],
                    evidence={
                        "keywords": keyword_matches,
                        "amount": amount,
                        "title": item.title,
                        "url": item.url,
                    },
                )
            )

    return alerts


# --- Compound-trigger detectors ---


def _check_market_panic(
    items: list[CollectedItem],
    fear_greed_value: int | None,
) -> list[BlackSwanAlert]:
    """Check for market panic: F&G < 15 AND BTC 24h drop > 10% AND volume > 3x average.

    Requires 2+ of these signals to trigger a critical alert.
    """
    signals: list[str] = []

    # Signal 1: Fear & Greed below threshold
    fg_triggered = False
    if fear_greed_value is not None and fear_greed_value < MARKET_PANIC_FG_THRESHOLD:
        fg_triggered = True
        signals.append(
            f"Fear & Greed Index at {fear_greed_value} (threshold: {MARKET_PANIC_FG_THRESHOLD})"
        )

    # Signal 2: BTC 24h drop > 10%
    btc_drop_triggered = False
    btc_change = None
    for item in items:
        meta = item.metadata
        if meta.get("data_type") != "market_price":
            continue
        if (meta.get("symbol") or "").lower() != "btc":
            continue
        btc_change = meta.get("price_change_24h")
        if btc_change is not None:
            try:
                btc_change = float(btc_change)
            except (ValueError, TypeError):
                btc_change = None
                continue
            if btc_change <= -MARKET_PANIC_BTC_DROP_THRESHOLD:
                btc_drop_triggered = True
                signals.append(
                    f"BTC 24h change: {btc_change:+.1f}% "
                    f"(threshold: {-MARKET_PANIC_BTC_DROP_THRESHOLD}%)"
                )
        break

    # Signal 3: Volume > 3x average (look for volume spike indicators)
    volume_spike_triggered = False
    for item in items:
        meta = item.metadata
        multiplier = meta.get("volume_multiplier") or meta.get("volume_spike")
        if multiplier is not None:
            try:
                multiplier = float(multiplier)
            except (ValueError, TypeError):
                continue
            if multiplier >= MARKET_PANIC_VOLUME_MULTIPLIER:
                volume_spike_triggered = True
                signals.append(
                    f"Volume spike: {multiplier:.1f}x average "
                    f"(threshold: {MARKET_PANIC_VOLUME_MULTIPLIER}x)"
                )
                break

    # Compound trigger: need 2+ signals
    triggered_count = sum([fg_triggered, btc_drop_triggered, volume_spike_triggered])
    if triggered_count >= 2:
        severity = "critical" if triggered_count >= 3 else "high"
        return [
            BlackSwanAlert(
                signal_type="market_panic",
                severity=severity,
                title=f"MARKET PANIC: {triggered_count}/3 signals triggered",
                description=(
                    f"Market panic detected with {triggered_count} converging signals. "
                    + " | ".join(signals)
                ),
                contributing_signals=signals,
                evidence={
                    "fear_greed_value": fear_greed_value,
                    "btc_change_24h": btc_change,
                    "signals_triggered": triggered_count,
                },
            )
        ]

    return []


def _check_liquidity_crisis(
    items: list[CollectedItem],
    net_liquidity_change: float | None,
) -> list[BlackSwanAlert]:
    """Check for liquidity crisis: Net Liquidity weekly drop > 5% AND VIX > 30 AND BTC drop > 8%.

    Requires 2+ of these signals to trigger.
    """
    signals: list[str] = []

    # Signal 1: Net Liquidity weekly drop > 5%
    nl_triggered = False
    if net_liquidity_change is not None and net_liquidity_change <= -LIQUIDITY_CRISIS_NL_DROP_THRESHOLD:
        nl_triggered = True
        signals.append(
            f"Net Liquidity weekly change: {net_liquidity_change:+.1f}% "
            f"(threshold: {-LIQUIDITY_CRISIS_NL_DROP_THRESHOLD}%)"
        )

    # Signal 2: VIX > 30 (look for macro data)
    vix_triggered = False
    for item in items:
        meta = item.metadata
        vix = meta.get("vix") or meta.get("vix_close")
        if vix is not None:
            try:
                vix = float(vix)
            except (ValueError, TypeError):
                continue
            if vix > LIQUIDITY_CRISIS_VIX_THRESHOLD:
                vix_triggered = True
                signals.append(
                    f"VIX at {vix:.1f} (threshold: {LIQUIDITY_CRISIS_VIX_THRESHOLD})"
                )
            break

    # Signal 3: BTC drop > 8%
    btc_drop_triggered = False
    for item in items:
        meta = item.metadata
        if meta.get("data_type") != "market_price":
            continue
        if (meta.get("symbol") or "").lower() != "btc":
            continue
        btc_change = meta.get("price_change_24h")
        if btc_change is not None:
            try:
                btc_change = float(btc_change)
            except (ValueError, TypeError):
                continue
            if btc_change <= -LIQUIDITY_CRISIS_BTC_DROP_THRESHOLD:
                btc_drop_triggered = True
                signals.append(
                    f"BTC 24h change: {btc_change:+.1f}% "
                    f"(threshold: {-LIQUIDITY_CRISIS_BTC_DROP_THRESHOLD}%)"
                )
        break

    # Compound trigger: need 2+ signals
    triggered_count = sum([nl_triggered, vix_triggered, btc_drop_triggered])
    if triggered_count >= 2:
        severity = "critical" if triggered_count >= 3 else "high"
        return [
            BlackSwanAlert(
                signal_type="liquidity_crisis",
                severity=severity,
                title=f"LIQUIDITY CRISIS: {triggered_count}/3 signals triggered",
                description=(
                    f"Liquidity crisis detected with {triggered_count} converging "
                    f"signals. " + " | ".join(signals)
                ),
                contributing_signals=signals,
                evidence={
                    "net_liquidity_change": net_liquidity_change,
                    "signals_triggered": triggered_count,
                },
            )
        ]

    return []


# --- Helpers ---


def _extract_dollar_amount(text: str) -> float | None:
    """Extract the largest dollar amount mentioned in text.

    Supports formats like: $10M, $1.5B, $500,000, $10 million, etc.
    """
    patterns = [
        # $1.5B or $10M
        r"\$(\d+(?:\.\d+)?)\s*[bB]",
        r"\$(\d+(?:\.\d+)?)\s*[mM]",
        # $10 billion, $500 million
        r"\$(\d+(?:\.\d+)?)\s*billion",
        r"\$(\d+(?:\.\d+)?)\s*million",
        # $1,000,000
        r"\$([\d,]+(?:\.\d+)?)",
    ]
    multipliers = [1e9, 1e6, 1e9, 1e6, 1]

    max_amount = None
    for pattern, mult in zip(patterns, multipliers):
        for match in re.finditer(pattern, text):
            try:
                raw = match.group(1).replace(",", "")
                amount = float(raw) * mult
                if max_amount is None or amount > max_amount:
                    max_amount = amount
            except (ValueError, IndexError):
                continue

    return max_amount
