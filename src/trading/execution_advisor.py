"""Signal-to-execution mapping and multi-signal position parameter calculator."""

from __future__ import annotations

import math
from typing import Any

from src.signals.base import TradeSignal


# ---------------------------------------------------------------------------
# Signal execution rules
# ---------------------------------------------------------------------------

SIGNAL_EXECUTION_RULES: dict[str, dict[str, Any]] = {
    "funding_reversion": {
        "timeframe": "1h",
        "leverage_range": (5, 10),
        "position_size_pct": 8.0,
        "stop_loss_pct": 1.5,
        "take_profit_pct": 2.0,
        "entry_type": "market",
        "max_hold_hours": 8,
        "requires_confirmation": False,
    },
    "liquidity_inflection": {
        "timeframe": "1d",
        "leverage_range": (3, 5),
        "position_size_pct": 15.0,
        "stop_loss_pct": 3.0,
        "take_profit_pct": 8.0,
        "entry_type": "limit",
        "max_hold_hours": 72,
        "requires_confirmation": True,
    },
    "max_pain_gravity": {
        "timeframe": "1h",
        "leverage_range": (5, 8),
        "position_size_pct": 6.0,
        "stop_loss_pct": 1.8,
        "take_profit_pct": None,  # target is max_pain price
        "entry_type": "limit",
        "max_hold_hours": 24,
        "requires_confirmation": False,
    },
    "liquidation_cascade": {
        "timeframe": "15m",
        "leverage_range": (8, 15),
        "position_size_pct": 10.0,
        "stop_loss_pct": 1.2,
        "take_profit_pct": 3.0,
        "entry_type": "market",  # must be market
        "max_hold_hours": 4,
        "requires_confirmation": False,
    },
    "smart_money_social": {
        "timeframe": "4h",
        "leverage_range": (8, 15),
        "position_size_pct": 20.0,
        "stop_loss_pct": 2.5,
        "take_profit_pct": 10.0,
        "entry_type": "limit",
        "max_hold_hours": 168,
        "requires_confirmation": False,
    },
    "exchange_reserve": {
        "timeframe": "4h",
        "leverage_range": (5, 8),
        "position_size_pct": 10.0,
        "stop_loss_pct": 2.0,
        "take_profit_pct": 6.0,
        "entry_type": "limit",
        "max_hold_hours": 72,
        "requires_confirmation": True,
    },
    "volatility_regime": {
        "timeframe": "4h",
        "leverage_range": (5, 10),
        "position_size_pct": 8.0,
        "stop_loss_pct": 2.0,
        "take_profit_pct": 8.0,
        "entry_type": "limit",
        "max_hold_hours": 48,
        "requires_confirmation": True,
    },
}


# ---------------------------------------------------------------------------
# Position sizing helper
# ---------------------------------------------------------------------------

def calc_position_size(
    signal: TradeSignal,
    capital: float,
    rules: dict[str, Any],
) -> float:
    """Calculate position size in USD based on signal confidence and rules.

    Higher confidence => closer to the rule's full position_size_pct.
    Confidence 50 => half of the base allocation; 100 => full.

    Args:
        signal: The trade signal with a confidence score 0-100.
        capital: Total available capital in USD.
        rules: The rule dict from SIGNAL_EXECUTION_RULES.

    Returns:
        Position size in USD.
    """
    base_pct = rules["position_size_pct"]  # e.g. 8.0
    # Scale linearly: confidence 0 -> 0.2x, confidence 100 -> 1.0x
    confidence_scalar = 0.2 + 0.8 * (signal.confidence / 100.0)
    size_usd = capital * (base_pct / 100.0) * confidence_scalar
    return round(size_usd, 2)


# ---------------------------------------------------------------------------
# Multi-signal execution parameter builder
# ---------------------------------------------------------------------------

def get_execution_params(
    consensus: dict[str, Any],
    triggered_signals: list[TradeSignal],
    capital: float,
) -> dict[str, Any]:
    """Merge multiple triggered signals into a single set of execution params.

    Rules:
    - Each additional aligned signal multiplies position by 1.2 (cap 2.5x).
    - Stop-loss: take the tightest (smallest %).
    - Take-profit: take the widest (largest %).
    - Leverage: weighted average biased toward the highest-confidence signal.
    - Entry type: if any signal requires market, use market.

    Args:
        consensus: The consensus dict (must include ``direction``).
        triggered_signals: List of TradeSignal objects that fired.
        capital: Available capital in USD.

    Returns:
        Dict with keys: direction, position_size_usd, leverage, stop_loss_pct,
        take_profit_pct, entry_type, max_hold_hours, signals_used,
        requires_confirmation, timeframe.
    """
    direction = consensus.get("direction", "LONG")

    if not triggered_signals:
        return {
            "direction": direction,
            "position_size_usd": 0.0,
            "leverage": 1,
            "stop_loss_pct": 2.0,
            "take_profit_pct": 4.0,
            "entry_type": "limit",
            "max_hold_hours": 24,
            "signals_used": [],
            "requires_confirmation": True,
            "timeframe": "4h",
        }

    # Collect per-signal params -------------------------------------------------
    sizes: list[float] = []
    stop_losses: list[float] = []
    take_profits: list[float] = []
    leverages: list[tuple[float, int]] = []  # (confidence, mid_leverage)
    entry_types: list[str] = []
    hold_hours: list[int] = []
    confirmations: list[bool] = []
    timeframes: list[str] = []

    for sig in triggered_signals:
        rules = SIGNAL_EXECUTION_RULES.get(sig.signal_type)
        if rules is None:
            continue

        sizes.append(calc_position_size(sig, capital, rules))
        stop_losses.append(rules["stop_loss_pct"])

        tp = rules["take_profit_pct"]
        if tp is not None:
            take_profits.append(tp)

        lev_lo, lev_hi = rules["leverage_range"]
        mid_lev = (lev_lo + lev_hi) / 2.0
        leverages.append((sig.confidence, mid_lev))

        entry_types.append(rules["entry_type"])
        hold_hours.append(rules["max_hold_hours"])
        confirmations.append(rules["requires_confirmation"])
        timeframes.append(rules["timeframe"])

    if not sizes:
        return {
            "direction": direction,
            "position_size_usd": 0.0,
            "leverage": 1,
            "stop_loss_pct": 2.0,
            "take_profit_pct": 4.0,
            "entry_type": "limit",
            "max_hold_hours": 24,
            "signals_used": [s.signal_type for s in triggered_signals],
            "requires_confirmation": True,
            "timeframe": "4h",
        }

    # Position size: base = max single signal, then ×1.2 per extra signal ------
    base_size = max(sizes)
    n_extra = len(sizes) - 1
    multiplier = min(1.2 ** n_extra, 2.5)
    position_size_usd = round(base_size * multiplier, 2)

    # Stop-loss: tightest (smallest %) -----------------------------------------
    stop_loss_pct = min(stop_losses)

    # Take-profit: widest (largest %) ------------------------------------------
    take_profit_pct = max(take_profits) if take_profits else 4.0

    # Leverage: confidence-weighted average ------------------------------------
    total_conf = sum(c for c, _ in leverages)
    if total_conf > 0:
        leverage = sum(c * lev for c, lev in leverages) / total_conf
    else:
        leverage = leverages[0][1] if leverages else 5.0
    leverage = max(1, math.floor(leverage))

    # Entry type: any market => market -----------------------------------------
    entry_type = "market" if "market" in entry_types else "limit"

    # Max hold hours: use the shortest -----------------------------------------
    max_hold = min(hold_hours)

    # Confirmation: only required if ALL signals require it --------------------
    requires_confirmation = all(confirmations)

    # Timeframe: pick the shortest ---------------------------------------------
    tf_order = {"15m": 0, "1h": 1, "4h": 2, "1d": 3}
    timeframe = min(timeframes, key=lambda t: tf_order.get(t, 99))

    return {
        "direction": direction,
        "position_size_usd": position_size_usd,
        "leverage": leverage,
        "stop_loss_pct": stop_loss_pct,
        "take_profit_pct": take_profit_pct,
        "entry_type": entry_type,
        "max_hold_hours": max_hold,
        "signals_used": [s.signal_type for s in triggered_signals],
        "requires_confirmation": requires_confirmation,
        "timeframe": timeframe,
    }
