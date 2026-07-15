"""Fail-closed boundary between research signals and real execution.

The repository previously exposed static 5-15x leverage and position-size recipes
for unvalidated signals. Nothing called them today, but leaving a callable mapping
made a future accidental wire-up indistinguishable from an approved strategy.

CryptoScope is an event and evidence system. Until a separately reviewed execution
adapter proves a lane with real fills and explicit user approval, this module can
only return a zero-notional research plan.
"""
from __future__ import annotations

from typing import Any

from src.signals.base import TradeSignal

AUTOMATIC_EXECUTION_ENABLED = False
SIGNAL_EXECUTION_RULES: dict[str, dict[str, Any]] = {}


def calc_position_size(signal: TradeSignal, capital: float,
                       rules: dict[str, Any]) -> float:
    """Reject direct sizing for signals that have no approved execution policy."""
    del signal, capital, rules
    raise RuntimeError("unverified signal sizing is disabled")


def get_execution_params(
    consensus: dict[str, Any],
    triggered_signals: list[TradeSignal],
    capital: float,
) -> dict[str, Any]:
    """Return an explicit non-executable research plan, never order parameters."""
    del capital
    return {
        "direction": consensus.get("direction"),
        "position_size_usd": 0.0,
        "leverage": 1,
        "stop_loss_pct": None,
        "take_profit_pct": None,
        "entry_type": None,
        "max_hold_hours": None,
        "signals_used": [signal.signal_type for signal in triggered_signals],
        "requires_confirmation": True,
        "execution_allowed": False,
        "execution_mode": "research_only",
        "block_reason": "no lane has an approved real-fill execution policy",
    }
