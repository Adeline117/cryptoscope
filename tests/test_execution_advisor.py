"""Unverified signals can never silently become leveraged order parameters."""
from types import SimpleNamespace

import pytest


def test_all_legacy_leverage_rules_are_removed():
    from src.trading import execution_advisor as advisor

    assert advisor.AUTOMATIC_EXECUTION_ENABLED is False
    assert advisor.SIGNAL_EXECUTION_RULES == {}


def test_execution_plan_is_zero_notional_research_only():
    from src.trading.execution_advisor import get_execution_params

    signal = SimpleNamespace(signal_type="liquidation_cascade", confidence=100)
    plan = get_execution_params({"direction": "SHORT"}, [signal], capital=1_000_000)
    assert plan["execution_allowed"] is False
    assert plan["execution_mode"] == "research_only"
    assert plan["position_size_usd"] == 0
    assert plan["leverage"] == 1
    assert plan["entry_type"] is None


def test_direct_position_sizing_fails_closed():
    from src.trading.execution_advisor import calc_position_size

    signal = SimpleNamespace(signal_type="funding_reversion", confidence=100)
    with pytest.raises(RuntimeError, match="disabled"):
        calc_position_size(signal, capital=100_000,
                           rules={"position_size_pct": 100})
