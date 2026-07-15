"""Offline contract for the CCXT adapters used by private exchange actions."""
from __future__ import annotations

import ccxt
import pytest
import requests

from src.contract.exchange import EXCHANGES


CCXT_VERSION = "4.5.58"
PRIVATE_CAPABILITIES = (
    "fetchBalance",
    "fetchPositions",
    "fetchTicker",
    "setLeverage",
    "createOrder",
)


def test_ccxt_version_is_frozen() -> None:
    assert ccxt.__version__ == CCXT_VERSION


@pytest.mark.parametrize("exchange_name", tuple(EXCHANGES))
def test_private_swap_capabilities_are_available_offline(
    exchange_name: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    def reject_network(*_args, **_kwargs):
        raise AssertionError("CCXT capability contract must not access the network")

    monkeypatch.setattr(requests.Session, "request", reject_network)
    exchange_id = EXCHANGES[exchange_name]["class"]
    exchange = getattr(ccxt, exchange_id)(
        {
            "enableRateLimit": False,
            "options": {"defaultType": "swap"},
        }
    )
    try:
        assert exchange.options["defaultType"] == "swap"
        assert {name: exchange.has.get(name) for name in PRIVATE_CAPABILITIES} == {
            name: True for name in PRIVATE_CAPABILITIES
        }
    finally:
        exchange.session.close()
