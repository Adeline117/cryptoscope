"""Exchange mutation cannot happen without explicit live mode and manual approval."""
from __future__ import annotations

import asyncio

import pytest


class _FakeExchange:
    def __init__(self):
        self.orders = []
        self.leverages = []

    def set_leverage(self, leverage, symbol):
        self.leverages.append((leverage, symbol))

    def create_order(self, *args, **kwargs):
        self.orders.append((args, kwargs))
        return {"id": "order-1", "price": 100, "status": "closed"}


def test_default_research_mode_blocks_before_exchange_client(monkeypatch):
    from src.contract import exchange

    touched = []
    monkeypatch.delenv("CRYPTOSCOPE_EXECUTION_MODE", raising=False)
    monkeypatch.delenv("CRYPTOSCOPE_LIVE_APPROVAL_TOKEN", raising=False)
    monkeypatch.setattr(exchange, "_get_exchange", lambda name: touched.append(name))

    result = exchange.place_order("okx", "BTC/USDT", "buy", 1)
    assert "blocked" in result["error"]
    assert touched == []


def test_live_mode_still_blocks_without_matching_manual_token(monkeypatch):
    from src.contract import exchange

    fake = _FakeExchange()
    monkeypatch.setenv("CRYPTOSCOPE_EXECUTION_MODE", "live")
    monkeypatch.setenv("CRYPTOSCOPE_LIVE_APPROVAL_TOKEN", "manual-secret")
    monkeypatch.setattr(exchange, "_get_exchange", lambda name: fake)

    result = exchange.place_order(
        "okx", "BTC/USDT", "buy", 1, approval_token="wrong")
    assert "manual approval" in result["error"]
    assert fake.orders == [] and fake.leverages == []


def test_matching_manual_token_allows_only_the_approved_call_path(monkeypatch):
    from src.contract import exchange

    fake = _FakeExchange()
    monkeypatch.setenv("CRYPTOSCOPE_EXECUTION_MODE", "live")
    monkeypatch.setenv("CRYPTOSCOPE_LIVE_APPROVAL_TOKEN", "manual-secret")
    monkeypatch.setattr(exchange, "_get_exchange", lambda name: fake)

    result = exchange.place_order(
        "okx", "BTC/USDT", "buy", 1, leverage=1,
        approval_token="manual-secret")
    assert result["order_id"] == "order-1"
    assert len(fake.orders) == 1


class _Session:
    def __init__(self, *, fail=False):
        self.fail = fail
        self.close_calls = 0

    def close(self):
        self.close_calls += 1
        if self.fail:
            raise RuntimeError("close failed")


class _Client:
    def __init__(self, *, fail=False):
        self.session = _Session(fail=fail)


def test_cached_exchange_sessions_close_idempotently_and_fail_independently():
    from src.contract import exchange

    first, broken, last = _Client(), _Client(fail=True), _Client()
    exchange._exchange_instances.clear()
    exchange._exchange_instances.update({
        "binance": first, "bybit": broken, "okx": last,
    })

    assert exchange.close_exchange_clients() == 2
    assert exchange.close_exchange_clients() == 0
    assert exchange._exchange_instances == {}
    assert first.session.close_calls == broken.session.close_calls == 1
    assert last.session.close_calls == 1


def test_okx_requires_passphrase_before_client_creation(monkeypatch):
    from src.contract import exchange

    exchange._exchange_instances.clear()
    monkeypatch.setenv("OKX_API_KEY", "key")
    monkeypatch.setenv("OKX_API_SECRET", "secret")
    monkeypatch.delenv("OKX_PASSPHRASE", raising=False)

    with pytest.raises(ValueError, match="OKX_PASSPHRASE"):
        exchange._get_exchange("okx")
    assert exchange._exchange_instances == {}


def test_bot_shutdown_always_closes_cached_exchange_clients(monkeypatch):
    from src.contract import exchange
    from src.distribution import telegram_bot

    calls = []
    monkeypatch.setattr(exchange, "close_exchange_clients", lambda: calls.append(True))

    asyncio.run(telegram_bot.stop_bot_polling(None))

    assert calls == [True]
