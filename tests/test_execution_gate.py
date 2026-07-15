"""Exchange mutation cannot happen without explicit live mode and manual approval."""
from __future__ import annotations


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
