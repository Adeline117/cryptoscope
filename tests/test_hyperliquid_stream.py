"""Hyperliquid's public feed is parsed and persisted without private-user claims."""
from __future__ import annotations

import json

import pytest


@pytest.fixture
def hl(tmp_path, monkeypatch):
    from src.pipeline import hyperliquid_stream

    monkeypatch.setattr(hyperliquid_stream, "DB", tmp_path / "hl.db")
    return hyperliquid_stream


def test_subscriptions_match_official_public_channel_shapes(hl):
    messages = hl.subscription_messages(("BTC",))
    assert messages == [
        {"method": "subscribe", "subscription": {"type": channel, "coin": "BTC"}}
        for channel in hl.CHANNELS
    ]
    assert all("user" not in message["subscription"] for message in messages)


def test_parse_ignores_ack_and_preserves_trade_identity(hl):
    assert hl.parse_message(json.dumps({"channel": "subscriptionResponse", "data": {}})) is None
    event = hl.parse_message(json.dumps({"channel": "trades", "data": [{
        "coin": "BTC", "side": "B", "px": "60000", "sz": "0.1",
        "hash": "0xabc", "time": 1780000000000, "tid": 42,
        "users": ["0xbuyer", "0xseller"],
    }]}))
    assert event.cursor == 1780000000000
    assert event.payload["data"][0]["tid"] == 42


def test_persists_bbo_book_context_and_deduped_trades(hl):
    hl.persist({"channel": "bbo", "data": {"coin": "BTC", "time": 1000,
                "bbo": [{"px": "99", "sz": "2", "n": 1},
                        {"px": "101", "sz": "3", "n": 1}]}})
    hl.persist({"channel": "l2Book", "data": {"coin": "BTC", "time": 1001,
                "levels": [[{"px": "99", "sz": "2", "n": 1}],
                           [{"px": "101", "sz": "3", "n": 1}]]}})
    hl.persist({"channel": "activeAssetCtx", "data": {"coin": "BTC", "ctx": {
        "markPx": "100", "midPx": "100", "oraclePx": "100.5", "funding": "0.0001",
        "openInterest": "1234", "dayNtlVlm": "99999"}}})
    trade = {"channel": "trades", "data": [{"coin": "BTC", "time": 1002,
             "tid": 7, "side": "A", "px": "100", "sz": "0.5", "hash": "0x1"}]}
    hl.persist(trade)
    hl.persist(trade)

    c = hl._conn()
    try:
        assert c.execute("SELECT bid_px,ask_px FROM bbo").fetchone() == (99.0, 101.0)
        assert c.execute("SELECT mark_px,funding FROM asset_ctx").fetchone() == (100.0, 0.0001)
        assert c.execute("SELECT COUNT(*) FROM trades").fetchone()[0] == 1
        levels = json.loads(c.execute("SELECT levels FROM books").fetchone()[0])
        assert levels[0][0]["px"] == "99"
    finally:
        c.close()


def test_hyperliquid_socket_uses_application_ping(hl):
    sent = []

    class Raw:
        def send(self, value): sent.append(value)
        def close(self): pass

    hl._HyperliquidSocket(Raw()).ping()
    assert json.loads(sent[0]) == {"method": "ping"}
