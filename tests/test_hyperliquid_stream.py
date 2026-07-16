"""Hyperliquid's public feed is parsed and persisted without private-user claims."""
from __future__ import annotations

import json

import pytest


@pytest.fixture
def hl(tmp_path, monkeypatch):
    from src.pipeline import hyperliquid_stream, stream_health

    monkeypatch.setattr(hyperliquid_stream, "DB", tmp_path / "hl.db")
    monkeypatch.setattr(stream_health, "DB", tmp_path / "health.db")
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


def test_store_batches_commits_and_flushes_on_close(hl):
    from src.ops.stream_disk_guard import DiskStateGuard

    ticks = iter((0.0, 0.1, 0.2, 0.3))
    store = hl.HyperliquidStore(hl.DB, batch_size=2, flush_seconds=99,
                                monotonic=lambda: next(ticks),
                                disk_guard=DiskStateGuard(
                                    probe=lambda: {"state": "ok"},
                                ))
    message = {"channel": "trades", "data": [{"coin": "BTC", "time": 1002,
               "tid": 7, "side": "A", "px": "100", "sz": "0.5", "hash": "0x1"}]}
    store.persist(message)
    assert store.pending == 1
    store.persist({"channel": "trades", "data": [{**message["data"][0], "tid": 8}]})
    assert store.pending == 0
    store.close()
    c = hl._conn()
    try:
        assert c.execute("SELECT COUNT(*) FROM trades").fetchone()[0] == 2
    finally:
        c.close()


@pytest.mark.parametrize("state", ["warn", "critical"])
def test_disk_pressure_sheds_only_raw_trades_and_reports_degraded(hl, state):
    from src.ops.stream_disk_guard import DiskStateGuard
    from src.pipeline import stream_health

    store = hl.HyperliquidStore(
        hl.DB, batch_size=1,
        disk_guard=DiskStateGuard(probe=lambda: {"state": state}),
    )
    store.persist({"channel": "bbo", "data": {
        "coin": "BTC", "time": 1000,
        "bbo": [{"px": "99", "sz": "2"}, {"px": "101", "sz": "3"}],
    }})
    store.persist({"channel": "l2Book", "data": {
        "coin": "BTC", "time": 1001, "levels": [[], []],
    }})
    store.persist({"channel": "activeAssetCtx", "data": {
        "coin": "BTC", "ctx": {"markPx": "100", "funding": "0.0001"},
    }})
    store.persist({"channel": "trades", "data": [{
        "coin": "BTC", "time": 1002, "tid": 7, "side": "A",
        "px": "100", "sz": "0.5", "hash": "0x1",
    }]})
    store.close()

    connection = hl._conn()
    try:
        assert connection.execute("SELECT COUNT(*) FROM bbo").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM books").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM asset_ctx").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM trades").fetchone()[0] == 0
    finally:
        connection.close()
    worker = next(
        row for row in stream_health.snapshot()
        if row["source"] == "hyperliquid" and row["stream"] == "raw_trade_retention"
    )
    assert worker["status"] == "degraded"
    assert worker["details"]["disk_state"] == state
    assert worker["details"]["raw_trades_policy"] == "raw_trades_shed"
    assert worker["details"]["raw_trades_retained"] is False


def test_unknown_disk_probe_retains_trades_but_reports_fail_open(hl):
    from src.ops.stream_disk_guard import DiskStateGuard
    from src.pipeline import stream_health

    guard = DiskStateGuard(
        probe=lambda: (_ for _ in ()).throw(OSError("volume unavailable")),
    )
    store = hl.HyperliquidStore(hl.DB, batch_size=1, disk_guard=guard)
    store.persist({"channel": "trades", "data": [{
        "coin": "BTC", "time": 1002, "tid": 7, "side": "A",
        "px": "100", "sz": "0.5", "hash": "0x1",
    }]})
    store.close()

    connection = hl._conn()
    try:
        assert connection.execute("SELECT COUNT(*) FROM trades").fetchone()[0] == 1
    finally:
        connection.close()
    worker = next(row for row in stream_health.snapshot()
                  if row["stream"] == "raw_trade_retention")
    assert worker["status"] == "degraded"
    assert worker["details"]["disk_state"] == "unknown"
    assert worker["details"]["raw_trades_policy"] == "disk_probe_unknown_fail_open"
    assert worker["details"]["raw_trades_retained"] is True
    assert worker["details"]["measurement_failed"] is True
    assert worker["details"]["error_kind"] == "OSError"
    assert "volume unavailable" not in repr(worker)


def test_disk_policy_health_is_bounded_but_state_changes_report_immediately(hl):
    now = [0.0]
    state = ["critical"]
    reports = []

    class Guard:
        def retain_optional_raw(self):
            current = state[0]
            return current not in {"warn", "critical"}, {"state": current}

    store = hl.HyperliquidStore(
        hl.DB, monotonic=lambda: now[0], disk_guard=Guard(),
        health_reporter=lambda *args, **kwargs: reports.append((args, kwargs)),
    )
    trade = {"channel": "trades", "data": [{
        "coin": "BTC", "time": 1002, "tid": 7, "side": "A",
        "px": "100", "sz": "0.5", "hash": "0x1",
    }]}
    for second in range(1, 60):
        now[0] = float(second)
        store.persist(trade)
    assert len(reports) == 1

    now[0] = 61.0
    store.persist(trade)
    assert len(reports) == 2

    state[0] = "warn"
    now[0] = 62.0
    store.persist(trade)
    assert len(reports) == 3
    assert reports[-1][1]["details"]["disk_state"] == "warn"
    store.close()
