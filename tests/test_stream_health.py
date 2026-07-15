"""Real-time health distinguishes quiet markets from gaps and dead connections."""
from datetime import datetime, timedelta, timezone

import pytest


@pytest.fixture
def health(tmp_path, monkeypatch):
    from src.pipeline import stream_health

    monkeypatch.setattr(stream_health, "DB", tmp_path / "streams.db")
    return stream_health


def test_contiguous_cursor_detects_gap_without_regressing(health):
    t0 = datetime(2026, 7, 14, 12, tzinfo=timezone.utc)
    health.observe("hyperliquid", "l2:BTC", cursor=10, event_at=t0, received_at=t0,
                   expect_contiguous=True)
    gap = health.observe("hyperliquid", "l2:BTC", cursor=13, event_at=t0,
                         received_at=t0 + timedelta(milliseconds=250),
                         expect_contiguous=True)
    assert gap == {"classification": "gap_detected", "cursor": 13,
                   "latency_ms": 250, "status": "degraded", "open_gaps": 1}
    old = health.observe("hyperliquid", "l2:BTC", cursor=12, event_at=t0,
                         received_at=t0 + timedelta(seconds=1), expect_contiguous=True)
    assert old["classification"] == "out_of_order" and old["cursor"] == 13


def test_gap_resolution_and_staleness_are_explicit(health):
    t0 = datetime(2026, 7, 14, 12, tzinfo=timezone.utc)
    health.observe("source", "stream", cursor=1, received_at=t0, expect_contiguous=True)
    health.observe("source", "stream", cursor=3, received_at=t0, expect_contiguous=True)
    c = health._conn()
    gap_id = c.execute("SELECT id FROM gaps").fetchone()[0]
    c.close()
    assert health.resolve_gap(gap_id, at=t0 + timedelta(seconds=5),
                              details={"backfilled": 1}) is True
    live = health.snapshot(now=t0 + timedelta(seconds=30), stale_after_seconds=60)[0]
    assert live["status"] == "live" and live["open_gaps"] == 0 and not live["stale"]
    stale = health.snapshot(now=t0 + timedelta(seconds=61), stale_after_seconds=60)[0]
    assert stale["status"] == "stale" and stale["stale"] is True


def test_disconnect_is_not_reported_as_a_quiet_live_market(health):
    t0 = datetime(2026, 7, 14, 12, tzinfo=timezone.utc)
    health.mark_disconnected("helius", "launches", "websocket closed", at=t0)
    row = health.snapshot(now=t0, stale_after_seconds=60)[0]
    assert row["status"] == "disconnected"
    assert row["last_error"] == "websocket closed"


def test_stream_clocks_require_timezone(health):
    with pytest.raises(ValueError, match="timezone"):
        health.observe("source", "stream", received_at="2026-07-14T12:00:00")
