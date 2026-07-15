"""Long-lived stream behavior is deterministic without a real network."""
from __future__ import annotations

import threading
from datetime import datetime, timezone

import pytest


class _Socket:
    def __init__(self, messages):
        self.messages = iter(messages)
        self.pings = 0
        self.closed = False

    def recv(self):
        item = next(self.messages)
        if isinstance(item, BaseException):
            raise item
        return item

    def ping(self):
        self.pings += 1

    def close(self):
        self.closed = True


@pytest.fixture
def health_db(tmp_path, monkeypatch):
    from src.pipeline import stream_health

    monkeypatch.setattr(stream_health, "DB", tmp_path / "streams.db")
    return stream_health


def test_runner_backfills_and_resolves_a_sequence_gap(health_db):
    from src.pipeline.stream_runner import StreamEvent, StreamRunner

    stop = threading.Event()
    ws = _Socket([{"seq": 1}, {"seq": 3}])
    backfills = []

    def on_event(payload):
        if payload["seq"] == 3:
            stop.set()

    runner = StreamRunner(
        source="hyperliquid", stream="l2:BTC", connect=lambda: ws,
        subscribe=lambda sock: None,
        parse=lambda raw: StreamEvent(raw, cursor=raw["seq"],
                                      event_at=datetime.now(timezone.utc)),
        on_event=on_event, expect_contiguous=True,
        backfill=lambda start, end: backfills.append((start, end)) or True,
    )
    assert runner.run_connection(stop) == 2
    assert backfills == [(2, 2)] and ws.closed is True
    state = health_db.snapshot(stale_after_seconds=60)[0]
    assert state["status"] == "live" and state["open_gaps"] == 0


def test_timeout_sends_heartbeat_without_declaring_disconnect(health_db):
    from src.pipeline.stream_runner import StreamEvent, StreamRunner

    stop = threading.Event()
    ws = _Socket([TimeoutError(), {"seq": 1}])
    runner = StreamRunner(
        source="helius", stream="launches", connect=lambda: ws,
        subscribe=lambda sock: None,
        parse=lambda raw: StreamEvent(raw, cursor=raw["seq"]),
        on_event=lambda payload: stop.set(), heartbeat_seconds=0,
    )
    assert runner.run_connection(stop) == 1
    assert ws.pings == 1


def test_backoff_is_bounded():
    from src.pipeline.stream_runner import StreamRunner

    runner = StreamRunner(source="s", stream="x", connect=lambda: None,
                          subscribe=lambda ws: None, parse=lambda raw: None,
                          on_event=lambda payload: None,
                          backoff_base_seconds=2, backoff_max_seconds=10)
    assert [runner._backoff(i) for i in (1, 2, 3, 4, 20)] == [2, 4, 8, 10, 10]
