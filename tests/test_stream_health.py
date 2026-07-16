"""Real-time health distinguishes quiet markets from gaps and dead connections."""
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
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
    assert gap["classification"] == "gap_detected" and gap["cursor"] == 13
    assert gap["latency_ms"] == 250 and gap["status"] == "degraded"
    assert gap["open_gaps"] == 1
    assert gap["gap"]["from_cursor"] == 11 and gap["gap"]["to_cursor"] == 12
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


def test_gap_resolution_cannot_erase_a_later_disconnect(health):
    t0 = datetime(2026, 7, 14, 12, tzinfo=timezone.utc)
    health.observe("solana", "launches", cursor=1, received_at=t0,
                   expect_contiguous=True)
    health.observe("solana", "launches", cursor=3, received_at=t0,
                   expect_contiguous=True)
    gap_id = health.open_gaps("solana", "launches")[0]["id"]
    health.mark_disconnected(
        "solana", "launches", "websocket closed", at=t0 + timedelta(seconds=1),
    )

    assert health.resolve_gap(gap_id, at=t0 + timedelta(seconds=2)) is True
    row = health.snapshot(now=t0 + timedelta(seconds=2), stale_after_seconds=60)[0]
    assert row["status"] == "disconnected"
    assert row["last_error"] == "websocket closed"
    assert row["open_gaps"] == 0


def test_worker_heartbeat_distinguishes_degraded_alive_from_stale(health):
    t0 = datetime(2026, 7, 14, 12, tzinfo=timezone.utc)
    health.report_worker(
        "solana", "pump_fun_maintenance", status="degraded",
        error="RPC circuit open", at=t0,
    )
    degraded = health.snapshot(
        now=t0 + timedelta(seconds=30), stale_after_seconds=60,
    )[0]
    assert degraded["status"] == "degraded" and degraded["stale"] is False
    assert degraded["last_event_at"] is None
    assert degraded["last_error"] == "RPC circuit open"

    health.report_worker(
        "solana", "pump_fun_maintenance", status="live",
        at=t0 + timedelta(seconds=45),
    )
    recovered = health.snapshot(
        now=t0 + timedelta(seconds=50), stale_after_seconds=60,
    )[0]
    assert recovered["status"] == "live" and recovered["last_error"] is None


def test_worker_details_are_structured_and_omission_clears_previous_run(health):
    t0 = datetime(2026, 7, 14, 12, tzinfo=timezone.utc)
    details = {
        "schema_version": 1, "outcome": "sealed_clean",
        "rpc": {"rpc_calls_total": 9, "rpc_failures_total": 0},
    }
    health.report_worker(
        "solana", "pump_fun_reconciliation", status="live",
        details=details, at=t0,
    )
    assert health.snapshot(now=t0)[0]["details"] == details

    health.report_worker(
        "solana", "pump_fun_reconciliation", status="degraded",
        error="next run did not configure telemetry", at=t0 + timedelta(seconds=1),
    )
    assert health.snapshot(now=t0 + timedelta(seconds=1))[0]["details"] is None


def test_legacy_stream_table_adds_worker_details_without_rebuild(health):
    legacy = sqlite3.connect(health.DB)
    legacy.execute("""CREATE TABLE streams(
        source TEXT NOT NULL, stream TEXT NOT NULL, cursor INTEGER,
        last_event_at TEXT, last_received_at TEXT, latency_ms INTEGER,
        status TEXT NOT NULL, last_error TEXT, updated_at TEXT NOT NULL,
        PRIMARY KEY(source,stream))""")
    legacy.commit()
    legacy.close()

    health.report_worker(
        "solana", "pump_fun_reconciliation", status="live",
        details={"schema_version": 1, "outcome": "waiting_finality"},
    )

    connection = health._conn()
    try:
        columns = [row[1] for row in connection.execute("PRAGMA table_info(streams)")]
    finally:
        connection.close()
    assert columns[-1] == "details"
    assert health.snapshot()[0]["details"]["outcome"] == "waiting_finality"


def test_worker_heartbeat_rejects_misleading_status(health):
    with pytest.raises(ValueError, match="live or degraded"):
        health.report_worker("solana", "maintenance", status="disconnected")
    with pytest.raises(ValueError, match="details must be an object"):
        health.report_worker(
            "solana", "maintenance", status="live", details=["not", "an", "object"],
        )


def test_stream_clocks_require_timezone(health):
    with pytest.raises(ValueError, match="timezone"):
        health.observe("source", "stream", received_at="2026-07-14T12:00:00")


def test_concurrent_legacy_gap_migration_is_idempotent(health):
    legacy = sqlite3.connect(health.DB)
    legacy.execute("""CREATE TABLE gaps(
        id INTEGER PRIMARY KEY AUTOINCREMENT, source TEXT NOT NULL, stream TEXT NOT NULL,
        from_cursor INTEGER NOT NULL, to_cursor INTEGER NOT NULL,
        detected_at TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'open',
        resolved_at TEXT, details TEXT,
        UNIQUE(source,stream,from_cursor,to_cursor))""")
    legacy.commit()
    legacy.close()

    workers = 12
    start = threading.Barrier(workers)

    def connect_and_read_schema() -> tuple[str, ...]:
        start.wait(timeout=5)
        connection = health._conn()
        try:
            return tuple(
                row[1] for row in connection.execute("PRAGMA table_info(gaps)")
            )
        finally:
            connection.close()

    with ThreadPoolExecutor(max_workers=workers) as pool:
        schemas = list(pool.map(lambda _index: connect_and_read_schema(), range(workers)))

    expected = ("retry_count", "next_retry_at", "last_error")
    assert all(schema[-3:] == expected for schema in schemas)


def test_open_gaps_returns_recovery_queue(health):
    health.observe("solana", "slots", cursor=10, expect_contiguous=True)
    health.observe("solana", "slots", cursor=12, expect_contiguous=True)
    gaps = health.open_gaps("solana", "slots")
    assert len(gaps) == 1
    assert {key: gaps[0][key] for key in ("id", "from_cursor", "to_cursor")} == {
        "id": 1, "from_cursor": 11, "to_cursor": 11}
    assert gaps[0]["details"] == '{"observed_after":12}'


def test_gap_prefix_can_advance_without_claiming_full_recovery(health):
    health.observe("solana", "slots", cursor=10, expect_contiguous=True)
    health.observe("solana", "slots", cursor=20, expect_contiguous=True)
    gap = health.open_gaps("solana", "slots")[0]

    assert health.advance_gap(gap["id"], 13, details={"chunk": [11, 13]}) \
        == "advanced"
    remaining = health.open_gaps("solana", "slots")
    assert [(item["from_cursor"], item["to_cursor"]) for item in remaining] == [(14, 19)]
    assert health.snapshot()[0]["status"] == "degraded"

    assert health.advance_gap(gap["id"], 19, details={"chunk": [14, 19]}) \
        == "resolved"
    assert health.open_gaps("solana", "slots") == []
    assert health.snapshot()[0]["status"] == "live"


def test_failed_gap_recovery_backs_off_without_hiding_the_gap(health):
    t0 = datetime(2026, 7, 14, 12, tzinfo=timezone.utc)
    health.observe("solana", "slots", cursor=10, received_at=t0,
                   expect_contiguous=True)
    health.observe("solana", "slots", cursor=13, received_at=t0,
                   expect_contiguous=True)
    gap = health.open_gaps("solana", "slots", now=t0)[0]

    first = health.defer_gap(gap["id"], "rpc response incomplete", at=t0,
                             base_delay_seconds=60)
    assert first["retry_count"] == 1 and first["delay_seconds"] == 60
    assert health.open_gaps("solana", "slots", now=t0 + timedelta(seconds=59)) == []
    state = health.snapshot(now=t0 + timedelta(seconds=30))[0]
    assert state["status"] == "degraded" and state["open_gaps"] == 1
    assert state["deferred_gaps"] == 1

    due = health.open_gaps("solana", "slots", now=t0 + timedelta(seconds=60))[0]
    assert due["retry_count"] == 1 and "incomplete" in due["last_error"]
    second = health.defer_gap(due["id"], "still limited",
                              at=t0 + timedelta(seconds=60),
                              base_delay_seconds=60)
    assert second["retry_count"] == 2 and second["delay_seconds"] == 120
    assert health.open_gaps(
        "solana", "slots", now=t0 + timedelta(seconds=179)) == []
    assert len(health.open_gaps(
        "solana", "slots", now=t0 + timedelta(seconds=180))) == 1
