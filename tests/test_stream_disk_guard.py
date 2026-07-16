"""Realtime disk guard is cached, fail-open on unknown, and strict on critical."""
from __future__ import annotations

import pytest


def _snapshot(state: str) -> dict:
    return {
        "state": state, "free_gib": 4.0, "free_percent": 2.0,
        "thresholds": {"critical_gib": 5.0},
    }


def test_disk_guard_caches_each_probe_for_exactly_five_seconds():
    from src.ops.stream_disk_guard import DiskStateGuard

    now = [0.0]
    states = iter((_snapshot("critical"), _snapshot("ok")))
    calls = []

    def probe():
        calls.append(True)
        return next(states)

    guard = DiskStateGuard(probe=probe, monotonic=lambda: now[0])
    first = guard.snapshot()
    now[0] = 4.999
    cached = guard.snapshot()
    now[0] = 5.0
    refreshed = guard.snapshot()

    assert len(calls) == 2
    assert first["state"] == cached["state"] == "critical"
    assert first["sample_id"] == cached["sample_id"]
    assert refreshed["state"] == "ok"
    assert refreshed["sample_id"] == first["sample_id"] + 1


def test_critical_blocks_evidence_but_unknown_fails_open_and_logs(monkeypatch):
    from src.ops import stream_disk_guard

    critical = stream_disk_guard.DiskStateGuard(
        probe=lambda: _snapshot("critical"),
    )
    with pytest.raises(stream_disk_guard.StreamDiskCritical, match="before write|blocked"):
        critical.require_evidence_write("solana")

    records = []

    class Logger:
        def warning(self, event, **fields):
            records.append((event, fields))

    monkeypatch.setattr(stream_disk_guard, "logger", Logger())
    unknown = stream_disk_guard.DiskStateGuard(
        probe=lambda: (_ for _ in ()).throw(OSError("volume unavailable")),
    )
    snapshot = unknown.require_evidence_write("ethereum")
    retain, same = unknown.retain_optional_raw()

    assert snapshot["state"] == same["state"] == "unknown"
    assert retain is True
    assert records[0][0] == "stream_disk_probe_unknown_fail_open"
    assert records[0][1] == {
        "measurement_failed": True,
        "error_kind": "OSError",
    }
    assert "volume unavailable" not in repr(snapshot)
    assert "volume unavailable" not in repr(records)


def test_unknown_probe_log_is_bounded_but_error_kind_changes_report(monkeypatch):
    from src.ops import stream_disk_guard

    records = []

    class Logger:
        def warning(self, event, **fields):
            records.append((event, fields))

    monkeypatch.setattr(stream_disk_guard, "logger", Logger())
    now = [0.0]
    error = [OSError("private message")]

    def probe():
        raise error[0]

    guard = stream_disk_guard.DiskStateGuard(
        probe=probe, monotonic=lambda: now[0],
    )
    guard.snapshot()
    for second in range(5, 60, 5):
        now[0] = float(second)
        guard.snapshot()
    assert len(records) == 1

    now[0] = 60.0
    guard.snapshot()
    assert len(records) == 2

    error[0] = RuntimeError("another private message")
    now[0] = 65.0
    guard.snapshot()
    assert len(records) == 3
    assert records[-1][1]["error_kind"] == "RuntimeError"
    assert "private message" not in repr(records)
