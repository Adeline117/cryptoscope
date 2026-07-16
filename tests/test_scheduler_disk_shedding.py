"""Disk pressure must shed only explicitly classified non-core scheduler work."""

from __future__ import annotations

import pytest


def _snapshot(state: str) -> dict:
    return {
        "state": state,
        "free_gib": 4.0 if state == "critical" else 10.0,
        "free_percent": 2.0 if state == "critical" else 6.0,
        "thresholds": {"warn_gib": 15.0, "critical_gib": 5.0},
    }


def test_every_active_job_has_one_exact_disk_policy_classification():
    from src.ops.disk_shedding import (
        DISK_CLASSIFIED_JOBS,
        DISK_PROTECTED_JOBS,
        DISK_SHED_AT_CRITICAL,
        DISK_SHED_AT_WARN,
    )
    from src.pipeline.scheduler import create_scheduler

    scheduler = create_scheduler()
    active = {job.id for job in scheduler.get_jobs()}
    assert active == DISK_CLASSIFIED_JOBS
    assert not (DISK_PROTECTED_JOBS & DISK_SHED_AT_WARN)
    assert not (DISK_PROTECTED_JOBS & DISK_SHED_AT_CRITICAL)
    assert not (DISK_SHED_AT_WARN & DISK_SHED_AT_CRITICAL)


@pytest.mark.parametrize("state", ["ok", "warn", "critical", "unknown"])
def test_disk_pressure_never_sheds_core_jobs(state):
    from src.ops.disk_shedding import DISK_PROTECTED_JOBS, disk_shedding_decision

    decisions = [
        disk_shedding_decision(job_id, _snapshot(state))
        for job_id in DISK_PROTECTED_JOBS
    ]
    assert decisions
    assert all(not decision["skip"] for decision in decisions)
    assert all(decision["disk_policy"] == "protected" for decision in decisions)


def test_warn_and_critical_shed_only_their_explicit_tiers():
    from src.ops.disk_shedding import (
        DISK_SHED_AT_CRITICAL,
        DISK_SHED_AT_WARN,
        disk_shedding_decision,
    )

    warn_skips = {
        job_id for job_id in DISK_SHED_AT_WARN | DISK_SHED_AT_CRITICAL
        if disk_shedding_decision(job_id, _snapshot("warn"))["skip"]
    }
    critical_skips = {
        job_id for job_id in DISK_SHED_AT_WARN | DISK_SHED_AT_CRITICAL
        if disk_shedding_decision(job_id, _snapshot("critical"))["skip"]
    }
    assert warn_skips == DISK_SHED_AT_WARN
    assert critical_skips == DISK_SHED_AT_WARN | DISK_SHED_AT_CRITICAL


@pytest.mark.asyncio
async def test_guard_logs_structured_skip_and_automatically_recovers(monkeypatch):
    from src.ops import disk_shedding
    from src.pipeline import scheduler

    snapshots = iter([_snapshot("critical"), _snapshot("ok")])
    monkeypatch.setattr(disk_shedding.health, "_disk_health", lambda: next(snapshots))

    records = []

    class FakeLogger:
        def warning(self, event, **fields):
            records.append(("warning", event, fields))

        def info(self, event, **fields):
            records.append(("info", event, fields))

    monkeypatch.setattr(scheduler, "logger", FakeLogger())
    calls = []

    async def low_priority_job():
        calls.append("ran")
        return "complete"

    guarded = scheduler._disk_guarded_job("holder_snapshots", low_priority_job)
    skipped = await guarded()
    recovered = await guarded()

    assert skipped == {
        "status": "skipped",
        "job_id": "holder_snapshots",
        "reason": "workspace_disk_critical_non_core_shed",
        "disk_state": "critical",
    }
    assert recovered == "complete"
    assert calls == ["ran"]
    assert records[0][0:2] == ("warning", "scheduled_job_disk_shed")
    assert records[0][2]["free_gib"] == 4.0
    assert records[0][2]["disk_policy"] == "shed_at_warn"
    assert records[1] == (
        "info",
        "scheduled_job_disk_shed_recovered",
        {
            "job_id": "holder_snapshots",
            "previous_disk_state": "critical",
            "disk_state": "ok",
            "disk_policy": "shed_at_warn",
        },
    )


def test_unknown_or_failed_disk_measurement_fails_open(monkeypatch):
    from src.ops import disk_shedding

    monkeypatch.setattr(
        disk_shedding.health,
        "_disk_health",
        lambda: (_ for _ in ()).throw(OSError("volume unavailable")),
    )
    decision = disk_shedding.disk_shedding_decision("holder_snapshots")
    assert decision["disk_state"] == "unknown"
    assert not decision["skip"]
    assert "volume unavailable" in decision["health_error"]


def test_unclassified_job_cannot_be_implicitly_shed():
    from src.ops.disk_shedding import disk_shedding_decision

    with pytest.raises(ValueError, match="unclassified scheduler job"):
        disk_shedding_decision("new_unreviewed_job", _snapshot("critical"))
