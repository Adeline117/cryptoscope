"""Resource-safety contracts for the long-running scheduler."""

import pytest


def test_scheduler_worker_budget_is_bounded(monkeypatch):
    from src.pipeline.scheduler import (
        _scheduler_log_level,
        _scheduler_nofile_target,
        _scheduler_worker_count,
    )

    monkeypatch.delenv("SCHEDULER_MAX_WORKERS", raising=False)
    assert _scheduler_worker_count() == 8

    monkeypatch.setenv("SCHEDULER_MAX_WORKERS", "999")
    assert _scheduler_worker_count() == 12

    monkeypatch.setenv("SCHEDULER_MAX_WORKERS", "0")
    assert _scheduler_worker_count() == 2

    monkeypatch.setenv("SCHEDULER_MAX_WORKERS", "not-a-number")
    assert _scheduler_worker_count() == 8

    monkeypatch.delenv("SCHEDULER_LOG_LEVEL", raising=False)
    assert _scheduler_log_level() == 20  # logging.INFO

    monkeypatch.setenv("SCHEDULER_LOG_LEVEL", "warning")
    assert _scheduler_log_level() == 30  # logging.WARNING

    monkeypatch.setenv("SCHEDULER_LOG_LEVEL", "not-a-level")
    assert _scheduler_log_level() == 20

    monkeypatch.delenv("SCHEDULER_NOFILE_SOFT", raising=False)
    assert _scheduler_nofile_target() == 2048
    monkeypatch.setenv("SCHEDULER_NOFILE_SOFT", "999999")
    assert _scheduler_nofile_target() == 8192
    monkeypatch.setenv("SCHEDULER_NOFILE_SOFT", "bad")
    assert _scheduler_nofile_target() == 2048


@pytest.mark.asyncio
async def test_descriptor_heavy_jobs_do_not_overlap(monkeypatch):
    """Accumulation owns the guard until done; sentinel must wait behind it."""
    import asyncio

    from src.pipeline import accumulation_pipeline, operator_sentinel, scheduler

    accumulation_started = asyncio.Event()
    release_accumulation = asyncio.Event()
    sentinel_started = asyncio.Event()

    async def accumulation(send=True):
        accumulation_started.set()
        await release_accumulation.wait()
        return {"status": "complete"}

    def sentinel(use_transfers=False):
        sentinel_started.set()
        return 0

    monkeypatch.setattr(accumulation_pipeline, "run_accumulation_pipeline", accumulation)
    monkeypatch.setattr(operator_sentinel, "run_and_alert", sentinel)

    first = asyncio.create_task(scheduler._run_accumulation())
    await accumulation_started.wait()
    second = asyncio.create_task(scheduler._run_operator_sentinel())
    await asyncio.sleep(0)
    assert not sentinel_started.is_set()

    release_accumulation.set()
    await asyncio.gather(first, second)
    assert sentinel_started.is_set()
