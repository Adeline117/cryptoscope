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

    async def sentinel(use_transfers=False):
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


@pytest.mark.asyncio
async def test_operator_scan_runs_off_event_loop(monkeypatch):
    import threading

    from src.pipeline import operator_sentinel

    loop_thread = threading.get_ident()
    scan_threads = []

    def check_run(use_transfers=False):
        scan_threads.append(threading.get_ident())
        return []

    monkeypatch.setattr(operator_sentinel, "check_run", check_run)
    monkeypatch.setattr(operator_sentinel, "_load", lambda: {})

    assert await operator_sentinel.run_and_alert(use_transfers=True) == 0
    assert scan_threads and scan_threads[0] != loop_thread


def test_perps_export_is_independent_from_wallet_watch():
    from src.pipeline.scheduler import create_scheduler

    scheduler = create_scheduler()
    jobs = {job.id: job for job in scheduler.get_jobs()}
    assert "perps_export" in jobs and "smart_wallet_watch" in jobs
    assert jobs["perps_export"].func is not jobs["smart_wallet_watch"].func
    assert "*/5" in str(jobs["perps_export"].trigger)


@pytest.mark.asyncio
async def test_wallet_watch_job_never_calls_perps_renderer(monkeypatch):
    from src.pipeline import board_export, scheduler

    rendered = []
    monkeypatch.setattr(board_export, "render_watch", lambda: {"watch": []})
    monkeypatch.setattr(
        board_export, "render_perps",
        lambda: (_ for _ in ()).throw(AssertionError("wallet job coupled to perps")),
    )
    monkeypatch.setattr(board_export, "write_views",
                        lambda **views: rendered.append(set(views)) or [])
    monkeypatch.setattr(board_export, "push_to_blob", lambda paths: 0)

    await scheduler._run_smart_wallet_watch()
    assert rendered == [{"watch"}]


@pytest.mark.asyncio
async def test_regular_board_export_excludes_operator_scan(monkeypatch):
    from src.pipeline import board_export, scheduler

    calls = []
    monkeypatch.setattr(board_export, "run",
                        lambda push, include_operators: calls.append((push, include_operators))
                        or {"views_written": 0})
    await scheduler._run_board_export()
    assert calls == [(True, False)]


def test_operator_export_has_an_independent_scheduler_job():
    from src.pipeline.scheduler import create_scheduler

    scheduler = create_scheduler()
    jobs = {job.id: job for job in scheduler.get_jobs()}
    assert "operator_export" in jobs and "board_export" in jobs
    assert jobs["operator_export"].func is not jobs["board_export"].func
