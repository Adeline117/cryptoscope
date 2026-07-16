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


@pytest.mark.asyncio
async def test_operator_hunt_and_auto_promote_run_off_event_loop(monkeypatch):
    """The synchronous hunt pipeline must not stall other scheduler coroutines."""
    import threading

    from src.distribution import telegram_sender
    from src.pipeline import operator_hunt, scheduler

    loop_thread = threading.get_ident()
    call_threads = []
    suspects = []

    def hunt():
        call_threads.append(("hunt", threading.get_ident()))
        return suspects

    def auto_promote(got):
        assert got is suspects
        call_threads.append(("auto_promote", threading.get_ident()))
        return []

    async def send_alert(_message):
        raise AssertionError("empty scan must not send an alert")

    monkeypatch.setattr(operator_hunt, "hunt", hunt)
    monkeypatch.setattr(operator_hunt, "auto_promote", auto_promote)
    monkeypatch.setattr(telegram_sender, "send_alert", send_alert)

    await scheduler._run_operator_hunt()

    assert [name for name, _thread in call_threads] == ["hunt", "auto_promote"]
    assert all(thread != loop_thread for _name, thread in call_threads)


@pytest.mark.asyncio
async def test_anomaly_screen_runs_off_event_loop(monkeypatch):
    import threading

    from src.distribution import telegram_sender
    from src.pipeline import anomaly_screener, scheduler

    loop_thread = threading.get_ident()
    scan_threads = []

    def screen_universe():
        scan_threads.append(threading.get_ident())
        return []

    async def send_alert(_message):
        raise AssertionError("empty screen must not send an alert")

    monkeypatch.setattr(anomaly_screener, "screen_universe", screen_universe)
    monkeypatch.setattr(telegram_sender, "send_alert", send_alert)

    await scheduler._run_anomaly_screen()

    assert scan_threads and scan_threads[0] != loop_thread


@pytest.mark.asyncio
async def test_daily_perp_cex_sweep_and_market_reads_run_off_event_loop(monkeypatch):
    """The hour-long daily sweep must not freeze five-minute Carry exports."""
    import threading

    from src.onchain import operator_id
    from src.pipeline import outcome_tracker, perp_scanner, operator_sentinel, scheduler

    loop_thread = threading.get_ident()
    call_threads = []
    hit = {
        "address": "0xtoken", "chain": "base", "symbol": "TEST",
        "cex_outflow": 1, "pct_of_cluster": 1,
    }

    def scan_cex_deposits():
        call_threads.append(("scan", threading.get_ident()))
        return [hit]

    def token_market(address):
        assert address == "0xtoken"
        call_threads.append(("market", threading.get_ident()))
        return {"available": False}

    monkeypatch.setattr(perp_scanner, "scan_cex_deposits", scan_cex_deposits)
    monkeypatch.setattr(operator_id, "_token_market", token_market)
    monkeypatch.setattr(outcome_tracker, "log_alert", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(operator_sentinel, "alerts_muted", lambda: True)

    await scheduler._run_perp_cex_scan()

    assert [name for name, _thread in call_threads] == ["scan", "market"]
    assert all(thread != loop_thread for _name, thread in call_threads)


@pytest.mark.asyncio
async def test_early_accumulation_sync_stages_run_off_event_loop(monkeypatch):
    import threading

    from src.onchain import smart_money
    from src.pipeline import operator_hunt, operator_sentinel, outcome_tracker, scheduler

    loop_thread = threading.get_ident()
    call_threads = []
    suspect = {"candidate": True}
    candidate = {
        "address": "0xtoken", "chain": "base", "symbol": "TEST",
        "price0": 1.0, "liquidity": 100_000, "age_days": 2,
        "largest_entity_pct": 10.0,
    }

    def hunt(**kwargs):
        assert kwargs == {"per_chain": 40, "max_scan": 50}
        call_threads.append(("hunt", threading.get_ident()))
        return [suspect]

    def candidates(got):
        assert got == [suspect]
        call_threads.append(("candidates", threading.get_ident()))
        return [candidate]

    def convergence(address, chain, *, max_check):
        assert (address, chain, max_check) == ("0xtoken", "base", 15)
        call_threads.append(("convergence", threading.get_ident()))
        return {"verdict": "none"}

    def log_alert(*args, **kwargs):
        call_threads.append(("log_alert", threading.get_ident()))

    def alerts_muted():
        call_threads.append(("alerts_muted", threading.get_ident()))
        return True

    monkeypatch.setattr(operator_hunt, "hunt", hunt)
    monkeypatch.setattr(operator_hunt, "early_accumulation_candidates", candidates)
    monkeypatch.setattr(smart_money, "convergence", convergence)
    monkeypatch.setattr(outcome_tracker, "log_alert", log_alert)
    monkeypatch.setattr(operator_sentinel, "alerts_muted", alerts_muted)

    await scheduler._run_early_accumulation()

    assert [name for name, _thread in call_threads] == [
        "hunt", "candidates", "convergence", "log_alert", "alerts_muted",
    ]
    assert all(thread != loop_thread for _name, thread in call_threads)


def test_perps_export_is_independent_from_wallet_watch():
    from src.pipeline.scheduler import create_scheduler

    scheduler = create_scheduler()
    jobs = {job.id: job for job in scheduler.get_jobs()}
    assert "perps_export" in jobs and "smart_wallet_watch" in jobs
    assert jobs["perps_export"].func is not jobs["smart_wallet_watch"].func
    assert "*/5" in str(jobs["perps_export"].trigger)


def test_operator_sentinel_interval_matches_observed_runtime():
    from src.pipeline.scheduler import create_scheduler

    scheduler = create_scheduler()
    job = {item.id: item for item in scheduler.get_jobs()}["operator_sentinel"]
    assert job.trigger.interval.total_seconds() == 15 * 60


def test_transfer_sentinel_rotates_bounded_targets_across_slots():
    from src.pipeline import operator_sentinel

    targets = {f"target-{i:02d}": {"symbol": f"T{i:02d}"} for i in range(12)}
    batches = [
        operator_sentinel._rotating_transfer_targets(targets, slot * 15 * 60)
        for slot in range(4)
    ]

    assert all(len(batch) == operator_sentinel.TRANSFER_TARGETS_PER_RUN == 3
               for batch in batches)
    assert set().union(*(set(batch) for batch in batches)) == set(targets)


def test_launch_quote_refresh_has_bounded_independent_fast_job():
    from src.pipeline.scheduler import create_scheduler

    scheduler = create_scheduler()
    jobs = {job.id: job for job in scheduler.get_jobs()}
    assert jobs["launch_quote_refresh"].func is not jobs["launch_radar"].func
    assert "0:00:30" in str(jobs["launch_quote_refresh"].trigger)


def test_solana_reconciliation_has_an_independent_bounded_job():
    from src.pipeline.scheduler import create_scheduler

    scheduler = create_scheduler()
    jobs = {job.id: job for job in scheduler.get_jobs()}
    job = jobs["solana_launch_reconciliation"]
    assert job.func is not jobs["launch_radar"].func
    assert job.trigger.interval.total_seconds() == 30


@pytest.mark.asyncio
async def test_unconfigured_reconciliation_is_fail_visible(monkeypatch):
    from src.pipeline import scheduler, solana_launch_reconcile, stream_health

    reports = []
    monkeypatch.delenv("SOLANA_RECONCILIATION_RPC_URL", raising=False)
    monkeypatch.setattr(
        solana_launch_reconcile, "reconcile_next_epoch",
        lambda *_args, **_kwargs: pytest.fail("unconfigured source must not run"),
    )
    monkeypatch.setattr(
        stream_health, "report_worker",
        lambda source, stream, **kwargs: reports.append((source, stream, kwargs)),
    )

    await scheduler._run_solana_launch_reconciliation()

    assert reports == [("solana", "pump_fun_reconciliation", {
        "status": "degraded",
        "error": "SOLANA_RECONCILIATION_RPC_URL is not configured",
    })]


@pytest.mark.asyncio
async def test_idle_launch_quote_refresh_keeps_liveness_local(monkeypatch):
    from src.pipeline import board_export, launch_radar, operator_sentinel, scheduler

    heartbeats = []
    monkeypatch.setattr(launch_radar, "refresh_quotes", lambda **kwargs: {
        "eligible": 0, "attempted": 0, "refreshed": 0, "skipped_fresh": 0,
        "skipped_backoff": 0, "errors": 0})
    monkeypatch.setattr(operator_sentinel, "_record_detector_heartbeat",
                        lambda name: heartbeats.append(name))
    monkeypatch.setattr(board_export, "render_launch", lambda: (_ for _ in ()).throw(
        AssertionError("idle refresh must not render")))
    monkeypatch.setattr(board_export, "write_views", lambda **_views: (_ for _ in ()).throw(
        AssertionError("idle refresh must not write")))
    monkeypatch.setattr(board_export, "push_to_blob", lambda _paths: (_ for _ in ()).throw(
        AssertionError("idle refresh must not push")))

    await scheduler._run_launch_quotes()

    assert heartbeats == ["launch_quote_refresh"]


@pytest.mark.asyncio
async def test_real_launch_quote_refresh_publishes_assessment(monkeypatch):
    from src.pipeline import board_export, launch_radar, operator_sentinel, scheduler

    calls = []
    launch = {"events": [{"id": "launch-1"}]}
    monkeypatch.setattr(launch_radar, "refresh_quotes", lambda **kwargs: {
        "eligible": 1, "attempted": 1, "refreshed": 1, "skipped_fresh": 0,
        "skipped_backoff": 0, "errors": 0})
    monkeypatch.setattr(operator_sentinel, "_record_detector_heartbeat",
                        lambda name: calls.append(("heartbeat", name)))
    monkeypatch.setattr(board_export, "render_launch",
                        lambda: calls.append(("render",)) or launch)
    monkeypatch.setattr(board_export, "write_views",
                        lambda **views: calls.append(("write", views)) or ["launch"])
    monkeypatch.setattr(board_export, "push_to_blob",
                        lambda paths: calls.append(("push", paths)) or 1)

    await scheduler._run_launch_quotes()

    assert calls == [
        ("heartbeat", "launch_quote_refresh"),
        ("render",),
        ("write", {"launch": launch}),
        ("push", ["launch"]),
    ]


@pytest.mark.asyncio
async def test_launch_render_failure_preserves_last_good_view(tmp_path, monkeypatch):
    from src.pipeline import board_export, launch_radar, operator_sentinel, scheduler
    from tests.test_board_data_contract import _launch_body

    monkeypatch.setattr(board_export, "EXPORT_DIR", tmp_path)
    old = board_export._envelope(_launch_body([{
            "id": "launch-1", "lane": "launch", "action_level": "A1_WATCH",
            "actionable_now": False, "auto_execution_allowed": False,
            "effective_decision": "WATCH",
        }]), view="launch")
    board_export.write_views(launch=old)
    before = {path.name: path.read_bytes() for path in tmp_path.glob("*.json")}
    monkeypatch.setattr(launch_radar, "refresh_quotes", lambda **kwargs: {
        "eligible": 1, "attempted": 1, "refreshed": 1, "skipped_fresh": 0,
        "skipped_backoff": 0, "errors": 0})
    monkeypatch.setattr(operator_sentinel, "_record_detector_heartbeat", lambda _name: None)
    monkeypatch.setattr(launch_radar, "view",
                        lambda: (_ for _ in ()).throw(RuntimeError("ledger unavailable")))
    pushed = []
    monkeypatch.setattr(board_export, "push_to_blob",
                        lambda paths: pushed.extend(paths) or len(paths))

    await scheduler._run_launch_quotes()

    assert {path.name: path.read_bytes() for path in tmp_path.glob("*.json")} == before
    assert pushed == []


@pytest.mark.asyncio
async def test_launch_scan_render_failure_also_preserves_last_good(monkeypatch):
    from src.pipeline import board_export, launch_radar, scheduler

    monkeypatch.setattr(launch_radar, "scan", lambda: {
        "scanned": 1, "assessed": 0, "inserted": 0, "events": [],
    })
    monkeypatch.setattr(board_export, "render_launch", lambda: (_ for _ in ()).throw(
        RuntimeError("ledger unavailable")))
    monkeypatch.setattr(board_export, "write_views", lambda **_views: (_ for _ in ()).throw(
        AssertionError("failed render must not write")))
    monkeypatch.setattr(board_export, "push_to_blob", lambda _paths: (_ for _ in ()).throw(
        AssertionError("failed render must not push")))

    await scheduler._run_launch_radar()


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
async def test_regular_board_export_excludes_independent_scans(monkeypatch):
    from src.pipeline import board_export, scheduler

    calls = []
    monkeypatch.setattr(board_export, "run",
                        lambda **kwargs: calls.append(kwargs)
                        or {"views_written": 0})
    await scheduler._run_board_export()
    assert calls == [{
        "push": True,
        "include_operators": False,
        "include_opportunities": False,
        "include_perps": False,
        "include_launch": False,
    }]


def test_operator_export_has_an_independent_scheduler_job():
    from src.pipeline.scheduler import create_scheduler

    scheduler = create_scheduler()
    jobs = {job.id: job for job in scheduler.get_jobs()}
    assert "operator_export" in jobs and "board_export" in jobs
    assert jobs["operator_export"].func is not jobs["board_export"].func


def test_opportunity_export_has_an_independent_scheduler_job():
    from src.pipeline.scheduler import create_scheduler

    scheduler = create_scheduler()
    jobs = {job.id: job for job in scheduler.get_jobs()}
    assert "opportunity_export" in jobs and "board_export" in jobs
    assert jobs["opportunity_export"].func is not jobs["board_export"].func


@pytest.mark.asyncio
async def test_canonical_outcomes_publish_before_slow_legacy_resolver(monkeypatch):
    from src.pipeline import board_export, opportunity_outcomes, outcome_tracker, scheduler

    order = []
    monkeypatch.setattr(opportunity_outcomes, "resolve",
                        lambda: order.append("canonical") or {"lookups": 1})
    monkeypatch.setattr(board_export, "render_stats",
                        lambda _opps: order.append("render_stats") or {"view": "stats"})
    monkeypatch.setattr(board_export, "write_views",
                        lambda **_views: order.append("write_stats") or ["stats"])
    monkeypatch.setattr(board_export, "push_to_blob",
                        lambda _paths: order.append("push_stats") or 1)
    monkeypatch.setattr(outcome_tracker, "resolve_outcomes",
                        lambda: order.append("legacy") or 0)

    await scheduler._run_resolve_outcomes()

    assert order == ["canonical", "render_stats", "write_stats", "push_stats", "legacy"]
