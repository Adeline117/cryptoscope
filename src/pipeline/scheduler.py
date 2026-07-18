"""APScheduler-based task scheduling for all pipelines."""

from __future__ import annotations

import asyncio
import logging
import math
import os
import re
import resource
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from functools import wraps

import structlog
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from src.config import load_settings

logger = structlog.get_logger()

_HEAVY_IO_LOCK: asyncio.Lock | None = None
_HEAVY_IO_LOOP: asyncio.AbstractEventLoop | None = None


def _disk_guarded_job(job_id: str, func):
    """Wrap one zero-argument async job with reversible disk-pressure shedding."""
    was_shed = False
    previous_state = None

    @wraps(func)
    async def guarded():
        nonlocal was_shed, previous_state

        from src.ops.disk_shedding import disk_shedding_decision

        decision = disk_shedding_decision(job_id)
        if decision["skip"]:
            was_shed = True
            previous_state = decision["disk_state"]
            logger.warning("scheduled_job_disk_shed", **decision)
            return {
                "status": "skipped",
                "job_id": job_id,
                "reason": decision["reason"],
                "disk_state": decision["disk_state"],
            }
        if was_shed:
            logger.info(
                "scheduled_job_disk_shed_recovered",
                job_id=job_id,
                previous_disk_state=previous_state,
                disk_state=decision["disk_state"],
                disk_policy=decision["disk_policy"],
            )
            was_shed = False
            previous_state = None
        return await func()

    return guarded


def _install_disk_shedding_guards(scheduler: AsyncIOScheduler) -> None:
    """Validate the complete active-job policy, then guard every scheduled call."""
    from src.ops.disk_shedding import validate_disk_job_policy

    jobs = scheduler.get_jobs()
    validate_disk_job_policy(job.id for job in jobs)
    for job in jobs:
        job.modify(func=_disk_guarded_job(job.id, job.func))


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _heavy_io_lock() -> asyncio.Lock:
    """One-at-a-time boundary for the two descriptor-heavy legacy scans.

    Pytest creates a fresh event loop per async test, so construct the lock lazily
    per loop rather than binding a module-global lock to the first loop forever.
    The production scheduler has one loop for its entire lifetime.
    """
    global _HEAVY_IO_LOCK, _HEAVY_IO_LOOP
    loop = asyncio.get_running_loop()
    if _HEAVY_IO_LOCK is None or _HEAVY_IO_LOOP is not loop:
        _HEAVY_IO_LOCK, _HEAVY_IO_LOOP = asyncio.Lock(), loop
    return _HEAVY_IO_LOCK


def _scheduler_nofile_target() -> int:
    """Desired soft fd limit: enough burst room, still finite and configurable."""
    try:
        return max(256, min(8192, int(os.getenv("SCHEDULER_NOFILE_SOFT", "2048"))))
    except ValueError:
        return 2048


def _configure_fd_limit() -> tuple[int, int]:
    """Raise launchd's small default soft limit without weakening the hard limit."""
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    target = _scheduler_nofile_target()
    ceiling = target if hard == resource.RLIM_INFINITY else min(target, hard)
    if soft < ceiling:
        resource.setrlimit(resource.RLIMIT_NOFILE, (ceiling, hard))
        soft = ceiling
    return soft, hard


def _scheduler_worker_count() -> int:
    """Return a deliberately bounded shared-worker budget.

    The pipelines are I/O heavy and most of their synchronous work is submitted by
    ``asyncio.to_thread``.  Python's implicit executor can create up to 32 workers,
    which allowed several overlapping scans to open enough HTTP/SQLite handles to
    exhaust the macOS process descriptor limit.  A small shared pool is a safety
    boundary: latency is preferable to a dead scheduler and silently missed events.
    """
    try:
        return max(2, min(12, int(os.getenv("SCHEDULER_MAX_WORKERS", "8"))))
    except ValueError:
        return 8


def _scheduler_log_level() -> int:
    """Resolve the scheduler's durable operational log level safely."""
    name = os.getenv("SCHEDULER_LOG_LEVEL", "INFO").upper()
    level = logging.getLevelName(name)
    return level if isinstance(level, int) else logging.INFO


def _configure_runtime_logging() -> None:
    """Drop per-RPC debug noise from the 24/7 LaunchAgent log.

    The underlying collectors emit one debug line for many ordinary fallback/RPC
    failures.  With launchd appending stdout indefinitely, that turned into hundreds
    of MB per day and obscured actionable warnings.  INFO remains the default;
    operators can set SCHEDULER_LOG_LEVEL=DEBUG temporarily for investigation.
    """
    structlog.configure(wrapper_class=structlog.make_filtering_bound_logger(_scheduler_log_level()))


def create_scheduler() -> AsyncIOScheduler:
    """Create and configure the scheduler with all pipeline jobs.

    EXECUTOR STARVATION (fixed): the scheduler ran with APScheduler's defaults, so
    every job shared one small pool. `operator_sentinel` fires every 5 minutes and
    takes 6-8, `perp_cex_scan` takes ~60 — they saturated the pool and everything else
    was silently dropped as a misfire. The logs held **11,240 "was missed by"**
    warnings, and the evidence resolver (`resolve_outcomes`, hourly) last ran two days
    ago while the sentinel ran two minutes ago.

    That is what was actually killing the thesis: alerts accrued and were never
    resolved, so no episode could ever be scored. A silent misfire is indistinguishable
    from a quiet market — the same disease, this time in the scheduler itself.

    Fixes: a wider thread pool, `coalesce` (a backlog runs once, not N times), and a
    generous `misfire_grace_time` so a late job RUNS LATE instead of being skipped.
    """
    # EVERY job here is `async def`. The previous fix set a ThreadPoolExecutor as the
    # default to cure executor starvation — but a thread pool does NOT await coroutines,
    # so it created each coroutine and discarded it ("coroutine was never awaited"): the
    # whole 24/7 automation silently stopped running ANY job. Async jobs MUST run on the
    # AsyncIOExecutor (the loop). Starvation is handled the right way instead — by
    # coalesce + misfire_grace_time below, and by heavy jobs offloading blocking work via
    # asyncio.to_thread (board_export/self_audit already do).
    from apscheduler.executors.asyncio import AsyncIOExecutor

    settings = load_settings()
    scheduler = AsyncIOScheduler(
        executors={"default": AsyncIOExecutor()},
        job_defaults={
            "coalesce": True,        # a backlog of missed runs collapses into one
            "max_instances": 1,      # never run two copies of the same job
            "misfire_grace_time": 3600,   # run up to 1h late rather than skip
        },
    )

    # Daily pipeline
    daily_time = settings.get("schedule", {}).get("daily_run", "08:00")
    hour, minute = daily_time.split(":")
    scheduler.add_job(
        _run_daily,
        CronTrigger(hour=int(hour), minute=int(minute)),
        id="daily_pipeline",
        name="Daily Collection & Analysis",
    )

    # Weekly pipeline (Sunday)
    weekly_cfg = settings.get("schedule", {}).get("weekly_run", "sunday 10:00")
    parts = weekly_cfg.split()
    day_of_week = parts[0][:3] if len(parts) > 1 else "sun"
    w_hour, w_minute = (parts[1] if len(parts) > 1 else parts[0]).split(":")
    scheduler.add_job(
        _run_weekly,
        CronTrigger(day_of_week=day_of_week, hour=int(w_hour), minute=int(w_minute)),
        id="weekly_pipeline",
        name="Weekly Arena Report & Market Overview",
    )

    # 2-hour highlight: top most shocking item across all sources → Telegram
    scheduler.add_job(
        _run_highlight,
        CronTrigger(minute=0, hour="*/2"),
        id="highlight_2h",
        name="2h Highlight — Top Shocking Item to Telegram",
    )

    # Anomaly check (every 30 minutes)
    scheduler.add_job(
        _run_anomaly_check,
        CronTrigger(minute="*/30"),
        id="anomaly_check",
        name="Anomaly Detection Scan",
    )

    # Accumulation detection (二级妖币 Stage 0/1) — every 30 minutes
    scheduler.add_job(
        _run_accumulation,
        CronTrigger(minute="*/30"),
        id="accumulation_detection",
        name="二级妖币 Accumulation Detection (Stage 0/1)",
    )

    # Exit monitor (distribution detection on accumulated tokens) — hourly
    scheduler.add_job(
        _run_exit_monitor,
        CronTrigger(minute=15),
        id="exit_monitor",
        name="二级妖币 Exit Monitor (distribution → exit)",
    )

    # Stage 2 launch detector — poll the narrow watchlist every 5 minutes
    scheduler.add_job(
        _run_stage2,
        CronTrigger(minute="*/5"),
        id="stage2_launch_detector",
        name="二级妖币 Stage 2 Launch Detector (watchlist poll)",
    )

    # Daily system-health summary to Telegram
    scheduler.add_job(
        _run_health_summary,
        CronTrigger(hour=9, minute=0),
        id="health_summary",
        name="Daily System Health Summary",
    )

    # Daily correctness self-audit: labeled-case validators + live balanceOf
    # ground-truth gate. Alarms on failure (ghost data / regressed case). Twice
    # daily so a ghost-data lie surfaces within ~12h, not a full day.
    scheduler.add_job(
        _run_self_audit,
        CronTrigger(hour="7,19", minute=20),
        id="self_audit",
        name="自检 (标注案例+链上真值网关 → 失败即告警)",
    )

    # General board export excludes the minutes-long operator verdict scan. Operators
    # have their own guarded job below, so one slow RPC fan-out cannot hold every view.
    scheduler.add_job(
        _run_board_export,
        CronTrigger(minute=50),
        id="board_export",
        name="证伪器看板常规视图导出 → Vercel Blob",
    )
    scheduler.add_job(
        _run_operator_export,
        CronTrigger(minute=10),
        id="operator_export",
        name="Operators 独立慢速判决导出 → Blob",
    )
    scheduler.add_job(
        _run_opportunity_export,
        CronTrigger(minute=20),
        id="opportunity_export",
        name="聪明钱 Opportunities 独立扫描与记分牌导出 → Blob",
    )

    # EARLIEST lane: poll watched proven wallets' fresh buys every 15 min (much more
    # often than the 45-min board export — this is the real-time-ish signal).
    scheduler.add_job(
        _run_smart_wallet_watch,
        CronTrigger(minute="*/15"),
        id="smart_wallet_watch",
        name="聪明钱实时监听 (watched wallets 刚买入 → Blob)",
    )
    # Freeze smart-money convergence events + resolve their forward returns so the
    # one accessible offense line proves (or falsifies) its own edge for free.
    scheduler.add_job(
        _run_convergence_ledger,
        CronTrigger(minute="2-59/15"),
        id="convergence_ledger",
        name="聪明钱收敛账本 (冻结事件 + 前向1h/24h/7d结果)",
    )
    # One ranked signal feed over the four get-rich directions (打新/吸筹/聪明钱/
    # 派发做空) → board signals.json + a Telegram digest every 30 min.
    scheduler.add_job(
        _run_signal_feed,
        CronTrigger(minute="7,37"),
        id="signal_feed",
        name="信号台 (四方向排序候选 → 看板 + Telegram)",
    )
    scheduler.add_job(
        _run_perps_export,
        CronTrigger(minute="*/5"),
        id="perps_export",
        name="永续/Cascade/Carry 独立导出 → Blob",
    )
    # Low-float launch discovery is a separate event feed. It must run far more
    # often than the 45-minute board export, otherwise the "first seen" price is
    # already a hindsight snapshot.
    scheduler.add_job(
        _run_launch_radar,
        CronTrigger(minute="*/3"),
        id="launch_radar",
        name="Launch Radar (首池/低流通事件账本)",
    )
    scheduler.add_job(
        _run_solana_launch_reconciliation,
        # One 128-slot epoch spans roughly 51 seconds at the nominal 400 ms slot
        # time. A 60-second cadence can never catch up after any delay, so run a
        # bounded single epoch every 30 seconds and let max_instances prevent overlap.
        IntervalTrigger(seconds=30),
        id="solana_launch_reconciliation",
        name="Solana Launch 独立 finalized epoch 对账",
    )
    scheduler.add_job(
        _run_launch_quotes,
        IntervalTrigger(seconds=30),
        id="launch_quote_refresh",
        name="Launch 最新只读报价刷新 → Blob",
    )
    scheduler.add_job(
        _run_structure_radar,
        CronTrigger(minute="*/2"),
        id="structure_radar",
        name="Structure Radar (公开上币事件账本)",
    )
    # HLP is a slow-moving passive-EV source; a 30-min refresh of its historical
    # return + drawdown is ample and stays well within the free HL API budget.
    scheduler.add_job(
        _run_hlp_tracker,
        IntervalTrigger(minutes=30),
        id="hlp_tracker",
        name="HLP 金库 (被动做市对手盘 历史年化/回撤)",
    )
    # Refresh the proven-wallet watchlist daily (the skilled set is stable day-to-day).
    scheduler.add_job(
        _run_harvest_wallets,
        CronTrigger(hour=8, minute=10),
        id="harvest_wallets",
        name="聪明钱名单收割 (GMGN PnL rank → watchlist)",
    )

    # Anomaly candidate screen → push suspected-accumulation coins every 6h
    scheduler.add_job(
        _run_anomaly_screen,
        CronTrigger(minute=30, hour="*/6"),
        id="anomaly_screen",
        name="疑似吸筹候选筛选 (market footprint → Telegram)",
    )

    # Operator sentinel → watch confirmed clusters (BASED…) for distribute/rug/
    # launch every 15 min. The transfer-heavy pass rotates a bounded target batch;
    # the previous full pass could exceed 11 minutes when one RPC fallback stalled.
    # A relative interval also avoids a permanent collision with :00/:30 accumulation.
    scheduler.add_job(
        _run_operator_sentinel,
        IntervalTrigger(minutes=15),
        id="operator_sentinel",
        name="操作者哨兵 (庄买/庄卖/砸盘/rug → Telegram)",
    )

    # Operator hunt → actively find NEW hidden-Sybil operators daily.
    scheduler.add_job(
        _run_operator_hunt,
        CronTrigger(hour=3, minute=15),
        id="operator_hunt",
        name="操作者猎手 (扫BSC/SOL找隐藏控盘 → Telegram)",
    )

    # Perp universe → daily rebuild.  Its evidence cache expires after 26 hours, so
    # a weekly cadence guaranteed multi-day blocked windows even when every refresh
    # succeeded.  A daily cron remains below TTL even across a one-hour DST shift.
    scheduler.add_job(
        _run_perp_universe_refresh,
        CronTrigger(hour=3, minute=30),
        id="perp_universe_refresh",
        name="永续宇宙日刷新 (可做空/做多的币 → 合约映射)",
    )

    # Operator-ID push → daily, runs the verified identify_operator on sentinels and
    # pushes actionable operator verdicts (loaded=pump / distributing=dump) to Telegram.
    scheduler.add_job(
        _run_operator_id_push,
        CronTrigger(hour=7, minute=0),
        id="operator_id_push",
        name="操盘判决推送 (loaded/distributing → Telegram)",
    )

    # Holder snapshots → daily, builds our OWN holder history for tracked tokens so
    # exited-operator detection becomes a local before/after diff (Dune-independent).
    scheduler.add_job(
        _run_holder_snapshots,
        CronTrigger(hour=6, minute=30),
        id="holder_snapshots",
        name="持币快照 (攒本地历史 → 未来抓离场庄,不依赖Dune)",
    )

    # Perp CEX-deposit scan → daily dump-precursor sweep over shortable coins.
    # ~60 min; runs at a quiet hour. Own infra, not a third-party feed.
    scheduler.add_job(
        _run_perp_cex_scan,
        CronTrigger(hour=5, minute=0),
        id="perp_cex_scan",
        name="永续做空前兆 (大户→CEX充值 → 记录+可选推送)",
    )

    # Every 6h: the ACCRUAL ENGINE for the kill-line thesis. Approval/gas/LP-unlock
    # moments on the 190 shortable coins — the only universe where a short edge can
    # be monetised. mobilization's lookback is 12h of blocks, so a 6h cadence has
    # 2x margin; a cursor that still falls outside logs a loud gap_skipped rather
    # than silently losing events.
    scheduler.add_job(
        _run_perp_mobilization,
        CronTrigger(hour="1,7,13,19", minute=40),
        id="perp_mobilization",
        name="永续戒备事件 (授权路由/注gas/LP解锁 → 记录)",
    )

    # Every 6h: the LONG-side early-capture experiment. Scans fresh launches for
    # verified-bought coordinated accumulation and forward-records candidates. Rare
    # signature -> needs continuous scanning to accrue any sample; the kill-line
    # (evidence.py) decides whether it has an edge.
    scheduler.add_job(
        _run_early_accumulation,
        CronTrigger(hour="2,8,14,20", minute=10),
        id="early_accumulation",
        name="早期操盘吸筹 (新发币·从市场买入·前向记录)",
    )

    # Every 3h: the 妖币 FINDER — the repo's day-one goal. Wide continuous scan of the
    # young-token window (NOT trending, which is post-pump) for verified real operators,
    # accumulating a ranked watchlist. Frequent because the signature is ~1% rare and
    # only a wide net over time builds a real list.
    scheduler.add_job(
        _run_yaobi_finder,
        CronTrigger(hour="*/3", minute=25),
        id="yaobi_finder",
        name="妖币发现器 (年轻币·核实真操盘·观察名单)",
    )

    # Cluster coverage → weekly Dune holder reconstruction per sentinel; alerts on
    # untracked big EOAs / reconstruction drift (the manual audit, institutionalized).
    scheduler.add_job(
        _run_cluster_coverage,
        CronTrigger(day_of_week="tue", hour=4, minute=0),
        id="cluster_coverage",
        name="簇覆盖周检 (Dune反推 vs 哨兵簇 → Telegram)",
    )

    # CEX label refresh → weekly full pull of verified BSC exchange labels.
    scheduler.add_job(
        _run_cex_label_refresh,
        CronTrigger(day_of_week="mon", hour=4, minute=0),
        id="cex_label_refresh",
        name="CEX标签周刷新 (Dune → 本地缓存)",
    )

    # Label verify → daily contamination sweep of every trusted operator address
    # against Dune labels (the check that would have caught Gate.io-in-SIREN weeks
    # earlier). Cheap: one batched Dune query.
    scheduler.add_job(
        _run_label_verify,
        CronTrigger(hour=4, minute=30),
        id="label_verify",
        name="实体标签验证 (受信地址≠交易所/桥 → Telegram)",
    )

    # Funder watch → verified operator roots. New fundee convergence =
    # 庄 seeding a fresh shell wallet, earlier than any concentration signal.
    scheduler.add_job(
        _run_funder_watch,
        CronTrigger(minute="*/30"),
        id="funder_watch",
        name="多币庄源头监控 (出资人给新地址转钱 → Telegram)",
    )

    # Holder-growth screener → a universe source independent of trending feeds: find
    # tokens whose float is CONCENTRATING (top10/gini rising, fetch-depth-stable) over
    # the snapshot history, then confirm with the operator signal (disperser-guarded).
    scheduler.add_job(
        _run_holder_growth_screen,
        CronTrigger(hour="*/6", minute=20),
        id="holder_growth_screen",
        name="持币集中度趋势筛 (浮筹被吸 → 操盘确认 → Telegram)",
    )

    # Second-leg classification → refresh daily (pumped+pulled-back+loaded setups).
    scheduler.add_job(
        _run_second_leg_assess,
        CronTrigger(hour="*/6", minute=5),
        id="second_leg_assess",
        name="二波候选评估 (已拉+回落+庄满仓)",
    )

    # Resolve alert outcomes hourly → measured hit-rate, feeds calibration.
    scheduler.add_job(
        _run_resolve_outcomes,
        CronTrigger(minute=8),
        id="resolve_outcomes",
        name="告警结果结算 (命中率)",
    )

    # Majors monitor (BTC/ETH/SOL) → flow + positioning signals every 30 min.
    scheduler.add_job(
        _run_majors_monitor,
        CronTrigger(minute="*/30"),
        id="majors_monitor",
        name="大币持仓监控 (费率/OI/多空比 → Telegram)",
    )

    # --- Platform Report Schedules ---

    # Tier 1 platform reports (every 30 minutes)
    scheduler.add_job(
        _run_tier1_reports,
        CronTrigger(minute="*/30"),
        id="tier1_platform_reports",
        name="Tier 1 Platform Reports (Glassnode, Nansen, Chainalysis, etc.)",
    )

    # Tier 2 platform reports (every 2 hours)
    scheduler.add_job(
        _run_tier2_reports,
        CronTrigger(minute=0, hour="*/2"),
        id="tier2_platform_reports",
        name="Tier 2 Platform Reports (Token Terminal, CryptoQuant, etc.)",
    )

    # Tier 3+ platform reports (every 6 hours)
    scheduler.add_job(
        _run_tier3plus_reports,
        CronTrigger(minute=0, hour="*/6"),
        id="tier3plus_platform_reports",
        name="Tier 3-6 Platform Reports",
    )

    # Known weekly report windows
    # Glassnode "The Week On-Chain" — typically Monday ~14:00 UTC
    scheduler.add_job(
        _check_glassnode_weekly,
        CronTrigger(day_of_week="mon", hour=14, minute=0),
        id="glassnode_weekly_check",
        name="Glassnode Week On-Chain Check",
    )

    # CoinShares Weekly Fund Flows — typically Monday ~10:00 UTC
    scheduler.add_job(
        _check_coinshares_weekly,
        CronTrigger(day_of_week="mon", hour=10, minute=0),
        id="coinshares_weekly_check",
        name="CoinShares Weekly Fund Flows Check",
    )

    # Coin Metrics "State of the Network" — typically Tuesday ~15:00 UTC
    scheduler.add_job(
        _check_coinmetrics_weekly,
        CronTrigger(day_of_week="tue", hour=15, minute=0),
        id="coinmetrics_weekly_check",
        name="Coin Metrics SOTN Check",
    )

    # --- Macro Economic Schedules ---

    # Daily macro snapshot (06:00 UTC, before US market open)
    scheduler.add_job(
        _run_daily_macro,
        CronTrigger(hour=6, minute=0),
        id="daily_macro_snapshot",
        name="Daily Macro Snapshot (FRED + Net Liquidity)",
    )

    # Political/regulatory scan (every 2 hours)
    scheduler.add_job(
        _run_regulatory_scan,
        CronTrigger(minute=0, hour="*/2"),
        id="regulatory_scan",
        name="Political & Regulatory News Scan",
    )

    # --- GitHub Star Velocity ---

    # Star count check (every hour)
    scheduler.add_job(
        _run_github_star_check,
        CronTrigger(minute=0),
        id="github_star_hourly",
        name="GitHub Star Velocity Check",
    )

    # GitHub discovery scan (every 6 hours)
    scheduler.add_job(
        _run_github_discovery,
        CronTrigger(minute=0, hour="*/6"),
        id="github_discovery",
        name="GitHub New Repo Discovery",
    )

    # GitHub weekly digest (Friday 18:00 UTC)
    scheduler.add_job(
        _run_github_weekly_digest,
        CronTrigger(day_of_week="fri", hour=18, minute=0),
        id="github_weekly_digest",
        name="GitHub Weekly Star Digest",
    )

    # 用户只要妖币(操盘检测)通知,不要新闻/报告/宏观/GitHub/大币推送。
    # 移除这些非妖币任务;妖币检测器(哨兵/猎手/源头/吸筹/持币趋势/二波)保留。
    _NON_YAOBI_JOBS = {
        "daily_pipeline", "weekly_pipeline", "highlight_2h", "majors_monitor",
        "tier1_platform_reports", "tier2_platform_reports", "tier3plus_platform_reports",
        "glassnode_weekly_check", "coinshares_weekly_check", "coinmetrics_weekly_check",
        "daily_macro_snapshot", "regulatory_scan",
        "github_star_hourly", "github_discovery", "github_weekly_digest",
    }
    for _jid in _NON_YAOBI_JOBS:
        try:
            scheduler.remove_job(_jid)
        except Exception:
            pass

    # 庄家/operator/smart-money detection jobs feed the on-chain signal feed.
    # Resumed 2026-07 after the user opted into paid BSC/Base operator coverage
    # (Moralis re-enabled via MORALIS_ENABLED). To pause a lane again for cost,
    # add its id back here AND move it out of the disk_shedding classification.
    # operator_sentinel at */15 (96/day) is the largest Moralis cost driver —
    # the first knob to turn if the bill spikes.
    _PAUSED_MORALIS_JOBS: set[str] = set()
    for _jid in _PAUSED_MORALIS_JOBS:
        try:
            scheduler.remove_job(_jid)
        except Exception:
            pass

    _install_disk_shedding_guards(scheduler)
    return scheduler


async def _run_daily():
    logger.info("scheduled_daily_pipeline")
    from src.pipeline.daily_pipeline import run_daily_pipeline
    await run_daily_pipeline()


async def _run_weekly():
    logger.info("scheduled_weekly_pipeline")
    from src.pipeline.daily_pipeline import run_daily_pipeline
    from src.distribution.draft_manager import DraftManager
    from src.distribution.telegram_sender import send_alert

    # Run the daily pipeline as part of weekly
    daily_result = await run_daily_pipeline()

    # Generate weekly summary from draft stats
    try:
        dm = DraftManager()
        await dm.init()
        stats = await dm.get_stats()
        await dm.close()

        stats_lines = "\n".join(f"  - {status}: {count}" for status, count in stats.items())
        weekly_summary = (
            f"Weekly Digest\n\n"
            f"Daily pipeline result: {daily_result.get('status', 'unknown')}\n"
            f"Items collected this run: {daily_result.get('items_collected', 0)}\n"
            f"Threads generated: {daily_result.get('threads_generated', 0)}\n"
            f"Anomalies: {daily_result.get('anomalies', 0)}\n\n"
            f"Draft stats (all time):\n{stats_lines}\n\n"
            f"Top narratives: {', '.join(daily_result.get('top_narratives', []))}"
        )
        await send_alert(weekly_summary)
        logger.info("weekly_digest_sent")
    except Exception as e:
        logger.error("weekly_digest_failed", error=str(e))


async def _run_highlight():
    """Every 2 hours: collect all sources, pick the most shocking item, send to Telegram."""
    logger.info("scheduled_highlight_pipeline")
    from src.pipeline.highlight_pipeline import run_highlight_pipeline
    await run_highlight_pipeline()


async def _run_anomaly_check():
    logger.info("scheduled_anomaly_check")
    from src.collectors.chain_data import DeFiLlamaCollector
    from src.analysis.anomaly_engine import detect_all_anomalies

    collector = DeFiLlamaCollector()
    result = await collector.collect()
    anomalies = detect_all_anomalies(result.items)

    critical = [a for a in anomalies if a.severity == "critical"]
    if critical:
        logger.warning("critical_anomalies_found", count=len(critical))
        from src.distribution.telegram_sender import send_alert
        for anomaly in critical[:5]:
            await send_alert(
                f"Critical Anomaly Detected\n\n"
                f"Type: {anomaly.anomaly_type}\n"
                f"Details: {anomaly.description}\n"
                f"Severity: {anomaly.severity}"
            )


async def _run_accumulation():
    logger.info("scheduled_accumulation_detection")
    from src.pipeline.accumulation_pipeline import run_accumulation_pipeline

    # This scan repeatedly creates short-lived HTTP/SQLite resources. Never overlap
    # it with operator_sentinel: their :00/:30 schedules previously crossed the
    # launchd soft fd limit and made an ordinary state-file write fail with EMFILE.
    async with _heavy_io_lock():
        result = await run_accumulation_pipeline()
    logger.info("accumulation_detection_done", **result)


async def _run_exit_monitor():
    logger.info("scheduled_exit_monitor")
    from src.pipeline.exit_monitor import run_exit_monitor

    result = await run_exit_monitor()
    logger.info("exit_monitor_done", **result)


async def _run_stage2():
    logger.info("scheduled_stage2_detector")
    from src.pipeline.stage2_detector import run_stage2_detector

    result = await run_stage2_detector()
    logger.info("stage2_detector_done", **result)


async def _run_smart_wallet_watch():
    """Every 15 min: poll the watched proven wallets, push the fresh-buys lane. This is
    the board's EARLIEST signal so it runs far more often than the 45-min board export."""
    import asyncio

    logger.info("scheduled_smart_wallet_watch")
    from src.pipeline import board_export
    try:
        watch = await asyncio.to_thread(board_export.render_watch)
        paths = await asyncio.to_thread(board_export.write_views, watch=watch)
        n = await asyncio.to_thread(board_export.push_to_blob, paths)
        health = watch.get("source_health") or {}
        logger.info("smart_wallet_watch_done", tokens=len(watch.get("watch", [])),
                    source_state=health.get("state"),
                    source_error=health.get("error_kind"),
                    observed=health.get("observed"),
                    request_failed=health.get("request_failed"),
                    error_counts=health.get("error_counts"), pushed=n)
    except Exception as e:
        logger.error("smart_wallet_watch_failed", error=str(e)[:120])


async def _run_perps_export():
    """Publish perpetual/cascade/carry independently of wallet APIs and slow scans."""
    import asyncio

    logger.info("scheduled_perps_export")
    from src.pipeline import board_export
    try:
        perps = await asyncio.to_thread(board_export.render_perps)
        paths = await asyncio.to_thread(board_export.write_views, perps=perps)
        pushed = await asyncio.to_thread(board_export.push_to_blob, paths)
        logger.info("perps_export_done", perps=len(perps.get("perps", [])),
                    cascades=len(perps.get("cascade_events", [])),
                    carry=len(perps.get("carry", [])), pushed=pushed)
    except Exception as e:
        logger.error("perps_export_failed", error=str(e)[:120])


async def _run_signal_feed():
    """Aggregate the four get-rich directions into one ranked feed → board + Telegram."""
    import asyncio

    logger.info("scheduled_signal_feed")
    try:
        from src.pipeline import signal_feed
        res = await asyncio.to_thread(signal_feed.run)
        logger.info("signal_feed_done", **res)
    except Exception as e:
        logger.error("signal_feed_failed", error=str(e)[:120])


async def _run_convergence_ledger():
    """Freeze new smart-money convergence events + resolve due forward outcomes.

    Free (GMGN via FlareSolverr + keyless DexScreener); no Moralis, no real orders.
    """
    import asyncio

    logger.info("scheduled_convergence_ledger")
    try:
        from src.pipeline import convergence_ledger
        res = await asyncio.to_thread(convergence_ledger.run)
        logger.info("convergence_ledger_done",
                    inserted=res["recorded"]["inserted"],
                    resolved=res["resolved"]["resolved"],
                    source_state=res.get("source_state"))
    except Exception as e:
        logger.error("convergence_ledger_failed", error=str(e)[:120])


async def _run_harvest_wallets():
    """Daily: refresh the proven-wallet watchlist from GMGN's PnL rank."""
    import asyncio

    logger.info("scheduled_harvest_wallets")
    from src.onchain import smart_wallets
    try:
        res = await asyncio.to_thread(smart_wallets.harvest_all)
        logger.info("harvest_wallets_done", **res)
    except Exception as e:
        logger.error("harvest_wallets_failed", error=str(e)[:120])


async def _run_launch_radar():
    """Ingest new DEX launch events separately from board rendering."""
    import asyncio

    logger.info("scheduled_launch_radar")
    try:
        from src.pipeline.launch_radar import scan
        res = await asyncio.to_thread(scan)
        # Publish the event lane immediately; waiting for the 45-minute full board
        # export would destroy the very latency advantage this lane is meant to test.
        from src.pipeline import board_export
        launch = await asyncio.to_thread(board_export.render_launch)
        delivery = await _publish_launch_with_delivery(launch)
        logger.info("launch_radar_done", scanned=res["scanned"], assessed=res.get("assessed", 0),
                    inserted=res["inserted"],
                    active=len(res["events"]), **delivery)
    except Exception as e:
        logger.error("launch_radar_failed", error=str(e)[:120])


async def _run_solana_launch_reconciliation():
    """Seal one bounded epoch; frequent runs can catch up without unbounded loops."""
    import asyncio

    from src.pipeline import solana_launch_reconcile as reconcile
    from src.pipeline import solana_launch_stream as stream
    from src.pipeline import stream_health

    telemetry = reconcile.new_rpc_run_telemetry()
    endpoint = reconcile.configured_archive_endpoint()
    if not endpoint:
        await asyncio.to_thread(
            stream_health.report_worker,
            "solana", "pump_fun_reconciliation", status="degraded",
            error="SOLANA_RECONCILIATION_RPC_URL is not configured",
            details=reconcile.reconciliation_worker_details(
                telemetry, outcome="unconfigured",
            ),
        )
        logger.warning("solana_launch_reconciliation_unconfigured")
        return
    live_rpc = stream.JsonRpc(stream.configured_rpc_endpoint())
    archive_rpc = stream.JsonRpc(endpoint)
    current = _utc_now()
    try:
        circuit = await asyncio.to_thread(
            reconcile.reconciliation_circuit_state,
            live_rpc, archive_rpc, now=current,
        )
    except Exception as exc:
        await asyncio.to_thread(
            stream_health.report_worker,
            "solana", "pump_fun_reconciliation", status="degraded",
            error=f"persistent circuit unavailable: {type(exc).__name__}"[:240],
            details=reconcile.reconciliation_worker_details(
                telemetry, outcome="failed", error_kind=type(exc).__name__,
            ),
        )
        logger.error(
            "solana_launch_reconciliation_circuit_failed",
            error_kind=type(exc).__name__,
        )
        return
    if circuit["state"] == "open":
        retry_at = datetime.fromisoformat(circuit["next_retry_at"])
        retry_in = max(0, math.ceil((retry_at - current).total_seconds()))
        # The pressure failure was already reported when the circuit opened.  Keep
        # ordinary 30-second scheduler ticks quiet until a real retry is allowed.
        logger.debug(
            "solana_launch_reconciliation_circuit_open",
            retry_in_seconds=retry_in,
            pressure_failures=circuit["consecutive_pressure_failures"],
        )
        return
    try:
        start_raw = os.getenv("SOLANA_RECONCILIATION_START_SLOT", "").strip()
        start_slot = int(start_raw) if start_raw else None
        result = await asyncio.to_thread(
            reconcile.reconcile_next_epoch,
            live_rpc, archive_rpc,
            start_slot=start_slot, telemetry=telemetry,
        )
        circuit = await asyncio.to_thread(
            reconcile.clear_reconciliation_circuit, live_rpc, archive_rpc,
        )
        healthy = result.get("state") in {"sealed_clean", "waiting_finality"}
        await asyncio.to_thread(
            stream_health.report_worker,
            "solana", "pump_fun_reconciliation",
            status="live" if healthy else "degraded",
            error=None if healthy else str(result)[:240],
            details=reconcile.reconciliation_worker_details(
                telemetry, outcome=str(result.get("state") or "unknown"),
                circuit=circuit,
            ),
        )
        logger.info("solana_launch_reconciliation_done", **result)
    except stream.RpcPressureError as exc:
        failed_method = reconcile.reconciliation_failed_method(telemetry)
        if failed_method is None:
            failed_method = "unknown"
        failure_clock = _utc_now()
        circuit = await asyncio.to_thread(
            reconcile.open_reconciliation_circuit,
            live_rpc, archive_rpc, pressure_kind=exc.kind,
            failed_method=failed_method,
            retry_after_seconds=exc.retry_after_seconds, now=failure_clock,
        )
        retry_at = datetime.fromisoformat(circuit["next_retry_at"])
        cooldown = max(0, math.ceil((retry_at - failure_clock).total_seconds()))
        await asyncio.to_thread(
            stream_health.report_worker,
            "solana", "pump_fun_reconciliation", status="degraded",
            error=(f"archive RPC {exc.kind}; retry in {cooldown}s")[:240],
            details=reconcile.reconciliation_worker_details(
                telemetry, outcome="rpc_pressure", error_kind=exc.kind,
                circuit=circuit,
            ),
        )
        logger.warning(
            "solana_launch_reconciliation_pressure",
            pressure_kind=exc.kind,
            retry_in_seconds=cooldown,
            pressure_failures=circuit["consecutive_pressure_failures"],
        )
    except Exception as exc:
        try:
            circuit = await asyncio.to_thread(
                reconcile.clear_reconciliation_circuit, live_rpc, archive_rpc,
            )
        except Exception:
            circuit = None
        await asyncio.to_thread(
            stream_health.report_worker,
            "solana", "pump_fun_reconciliation", status="degraded",
            error=f"{type(exc).__name__}: {exc}"[:240],
            details=reconcile.reconciliation_worker_details(
                telemetry, outcome="failed", error_kind=type(exc).__name__,
                circuit=circuit,
            ),
        )
        logger.error(
            "solana_launch_reconciliation_failed", error=str(exc)[:160],
        )


async def _run_launch_quotes():
    """Publish only a real quote assessment; keep idle liveness local."""
    import asyncio

    logger.info("scheduled_launch_quote_refresh")
    try:
        from src.pipeline.launch_radar import refresh_quotes
        max_candidates = max(1, min(5, int(os.getenv("LAUNCH_QUOTE_REFRESH_MAX", "1"))))
        result = await asyncio.to_thread(refresh_quotes, max_candidates=max_candidates)
        # Job liveness is local operational state, not a market/source observation.
        # It must never advance the public view's generated_at clock.
        from src.pipeline.operator_sentinel import _record_detector_heartbeat
        await asyncio.to_thread(_record_detector_heartbeat, "launch_quote_refresh")
        if not result["refreshed"]:
            logger.info("launch_quote_refresh_idle", **result, pushed=0)
            return
        from src.pipeline import board_export
        launch = await asyncio.to_thread(board_export.render_launch)
        delivery = await _publish_launch_with_delivery(launch)
        logger.info("launch_quote_refresh_done", **result, **delivery)
    except Exception as exc:
        logger.error("launch_quote_refresh_failed", error=str(exc)[:120])


async def _publish_launch_with_delivery(launch: dict) -> dict:
    """Publish stable discovery first, then prove an immutable public snapshot.

    Blob overwrite propagation is not a delivery SLA.  If the stable launch/meta
    batch is incomplete, no immutable proof is attempted and the row remains A2.
    """
    import asyncio

    from src.pipeline import board_export

    paths = await asyncio.to_thread(board_export.write_views, launch=launch)
    pushed = await asyncio.to_thread(board_export.push_to_blob, paths)
    summary = {
        "pushed": pushed,
        "delivery_eligible": 0,
        "delivery_attempted": 0,
        "delivery_uploaded": 0,
        "delivery_read_back": 0,
        "delivery_inserted": 0,
        "delivery_errors": 0,
        "delivery_deferred": 0,
        "a3_pushed": 0,
    }
    if pushed != len(paths):
        return summary

    from src.pipeline.launch_delivery import publish_and_verify_launch_snapshots

    proof = await asyncio.to_thread(publish_and_verify_launch_snapshots, launch)
    for field in (
        "eligible", "attempted", "uploaded", "read_back", "inserted", "errors",
        "deferred",
    ):
        summary[f"delivery_{field}"] = proof.get(field, 0)
    if not proof.get("inserted"):
        return summary

    # The proof is a separate immutable fact. Re-read the ledger so A3 is derived,
    # validated against SQL authority, and then published with the public snapshot
    # URL. Automatic execution remains hard-disabled by both contracts.
    promoted = await asyncio.to_thread(board_export.render_launch)
    promoted_paths = await asyncio.to_thread(board_export.write_views, launch=promoted)
    summary["a3_pushed"] = await asyncio.to_thread(
        board_export.push_to_blob, promoted_paths,
    )
    return summary


async def _run_structure_radar():
    """Ingest public exchange-listing events; no rumor feed or directional call."""
    import asyncio

    logger.info("scheduled_structure_radar")
    try:
        from src.pipeline.structure_radar import scan
        from src.pipeline import board_export
        res = await asyncio.to_thread(scan)
        structure = await asyncio.to_thread(board_export.render_structure)
        paths = await asyncio.to_thread(board_export.write_views, structure=structure)
        pushed = await asyncio.to_thread(board_export.push_to_blob, paths)
        logger.info("structure_radar_done", **{k: res[k] for k in ("scanned", "inserted")},
                    active=len(res["events"]), pushed=pushed)
    except Exception as e:
        logger.error("structure_radar_failed", error=str(e)[:120])


async def _run_hlp_tracker():
    """Refresh the HLP vault money view (historical return + drawdown). Slow-moving."""
    import asyncio

    logger.info("scheduled_hlp_tracker")
    try:
        from src.pipeline import hlp_tracker
        state = await asyncio.to_thread(hlp_tracker.run)
        if state.get("available"):
            allt = state["windows"]["allTime"]
            logger.info("hlp_tracker_done", tvl=state["current_tvl_usd"],
                        annualized_pct=allt["annualized_pct"],
                        max_drawdown_pct=allt["max_drawdown_pct"])
        else:
            logger.warning("hlp_tracker_unavailable", reason=state.get("reason"))
    except Exception as e:
        logger.error("hlp_tracker_failed", error=str(e)[:120])


async def _run_board_export():
    """Render regular views without independently scheduled slow or Perps scans."""
    import asyncio

    logger.info("scheduled_board_export")
    from src.pipeline import board_export
    try:
        res = await asyncio.to_thread(
            board_export.run,
            push=True,
            include_operators=False,
            include_opportunities=False,
            include_perps=False,
            include_launch=False,
        )
        logger.info("board_export_done", **res)
    except Exception as e:
        logger.error("board_export_failed", error=str(e)[:120])


async def _run_operator_export():
    """Render operators alone and serialize it with other descriptor-heavy scans."""
    import asyncio

    logger.info("scheduled_operator_export")
    from src.pipeline import board_export
    try:
        async with _heavy_io_lock():
            operators = await asyncio.to_thread(board_export.render_operators)
        paths = await asyncio.to_thread(board_export.write_views, operators=operators)
        pushed = await asyncio.to_thread(board_export.push_to_blob, paths)
        logger.info("operator_export_done", operators=len(operators.get("operators", [])),
                    pushed=pushed)
    except Exception as e:
        logger.error("operator_export_failed", error=str(e)[:120])


async def _run_opportunity_export():
    """Scan smart-money opportunities and update their control scorecard alone."""
    import asyncio

    logger.info("scheduled_opportunity_export")
    from src.pipeline import board_export
    try:
        async with _heavy_io_lock():
            opportunities = await asyncio.to_thread(board_export.render_opportunities)
        stats = await asyncio.to_thread(board_export.render_stats, opportunities)
        paths = await asyncio.to_thread(
            board_export.write_views, opportunities=opportunities, stats=stats)
        pushed = await asyncio.to_thread(board_export.push_to_blob, paths)
        gmgn_health = (opportunities.get("source_health")
                       or opportunities.get("upstream_source_health", {}).get("gmgn", {}))
        fallback_health = opportunities.get("fallback_source_health", {})
        logger.info("opportunity_export_done",
                    opportunities=len(opportunities.get("opportunities", [])), pushed=pushed,
                    gmgn_state=gmgn_health.get("state", "unknown"),
                    gmgn_error=gmgn_health.get("error_kind"),
                    gmgn_chains=(
                        f"{gmgn_health.get('successful_chains', '?')}/"
                        f"{gmgn_health.get('requested_chains', '?')}"),
                    fallback_state=fallback_health.get("state"),
                    fallback_observed=fallback_health.get("observed"),
                    fallback_failed=fallback_health.get("failed"))
    except Exception as e:
        logger.error("opportunity_export_failed", error=str(e)[:120])


async def _run_health_summary():
    logger.info("scheduled_health_summary")
    from src.ops.health import send_health_summary

    await send_health_summary()


async def _run_self_audit():
    """Standing correctness self-audit: run the labeled-case validators INCLUDING the
    live balanceOf ground-truth gate that reconciles holder snapshots against the
    chain. This is the one mechanism that can catch the ghost-holder lie (the data
    source itself lies; no unit test can). It runs here — not in CI — because it
    needs RPC + keys and is rate-limit flaky; a real failure is an operational alarm.

    On failure it sends a DISTINCT alarm, deliberately NOT gated by
    OPERATOR_ALERTS_MUTED and NOT folded into the daily health summary (which is
    designed to read 'healthy'). A silently-broken verdict engine must be loud."""
    import asyncio

    logger.info("scheduled_self_audit")
    from src.backtest import validate_detectors

    # validate_detectors.run() is synchronous and hits the network — run it off the
    # event loop so the 5-min sentinel/scheduler jobs aren't blocked.
    try:
        res = await asyncio.to_thread(validate_detectors.run, True)
    except Exception as e:
        logger.error("self_audit_crashed", error=str(e))
        res = {"passed": 0, "total": 1, "live": True, "live_failed": 1,
               "crash": str(e)[:120]}

    passed, total = res.get("passed", 0), res.get("total", 1)
    live_failed = res.get("live_failed", 0)
    # Two independent failure conditions: the live chain-arbitration gate tripped
    # (ghost data — highest severity), OR overall correctness dropped below a hard
    # floor (a labeled case regressed).
    floor = 0.85
    ok = (live_failed == 0) and (passed / max(total, 1) >= floor)
    logger.info("self_audit_done", passed=passed, total=total,
                live_failed=live_failed, ok=ok)
    if not ok:
        from src.distribution.telegram_sender import send_alert
        crash = res.get("crash")
        msg = (
            "🚨 <b>自检失败 · SELF-AUDIT FAILURE</b>\n"
            "━━━━━━━━━━━━━━\n"
            f"标注案例正确率: <b>{passed}/{total}</b> (下限 {int(floor*100)}%)\n"
            + (f"⚠️ 链上真值网关失败: <b>{live_failed}</b> 处 — 持币快照与链上不符=数据源在撒谎(幽灵余额)\n"
               if live_failed else "")
            + (f"崩溃: {crash}\n" if crash else "")
            + "<i>判决引擎可能在悄悄输出错的东西 — 立刻查,不要相信当前告警</i>"
        )
        try:
            await send_alert(msg)
        except Exception as e:
            logger.error("self_audit_alarm_send_failed", error=str(e))


async def _run_anomaly_screen():
    logger.info("scheduled_anomaly_screen")
    from src.pipeline.anomaly_screener import screen_universe, format_candidates
    from src.distribution.telegram_sender import send_alert

    cands = await asyncio.to_thread(screen_universe)
    if cands:
        await send_alert(format_candidates(cands))
    logger.info("anomaly_screen_done", candidates=len(cands))


async def _run_operator_sentinel():
    logger.info("scheduled_operator_sentinel")
    from src.pipeline.operator_sentinel import run_and_alert

    # run_and_alert offloads its synchronous check_run phase to the bounded shared
    # executor. Serialize that whole operation with accumulation.
    async with _heavy_io_lock():
        await run_and_alert(use_transfers=True)


async def _run_operator_id_push():
    """Daily: run the verified identify_operator on tracked sentinels and PUSH the
    actionable verdicts to Telegram — accumulating/live = pump-watch, distributing/
    exited/rotating = dump-threat. Non-actionable (indeterminate/churn/too_young/
    dormant) are NOT pushed (noise)."""
    logger.info("scheduled_operator_id_push")
    import json

    from src.config import DATA_DIR
    from src.onchain.operator_id import (LONG_ACTIONABLE, SHORT_ACTIONABLE,
                                         identify_operator, promotable)

    # F8: only an ACCUMULATING loaded cluster is a pump warning. loaded_dormant /
    # velocity-unavailable loaded_live are STATE, not signal — pushing them as 拉盘预警
    # was the BASED failure (right label, useless trade). live_operator (hidden-Sybil
    # cluster) stays a watch-level pump line. promotable() also drops `borderline`.
    PUMP = LONG_ACTIONABLE | {"live_operator"}
    DUMP = SHORT_ACTIONABLE
    EVM = {"bsc", "ethereum", "base", "arbitrum"}
    try:
        reg = json.loads((DATA_DIR / "operator_sentinels.json").read_text())
    except Exception:
        return
    lines = []
    for s in reg.values():
        if s.get("chain") not in EVM:
            continue
        try:
            r = identify_operator(s["token"], s["chain"])
        except Exception:
            continue
        v, c = r["verdict"], r["confidence"]
        if not promotable(r):
            continue
        if v in PUMP:
            lines.append(f"🟢 <b>{s['symbol']}</b> 庄在吸筹(拉盘预警) conf{c}\n  {r['evidence'][:70]}")
        elif v in DUMP:
            lines.append(f"🔴 <b>{s['symbol']}</b> 派发/换钱包(砸盘预警) conf{c}\n  {r['evidence'][:70]}")
    from src.pipeline.operator_sentinel import alerts_muted
    if lines and not alerts_muted():
        from src.distribution.telegram_sender import send_alert
        await send_alert("🎯 <b>操盘判决(identify_operator)</b>\n━━━━━━━━━━\n" + "\n\n".join(lines[:12]))
    elif lines:
        logger.info("verdicts_suppressed_unproven", n=len(lines),
                    note="判决已计算未推送:尚未证明有edge")
    logger.info("operator_id_push_done", pushed=len(lines), muted=alerts_muted())


_PERP_RESULT_STATUSES = {
    "verified", "research_only", "blocked", "invalid", "stale", "unavailable",
}
_PERP_REASON_CODE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_PERP_SYMBOL = re.compile(r"^[A-Z0-9]{1,32}$")
_PERP_MAX_UNIVERSE_ROWS = 20_000
_PERP_MAX_MARKET_COUNT = 100_000
_PERP_MAX_SOURCE_COUNT = 64


def _invalid_perp_result_metrics() -> dict[str, object]:
    return {
        "contract_valid": False,
        "status": "invalid",
        "reason_codes": ["universe_result_invalid"],
        "research_mapped": 0,
        "actionable": 0,
        "independent_source_count": 0,
        "observed_path_count": 0,
        "cache_preserved": None,
        "market_count": 0,
    }


def _perp_result_metrics(result: object) -> dict[str, object]:
    """Project a universe envelope into bounded scheduler observability fields."""
    if not isinstance(result, dict):
        return _invalid_perp_result_metrics()

    status = result.get("status")
    if not isinstance(status, str) or status not in _PERP_RESULT_STATUSES:
        return _invalid_perp_result_metrics()

    raw_reasons = result.get("reason_codes")
    if (
        not isinstance(raw_reasons, list)
        or len(raw_reasons) > 8
        or any(
            not isinstance(reason, str)
            or _PERP_REASON_CODE.fullmatch(reason) is None
            for reason in raw_reasons
        )
    ):
        return _invalid_perp_result_metrics()

    research = result.get("research_universe")
    actionable = result.get("actionable_universe")
    for universe in (research, actionable):
        if (
            not isinstance(universe, dict)
            or len(universe) > _PERP_MAX_UNIVERSE_ROWS
            or any(
                not isinstance(symbol, str)
                or _PERP_SYMBOL.fullmatch(symbol) is None
                or not isinstance(row, dict)
                for symbol, row in universe.items()
            )
        ):
            return _invalid_perp_result_metrics()

    if any(
        row.get("actionability") != "research_only"
        for row in research.values()
    ):
        return _invalid_perp_result_metrics()
    if status == "research_only" and not research:
        return _invalid_perp_result_metrics()

    from src.pipeline.perp_scanner import validated_verified_universe

    if status == "verified":
        if not actionable or validated_verified_universe(actionable) is None:
            return _invalid_perp_result_metrics()
    elif actionable:
        return _invalid_perp_result_metrics()

    counts = result.get("source_counts", {})
    if not isinstance(counts, dict):
        return _invalid_perp_result_metrics()

    def _count(name: str) -> int | None:
        value = counts.get(name)
        if value is None:
            return 0
        if (
            type(value) is not int
            or value < 0
            or value > _PERP_MAX_SOURCE_COUNT
        ):
            return None
        return value

    independent_source_count = _count("independent_source_count")
    observed_path_count = _count("observed_path_count")
    if independent_source_count is None or observed_path_count is None:
        return _invalid_perp_result_metrics()

    cache_preserved = result.get("cache_preserved", None)
    if cache_preserved is not None and type(cache_preserved) is not bool:
        return _invalid_perp_result_metrics()
    market_count = result.get("market_count", 0)
    if (
        type(market_count) is not int
        or market_count < 0
        or market_count > _PERP_MAX_MARKET_COUNT
    ):
        return _invalid_perp_result_metrics()
    if "refresh_status" in result:
        raw_refresh_status = result["refresh_status"]
        if (
            type(raw_refresh_status) is not str
            or raw_refresh_status not in {"written", "unchanged"}
        ):
            return _invalid_perp_result_metrics()
        if raw_refresh_status == "unchanged" and (
            status not in {"research_only", "verified", "stale"}
            or cache_preserved is not True
        ):
            return _invalid_perp_result_metrics()

    return {
        "contract_valid": True,
        "status": status,
        "reason_codes": list(raw_reasons),
        "research_mapped": len(research),
        "actionable": len(actionable),
        "independent_source_count": independent_source_count,
        "observed_path_count": observed_path_count,
        "cache_preserved": cache_preserved,
        "market_count": market_count,
    }


def _load_verified_perp_universe() -> tuple[dict, dict[str, object]]:
    """Load one actionable snapshot for an entire job, or fail closed."""
    from src.onchain.perp_universe import load_result

    try:
        result = load_result()
    except Exception as exc:
        # Never log exception text: transport exceptions can embed full URLs.
        logger.warning(
            "perp_universe_runtime_load_failed",
            error_kind=type(exc).__name__,
        )
        result = {
            "status": "unavailable",
            "reason_codes": ["universe_load_failed"],
            "research_universe": {},
            "actionable_universe": {},
            "source_counts": {},
        }
    metrics = _perp_result_metrics(result)
    actionable = result.get("actionable_universe") if isinstance(result, dict) else None
    from src.pipeline.perp_scanner import validated_verified_universe

    validated = validated_verified_universe(actionable)
    verified = validated if (
        metrics["contract_valid"]
        and metrics["status"] == "verified"
        and validated
    ) else {}
    metadata = {
        "universe_contract_valid": metrics["contract_valid"],
        "universe_status": metrics["status"],
        "universe_reason_codes": metrics["reason_codes"],
        "research_mapped": metrics["research_mapped"],
        "actionable": len(verified),
        "independent_source_count": metrics["independent_source_count"],
        "observed_path_count": metrics["observed_path_count"],
        "cache_preserved": metrics["cache_preserved"],
    }
    return verified, metadata


def _blocked_perp_job(job_id: str, metadata: dict[str, object]) -> dict[str, object]:
    outcome = {
        "status": "blocked",
        "job_id": job_id,
        "block_reason": "no_verified_actionable_universe",
        **metadata,
    }
    logger.warning(
        f"{job_id}_blocked",
        block_reason=outcome["block_reason"],
        **metadata,
    )
    return outcome


async def _run_holder_snapshots():
    """Daily: snapshot top holders of every tracked token (sentinels + perp universe)
    into holder_snapshots.db. Builds OUR OWN history so 'who held big before a run
    and dumped into it' becomes a LOCAL before/after diff — Dune-independent
    exited-operator detection for everything we start watching now."""
    logger.info("scheduled_holder_snapshots")
    import json

    from src.config import DATA_DIR
    from src.onchain.holder_snapshot import fetch_holders_evm, save_snapshot

    _CID = {"bsc": 56, "ethereum": 1, "base": 8453, "arbitrum": 42161}
    targets: dict[tuple, None] = {}
    try:
        reg = json.loads((DATA_DIR / "operator_sentinels.json").read_text())
        for s in reg.values():
            if s.get("chain") in _CID:
                targets[(s["token"].lower(), s["chain"])] = None
    except Exception:
        pass
    sentinel_targets = len(targets)

    verified_universe, universe_meta = _load_verified_perp_universe()
    perp_target_keys = {
        (rec["address"], rec["chain"])
        for rec in verified_universe.values()
        if rec["chain"] in _CID
    }
    if verified_universe:
        for key in perp_target_keys:
            targets[key] = None
        perp_status = "ready"
    else:
        perp_status = "blocked"
        logger.warning(
            "holder_snapshots_perp_blocked",
            block_reason="no_verified_actionable_universe",
            **universe_meta,
        )

    saved = 0
    for (tok, chain) in targets:
        try:
            h = fetch_holders_evm(tok, chain_id=_CID[chain], max_pages=4)
            if h:
                save_snapshot(tok, chain, h)
                saved += 1
        except Exception as exc:
            logger.debug(
                "snapshot_failed",
                token=tok,
                reason_code="holder_snapshot_failed",
                error_kind=type(exc).__name__,
            )
    outcome = {
        "status": "partial" if perp_status == "blocked" else "complete",
        "perp_status": perp_status,
        "tokens": len(targets),
        "sentinel_targets": sentinel_targets,
        "perp_targets": len(perp_target_keys),
        "saved": saved,
        **universe_meta,
    }
    logger.info("holder_snapshots_done", **outcome)
    return outcome


async def _run_perp_cex_scan():
    """Daily: dump-precursor scan over the perp (shortable) universe — top holders
    moving to CEX deposits. Runs on our own validated infra (holders + cex_flow),
    not a third-party feed. ~60 min for the full EVM set; quiet baseline is normal."""
    logger.info("scheduled_perp_cex_scan")
    verified_universe, universe_meta = _load_verified_perp_universe()
    if not verified_universe:
        return _blocked_perp_job("perp_cex_scan", universe_meta)

    from src.onchain.operator_id import _token_market
    from src.pipeline.operator_sentinel import alerts_muted
    from src.pipeline.outcome_tracker import log_alert
    from src.pipeline.perp_scanner import scan_cex_deposits

    # This is a roughly hour-long synchronous network sweep. Running it directly in
    # an ``async def`` freezes the scheduler loop, so even the independent five-minute
    # Carry export cannot start. Keep it inside the shared bounded executor instead.
    hits = await asyncio.to_thread(
        scan_cex_deposits,
        verified_universe=verified_universe,
    )

    # RECORD EVERY EVENT, ALWAYS. This scan used to push to Telegram and store
    # nothing — so the perp short thesis could never accumulate the ~120-150
    # independent events needed to test it. Recording is the whole strategy; the
    # push is optional and currently muted.
    logged = 0
    for h in hits:
        try:
            mkt = await asyncio.to_thread(_token_market, h["address"])
            px = float(mkt.get("price_usd") or 0) if mkt.get("available") else 0.0
            liq = float(mkt.get("liquidity_usd") or 0) if mkt.get("available") else 0.0
            if not px:
                logger.warning("perp_event_unpriced", symbol=h["symbol"],
                               note="无价格 → 该事件无法结算,仍记录但不可打分")
            log_alert(h["address"], h["chain"], h["symbol"], "CEX充值前兆",
                      "short", px, liq, phase="arm")
            logged += 1
        except Exception as exc:
            logger.warning("perp_event_log_failed", symbol=h.get("symbol"),
                           reason_code="event_persistence_failed",
                           error_kind=type(exc).__name__)

    if hits and not alerts_muted():
        from src.distribution.telegram_sender import send_alert

        msg = "📉 <b>做空前兆 — 永续币大户→CEX充值</b>\n━━━━━━━━━━\n"
        for h in hits[:10]:
            pct = ("%.1f%%" % h["pct_of_cluster"]) if h.get("pct_of_cluster") else "?"
            msg += (f"<b>{h['symbol']}</b> [{h['chain']}] 大户向交易所充值 "
                    f"{(h.get('cex_outflow') or 0):,.0f} (持仓{pct})\n"
                    f"→ 砸盘前兆,考虑做空(带止损,仅信号)\n\n")
        await send_alert(msg)
    elif hits:
        logger.info("perp_alerts_suppressed_unproven", n=len(hits))
    outcome = {
        "status": "complete",
        "hits": len(hits),
        "logged": logged,
        **universe_meta,
    }
    logger.info("perp_cex_scan_done", **outcome)
    return outcome


async def _run_yaobi_finder():
    """The 妖币 finder — the repo's actual goal: FIND operator tokens, early. Wide scan
    of the young-token accumulation window for verified real operators, persisted to a
    ranked watchlist and forward-recorded. Records always; pushes the strongest (young
    + verified-bought) when unmuted. It finds real operators — it does not claim which
    pump; timing is the user's call on a clean list."""
    logger.info("scheduled_yaobi_finder")
    from src.pipeline.operator_sentinel import alerts_muted
    from src.pipeline.outcome_tracker import log_alert
    from src.pipeline.yaobi_finder import scan, watchlist

    try:
        finds = scan(chains=("bsc", "base"), pages=3, max_analyze=60)
    except Exception as e:
        logger.warning("yaobi_finder_failed", error=str(e)[:80])
        return
    logged = 0
    for r in finds:
        d = r.get("direction")
        if r.get("price") and d in ("long", "short"):
            try:
                kind = "妖币·会涨(健康+聪明钱)" if d == "long" else "妖币·会砸(操盘装弹)"
                log_alert(r["address"], r["chain"], r["symbol"], kind, d,
                          r["price"], r.get("liq") or 0,
                          phase="accumulate" if d == "long" else "sell")
                logged += 1
            except Exception:
                pass
    longs = [r for r in finds if r.get("direction") == "long"]
    shorts = [r for r in finds if r.get("direction") == "short"]
    if (longs or shorts) and not alerts_muted():
        from src.distribution.telegram_sender import send_alert
        msg = "🎯 <b>妖币发现(双向)</b>\n━━━━━━━━━━\n"
        for r in longs[:6]:
            msg += f"🟢 <b>{r['symbol']}</b> [{r['chain']}] 龄{r['age_days']}d {r.get('signals','')}\n"
        for r in shorts[:6]:
            msg += f"🔴 <b>{r['symbol']}</b> [{r['chain']}] 龄{r['age_days']}d {r.get('signals','')}\n"
        msg += "会涨=健康+聪明钱;会砸=操盘装弹。已核实,非保证,仓位自负。"
        await send_alert(msg)
    logger.info("yaobi_finder_done", found=len(finds), longs=len(longs),
                shorts=len(shorts), logged=logged, watchlist_total=len(watchlist()))


async def _run_early_accumulation():
    """The early-capture experiment, forward-tracked. Scans fresh launches, keeps only
    VERIFIED-BOUGHT coordinated clusters on young tradeable coins, and RECORDS each as
    a long candidate so its forward return accrues against a base rate. This is the one
    offense angle that survived the night's falsification — a leading event on coins we
    can trade, verified real, not the standing verdict that carries no timing.

    Records always (recording is the experiment); pushes only when unmuted. Whether the
    signature predicts pumps is UNMEASURED — the kill-line decides, not faith."""
    logger.info("scheduled_early_accumulation")
    from src.pipeline.operator_hunt import early_accumulation_candidates, hunt
    from src.pipeline.operator_sentinel import alerts_muted
    from src.pipeline.outcome_tracker import log_alert

    try:
        suspects = await asyncio.to_thread(hunt, per_chain=40, max_scan=50)
    except Exception as e:
        logger.warning("early_accumulation_hunt_failed", error=str(e)[:80])
        return
    cands = await asyncio.to_thread(early_accumulation_candidates, suspects)

    # SMART-MONEY CONVERGENCE — the second, independent leading signal, on the SAME
    # fresh-token shortlist. Built on realized PnL (unfakeable), so it corroborates the
    # structural accumulation signal from a different axis. Only checked on the already-
    # shortlisted young tokens (the profitability lookups are expensive).
    from src.onchain.smart_money import convergence
    logged = 0
    for c in cands:
        if not c.get("price0"):
            logger.warning("early_cand_unpriced", symbol=c.get("symbol"))
            continue
        kind = "早期操盘吸筹"
        try:
            conv = await asyncio.to_thread(
                convergence, c["address"], c["chain"], max_check=15,
            )
            c["smart_money"] = conv.get("verdict")
            if conv.get("verdict") == "convergence":
                kind = "早期吸筹+聪明钱收敛"     # both signals agree — strongest form
        except Exception:
            pass
        try:
            await asyncio.to_thread(
                log_alert, c["address"], c["chain"], c.get("symbol", "?"),
                kind, "long", c["price0"], c.get("liquidity") or 0,
                phase="accumulate",
            )
            logged += 1
        except Exception:
            pass
    if cands and not await asyncio.to_thread(alerts_muted):
        from src.distribution.telegram_sender import send_alert
        msg = "🟢 <b>早期操盘吸筹候选(前向实验)</b>\n━━━━━━━━━━\n"
        for c in cands[:8]:
            msg += (f"<b>{c['symbol']}</b> [{c['chain']}] 龄{c.get('age_days')}d "
                    f"簇持{c.get('largest_entity_pct'):.0f}% 从市场买入\n")
        msg += "⚠️ 择时未验证、先验低、可能逆向选择。研究彩票,非买入建议。"
        await send_alert(msg)
    logger.info("early_accumulation_done", suspects=len(suspects),
                candidates=len(cands), logged=logged)


async def _run_perp_mobilization():
    """Widen the event surface on SHORTABLE coins — the only universe where a short
    edge can be monetised. Records router-approval / gas-topup / LP-unlock moments
    for the 190 perp coins so the kill-line thesis can accrue evidence at all.

    Escalation events (phase='arm'), not entry signals: they show capability and
    preparation, never intent or timing. Recorded always; pushed only when unmuted."""
    logger.info("scheduled_perp_mobilization")
    verified_universe, universe_meta = _load_verified_perp_universe()
    if not verified_universe:
        return _blocked_perp_job("perp_mobilization", universe_meta)

    import json

    from src.config import DATA_DIR
    from src.onchain.operator_id import _token_market
    from src.pipeline.operator_sentinel import alerts_muted
    from src.pipeline.outcome_tracker import log_alert
    from src.pipeline.perp_mobilization import scan_lp_unlock, scan_mobilization

    state_file = DATA_DIR / "perp_mobilization_state.json"
    try:
        state = json.loads(state_file.read_text())
    except Exception:
        state = {"mobil": {}, "lp": {}}

    events, state["mobil"] = scan_mobilization(
        prev_state=state.get("mobil"),
        verified_universe=verified_universe,
    )
    lp_events, state["lp"] = scan_lp_unlock(
        prev_state=state.get("lp"),
        verified_universe=verified_universe,
    )
    events += lp_events
    state_file.parent.mkdir(parents=True, exist_ok=True)
    state_file.write_text(json.dumps(state))

    logged = 0
    for e in events:
        try:
            mkt = _token_market(e["address"])
            px = float(mkt.get("price_usd") or 0) if mkt.get("available") else 0.0
            liq = float(mkt.get("liquidity_usd") or 0) if mkt.get("available") else 0.0
            log_alert(e["address"], e["chain"], e["symbol"], e["kind"],
                      "short", px, liq, phase="arm")
            logged += 1
        except Exception as exc:
            logger.warning("perp_mobil_log_failed", symbol=e.get("symbol"),
                           reason_code="event_persistence_failed",
                           error_kind=type(exc).__name__)

    if events and not alerts_muted():
        from src.distribution.telegram_sender import send_alert
        msg = "🟠 <b>戒备 — 永续币大户砸盘前置动作</b>\n━━━━━━━━━━\n"
        for e in events[:10]:
            msg += f"<b>{e['symbol']}</b> [{e['chain']}] {e['kind']}\n  {e['detail']}\n\n"
        msg += "⚠️ 前置动作 ≠ 必砸。减仓/收紧止损,勿据此开仓。"
        await send_alert(msg)
    elif events:
        logger.info("perp_mobil_suppressed_unproven", n=len(events))
    outcome = {
        "status": "complete",
        "events": len(events),
        "logged": logged,
        **universe_meta,
    }
    logger.info("perp_mobilization_done", **outcome)
    return outcome


async def _run_perp_universe_refresh():
    """Daily: refresh before the 26-hour evidence TTL can expire."""
    logger.info("scheduled_perp_universe_refresh")
    from src.onchain.perp_universe import refresh_result

    try:
        result = await asyncio.to_thread(refresh_result)
    except Exception as exc:
        # As with the runtime gate, exception values can contain sensitive URLs.
        logger.warning(
            "perp_universe_refresh_failed",
            error_kind=type(exc).__name__,
            status="unavailable",
            reason_codes=["refresh_result_failed"],
            research_mapped=0,
            actionable=0,
            independent_source_count=0,
            observed_path_count=0,
            cache_preserved=None,
            market_count=0,
        )
        return {
            "contract_valid": False,
            "status": "unavailable",
            "reason_codes": ["refresh_result_failed"],
            "research_mapped": 0,
            "actionable": 0,
            "independent_source_count": 0,
            "observed_path_count": 0,
            "cache_preserved": None,
            "market_count": 0,
            "refresh_status": None,
        }

    metrics = _perp_result_metrics(result)
    raw_refresh_status = (
        result.get("refresh_status") if isinstance(result, dict) else None
    )
    refresh_status = None
    if metrics["contract_valid"]:
        if (
            raw_refresh_status == "written"
            and metrics["status"] in {"research_only", "verified"}
        ):
            refresh_status = "written"
        elif raw_refresh_status == "unchanged":
            refresh_status = "unchanged"
    outcome = {
        **metrics,
        "refresh_status": refresh_status,
    }
    log_fields = {
        "status": metrics["status"],
        "refresh_status": refresh_status,
        "reason_codes": metrics["reason_codes"],
        "research_mapped": metrics["research_mapped"],
        "actionable": metrics["actionable"],
        "independent_source_count": metrics["independent_source_count"],
        "observed_path_count": metrics["observed_path_count"],
        "cache_preserved": metrics["cache_preserved"],
        "market_count": metrics["market_count"],
    }
    if refresh_status == "written" and metrics["status"] in {
        "research_only", "verified",
    }:
        logger.info("perp_universe_refresh_written", **log_fields)
    elif refresh_status == "unchanged":
        log_method = (
            logger.info
            if metrics["status"] in {"research_only", "verified"}
            else logger.warning
        )
        log_method("perp_universe_refresh_unchanged", **log_fields)
    else:
        logger.warning("perp_universe_refresh_failed", **log_fields)
    return outcome


async def _run_cluster_coverage():
    """Weekly: Dune-reconstruct each BSC sentinel's holder list; alert on untracked
    big EOAs (the ESPORTS-20%-whale class) and cross-check drift (reflection)."""
    logger.info("scheduled_cluster_coverage")
    from src.ops.cluster_coverage import run_audit

    results = run_audit()
    problems = [r for r in results if not r["verified"] or r["blind"] or
                (r["drift_pct"] is not None and r["drift_pct"] > 2)]
    if problems:
        from src.distribution.telegram_sender import send_alert

        msg = "🔍 <b>簇覆盖周检 — 需要处理</b>\n━━━━━━━━━━\n"
        for r in problems[:8]:
            if not r["verified"]:
                msg += f"<b>{r['symbol']}</b>: Dune反推失败 — 完整性未验证\n"
                continue
            if r["drift_pct"] is not None and r["drift_pct"] > 2:
                msg += f"<b>{r['symbol']}</b>: 推算vs链上偏差 {r['drift_pct']}%(反射/税币?)\n"
            for b in r["blind"][:4]:
                msg += (f"<b>{r['symbol']}</b> 未盯大额EOA {b['share']}%: "
                        f"<code>{b['address']}</code>\n")
        await send_alert(msg)
    logger.info("cluster_coverage_done", tokens=len(results), problems=len(problems))


async def _run_cex_label_refresh():
    """Weekly: pull the full verified BSC exchange-label set from Dune into
    data/cex_labels_bsc.json (offline-first cache merged by evm_exchanges)."""
    logger.info("scheduled_cex_label_refresh")
    import json

    from src.config import DATA_DIR
    from src.onchain.dune_client import bsc_cex_addresses

    labels = bsc_cex_addresses()
    if labels:                       # failure ≠ empty: never overwrite with nothing
        (DATA_DIR / "cex_labels_bsc.json").write_text(json.dumps(labels, ensure_ascii=False))
        logger.info("cex_labels_refreshed", count=len(labels))
    else:
        logger.warning("cex_label_refresh_failed", note="kept previous cache")


async def _run_label_verify():
    """Daily contamination sweep: any address the system TRUSTS as an operator
    entity that carries an exchange/bridge label = poisoned entity model (the
    Gate.io-in-SIREN-cluster class of error, institutionalized as a check)."""
    logger.info("scheduled_label_verify")
    from src.onchain.label_verify import sweep

    res = sweep()
    if res["hits"]:
        from src.distribution.telegram_sender import send_alert

        msg = "🧪 <b>实体污染告警 — 受信地址带交易所/桥标签</b>\n━━━━━━━━━━\n"
        for h in res["hits"][:10]:
            msg += (f"<code>{h['address']}</code>\n"
                    f"  身份: {h['role']} → 实为 <b>{h['label']}</b> ({h['source']})\n"
                    f"  → 从簇/白名单中剔除并重审相关结论\n\n")
        await send_alert(msg)
    logger.info("label_verify_done", checked=res["checked"],
                hits=len(res["hits"]), complete=res["complete"])


async def _run_funder_watch():
    """Watch the multi-token operator family's FUNDER (0x6596da8b…). A new fundee =
    the 庄 seeding a fresh shell wallet — the earliest possible launch signal."""
    logger.info("scheduled_funder_watch")
    from src.onchain.funder_watch import check_new_fundees

    res = check_new_fundees()
    shells = res.get("shell_candidates", [])
    new_f = res.get("new_fundees", [])
    # Alert ONLY on convergence (>=2 fresh wallets on one new token) — the real
    # shell signal. Bare new fundees are noisy at this funder's high fan-out.
    if shells:
        from src.distribution.telegram_sender import send_alert

        msg = "🚨 <b>多币庄疑似开新壳(钱包收敛)</b>\n━━━━━━━━━━\n"
        for s in shells[:8]:
            msg += (f"<b>{s['label']}</b>\n"
                    f"{len(set(s['holders']))} 个新钱包同时吸 <b>{s['symbol']}</b>\n"
                    f"<code>{s['token']}</code>\n"
                    f"→ 疑似下一个壳正在装弹,深查持币/funder\n\n")
        await send_alert(msg)
    logger.info("funder_watch_done", new_fundees=len(new_f), shell_candidates=len(shells))


async def _run_second_leg_assess():
    logger.info("scheduled_second_leg_assess")
    from src.pipeline.operator_sentinel import assess_second_leg

    assess_second_leg()


async def _run_resolve_outcomes():
    logger.info("scheduled_resolve_outcomes")
    import asyncio

    from src.pipeline.outcome_tracker import resolve_outcomes
    from src.pipeline.opportunity_outcomes import resolve as resolve_opportunities

    # Canonical five-lane evidence is small and directly gates the product. Run it
    # before the legacy resolver, which can spend tens of minutes on historical
    # controls, then publish its scorecard immediately instead of waiting for :50.
    opportunities = await asyncio.to_thread(resolve_opportunities)
    from src.pipeline import board_export
    stats = await asyncio.to_thread(board_export.render_stats, None)
    paths = await asyncio.to_thread(board_export.write_views, stats=stats)
    pushed = await asyncio.to_thread(board_export.push_to_blob, paths)
    # Both resolvers perform synchronous historical-price I/O. Keep them off the
    # event loop and sequential inside the bounded executor so they cannot reopen
    # the old FD leak.
    alerts = await asyncio.to_thread(resolve_outcomes)
    logger.info("scheduled_resolve_outcomes_done", alerts=alerts,
                stats_pushed=pushed, **opportunities)


async def _run_majors_monitor():
    logger.info("scheduled_majors_monitor")
    from src.pipeline.majors_monitor import run_and_alert

    await run_and_alert()


async def _run_operator_hunt():
    logger.info("scheduled_operator_hunt")
    from src.pipeline.operator_hunt import auto_promote, hunt
    from src.distribution.telegram_sender import send_alert

    # Both stages perform synchronous HTTP/RPC and SQLite work.  This is an async
    # APScheduler job, so running either inline would stall every other scheduled
    # coroutine until the operator scan finishes.  Keep the stages sequential (the
    # promotion consumes this scan's suspects) but offload them to the scheduler's
    # bounded shared executor.
    suspects = await asyncio.to_thread(hunt)
    # STRICT auto-promotion: any suspect clearing EVERY hard gate (age>=14d, focused
    # funder, mostly-EOA cluster, non-anon identity, proven 聪明庄 history) is
    # registered as a sentinel automatically — closes the manual hunt→sentinel gap
    # without re-admitting the MAME class.
    try:
        promoted = await asyncio.to_thread(auto_promote, suspects)
        if promoted:
            pm = "🤖 <b>自动晋升哨兵(过全部硬门)</b>\n━━━━━━━━━━\n" + "\n".join(
                f"<b>{p['symbol']}</b> [{p['chain']}] {p['wallets']}钱包 · {p['age_days']:.0f}天"
                for p in promoted)
            await send_alert(pm)
    except Exception as e:
        logger.debug("auto_promote_job_failed", error=str(e)[:80])
    # Only alert on real hidden-Sybil operators (隐藏簇) or mixed — not single-wallet
    # team/treasury concentration, and never pegs/majors (filtered upstream).
    strong = [s for s in suspects if s.get("funder_complete")
              and s.get("shape") in ("隐藏簇", "混合")]
    if strong:
        msg = "🎯 <b>操作者猎手 — 隐藏控盘嫌疑</b>\n━━━━━━━━━━\n"
        for s in strong[:8]:
            conf = s.get("cluster_confidence")
            conf_str = f" 置信{conf}" if conf is not None else ""
            msg += (f"<b>{s['symbol']}</b> [{s['chain']}] {s.get('shape')}{conf_str} "
                    f"实体{s['largest_entity_pct']:.0f}%供应 缺口{s['concentration_gap']:+.0f}\n"
                    f"<code>{s['address']}</code>\n")
        await send_alert(msg)
    logger.info("operator_hunt_done", scanned=len(suspects), strong=len(strong))


async def _run_holder_growth_screen():
    logger.info("scheduled_holder_growth_screen")
    from src.pipeline.holder_growth_screener import screen_holder_growth, confirm_operators
    from src.distribution.telegram_sender import send_alert

    cands = screen_holder_growth()
    confirmed = confirm_operators(cands, top=8)
    if confirmed:
        msg = "📈 <b>持币集中度趋势 — 浮筹被吸(非trending)</b>\n━━━━━━━━━━\n"
        for c in confirmed[:6]:
            msg += (f"<b>{c['chain']}</b> 实体{c.get('largest_entity_pct',0):.0f}%供应 "
                    f"缺口{c.get('concentration_gap',0):+.0f} top10+{c['top10_delta']}pp\n"
                    f"<code>{c['token']}</code>\n")
        await send_alert(msg)
    logger.info("holder_growth_done", candidates=len(cands), confirmed=len(confirmed))


async def _run_tier1_reports():
    """Collect Tier 1 platform reports every 30 min."""
    logger.info("collecting_tier1_reports")
    from src.collectors.platform_reports import Tier1ReportCollector
    from src.analysis.priority_scorer import score_and_rank

    collector = Tier1ReportCollector()
    result = await collector.collect()
    if result.items:
        scored = score_and_rank(result.items)
        high_priority = [(item, score) for item, score in scored if score > 60]
        if high_priority:
            logger.info("tier1_high_priority_reports", count=len(high_priority))
            from src.distribution.telegram_sender import send_thread_for_review
            for item, score in high_priority[:3]:
                await send_thread_for_review(
                    topic=item.title,
                    priority_score=score,
                    sources_used=[item.url] if item.url else [],
                    thread_text_en=f"📊 {item.title}\n\n{item.content[:500]}",
                    thread_text_zh=f"📊 {item.title}\n\n{item.content[:500]}",
                )


async def _run_tier2_reports():
    """Collect Tier 2 platform reports every 2 hours."""
    logger.info("collecting_tier2_reports")
    from src.collectors.platform_reports import PlatformReportCollector

    collector = PlatformReportCollector(tiers=["tier2"])
    result = await collector.collect()
    logger.info("tier2_reports_collected", items=len(result.items))


async def _run_tier3plus_reports():
    """Collect Tier 3-6 platform reports every 6 hours."""
    logger.info("collecting_tier3plus_reports")
    from src.collectors.platform_reports import PlatformReportCollector

    collector = PlatformReportCollector(tiers=["tier3", "tier4", "tier5", "tier6"])
    result = await collector.collect()
    logger.info("tier3plus_reports_collected", items=len(result.items))


async def _check_glassnode_weekly():
    """Check for Glassnode's 'The Week On-Chain' report. Auto-draft if found."""
    logger.info("checking_glassnode_weekly")
    from src.collectors.platform_reports import PlatformReportCollector

    collector = PlatformReportCollector(tiers=["tier1"])
    result = await collector.collect()
    for item in result.items:
        if "glassnode" in item.metadata.get("platform", "") and "week" in item.title.lower():
            logger.info("glassnode_weekly_found", title=item.title)
            from src.distribution.telegram_sender import send_alert
            await send_alert(
                f"Glassnode Weekly Report Available\n\n"
                f"Title: {item.title}\n"
                f"URL: {item.url or 'N/A'}\n"
                f"Summary: {item.content[:300]}"
            )
            break


async def _check_coinshares_weekly():
    """Check for CoinShares weekly fund flows report. Must-draft content."""
    logger.info("checking_coinshares_weekly")
    from src.collectors.platform_reports import PlatformReportCollector

    collector = PlatformReportCollector(tiers=["tier5"])
    result = await collector.collect()
    for item in result.items:
        if "coinshares" in item.metadata.get("platform", "") and "flow" in item.title.lower():
            logger.info("coinshares_weekly_found", title=item.title)
            from src.distribution.telegram_sender import send_alert
            await send_alert(
                f"CoinShares Weekly Fund Flows Report\n\n"
                f"Title: {item.title}\n"
                f"URL: {item.url or 'N/A'}\n"
                f"Summary: {item.content[:300]}"
            )
            break


async def _check_coinmetrics_weekly():
    """Check for Coin Metrics 'State of the Network' report."""
    logger.info("checking_coinmetrics_weekly")
    from src.collectors.platform_reports import PlatformReportCollector

    collector = PlatformReportCollector(tiers=["tier2"])
    result = await collector.collect()
    for item in result.items:
        if "coin_metrics" in item.metadata.get("platform", "") and "state" in item.title.lower():
            logger.info("coinmetrics_sotn_found", title=item.title)
            from src.distribution.telegram_sender import send_alert
            await send_alert(
                f"Coin Metrics State of the Network Report\n\n"
                f"Title: {item.title}\n"
                f"URL: {item.url or 'N/A'}\n"
                f"Summary: {item.content[:300]}"
            )
            break


async def _run_daily_macro():
    """Daily macro snapshot: FRED data + Net Liquidity calculation."""
    logger.info("daily_macro_snapshot")
    try:
        from src.collectors.macro_economic import FREDCollector

        collector = FREDCollector()
        result = await collector.collect()
        logger.info("macro_data_collected", items=len(result.items))

        # Check for net liquidity calculation
        for item in result.items:
            if item.metadata.get("data_type") == "net_liquidity":
                logger.info("net_liquidity_calculated", value=item.metadata.get("value"))
    except Exception as e:
        logger.warning("macro_snapshot_failed", error=str(e))


async def _run_regulatory_scan():
    """Scan regulatory and political news sources."""
    logger.info("regulatory_scan")
    from src.collectors.political_regulatory import PoliticalRegulatoryCollector
    from src.analysis.priority_scorer import score_and_rank

    collector = PoliticalRegulatoryCollector()
    result = await collector.collect()
    if result.items:
        scored = score_and_rank(result.items)
        critical = [(item, score) for item, score in scored if score > 65]
        if critical:
            logger.warning("critical_regulatory_news", count=len(critical))
            from src.distribution.telegram_sender import send_alert
            for item, score in critical[:5]:
                await send_alert(
                    f"Critical Regulatory News (Score: {score:.0f})\n\n"
                    f"Title: {item.title}\n"
                    f"URL: {item.url or 'N/A'}\n"
                    f"Summary: {item.content[:300]}"
                )


async def _run_github_star_check():
    """Hourly GitHub star count collection."""
    logger.info("github_star_check")
    try:
        from src.collectors.github_star_velocity import GitHubStarVelocityCollector

        detector = GitHubStarVelocityCollector()
        result = await detector.collect()
        # Check for anomalies
        alerts = [i for i in result.items if i.metadata.get("alert_level") in ("HIGH", "MEDIUM")]
        if alerts:
            logger.warning("github_star_alerts", count=len(alerts))
            for a in alerts[:3]:
                logger.info("star_alert", repo=a.metadata.get("repo"), level=a.metadata.get("alert_level"))
    except ImportError:
        pass  # Star velocity detector not yet enabled
    except Exception as e:
        logger.debug("github_star_check_error", error=str(e))


async def _run_github_discovery():
    """Discover new trending crypto repos."""
    logger.info("github_discovery_scan")
    try:
        from src.collectors.github_star_velocity import GitHubStarVelocityCollector

        detector = GitHubStarVelocityCollector()
        new_repos = await detector.search_new_repos()
        trending = await detector.scan_trending()
        logger.info("github_discovery_complete", new_repos=len(new_repos), trending=len(trending))
    except Exception as e:
        logger.debug("github_discovery_error", error=str(e))


async def _run_github_weekly_digest():
    """Generate weekly GitHub star digest."""
    logger.info("github_weekly_digest")
    try:
        from src.collectors.github_star_velocity import GitHubStarVelocityCollector
        from src.distribution.telegram_sender import send_alert

        detector = GitHubStarVelocityCollector()
        result = await detector.collect()
        if result.items:
            # Compile digest from collected items
            lines = ["GitHub Weekly Star Digest\n"]
            for item in result.items[:15]:
                repo = item.metadata.get("repo", item.title)
                stars = item.metadata.get("stars_delta", "N/A")
                level = item.metadata.get("alert_level", "")
                lines.append(f"  - {repo}: +{stars} stars {f'[{level}]' if level else ''}")
            summary = "\n".join(lines)
            await send_alert(summary)
            logger.info("github_weekly_digest_sent", items=len(result.items))
        else:
            logger.info("github_weekly_digest_empty")
    except Exception as e:
        logger.debug("github_weekly_digest_error", error=str(e))


async def main():
    """Run the scheduler."""
    # Load .env so credentials (TELEGRAM_BOT_TOKEN, TG_REVIEW_CHANNEL, API keys)
    # are available when run as a long-lived process (e.g. under launchd/Docker).
    # Load from the project root explicitly so it works regardless of cwd.
    try:
        from dotenv import load_dotenv

        from src.config import PROJECT_ROOT

        load_dotenv(PROJECT_ROOT / ".env")
    except ImportError:
        pass

    _configure_runtime_logging()
    nofile_soft, nofile_hard = _configure_fd_limit()

    # All ``asyncio.to_thread`` calls share this executor.  Keep it bounded so
    # concurrent recurring scans cannot exhaust sockets/files/SQLite descriptors.
    # APScheduler itself still executes async job coroutines on the event loop.
    executor = ThreadPoolExecutor(
        max_workers=_scheduler_worker_count(), thread_name_prefix="cryptoscope-io"
    )
    asyncio.get_running_loop().set_default_executor(executor)

    logger.info("starting_scheduler", io_workers=_scheduler_worker_count(),
                log_level=logging.getLevelName(_scheduler_log_level()),
                nofile_soft=nofile_soft, nofile_hard=nofile_hard)
    scheduler = create_scheduler()
    scheduler.start()

    # Keep running
    try:
        while True:
            await asyncio.sleep(3600)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        logger.info("scheduler_stopped")
    finally:
        executor.shutdown(wait=False, cancel_futures=True)


if __name__ == "__main__":
    asyncio.run(main())
