"""APScheduler-based task scheduling for all pipelines."""

from __future__ import annotations

import asyncio

import structlog
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from src.config import load_settings

logger = structlog.get_logger()


def create_scheduler() -> AsyncIOScheduler:
    """Create and configure the scheduler with all pipeline jobs."""
    settings = load_settings()
    scheduler = AsyncIOScheduler()

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

    # Anomaly candidate screen → push suspected-accumulation coins every 6h
    scheduler.add_job(
        _run_anomaly_screen,
        CronTrigger(minute=30, hour="*/6"),
        id="anomaly_screen",
        name="疑似吸筹候选筛选 (market footprint → Telegram)",
    )

    # Operator sentinel → watch confirmed clusters (BASED…) for distribute/rug/
    # launch every 15 min. Free (archive eth_call + DexScreener).
    scheduler.add_job(
        _run_operator_sentinel,
        CronTrigger(minute="*/15"),
        id="operator_sentinel",
        name="操作者哨兵 (派发/rug/启动 → Telegram)",
    )

    # Operator hunt → actively find NEW hidden-Sybil operators daily.
    scheduler.add_job(
        _run_operator_hunt,
        CronTrigger(hour=3, minute=15),
        id="operator_hunt",
        name="操作者猎手 (扫BSC/SOL找隐藏控盘 → Telegram)",
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


async def _run_health_summary():
    logger.info("scheduled_health_summary")
    from src.ops.health import send_health_summary

    await send_health_summary()


async def _run_anomaly_screen():
    logger.info("scheduled_anomaly_screen")
    from src.pipeline.anomaly_screener import screen_universe, format_candidates
    from src.distribution.telegram_sender import send_alert

    cands = screen_universe()
    if cands:
        await send_alert(format_candidates(cands))
    logger.info("anomaly_screen_done", candidates=len(cands))


async def _run_operator_sentinel():
    logger.info("scheduled_operator_sentinel")
    from src.pipeline.operator_sentinel import run_and_alert

    await run_and_alert()


async def _run_operator_hunt():
    logger.info("scheduled_operator_hunt")
    from src.pipeline.operator_hunt import hunt, format_suspects
    from src.distribution.telegram_sender import send_alert

    suspects = hunt()
    # Only alert on real hidden-Sybil operators (隐藏簇) or mixed — not single-wallet
    # team/treasury concentration, and never pegs/majors (filtered upstream).
    strong = [s for s in suspects if s.get("funder_complete")
              and s.get("shape") in ("隐藏簇", "混合")]
    if strong:
        msg = "🎯 <b>操作者猎手 — 隐藏控盘嫌疑</b>\n━━━━━━━━━━\n"
        for s in strong[:8]:
            msg += (f"<b>{s['symbol']}</b> [{s['chain']}] {s.get('shape')} "
                    f"实体{s['largest_entity_pct']:.0f}%供应 缺口{s['concentration_gap']:+.0f}\n"
                    f"<code>{s['address']}</code>\n")
        await send_alert(msg)
    logger.info("operator_hunt_done", scanned=len(suspects), strong=len(strong))


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

    logger.info("starting_scheduler")
    scheduler = create_scheduler()
    scheduler.start()

    # Keep running
    try:
        while True:
            await asyncio.sleep(3600)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        logger.info("scheduler_stopped")


if __name__ == "__main__":
    asyncio.run(main())
