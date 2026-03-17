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
    # Weekly pipeline implementation
    from src.pipeline.daily_pipeline import run_daily_pipeline
    await run_daily_pipeline()  # Placeholder — weekly has extra Arena report logic


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
        # TODO: trigger event_pipeline for critical anomalies


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
            # TODO: auto-draft threads for high-scoring reports


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
            # TODO: auto-generate thread for this report
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
            # TODO: auto-generate fund flows visualization + thread
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
            # TODO: auto-generate thread
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
            # TODO: trigger event_pipeline for critical regulatory events


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
    # TODO: compile week's star data, generate charts, draft thread


async def main():
    """Run the scheduler."""
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
