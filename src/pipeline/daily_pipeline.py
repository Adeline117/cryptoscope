"""Daily pipeline: collect → score → analyze → generate → send for review."""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timezone

import structlog

from src.analysis.anomaly_engine import detect_all_anomalies
from src.analysis.narrative_detector import extract_keyword_narratives
from src.analysis.priority_scorer import filter_by_tier, score_and_rank
from src.analysis.topic_classifier import classify_items
from src.collectors.base import CollectedItem, CollectionResult
from src.collectors.chain_data import DeFiLlamaCollector
from src.collectors.github_tracker import GitHubTracker
from src.collectors.news_feed import NewsFeedCollector
from src.collectors.platform_reports import PlatformReportCollector
from src.collectors.political_regulatory import PoliticalRegulatoryCollector
from src.content.thread_formatter import format_for_telegram, format_for_twitter
from src.distribution.draft_manager import DraftManager
from src.distribution.telegram_sender import send_daily_digest, send_thread_for_review

logger = structlog.get_logger()


async def _run_collectors() -> list[CollectedItem]:
    """Run all enabled collectors in parallel."""
    collectors = [
        NewsFeedCollector(),
        DeFiLlamaCollector(),
        GitHubTracker(),
        PlatformReportCollector(),       # On-chain platform reports (all tiers)
        PoliticalRegulatoryCollector(),  # SEC/CFTC/Fed/geopolitics
        # FREDCollector(),              # Enable when FRED_API_KEY available
        # GitHubStarVelocityCollector(), # Enable after 30-day baseline collected
        # AcademicPapersCollector(),    # Enable when Semantic Scholar key available
        # GovernanceWatchCollector(),   # Enable when needed
        # ArenaDBCollector(),           # Enable when Arena DB configured
    ]

    tasks = [c.collect() for c in collectors]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    all_items = []
    for result in results:
        if isinstance(result, CollectionResult):
            all_items.extend(result.items)
            logger.info(
                "collector_done",
                source=result.source_name,
                items=len(result.items),
            )
        elif isinstance(result, Exception):
            logger.error("collector_failed", error=str(result))

    return all_items


async def run_daily_pipeline() -> dict:
    """Execute the full daily pipeline.

    Returns summary dict with stats.
    """
    logger.info("daily_pipeline_started")
    start_time = datetime.now(timezone.utc)

    # Step 1: Collect from all sources
    all_items = await _run_collectors()
    logger.info("collection_complete", total_items=len(all_items))

    if not all_items:
        logger.warning("no_items_collected")
        return {"status": "empty", "items_collected": 0}

    # Step 2: Score and rank
    scored = score_and_rank(all_items)
    tiers = filter_by_tier(scored)

    logger.info(
        "scoring_complete",
        auto_draft=len(tiers["auto_draft"]),
        digest=len(tiers["digest"]),
        archive=len(tiers["archive"]),
    )

    # Step 3: Detect anomalies
    anomalies = detect_all_anomalies(all_items)
    if anomalies:
        logger.info("anomalies_detected", count=len(anomalies))

    # Step 4: Detect narratives
    narratives = extract_keyword_narratives(all_items)

    # Step 5: Classify by topic
    by_topic = classify_items(all_items)
    top_topics = sorted(by_topic.keys(), key=lambda t: len(by_topic[t]), reverse=True)[:10]

    # Step 6: Generate threads for top auto-draft items
    draft_manager = DraftManager()
    await draft_manager.init()

    threads_generated = 0
    for item, score in tiers["auto_draft"][:3]:  # Top 3 items
        try:
            draft_id = f"daily_{datetime.now(timezone.utc).strftime('%Y%m%d')}_{uuid.uuid4().hex[:8]}"

            # For now, create draft with item content directly
            # Thread generation via Claude API would go here
            text_en = f"📊 {item.title}\n\n{item.content[:500]}"
            text_zh = text_en  # Placeholder — real bilingual generation needs API key

            # Long-form analysis (expanded version of the thread)
            long_en = (
                f"{item.title}\n\n"
                f"{item.content}\n\n"
                f"Source: {item.url}\n\n"
                f"---\n"
                f"CryptoScope · Arena (arenafi.org)"
            )
            long_zh = long_en  # Placeholder — real bilingual needs API key

            await draft_manager.create_draft(
                draft_id=draft_id,
                topic=item.title,
                text_en=text_en,
                text_zh=text_zh,
                priority_score=score,
                sources=[item.url] if item.url else [],
            )

            # Send thread + long-form to your Telegram
            await send_thread_for_review(
                topic=item.title,
                priority_score=score,
                sources_used=[item.url] if item.url else [],
                thread_text_en=text_en,
                thread_text_zh=text_zh,
                long_form_en=long_en,
                long_form_zh=long_zh,
            )
            threads_generated += 1

        except Exception as e:
            logger.error("thread_generation_failed", item=item.title, error=str(e))

    await draft_manager.close()

    # Step 7: Send daily digest
    digest_summary = (
        f"🔍 Narratives: {', '.join(list(narratives.keys())[:5])}\n"
        f"⚠️ Anomalies: {len(anomalies)}\n"
        f"📝 Threads generated: {threads_generated}"
    )
    await send_daily_digest(
        summary=digest_summary,
        item_count=len(all_items),
        top_topics=top_topics,
    )

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    summary = {
        "status": "complete",
        "items_collected": len(all_items),
        "auto_draft": len(tiers["auto_draft"]),
        "digest": len(tiers["digest"]),
        "anomalies": len(anomalies),
        "threads_generated": threads_generated,
        "top_narratives": list(narratives.keys())[:5],
        "elapsed_seconds": round(elapsed, 1),
    }
    logger.info("daily_pipeline_complete", **summary)
    return summary


if __name__ == "__main__":
    result = asyncio.run(run_daily_pipeline())
    import json
    print(json.dumps(result, indent=2))
