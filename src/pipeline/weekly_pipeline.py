"""Weekly pipeline: run daily pipeline + generate and send weekly summary."""

from __future__ import annotations

import asyncio
from collections import Counter
from datetime import datetime, timezone

import structlog

from src.distribution.telegram_sender import send_alert
from src.pipeline.daily_pipeline import run_daily_pipeline

logger = structlog.get_logger()


async def run_weekly_pipeline() -> dict:
    """Execute the weekly pipeline.

    1. Run the daily pipeline to collect and process today's items.
    2. Generate a weekly summary with item counts, top topics, and top anomalies.
    3. Send the summary via Telegram alert.

    Returns summary dict with weekly stats.
    """
    logger.info("weekly_pipeline_started")
    start_time = datetime.now(timezone.utc)

    # Step 1: Run the daily pipeline
    daily_result = await run_daily_pipeline()

    # Step 2: Build weekly summary
    items_collected = daily_result.get("items_collected", 0)
    anomalies_count = daily_result.get("anomalies", 0)
    threads_generated = daily_result.get("threads_generated", 0)
    top_narratives = daily_result.get("top_narratives", [])

    week_label = datetime.now(timezone.utc).strftime("Week of %Y-%m-%d")

    summary_lines = [
        f"Weekly Summary - {week_label}",
        "",
        f"Items collected: {items_collected}",
        f"Threads generated: {threads_generated}",
        f"Anomalies detected: {anomalies_count}",
    ]

    if top_narratives:
        summary_lines.append("")
        summary_lines.append("Top topics:")
        for narrative in top_narratives[:5]:
            summary_lines.append(f"  - {narrative}")

    if anomalies_count > 0:
        summary_lines.append("")
        summary_lines.append(
            f"Top anomalies: {anomalies_count} anomalies flagged this run. "
            "Review the daily digest for details."
        )

    summary_text = "\n".join(summary_lines)

    # Step 3: Send weekly summary via Telegram
    await send_alert(summary_text)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    result = {
        "status": "complete",
        "type": "weekly",
        "daily_result": daily_result,
        "elapsed_seconds": round(elapsed, 1),
    }
    logger.info("weekly_pipeline_complete", **result)
    return result


if __name__ == "__main__":
    result = asyncio.run(run_weekly_pipeline())
    import json
    print(json.dumps(result, indent=2))
