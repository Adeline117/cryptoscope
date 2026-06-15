"""Every-2-hour highlight pipeline: collect → score → pick top → send to Telegram.

Sends you the single most shocking/eye-catching item across ALL 600+ sources.
Includes a brief top-5 rundown for context.
"""

from __future__ import annotations

import asyncio
import re
from datetime import datetime, timezone

import structlog

from src.analysis.anomaly_engine import detect_all_anomalies
from src.analysis.priority_scorer import score_and_rank
from src.collectors.base import CollectedItem, CollectionResult
from src.collectors.chain_data import DeFiLlamaCollector
from src.collectors.github_tracker import GitHubTracker
from src.collectors.news_feed import NewsFeedCollector
from src.collectors.platform_reports import PlatformReportCollector
from src.collectors.political_regulatory import PoliticalRegulatoryCollector
from src.distribution.telegram_sender import send_alert

logger = structlog.get_logger()

# Items older than this are NOT eligible for the realtime 2h highlight. RSS feeds
# routinely serve old articles (re-published, or still in the feed window); a
# 3-month-old item once leaked into the realtime stream because this filter only
# checked the source, never the timestamp.
HIGHLIGHT_MAX_AGE_HOURS = 48


async def _collect_without_dedup(collector) -> CollectionResult:
    """Run a collector WITHOUT dedup so highlights can re-scan all recent items."""
    await collector.setup()
    try:
        return await collector._collect()
    except Exception as e:
        logger.error("highlight_collector_failed", collector=collector.source_id, error=str(e))
        return CollectionResult(source_id=collector.source_id, source_name=collector.source_name, source_type=collector.source_type)
    finally:
        await collector.teardown()


def _is_genuinely_recent(item: CollectedItem, hours: int = HIGHLIGHT_MAX_AGE_HOURS) -> bool:
    """Filter out STALE items, historical data, and secondary news media.

    We only want PRIMARY sources — original research, on-chain data,
    official announcements, regulatory filings, GitHub activity —
    AND only items actually published within the last `hours`.
    """
    # --- Recency gate (the actual "recent" check) ---
    # Drop anything with a publish date older than the window. Items without a
    # date (much on-chain / GitHub activity) are kept — they are inherently
    # "now" and the data_type gate below removes historical snapshots.
    published = item.published_at
    if published is not None:
        if published.tzinfo is None:
            published = published.replace(tzinfo=timezone.utc)
        age_hours = (datetime.now(timezone.utc) - published).total_seconds() / 3600
        if age_hours > hours:
            return False

    data_type = item.metadata.get("data_type", "")
    # DeFiLlama snapshots/historical data — not breaking news
    if data_type in ("hack", "protocol_tvl", "yield_pool", "stablecoin", "chain_tvl", "bridge"):
        return False

    # Competitor / secondary news media — they all have Twitter and publish
    # the same news before us. We want first-hand sources only.
    source_name = item.metadata.get("source_name", item.metadata.get("feed_title", "")).lower()
    source_id = item.metadata.get("source_id", item.id.split("_")[0] if "_" in item.id else "").lower()

    SECONDARY_MEDIA = {
        # Chinese crypto media (competitors)
        "吴说区块链", "wu blockchain", "wublock",
        "chaincatcher", "链捕手",
        "blockbeats", "律动",
        "foresight news", "foresightnews",
        "panews",
        "odaily", "星球日报",
        # English crypto media (all have Twitter, repackage news)
        "coindesk", "the block", "theblock",
        "blockworks", "decrypt", "cointelegraph",
        "protos", "dl news", "dlnews",
        "unchained", "rekt news",
        "bitcoin magazine",
        "bloomberg crypto", "reuters crypto",
    }

    for media in SECONDARY_MEDIA:
        if media in source_name or media in source_id:
            return False

    return True


async def _collect_all() -> list[CollectedItem]:
    """Run all enabled collectors in parallel, without dedup, filtering to real news."""
    collectors = [
        NewsFeedCollector(),
        DeFiLlamaCollector(),
        GitHubTracker(),
        PlatformReportCollector(),
        PoliticalRegulatoryCollector(),
    ]

    tasks = [_collect_without_dedup(c) for c in collectors]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    all_items: list[CollectedItem] = []
    for result in results:
        if isinstance(result, CollectionResult):
            # Only keep genuinely recent news items
            for item in result.items:
                if _is_genuinely_recent(item):
                    all_items.append(item)
            logger.info("highlight_collector_done", source=result.source_name, kept=sum(1 for i in result.items if _is_genuinely_recent(i)), total=len(result.items))
        elif isinstance(result, Exception):
            logger.error("highlight_collector_failed", error=str(result))

    return all_items


def _strip_html(text: str) -> str:
    """Remove HTML tags and FULLY decode entities.

    RSS content is full of numeric/named entities (&#8217; &mdash; &hellip; …);
    decoding only a few left literals that then got re-escaped into garbled text.
    html.unescape handles them all.
    """
    import html as _html

    text = re.sub(r"<[^>]+>", "", text)
    text = _html.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _format_time(dt: datetime | None) -> str:
    """Format published_at to a readable string in Seattle time (US/Pacific)."""
    if dt is None:
        return "时间未知"
    from zoneinfo import ZoneInfo
    seattle = dt.astimezone(ZoneInfo("America/Los_Angeles"))
    return seattle.strftime("%Y-%m-%d %H:%M") + " (Seattle)"


def _item_block(item: CollectedItem, score: float, rank: int) -> list[str]:
    """Format a single item with time, source, link."""
    lines: list[str] = []
    prefix = "🏆" if rank == 1 else f"{rank}."

    lines.append(f"{prefix} <b>{_esc(item.title[:200])}</b>")
    lines.append(f"   ⏰ {_format_time(item.published_at)}")
    lines.append(f"   📊 Score: {score:.0f}/100")

    # Source name
    source = item.metadata.get("source_name", item.metadata.get("feed_title", ""))
    category = item.metadata.get("category", "")
    if source or category:
        tag_parts = [p for p in [source, category] if p]
        lines.append(f"   📌 {' · '.join(tag_parts)}")

    # Clean content snippet (strip HTML)
    if item.content and rank == 1:
        clean = _strip_html(item.content)
        snippet = clean[:500]
        if len(clean) > 500:
            snippet += "…"
        lines.append(f"\n{_esc(snippet)}")

    # Original source link
    if item.url:
        lines.append(f"   🔗 {item.url}")

    return lines


def _format_highlight_message(
    scored: list[tuple[CollectedItem, float]],
    anomaly_count: int,
) -> str:
    """Format the top items into a single Telegram message."""
    if not scored:
        return "No new items collected this cycle."

    lines: list[str] = []

    # Header
    now_str = _format_time(datetime.now(timezone.utc))
    lines.append(f"🔥 <b>2h Highlight — 最抓眼球的消息</b>")
    lines.append(f"📅 {now_str}")
    lines.append("")

    # #1 — the most shocking item, full detail
    lines.extend(_item_block(*scored[0], rank=1))

    # Runner-up top 5
    if len(scored) > 1:
        lines.append("")
        lines.append("── <b>Top 5 其他热门</b> ──")
        for i, (item, score) in enumerate(scored[1:5], start=2):
            lines.append("")
            lines.extend(_item_block(item, score, rank=i))

    # Footer stats
    lines.append("")
    total = len(scored)
    high = sum(1 for _, s in scored if s >= 70)
    lines.append(
        f"📊 {total} items scanned · {high} high-priority · {anomaly_count} anomalies"
    )

    return "\n".join(lines)


def _esc(text: str) -> str:
    """Escape for Telegram HTML. Decode any pre-existing entities FIRST so RSS
    content like &#8217;/&mdash; doesn't get re-escaped into garbled &amp;#8217;."""
    import html as _html

    text = _html.unescape(str(text))
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


async def run_highlight_pipeline() -> dict:
    """Execute the 2-hour highlight pipeline.

    1. Collect from all sources
    2. Score and rank
    3. Detect anomalies
    4. Format and send top items via Telegram
    """
    logger.info("highlight_pipeline_started")
    start = datetime.now(timezone.utc)

    # Collect
    all_items = await _collect_all()
    logger.info("highlight_collection_done", count=len(all_items))

    if not all_items:
        logger.warning("highlight_no_items")
        return {"status": "empty", "items": 0}

    # Score
    scored = score_and_rank(all_items)

    # Anomalies
    anomalies = detect_all_anomalies(all_items)

    # Format message
    message = _format_highlight_message(scored, len(anomalies))

    # Send via Telegram
    sent = await _send_highlight(message)

    elapsed = (datetime.now(timezone.utc) - start).total_seconds()
    summary = {
        "status": "sent" if sent else "telegram_failed",
        "items": len(all_items),
        "top_score": scored[0][1] if scored else 0,
        "top_title": scored[0][0].title if scored else "",
        "anomalies": len(anomalies),
        "elapsed_seconds": round(elapsed, 1),
    }
    logger.info("highlight_pipeline_complete", **summary)
    return summary


async def _send_highlight(message: str) -> bool:
    """Send highlight message via Telegram bot."""
    import os

    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TG_REVIEW_CHANNEL")

    if not bot_token or not chat_id:
        logger.warning("telegram_not_configured_for_highlight")
        # Fall back to printing to console
        print("\n" + "=" * 60)
        print(message.replace("<b>", "").replace("</b>", ""))
        print("=" * 60 + "\n")
        return False

    try:
        from telegram import Bot

        bot = Bot(token=bot_token)

        # Split if too long for Telegram (4096 char limit)
        if len(message) <= 4000:
            await bot.send_message(
                chat_id=chat_id, text=message, parse_mode="HTML",
                disable_web_page_preview=True,
            )
        else:
            # Split at blank lines
            parts = message.split("\n\n")
            chunk = ""
            for part in parts:
                if len(chunk) + len(part) + 2 > 4000:
                    if chunk:
                        await bot.send_message(
                            chat_id=chat_id, text=chunk, parse_mode="HTML",
                            disable_web_page_preview=True,
                        )
                    chunk = part
                else:
                    chunk = chunk + "\n\n" + part if chunk else part
            if chunk:
                await bot.send_message(
                    chat_id=chat_id, text=chunk, parse_mode="HTML",
                    disable_web_page_preview=True,
                )

        logger.info("highlight_telegram_sent")
        return True
    except Exception as e:
        logger.error("highlight_telegram_failed", error=str(e))
        return False


if __name__ == "__main__":
    import json

    result = asyncio.run(run_highlight_pipeline())
    print(json.dumps(result, indent=2, ensure_ascii=False))
