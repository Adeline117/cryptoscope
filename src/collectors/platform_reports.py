"""On-chain data platform report collector.

Collects research reports, blog posts, and data updates published by
on-chain analytics platforms themselves (not their raw data APIs).

Three collection strategies:
1. RSS — Glassnode Insights, Coin Metrics Substack, Santiment, Flashbots, etc.
2. Blog/scrape — Nansen, Chainalysis, Messari, Token Terminal, Arkham, etc.
3. API — DeFiLlama endpoints for data change detection

This module complements chain_data.py (raw data) by capturing the
platforms' own analysis and commentary.
"""

from __future__ import annotations

import asyncio
import hashlib
import re
from datetime import datetime, timezone
from time import mktime
from typing import Any

import feedparser

from src.collectors.base import BaseCollector, CollectedItem, CollectionResult
from src.config import load_sources

# Report type classification keywords (order matters — more specific types first)
REPORT_TYPE_KEYWORDS = {
    "fund_flows": ["fund flow", "inflow", "outflow", "etf flow", "institutional flow", "etf", "institutional"],
    "security_incident": ["hack", "exploit", "incident", "vulnerability", "attack", "drain"],
    "weekly_digest": ["week on-chain", "weekly report", "week in review", "this week", "state of the network"],
    "deep_dive": ["deep dive", "report", "analysis", "research", "thesis", "theses"],
    "data_update": ["dashboard", "data", "metrics", "rankings", "update"],
    "market_commentary": ["market", "outlook", "commentary", "macro", "monthly"],
    "sector_report": ["sector", "defi", "l2", "nft", "gaming", "stablecoin"],
    "protocol_analysis": ["protocol", "token", "project", "deep dive"],
    "developer_report": ["developer", "dev activity", "github", "commits"],
}


def classify_report(title: str, content: str = "") -> str:
    """Auto-classify a report by type based on title and content keywords."""
    text = (title + " " + content).lower()
    scores = {}
    for report_type, keywords in REPORT_TYPE_KEYWORDS.items():
        scores[report_type] = sum(1 for kw in keywords if kw in text)
    best = max(scores, key=scores.get)
    return best if scores[best] > 0 else "general"


class PlatformReportCollector(BaseCollector):
    """Collect research reports and insights from on-chain analytics platforms."""

    source_id = "platform_reports"
    source_name = "On-Chain Platform Reports"
    source_type = "rss_and_scrape"

    def __init__(self, tiers: list[str] | None = None, **kwargs):
        """Initialize with optional tier filter.

        Args:
            tiers: List of tier subcategories to collect from.
                   None = all tiers. e.g. ["tier1", "tier2"]
        """
        super().__init__(cache_ttl=1800, **kwargs)  # 30 min cache
        self.tiers = tiers

    def _get_sources(self) -> list[dict]:
        """Load platform report sources from registry."""
        try:
            sources = load_sources("onchain_platform_reports.yaml")
            if self.tiers:
                sources = [s for s in sources if s.get("subcategory") in self.tiers]
            return [s for s in sources if s.get("enabled", True)]
        except FileNotFoundError:
            self.log.warning("onchain_platform_reports.yaml not found")
            return []

    def _parse_feed_date(self, entry: dict) -> datetime | None:
        for field in ("published_parsed", "updated_parsed"):
            parsed = entry.get(field)
            if parsed:
                try:
                    return datetime.fromtimestamp(mktime(parsed), tz=timezone.utc)
                except (ValueError, OverflowError):
                    continue
        return None

    async def _collect_rss_source(self, source: dict) -> list[CollectedItem]:
        """Collect from an RSS feed source."""
        rss_url = source.get("rss_url") or source.get("url")
        if not rss_url:
            return []

        try:
            raw_text = await self._fetch_url(rss_url, use_cache=True)
            feed = feedparser.parse(raw_text)

            items = []
            for entry in feed.entries:
                item_id = entry.get("id") or entry.get("link", "")
                title = entry.get("title", "Untitled")
                link = entry.get("link", "")
                summary = entry.get("summary", "")
                content_parts = entry.get("content", [])
                full_content = content_parts[0].get("value", "") if content_parts else summary

                report_type = classify_report(title, full_content)

                items.append(
                    CollectedItem(
                        id=f"platform_{source['id']}_{hashlib.md5(item_id.encode()).hexdigest()[:8]}",
                        title=f"[{source['name']}] {title}",
                        content=full_content,
                        url=link,
                        published_at=self._parse_feed_date(entry),
                        metadata={
                            "data_type": "platform_report",
                            "platform": source["id"],
                            "platform_name": source["name"],
                            "report_type": report_type,
                            "category": "onchain_platform_report",
                            "subcategory": source.get("subcategory", ""),
                            "priority": source.get("priority", "medium"),
                            "language": source.get("language", "en"),
                            "free_content": source.get("free_content", True),
                            "author": entry.get("author", ""),
                        },
                        raw={"entry_keys": list(entry.keys())},
                    )
                )

            self.log.info(
                "rss_platform_parsed",
                source=source["name"],
                items=len(items),
            )
            return items

        except Exception as e:
            self.log.warning(
                "rss_platform_error",
                source=source["name"],
                error=str(e),
            )
            return []

    async def _collect_blog_source(self, source: dict) -> list[CollectedItem]:
        """Collect from a blog/webpage source by fetching HTML and extracting links.

        Falls back to treating the URL as a potential RSS feed first.
        """
        url = source.get("url", "")
        if not url:
            return []

        try:
            raw_text = await self._fetch_url(url, use_cache=True)

            # Try parsing as RSS first (many blog URLs actually serve feeds)
            feed = feedparser.parse(raw_text)
            if feed.entries:
                source_copy = dict(source)
                source_copy["rss_url"] = url
                return await self._collect_rss_source(source_copy)

            # Otherwise, extract article links from HTML
            items = []
            # Simple pattern: find article links in common blog HTML structures
            link_pattern = re.compile(
                r'<a[^>]+href=["\']([^"\']+)["\'][^>]*>([^<]+)</a>',
                re.IGNORECASE,
            )
            matches = link_pattern.findall(raw_text)

            for href, text in matches[:20]:  # Limit to 20 most recent
                text = text.strip()
                if len(text) < 15 or len(text) > 300:
                    continue
                # Skip navigation/footer links
                if any(skip in text.lower() for skip in ["menu", "nav", "footer", "cookie", "privacy", "terms"]):
                    continue

                # Make absolute URL
                if href.startswith("/"):
                    from urllib.parse import urlparse
                    parsed = urlparse(url)
                    href = f"{parsed.scheme}://{parsed.netloc}{href}"
                elif not href.startswith("http"):
                    continue

                report_type = classify_report(text)

                items.append(
                    CollectedItem(
                        id=f"platform_{source['id']}_{hashlib.md5(href.encode()).hexdigest()[:8]}",
                        title=f"[{source['name']}] {text}",
                        content="",
                        url=href,
                        published_at=datetime.now(timezone.utc),
                        metadata={
                            "data_type": "platform_report",
                            "platform": source["id"],
                            "platform_name": source["name"],
                            "report_type": report_type,
                            "category": "onchain_platform_report",
                            "subcategory": source.get("subcategory", ""),
                            "priority": source.get("priority", "medium"),
                            "language": source.get("language", "en"),
                            "free_content": source.get("free_content", True),
                        },
                        raw={},
                    )
                )

            self.log.info(
                "blog_platform_parsed",
                source=source["name"],
                items=len(items),
            )
            return items

        except Exception as e:
            self.log.warning(
                "blog_platform_error",
                source=source["name"],
                error=str(e),
            )
            return []

    async def _collect(self) -> CollectionResult:
        """Collect from all configured platform report sources."""
        sources = self._get_sources()
        self.log.info("collecting_platform_reports", count=len(sources))

        # Separate RSS and blog/scrape sources
        rss_sources = [s for s in sources if s.get("type") in ("rss", "rss_and_scrape") and s.get("rss_url")]
        blog_sources = [s for s in sources if s not in rss_sources]

        # Collect RSS sources (fast, parallel)
        rss_tasks = [self._collect_rss_source(s) for s in rss_sources]
        # Collect blog sources (slower, also parallel but with rate limiting)
        blog_tasks = [self._collect_blog_source(s) for s in blog_sources]

        all_results = await asyncio.gather(
            *rss_tasks, *blog_tasks, return_exceptions=True
        )

        all_items = []
        seen_urls = set()
        for result in all_results:
            if isinstance(result, list):
                for item in result:
                    # Deduplicate by URL across platforms
                    if item.url and item.url not in seen_urls:
                        seen_urls.add(item.url)
                        all_items.append(item)

        return CollectionResult(
            source_id=self.source_id,
            source_name=self.source_name,
            source_type=self.source_type,
            items=all_items,
        )


class Tier1ReportCollector(PlatformReportCollector):
    """Collect only from Tier 1 (critical) platform report sources. Runs every 30 min."""

    source_id = "platform_reports_tier1"
    source_name = "Tier 1 Platform Reports"

    def __init__(self, **kwargs):
        super().__init__(tiers=["tier1"], **kwargs)


class InstitutionalReportCollector(PlatformReportCollector):
    """Collect from institutional/asset manager report sources (Tier 5)."""

    source_id = "platform_reports_institutional"
    source_name = "Institutional Reports"

    def __init__(self, **kwargs):
        super().__init__(tiers=["tier5"], **kwargs)
