"""Political and regulatory news collector.

Monitors US and global crypto regulation, geopolitical events,
and political developments relevant to crypto markets.

Sources:
- SEC/CFTC/FinCEN/OCC press releases (RSS)
- OFAC sanctions updates
- Congressional crypto bills
- Global regulatory bodies
- Geopolitical news with crypto-relevant keyword filtering
"""

from __future__ import annotations

import asyncio
import hashlib
from datetime import datetime, timezone
from time import mktime

import feedparser

from src.collectors.base import BaseCollector, CollectedItem, CollectionResult

# Keywords for filtering geopolitical/political news to crypto-relevant items
CRYPTO_RELEVANT_KEYWORDS = [
    # Direct crypto
    "cryptocurrency", "bitcoin", "crypto", "blockchain", "digital asset",
    "stablecoin", "defi", "web3", "token", "nft",
    # Regulatory
    "sec", "cftc", "securities", "commodity", "regulation", "compliance",
    "sanctions", "ofac", "tornado cash", "money laundering", "aml", "kyc",
    "etf", "spot etf",
    # Financial / macro → crypto
    "de-dollarization", "cbdc", "digital dollar", "digital yuan", "e-cny",
    "capital controls", "currency crisis", "hyperinflation",
    # Geopolitical → crypto
    "sanctions evasion", "financial warfare", "swift", "brics currency",
    # Legislative
    "crypto bill", "fit21", "market structure", "stablecoin legislation",
    "strategic reserve", "bitcoin reserve",
]

# US regulatory RSS feeds
US_REGULATORY_FEEDS = [
    {
        "id": "sec_press",
        "name": "SEC Press Releases",
        "url": "https://www.sec.gov/rss/news/press.xml",
        "priority": "critical",
    },
    {
        "id": "sec_litigation",
        "name": "SEC Litigation Releases",
        "url": "https://www.sec.gov/rss/litigation/litreleases.xml",
        "priority": "critical",
    },
    {
        "id": "cftc_press",
        "name": "CFTC Press Releases",
        "url": "https://www.cftc.gov/PressRoom/PressReleases/RSS",
        "priority": "high",
    },
    {
        "id": "occ_news",
        "name": "OCC News",
        "url": "https://www.occ.gov/rss/occ-news.xml",
        "priority": "medium",
    },
    {
        "id": "fed_speeches",
        "name": "Fed Governor Speeches",
        "url": "https://www.federalreserve.gov/feeds/speeches.xml",
        "priority": "high",
    },
]

# Geopolitical news RSS feeds
GEOPOLITICAL_FEEDS = [
    {
        "id": "bbc_world",
        "name": "BBC World News",
        "url": "http://feeds.bbci.co.uk/news/world/rss.xml",
        "priority": "medium",
    },
    {
        "id": "al_jazeera",
        "name": "Al Jazeera",
        "url": "https://www.aljazeera.com/xml/rss/all.xml",
        "priority": "medium",
    },
]


def _is_crypto_relevant(title: str, content: str = "") -> bool:
    """Check if a news item is relevant to crypto markets."""
    text = (title + " " + content).lower()
    return any(kw in text for kw in CRYPTO_RELEVANT_KEYWORDS)


class PoliticalRegulatoryCollector(BaseCollector):
    """Collect political and regulatory news relevant to crypto."""

    source_id = "political_regulatory"
    source_name = "Political & Regulatory News"
    source_type = "rss"

    def _parse_date(self, entry: dict) -> datetime | None:
        for field in ("published_parsed", "updated_parsed"):
            parsed = entry.get(field)
            if parsed:
                try:
                    return datetime.fromtimestamp(mktime(parsed), tz=timezone.utc)
                except (ValueError, OverflowError):
                    continue
        return None

    async def _collect_feed(
        self, feed_config: dict, require_keyword_match: bool = False
    ) -> list[CollectedItem]:
        """Collect from a single RSS feed."""
        url = feed_config["url"]
        source_name = feed_config["name"]
        source_id = feed_config["id"]
        priority = feed_config.get("priority", "medium")

        try:
            raw = await self._fetch_url(url, use_cache=True)
            feed = feedparser.parse(raw)

            items = []
            for entry in feed.entries:
                title = entry.get("title", "")
                summary = entry.get("summary", "")

                # Apply keyword filter for non-regulatory sources
                if require_keyword_match and not _is_crypto_relevant(title, summary):
                    continue

                item_id = entry.get("id") or entry.get("link", "")
                link = entry.get("link", "")

                items.append(
                    CollectedItem(
                        id=f"polreg_{source_id}_{hashlib.md5(item_id.encode()).hexdigest()[:8]}",
                        title=f"[{source_name}] {title}",
                        content=summary,
                        url=link,
                        published_at=self._parse_date(entry),
                        metadata={
                            "data_type": "regulatory" if not require_keyword_match else "geopolitical",
                            "source_id": source_id,
                            "source_name": source_name,
                            "priority": priority,
                            "category": "regulatory",
                            "crypto_relevant": _is_crypto_relevant(title, summary),
                        },
                        raw={"entry_keys": list(entry.keys())},
                    )
                )
            self.log.info("regulatory_feed_parsed", source=source_name, items=len(items))
            return items
        except Exception as e:
            self.log.warning("regulatory_feed_error", source=source_name, error=str(e))
            return []

    async def _collect(self) -> CollectionResult:
        """Collect from all regulatory and political feeds."""
        # Regulatory feeds: collect all items (they're already crypto-focused)
        reg_tasks = [self._collect_feed(f, require_keyword_match=False) for f in US_REGULATORY_FEEDS]

        # Geopolitical feeds: filter to crypto-relevant only
        geo_tasks = [self._collect_feed(f, require_keyword_match=True) for f in GEOPOLITICAL_FEEDS]

        all_results = await asyncio.gather(*reg_tasks, *geo_tasks, return_exceptions=True)

        all_items = []
        for result in all_results:
            if isinstance(result, list):
                all_items.extend(result)

        return CollectionResult(
            source_id=self.source_id,
            source_name=self.source_name,
            source_type=self.source_type,
            items=all_items,
        )
