"""RSS/Atom feed collector for news, blogs, newsletters, and podcasts."""

from __future__ import annotations

from datetime import datetime, timezone
from time import mktime

import feedparser

from src.collectors.base import BaseCollector, CollectedItem, CollectionResult
from src.config import load_sources


class NewsFeedCollector(BaseCollector):
    """Collect items from RSS/Atom feeds defined in source registry."""

    source_id = "news_feed"
    source_name = "RSS/Atom News Feed"
    source_type = "rss"

    def __init__(self, source_files: list[str] | None = None, **kwargs):
        """Initialize with optional list of source YAML files to pull feeds from.

        If not specified, loads from news_media.yaml, newsletters.yaml,
        research_firms.yaml, and podcasts.yaml.
        """
        super().__init__(**kwargs)
        self.source_files = source_files or [
            "news_media.yaml",
            "newsletters.yaml",
            "research_firms.yaml",
            "podcasts.yaml",
        ]

    def _get_feed_sources(self) -> list[dict]:
        """Load all RSS-type sources from the registry."""
        feeds = []
        for sf in self.source_files:
            try:
                sources = load_sources(sf)
                for s in sources:
                    if s.get("type") == "rss" and s.get("enabled", True):
                        feeds.append(s)
            except FileNotFoundError:
                self.log.warning("source_file_not_found", file=sf)
        return feeds

    def _parse_date(self, entry: dict) -> datetime | None:
        for field in ("published_parsed", "updated_parsed"):
            parsed = entry.get(field)
            if parsed:
                try:
                    return datetime.fromtimestamp(mktime(parsed), tz=timezone.utc)
                except (ValueError, OverflowError):
                    continue
        return None

    async def _collect_single_feed(self, source: dict) -> list[CollectedItem]:
        """Fetch and parse a single RSS feed."""
        url = source["url"]
        source_name = source.get("name", url)
        try:
            raw_text = await self._fetch_url(url, use_cache=True)
            feed = feedparser.parse(raw_text)

            items = []
            for entry in feed.entries:
                item_id = entry.get("id") or entry.get("link", "")
                title = entry.get("title", "Untitled")
                link = entry.get("link", "")
                summary = entry.get("summary", "")
                content_parts = entry.get("content", [])
                full_content = content_parts[0].get("value", "") if content_parts else summary

                items.append(
                    CollectedItem(
                        id=item_id,
                        title=title,
                        content=full_content,
                        url=link,
                        published_at=self._parse_date(entry),
                        metadata={
                            "source_name": source_name,
                            "source_id": source.get("id", ""),
                            "category": source.get("category", ""),
                            "subcategory": source.get("subcategory", ""),
                            "priority": source.get("priority", "medium"),
                            "tags": entry.get("tags", []),
                            "author": entry.get("author", ""),
                        },
                        raw={"entry_keys": list(entry.keys())},
                    )
                )
            self.log.info("feed_parsed", source=source_name, items=len(items))
            return items

        except Exception as e:
            self.log.warning("feed_error", source=source_name, error=str(e))
            return []

    async def _collect(self) -> CollectionResult:
        """Collect from all configured RSS feeds."""
        import asyncio

        feeds = self._get_feed_sources()
        self.log.info("collecting_feeds", count=len(feeds))

        tasks = [self._collect_single_feed(feed) for feed in feeds]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        all_items = []
        for result in results:
            if isinstance(result, list):
                all_items.extend(result)

        return CollectionResult(
            source_id=self.source_id,
            source_name=self.source_name,
            source_type=self.source_type,
            items=all_items,
        )
