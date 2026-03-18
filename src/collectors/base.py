"""Abstract base collector with retry, rate limiting, caching, and deduplication."""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import aiohttp
import aiosqlite
import structlog
from pydantic import BaseModel, Field
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from src.config import DATA_DIR

logger = structlog.get_logger()

# --- Data Models ---


class CollectedItem(BaseModel):
    """A single collected data item."""

    id: str
    title: str
    content: str = ""
    url: str = ""
    published_at: datetime | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    raw: dict[str, Any] = Field(default_factory=dict)


class CollectionResult(BaseModel):
    """Result from a collector run."""

    source_id: str
    source_name: str
    source_type: str  # "rss", "api", "scrape", "db"
    collected_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    items: list[CollectedItem] = Field(default_factory=list)


# --- Cache ---

CACHE_DB = DATA_DIR / "cache.db"


class CollectorCache:
    """SQLite-based cache to avoid re-fetching and enable deduplication."""

    def __init__(self, db_path: Path = CACHE_DB):
        self.db_path = db_path
        self._db: aiosqlite.Connection | None = None

    async def init(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._db = await aiosqlite.connect(str(self.db_path))
        await self._db.execute("""
            CREATE TABLE IF NOT EXISTS seen_items (
                item_hash TEXT PRIMARY KEY,
                source_id TEXT NOT NULL,
                item_id TEXT NOT NULL,
                url TEXT,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL
            )
        """)
        await self._db.execute("""
            CREATE TABLE IF NOT EXISTS response_cache (
                cache_key TEXT PRIMARY KEY,
                response_body TEXT NOT NULL,
                cached_at TEXT NOT NULL,
                ttl_seconds INTEGER NOT NULL
            )
        """)
        await self._db.commit()

    async def close(self) -> None:
        if self._db:
            await self._db.close()

    def _hash_item(self, source_id: str, item_id: str, url: str) -> str:
        raw = f"{source_id}:{item_id}:{url}"
        return hashlib.sha256(raw.encode()).hexdigest()

    async def is_seen(self, source_id: str, item_id: str, url: str) -> bool:
        h = self._hash_item(source_id, item_id, url)
        async with self._db.execute(
            "SELECT 1 FROM seen_items WHERE item_hash = ?", (h,)
        ) as cursor:
            return await cursor.fetchone() is not None

    async def mark_seen(self, source_id: str, item_id: str, url: str) -> None:
        h = self._hash_item(source_id, item_id, url)
        now = datetime.now(timezone.utc).isoformat()
        await self._db.execute(
            """INSERT INTO seen_items (item_hash, source_id, item_id, url, first_seen_at, last_seen_at)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(item_hash) DO UPDATE SET last_seen_at = ?""",
            (h, source_id, item_id, url, now, now, now),
        )
        await self._db.commit()

    async def get_cached_response(self, cache_key: str) -> str | None:
        async with self._db.execute(
            "SELECT response_body, cached_at, ttl_seconds FROM response_cache WHERE cache_key = ?",
            (cache_key,),
        ) as cursor:
            row = await cursor.fetchone()
            if row is None:
                return None
            body, cached_at, ttl = row
            cached_time = datetime.fromisoformat(cached_at).replace(tzinfo=timezone.utc)
            elapsed = (datetime.now(timezone.utc) - cached_time).total_seconds()
            if elapsed > ttl:
                await self._db.execute(
                    "DELETE FROM response_cache WHERE cache_key = ?", (cache_key,)
                )
                await self._db.commit()
                return None
            return body

    async def set_cached_response(
        self, cache_key: str, body: str, ttl_seconds: int = 3600
    ) -> None:
        now = datetime.now(timezone.utc).isoformat()
        await self._db.execute(
            """INSERT INTO response_cache (cache_key, response_body, cached_at, ttl_seconds)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(cache_key) DO UPDATE SET response_body = ?, cached_at = ?, ttl_seconds = ?""",
            (cache_key, body, now, ttl_seconds, body, now, ttl_seconds),
        )
        await self._db.commit()


# --- Base Collector ---


class BaseCollector(ABC):
    """Abstract base class for all data collectors.

    Subclasses must implement:
        - source_id: str property
        - source_name: str property
        - source_type: str property (rss, api, scrape, db)
        - _collect(): the actual collection logic
    """

    def __init__(
        self,
        max_concurrent: int = 5,
        cache_ttl: int = 3600,
    ):
        self.max_concurrent = max_concurrent
        self.cache_ttl = cache_ttl
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._cache = CollectorCache()
        self._session: aiohttp.ClientSession | None = None
        self.log = logger.bind(collector=self.source_id)

    @property
    @abstractmethod
    def source_id(self) -> str: ...

    @property
    @abstractmethod
    def source_name(self) -> str: ...

    @property
    @abstractmethod
    def source_type(self) -> str: ...

    async def setup(self) -> None:
        """Initialize HTTP session and cache."""
        await self._cache.init()
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=30)
            self._session = aiohttp.ClientSession(timeout=timeout)

    async def teardown(self) -> None:
        """Clean up resources."""
        if self._session and not self._session.closed:
            await self._session.close()
        await self._cache.close()

    async def collect(self) -> CollectionResult:
        """Run collection with setup/teardown and deduplication."""
        await self.setup()
        try:
            self.log.info("collection_started")
            result = await self._collect()

            # Deduplicate against cache
            new_items = []
            for item in result.items:
                if not await self._cache.is_seen(
                    result.source_id, item.id, item.url
                ):
                    new_items.append(item)
                    await self._cache.mark_seen(
                        result.source_id, item.id, item.url
                    )

            dedup_count = len(result.items) - len(new_items)
            result.items = new_items

            self.log.info(
                "collection_complete",
                total=len(result.items) + dedup_count,
                new=len(result.items),
                duplicates=dedup_count,
            )
            return result
        except Exception as e:
            self.log.error("collection_failed", error=str(e))
            raise
        finally:
            await self.teardown()

    @abstractmethod
    async def _collect(self) -> CollectionResult:
        """Implement the actual collection logic. Called by collect()."""
        ...

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)),
    )
    async def _fetch_url(
        self,
        url: str,
        headers: dict | None = None,
        params: dict | None = None,
        use_cache: bool = True,
    ) -> str:
        """Fetch a URL with retry, rate limiting, and optional caching."""
        cache_key = hashlib.sha256(
            f"{url}:{json.dumps(params, sort_keys=True)}".encode()
        ).hexdigest()

        if use_cache:
            cached = await self._cache.get_cached_response(cache_key)
            if cached is not None:
                self.log.debug("cache_hit", url=url)
                return cached

        async with self._semaphore:
            self.log.debug("fetching", url=url)
            async with self._session.get(
                url, headers=headers, params=params
            ) as resp:
                resp.raise_for_status()
                body = await resp.text()

            if use_cache:
                await self._cache.set_cached_response(
                    cache_key, body, self.cache_ttl
                )
            return body

    async def _fetch_json(
        self,
        url: str,
        headers: dict | None = None,
        params: dict | None = None,
        use_cache: bool = True,
    ) -> Any:
        """Fetch URL and parse as JSON."""
        body = await self._fetch_url(url, headers, params, use_cache)
        try:
            return json.loads(body)
        except json.JSONDecodeError as e:
            self.log.warning("json_parse_error", url=url, error=str(e))
            raise
