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
CACHE_CLEANUP_BATCH_SIZE = 250
MAX_RESPONSE_CACHE_BODY_BYTES = 4 * 1024 * 1024


class CollectorCache:
    """SQLite-based cache to avoid re-fetching and enable deduplication."""

    # Collector instances are created independently but commonly point at the
    # same file. Serialize cold schema setup inside this process; SQLite's
    # busy_timeout remains the cross-process backstop.
    _init_locks: dict[str, asyncio.Lock] = {}

    def __init__(self, db_path: Path = CACHE_DB):
        self.db_path = db_path
        self._db: aiosqlite.Connection | None = None

    async def init(self) -> None:
        lock_key = str(self.db_path.resolve())
        init_lock = self._init_locks.setdefault(lock_key, asyncio.Lock())
        async with init_lock:
            if self._db is not None:
                return

            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            db = await aiosqlite.connect(str(self.db_path))
            try:
                # Set the timeout before asking SQLite to switch journal mode so
                # simultaneous collector startups wait rather than fail fast.
                await db.execute("PRAGMA busy_timeout=10000")
                await db.execute("PRAGMA journal_mode=WAL")
                await db.execute("""
                    CREATE TABLE IF NOT EXISTS seen_items (
                        item_hash TEXT PRIMARY KEY,
                        source_id TEXT NOT NULL,
                        item_id TEXT NOT NULL,
                        url TEXT,
                        first_seen_at TEXT NOT NULL,
                        last_seen_at TEXT NOT NULL
                    )
                """)
                await db.execute("""
                    CREATE TABLE IF NOT EXISTS response_cache (
                        cache_key TEXT PRIMARY KEY,
                        response_body TEXT NOT NULL,
                        cached_at TEXT NOT NULL,
                        ttl_seconds INTEGER NOT NULL
                    )
                """)
                await db.commit()
                await self._delete_expired_responses(db)
            except Exception:
                await db.close()
                raise
            self._db = db

    @staticmethod
    def _expiration_reason(
        cached_at: Any, ttl_seconds: Any, now: datetime
    ) -> str | None:
        """Return why a cache row is unusable, or None while it is fresh."""
        try:
            raw_cached_at = cached_at.strip()
            if raw_cached_at.endswith(("Z", "z")):
                raw_cached_at = f"{raw_cached_at[:-1]}+00:00"
            cached_time = datetime.fromisoformat(raw_cached_at)
            if cached_time.tzinfo is None:
                # Backward compatibility for rows written before timestamps
                # were explicitly timezone-aware.
                cached_time = cached_time.replace(tzinfo=timezone.utc)
            else:
                cached_time = cached_time.astimezone(timezone.utc)
        except (AttributeError, TypeError, ValueError):
            return "invalid_cached_at"

        try:
            ttl = int(ttl_seconds)
        except (TypeError, ValueError):
            return "invalid_ttl"
        if ttl <= 0:
            return "invalid_ttl"

        if cached_time > now:
            return "future_cached_at"
        if (now - cached_time).total_seconds() >= ttl:
            return "expired"
        return None

    async def _delete_expired_responses(
        self, db: aiosqlite.Connection
    ) -> None:
        """Remove unusable responses in bounded transactions, never VACUUMing."""
        last_key: str | None = None
        deleted = 0
        invalid = 0

        while True:
            if last_key is None:
                sql = """
                    SELECT cache_key, cached_at, ttl_seconds
                    FROM response_cache
                    ORDER BY cache_key
                    LIMIT ?
                """
                params: tuple[Any, ...] = (CACHE_CLEANUP_BATCH_SIZE,)
            else:
                sql = """
                    SELECT cache_key, cached_at, ttl_seconds
                    FROM response_cache
                    WHERE cache_key > ?
                    ORDER BY cache_key
                    LIMIT ?
                """
                params = (last_key, CACHE_CLEANUP_BATCH_SIZE)

            async with db.execute(sql, params) as cursor:
                rows = await cursor.fetchall()
            if not rows:
                break

            last_key = rows[-1][0]
            # Sample after SELECT so a legitimate concurrent refresh written
            # after cleanup started is not mistaken for a future-dated row.
            now = datetime.now(timezone.utc)
            stale_rows = []
            for cache_key, cached_at, ttl_seconds in rows:
                reason = self._expiration_reason(cached_at, ttl_seconds, now)
                if reason is not None:
                    stale_rows.append((cache_key, cached_at, ttl_seconds))
                    if reason != "expired":
                        invalid += 1

            if stale_rows:
                before = db.total_changes
                # Include the observed clock and TTL in the predicate. If a
                # concurrent writer refreshed the key, its new row survives.
                await db.executemany(
                    """
                    DELETE FROM response_cache
                    WHERE cache_key = ? AND cached_at IS ? AND ttl_seconds IS ?
                    """,
                    stale_rows,
                )
                await db.commit()
                deleted += db.total_changes - before

        if deleted:
            logger.info(
                "response_cache_expired_pruned",
                deleted=deleted,
                invalid_clocks=invalid,
            )

    async def close(self) -> None:
        if self._db:
            await self._db.close()
            self._db = None

    def _hash_item(self, source_id: str, item_id: str, url: str) -> str:
        raw = f"{source_id}:{item_id}:{url}"
        return hashlib.sha256(raw.encode()).hexdigest()

    @staticmethod
    def _key_fingerprint(cache_key: str) -> str:
        return hashlib.sha256(cache_key.encode()).hexdigest()[:12]

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
            reason = self._expiration_reason(
                cached_at, ttl, datetime.now(timezone.utc)
            )
            if reason is not None:
                await self._db.execute(
                    """
                    DELETE FROM response_cache
                    WHERE cache_key = ? AND cached_at IS ? AND ttl_seconds IS ?
                    """,
                    (cache_key, cached_at, ttl),
                )
                await self._db.commit()
                if reason != "expired":
                    logger.warning(
                        "response_cache_invalid_clock",
                        cache_key_hash=self._key_fingerprint(cache_key),
                        reason=reason,
                    )
                return None
            return body

    async def set_cached_response(
        self, cache_key: str, body: str, ttl_seconds: int = 3600
    ) -> bool:
        if (
            isinstance(ttl_seconds, bool)
            or not isinstance(ttl_seconds, int)
            or ttl_seconds <= 0
        ):
            logger.warning(
                "response_cache_invalid_ttl",
                cache_key_hash=self._key_fingerprint(cache_key),
                ttl_type=type(ttl_seconds).__name__,
            )
            return False

        body_bytes = len(body.encode("utf-8"))
        if body_bytes > MAX_RESPONSE_CACHE_BODY_BYTES:
            logger.warning(
                "response_cache_body_too_large",
                cache_key_hash=self._key_fingerprint(cache_key),
                body_bytes=body_bytes,
                max_body_bytes=MAX_RESPONSE_CACHE_BODY_BYTES,
            )
            return False

        now = datetime.now(timezone.utc).isoformat()
        await self._db.execute(
            """INSERT INTO response_cache (cache_key, response_body, cached_at, ttl_seconds)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(cache_key) DO UPDATE SET response_body = ?, cached_at = ?, ttl_seconds = ?""",
            (cache_key, body, now, ttl_seconds, body, now, ttl_seconds),
        )
        await self._db.commit()
        return True


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
