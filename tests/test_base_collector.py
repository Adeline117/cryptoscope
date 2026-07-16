"""Tests for BaseCollector, cache, and data models."""

import asyncio
import hashlib
import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

import src.collectors.base as base_module
from src.collectors.base import (
    BaseCollector,
    CollectedItem,
    CollectionResult,
    CollectorCache,
)


class MockCollector(BaseCollector):
    source_id = "mock"
    source_name = "Mock Collector"
    source_type = "api"

    def __init__(self, items=None, **kwargs):
        super().__init__(**kwargs)
        self._items = items or []

    async def _collect(self):
        return CollectionResult(
            source_id=self.source_id,
            source_name=self.source_name,
            source_type=self.source_type,
            items=self._items,
        )


@pytest.fixture
def sample_items():
    return [
        CollectedItem(
            id="item1",
            title="Test Item 1",
            content="Content 1",
            url="https://example.com/1",
            published_at=datetime.now(timezone.utc),
        ),
        CollectedItem(
            id="item2",
            title="Test Item 2",
            content="Content 2",
            url="https://example.com/2",
            published_at=datetime.now(timezone.utc),
        ),
    ]


@pytest.mark.asyncio
async def test_collection_returns_items(sample_items, tmp_path):
    collector = MockCollector(items=sample_items)
    collector._cache.db_path = tmp_path / "test_cache.db"
    result = await collector.collect()
    assert len(result.items) == 2
    assert result.source_id == "mock"


@pytest.mark.asyncio
async def test_deduplication(sample_items, tmp_path):
    collector = MockCollector(items=sample_items)
    collector._cache.db_path = tmp_path / "test_cache.db"

    # First run: all items new
    result1 = await collector.collect()
    assert len(result1.items) == 2

    # Second run: all items should be deduplicated
    result2 = await collector.collect()
    assert len(result2.items) == 0


@pytest.mark.asyncio
async def test_cache_ttl(tmp_path):
    cache = CollectorCache(db_path=tmp_path / "test_cache.db")
    await cache.init()

    await cache.set_cached_response("key1", '{"data": 1}', ttl_seconds=3600)
    result = await cache.get_cached_response("key1")
    assert result == '{"data": 1}'

    # Non-existent key
    result2 = await cache.get_cached_response("key_missing")
    assert result2 is None

    await cache.close()


@pytest.mark.asyncio
async def test_cache_init_prunes_expired_and_invalid_rows_in_batches(
    tmp_path, monkeypatch
):
    db_path = tmp_path / "test_cache.db"
    seed = CollectorCache(db_path=db_path)
    await seed.init()
    await seed.mark_seen("source", "item", "https://example.com/item")

    now = datetime.now(timezone.utc)
    expired_at = (now - timedelta(hours=2)).isoformat()
    live_offset_at = now.astimezone(timezone(timedelta(hours=8))).isoformat()
    rows = [
        (f"expired-{index}", "old", expired_at, 60) for index in range(7)
    ]
    rows.extend(
        [
            ("invalid-clock", "bad", "not-a-clock", 3600),
            (
                "future-clock",
                "future",
                (now + timedelta(hours=1)).isoformat(),
                3600,
            ),
            ("live-aware", "fresh", live_offset_at, 3600),
        ]
    )
    await seed._db.executemany(
        """
        INSERT INTO response_cache
            (cache_key, response_body, cached_at, ttl_seconds)
        VALUES (?, ?, ?, ?)
        """,
        rows,
    )
    await seed._db.commit()
    await seed.close()

    monkeypatch.setattr(base_module, "CACHE_CLEANUP_BATCH_SIZE", 2)
    caches = [CollectorCache(db_path=db_path) for _ in range(4)]
    await asyncio.gather(*(cache.init() for cache in caches))
    await asyncio.gather(*(cache.close() for cache in caches))

    with sqlite3.connect(db_path) as db:
        cached_rows = db.execute(
            "SELECT cache_key, response_body FROM response_cache"
        ).fetchall()
        seen_count = db.execute("SELECT COUNT(*) FROM seen_items").fetchone()[0]

    assert cached_rows == [("live-aware", "fresh")]
    assert seen_count == 1


@pytest.mark.asyncio
async def test_cache_concurrent_init_on_new_database(tmp_path):
    db_path = tmp_path / "new_cache.db"
    caches = [CollectorCache(db_path=db_path) for _ in range(8)]

    await asyncio.gather(*(cache.init() for cache in caches))
    await asyncio.gather(*(cache.close() for cache in caches))

    with sqlite3.connect(db_path) as db:
        tables = {
            row[0]
            for row in db.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
    assert {"response_cache", "seen_items"} <= tables


@pytest.mark.asyncio
async def test_cache_rejects_oversized_body_by_utf8_bytes(
    tmp_path, monkeypatch
):
    warnings = []

    class RecordingLogger:
        def warning(self, event, **fields):
            warnings.append((event, fields))

    monkeypatch.setattr(base_module, "MAX_RESPONSE_CACHE_BODY_BYTES", 4)
    monkeypatch.setattr(base_module, "logger", RecordingLogger())

    cache = CollectorCache(db_path=tmp_path / "test_cache.db")
    await cache.init()
    secret_key = "https://api.example.test/data?token=super-secret"
    key_hash = hashlib.sha256(secret_key.encode()).hexdigest()[:12]
    assert await cache.set_cached_response("small", "éé") is True

    # Three two-byte UTF-8 characters exceed the four-byte limit even though
    # the Python string contains only three code points.
    assert await cache.set_cached_response(secret_key, "ééé") is False
    assert await cache.get_cached_response(secret_key) is None
    await cache.close()

    assert warnings == [
        (
            "response_cache_body_too_large",
            {
                "cache_key_hash": key_hash,
                "body_bytes": 6,
                "max_body_bytes": 4,
            },
        )
    ]
    assert "super-secret" not in repr(warnings)


@pytest.mark.asyncio
@pytest.mark.parametrize("ttl", [True, False, 0, -1, 1.0, "3600"])
async def test_cache_rejects_non_positive_or_non_integer_ttl(
    tmp_path, monkeypatch, ttl
):
    warnings = []

    class RecordingLogger:
        def warning(self, event, **fields):
            warnings.append((event, fields))

    monkeypatch.setattr(base_module, "logger", RecordingLogger())
    cache = CollectorCache(db_path=tmp_path / "test_cache.db")
    await cache.init()
    secret_key = "cache-key-with-secret-token"

    assert await cache.set_cached_response(secret_key, "body", ttl) is False
    assert await cache.get_cached_response(secret_key) is None
    await cache.close()

    assert warnings[0][0] == "response_cache_invalid_ttl"
    assert warnings[0][1]["cache_key_hash"] == hashlib.sha256(
        secret_key.encode()
    ).hexdigest()[:12]
    assert "secret-token" not in repr(warnings)


@pytest.mark.asyncio
async def test_cache_get_supports_aware_iso_and_deletes_bad_clock(tmp_path):
    cache = CollectorCache(db_path=tmp_path / "test_cache.db")
    await cache.init()
    now = datetime.now(timezone.utc)
    aware_offset = (
        now - timedelta(seconds=30)
    ).astimezone(timezone(timedelta(hours=-7)))
    await cache._db.executemany(
        """
        INSERT INTO response_cache
            (cache_key, response_body, cached_at, ttl_seconds)
        VALUES (?, ?, ?, ?)
        """,
        [
            ("aware", "fresh", aware_offset.isoformat(), 3600),
            ("bad", "unsafe", "definitely-not-iso", 3600),
        ],
    )
    await cache._db.commit()

    assert await cache.get_cached_response("aware") == "fresh"
    assert await cache.get_cached_response("bad") is None
    async with cache._db.execute(
        "SELECT 1 FROM response_cache WHERE cache_key = 'bad'"
    ) as cursor:
        assert await cursor.fetchone() is None
    await cache.close()


def test_collected_item_model():
    item = CollectedItem(
        id="test",
        title="Test",
        url="https://example.com",
    )
    assert item.content == ""
    assert item.metadata == {}
    assert item.raw == {}


def test_priority_scorer():
    from src.analysis.priority_scorer import score_item

    item = CollectedItem(
        id="test",
        title="Major exploit drains $100 billion from protocol",
        content="A critical vulnerability was discovered...",
        url="https://example.com/exploit",
        published_at=datetime.now(timezone.utc),
        metadata={"priority": "high", "category": "security"},
    )
    score = score_item(item)
    assert score > 50  # High priority + recent + security keywords
