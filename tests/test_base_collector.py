"""Tests for BaseCollector, cache, and data models."""

import asyncio
from datetime import datetime, timezone

import pytest

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
