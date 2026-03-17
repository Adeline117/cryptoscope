"""Cross-source signal correlation to boost confidence in trends."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass

from src.collectors.base import CollectedItem


@dataclass
class CorrelatedSignal:
    """A signal confirmed across multiple sources."""

    topic: str
    source_count: int
    sources: list[str]
    items: list[CollectedItem]
    confidence: float  # 0-1


def find_correlated_signals(
    items: list[CollectedItem],
    min_sources: int = 2,
) -> list[CorrelatedSignal]:
    """Find topics that appear across multiple independent sources.

    When multiple sources discuss the same topic, it's a stronger signal.
    """
    from src.analysis.topic_classifier import classify_item

    # Group items by topic AND source
    topic_sources: dict[str, dict[str, list[CollectedItem]]] = defaultdict(lambda: defaultdict(list))

    for item in items:
        topics = classify_item(item)
        source = item.metadata.get("source_name") or item.metadata.get("source_id", "unknown")
        for topic in topics:
            topic_sources[topic][source].append(item)

    # Build correlated signals
    signals = []
    for topic, sources in topic_sources.items():
        if len(sources) >= min_sources:
            all_items = []
            for source_items in sources.values():
                all_items.extend(source_items)

            confidence = min(len(sources) / 5, 1.0)  # 5+ sources = max confidence

            signals.append(
                CorrelatedSignal(
                    topic=topic,
                    source_count=len(sources),
                    sources=list(sources.keys()),
                    items=all_items,
                    confidence=confidence,
                )
            )

    signals.sort(key=lambda s: (s.confidence, s.source_count), reverse=True)
    return signals
