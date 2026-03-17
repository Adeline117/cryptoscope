"""Score collected items by newsworthiness and relevance (0-100)."""

from __future__ import annotations

import math
from datetime import datetime, timezone

from src.collectors.base import CollectedItem

# Source tier weights
SOURCE_TIERS: dict[str, float] = {
    "high": 1.0,
    "medium": 0.7,
    "low": 0.4,
}

# Category relevance boosts for Arena-aligned domains
CATEGORY_BOOSTS: dict[str, float] = {
    "exchange_data": 15,
    "onchain_data": 12,
    "defi_protocol": 12,
    "trading": 15,
    "derivatives": 14,
    "mev": 10,
    "governance": 8,
    "academic": 8,
    "regulatory": 10,
    "stablecoin": 10,
    "l2_scaling": 9,
    "security": 12,
}

# Keywords that boost score
HIGH_SIGNAL_KEYWORDS = [
    "exploit", "hack", "vulnerability", "rug pull", "depeg",
    "billion", "record", "all-time", "first ever", "launch",
    "acquisition", "merge", "fork", "upgrade", "migration",
    "sec", "regulation", "ban", "approve", "etf",
    "arena", "sybil", "wash trading",
]

# Viral / shocking patterns — stories that are inherently eye-catching
# and have high potential for X engagement. Scored separately from keywords.
import re

VIRAL_PATTERNS: list[tuple[re.Pattern, float]] = [
    # Huge loss / gain disparity (e.g. "$50M swap → $36K outcome")
    (re.compile(r"\$[\d,.]+[MBmb].*\$[\d,.]+[Kk]", re.I), 20),
    (re.compile(r"\$[\d,.]+[Kk].*\$[\d,.]+[MBmb]", re.I), 15),
    # Massive dollar amounts
    (re.compile(r"\$\d+\s*[Bb]illion", re.I), 18),
    (re.compile(r"\$[\d,.]*[1-9]\d{2,}[Mm]", re.I), 12),  # $100M+
    # Dramatic percentage moves
    (re.compile(r"(\d{3,})%", re.I), 15),  # 100%+ moves
    (re.compile(r"(9[0-9]|[5-8]\d)%\s*(drop|crash|loss|decline|down|fell|plunge)", re.I), 18),
    # Conflict / drama / death / war
    (re.compile(r"(killed|assassination|strike|attack|war|bomb|invasion)", re.I), 15),
    # Arrests / legal drama
    (re.compile(r"(arrested|indicted|charged|sentenced|guilty|fraud|sued)", re.I), 15),
    # Firsts / records / extremes
    (re.compile(r"(first.ever|all.time.high|all.time.low|largest.ever|biggest.ever|record.breaking)", re.I), 15),
    # Shocking outcomes / failures
    (re.compile(r"(only\s+\$[\d,.]+\s+(left|remain|outcome|received))", re.I), 18),
    (re.compile(r"(lost\s+\$[\d,.]+[MBmb]|stole|stolen|drained|wiped)", re.I), 18),
    # Ban / shutdown / collapse
    (re.compile(r"(collapse[ds]?|shut.?down|bankrupt|insolvent|exit.?scam)", re.I), 18),
    # Geopolitical / macro shocks
    (re.compile(r"(sanction|tariff|default|recession|rate.?cut|rate.?hike|emergency)", re.I), 12),
]


def _recency_score(published_at: datetime | None, max_score: float = 25) -> float:
    """Exponential decay: newer = higher score. Full marks within 1h, halves every 6h."""
    if published_at is None:
        return max_score * 0.3  # Unknown date gets low default

    now = datetime.now(timezone.utc)
    hours_ago = (now - published_at).total_seconds() / 3600

    if hours_ago < 0:
        hours_ago = 0
    # Exponential decay with 6h half-life
    return max_score * math.exp(-0.115 * hours_ago)


def _source_tier_score(item: CollectedItem, max_score: float = 20) -> float:
    """Score based on source tier (high/medium/low)."""
    tier = item.metadata.get("priority", "medium")
    multiplier = SOURCE_TIERS.get(tier, 0.5)
    return max_score * multiplier


def _category_boost(item: CollectedItem) -> float:
    """Bonus points for Arena-relevant categories."""
    category = item.metadata.get("category", "")
    subcategory = item.metadata.get("subcategory", "")
    boost = CATEGORY_BOOSTS.get(category, 0) + CATEGORY_BOOSTS.get(subcategory, 0)
    return min(boost, 15)  # Cap at 15


def _keyword_score(item: CollectedItem, max_score: float = 15) -> float:
    """Score based on presence of high-signal keywords."""
    text = (item.title + " " + item.content).lower()
    matches = sum(1 for kw in HIGH_SIGNAL_KEYWORDS if kw in text)
    return min(matches * 3, max_score)


def _viral_score(item: CollectedItem, max_score: float = 20) -> float:
    """Score based on how shocking / viral the title + content are.

    Matches patterns that indicate inherently eye-catching stories:
    huge losses, dramatic percentages, arrests, collapses, etc.
    """
    text = item.title + " " + item.content[:500]
    best = 0.0
    for pattern, points in VIRAL_PATTERNS:
        if pattern.search(text):
            best = max(best, points)
    return min(best, max_score)


def _anomaly_score(item: CollectedItem, max_score: float = 15) -> float:
    """Score based on anomaly indicators in metadata."""
    score = 0.0
    meta = item.metadata

    # TVL change anomalies
    tvl_1d = meta.get("tvl_change_1d")
    if tvl_1d is not None:
        try:
            change = abs(float(tvl_1d))
            if change > 20:
                score += max_score
            elif change > 10:
                score += max_score * 0.7
            elif change > 5:
                score += max_score * 0.3
        except (ValueError, TypeError):
            pass

    # Engagement signals (if from social sources)
    engagement = meta.get("engagement_score", 0)
    if engagement > 0:
        score += min(engagement / 100 * max_score, max_score * 0.5)

    return min(score, max_score)


def score_item(item: CollectedItem) -> float:
    """Compute priority score (0-100) for a collected item.

    Breakdown (max 110, capped at 100):
      - Recency: 0-25
      - Source tier: 0-20
      - Category: 0-15
      - Keywords: 0-15
      - Viral/shocking: 0-20  ← NEW: rewards inherently eye-catching stories
      - Anomaly: 0-15
    """
    total = (
        _recency_score(item.published_at)
        + _source_tier_score(item)
        + _category_boost(item)
        + _keyword_score(item)
        + _viral_score(item)
        + _anomaly_score(item)
    )
    return min(round(total, 1), 100.0)


def score_and_rank(items: list[CollectedItem]) -> list[tuple[CollectedItem, float]]:
    """Score all items and return sorted by score descending."""
    scored = [(item, score_item(item)) for item in items]
    scored.sort(key=lambda x: x[1], reverse=True)
    return scored


def filter_by_tier(
    scored_items: list[tuple[CollectedItem, float]],
    auto_draft_threshold: float = 70,
    digest_threshold: float = 40,
) -> dict[str, list[tuple[CollectedItem, float]]]:
    """Split scored items into tiers: auto_draft, digest, archive."""
    tiers: dict[str, list[tuple[CollectedItem, float]]] = {
        "auto_draft": [],
        "digest": [],
        "archive": [],
    }
    for item, score in scored_items:
        if score >= auto_draft_threshold:
            tiers["auto_draft"].append((item, score))
        elif score >= digest_threshold:
            tiers["digest"].append((item, score))
        else:
            tiers["archive"].append((item, score))
    return tiers
