"""Crypto news collector using CryptoPanic free public API."""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any

from src.collectors.base import BaseCollector, CollectedItem, CollectionResult

# Keyword-based categorization when API tags are insufficient
CATEGORY_KEYWORDS: dict[str, list[str]] = {
    "regulatory": [
        "sec", "regulation", "compliance", "law", "legal", "ban", "sanction",
        "cftc", "congress", "senate", "legislation", "tax", "irs", "enforcement",
        "court", "ruling", "policy", "government", "federal", "mica",
    ],
    "market": [
        "price", "rally", "dump", "crash", "ath", "bull", "bear", "volume",
        "market cap", "trading", "etf", "spot", "futures", "options", "whale",
        "liquidation", "long", "short", "breakout", "resistance", "support",
    ],
    "defi": [
        "defi", "dex", "amm", "yield", "farming", "liquidity", "lending",
        "borrow", "stake", "staking", "tvl", "protocol", "dao", "swap",
        "aave", "uniswap", "compound", "maker", "curve",
    ],
    "nft": [
        "nft", "opensea", "collectible", "mint", "pfp", "metaverse",
        "digital art", "ordinals", "inscription", "blur",
    ],
    "security": [
        "hack", "exploit", "vulnerability", "breach", "scam", "rug pull",
        "phishing", "malware", "drain", "attack", "theft", "stolen",
        "audit", "bug bounty",
    ],
    "technology": [
        "upgrade", "fork", "merge", "layer 2", "l2", "rollup", "zk",
        "scaling", "bridge", "cross-chain", "interoperability", "consensus",
        "sharding", "eip", "bip", "testnet", "mainnet",
    ],
    "adoption": [
        "adoption", "institutional", "payment", "partnership", "integrate",
        "launch", "accept", "retail", "merchant", "cbdc", "tokenization",
    ],
}


def categorize_text(text: str, api_tags: list[str] | None = None) -> str:
    """Categorize a news item based on API tags and keyword matching.

    Args:
        text: Combined title and content text for keyword matching.
        api_tags: Tags or categories provided by the API.

    Returns:
        A category string (e.g., 'regulatory', 'market', 'defi').
    """
    # Check API tags first
    if api_tags:
        tag_str = " ".join(t.lower() for t in api_tags)
        for category, keywords in CATEGORY_KEYWORDS.items():
            for kw in keywords:
                if kw in tag_str:
                    return category

    # Fall back to keyword matching on text
    text_lower = text.lower()
    best_category = "general"
    best_score = 0
    for category, keywords in CATEGORY_KEYWORDS.items():
        score = sum(1 for kw in keywords if kw in text_lower)
        if score > best_score:
            best_score = score
            best_category = category

    return best_category


class CryptoNewsAPICollector(BaseCollector):
    """Collect crypto news from CryptoPanic free public API.

    Uses the CryptoPanic public endpoint which provides recent crypto news
    aggregated from multiple sources. No API key required for the free tier.
    """

    source_id = "crypto_news_api"
    source_name = "CryptoPanic Crypto News"
    source_type = "api"

    BASE_URL = "https://cryptopanic.com/api/free/v1/posts/"

    def __init__(self, **kwargs):
        super().__init__(cache_ttl=900, **kwargs)

    def _parse_datetime(self, dt_str: str | None) -> datetime | None:
        """Parse ISO datetime string from the API."""
        if not dt_str:
            return None
        try:
            # CryptoPanic uses ISO 8601 format
            cleaned = dt_str.replace("Z", "+00:00")
            return datetime.fromisoformat(cleaned)
        except (ValueError, TypeError):
            return None

    def _make_item_id(self, post: dict[str, Any]) -> str:
        """Generate a stable ID from a post."""
        post_id = post.get("id")
        if post_id:
            return f"cpanic_{post_id}"
        # Fallback: hash the URL or title
        raw = post.get("url", "") or post.get("title", "")
        return f"cpanic_{hashlib.sha256(raw.encode()).hexdigest()[:12]}"

    async def _collect(self) -> CollectionResult:
        """Fetch latest news from CryptoPanic free API."""
        items: list[CollectedItem] = []

        try:
            data = await self._fetch_json(
                self.BASE_URL,
                params={
                    "auth_token": "free",
                    "public": "true",
                },
            )

            posts: list[dict[str, Any]] = data.get("results", [])

            for post in posts:
                title = post.get("title", "Untitled")
                url = post.get("url", "")
                source_info = post.get("source", {})
                source_title = source_info.get("title", "") if isinstance(source_info, dict) else ""

                # Extract tags/currencies from API
                currencies = post.get("currencies", []) or []
                currency_codes = [
                    c.get("code", "") for c in currencies if isinstance(c, dict)
                ]

                api_tags = currency_codes.copy()
                if post.get("kind"):
                    api_tags.append(post["kind"])

                # Categorize the article
                category = categorize_text(title, api_tags)

                published_at = self._parse_datetime(post.get("published_at"))

                metadata: dict[str, Any] = {
                    "data_type": "crypto_news",
                    "category": category,
                    "source_title": source_title,
                    "source_domain": source_info.get("domain", "") if isinstance(source_info, dict) else "",
                    "currencies": currency_codes,
                    "kind": post.get("kind", "news"),
                    "votes": post.get("votes", {}),
                    "priority": "high" if category in ("regulatory", "security") else "medium",
                }

                items.append(
                    CollectedItem(
                        id=self._make_item_id(post),
                        title=title,
                        content=title,  # Free API does not provide full content
                        url=url,
                        published_at=published_at,
                        metadata=metadata,
                        raw=post,
                    )
                )

            self.log.info(
                "crypto_news_collected",
                total=len(items),
                categories={
                    cat: sum(1 for it in items if it.metadata.get("category") == cat)
                    for cat in set(it.metadata.get("category", "") for it in items)
                },
            )

        except Exception as e:
            self.log.error("crypto_news_api_error", error=str(e))
            # Return empty result on failure; base class logs the error
            return CollectionResult(
                source_id=self.source_id,
                source_name=self.source_name,
                source_type=self.source_type,
            )

        return CollectionResult(
            source_id=self.source_id,
            source_name=self.source_name,
            source_type=self.source_type,
            items=items,
        )
