"""Classify collected items by topic and sector."""

from __future__ import annotations

from src.collectors.base import CollectedItem

# Topic keyword mapping
TOPIC_KEYWORDS: dict[str, list[str]] = {
    "defi_lending": ["aave", "compound", "morpho", "lending", "borrow", "liquidat"],
    "dex_amm": ["uniswap", "sushiswap", "curve", "dex", "amm", "swap", "liquidity pool"],
    "derivatives": ["perp", "futures", "options", "funding rate", "open interest", "gmx", "dydx", "hyperliquid"],
    "stablecoin": ["usdc", "usdt", "dai", "stablecoin", "depeg", "tether", "circle"],
    "l2_scaling": ["arbitrum", "optimism", "base", "zksync", "starknet", "scroll", "l2", "rollup", "layer 2"],
    "restaking": ["eigenlayer", "restaking", "avs", "liquid restaking"],
    "bridge": ["bridge", "cross-chain", "wormhole", "layerzero", "stargate", "across"],
    "governance": ["governance", "proposal", "vote", "dao", "snapshot", "tally"],
    "security": ["hack", "exploit", "vulnerability", "audit", "rug pull", "drain"],
    "regulatory": ["sec", "cftc", "regulation", "compliance", "etf", "micas", "license"],
    "mev": ["mev", "flashbots", "sandwich", "frontrun", "backrun", "builder", "searcher"],
    "nft": ["nft", "opensea", "blur", "collection", "mint"],
    "bitcoin": ["bitcoin", "btc", "ordinals", "inscription", "lightning"],
    "solana": ["solana", "sol", "jupiter", "raydium", "jito", "marinade"],
    "ai_crypto": ["ai agent", "artificial intelligence", "machine learning", "gpt", "llm"],
    "rwa": ["rwa", "real world asset", "tokeniz", "ondo", "centrifuge", "treasury"],
    "infrastructure": ["chainlink", "oracle", "pyth", "indexer", "subgraph"],
}


def classify_item(item: CollectedItem) -> list[str]:
    """Classify an item into topic categories based on content."""
    text = (item.title + " " + item.content).lower()
    topics = []
    for topic, keywords in TOPIC_KEYWORDS.items():
        if any(kw in text for kw in keywords):
            topics.append(topic)
    return topics or ["general"]


def classify_items(items: list[CollectedItem]) -> dict[str, list[CollectedItem]]:
    """Classify all items and group by topic."""
    grouped: dict[str, list[CollectedItem]] = {}
    for item in items:
        topics = classify_item(item)
        for topic in topics:
            grouped.setdefault(topic, []).append(item)
    return grouped
