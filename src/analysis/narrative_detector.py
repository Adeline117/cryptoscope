"""Detect emerging narratives from collected text sources using Claude API."""

from __future__ import annotations

import json
import os
import re
from collections import Counter
from datetime import datetime, timezone

import structlog

from src.collectors.base import CollectedItem

logger = structlog.get_logger()

# Known narrative tags to track
KNOWN_NARRATIVES = [
    "AI agents", "RWA", "restaking", "L2 wars", "modular blockchain",
    "intent-based", "account abstraction", "chain abstraction",
    "liquid staking", "liquid restaking", "points meta", "airdrop",
    "memecoin", "SocialFi", "DePIN", "DeSci", "GameFi",
    "Bitcoin L2", "Bitcoin DeFi", "Solana DeFi", "Base ecosystem",
    "ETF flows", "institutional adoption", "regulatory",
    "stablecoin", "CBDC", "privacy", "ZK proofs",
    "MEV", "PBS", "proposer-builder separation",
    "cross-chain", "interoperability", "bridge exploit",
    "governance attack", "token migration", "protocol merge",
]

# Narrative keyword mapping for keyword-based detection
NARRATIVES: dict[str, list[str]] = {
    "farcaster_frames": ["farcaster", "frames", "onchain social", "lens protocol"],
    "bitcoin_l2": ["bitcoin l2", "runes", "inscriptions", "ordinals", "stacks", "lightning"],
    "intent_architecture": ["intent", "intent-based", "solver", "order flow auction"],
    "eigenlayer_avs": ["eigenlayer", "avs", "actively validated", "restaking operator"],
    "modular_blockchain": ["modular", "celestia", "data availability", "da layer", "avail"],
    "fhe_privacy": ["fhe", "fully homomorphic", "fhevm", "zama", "encrypted computation"],
    "agent_framework": ["ai agent", "agent framework", "autonomous agent", "agent infrastructure"],
}


def extract_keyword_narratives(items: list[CollectedItem]) -> dict[str, int]:
    """Simple keyword-based narrative detection (no API needed)."""
    narrative_counts: Counter = Counter()

    for item in items:
        text = (item.title + " " + item.content).lower()
        for narrative in KNOWN_NARRATIVES:
            if narrative.lower() in text:
                narrative_counts[narrative] += 1

    return dict(narrative_counts.most_common(20))


async def detect_narratives_with_llm(
    items: list[CollectedItem],
    max_items: int = 50,
) -> dict:
    """Use Claude API to cluster items and detect emerging narratives."""
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        logger.warning("no_anthropic_key_for_narratives")
        return {"narratives": extract_keyword_narratives(items), "method": "keyword"}

    try:
        import anthropic

        client = anthropic.Anthropic(api_key=api_key)

        # Prepare input: titles + short summaries of recent items
        summaries = []
        for item in items[:max_items]:
            pub = item.published_at.isoformat() if item.published_at else "unknown"
            summaries.append(f"[{pub}] {item.title}\n{item.content[:200]}")

        input_text = "\n---\n".join(summaries)

        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=2000,
            messages=[
                {
                    "role": "user",
                    "content": (
                        "Analyze these recent crypto news items and identify the top emerging narratives. "
                        "For each narrative, provide:\n"
                        "1. Narrative name (short tag)\n"
                        "2. Strength (1-10, based on how many sources mention it)\n"
                        "3. Trend (emerging/peak/declining)\n"
                        "4. Key data points supporting it\n\n"
                        f"Items:\n{input_text}\n\n"
                        "Return as JSON: {\"narratives\": [{\"name\": str, \"strength\": int, "
                        "\"trend\": str, \"evidence\": [str]}]}"
                    ),
                }
            ],
        )

        result_text = response.content[0].text

        # Try parsing full response as JSON first
        parsed = None
        try:
            parsed = json.loads(result_text)
        except json.JSONDecodeError:
            # Fallback: extract JSON between first { and last }
            match = re.search(r"\{.*\}", result_text, re.DOTALL)
            if match:
                try:
                    parsed = json.loads(match.group())
                except json.JSONDecodeError:
                    pass

        if parsed is not None:
            parsed["method"] = "llm"
            return parsed

        # Final fallback: keyword method
        return {"narratives": extract_keyword_narratives(items), "method": "keyword_fallback"}

    except Exception as e:
        logger.warning("llm_narrative_detection_failed", error=str(e))

    return {"narratives": extract_keyword_narratives(items), "method": "keyword_fallback"}
