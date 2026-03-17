"""Detect emerging narratives from collected text sources using Claude API."""

from __future__ import annotations

import json
import os
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
        # Extract JSON from response
        start = result_text.find("{")
        end = result_text.rfind("}") + 1
        if start >= 0 and end > start:
            parsed = json.loads(result_text[start:end])
            parsed["method"] = "llm"
            return parsed

    except Exception as e:
        logger.warning("llm_narrative_detection_failed", error=str(e))

    return {"narratives": extract_keyword_narratives(items), "method": "keyword_fallback"}
