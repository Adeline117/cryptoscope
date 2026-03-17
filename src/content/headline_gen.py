"""Generate alternative opening tweets for threads."""

from __future__ import annotations

import json
import os

import structlog

logger = structlog.get_logger()


async def generate_headlines(
    topic: str,
    data_points: list[str],
    count: int = 5,
) -> list[dict[str, str]]:
    """Generate alternative hook tweets for a thread topic.

    Returns list of {text_en, text_zh} dicts.
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        # Fallback: return a basic headline
        return [{"text_en": topic, "text_zh": topic}]

    import anthropic

    client = anthropic.AsyncAnthropic(api_key=api_key)

    response = await client.messages.create(
        model=os.environ.get("CRYPTOSCOPE_CLAUDE_MODEL", "claude-sonnet-4-20250514"),
        max_tokens=2000,
        messages=[
            {
                "role": "user",
                "content": (
                    f"Generate {count} alternative opening tweets for a crypto research thread.\n\n"
                    f"Topic: {topic}\n"
                    f"Key data: {'; '.join(data_points[:5])}\n\n"
                    "Requirements:\n"
                    "- Each must be <= 280 characters\n"
                    "- Create urgency or curiosity\n"
                    "- Lead with data, not hype\n"
                    "- No 🚀💎🔥 emojis\n"
                    "- Include both English and Chinese versions\n\n"
                    'Return JSON: [{"text_en": "...", "text_zh": "..."}, ...]'
                ),
            }
        ],
    )

    text = response.content[0].text
    start = text.find("[")
    end = text.rfind("]") + 1
    if start >= 0 and end > start:
        return json.loads(text[start:end])

    return [{"text_en": topic, "text_zh": topic}]
