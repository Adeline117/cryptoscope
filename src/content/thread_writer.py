"""Generate bilingual Twitter/X threads using Claude API."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass

import structlog

logger = structlog.get_logger()

SYSTEM_PROMPT = """You are a crypto research analyst writing Twitter/X threads.

Style guidelines:
- First tweet must be a hook that creates urgency or curiosity
- Use data to support every claim — no vague statements
- Include specific numbers: "$X billion TVL", "up 47% in 7 days"
- Reference original sources with links
- Tone: smart but accessible, not overly academic, not shilling
- End with a clear takeaway or thesis
- Thread length: 5-12 tweets depending on topic depth
- Use 🔍 📊 ⚡ 🔑 sparingly for visual breaks
- Never use: 🚀 💎 🔥 (too shilly)

For Arena data threads:
- Lead with the insight, not "Arena data shows..."
- Frame around what traders are DOING, not what the tool does
- Always include the arenafi.org link naturally

Output format:
Return a JSON array of tweet objects:
[
  {"tweet_number": 1, "text_en": "...", "text_zh": "...", "media": null, "source_url": "..."},
  ...
]

For Chinese version:
- Not a literal translation — rewrite for Chinese crypto audience
- Use Chinese crypto jargon naturally (链上, 巨鲸, 资金费率, etc.)
- Adjust cultural references if needed
- Keep data points identical

IMPORTANT: Each tweet must be <= 280 characters. Return ONLY the JSON array, no other text."""


@dataclass
class ThreadTweet:
    tweet_number: int
    text_en: str
    text_zh: str
    media: str | None = None
    source_url: str | None = None


@dataclass
class GeneratedThread:
    topic: str
    tweets: list[ThreadTweet]
    model_used: str
    charts: list[str]


async def generate_thread(
    topic: str,
    data_points: list[str],
    source_urls: list[str],
    chart_filenames: list[str] | None = None,
    openclaw_url: str | None = None,
    max_tweets: int = 12,
) -> GeneratedThread:
    """Generate a bilingual thread using Claude API.

    Args:
        topic: The topic/event to write about
        data_points: Key data points to include
        source_urls: URLs to reference
        chart_filenames: Associated chart files
        openclaw_url: OpenClaw article URL for the last tweet
        max_tweets: Maximum number of tweets
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY not set")

    import anthropic

    client = anthropic.AsyncAnthropic(api_key=api_key)

    data_section = "\n".join(f"- {dp}" for dp in data_points)
    sources_section = "\n".join(f"- {url}" for url in source_urls)

    user_prompt = f"""Write a Twitter/X thread about:

Topic: {topic}

Key data points:
{data_section}

Sources:
{sources_section}

Charts available: {chart_filenames or 'none'}

Generate {min(max_tweets, 12)} tweets. Each tweet MUST be <= 280 characters.
"""

    if openclaw_url:
        user_prompt += f"\nInclude this link in the LAST tweet as 'Full analysis': {openclaw_url}"

    model = os.environ.get("CRYPTOSCOPE_CLAUDE_MODEL", "claude-sonnet-4-20250514")

    response = await client.messages.create(
        model=model,
        max_tokens=4000,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_prompt}],
    )

    result_text = response.content[0].text

    # Parse JSON response
    start = result_text.find("[")
    end = result_text.rfind("]") + 1
    if start < 0 or end <= start:
        raise ValueError(f"Failed to parse thread JSON from response: {result_text[:200]}")

    tweets_data = json.loads(result_text[start:end])

    tweets = []
    for t in tweets_data:
        tweets.append(
            ThreadTweet(
                tweet_number=t["tweet_number"],
                text_en=t["text_en"],
                text_zh=t["text_zh"],
                media=t.get("media"),
                source_url=t.get("source_url"),
            )
        )

    logger.info("thread_generated", topic=topic, tweet_count=len(tweets), model=model)

    return GeneratedThread(
        topic=topic,
        tweets=tweets,
        model_used=model,
        charts=chart_filenames or [],
    )
