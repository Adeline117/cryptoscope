"""Format generated threads for different platforms."""

from __future__ import annotations

from src.content.thread_writer import GeneratedThread


def format_for_twitter(thread: GeneratedThread, lang: str = "en") -> list[str]:
    """Format thread as list of tweet-ready strings with char count validation."""
    tweets = []
    total = len(thread.tweets)
    for t in thread.tweets:
        text = t.text_en if lang == "en" else t.text_zh
        # Add thread numbering
        prefix = f"{t.tweet_number}/{total} "
        full = prefix + text

        # Validate character count
        if len(full) > 280:
            full = full[:277] + "..."

        tweets.append(full)
    return tweets


def format_for_telegram(thread: GeneratedThread) -> str:
    """Format full thread as a single Telegram message with both languages."""
    lines = []
    lines.append(f"📊 <b>{thread.topic}</b>\n")

    lines.append("<b>🇬🇧 English Thread:</b>\n")
    for t in thread.tweets:
        lines.append(f"{t.tweet_number}. {t.text_en}")
        if t.source_url:
            lines.append(f"   🔗 {t.source_url}")

    lines.append("\n<b>🇨🇳 中文版:</b>\n")
    for t in thread.tweets:
        lines.append(f"{t.tweet_number}. {t.text_zh}")

    if thread.charts:
        lines.append(f"\n📈 Charts: {len(thread.charts)} attached")

    return "\n".join(lines)


def format_twitter_thread_copyable(thread: GeneratedThread, lang: str = "en") -> str:
    """Format entire thread as a single copyable text block for Telegram.

    Each tweet separated by blank line, numbered. Ready to paste into X
    tweet-by-tweet.
    """
    tweets = format_for_twitter(thread, lang)
    # Add a placeholder for the last tweet's OpenClaw link
    last = tweets[-1]
    if "openclaw" not in last.lower() and "full analysis" not in last.lower():
        tweets.append(f"{len(tweets)+1}/{len(tweets)+1} Full analysis: [PASTE OPENCLAW URL HERE]")
    return "\n\n".join(tweets)
