"""OpenClaw long-form draft generator.

You publish on OpenClaw manually — either by:
1. Using your Claude subscription to expand the thread into a long-form article
2. Or starting from the Markdown draft this module generates

This is a helper, not an API integration.
"""

from __future__ import annotations

from datetime import datetime, timezone

from src.content.thread_writer import GeneratedThread


def generate_draft(thread: GeneratedThread, lang: str = "en") -> str:
    """Generate a Markdown starting draft for OpenClaw.

    You'll likely rewrite/expand this with Claude before publishing.
    """
    lines: list[str] = []
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    lines.append(f"# {thread.topic}")
    lines.append(f"*{now} · CryptoScope*")
    lines.append("")

    for t in thread.tweets:
        text = t.text_en if lang == "en" else t.text_zh
        lines.append(text)
        lines.append("")
        if t.media:
            lines.append(f"![{t.media}]({t.media})")
            lines.append("")
        if t.source_url:
            lines.append(f"> Source: {t.source_url}")
            lines.append("")

    lines.append("---")
    lines.append("*[Arena](https://arenafi.org) · CryptoScope*")

    return "\n".join(lines)
