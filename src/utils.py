"""Shared utility functions."""

from __future__ import annotations


def esc_html(text: str) -> str:
    """Escape text for Telegram HTML, avoiding double-escape."""
    text = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    return text
