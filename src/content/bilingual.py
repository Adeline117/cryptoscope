"""Bilingual content handling and glossary enforcement."""

from __future__ import annotations

import re

from src.content.jargon_glossary import GLOSSARY
from src.content.thread_writer import GeneratedThread


def verify_data_consistency(thread: GeneratedThread) -> list[str]:
    """Verify that key data points are consistent between EN and ZH versions.

    Returns list of warnings if inconsistencies found.
    """
    import re

    warnings = []
    # Extract numbers from both versions
    number_pattern = re.compile(r"\$[\d,.]+[BMK]?|\d+\.?\d*%|\d{1,3}(?:,\d{3})+")

    for tweet in thread.tweets:
        en_numbers = set(number_pattern.findall(tweet.text_en))
        zh_numbers = set(number_pattern.findall(tweet.text_zh))

        if en_numbers and zh_numbers:
            en_only = en_numbers - zh_numbers
            zh_only = zh_numbers - en_numbers
            if en_only or zh_only:
                warnings.append(
                    f"Tweet {tweet.tweet_number}: EN has {en_only}, ZH has {zh_only}"
                )

    return warnings


def enforce_glossary(text_zh: str) -> str:
    """Ensure consistent Chinese crypto terminology."""
    for en_term, zh_term in GLOSSARY.items():
        # Replace English terms that appear in the Chinese text
        # Use case-insensitive word boundary matching
        pattern = re.compile(r'\b' + re.escape(en_term) + r'\b', re.IGNORECASE)
        text_zh = pattern.sub(zh_term, text_zh)
    return text_zh
