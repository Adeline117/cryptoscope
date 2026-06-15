"""Send thread + long-form analysis directly to you via Telegram bot.

Flow:
1. Auto-generate thread + charts + long-form analysis
2. All sent to your Telegram bot DM
3. You review → copy thread to X, long-form to OpenClaw (if you want)
"""

from __future__ import annotations

import asyncio
import os
from pathlib import Path

import structlog

logger = structlog.get_logger()


def _get_chat_id() -> str | None:
    return os.environ.get("TG_REVIEW_CHANNEL")


async def send_thread_for_review(
    topic: str,
    priority_score: float,
    sources_used: list[str],
    thread_text_en: str,
    thread_text_zh: str,
    long_form_en: str = "",
    long_form_zh: str = "",
    chart_paths: list[Path] | None = None,
    **kwargs,
) -> bool:
    """Send thread + long-form analysis directly to you.

    Messages:
    1. Header (topic, score, sources)
    2. EN thread (copy to X)
    3. ZH thread (copy to X)
    4. Charts
    5. EN long-form analysis
    6. ZH long-form analysis (长文版)
    """
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = _get_chat_id()

    if not bot_token or not chat_id:
        logger.warning("telegram_not_configured")
        return False

    try:
        from telegram import Bot

        bot = Bot(token=bot_token)
        any_sent = False

        # 1. Header
        try:
            sources_text = "\n".join(f"  • {s}" for s in sources_used[:10])
            await bot.send_message(
                chat_id=chat_id,
                text=(
                    f"📊 <b>{_esc(topic)}</b>\n"
                    f"Score: {priority_score:.0f}/100\n\n"
                    f"Sources:\n{_esc(sources_text)}"
                ),
                parse_mode="HTML",
            )
            any_sent = True
        except Exception as e:
            logger.error("telegram_header_failed", error=str(e))

        # 2. EN thread
        try:
            await asyncio.sleep(0.3)
            await bot.send_message(
                chat_id=chat_id,
                text=f"🇬🇧 <b>EN Thread:</b>\n\n{_esc(thread_text_en)}",
                parse_mode="HTML",
            )
            any_sent = True
        except Exception as e:
            logger.error("telegram_en_thread_failed", error=str(e))

        # 3. ZH thread
        try:
            await asyncio.sleep(0.3)
            await bot.send_message(
                chat_id=chat_id,
                text=f"🇨🇳 <b>中文 Thread:</b>\n\n{_esc(thread_text_zh)}",
                parse_mode="HTML",
            )
            any_sent = True
        except Exception as e:
            logger.error("telegram_zh_thread_failed", error=str(e))

        # 4. Charts
        try:
            if chart_paths:
                for p in chart_paths:
                    if p.exists():
                        await asyncio.sleep(0.3)
                        with open(p, "rb") as f:
                            await bot.send_photo(chat_id=chat_id, photo=f)
                        any_sent = True
        except Exception as e:
            logger.error("telegram_charts_failed", error=str(e))

        # 5. EN long-form analysis
        try:
            if long_form_en:
                await asyncio.sleep(0.3)
                await _send_long_text(
                    bot, chat_id,
                    f"📝 <b>EN Full Analysis:</b>\n\n{_esc(long_form_en)}",
                )
                any_sent = True
        except Exception as e:
            logger.error("telegram_long_en_failed", error=str(e))

        # 6. ZH long-form analysis
        try:
            if long_form_zh:
                await asyncio.sleep(0.3)
                await _send_long_text(
                    bot, chat_id,
                    f"📝 <b>中文长文分析:</b>\n\n{_esc(long_form_zh)}",
                )
                any_sent = True
        except Exception as e:
            logger.error("telegram_long_zh_failed", error=str(e))

        logger.info("telegram_sent", topic=topic, any_sent=any_sent)
        return any_sent

    except Exception as e:
        logger.error("telegram_send_failed", error=str(e))
        return False


async def _send_long_text(bot, chat_id: str, text: str) -> None:
    """Send long text, splitting into multiple messages if needed (4096 char limit)."""
    chunks = _split_text(text, 4000)
    for i, chunk in enumerate(chunks):
        if i > 0:
            await asyncio.sleep(0.3)
        await bot.send_message(
            chat_id=chat_id,
            text=chunk,
            parse_mode="HTML",
        )


def _split_text(text: str, max_len: int) -> list[str]:
    """Split text at paragraph boundaries."""
    if len(text) <= max_len:
        return [text]

    chunks = []
    current = ""
    for line in text.split("\n"):
        if len(current) + len(line) + 1 > max_len:
            if current:
                chunks.append(current)
            current = line
        else:
            current = current + "\n" + line if current else line
    if current:
        chunks.append(current)
    return chunks


async def send_daily_digest(
    summary: str,
    item_count: int,
    top_topics: list[str],
) -> bool:
    """Send daily digest."""
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = _get_chat_id()

    if not bot_token or not chat_id:
        return False

    try:
        from telegram import Bot

        bot = Bot(token=bot_token)
        topics_text = "\n".join(f"  • {t}" for t in top_topics[:10])
        await bot.send_message(
            chat_id=chat_id,
            text=(
                f"📋 <b>Daily Digest</b>\n\n"
                f"<b>{item_count}</b> items collected\n\n"
                f"<b>Top Topics:</b>\n{_esc(topics_text)}\n\n"
                f"{_esc(summary)}"
            ),
            parse_mode="HTML",
        )
        return True
    except Exception as e:
        logger.error("digest_send_failed", error=str(e))
        return False


async def send_alert(message: str) -> bool:
    """Send an alert. The message is sent AS-IS with parse_mode=HTML.

    Callers pass pre-formatted HTML (the message templates already escape their
    own interpolated values). It must NOT be re-escaped here — doing so turned
    every <b>/<code> tag into literal &lt;b&gt; text (garbled messages). Long
    messages are split at the 4096-char limit.
    """
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = _get_chat_id()

    if not bot_token or not chat_id:
        return False

    try:
        from telegram import Bot

        bot = Bot(token=bot_token)
        for i, chunk in enumerate(_split_text(message, 4000)):
            if i > 0:
                await asyncio.sleep(0.3)
            await bot.send_message(
                chat_id=chat_id, text=chunk, parse_mode="HTML",
                disable_web_page_preview=True,
            )
        return True
    except Exception as e:
        logger.error("alert_send_failed", error=str(e))
        return False


async def send_meme_alert(message: str, reply_markup=None) -> bool:
    """Send a pre-formatted meme/pool alert (HTML) to the review channel.

    The message is already HTML-escaped by the message templates, so it is sent
    as-is with parse_mode="HTML". Long messages are split at the 4096-char limit;
    an optional inline keyboard is attached to the final chunk.
    """
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = _get_chat_id()

    if not bot_token or not chat_id:
        logger.warning("telegram_not_configured")
        return False

    try:
        from telegram import Bot

        bot = Bot(token=bot_token)
        chunks = _split_text(message, 4000)
        for i, chunk in enumerate(chunks):
            if i > 0:
                await asyncio.sleep(0.3)
            await bot.send_message(
                chat_id=chat_id,
                text=chunk,
                parse_mode="HTML",
                # Attach keyboard only to the last chunk
                reply_markup=reply_markup if i == len(chunks) - 1 else None,
            )
        return True
    except Exception as e:
        logger.error("meme_alert_send_failed", error=str(e))
        return False


async def send_critical_alert(message: str) -> bool:
    """Send a pre-formatted P0 critical alert (HTML) to the review channel.

    Like send_meme_alert, the message is already HTML-formatted and is sent
    without re-escaping. Long messages are split at the 4096-char limit.
    """
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = _get_chat_id()

    if not bot_token or not chat_id:
        logger.warning("telegram_not_configured")
        return False

    try:
        from telegram import Bot

        bot = Bot(token=bot_token)
        for i, chunk in enumerate(_split_text(message, 4000)):
            if i > 0:
                await asyncio.sleep(0.3)
            await bot.send_message(
                chat_id=chat_id,
                text=chunk,
                parse_mode="HTML",
            )
        return True
    except Exception as e:
        logger.error("critical_alert_send_failed", error=str(e))
        return False


def _esc(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
