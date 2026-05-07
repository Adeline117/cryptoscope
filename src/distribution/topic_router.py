"""Forum Topic router for Telegram Supergroup with topics (forum) enabled.

Maps message types to specific forum topic (thread) IDs, and provides
a helper to send messages to the correct topic with fallback to main chat.
"""

from __future__ import annotations

import os

import structlog

logger = structlog.get_logger()

# Message type -> environment variable name for the topic ID
TOPIC_ENV_MAP: dict[str, str] = {
    "critical": "TG_TOPIC_CRITICAL",
    "whale": "TG_TOPIC_WHALE",
    "highlight": "TG_TOPIC_HIGHLIGHTS",
    "daily": "TG_TOPIC_DAILY",
    "meme": "TG_TOPIC_MEME",
}


def get_topic_id(message_type: str) -> int | None:
    """Return the forum topic (message_thread_id) for the given message type.

    Returns None if the environment variable is not set or the type is unknown,
    which means the message should be sent to the main/general chat.
    """
    env_var = TOPIC_ENV_MAP.get(message_type.lower())
    if env_var is None:
        return None
    raw = os.environ.get(env_var)
    if raw is None:
        return None
    try:
        return int(raw)
    except (ValueError, TypeError):
        logger.warning("invalid_topic_id", env_var=env_var, raw_value=raw)
        return None


async def send_to_topic(
    bot,
    chat_id: int | str,
    text: str,
    message_type: str,
    parse_mode: str = "HTML",
    reply_markup=None,
    disable_notification: bool = False,
) -> bool:
    """Send a message to the appropriate forum topic, falling back to main chat.

    Args:
        bot: telegram.Bot instance
        chat_id: Supergroup or chat ID
        text: Message text (HTML)
        message_type: One of "critical", "whale", "highlight", "daily"
        parse_mode: Parse mode (default "HTML")
        reply_markup: Optional InlineKeyboardMarkup
        disable_notification: If True, send silently
    Returns:
        True if sent successfully, False otherwise.
    """
    topic_id = get_topic_id(message_type)

    kwargs: dict = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": parse_mode,
        "disable_notification": disable_notification,
    }
    if reply_markup is not None:
        kwargs["reply_markup"] = reply_markup
    if topic_id is not None:
        kwargs["message_thread_id"] = topic_id

    try:
        await bot.send_message(**kwargs)
        logger.info(
            "topic_message_sent",
            message_type=message_type,
            topic_id=topic_id,
            chat_id=chat_id,
        )
        return True
    except Exception as e:
        # If topic send fails (e.g. topic deleted), fall back to main chat
        if topic_id is not None:
            logger.warning(
                "topic_send_failed_fallback",
                message_type=message_type,
                topic_id=topic_id,
                error=str(e),
            )
            try:
                kwargs.pop("message_thread_id", None)
                await bot.send_message(**kwargs)
                logger.info("topic_fallback_sent", message_type=message_type)
                return True
            except Exception as e2:
                logger.error("topic_fallback_also_failed", error=str(e2))
                return False
        else:
            logger.error(
                "topic_message_send_failed",
                message_type=message_type,
                error=str(e),
            )
            return False
