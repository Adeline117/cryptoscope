"""Telegram inline keyboard builders for CryptoScope bot.

All functions return ``InlineKeyboardMarkup`` instances from python-telegram-bot v20.
"""

from __future__ import annotations

from telegram import InlineKeyboardButton, InlineKeyboardMarkup


def price_action_keyboard(symbol: str) -> InlineKeyboardMarkup:
    """Keyboard attached to price query results.

    Layout:
      [📊 K线图] [📈 7天图]
      [⭐ 加入关注] [🔔 价格提醒]
      [💰 费率] [🐋 鲸鱼]
    """
    sym = symbol.upper()
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("📊 K线图", callback_data=f"chart_kline:{sym}"),
                InlineKeyboardButton("📈 7天图", callback_data=f"chart_7d:{sym}"),
            ],
            [
                InlineKeyboardButton("⭐ 加入关注", callback_data=f"watchlist_add:{sym}"),
                InlineKeyboardButton("🔔 价格提醒", callback_data=f"price_alert:{sym}"),
            ],
            [
                InlineKeyboardButton("💰 费率", callback_data=f"funding:{sym}"),
                InlineKeyboardButton("🐋 鲸鱼", callback_data=f"whale:{sym}"),
            ],
        ]
    )


def highlight_action_keyboard(item_id: str) -> InlineKeyboardMarkup:
    """Keyboard attached to highlight push items.

    Layout:
      [📖 看全文] [🔗 原文]
      [🇬🇧 English] [🇨🇳 中文]
    """
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("📖 看全文", callback_data=f"full_article:{item_id}"),
                InlineKeyboardButton("🔗 原文", callback_data=f"source_url:{item_id}"),
            ],
            [
                InlineKeyboardButton("🇬🇧 English", callback_data=f"lang_en:{item_id}"),
                InlineKeyboardButton("🇨🇳 中文", callback_data=f"lang_zh:{item_id}"),
            ],
        ]
    )


def alert_action_keyboard() -> InlineKeyboardMarkup:
    """Keyboard attached to critical alerts.

    Layout:
      [📊 风险面板] [📉 查价格]
      [🔇 静音1h] [🔇 静音4h]
    """
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("📊 风险面板", callback_data="cmd:risk"),
                InlineKeyboardButton("📉 查价格", callback_data="cmd:price_btc"),
            ],
            [
                InlineKeyboardButton("🔇 静音1h", callback_data="mute:1h"),
                InlineKeyboardButton("🔇 静音4h", callback_data="mute:4h"),
            ],
        ]
    )


def meme_action_keyboard(symbol: str, chain: str = "", address: str = "") -> InlineKeyboardMarkup:
    """Keyboard attached to meme coin alerts.

    Layout:
      [📊 DexScreener] [🛡️ RugCheck]
      [🐋 聪明钱] [📈 Alpha详情]
      [⭐ 关注] [🔇 静音此币]
    """
    sym = symbol.upper()
    addr_short = address[:8] if address else ""
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "📊 DexScreener",
                    url=f"https://dexscreener.com/{chain}/{address}" if chain and address else f"https://dexscreener.com/search?q={sym}",
                ),
                InlineKeyboardButton(
                    "🛡️ RugCheck",
                    url=f"https://rugcheck.xyz/tokens/{address}" if address else "https://rugcheck.xyz",
                ),
            ],
            [
                InlineKeyboardButton("🐋 聪明钱", callback_data=f"meme_sm:{sym}"),
                InlineKeyboardButton("📈 Alpha详情", callback_data=f"meme_alpha:{sym}"),
            ],
            [
                InlineKeyboardButton("⭐ 关注", callback_data=f"watchlist_add:{sym}"),
                InlineKeyboardButton("🔇 静音此币", callback_data=f"meme_mute:{addr_short}"),
            ],
        ]
    )


def main_menu_keyboard() -> InlineKeyboardMarkup:
    """Main menu keyboard for /start or /menu.

    Layout:
      [💰BTC] [💰ETH] [💰SOL]
      [😱F&G] [📊风险] [💧费率]
      [🔥立即扫描] [⭐关注列表]
    """
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("💰BTC", callback_data="cmd:price_btc"),
                InlineKeyboardButton("💰ETH", callback_data="cmd:price_eth"),
                InlineKeyboardButton("💰SOL", callback_data="cmd:price_sol"),
            ],
            [
                InlineKeyboardButton("😱F&G", callback_data="cmd:fg"),
                InlineKeyboardButton("📊风险", callback_data="cmd:risk"),
                InlineKeyboardButton("💧费率", callback_data="cmd:funding"),
            ],
            [
                InlineKeyboardButton("🔥立即扫描", callback_data="cmd:snapshot"),
                InlineKeyboardButton("🎰MEME扫描", callback_data="cmd:meme_scan"),
            ],
            [
                InlineKeyboardButton("⭐关注列表", callback_data="cmd:watchlist"),
            ],
        ]
    )
