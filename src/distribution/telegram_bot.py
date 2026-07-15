"""Interactive Telegram bot with command handlers using python-telegram-bot v20.

Commands:
/snapshot  - Trigger highlight pipeline immediately
/price BTC - Check current price from CoinGecko
/fg        - Check current Fear & Greed index
/funding   - Check BTC funding rate
/risk      - Show risk dashboard summary
/top       - Top 5 gainers & losers
/gas       - ETH gas price
/watchlist - Show watchlist
/whale     - Recent large transfers
/calendar  - This week's major events
/mute 4h   - Mute notifications for N hours
/unmute    - Cancel mute
/help      - Show all commands
"""

from __future__ import annotations

import asyncio
import os
import re
from datetime import datetime, timedelta, timezone

import structlog

logger = structlog.get_logger()

# Global mute state
_mute_until: datetime | None = None

# In-memory watchlist (per-chat persistence would need a DB)
_watchlists: dict[int, list[str]] = {}  # chat_id -> [symbols]


def is_muted() -> bool:
    """Check if bot notifications are currently muted."""
    global _mute_until
    if _mute_until is None:
        return False
    if datetime.now(timezone.utc) >= _mute_until:
        _mute_until = None
        return False
    return True


def get_mute_remaining() -> str:
    """Return human-readable remaining mute time."""
    global _mute_until
    if _mute_until is None:
        return ""
    remaining = _mute_until - datetime.now(timezone.utc)
    if remaining.total_seconds() <= 0:
        return ""
    hours = int(remaining.total_seconds() // 3600)
    minutes = int((remaining.total_seconds() % 3600) // 60)
    if hours > 0:
        return f"{hours}h{minutes}m"
    return f"{minutes}m"


async def _cmd_scan(update, context) -> None:
    """Handle /scan - scan meme tools for smart money signals."""
    await update.message.reply_text("🔍 扫描聪明钱动态中...")
    try:
        from src.collectors.meme_tools import scan_all_sources, format_scan_report
        data = scan_all_sources("sol")
        report = format_scan_report(data)
        if report:
            await update.message.reply_text(report, parse_mode="HTML")
        else:
            await update.message.reply_text("暂无数据")
    except Exception as e:
        logger.error("scan_failed", error=str(e))
        await update.message.reply_text(f"扫描失败: {e}")


async def _cmd_analyze(update, context) -> None:
    """Handle /analyze <address> - deep analysis of a specific token."""
    args = context.args
    if not args:
        await update.message.reply_text("用法: /analyze <合约地址>\n示例: /analyze DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263")
        return

    address = args[0]
    chain = args[1] if len(args) > 1 else "solana"
    await update.message.reply_text(f"🔍 分析中: {address[:16]}...")

    try:
        from src.collectors.meme_tools import analyze_token, format_token_report
        data = analyze_token(chain, address)
        report = format_token_report(data)
        await update.message.reply_text(report, parse_mode="HTML", disable_web_page_preview=True)
    except Exception as e:
        logger.error("analyze_failed", error=str(e))
        await update.message.reply_text(f"分析失败: {e}")


async def _cmd_signals(update, context) -> None:
    """Handle /signals - show current trade signals."""
    try:
        from src.signals.signal_generator import generate_signals, format_signal_message
        signals = generate_signals()
        if not signals:
            await update.message.reply_text("⏸ 当前无交易信号 — 市场平静。")
            return
        for s in signals[:3]:
            msg = format_signal_message(s)
            await update.message.reply_text(msg, parse_mode="HTML")
    except Exception as e:
        logger.error("signals_failed", error=str(e))
        await update.message.reply_text(f"信号生成失败: {e}")


async def _cmd_positions(update, context) -> None:
    """Handle /positions - show paper trading positions and P&L."""
    try:
        from src.trading.paper_trader import format_performance_message, get_open_positions
        msg = format_performance_message()
        await update.message.reply_text(msg, parse_mode="HTML")

        # Show open positions
        positions = get_open_positions()
        if positions:
            lines = ["📊 <b>持仓中:</b>"]
            for p in positions:
                lines.append(f"  {p['direction']} {p['asset']} @ ${p['entry_price']:.4f} · {p['amount_sol']:.3f} SOL")
            await update.message.reply_text("\n".join(lines), parse_mode="HTML")
    except Exception as e:
        logger.error("positions_failed", error=str(e))
        await update.message.reply_text(f"查询失败: {e}")


async def _cmd_scorecard(update, context) -> None:
    """Handle /scorecard - show signal accuracy tracking."""
    try:
        from src.trading.signal_scorecard import format_scorecard_message
        msg = format_scorecard_message()
        await update.message.reply_text(msg, parse_mode="HTML")
    except Exception as e:
        logger.error("scorecard_failed", error=str(e))
        await update.message.reply_text(f"查询失败: {e}")


async def _cmd_watch(update, context) -> None:
    """Handle /watch <address> <label> - add smart money wallet to track."""
    args = context.args
    if not args:
        await update.message.reply_text(
            "用法: /watch <地址> <标签>\n"
            "示例: /watch 7nYB...x3Kp 'Meme猎手'\n\n"
            "当前追踪: 查看 config/smart_money_wallets.yaml"
        )
        return

    address = args[0]
    label = " ".join(args[1:]) if len(args) > 1 else f"用户添加_{address[:8]}"

    # Add to watchlist (append to yaml)
    try:
        import yaml
        config_path = "config/smart_money_wallets.yaml"
        with open(config_path) as f:
            data = yaml.safe_load(f) or {}
        wallets = data.get("wallets", [])

        # Check duplicate
        existing = [w for w in wallets if w.get("address") == address]
        if existing:
            await update.message.reply_text(f"⚠️ 地址已在追踪列表中: {existing[0].get('label', address[:12])}")
            return

        wallets.append({
            "address": address,
            "label": label,
            "chain": "solana" if len(address) < 50 and not address.startswith("0x") else "ethereum",
            "tier": 3,
            "notes": "用户通过Telegram添加",
        })
        data["wallets"] = wallets
        with open(config_path, "w") as f:
            yaml.dump(data, f, allow_unicode=True, default_flow_style=False)

        total = len(wallets)
        await update.message.reply_text(
            f"✅ 已添加追踪钱包\n\n"
            f"地址: {address[:16]}...\n"
            f"标签: {label}\n"
            f"总追踪数: {total}"
        )
    except Exception as e:
        logger.error("watch_add_failed", error=str(e))
        await update.message.reply_text(f"添加失败: {e}")


async def _cmd_check(update, context) -> None:
    """Handle /check <address> - full security scan of a token."""
    args = context.args
    if not args:
        await update.message.reply_text("用法: /check <合约地址>\n示例: /check DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263")
        return

    mint = args[0]
    chain = args[1] if len(args) > 1 else "solana"
    await update.message.reply_text(f"🛡️ 安全检测中: {mint[:16]}...")

    try:
        from src.sniper.rug_detector import full_security_check, format_risk_report
        report = full_security_check(mint, chain)
        msg = format_risk_report(report)
        await update.message.reply_text(msg, parse_mode="HTML")
    except Exception as e:
        logger.error("check_failed", error=str(e))
        await update.message.reply_text(f"检测失败: {e}")


async def _cmd_profile(update, context) -> None:
    """Handle /profile <address> - wallet profile with holdings and stats."""
    args = context.args
    if not args:
        await update.message.reply_text("用法: /profile <钱包地址>\n示例: /profile HWdeC9mMkYzqqfhxkLYmKFq5bgFYNuUxTD2TDJK4pump")
        return

    address = args[0]
    label = " ".join(args[1:]) if len(args) > 1 else ""
    await update.message.reply_text(f"🐋 分析钱包中: {address[:16]}...")

    try:
        from src.sniper.wallet_profiler import profile_wallet, format_wallet_profile
        profile = profile_wallet(address, label)
        msg = format_wallet_profile(profile)
        await update.message.reply_text(msg, parse_mode="HTML")
    except Exception as e:
        logger.error("profile_failed", error=str(e))
        await update.message.reply_text(f"分析失败: {e}")


async def _cmd_smartmoney(update, context) -> None:
    """Handle /smartmoney - recent smart money activity summary."""
    await update.message.reply_text("🐋 查询聪明钱最近动态中...")
    try:
        import yaml
        with open("config/smart_money_wallets.yaml") as f:
            data = yaml.safe_load(f) or {}
        wallets = data.get("wallets", [])
        t1 = [w for w in wallets if w.get("tier") == 1]
        sol_wallets = [w for w in wallets if w.get("chain") == "solana"]
        eth_wallets = [w for w in wallets if w.get("chain") == "ethereum"]

        msg = (
            f"🐋 <b>聪明钱追踪状态</b>\n\n"
            f"总追踪: {len(wallets)} 个钱包\n"
            f"  Solana: {len(sol_wallets)} (T1: {len([w for w in sol_wallets if w.get('tier')==1])})\n"
            f"  Ethereum: {len(eth_wallets)} (T1: {len([w for w in eth_wallets if w.get('tier')==1])})\n\n"
            f"<b>T1钱包 (最高信号):</b>\n"
        )
        for w in t1[:10]:
            label = w.get("label", "?")
            addr = w.get("address", "")[:12]
            msg += f"  · {label} ({addr}...)\n"

        msg += f"\n/watch <地址> <标签> 添加新钱包\n/scan 扫描聪明钱动态"
        await update.message.reply_text(msg, parse_mode="HTML")
    except Exception as e:
        logger.error("smartmoney_failed", error=str(e))
        await update.message.reply_text(f"查询失败: {e}")


async def _cmd_balance(update, context) -> None:
    """Handle /balance [exchange] - show account balance."""
    args = context.args
    exchange_name = args[0].lower() if args else "binance"
    try:
        from src.contract.exchange import get_balance, format_balance_message
        bal = get_balance(exchange_name)
        await update.message.reply_text(format_balance_message(bal), parse_mode="HTML")
    except Exception as e:
        await update.message.reply_text(f"❌ 余额查询失败: {e}\n\n请确认已设置API Key")


async def _cmd_pos(update, context) -> None:
    """Handle /pos [exchange] - show open futures positions."""
    args = context.args
    exchange_name = args[0].lower() if args else "binance"
    try:
        from src.contract.exchange import get_positions, format_positions_message
        positions = get_positions(exchange_name)
        msg = format_positions_message(positions, exchange_name)
        await update.message.reply_text(msg, parse_mode="HTML")
    except Exception as e:
        await update.message.reply_text(f"❌ 持仓查询失败: {e}")


async def _cmd_trade(update, context) -> None:
    """Handle /trade BTC LONG 5x $100 - prepare a futures trade.

    Does NOT auto-execute. Shows risk check + confirmation buttons.
    """
    args = context.args
    if not args or len(args) < 2:
        await update.message.reply_text(
            "用法: /trade <币种> <方向> [杠杆] [金额]\n"
            "示例:\n"
            "  /trade BTC LONG 5x $500\n"
            "  /trade ETH SHORT 10x $200\n"
            "  /trade SOL LONG 3x $100"
        )
        return

    symbol_input = args[0].upper()
    direction = args[1].upper()
    leverage = 5
    amount_usd = 100

    # Parse optional leverage and amount
    for arg in args[2:]:
        arg_lower = arg.lower().replace("$", "").replace("x", "")
        if "x" in arg.lower():
            try:
                leverage = int(arg_lower)
            except ValueError:
                pass
        elif arg.startswith("$") or arg.replace(".", "").isdigit():
            try:
                amount_usd = float(arg.replace("$", ""))
            except ValueError:
                pass

    if direction not in ("LONG", "SHORT"):
        await update.message.reply_text("方向必须是 LONG 或 SHORT")
        return

    symbol = f"{symbol_input}/USDT:USDT"
    side = "buy" if direction == "LONG" else "sell"
    dir_emoji = "🟢" if direction == "LONG" else "🔴"

    # Risk check
    try:
        from src.contract.risk import check_trade_risk, format_risk_check_message
        # Assume $5000 account for risk calc (prop firm default)
        account_balance = 5000
        check = check_trade_risk(account_balance, amount_usd, leverage, 0)
        risk_msg = format_risk_check_message(check)
    except Exception:
        risk_msg = "⚠️ 风控检查不可用"
        check = None

    # Get current price
    price_str = "获取中..."
    try:
        import aiohttp
        coin_map = {"BTC": "bitcoin", "ETH": "ethereum", "SOL": "solana"}
        cg_id = coin_map.get(symbol_input, symbol_input.lower())
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"https://api.coingecko.com/api/v3/simple/price?ids={cg_id}&vs_currencies=usd"
            ) as resp:
                data = await resp.json()
        price = data.get(cg_id, {}).get("usd", 0)
        price_str = f"${price:,.2f}"

        # Calculate TP/SL
        if direction == "LONG":
            tp1 = price * 1.03
            tp2 = price * 1.05
            sl = price * 0.98
        else:
            tp1 = price * 0.97
            tp2 = price * 0.95
            sl = price * 1.02
    except Exception:
        tp1 = tp2 = sl = 0

    msg = (
        f"📊 <b>下单确认</b> | {symbol_input}/USDT\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"方向: {dir_emoji} <b>{direction}</b>\n"
        f"当前价: {price_str}\n"
        f"杠杆: {leverage}x\n"
        f"仓位: ${amount_usd:,.0f}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"TP1: ${tp1:,.2f} (+3%) | TP2: ${tp2:,.2f} (+5%)\n"
        f"SL: ${sl:,.2f} (-2%)\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"{risk_msg}\n\n"
        f"⚠️ <b>请在交易所手动确认执行</b>\n"
        f"(自动执行需配置交易所API Key)"
    )

    await update.message.reply_text(msg, parse_mode="HTML")


async def _cmd_closeall(update, context) -> None:
    """Handle /closeall [exchange] - emergency close all positions."""
    args = context.args
    exchange_name = args[0].lower() if args else "binance"
    await update.message.reply_text(
        f"🚨 <b>确认平仓所有 {exchange_name.upper()} 持仓?</b>\n\n"
        f"⚠️ 此操作不可撤销\n"
        f"请回复 'YES' 确认",
        parse_mode="HTML",
    )
    # In a full implementation, this would use ConversationHandler
    # For now, it's a manual confirmation flow


async def _cmd_daily(update, context) -> None:
    """Handle /daily - show today's trading P&L summary."""
    try:
        from src.contract.risk import get_daily_summary
        summary = get_daily_summary()
        pnl = summary["pnl_usd"]
        emoji = "📈" if pnl >= 0 else "📉"
        wr_emoji = "🟢" if summary["win_rate"] > 50 else "🔴"

        msg = (
            f"📋 <b>今日交易日报</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"{emoji} PnL: ${pnl:+,.2f}\n"
            f"📊 交易: {summary['trades']}笔\n"
            f"{wr_emoji} 胜率: {summary['win_rate']:.0f}% ({summary['wins']}胜/{summary['losses']}负)"
        )
        await update.message.reply_text(msg, parse_mode="HTML")
    except Exception as e:
        await update.message.reply_text(f"查询失败: {e}")


async def _cmd_price(update, context) -> None:
    """Handle /price <symbol> - check current price from CoinGecko."""
    args = context.args
    if not args:
        await update.message.reply_text("用法: /price BTC\n示例: /price ETH, /price SOL")
        return

    symbol = args[0].upper()
    # Map common symbols to CoinGecko IDs
    SYMBOL_MAP = {
        "BTC": "bitcoin",
        "ETH": "ethereum",
        "SOL": "solana",
        "BNB": "binancecoin",
        "XRP": "ripple",
        "ADA": "cardano",
        "DOGE": "dogecoin",
        "AVAX": "avalanche-2",
        "DOT": "polkadot",
        "MATIC": "matic-network",
        "LINK": "chainlink",
        "UNI": "uniswap",
        "ATOM": "cosmos",
        "LTC": "litecoin",
        "ARB": "arbitrum",
        "OP": "optimism",
        "APT": "aptos",
        "SUI": "sui",
        "SEI": "sei-network",
        "TIA": "celestia",
    }

    coin_id = SYMBOL_MAP.get(symbol, symbol.lower())

    try:
        import aiohttp

        url = "https://api.coingecko.com/api/v3/coins/markets"
        params = {
            "vs_currency": "usd",
            "ids": coin_id,
            "price_change_percentage": "1h,24h,7d",
        }
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as resp:
                data = await resp.json()

        if not data:
            await update.message.reply_text(f"未找到 {symbol} 的价格数据，请检查代币名称。")
            return

        coin = data[0]
        price = coin.get("current_price", 0)
        change_1h = coin.get("price_change_percentage_1h_in_currency") or 0
        change_24h = coin.get("price_change_percentage_24h") or 0
        change_7d = coin.get("price_change_percentage_7d_in_currency") or 0
        mcap = coin.get("market_cap", 0)
        vol_24h = coin.get("total_volume", 0)
        high_24h = coin.get("high_24h", 0)
        low_24h = coin.get("low_24h", 0)

        def _arrow(v: float) -> str:
            return "+" if v >= 0 else ""

        await update.message.reply_text(
            f"{coin.get('name', symbol)} ({symbol})\n\n"
            f"价格: ${price:,.2f}\n"
            f"1h:  {_arrow(change_1h)}{change_1h:.2f}%\n"
            f"24h: {_arrow(change_24h)}{change_24h:.2f}%\n"
            f"7d:  {_arrow(change_7d)}{change_7d:.2f}%\n\n"
            f"24h 最高: ${high_24h:,.2f}\n"
            f"24h 最低: ${low_24h:,.2f}\n"
            f"市值: ${mcap:,.0f}\n"
            f"24h 交易量: ${vol_24h:,.0f}",
        )
    except Exception as e:
        logger.error("price_command_failed", error=str(e))
        await update.message.reply_text(f"查询失败: {e}")


async def _cmd_fg(update, context) -> None:
    """Handle /fg - check current Fear & Greed index."""
    try:
        import aiohttp

        url = "https://api.alternative.me/fng/"
        params = {"limit": "7", "format": "json"}
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as resp:
                data = await resp.json()

        entries = data.get("data", [])
        if not entries:
            await update.message.reply_text("无法获取恐惧贪婪指数数据。")
            return

        current = entries[0]
        value = int(current.get("value", 0))
        classification = current.get("value_classification", "Unknown")
        timestamp = int(current.get("timestamp", 0))
        dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)

        # Gauge emoji
        if value < 25:
            emoji = "极度恐惧"
            bar = "[---------]"
        elif value < 40:
            emoji = "恐惧"
            bar = "[--        ]"
        elif value < 60:
            emoji = "中性"
            bar = "[-----     ]"
        elif value < 75:
            emoji = "贪婪"
            bar = "[-------   ]"
        else:
            emoji = "极度贪婪"
            bar = "[----------]"

        # 7-day trend
        trend_lines = []
        for entry in entries[:7]:
            v = int(entry.get("value", 0))
            ts = int(entry.get("timestamp", 0))
            d = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%m-%d")
            trend_lines.append(f"  {d}: {v} ({entry.get('value_classification', '')})")

        await update.message.reply_text(
            f"恐惧贪婪指数\n\n"
            f"当前: {value}/100 — {classification} ({emoji})\n"
            f"{bar}\n"
            f"更新时间: {dt.strftime('%Y-%m-%d %H:%M UTC')}\n\n"
            f"近7天趋势:\n" + "\n".join(trend_lines),
        )
    except Exception as e:
        logger.error("fg_command_failed", error=str(e))
        await update.message.reply_text(f"查询失败: {e}")


async def _cmd_funding(update, context) -> None:
    """Handle /funding - check BTC funding rate from Binance."""
    try:
        import aiohttp

        # Binance perpetual funding rate (public, no key needed)
        url = "https://fapi.binance.com/fapi/v1/fundingRate"
        symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
        lines = ["BTC/ETH/SOL 资金费率 (Binance)\n"]

        async with aiohttp.ClientSession() as session:
            for sym in symbols:
                params = {"symbol": sym, "limit": "3"}
                async with session.get(url, params=params) as resp:
                    data = await resp.json()

                if data and isinstance(data, list):
                    latest = data[-1]
                    rate = float(latest.get("fundingRate", 0))
                    ts = int(latest.get("fundingTime", 0)) / 1000
                    dt = datetime.fromtimestamp(ts, tz=timezone.utc)

                    # Annualized rate
                    annual = rate * 3 * 365 * 100

                    indicator = "+" if rate >= 0 else ""
                    lines.append(
                        f"{sym.replace('USDT', '')}:\n"
                        f"  费率: {indicator}{rate:.4%}\n"
                        f"  年化: {indicator}{annual:.1f}%\n"
                        f"  时间: {dt.strftime('%H:%M UTC')}"
                    )
                else:
                    lines.append(f"{sym.replace('USDT', '')}: 数据不可用")

        # Interpretation
        lines.append("\n解读:")
        lines.append("  费率 > 0.01% = 多头过热")
        lines.append("  费率 < -0.005% = 空头过热")
        lines.append("  费率 ~ 0% = 市场平衡")

        await update.message.reply_text("\n".join(lines))
    except Exception as e:
        logger.error("funding_command_failed", error=str(e))
        await update.message.reply_text(f"查询失败: {e}")


async def _cmd_risk(update, context) -> None:
    """Handle /risk - show risk dashboard summary."""
    try:
        # Gather multiple data points in parallel
        import aiohttp

        fg_value = None
        fg_class = ""
        btc_price = 0.0
        btc_change_24h = 0.0
        funding_rate = 0.0

        async with aiohttp.ClientSession() as session:
            # Fear & Greed
            try:
                async with session.get(
                    "https://api.alternative.me/fng/",
                    params={"limit": "1", "format": "json"},
                ) as resp:
                    data = await resp.json()
                    entries = data.get("data", [])
                    if entries:
                        fg_value = int(entries[0].get("value", 0))
                        fg_class = entries[0].get("value_classification", "")
            except Exception:
                pass

            # BTC price
            try:
                async with session.get(
                    "https://api.coingecko.com/api/v3/coins/markets",
                    params={"vs_currency": "usd", "ids": "bitcoin"},
                ) as resp:
                    data = await resp.json()
                    if data:
                        btc_price = data[0].get("current_price", 0)
                        btc_change_24h = data[0].get("price_change_percentage_24h", 0) or 0
            except Exception:
                pass

            # BTC funding rate
            try:
                async with session.get(
                    "https://fapi.binance.com/fapi/v1/fundingRate",
                    params={"symbol": "BTCUSDT", "limit": "1"},
                ) as resp:
                    data = await resp.json()
                    if data and isinstance(data, list):
                        funding_rate = float(data[0].get("fundingRate", 0))
            except Exception:
                pass

        # Risk assessment
        risk_factors = []
        overall_risk = 0

        # F&G risk
        if fg_value is not None:
            if fg_value < 20:
                risk_factors.append(f"极度恐惧 ({fg_value}) — 高风险，但可能接近底部")
                overall_risk += 3
            elif fg_value < 35:
                risk_factors.append(f"恐惧 ({fg_value}) — 市场谨慎")
                overall_risk += 2
            elif fg_value > 80:
                risk_factors.append(f"极度贪婪 ({fg_value}) — 高风险，泡沫信号")
                overall_risk += 3
            elif fg_value > 65:
                risk_factors.append(f"贪婪 ({fg_value}) — 注意风控")
                overall_risk += 1
            else:
                risk_factors.append(f"中性 ({fg_value}) — 市场平衡")

        # Funding risk
        if abs(funding_rate) > 0.001:
            risk_factors.append(f"资金费率异常 ({funding_rate:.4%}) — 杠杆过高")
            overall_risk += 2
        elif abs(funding_rate) > 0.0005:
            risk_factors.append(f"资金费率偏高 ({funding_rate:.4%})")
            overall_risk += 1
        else:
            risk_factors.append(f"资金费率正常 ({funding_rate:.4%})")

        # Volatility risk
        if abs(btc_change_24h) > 10:
            risk_factors.append(f"BTC 24h 波动剧烈 ({btc_change_24h:+.1f}%)")
            overall_risk += 2
        elif abs(btc_change_24h) > 5:
            risk_factors.append(f"BTC 24h 波动较大 ({btc_change_24h:+.1f}%)")
            overall_risk += 1
        else:
            risk_factors.append(f"BTC 24h 波动正常 ({btc_change_24h:+.1f}%)")

        # Risk level
        if overall_risk >= 5:
            level = "HIGH"
            level_zh = "高风险"
        elif overall_risk >= 3:
            level = "MEDIUM"
            level_zh = "中等风险"
        else:
            level = "LOW"
            level_zh = "低风险"

        factors_text = "\n".join(f"  - {f}" for f in risk_factors)

        await update.message.reply_text(
            f"风险仪表盘\n\n"
            f"综合风险: {level_zh} ({level})\n"
            f"BTC: ${btc_price:,.0f} ({btc_change_24h:+.1f}%)\n"
            f"恐惧贪婪: {fg_value if fg_value is not None else 'N/A'} ({fg_class})\n"
            f"BTC 资金费率: {funding_rate:.4%}\n\n"
            f"风险因子:\n{factors_text}\n\n"
            f"使用 /snapshot 获取完整热点扫描",
        )

        # Dashboard image removed — was in deleted visuals/ module

    except Exception as e:
        logger.error("risk_command_failed", error=str(e))
        await update.message.reply_text(f"查询失败: {e}")


async def _cmd_top(update, context) -> None:
    """Handle /top - show Top 5 gainers and Top 5 losers from CoinGecko."""
    try:
        import aiohttp

        url = "https://api.coingecko.com/api/v3/coins/markets"
        params = {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": "100",
            "page": "1",
            "price_change_percentage": "24h",
        }
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as resp:
                data = await resp.json()

        if not data or not isinstance(data, list):
            await update.message.reply_text("无法获取市场数据。")
            return

        # Sort by 24h change
        valid = [c for c in data if c.get("price_change_percentage_24h") is not None]
        sorted_up = sorted(valid, key=lambda c: c["price_change_percentage_24h"], reverse=True)
        sorted_down = sorted(valid, key=lambda c: c["price_change_percentage_24h"])

        lines = ["🚀 <b>Top 5 涨幅 Gainers (24h)</b>\n"]
        for i, c in enumerate(sorted_up[:5], 1):
            sym = c.get("symbol", "?").upper()
            chg = c["price_change_percentage_24h"]
            price = c.get("current_price", 0)
            lines.append(f"  {i}. <b>{sym}</b>  ${price:,.4f}  📈 +{chg:.2f}%")

        lines.append(f"\n💥 <b>Top 5 跌幅 Losers (24h)</b>\n")
        for i, c in enumerate(sorted_down[:5], 1):
            sym = c.get("symbol", "?").upper()
            chg = c["price_change_percentage_24h"]
            price = c.get("current_price", 0)
            lines.append(f"  {i}. <b>{sym}</b>  ${price:,.4f}  📉 {chg:.2f}%")

        await update.message.reply_text("\n".join(lines), parse_mode="HTML")
    except Exception as e:
        logger.error("top_command_failed", error=str(e))
        await update.message.reply_text(f"查询失败: {e}")


async def _cmd_gas(update, context) -> None:
    """Handle /gas - show ETH gas prices from Etherscan."""
    try:
        import aiohttp

        api_key = os.environ.get("ETHERSCAN_API_KEY", "")
        url = "https://api.etherscan.io/v2/api"
        params = {
            "chainid": "1",
            "module": "gastracker",
            "action": "gasoracle",
            "apikey": api_key,
        }
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as resp:
                data = await resp.json()

        result = data.get("result", {})
        if not isinstance(result, dict):
            await update.message.reply_text("无法获取 Gas 数据。")
            return

        safe = result.get("SafeGasPrice", "?")
        propose = result.get("ProposeGasPrice", "?")
        fast = result.get("FastGasPrice", "?")
        base_fee = result.get("suggestBaseFee", "?")

        await update.message.reply_text(
            f"⛽ <b>ETH Gas Price</b>\n\n"
            f"🟢 Safe:      {safe} Gwei\n"
            f"🟡 Standard:  {propose} Gwei\n"
            f"🔴 Fast:      {fast} Gwei\n\n"
            f"Base Fee: {base_fee} Gwei",
            parse_mode="HTML",
        )
    except Exception as e:
        logger.error("gas_command_failed", error=str(e))
        await update.message.reply_text(f"查询失败: {e}")


async def _cmd_watchlist(update, context) -> None:
    """Handle /watchlist - display user's watchlist."""
    chat_id = update.effective_chat.id
    symbols = _watchlists.get(chat_id, [])

    if not symbols:
        await update.message.reply_text(
            "⭐ <b>关注列表为空</b>\n\n"
            "使用 /price BTC 查询后点击 ⭐加入关注 按钮添加。\n"
            "或直接编辑: 在此消息后回复代币符号（如 BTC ETH SOL）以批量添加。",
            parse_mode="HTML",
        )
        return

    try:
        import aiohttp

        SYMBOL_MAP = {
            "BTC": "bitcoin", "ETH": "ethereum", "SOL": "solana",
            "BNB": "binancecoin", "XRP": "ripple", "ADA": "cardano",
            "DOGE": "dogecoin", "AVAX": "avalanche-2", "DOT": "polkadot",
            "LINK": "chainlink", "UNI": "uniswap", "ARB": "arbitrum",
            "OP": "optimism", "APT": "aptos", "SUI": "sui",
        }
        ids = ",".join(SYMBOL_MAP.get(s, s.lower()) for s in symbols)
        url = "https://api.coingecko.com/api/v3/coins/markets"
        params = {
            "vs_currency": "usd",
            "ids": ids,
            "price_change_percentage": "24h",
        }
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as resp:
                data = await resp.json()

        if not data or not isinstance(data, list):
            # Fallback: just list symbols
            sym_text = ", ".join(symbols)
            await update.message.reply_text(f"⭐ <b>关注列表:</b> {sym_text}", parse_mode="HTML")
            return

        lines = ["⭐ <b>关注列表 Watchlist</b>\n"]
        for c in data:
            sym = c.get("symbol", "?").upper()
            price = c.get("current_price", 0)
            chg = c.get("price_change_percentage_24h") or 0
            arrow = "📈" if chg >= 0 else "📉"
            prefix = "+" if chg >= 0 else ""
            lines.append(f"  {arrow} <b>{sym}</b>  ${price:,.2f}  {prefix}{chg:.2f}%")

        await update.message.reply_text("\n".join(lines), parse_mode="HTML")
    except Exception as e:
        logger.error("watchlist_command_failed", error=str(e))
        sym_text = ", ".join(symbols)
        await update.message.reply_text(f"⭐ 关注列表: {sym_text}\n\n(价格查询失败: {e})")


async def _cmd_whale(update, context) -> None:
    """Handle /whale - show recent large transfers."""
    try:
        import aiohttp

        # Use whale-alert public API (free tier, limited)
        api_key = os.environ.get("WHALE_ALERT_API_KEY", "")
        now = int(datetime.now(timezone.utc).timestamp())
        start = now - 3600  # last 1 hour

        url = "https://api.whale-alert.io/v1/transactions"
        params = {
            "api_key": api_key,
            "min_value": "1000000",
            "start": str(start),
            "limit": "10",
        }

        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as resp:
                if resp.status != 200:
                    await update.message.reply_text(
                        "🐋 <b>鲸鱼动态 Whale Alert</b>\n\n"
                        "⚠️ API 不可用或未配置 WHALE_ALERT_API_KEY。\n"
                        "请设置环境变量后重试。",
                        parse_mode="HTML",
                    )
                    return
                data = await resp.json()

        txs = data.get("transactions", [])
        if not txs:
            await update.message.reply_text(
                "🐋 <b>鲸鱼动态</b>\n\n过去1小时无大额转账 (>$1M)。",
                parse_mode="HTML",
            )
            return

        lines = ["🐋 <b>鲸鱼动态 Whale Alert (1h)</b>\n"]
        for tx in txs[:8]:
            symbol = tx.get("symbol", "?").upper()
            amount = tx.get("amount", 0)
            usd = tx.get("amount_usd", 0)
            from_label = tx.get("from", {}).get("owner", "unknown")
            to_label = tx.get("to", {}).get("owner", "unknown")

            if usd >= 1_000_000_000:
                usd_str = f"${usd / 1_000_000_000:.2f}B"
            elif usd >= 1_000_000:
                usd_str = f"${usd / 1_000_000:.1f}M"
            else:
                usd_str = f"${usd:,.0f}"

            lines.append(
                f"  🐋 <b>{amount:,.0f} #{symbol}</b> ({usd_str})\n"
                f"     {from_label} → {to_label}"
            )

        await update.message.reply_text("\n\n".join(lines), parse_mode="HTML")
    except Exception as e:
        logger.error("whale_command_failed", error=str(e))
        await update.message.reply_text(f"查询失败: {e}")


async def _cmd_calendar(update, context) -> None:
    """Handle /calendar - show upcoming major crypto events this week."""
    try:
        import aiohttp

        # CoinGecko events are no longer free; we use CoinMarketCal as fallback
        # or show a curated static + dynamic approach
        api_key = os.environ.get("COINMARKETCAL_API_KEY", "")

        today = datetime.now(timezone.utc)
        end = today + timedelta(days=7)
        date_from = today.strftime("%Y-%m-%d")
        date_to = end.strftime("%Y-%m-%d")

        events_found = False
        lines = [
            f"📅 <b>本周加密日历 {date_from} ~ {date_to}</b>\n"
        ]

        if api_key:
            url = "https://developers.coinmarketcal.com/v1/events"
            headers = {"x-api-key": api_key, "Accept": "application/json"}
            params = {
                "dateRangeStart": date_from,
                "dateRangeEnd": date_to,
                "sortBy": "hot_events",
                "page": "1",
                "max": "10",
            }
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, headers=headers, params=params) as resp:
                        data = await resp.json()
                body = data.get("body", [])
                for ev in body[:10]:
                    title = ev.get("title", {}).get("en", "")
                    coins = ", ".join(c.get("symbol", "") for c in ev.get("coins", [])[:3])
                    date_event = ev.get("date_event", "")[:10]
                    lines.append(f"  📌 <b>{date_event}</b> — {title}")
                    if coins:
                        lines.append(f"     Coins: {coins}")
                events_found = len(body) > 0
            except Exception:
                pass

        if not events_found:
            # Fallback: show well-known recurring events
            lines.append("  📌 Token unlocks — check tokenterminal.com")
            lines.append("  📌 FOMC / macro events — check forexfactory.com")
            lines.append("  📌 ETH upgrade milestones — check ethereum.org")
            lines.append("")
            lines.append(
                "<i>Set COINMARKETCAL_API_KEY for live event data.</i>"
            )

        await update.message.reply_text("\n".join(lines), parse_mode="HTML")
    except Exception as e:
        logger.error("calendar_command_failed", error=str(e))
        await update.message.reply_text(f"查询失败: {e}")


async def _cmd_mute(update, context) -> None:
    """Handle /mute <duration> - mute notifications."""
    global _mute_until
    args = context.args
    if not args:
        await update.message.reply_text("用法: /mute 4h\n支持格式: 30m, 1h, 4h, 12h, 1d")
        return

    duration_str = args[0].lower()
    match = re.match(r"^(\d+)(m|h|d)$", duration_str)
    if not match:
        await update.message.reply_text("格式错误。支持: 30m, 1h, 4h, 12h, 1d")
        return

    amount = int(match.group(1))
    unit = match.group(2)

    if unit == "m":
        delta = timedelta(minutes=amount)
    elif unit == "h":
        delta = timedelta(hours=amount)
    else:
        delta = timedelta(days=amount)

    # Cap at 7 days
    if delta > timedelta(days=7):
        await update.message.reply_text("最多静音7天。")
        return

    _mute_until = datetime.now(timezone.utc) + delta
    await update.message.reply_text(
        f"已静音 {duration_str}。\n"
        f"恢复时间: {_mute_until.strftime('%Y-%m-%d %H:%M UTC')}\n"
        f"使用 /unmute 提前取消。\n"
        f"注意: P0 级别紧急警报不受静音影响。",
    )
    logger.info("bot_muted", duration=duration_str, until=_mute_until.isoformat())


async def _cmd_unmute(update, context) -> None:
    """Handle /unmute - cancel mute."""
    global _mute_until
    if _mute_until is None:
        await update.message.reply_text("当前没有静音。")
        return

    _mute_until = None
    await update.message.reply_text("已取消静音，通知恢复正常。")
    logger.info("bot_unmuted")


async def _cmd_help(update, context) -> None:
    """Handle /help - show all commands."""
    await update.message.reply_text(
        "<b>🎯 CryptoScope Trading Bot</b>\n\n"
        "<b>📊 行情</b>\n"
        "/price BTC — 查价格\n"
        "/top — 涨跌幅排行\n"
        "/fg — 恐惧贪婪指数\n"
        "/funding — 资金费率\n"
        "/gas — ETH Gas\n\n"
        "<b>🔥 Meme + 聪明钱</b>\n"
        "/scan — 扫描聪明钱动态\n"
        "/analyze <地址> — 深度分析代币\n"
        "/check <地址> — 安全检测(GoPlus+RugCheck)\n"
        "/profile <钱包> — 钱包画像(持仓+PnL)\n"
        "/smartmoney — 聪明钱追踪状态\n"
        "/watch <地址> <标签> — 添加追踪钱包\n\n"
        "<b>📈 交易信号</b>\n"
        "/signals — 当前交易信号\n"
        "/positions — 模拟持仓和P&amp;L\n"
        "/scorecard — 信号准确率记分卡\n\n"
        "<b>💹 合约交易</b>\n"
        "/trade BTC LONG 5x $500 — 下单(需确认)\n"
        "/balance — 账户余额\n"
        "/pos — 当前持仓\n"
        "/daily — 今日PnL日报\n"
        "/closeall — 紧急全平\n\n"
        "<b>🔍 监控</b>\n"
        "/risk — 风险仪表盘\n"
        "/whale — 大额转账\n"
        "/calendar — 本周事件\n"
        "/watchlist — 价格关注列表\n\n"
        "<b>⚙️ 设置</b>\n"
        "/mute 4h — 静音\n"
        "/unmute — 取消静音\n\n"
        "自动推送: 聪明钱聚集 · Token解锁做空 · 费率极端 · 黑天鹅预警",
        parse_mode="HTML",
    )


async def _cmd_start(update, context) -> None:
    """Handle /start - welcome message."""
    await update.message.reply_text(
        "CryptoScope Bot 已启动。\n\n"
        "输入 /help 查看所有可用命令。",
    )


def create_bot_application():
    """Create and configure the Telegram bot application.

    Returns the Application instance (not started).
    Call `application.initialize()` then `application.start()` + `application.updater.start_polling()`
    to run the bot.
    """
    from telegram.ext import ApplicationBuilder, CommandHandler

    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
    if not bot_token:
        logger.warning("telegram_bot_token_not_set")
        return None

    application = (
        ApplicationBuilder()
        .token(bot_token)
        .build()
    )

    # Register command handlers
    application.add_handler(CommandHandler("start", _cmd_start))
    application.add_handler(CommandHandler("help", _cmd_help))
    # Trading commands
    application.add_handler(CommandHandler("scan", _cmd_scan))
    application.add_handler(CommandHandler("analyze", _cmd_analyze))
    application.add_handler(CommandHandler("signals", _cmd_signals))
    application.add_handler(CommandHandler("positions", _cmd_positions))
    application.add_handler(CommandHandler("scorecard", _cmd_scorecard))
    application.add_handler(CommandHandler("watch", _cmd_watch))
    application.add_handler(CommandHandler("smartmoney", _cmd_smartmoney))
    application.add_handler(CommandHandler("check", _cmd_check))
    application.add_handler(CommandHandler("profile", _cmd_profile))
    # Contract trading commands
    application.add_handler(CommandHandler("balance", _cmd_balance))
    application.add_handler(CommandHandler("pos", _cmd_pos))
    application.add_handler(CommandHandler("trade", _cmd_trade))
    application.add_handler(CommandHandler("closeall", _cmd_closeall))
    application.add_handler(CommandHandler("daily", _cmd_daily))
    # Market data commands
    application.add_handler(CommandHandler("price", _cmd_price))
    application.add_handler(CommandHandler("fg", _cmd_fg))
    application.add_handler(CommandHandler("funding", _cmd_funding))
    application.add_handler(CommandHandler("risk", _cmd_risk))
    application.add_handler(CommandHandler("top", _cmd_top))
    application.add_handler(CommandHandler("gas", _cmd_gas))
    application.add_handler(CommandHandler("watchlist", _cmd_watchlist))
    application.add_handler(CommandHandler("whale", _cmd_whale))
    application.add_handler(CommandHandler("calendar", _cmd_calendar))
    # Settings
    application.add_handler(CommandHandler("mute", _cmd_mute))
    application.add_handler(CommandHandler("unmute", _cmd_unmute))

    logger.info("telegram_bot_configured", commands=21)

    return application


async def start_bot_polling() -> None:
    """Start the bot polling in the background (non-blocking).

    This initializes the application, starts it, and begins polling.
    Designed to be called from an already-running asyncio event loop.
    """
    application = create_bot_application()
    if application is None:
        return

    await application.initialize()
    await application.start()
    await application.updater.start_polling(drop_pending_updates=True)
    logger.info("telegram_bot_polling_started")


async def stop_bot_polling(application) -> None:
    """Gracefully stop polling and release cached exchange HTTP sessions."""
    try:
        if application and application.updater:
            await application.updater.stop()
            await application.stop()
            await application.shutdown()
            logger.info("telegram_bot_stopped")
    finally:
        from src.contract.exchange import close_exchange_clients

        close_exchange_clients()


if __name__ == "__main__":
    async def _main():
        app = create_bot_application()
        if app is None:
            print("Set TELEGRAM_BOT_TOKEN environment variable.")
            return
        await app.initialize()
        await app.start()
        await app.updater.start_polling(drop_pending_updates=True)
        print("Bot is running. Press Ctrl+C to stop.")
        try:
            while True:
                await asyncio.sleep(3600)
        except KeyboardInterrupt:
            pass
        finally:
            await stop_bot_polling(app)

    asyncio.run(_main())
