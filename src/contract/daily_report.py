"""Daily PnL Report — auto-sends at end of day.

Combines:
- Paper trading P&L
- Signal scorecard accuracy
- Smart money activity summary
- Open positions status
"""

from __future__ import annotations

from datetime import datetime, timezone

import structlog

logger = structlog.get_logger()


def generate_daily_report() -> str:
    """Generate comprehensive daily report as Telegram HTML."""
    now = datetime.now(timezone.utc)
    lines = [
        f"📋 <b>每日交易日报</b>",
        f"📅 {now.strftime('%Y-%m-%d')}",
        f"━━━━━━━━━━━━━━━━━━━━",
        "",
    ]

    # 1. Paper trading P&L
    try:
        from src.trading.paper_trader import get_performance_summary
        perf = get_performance_summary()
        pnl = perf["total_pnl_sol"]
        pnl_emoji = "📈" if pnl >= 0 else "📉"
        wr = perf["win_rate"]
        wr_emoji = "🟢" if wr > 50 else "🔴" if wr < 40 else "🟡"

        lines.append(f"<b>模拟交易</b>")
        lines.append(f"  {pnl_emoji} PnL: {pnl:+.4f} SOL ({perf['total_pnl_pct']:+.1f}%)")
        lines.append(f"  💰 余额: {perf['balance_sol']:.4f} SOL")
        lines.append(f"  {wr_emoji} 胜率: {wr:.0f}% ({perf['winners']}W/{perf['losers']}L)")
        lines.append(f"  📊 持仓中: {perf['open_positions']}个")
        lines.append("")
    except Exception as e:
        lines.append(f"模拟交易: 数据不可用 ({e})")
        lines.append("")

    # 2. Signal scorecard
    try:
        from src.trading.signal_scorecard import get_scorecard_summary
        summary = get_scorecard_summary()
        if summary:
            lines.append(f"<b>信号准确率</b>")
            for sig_type, data in summary.items():
                type_labels = {
                    "token_unlock": "解锁做空",
                    "funding_reversion": "费率回归",
                    "boost_detection": "Boost",
                    "smart_money_cluster": "聪明钱",
                }
                label = type_labels.get(sig_type, sig_type)
                completed = data["completed"]
                if completed > 0:
                    lines.append(f"  {label}: 4h胜率{data['win_rate_4h']} · 均PnL{data['avg_pnl_4h']}")
            lines.append("")
    except Exception:
        pass

    # 3. Risk check
    try:
        from src.contract.risk import get_daily_summary
        daily = get_daily_summary()
        if daily["trades"] > 0:
            lines.append(f"<b>合约交易</b>")
            lines.append(f"  PnL: ${daily['pnl_usd']:+,.2f}")
            lines.append(f"  交易: {daily['trades']}笔 · 胜率: {daily['win_rate']:.0f}%")
            lines.append("")
    except Exception:
        pass

    # 4. Market snapshot
    try:
        import json
        import urllib.request
        url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum,solana&vs_currencies=usd&include_24hr_change=true"
        req = urllib.request.Request(url, headers={"User-Agent": "CryptoScope/1.0"})
        with urllib.request.urlopen(req, timeout=8) as resp:
            data = json.loads(resp.read().decode())

        btc = data.get("bitcoin", {})
        eth = data.get("ethereum", {})
        sol = data.get("solana", {})
        lines.append(f"<b>市场</b>")
        lines.append(f"  BTC ${btc.get('usd',0):,.0f} ({btc.get('usd_24h_change',0):+.1f}%)")
        lines.append(f"  ETH ${eth.get('usd',0):,.0f} ({eth.get('usd_24h_change',0):+.1f}%)")
        lines.append(f"  SOL ${sol.get('usd',0):,.0f} ({sol.get('usd_24h_change',0):+.1f}%)")
    except Exception:
        pass

    lines.append("")
    lines.append("━━━━━━━━━━━━━━━━━━━━")
    lines.append(f"⏰ {now.strftime('%H:%M UTC')}")

    return "\n".join(lines)


async def send_daily_report() -> bool:
    """Generate and send daily report to Telegram."""
    report = generate_daily_report()
    try:
        from src.distribution.telegram_sender import send_digest
        return await send_digest(report)
    except Exception as e:
        logger.error("daily_report_send_failed", error=str(e))
        return False
