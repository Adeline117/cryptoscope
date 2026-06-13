"""Telegram HTML message templates for CryptoScope push notifications.

All templates output Telegram-compatible HTML (parse_mode="HTML").
"""

from __future__ import annotations

from datetime import datetime
from html import escape as _html_escape


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


def _esc(text: str | None) -> str:
    """HTML-escape user-supplied text for Telegram."""
    if text is None:
        return ""
    return _html_escape(str(text))


def _format_usd(value: float | int | None) -> str:
    """Format a USD value with human-readable suffixes.

    Examples: $65,432,100 -> $65.43M, $1,200,000,000 -> $1.20B
    """
    if value is None:
        return "N/A"
    v = abs(value)
    sign = "-" if value < 0 else ""
    if v >= 1_000_000_000_000:
        return f"{sign}${v / 1_000_000_000_000:,.2f}T"
    if v >= 1_000_000_000:
        return f"{sign}${v / 1_000_000_000:,.2f}B"
    if v >= 1_000_000:
        return f"{sign}${v / 1_000_000:,.2f}M"
    if v >= 1_000:
        return f"{sign}${v / 1_000:,.2f}K"
    if v >= 1:
        return f"{sign}${v:,.2f}"
    # Sub-dollar (for low-cap tokens)
    return f"{sign}${v:.6f}"


def _change_indicator(change: float | None) -> str:
    """Return an arrow/emoji indicator for a percentage change value.

    Positive  -> 📈 +X.XX%
    Negative  -> 📉 X.XX%
    Zero/None -> ➖ 0.00%
    """
    if change is None:
        return "➖ N/A"
    prefix = "+" if change > 0 else ""
    if change > 0:
        emoji = "📈"
    elif change < 0:
        emoji = "📉"
    else:
        emoji = "➖"
    return f"{emoji} {prefix}{change:.2f}%"


def _severity_bar(level: int, max_level: int = 5) -> str:
    """Build a severity indicator bar using red circles.

    level=5, max_level=5  -> 🔴🔴🔴🔴🔴
    level=3, max_level=5  -> 🔴🔴🔴⚪⚪
    """
    level = max(0, min(level, max_level))
    return "🔴" * level + "⚪" * (max_level - level)


def _progress_bar(value: int, max_value: int = 100, length: int = 10) -> str:
    """Build a text-based progress bar.

    value=73, max_value=100 -> [███████▒▒▒]
    """
    filled = round(value / max_value * length)
    filled = max(0, min(filled, length))
    return "[" + "█" * filled + "▒" * (length - filled) + "]"


# ---------------------------------------------------------------------------
# Template: Critical Alert
# ---------------------------------------------------------------------------


def format_critical_alert(
    alert_type: str,
    title: str,
    details: str,
    affected_assets: list[str],
    price_impact: str,
    source_url: str = "",
) -> str:
    """Format a critical/P0 alert message.

    Args:
        alert_type: e.g. "DEPEG", "FLASH_CRASH", "HACK", "LIQUIDATION"
        title: Bold headline
        details: Paragraph of detail text
        affected_assets: List of affected tokens/protocols
        price_impact: Human-readable impact description
        source_url: Optional link to source
    """
    severity = _severity_bar(5, 5)
    assets_str = " ".join(f"#{_esc(a)}" for a in affected_assets[:8])
    source_line = f'\n🔗 <a href="{_esc(source_url)}">Source</a>' if source_url else ""

    return (
        f"{severity}\n"
        f"🚨 <b>CRITICAL — {_esc(alert_type)}</b>\n\n"
        f"<b>{_esc(title)}</b>\n\n"
        f"{_esc(details)}\n\n"
        f"💥 <b>Affected:</b> {assets_str}\n"
        f"📉 <b>Price Impact:</b> {_esc(price_impact)}\n"
        f"{source_line}\n\n"
        f"⏰ {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}"
    )


# ---------------------------------------------------------------------------
# Template: Highlight Push (热点速报)
# ---------------------------------------------------------------------------


def format_highlight_push(
    items: list[dict],
    fear_greed: int | None = None,
    btc_price: float | None = None,
    btc_change_24h: float | None = None,
    scan_count: int = 0,
    anomaly_count: int = 0,
    anomaly_details: list[dict] | None = None,
    narratives: list[dict] | None = None,
    funding_rate: float | None = None,
) -> str:
    """Format an intelligence-grade highlight push — thesis-driven, not headline-driven.

    Only shows items that matter. Includes anomalies and narrative context.
    If nothing is genuinely important, says so explicitly.

    Args:
        items: List of dicts with keys: title, impact, impact_probability,
               impact_magnitude, impact_timeframe, score, summary, affected_assets
        fear_greed: Current F&G index (0-100)
        btc_price: Current BTC price in USD
        btc_change_24h: BTC 24h change %
        scan_count: Number of items scanned
        anomaly_count: Number of anomalies detected
        anomaly_details: List of {type, description, severity} dicts
        narratives: List of {name, strength, trend} dicts
        funding_rate: Current BTC funding rate
    """
    lines: list[str] = []

    # Market snapshot — one line
    btc_str = _format_usd(btc_price) if btc_price else "?"
    btc_chg = f"({btc_change_24h:+.1f}%)" if btc_change_24h is not None else ""
    fg_emoji = ""
    if fear_greed is not None:
        if fear_greed < 25:
            fg_emoji = "😱"
        elif fear_greed < 40:
            fg_emoji = "😰"
        elif fear_greed < 60:
            fg_emoji = "😐"
        elif fear_greed < 75:
            fg_emoji = "😃"
        else:
            fg_emoji = "🤑"
    fg_str = f"{fg_emoji}F&amp;G {fear_greed}" if fear_greed is not None else ""
    fr_str = f"费率 {funding_rate:+.4%}" if funding_rate is not None else ""

    market_parts = [f"BTC {btc_str} {btc_chg}", fg_str, fr_str]
    market_line = " · ".join(filter(None, market_parts))

    lines.append(f"⚡ <b>实时情报</b> · {datetime.utcnow().strftime('%H:%M UTC')}")
    lines.append(market_line)

    # Filter to only genuinely important items (score > 70 with real impact)
    important = [i for i in items if i.get("score", 0) >= 70]

    if not important:
        lines.append("")
        lines.append(f"📡 扫描 {scan_count} 条 · 无需关注事项")
        if anomaly_count:
            lines.append(f"⚠️ {anomaly_count} 异常信号 (详见下方)")
    else:
        lines.append("")
        # Show only top 1-2 items with full context
        for i, item in enumerate(important[:2]):
            if i > 0:
                lines.append("")

            title = _esc(item.get("title", ""))
            impact = item.get("impact", "neutral").lower()
            prob = item.get("impact_probability")
            magnitude = item.get("impact_magnitude", "")
            timeframe = item.get("impact_timeframe", "")
            affected = item.get("affected_assets", [])
            summary = item.get("summary", "")
            score = item.get("score", 0)

            # Direction with impact details
            if impact == "bullish":
                dir_emoji = "📈"
                dir_label = "看涨"
            elif impact == "bearish":
                dir_emoji = "📉"
                dir_label = "看跌"
            else:
                dir_emoji = "➡️"
                dir_label = "中性"

            prefix = "🔴" if score >= 85 else "🟡" if score >= 70 else "🔵"
            lines.append(f"{prefix} <b>{title}</b>")

            # Impact assessment — the "so what"
            impact_parts = [f"{dir_emoji} {dir_label}"]
            if affected:
                impact_parts.append("/".join(affected[:3]))
            if prob is not None:
                impact_parts.append(f"概率 {prob:.0%}")
            if magnitude:
                mag_zh = {"high": "影响大", "medium": "影响中", "low": "影响小"}.get(magnitude, magnitude)
                impact_parts.append(mag_zh)
            if timeframe:
                impact_parts.append(timeframe)
            lines.append(f"  {' · '.join(impact_parts)}")

            # Summary — the "why it matters"
            if summary:
                lines.append(f"  {_esc(summary[:150])}")

    # Anomaly signals — surface them properly
    if anomaly_details:
        lines.append("")
        lines.append("━━ <b>异常信号</b> ━━━━━━━━━━━")
        for anom in (anomaly_details or [])[:3]:
            sev = anom.get("severity", "")
            sev_emoji = "🔴" if sev == "critical" else "🟡" if sev == "high" else "⚪"
            desc = _esc(anom.get("description", "")[:80])
            lines.append(f"  {sev_emoji} {desc}")

    # Narrative tracker — what themes are hot
    if narratives:
        lines.append("")
        lines.append("━━ <b>叙事追踪</b> ━━━━━━━━━━━")
        for narr in (narratives or [])[:3]:
            name = _esc(narr.get("name", ""))
            strength = narr.get("strength", 0)
            trend = narr.get("trend", "")
            trend_emoji = "↑" if trend == "rising" else "↓" if trend == "falling" else "→"
            bar = "█" * min(strength, 10) + "▒" * max(0, 10 - strength)
            lines.append(f"  {trend_emoji} {name}: [{bar}] {strength}/10")

    # Footer
    lines.append("")
    remaining = len(items) - len(important[:2])
    if remaining > 0:
        lines.append(f"📊 扫描 {scan_count} 条 · 其余 {remaining} 条已归档")
    else:
        lines.append(f"📊 扫描 {scan_count} 条")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Template: Daily Digest (日报)
# ---------------------------------------------------------------------------


def format_daily_digest(
    date_str: str,
    market_summary: dict,
    top_stories: list[dict],
    sector_performance: list[dict],
    whale_moves: list[dict],
    upcoming_events: list[dict],
    regime: dict | None = None,
    thesis: str = "",
    positioning_bias: str = "",
    narratives: list[dict] | None = None,
) -> str:
    """Format a daily intelligence brief — thesis-driven, not data-driven.

    Args:
        date_str: e.g. "2026-03-19"
        market_summary: {btc_price, btc_change, eth_price, eth_change,
                         fear_greed, funding_rate, total_mcap}
        top_stories: [{title, summary, impact, impact_probability,
                       impact_magnitude, affected_assets}] up to 3
        sector_performance: [{name, change}]
        whale_moves: [{amount, token, direction, wallet_label, signal}]
        upcoming_events: [{date, title, trade_setup, scenarios}]
        regime: {state, confidence, duration_days, transition_prob} from regime_detector
        thesis: TL;DR market thesis (1-3 sentences)
        positioning_bias: e.g. "谨慎做多 BTC $86K 以上"
        narratives: [{name, strength, trend, note}]
    """
    lines: list[str] = []

    # === Header with regime ===
    regime_str = ""
    if regime:
        state_zh = {"bull": "牛市", "bear": "熊市", "sideways": "横盘", "high_volatility": "高波动"}.get(
            regime.get("state", ""), regime.get("state", "")
        )
        conf = regime.get("confidence", 0)
        days = regime.get("duration_days", 0)
        regime_str = f" · {state_zh}(第{days}天 · {conf:.0%})"

    lines.append(f"📋 <b>CryptoScope 日报 — {_esc(date_str)}</b>{regime_str}")
    lines.append("━━━━━━━━━━━━━━━━━━━━")

    # === TL;DR Thesis — the MOST IMPORTANT section ===
    if thesis or positioning_bias:
        lines.append("")
        if thesis:
            lines.append(f"<b>TL;DR</b> {_esc(thesis)}")
        if positioning_bias:
            lines.append(f"<b>定位:</b> {_esc(positioning_bias)}")

    # === Market snapshot — compact ===
    btc_p = _format_usd(market_summary.get("btc_price"))
    btc_c = market_summary.get("btc_change", 0)
    eth_p = _format_usd(market_summary.get("eth_price"))
    eth_c = market_summary.get("eth_change", 0)
    fg = market_summary.get("fear_greed", 0)
    fr = market_summary.get("funding_rate", 0)
    mcap = _format_usd(market_summary.get("total_mcap"))

    lines.append("")
    lines.append(
        f"BTC {btc_p} {_change_indicator(btc_c)} · "
        f"ETH {eth_p} {_change_indicator(eth_c)}"
    )
    lines.append(f"F&amp;G {fg} · 费率 {fr:+.4%} · 市值 {mcap}")

    # === Key Signal (top 1-2 stories with full context) ===
    if top_stories:
        lines.append("")
        lines.append("━━ <b>关键信号</b> ━━━━━━━━━━━")
        for s in top_stories[:2]:
            title = _esc(s.get("title", ""))
            impact = s.get("impact", "neutral").lower()
            prob = s.get("impact_probability")
            magnitude = s.get("impact_magnitude", "")
            affected = s.get("affected_assets", [])
            summary = s.get("summary", "")

            dir_emoji = "📈" if impact == "bullish" else "📉" if impact == "bearish" else "➡️"
            dir_label = "看涨" if impact == "bullish" else "看跌" if impact == "bearish" else "中性"

            lines.append(f"\n{dir_emoji} <b>{title}</b>")
            # Impact line with probability and magnitude
            impact_parts = [dir_label]
            if affected:
                impact_parts.append("/".join(affected[:3]))
            if prob is not None:
                impact_parts.append(f"概率 {prob:.0%}")
            if magnitude:
                mag_zh = {"high": "影响大", "medium": "影响中", "low": "影响小"}.get(magnitude, magnitude)
                impact_parts.append(mag_zh)
            lines.append(f"  {' · '.join(impact_parts)}")
            if summary:
                lines.append(f"  {_esc(summary[:200])}")

    # === Narrative Tracker (with trend vs yesterday) ===
    if narratives:
        lines.append("")
        lines.append("━━ <b>叙事追踪</b> ━━━━━━━━━━━")
        for n in (narratives or [])[:4]:
            name = _esc(n.get("name", ""))
            strength = n.get("strength", 0)
            trend = n.get("trend", "stable")
            note = n.get("note", "")
            trend_map = {"rising": "↑强化", "falling": "↓减弱", "peak": "→见顶", "stable": "→稳定"}
            trend_label = trend_map.get(trend, trend)
            bar = "█" * min(strength, 10) + "▒" * max(0, 10 - strength)
            lines.append(f"  [{bar}] {strength}/10 {name} {trend_label}")
            if note:
                lines.append(f"    → {_esc(note[:80])}")

    # === Whale Flow Analysis (contextualized) ===
    if whale_moves:
        lines.append("")
        lines.append("━━ <b>鲸鱼流向</b> ━━━━━━━━━━━")
        for w in whale_moves[:4]:
            amt = w.get("amount", 0)
            token = _esc(w.get("token", ""))
            direction = _esc(w.get("direction", ""))
            label = w.get("wallet_label", "")
            signal = w.get("signal", "")

            label_str = f" ({_esc(label)})" if label else ""
            signal_str = ""
            if signal:
                sig_emoji = "📈" if "bullish" in signal.lower() else "📉" if "bearish" in signal.lower() else "➡️"
                signal_str = f"\n    {sig_emoji} {_esc(signal)}"

            lines.append(f"  🐋 {amt:,.0f} #{token} — {direction}{label_str}{signal_str}")

    # === Sector Performance — compact ===
    if sector_performance:
        lines.append("")
        sector_parts = []
        for s in sector_performance[:6]:
            name = _esc(s.get("name", ""))
            chg = s.get("change", 0)
            arrow = "🟢" if chg >= 0 else "🔴"
            sector_parts.append(f"{arrow}{name} {chg:+.1f}%")
        lines.append("板块: " + " · ".join(sector_parts))

    # === Events + Trade Setups ===
    if upcoming_events:
        lines.append("")
        lines.append("━━ <b>事件 + 策略</b> ━━━━━━━━━")
        for e in upcoming_events[:3]:
            edate = _esc(e.get("date", ""))
            etitle = _esc(e.get("title", ""))
            setup = e.get("trade_setup", "")
            scenarios = e.get("scenarios", [])

            lines.append(f"  📅 {edate}  <b>{etitle}</b>")
            if setup:
                lines.append(f"    策略: {_esc(setup[:120])}")
            for sc in scenarios[:2]:
                lines.append(f"    · {_esc(sc[:80])}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Template: Whale Alert (鲸鱼转账)
# ---------------------------------------------------------------------------


def format_whale_alert(
    amount: float | int,
    token: str,
    usd_value: float | int,
    from_label: str,
    to_label: str,
    tx_hash: str = "",
    chain: str = "Ethereum",
) -> str:
    """Format a whale transfer alert in Whale Alert style.

    Output example:
    🐋 1,000 #BTC ($65.00M)
    Binance -> unknown wallet
    """
    usd_str = _format_usd(usd_value)
    token_esc = _esc(token.upper())
    from_esc = _esc(from_label)
    to_esc = _esc(to_label)
    chain_esc = _esc(chain)

    tx_line = ""
    if tx_hash:
        short_hash = tx_hash[:10] + "..." + tx_hash[-6:] if len(tx_hash) > 16 else tx_hash
        if chain.lower() == "ethereum":
            tx_line = f'\n🔗 <a href="https://etherscan.io/tx/{_esc(tx_hash)}">Tx: {_esc(short_hash)}</a>'
        elif chain.lower() == "bitcoin":
            tx_line = f'\n🔗 <a href="https://mempool.space/tx/{_esc(tx_hash)}">Tx: {_esc(short_hash)}</a>'
        elif chain.lower() == "solana":
            tx_line = f'\n🔗 <a href="https://solscan.io/tx/{_esc(tx_hash)}">Tx: {_esc(short_hash)}</a>'
        else:
            tx_line = f"\n🔗 Tx: {_esc(short_hash)}"

    return (
        f"🐋 <b>{amount:,.0f} #{token_esc}</b> ({usd_str})\n"
        f"{from_esc} → {to_esc}\n"
        f"⛓ {chain_esc}{tx_line}\n\n"
        f"⏰ {datetime.utcnow().strftime('%H:%M UTC')}"
    )


# ---------------------------------------------------------------------------
# Black Swan Playbooks (黑天鹅应急清单)
# ---------------------------------------------------------------------------

BLACK_SWAN_PLAYBOOKS: dict[str, str] = {
    "stablecoin_depeg": (
        "🚨 <b>稳定币脱锚应急清单</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "<b>立即执行:</b>\n"
        "1. 检查持仓中所有涉及该稳定币的头寸\n"
        "2. 从 DEX 流动性池中撤出含该稳定币的 LP\n"
        "3. 将该稳定币兑换为 USDC/USDT (选流动性最深的路径)\n"
        "4. 取消所有以该稳定币计价的挂单\n"
        "5. 检查借贷协议中以该稳定币为抵押品的仓位，补充抵押或还贷\n"
        "6. 评估跨链桥上锁定的该稳定币风险\n\n"
        "<b>持续监控:</b>\n"
        "- Curve 3pool / 相关池子的比例偏离度\n"
        "- 发行方储备证明和审计更新\n"
        "- 链上大额赎回和铸造数据\n"
        "- 相关协议治理论坛的紧急提案\n\n"
        "<b>恢复信号:</b>\n"
        "- 价格回到 $0.995 以上并稳定 4 小时\n"
        "- 发行方发布官方储备证明\n"
        "- Curve 池比例恢复到 ±5% 以内\n"
        "- 主流借贷协议恢复正常清算参数"
    ),

    "exchange_reserve_collapse": (
        "🚨 <b>交易所储备异常应急清单</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "<b>立即执行:</b>\n"
        "1. 将该交易所所有资产提现至自托管钱包\n"
        "2. 优先提现 BTC、ETH 等主流资产 (链上确认快)\n"
        "3. 取消所有该交易所的未成交订单\n"
        "4. 关闭所有杠杆/合约仓位\n"
        "5. 检查是否有资产锁定在理财/质押产品中\n"
        "6. 记录所有账户余额截图 (留存证据)\n\n"
        "<b>持续监控:</b>\n"
        "- 交易所冷/热钱包链上余额变化\n"
        "- 提现延迟和用户反馈 (Twitter/Reddit)\n"
        "- 交易所平台币价格和交易量\n"
        "- 监管机构声明\n\n"
        "<b>恢复信号:</b>\n"
        "- 交易所发布经第三方验证的储备证明\n"
        "- 提现恢复正常速度 (< 1小时)\n"
        "- 平台币价格企稳并回升\n"
        "- 独立审计机构确认偿付能力"
    ),

    "market_panic": (
        "🚨 <b>市场恐慌应急清单</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "<b>立即执行:</b>\n"
        "1. 检查所有杠杆仓位的清算价格，必要时追加保证金\n"
        "2. 关闭高杠杆 (>5x) 仓位，保留低杠杆核心持仓\n"
        "3. 设置关键支撑位止损单 (链下 CEX)\n"
        "4. 确认稳定币持仓充足，准备抄底资金\n"
        "5. 暂停所有 DCA 自动买入计划\n"
        "6. 评估 DeFi 借贷健康度，提高抵押率至 200%+\n\n"
        "<b>持续监控:</b>\n"
        "- BTC 关键支撑位和成交量\n"
        "- 恐惧贪婪指数 (极度恐惧 <15 可能是底部)\n"
        "- 资金费率 (极负值 = 过度做空)\n"
        "- 稳定币市值流出/流入\n"
        "- 鲸鱼链上动态 (大额买入 = 潜在底部)\n\n"
        "<b>恢复信号:</b>\n"
        "- 恐惧贪婪指数从极度恐惧回升至 25+\n"
        "- 资金费率从极负值回归中性\n"
        "- 日线级别放量收阳\n"
        "- 机构买入信号 (ETF 净流入转正)"
    ),

    "liquidity_crisis": (
        "🚨 <b>流动性危机应急清单</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "<b>立即执行:</b>\n"
        "1. 从小型 DEX 和低 TVL 池中撤出流动性\n"
        "2. 将资产集中到主流协议 (Aave, Uniswap, Curve)\n"
        "3. 避免大额交易，分批执行以减少滑点\n"
        "4. 检查跨链桥流动性，避免资产卡在桥上\n"
        "5. 确保 Gas 储备充足 (ETH/SOL 等原生代币)\n"
        "6. 评估所有 DeFi 头寸的退出路径是否畅通\n\n"
        "<b>持续监控:</b>\n"
        "- 主流池子的 TVL 和滑点变化\n"
        "- 借贷协议利用率 (>90% = 取款困难)\n"
        "- 链上 Gas 价格 (暴涨 = 拥堵)\n"
        "- 预言机价格偏差\n\n"
        "<b>恢复信号:</b>\n"
        "- 借贷协议利用率回落至 80% 以下\n"
        "- DEX 滑点恢复正常水平\n"
        "- 跨链桥恢复正常运作\n"
        "- 新资金开始流入 DeFi 协议"
    ),

    "governance_emergency": (
        "🚨 <b>协议安全事件应急清单</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "<b>立即执行:</b>\n"
        "1. 撤销该协议的所有 Token Approval (用 revoke.cash)\n"
        "2. 从该协议撤出所有存款/流动性\n"
        "3. 检查钱包是否与该协议有未完成的交互\n"
        "4. 如持有该协议治理代币，评估是否减仓\n"
        "5. 检查其他协议是否依赖该协议 (组合风险)\n"
        "6. 将资产转移到全新钱包 (如怀疑私钥泄露)\n\n"
        "<b>持续监控:</b>\n"
        "- 协议官方通报和事后分析\n"
        "- 安全团队的漏洞分析报告\n"
        "- 被盗资金链上追踪\n"
        "- 保险协议 (Nexus Mutual) 的索赔流程\n\n"
        "<b>恢复信号:</b>\n"
        "- 漏洞已修复并经第三方审计确认\n"
        "- 被盗资金已追回或协议承诺补偿\n"
        "- 协议发布详细的事后分析报告\n"
        "- 多家安全团队确认修复有效"
    ),
}


def format_black_swan_alert(
    alert_type: str,
    trigger_event: str = "",
    affected_assets: list[str] | None = None,
    severity: int = 5,
) -> str:
    """Format a black swan emergency alert with the matching playbook.

    Args:
        alert_type: Key in BLACK_SWAN_PLAYBOOKS (e.g. "stablecoin_depeg").
        trigger_event: Description of the triggering event.
        affected_assets: List of affected token symbols.
        severity: 1-5 severity rating.

    Returns:
        Telegram HTML formatted string with the playbook.
    """
    sev_bar = _severity_bar(severity, 5)
    assets_str = (
        " ".join(f"#{_esc(a)}" for a in affected_assets[:8])
        if affected_assets
        else ""
    )

    playbook = BLACK_SWAN_PLAYBOOKS.get(alert_type, "")
    if not playbook:
        playbook = (
            f"<b>未知事件类型:</b> {_esc(alert_type)}\n"
            "请联系管理员更新应急清单。"
        )

    trigger_line = (
        f"\n<b>触发事件:</b> {_esc(trigger_event)}\n" if trigger_event else ""
    )
    assets_line = f"<b>涉及资产:</b> {assets_str}\n" if assets_str else ""

    return (
        f"{sev_bar}\n"
        f"{trigger_line}"
        f"{assets_line}\n"
        f"{playbook}\n\n"
        f"⏰ {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}"
    )


def format_trade_signal(
    signal: dict,
    execution_params: dict | None = None,
) -> str:
    """Format a trade signal with optional execution parameters.

    Args:
        signal: Dict with keys: action ("BUY"/"SELL"), asset, reason,
                confidence (0-100), target_price, stop_loss.
        execution_params: Optional dict with keys: order_type, size_pct,
                          time_horizon, dca_splits.

    Returns:
        Telegram HTML formatted string.
    """
    action = signal.get("action", "UNKNOWN").upper()
    asset = _esc(signal.get("asset", ""))
    reason = _esc(signal.get("reason", ""))
    confidence = signal.get("confidence", 0)
    target = signal.get("target_price")
    stop_loss = signal.get("stop_loss")

    action_emoji = "🟢 BUY" if action == "BUY" else "🔴 SELL"
    conf_bar = _progress_bar(confidence, 100, 10)

    header = (
        f"📊 <b>交易信号 Trade Signal</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"  {action_emoji} <b>{asset}</b>\n"
        f"  置信度: {confidence}/100 {conf_bar}\n"
        f"  理由: {reason}\n"
    )

    price_block = ""
    if target is not None:
        price_block += f"  目标价: {_format_usd(target)}\n"
    if stop_loss is not None:
        price_block += f"  止损价: {_format_usd(stop_loss)}\n"

    exec_block = ""
    if execution_params:
        order_type = _esc(execution_params.get("order_type", "limit"))
        size_pct = execution_params.get("size_pct", 0)
        horizon = _esc(execution_params.get("time_horizon", ""))
        dca = execution_params.get("dca_splits", 1)

        exec_block = (
            f"\n  <b>执行参数:</b>\n"
            f"    订单类型: {order_type}\n"
            f"    仓位比例: {size_pct}%\n"
        )
        if horizon:
            exec_block += f"    时间周期: {horizon}\n"
        if dca and dca > 1:
            exec_block += f"    DCA 分批: {dca} 次\n"

    footer = f"\n⏰ {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}"

    return header + price_block + exec_block + footer


def format_weekly_risk_report(report_text: str) -> str:
    """Wrap a pre-generated weekly risk/yield report for Telegram delivery.

    Args:
        report_text: Plain-text report content (e.g. from defi_yield_weekly).

    Returns:
        Telegram HTML formatted string with header/footer.
    """
    return (
        f"📋 <b>CryptoScope 周报</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"<pre>{_esc(report_text)}</pre>\n\n"
        f"⏰ {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}"
    )


def format_yield_opportunity(
    pool_data: dict,
    safety_score: int = 0,
    safety_tier: str = "未评级",
) -> str:
    """Format a single DeFi yield opportunity for Telegram push.

    Args:
        pool_data: Dict with keys: project, symbol, chain, apy, apyMean30d,
                   tvlUsd, apyReward.
        safety_score: Protocol safety score (0-100).
        safety_tier: Safety tier label.

    Returns:
        Telegram HTML formatted string.
    """
    project = _esc(pool_data.get("project", "Unknown"))
    symbol = _esc(pool_data.get("symbol", "Unknown"))
    chain = _esc(pool_data.get("chain", "Unknown"))
    apy = pool_data.get("apy") or 0
    apy_mean = pool_data.get("apyMean30d")
    tvl = pool_data.get("tvlUsd") or 0
    apy_reward = pool_data.get("apyReward") or 0

    apy_trend = ""
    if apy_mean and apy_mean > 0:
        diff = ((apy - apy_mean) / apy_mean) * 100
        if diff > 10:
            apy_trend = " 📈上升"
        elif diff < -10:
            apy_trend = " 📉下降"
        else:
            apy_trend = " ➖稳定"

    safety_display = (
        f"{safety_score}/100 ({_esc(safety_tier)})"
        if safety_score > 0
        else "未评级"
    )

    score_bar = _progress_bar(safety_score, 100, 10) if safety_score > 0 else ""

    mean_line = f"  30日均值: {apy_mean:.2f}%\n" if apy_mean else ""
    reward_line = f"  奖励 APY: {apy_reward:.2f}%\n" if apy_reward else ""

    return (
        f"💰 <b>收益机会 Yield Opportunity</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"  <b>{project}</b> — {symbol} [{chain}]\n\n"
        f"  APY: <b>{apy:.2f}%</b>{apy_trend}\n"
        f"{mean_line}"
        f"{reward_line}"
        f"  TVL: {_format_usd(tvl)}\n\n"
        f"  安全评分: {safety_display} {score_bar}\n\n"
        f"⏰ {datetime.utcnow().strftime('%H:%M UTC')}"
    )


# ---------------------------------------------------------------------------
# Template: Price Response (价格查询回复)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Template: Meme Coin Alert (Meme 狙击雷达)
# ---------------------------------------------------------------------------


def format_meme_alert(token: dict) -> str:
    """Format a meme coin alert — actionable, with strategy and exit plan.

    Args:
        token: Dict with token data (see _build_meme_sections for fields).
    """
    name = _esc(token.get("token_name", "???"))
    symbol = _esc(token.get("token_symbol", "???")).upper()
    chain = _esc(token.get("chain_id", "unknown"))
    platform = _esc(token.get("platform", token.get("dex_id", "")))
    addr = token.get("token_address", "")
    addr_short = f"{addr[:6]}...{addr[-4:]}" if len(addr) > 10 else addr

    # Age
    age_min = token.get("age_minutes")
    if age_min is not None:
        if age_min < 60:
            age_str = f"{age_min}m"
        elif age_min < 1440:
            age_str = f"{age_min // 60}h{age_min % 60}m"
        else:
            age_str = f"{age_min // 1440}d"
    else:
        age_str = "?"

    # Market data
    mc_raw = token.get("market_cap") or 0
    mc = _format_usd(mc_raw)
    liq_raw = token.get("liquidity_usd") or 0
    liq = _format_usd(liq_raw)
    vol = _format_usd(token.get("volume_24h"))
    price = token.get("price_usd")
    price_str = f"${float(price):.8f}" if price else "N/A"

    # Price changes with momentum indicator
    chg_5m = token.get("price_change_5m")
    chg_1h = token.get("price_change_1h")
    chg_24h = token.get("price_change_24h")

    def _chg(v):
        if v is None:
            return "N/A"
        return f"+{v:.1f}%" if v > 0 else f"{v:.1f}%"

    # Momentum: is price accelerating or decelerating?
    momentum = ""
    if chg_5m is not None and chg_1h is not None and chg_1h != 0:
        # If 5m change rate > 1h change rate (annualized), accelerating
        rate_5m = abs(chg_5m) * 12  # project to 1h
        if chg_5m > 0 and rate_5m > abs(chg_1h) * 1.5:
            momentum = " 加速中 🚀"
        elif chg_5m < 0 and chg_1h > 0:
            momentum = " 减速 ⚠️"
        elif chg_5m > 0:
            momentum = " 上涨中 📈"

    # Security checklist — compact
    sec = token.get("security", {})
    checks = []
    _add_check(checks, not sec.get("is_honeypot"), "蜜罐")
    _add_check(checks, not sec.get("can_mint"), "增发")
    _add_check(checks, not sec.get("has_freeze"), "冻结")
    _add_check(checks, not sec.get("has_proxy"), "代理")

    lp_burned = sec.get("lp_burned_pct")
    lp_str = f"LP {lp_burned:.0f}%" if lp_burned is not None else ""
    goplus = sec.get("goplus_score")
    goplus_str = f"GoPlus {goplus}" if goplus is not None else ""
    safety_line = " ".join(checks)
    safety_extra = " · ".join(filter(None, [lp_str, goplus_str]))

    # Holders — compact
    holders = token.get("holder_count")
    dev_pct = token.get("dev_holding_pct")
    top10_pct = token.get("top10_holder_pct")
    holder_parts = []
    if holders is not None:
        holder_parts.append(f"{holders:,}人")
    if dev_pct is not None:
        dev_warn = "⚠️" if dev_pct > 5 else ""
        holder_parts.append(f"Dev {dev_pct:.1f}%{dev_warn}")
    if top10_pct is not None:
        holder_parts.append(f"Top10 {top10_pct:.1f}%")

    # Smart money — show details not just counts
    sm = token.get("smart_money", {})
    t1 = sm.get("t1_count", 0)
    t2 = sm.get("t2_count", 0)
    sm_details = sm.get("details", [])  # list of {label, amount, price, holding}
    sm_lines = []
    if t1 > 0 or t2 > 0:
        sm_lines.append(f"  聪明钱: {t1} T1 · {t2} T2")
    for detail in sm_details[:2]:
        label = _esc(detail.get("label", "?"))
        amt = _format_usd(detail.get("amount"))
        holding = "持仓中 ✅" if detail.get("holding") else "已卖出 ❌"
        sm_lines.append(f"    {label} 买入 {amt} · {holding}")
    sm_block = "\n".join(sm_lines)

    # Alpha score
    alpha = token.get("alpha_score", 0)
    grade = token.get("alpha_grade", "?")
    rec = token.get("recommendation", "")

    # ========== STRATEGY SECTION ==========
    # This is the key differentiator: actionable exit plan based on market cap
    strategy_lines = []

    if rec in ("BUY", "WATCH"):
        # Position sizing based on liquidity
        if liq_raw >= 50_000:
            size_suggestion = "$200-500"
        elif liq_raw >= 20_000:
            size_suggestion = "$100-200"
        elif liq_raw >= 5_000:
            size_suggestion = "$50-100"
        else:
            size_suggestion = "$20-50 (极低流动性)"

        # TP targets based on current market cap
        tp1_mc = mc_raw * 2.5 if mc_raw else 0
        tp2_mc = mc_raw * 5 if mc_raw else 0
        tp3_mc = mc_raw * 10 if mc_raw else 0

        # Stop loss
        sl_mc = mc_raw * 0.5 if mc_raw else 0

        # Time horizon based on age and platform
        if platform and "pump" in platform.lower() and mc_raw and mc_raw < 69_000:
            time_note = "毕业前交易 · 目标MC $69K+"
        elif age_min and age_min < 60:
            time_note = "极早期 · 1-4小时内决定"
        elif age_min and age_min < 360:
            time_note = "早期 · 24-48h窗口"
        else:
            time_note = "持仓上限 7-14天"

        strategy_lines = [
            f"━━ <b>策略建议</b> ━━━━━━━━━━━",
            f"  信号: <b>{rec}</b> · Alpha {alpha}/100 ({grade})",
            f"  建议仓位: {size_suggestion} (投机仓 ≤总仓2%)",
            f"  TP1: MC {_format_usd(tp1_mc)} (2.5x) 出50%",
            f"  TP2: MC {_format_usd(tp2_mc)} (5x) 出25%",
            f"  TP3: MC {_format_usd(tp3_mc)} (10x) 出15% · 余10%跑",
            f"  SL: MC {_format_usd(sl_mc)} (-50%) 或 Dev钱包异动",
            f"  R:R = 1:5 · {time_note}",
        ]
    elif rec == "SKIP":
        strategy_lines = [
            f"━━ <b>评估结果</b> ━━━━━━━━━━━",
            f"  ❌ SKIP · Alpha {alpha}/100 ({grade})",
        ]
        # Show veto reasons
        red_flags = token.get("red_flags", [])
        if red_flags:
            strategy_lines.append(f"  原因: {'; '.join(red_flags[:3])}")

    # Risk flags
    flags = token.get("flags", [])
    flag_line = ""
    if flags:
        flag_line = f"\n⚠️ {', '.join(flags[:3])}"

    # URL
    url = token.get("url", "")
    url_line = f' · <a href="{_esc(url)}">图表</a>' if url else ""

    strategy_block = "\n".join(strategy_lines) if strategy_lines else ""

    return (
        f"🎯 <b>${symbol}</b> ({name})\n"
        f"⛓ {chain} · {platform} · {age_str} · GoPlus {goplus or '?'}\n\n"
        f"💰 MC {mc} · Liq {liq} · Vol {vol}\n"
        f"📊 5m {_chg(chg_5m)} · 1h {_chg(chg_1h)} · 24h {_chg(chg_24h)}{momentum}\n\n"
        f"🛡️ {safety_line}\n"
        f"  {safety_extra}\n"
        f"👥 {' · '.join(holder_parts)}\n"
        f"{sm_block}\n\n"
        f"{strategy_block}{flag_line}\n\n"
        f"📍 CA: <code>{_esc(addr_short)}</code>{url_line}\n"
        f"⏰ {datetime.utcnow().strftime('%H:%M UTC')}"
    )


def _add_check(checks: list, is_safe: bool, label: str) -> None:
    """Append a safety check with pass/fail emoji."""
    checks.append(f"{'✅' if is_safe else '❌'}{label}")


def format_meme_digest(
    tokens: list[dict],
    scan_stats: dict | None = None,
) -> str:
    """Format a meme coin digest summary with multiple tokens.

    Args:
        tokens: List of token dicts (same format as format_meme_alert).
        scan_stats: Optional dict with total_scanned, passed_filter, chains.
    """
    stats = scan_stats or {}
    total = stats.get("total_scanned", 0)
    passed = stats.get("passed_filter", len(tokens))
    chains = stats.get("chains", [])

    lines = [
        f"🎰 <b>MEME 扫描报告</b>",
        f"━━━━━━━━━━━━━━━━━━━━",
        f"📡 扫描 {total} 代币 · {passed} 通过筛选",
    ]
    if chains:
        lines.append(f"⛓ {', '.join(chains)}")
    lines.append("")

    for i, t in enumerate(tokens[:8], 1):
        symbol = _esc(t.get("token_symbol", "???")).upper()
        chain = _esc(t.get("chain_id", ""))
        mc = _format_usd(t.get("market_cap"))
        liq = _format_usd(t.get("liquidity_usd"))
        alpha = t.get("alpha_score", 0)
        grade = t.get("alpha_grade", "?")
        rec = t.get("recommendation", "")
        chg_1h = t.get("price_change_1h")
        chg_str = f"{chg_1h:+.0f}%" if chg_1h is not None else ""

        # Safety quick check
        sec = t.get("security", {})
        safe_count = sum(1 for k in ("is_honeypot", "can_mint", "has_freeze") if not sec.get(k))
        safe_bar = "🟢" * safe_count + "🔴" * (3 - safe_count)

        rec_tag = {"BUY": "🟢", "WATCH": "🟡", "SKIP": "🔴"}.get(rec, "⚪")

        rank_prefix = "🏆" if i == 1 else f"{i}."
        url = t.get("url", "")
        link = f' <a href="{_esc(url)}">↗</a>' if url else ""

        lines.append(
            f"{rank_prefix} {rec_tag} <b>${symbol}</b> [{chain}] {chg_str}\n"
            f"   MC {mc} · Liq {liq} · Alpha {alpha} ({grade}) {safe_bar}{link}"
        )
        lines.append("")

    lines.append(f"⏰ {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Template: Price Response (价格查询回复)
# ---------------------------------------------------------------------------


def format_price_response(
    symbol: str,
    name: str,
    price: float,
    changes: dict,
    high_low: dict,
    mcap: float | int = 0,
    volume: float | int = 0,
    rank: int | None = None,
) -> str:
    """Format a price query response.

    Args:
        symbol: e.g. "BTC"
        name: e.g. "Bitcoin"
        price: Current price in USD
        changes: {1h: float, 24h: float, 7d: float}
        high_low: {high_24h: float, low_24h: float}
        mcap: Market cap in USD
        volume: 24h volume in USD
        rank: CoinGecko market cap rank
    """
    chg_24h = changes.get("24h", 0) or 0
    # Trend emoji based on 24h change
    if chg_24h > 5:
        trend = "🚀"
    elif chg_24h > 0:
        trend = "📈"
    elif chg_24h < -5:
        trend = "💥"
    elif chg_24h < 0:
        trend = "📉"
    else:
        trend = "➖"

    rank_str = f"  Rank #{rank}" if rank else ""
    symbol_esc = _esc(symbol.upper())
    name_esc = _esc(name)

    chg_1h = _change_indicator(changes.get("1h"))
    chg_24h_str = _change_indicator(chg_24h)
    chg_7d = _change_indicator(changes.get("7d"))

    high = _format_usd(high_low.get("high_24h"))
    low = _format_usd(high_low.get("low_24h"))

    return (
        f"{trend} <b>{name_esc} ({symbol_esc})</b>{rank_str}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 <b>Price:</b> {_format_usd(price)}\n\n"
        f"  1h   {chg_1h}\n"
        f"  24h  {chg_24h_str}\n"
        f"  7d   {chg_7d}\n\n"
        f"📊 24h High: {high}\n"
        f"📊 24h Low:  {low}\n\n"
        f"💎 Market Cap: {_format_usd(mcap)}\n"
        f"📊 24h Volume: {_format_usd(volume)}"
    )


def format_accumulation_alert(
    token: dict,
    divergence: dict,
    position_hint: dict | None = None,
) -> str:
    """Format a 二级妖币 accumulation-divergence alert (Telegram HTML).

    Args:
        token: {symbol, address, chain, mc, liquidity, security_score, url}
        divergence: signal components {gap_slope, effective_level_pct,
            decelerating, float_active, nominal_top_n_pct?}
        position_hint: optional {pct, note} from the Kelly sizer.
    """
    sym = _esc(token.get("symbol", "?"))
    chain = _esc(token.get("chain", ""))
    addr = _esc(token.get("address", ""))
    url = token.get("url", "")

    eff = divergence.get("effective_level_pct", 0)
    nominal = divergence.get("nominal_top_n_pct")
    gap_slope = divergence.get("gap_slope", 0)
    float_active = divergence.get("float_active", 0)
    decel = divergence.get("decelerating", False)

    lines = [
        f"🕵️ <b>二级妖币吸筹背离</b> — {sym}",
        f"链: {chain}",
        "",
        "<b>━━ 集中度背离 ━━</b>",
        f"有效集中度(聚类后): <b>{eff:.0f}%</b>"
        + (f"  vs 名义 {nominal:.0f}%" if nominal is not None else ""),
        f"背离斜率: {gap_slope:.2f} /快照 {'📈' if gap_slope > 0 else ''}",
        f"吸筹斜率: {'拐头减速 ⚠️ 庄快收完' if decel else '仍在加速'}",
        f"浮筹活跃度: {float_active:.0%} {'(启动还在前面 ✅)' if float_active >= 0.35 else '(浮筹偏少)'}",
    ]

    mc = token.get("mc")
    liq = token.get("liquidity")
    sec = token.get("security_score")
    if mc is not None or liq is not None or sec is not None:
        lines.append("")
        lines.append("<b>━━ 基本面 ━━</b>")
        if mc is not None:
            lines.append(f"市值: {_format_usd(mc)}")
        if liq is not None:
            lines.append(f"流动性: {_format_usd(liq)}")
        if sec is not None:
            lines.append(f"安全分: {sec}/100 {'✅' if sec >= 70 else '⚠️'}")

    if position_hint:
        lines.append("")
        lines.append("<b>━━ 纸面仓位建议 (Kelly折半) ━━</b>")
        lines.append(f"建议仓位: {position_hint.get('pct', 0):.1%}")
        if position_hint.get("note"):
            lines.append(_esc(position_hint["note"]))

    if addr:
        lines.append("")
        lines.append(f"<code>{addr}</code>")
    if url:
        lines.append(f'🔗 <a href="{_esc(url)}">图表</a>')

    lines.append("")
    lines.append("<i>检测/研究信号，非投资建议。慢信号，需耐心等启动。</i>")
    return "\n".join(lines)
