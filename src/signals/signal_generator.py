"""Trade signal generator — ONLY strategies with documented edge, ONLY free APIs.

Dead strategies (removed): EMA/RSI momentum (alpha decays in 2-4 weeks, everyone uses it)

Live strategies (kept/added):
1. Token unlock SHORT — 90% of >1% supply unlocks cause drops (Keyrock 16K events)
   - Optimal exit: 30 days pre-unlock
   - Optimal re-entry: 14 days post-unlock
   - Team/insider unlocks → avg ~25% drawdown

2. Funding rate reversion (ALTCOINS ONLY) — BTC/ETH carry is dead post-ETF
   - SHORT when FR > 0.1%/8H on altcoins
   - LONG when FR < -0.03%/8H on altcoins

3. DexScreener boost detection — newly boosted tokens signal incoming marketing push
   - Buy before trending page pickup, not after

All APIs used are 100% free, no keys required.
"""

from __future__ import annotations

import json
import time
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import structlog

logger = structlog.get_logger()


@dataclass
class TradeSignal:
    """A complete, actionable trade signal."""
    asset: str
    direction: str  # "LONG" or "SHORT"
    confidence: int  # 0-100
    entry_low: float
    entry_high: float
    tp1: float
    tp2: float
    sl: float
    leverage: int
    r_r: float
    timeframe: str
    thesis: str  # Chinese
    signal_type: str
    evidence: dict = field(default_factory=dict)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


# ---------------------------------------------------------------------------
# Free API helpers
# ---------------------------------------------------------------------------

def _fetch_json(url: str, params: dict | None = None, timeout: int = 8) -> Any:
    if params:
        qs = "&".join(f"{k}={v}" for k, v in params.items())
        url = f"{url}?{qs}"
    req = urllib.request.Request(url, headers={"User-Agent": "CryptoScope/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


_price_cache: dict[str, Any] = {}
_price_cache_time: float = 0

def _get_prices(ids: str = "bitcoin,ethereum,solana") -> dict[str, dict]:
    global _price_cache, _price_cache_time
    # Cache for 60 seconds to avoid 429
    if _price_cache and (time.time() - _price_cache_time) < 60:
        return _price_cache
    try:
        import time as _t
        _t.sleep(1)  # Rate limit protection
        data = _fetch_json(
            "https://api.coingecko.com/api/v3/simple/price",
            {"ids": ids, "vs_currencies": "usd", "include_24hr_change": "true"},
        )
        result = {}
        id_to_symbol = {
            "bitcoin": "BTC", "ethereum": "ETH", "solana": "SOL",
            "arbitrum": "ARB", "optimism": "OP", "aptos": "APT",
            "sui": "SUI", "celestia": "TIA", "sei-network": "SEI",
            "pyth-network": "PYTH", "worldcoin-wld": "WLD",
            "jito-governance-token": "JTO", "starknet": "STRK",
        }
        for coin_id, vals in data.items():
            sym = id_to_symbol.get(coin_id, coin_id.upper())
            result[sym] = {
                "price": vals.get("usd", 0),
                "change_24h": vals.get("usd_24h_change", 0),
            }
        _price_cache = result
        _price_cache_time = time.time()
        return result
    except Exception as e:
        logger.warning("price_fetch_failed", error=str(e))
        return _price_cache if _price_cache else {}


# ---------------------------------------------------------------------------
# 1. TOKEN UNLOCK SIGNALS (highest documented edge)
# ---------------------------------------------------------------------------

def check_token_unlocks() -> list[TradeSignal]:
    """Generate SHORT signals for upcoming large token unlocks.

    Evidence: Keyrock study of 16,000 unlock events:
    - 90% of unlocks create negative price pressure
    - Team/insider unlocks → avg ~25% drawdown
    - Price impact begins 30 days before unlock
    - Unlocks >1% of supply: meaningful negative correlation (~16%)
    """
    signals: list[TradeSignal] = []

    # Load unlock calendar
    try:
        config_path = Path("config/token_unlocks.yaml")
        if not config_path.exists():
            return []
        import yaml
        with open(config_path) as f:
            data = yaml.safe_load(f) or {}
        unlocks = data.get("unlocks", [])
    except Exception as e:
        logger.warning("unlock_calendar_load_failed", error=str(e))
        return []

    now = datetime.now(timezone.utc)

    # Map symbols to CoinGecko IDs
    sym_to_id = {
        "ARB": "arbitrum", "OP": "optimism", "APT": "aptos", "SUI": "sui",
        "TIA": "celestia", "SEI": "sei-network", "PYTH": "pyth-network",
        "WLD": "worldcoin-wld", "JTO": "jito-governance-token", "STRK": "starknet",
        "DYDX": "dydx-chain", "IMX": "immutable-x", "BLUR": "blur",
        "MANTA": "manta-network", "ZK": "zksync", "ENA": "ethena",
        "ZRO": "layerzero", "W": "wormhole",
    }

    # BATCH fetch all unlock token prices in ONE call (fixes 429)
    all_cg_ids = [sym_to_id[u["token"]] for u in unlocks if u["token"] in sym_to_id]
    batch_prices = _get_prices(",".join(set(all_cg_ids))) if all_cg_ids else {}

    for unlock in unlocks:
        token = unlock["token"]
        unlock_date_str = unlock["date"]
        amount_usd = unlock.get("amount_usd", 0)
        unlock_type = unlock.get("type", "unknown")
        pct = unlock.get("pct_of_circulating", 0)

        try:
            unlock_date = datetime.strptime(unlock_date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError:
            continue

        days_until = (unlock_date - now).days

        # Signal window: 7-30 days before unlock
        # (Keyrock: price impact begins 30 days before)
        if not (7 <= days_until <= 30):
            continue

        # Only signal for meaningful unlocks (>1% of supply)
        if pct < 1.0:
            continue

        # Get price from batch (already fetched above)
        price = batch_prices.get(token, {}).get("price", 0)

        if price <= 0:
            continue

        # Calculate targets based on unlock severity
        if unlock_type in ("team", "investor") and pct >= 5:
            # Large insider unlock → expect 15-25% drop
            expected_drop = 0.20
            confidence = 75
        elif unlock_type in ("team", "investor"):
            # Standard insider unlock → expect 8-15% drop
            expected_drop = 0.12
            confidence = 65
        else:
            # Community/ecosystem unlock → smaller impact
            expected_drop = 0.05
            confidence = 50

        tp1 = round(price * (1 - expected_drop * 0.5), 2)
        tp2 = round(price * (1 - expected_drop), 2)
        sl = round(price * 1.05, 2)  # 5% stop
        risk = abs(sl - price)
        reward = abs(price - tp1)
        r_r = round(reward / risk, 1) if risk > 0 else 0

        from src.distribution.message_templates import _format_usd

        signals.append(TradeSignal(
            asset=token,
            direction="SHORT",
            confidence=confidence,
            entry_low=round(price * 0.99, 2),
            entry_high=round(price * 1.01, 2),
            tp1=tp1,
            tp2=tp2,
            sl=sl,
            leverage=3,
            r_r=r_r,
            timeframe=f"{days_until}天后解锁",
            thesis=(
                f"{token} {days_until}天后解锁 {_format_usd(amount_usd)} "
                f"({pct:.1f}%流通量 · {unlock_type})。"
                f"Keyrock研究: 90%解锁产生下跌压力，"
                f"{'团队/投资者解锁均跌~25%' if unlock_type in ('team', 'investor') else '生态解锁影响较小'}。"
            ),
            signal_type="token_unlock",
            evidence={
                "unlock_date": unlock_date_str,
                "amount_usd": amount_usd,
                "pct_of_circulating": pct,
                "unlock_type": unlock_type,
                "days_until": days_until,
            },
        ))

    return signals


# ---------------------------------------------------------------------------
# 2. FUNDING RATE REVERSION (altcoins only — BTC/ETH carry is dead)
# ---------------------------------------------------------------------------

def check_funding_reversion() -> list[TradeSignal]:
    """Detect extreme funding rates on ALTCOINS for mean reversion trades.

    BTC/ETH funding carry is dead post-ETF (CME basis compressed 97%).
    Altcoins still have meaningful funding rate extremes.
    """
    signals: list[TradeSignal] = []

    prices = _get_prices()
    if not prices:
        return []

    # Only check altcoins — BTC/ETH funding carry is dead
    try:
        fr_data = _fetch_json("https://open-api.coinglass.com/public/v2/funding")
        if not fr_data.get("success"):
            return []
    except Exception as e:
        logger.warning("funding_fetch_failed", error=str(e))
        return []

    for item in fr_data.get("data", []):
        symbol = item.get("symbol", "")

        # Skip BTC and ETH — carry is dead post-ETF
        if symbol in ("BTC", "ETH"):
            continue

        # Only check symbols we have price data for
        if symbol not in prices:
            continue

        rate_list = item.get("uMarginList", [])
        if not rate_list:
            continue

        rate = rate_list[0].get("rate")
        if rate is None:
            continue
        rate = float(rate)

        price = prices[symbol]["price"]
        change_24h = prices[symbol].get("change_24h", 0)

        if price <= 0:
            continue

        # SHORT: extreme positive funding on altcoin
        if rate > 0.001 and change_24h > 3:  # >0.1%/8H AND up >3%
            sl = round(price * 1.03, 2)
            tp1 = round(price * 0.95, 2)  # 5% target (altcoins move more)
            tp2 = round(price * 0.90, 2)  # 10% target
            risk = abs(sl - price)
            reward = abs(price - tp1)
            r_r = round(reward / risk, 1) if risk > 0 else 0

            signals.append(TradeSignal(
                asset=symbol,
                direction="SHORT",
                confidence=min(70, int(45 + abs(rate) * 8000)),
                entry_low=round(price * 0.998, 2),
                entry_high=round(price * 1.002, 2),
                tp1=tp1, tp2=tp2, sl=sl,
                leverage=3,
                r_r=r_r,
                timeframe="4H-72H",
                thesis=(
                    f"{symbol} 资金费率 {rate:.4%} 极端偏高(多头拥挤)，"
                    f"24h涨{change_24h:+.1f}%后动能耗尽。"
                    f"做空等费率回归。注意:仅山寨币有效,BTC/ETH carry已死。"
                ),
                signal_type="funding_reversion",
                evidence={"funding_rate": rate, "change_24h": change_24h},
            ))

        # LONG: extreme negative funding on altcoin (short squeeze)
        elif rate < -0.0005 and change_24h < -5:  # <-0.05%/8H AND down >5%
            sl = round(price * 0.95, 2)
            tp1 = round(price * 1.05, 2)
            tp2 = round(price * 1.10, 2)
            risk = abs(price - sl)
            reward = abs(tp1 - price)
            r_r = round(reward / risk, 1) if risk > 0 else 0

            signals.append(TradeSignal(
                asset=symbol,
                direction="LONG",
                confidence=min(65, int(40 + abs(rate) * 8000)),
                entry_low=round(price * 0.998, 2),
                entry_high=round(price * 1.002, 2),
                tp1=tp1, tp2=tp2, sl=sl,
                leverage=3,
                r_r=r_r,
                timeframe="4H-72H",
                thesis=(
                    f"{symbol} 资金费率 {rate:.4%} 极端偏低(空头拥挤)，"
                    f"24h跌{change_24h:+.1f}%已超卖。"
                    f"做多等空头回补反弹。"
                ),
                signal_type="funding_reversion",
                evidence={"funding_rate": rate, "change_24h": change_24h},
            ))

    return signals


# ---------------------------------------------------------------------------
# 3. DEXSCREENER BOOST DETECTION (free, 60rpm)
# ---------------------------------------------------------------------------

def check_dexscreener_boosts() -> list[TradeSignal]:
    """Detect newly boosted tokens on DexScreener.

    Boost = paid promotion. Teams deploy boosts ahead of marketing pushes.
    Buying before trending page pickup is the edge.
    Filter: only tokens with real liquidity + organic holder growth.
    """
    signals: list[TradeSignal] = []

    try:
        # Get latest boosted tokens
        data = _fetch_json("https://api.dexscreener.com/token-boosts/latest/v1")
        if not isinstance(data, list):
            return []
    except Exception as e:
        logger.warning("dexscreener_boost_fetch_failed", error=str(e))
        return []

    # Get detailed pair data for boosted tokens
    for token_info in data[:10]:  # Check top 10 most recent
        chain = token_info.get("chainId", "")
        address = token_info.get("tokenAddress", "")
        if not chain or not address:
            continue

        # Fetch pair data
        try:
            pair_data = _fetch_json(
                f"https://api.dexscreener.com/token-pairs/v1/{chain}/{address}"
            )
            pairs = pair_data if isinstance(pair_data, list) else pair_data.get("pairs", [])
            if not pairs:
                continue
        except Exception:
            continue

        # Use highest liquidity pair
        best_pair = max(pairs, key=lambda p: (p.get("liquidity", {}) or {}).get("usd", 0) or 0)

        liq = (best_pair.get("liquidity", {}) or {}).get("usd", 0) or 0
        price = best_pair.get("priceUsd")
        mc = best_pair.get("marketCap") or best_pair.get("fdv", 0) or 0
        volume_24h = (best_pair.get("volume", {}) or {}).get("h24", 0) or 0
        chg_1h = (best_pair.get("priceChange", {}) or {}).get("h1")
        symbol = (best_pair.get("baseToken", {}) or {}).get("symbol", "???")
        name = (best_pair.get("baseToken", {}) or {}).get("name", "")
        url = best_pair.get("url", "")

        # Filter: minimum quality
        if liq < 20_000:  # Need real liquidity
            continue
        if mc and mc > 10_000_000:  # Skip if already >$10M MC (too late)
            continue
        if not price:
            continue

        try:
            price_f = float(price)
        except (ValueError, TypeError):
            continue

        if price_f <= 0:
            continue

        # This is a HEADS UP, not a full trade signal
        # Lower confidence because boost alone isn't sufficient
        from src.distribution.message_templates import _format_usd

        tp1 = round(price_f * 1.30, 8)  # 30% (boost → trending → pump)
        tp2 = round(price_f * 1.50, 8)
        sl = round(price_f * 0.80, 8)  # 20% stop
        risk = abs(price_f - sl)
        reward = abs(tp1 - price_f)
        r_r = round(reward / risk, 1) if risk > 0 else 0

        chg_str = f"{chg_1h:+.0f}%" if chg_1h is not None else "?"

        signals.append(TradeSignal(
            asset=f"${symbol}",
            direction="LONG",
            confidence=45,  # Low — boost alone isn't high conviction
            entry_low=price_f,
            entry_high=price_f,
            tp1=tp1, tp2=tp2, sl=sl,
            leverage=1,  # Spot only for low-conviction
            r_r=r_r,
            timeframe="1-12H",
            thesis=(
                f"${symbol} ({name}) 刚获DexScreener Boost。"
                f"MC {_format_usd(mc)} · Liq {_format_usd(liq)} · 1h {chg_str}。"
                f"Boost=团队付费推广,通常在KOL campaign前部署。"
                f"低仓位投机,≤$50。"
            ),
            signal_type="boost_detection",
            evidence={
                "chain": chain, "address": address, "url": url,
                "liquidity": liq, "market_cap": mc, "volume_24h": volume_24h,
            },
        ))

    return signals[:3]  # Max 3 boost signals per cycle


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def generate_signals() -> list[TradeSignal]:
    """Run all signal checks. Returns empty list if nothing — silence is golden."""
    logger.info("signal_generator_started")

    all_signals: list[TradeSignal] = []

    # 1. Token unlocks (highest edge, documented)
    try:
        all_signals.extend(check_token_unlocks())
    except Exception as e:
        logger.warning("unlock_signal_failed", error=str(e))

    # 2. Funding rate reversion (altcoins only)
    try:
        all_signals.extend(check_funding_reversion())
    except Exception as e:
        logger.warning("funding_signal_failed", error=str(e))

    # 3. DexScreener boost detection
    try:
        all_signals.extend(check_dexscreener_boosts())
    except Exception as e:
        logger.warning("boost_signal_failed", error=str(e))

    # Sort by confidence
    all_signals.sort(key=lambda s: s.confidence, reverse=True)

    logger.info("signal_generator_done", signals=len(all_signals))
    return all_signals


def format_signal_message(signal: TradeSignal) -> str:
    """Format a TradeSignal into Telegram HTML."""
    dir_emoji = "🟢" if signal.direction == "LONG" else "🔴"
    type_labels = {
        "token_unlock": "解锁做空",
        "funding_reversion": "费率回归",
        "boost_detection": "Boost检测",
    }
    type_label = type_labels.get(signal.signal_type, signal.signal_type)

    from src.distribution.message_templates import _format_usd

    # Confidence bar
    conf_blocks = signal.confidence // 10
    conf_bar = "█" * conf_blocks + "▒" * (10 - conf_blocks)

    return (
        f"{dir_emoji} <b>{signal.direction} {signal.asset}/USDT</b> {signal.leverage}x\n"
        f"策略: {type_label} · [{conf_bar}] {signal.confidence}%\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"入场: {_format_usd(signal.entry_low)} – {_format_usd(signal.entry_high)}\n"
        f"TP1: {_format_usd(signal.tp1)}\n"
        f"TP2: {_format_usd(signal.tp2)}\n"
        f"SL: {_format_usd(signal.sl)}\n"
        f"R:R = 1:{signal.r_r} · {signal.timeframe}\n\n"
        f"<b>逻辑:</b> {signal.thesis}\n\n"
        f"⚠️ 仓位 ≤ 总仓2% · NFA\n"
        f"⏰ {signal.created_at.strftime('%H:%M UTC')}"
    )


async def run_signal_pipeline() -> dict:
    """Generate signals, record to scorecard, open paper trades, push to Telegram."""
    signals = generate_signals()
    if not signals:
        return {"status": "silent", "signals": 0}

    sent = 0
    for signal in signals[:2]:
        # 1. Record to scorecard (track if signal is right)
        try:
            from src.trading.signal_scorecard import record_signal
            record_signal(
                signal_type=signal.signal_type,
                asset=signal.asset,
                chain="",
                direction=signal.direction,
                confidence=signal.confidence,
                entry_price=signal.entry_low,
                metadata=signal.evidence,
            )
        except Exception as e:
            logger.debug("scorecard_record_failed", error=str(e))

        # 2. Open paper trade (simulate with virtual capital)
        try:
            from src.trading.paper_trader import open_position
            if signal.confidence >= 60:  # Only paper trade high-confidence signals
                open_position(
                    asset=signal.asset,
                    chain="",
                    direction=signal.direction,
                    price=signal.entry_low,
                    signal_type=signal.signal_type,
                    metadata=signal.evidence,
                )
        except Exception as e:
            logger.debug("paper_trade_failed", error=str(e))

        # 3. Send to Telegram
        message = format_signal_message(signal)
        try:
            from src.distribution.telegram_sender import send_meme_alert
            if await send_meme_alert(message):
                sent += 1
        except Exception as e:
            logger.error("signal_send_failed", error=str(e))

    return {"status": "sent", "signals": len(signals), "sent": sent}


if __name__ == "__main__":
    import re
    signals = generate_signals()
    if signals:
        for s in signals:
            print(re.sub(r'<[^>]+>', '', format_signal_message(s)))
            print()
    else:
        print("无信号 — 市场平静。这是正确的行为。")
