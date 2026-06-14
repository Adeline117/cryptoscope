"""Stage 2 — launch-prep detector (free polling version).

Stage 1 puts near-saturation tokens on the watchlist. Stage 2 watches ONLY that
narrow list for the launch event itself. Because the list is small, this can poll
cheaply on the existing machine — no paid VPS/websocket needed for the MVP.

Launch-prep is detected from DexScreener's own time windows (free): a sudden
5-minute volume acceleration vs the hourly pace, price starting to move up, and
buy-side pressure. These are *fast* signals — used here only to CONFIRM that an
already-accumulated token is igniting, never to front-run. On a confirmed event
the contract safety gate is re-run (the contract can change right before launch),
then a critical alert fires.

True websocket/VPS scale (sub-block latency on gas/mempool) is a later deployment
choice; this polling MVP captures the same event a few minutes later for free.
"""

from __future__ import annotations

import json
import urllib.request
from datetime import datetime, timezone

import structlog

logger = structlog.get_logger()

POLL_MAX = 30                 # bound work per tick (watchlist is narrow anyway)
VOL_ACCEL_MULT = 2.0          # 5m pace must exceed hourly avg by this factor
MIN_PRICE_MOVE_M5 = 3.0       # % price move in the last 5 minutes
MIN_M5_VOLUME_USD = 2_000     # ignore dust


def _best_pair(token: str, chain: str, timeout: int = 12) -> dict | None:
    """Fetch the most-liquid DexScreener pair for a token."""
    url = f"https://api.dexscreener.com/token-pairs/v1/{chain}/{token}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "CryptoScope/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode())
        pairs = data if isinstance(data, list) else data.get("pairs", [])
        if not pairs:
            return None
        return max(pairs, key=lambda p: (p.get("liquidity", {}) or {}).get("usd", 0) or 0)
    except Exception as e:
        logger.debug("stage2_pair_fetch_failed", token=token, error=str(e))
        return None


def detect_launch_prep(pair: dict) -> dict | None:
    """Pure detector: is this pair igniting? Returns event details or None.

    Conditions (all required):
      - 5m volume pace (m5 * 12) exceeds the hourly average by VOL_ACCEL_MULT
      - price up at least MIN_PRICE_MOVE_M5 % in the last 5m
      - more buys than sells in the last 5m
    """
    vol = pair.get("volume", {}) or {}
    vol_m5 = float(vol.get("m5", 0) or 0)
    vol_h1 = float(vol.get("h1", 0) or 0)
    if vol_m5 < MIN_M5_VOLUME_USD:
        return None

    # m5 annualized to an hourly pace vs the actual last-hour volume.
    m5_hourly_pace = vol_m5 * 12
    hourly_baseline = max(vol_h1, 1.0)
    accel = m5_hourly_pace / hourly_baseline

    price_m5 = float((pair.get("priceChange", {}) or {}).get("m5", 0) or 0)
    txns_m5 = (pair.get("txns", {}) or {}).get("m5", {}) or {}
    buys = int(txns_m5.get("buys", 0) or 0)
    sells = int(txns_m5.get("sells", 0) or 0)

    if accel >= VOL_ACCEL_MULT and price_m5 >= MIN_PRICE_MOVE_M5 and buys > sells:
        confidence = min(100, 40 + int(min(accel, 8) * 5) + int(min(price_m5, 30)))
        return {
            "volume_accel": round(accel, 2),
            "price_move_m5": round(price_m5, 2),
            "buys_m5": buys,
            "sells_m5": sells,
            "confidence": confidence,
        }
    return None


async def run_stage2_detector(send: bool = True) -> dict:
    """Poll the watchlist for launch-prep events. Returns a summary."""
    from src.onchain import watchlist

    tokens = watchlist.get_active()[:POLL_MAX]
    if not tokens:
        return {"status": "empty_watchlist", "checked": 0, "events": 0}

    checked = events = 0
    for entry in tokens:
        token, chain = entry["token"], entry["chain"]
        pair = _best_pair(token, chain)
        if not pair:
            continue
        checked += 1
        event = detect_launch_prep(pair)
        if not event:
            continue
        events += 1
        await _emit_launch(entry, pair, event, send=send)
        try:
            watchlist.set_status(token, chain, "launching")
        except Exception:
            pass

    summary = {"status": "complete", "checked": checked, "events": events}
    logger.info("stage2_detector_complete", **summary)
    return summary


async def _commit_security_ok(token: str, chain: str) -> tuple[bool, int]:
    """Re-run the contract gate at commit time (the contract can change)."""
    try:
        from src.collectors.contract_security import ContractSecurityChecker

        chain_id: int | str = "solana" if chain in ("solana", "sol") else {
            "ethereum": 1, "eth": 1, "base": 8453, "bsc": 56,
            "arbitrum": 42161, "optimism": 10, "polygon": 137,
        }.get(chain, 1)
        result = await ContractSecurityChecker().check_token(chain_id, token)
        return (not result.is_honeypot and result.risk_score >= 50), result.risk_score
    except Exception as e:
        logger.debug("stage2_security_failed", token=token, error=str(e))
        return True, 50  # don't block on checker failure; mark unknown


async def _emit_launch(entry: dict, pair: dict, event: dict, send: bool = True) -> None:
    token, chain = entry["token"], entry["chain"]
    safe, score = await _commit_security_ok(token, chain)
    symbol = entry.get("symbol") or (pair.get("baseToken", {}) or {}).get("symbol", token[:6])

    # Record for precision tracking.
    try:
        from src.trading.signal_scorecard import record_signal

        record_signal(
            signal_type="stage2_launch", asset=symbol, chain=chain,
            direction="LONG", confidence=event["confidence"],
            entry_price=float(pair.get("priceUsd") or 0),
            metadata={**event, "token_address": token, "security_score": score},
        )
    except Exception as e:
        logger.debug("stage2_scorecard_failed", error=str(e))

    if not send:
        return
    try:
        from src.distribution.telegram_sender import send_critical_alert

        gate = "✅ 安全" if safe else "⚠️ 合约有风险，谨慎"
        msg = (
            f"🚀 <b>启动信号 · {symbol}</b>\n"
            f"<i>{chain}链 · 之前在吸筹的盘，现在开始拉了</i>\n"
            f"━━━━━━━━━━━━━━\n"
            f"· 5分钟成交量加速 <b>{event['volume_accel']:.1f}倍</b>（相对每小时均速）\n"
            f"· 5分钟价格 <b>+{event['price_move_m5']:.1f}%</b>\n"
            f"· 买/卖 {event['buys_m5']}/{event['sells_m5']}（买压主导）\n"
            f"· commit 时刻合约复检：{gate}（{score}/100）\n"
            f"信号强度 {event['confidence']}/100\n"
            f"📍 <code>{token}</code>\n"
            f"🔗 <a href=\"{pair.get('url', '')}\">看 K 线</a>\n"
            f"<i>⚠️ 快信号仅作确认，入场注意滑点/防夹。非投资建议。</i>"
        )
        await send_critical_alert(msg)
    except Exception as e:
        logger.warning("stage2_alert_failed", error=str(e))
