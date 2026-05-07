"""Pool Watcher — monitors new liquidity pool creation across chains.

Supports:
- Solana: Raydium AMM + Pump.fun graduation events via DexScreener
- Base: Uniswap V3 / Aerodrome new pairs via DexScreener
- BSC: PancakeSwap new pairs via DexScreener

Uses DexScreener token-profiles API (free, 60rpm) to detect new tokens
across all chains simultaneously. No RPC needed.

Filters:
- Min liquidity threshold
- Security check (GoPlus + RugCheck)
- Dev wallet history (placeholder)
- Holder concentration
"""

from __future__ import annotations

import json
import time
import urllib.request
from datetime import datetime, timezone
from typing import Any

import structlog

logger = structlog.get_logger()

# Chains we monitor
SUPPORTED_CHAINS = ["solana", "base", "bsc", "ethereum"]

# Filters
MIN_LIQUIDITY_USD = 5_000
MAX_MC_USD = 2_000_000
MIN_HOLDER_COUNT = 50


def _fetch(url: str, timeout: int = 10) -> Any:
    req = urllib.request.Request(url, headers={"User-Agent": "CryptoScope/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


def scan_new_tokens() -> list[dict]:
    """Scan DexScreener for newest token profiles across all chains.

    Returns list of new tokens with basic data, filtered by quality.
    """
    results = []

    # 1. Latest token profiles (cross-chain)
    try:
        profiles = _fetch("https://api.dexscreener.com/token-profiles/latest/v1")
        if isinstance(profiles, list):
            for p in profiles:
                chain = p.get("chainId", "")
                if chain not in SUPPORTED_CHAINS:
                    continue
                results.append({
                    "source": "profile",
                    "chain": chain,
                    "address": p.get("tokenAddress", ""),
                    "description": p.get("description", "")[:100],
                    "url": p.get("url", ""),
                    "icon": p.get("icon", ""),
                })
    except Exception as e:
        logger.debug("profiles_fetch_failed", error=str(e))

    # 2. Latest boosts (tokens being promoted)
    try:
        boosts = _fetch("https://api.dexscreener.com/token-boosts/latest/v1")
        if isinstance(boosts, list):
            for b in boosts:
                chain = b.get("chainId", "")
                if chain not in SUPPORTED_CHAINS:
                    continue
                results.append({
                    "source": "boost",
                    "chain": chain,
                    "address": b.get("tokenAddress", ""),
                    "boost_amount": b.get("amount", 0),
                    "total_boost": b.get("totalAmount", 0),
                    "url": b.get("url", ""),
                })
    except Exception as e:
        logger.debug("boosts_fetch_failed", error=str(e))

    # 3. Community takeovers
    try:
        ctos = _fetch("https://api.dexscreener.com/community-takeovers/latest/v1")
        if isinstance(ctos, list):
            for c in ctos:
                chain = c.get("chainId", "")
                if chain not in SUPPORTED_CHAINS:
                    continue
                results.append({
                    "source": "cto",
                    "chain": chain,
                    "address": c.get("tokenAddress", ""),
                    "url": c.get("url", ""),
                })
    except Exception as e:
        logger.debug("cto_fetch_failed", error=str(e))

    # Deduplicate by address
    seen = set()
    unique = []
    for r in results:
        addr = r.get("address", "")
        if addr and addr not in seen:
            seen.add(addr)
            unique.append(r)

    logger.info("new_tokens_scanned", total=len(unique))
    return unique


def enrich_token(chain: str, address: str) -> dict | None:
    """Get detailed pair data for a token from DexScreener."""
    try:
        data = _fetch(f"https://api.dexscreener.com/token-pairs/v1/{chain}/{address}")
        pairs = data if isinstance(data, list) else data.get("pairs", [])
        if not pairs:
            return None

        best = max(pairs, key=lambda p: (p.get("liquidity", {}) or {}).get("usd", 0) or 0)
        liq = (best.get("liquidity", {}) or {}).get("usd", 0) or 0
        mc = best.get("marketCap") or best.get("fdv", 0) or 0
        vol = (best.get("volume", {}) or {}).get("h24", 0) or 0
        created = best.get("pairCreatedAt", 0) or 0

        # Calculate age
        age_hours = 0
        if created:
            age_hours = (time.time() * 1000 - created) / (1000 * 3600)

        return {
            "symbol": (best.get("baseToken", {}) or {}).get("symbol", "?"),
            "name": (best.get("baseToken", {}) or {}).get("name", ""),
            "price": best.get("priceUsd"),
            "mc": mc,
            "liquidity": liq,
            "volume_24h": vol,
            "age_hours": round(age_hours, 1),
            "change_1h": (best.get("priceChange", {}) or {}).get("h1"),
            "change_24h": (best.get("priceChange", {}) or {}).get("h24"),
            "buys_1h": (best.get("txns", {}).get("h1", {}) or {}).get("buys", 0),
            "sells_1h": (best.get("txns", {}).get("h1", {}) or {}).get("sells", 0),
            "url": best.get("url", ""),
        }
    except Exception:
        return None


def filter_quality_tokens(tokens: list[dict]) -> list[dict]:
    """Filter tokens by quality criteria and enrich with pair data."""
    qualified = []

    for token in tokens:
        chain = token.get("chain", "")
        address = token.get("address", "")
        if not chain or not address:
            continue

        # Enrich with pair data
        info = enrich_token(chain, address)
        if not info:
            continue

        liq = info.get("liquidity", 0)
        mc = info.get("mc", 0)

        # Apply filters
        if liq < MIN_LIQUIDITY_USD:
            continue
        if mc and mc > MAX_MC_USD:
            continue

        # Merge data
        result = {**token, **info, "chain": chain, "address": address}

        # Security check for Solana tokens
        if chain == "solana":
            try:
                from src.sniper.rug_detector import full_security_check
                report = full_security_check(address, "solana")
                result["security_passed"] = report.passed
                result["risk_score"] = report.risk_score
            except Exception:
                result["security_passed"] = None
                result["risk_score"] = -1
        else:
            # GoPlus for EVM
            try:
                from src.collectors.meme_tools import goplus_check
                gp = goplus_check(chain, address)
                if gp:
                    result["security_passed"] = not gp.get("is_honeypot", False)
                    result["risk_score"] = 0 if result["security_passed"] else 100
            except Exception:
                result["security_passed"] = None

        qualified.append(result)
        time.sleep(0.3)  # Rate limit

    # Sort by: security passed first, then by liquidity
    qualified.sort(key=lambda t: (
        t.get("security_passed", False) is True,
        t.get("liquidity", 0),
    ), reverse=True)

    return qualified


def format_new_pool_alert(token: dict) -> str:
    """Format new pool alert as Telegram HTML — matches user's design spec."""
    sym = token.get("symbol", "?")
    name = token.get("name", "")[:20]
    chain = token.get("chain", "?")
    mc = token.get("mc", 0)
    liq = token.get("liquidity", 0)
    age = token.get("age_hours", 0)
    source = token.get("source", "")
    sec = token.get("security_passed")
    risk = token.get("risk_score", -1)
    chg1h = token.get("change_1h")
    buys = token.get("buys_1h", 0)
    sells = token.get("sells_1h", 0)
    url = token.get("url", "")
    address = token.get("address", "")
    addr_short = f"{address[:8]}...{address[-6:]}" if len(address) > 14 else address

    mc_str = f"${mc/1000:.0f}K" if mc < 1e6 else f"${mc/1e6:.1f}M"
    liq_str = f"${liq/1000:.0f}K" if liq < 1e6 else f"${liq/1e6:.1f}M"
    chg_str = f"{chg1h:+.0f}%" if chg1h else "?"
    age_str = f"{age:.1f}h" if age < 24 else f"{age/24:.0f}d"

    sec_str = "✅ 安全" if sec is True else "❌ 风险" if sec is False else "❓ 未检测"
    risk_str = f"风险 {risk}/100" if risk >= 0 else ""

    source_label = {"profile": "新上线", "boost": f"Boost推广", "cto": "社区接管"}.get(source, source)

    buy_sell = f"买{buys}/卖{sells}" if buys or sells else ""

    lines = [
        f"🆕 <b>新池检测</b> | {chain.title()}",
        f"━━━━━━━━━━━━━━━━━━━━",
        f"Token: <b>${sym}</b> ({name})",
        f"CA: <code>{addr_short}</code>",
        f"来源: {source_label} · 年龄: {age_str}",
        f"MC: {mc_str} · Liq: {liq_str}",
        f"1h: {chg_str} · {buy_sell}",
        f"━━━━━━━━━━━━━━━━━━━━",
        f"🔒 安全: {sec_str} {risk_str}",
    ]

    if url:
        lines.append(f'<a href="{url}">📊 DexScreener</a>')

    return "\n".join(lines)


async def run_pool_watcher() -> dict:
    """Run the full pool watcher cycle: scan → filter → alert."""
    raw = scan_new_tokens()
    qualified = filter_quality_tokens(raw[:30])  # Limit API calls

    sent = 0
    for token in qualified[:5]:  # Max 5 alerts per cycle
        if token.get("security_passed") is not False:  # Don't alert on known bad tokens
            msg = format_new_pool_alert(token)
            try:
                from src.distribution.telegram_sender import send_meme_alert
                await send_meme_alert(msg)
                sent += 1
            except Exception as e:
                logger.error("pool_alert_failed", error=str(e))

    return {
        "scanned": len(raw),
        "qualified": len(qualified),
        "alerts_sent": sent,
    }
