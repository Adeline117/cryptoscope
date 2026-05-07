"""Meme coin smart money aggregator — integrates all打狗工具.

Pulls data from multiple sources simultaneously:
- GMGN: smart money, trending, new pools, sniper detection
- DexScreener: boost detection, trending pairs
- RugCheck: token security scoring (Solana)
- GoPlus: multi-chain security
- Solscan: Solana token holder/transfer data
- Alchemy: Solana RPC (fast)
- Dune: SQL queries on on-chain data
- Debank: whale portfolio tracking
"""

from __future__ import annotations

import json
import os
import time
import urllib.request
from datetime import datetime, timezone
from typing import Any

import structlog

logger = structlog.get_logger()

# API keys from env
ETHERSCAN_KEYS = [
    os.environ.get("ETHERSCAN_API_KEY", ""),
    os.environ.get("ETHERSCAN_API_KEY_2", ""),
    os.environ.get("ETHERSCAN_API_KEY_3", ""),
    os.environ.get("ETHERSCAN_API_KEY_4", ""),
    os.environ.get("ETHERSCAN_API_KEY_5", ""),
    os.environ.get("ETHERSCAN_API_KEY_6", ""),
]
ETHERSCAN_KEYS = [k for k in ETHERSCAN_KEYS if k]
_etherscan_idx = 0

ALCHEMY_KEY = os.environ.get("ALCHEMY_API_KEY", "")
SOLANA_RPC = os.environ.get("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")
DUNE_KEY = os.environ.get("DUNE_API_KEY", "")
SOLSCAN_KEY = os.environ.get("SOLSCAN_API_KEY", "")


def _get_etherscan_key() -> str:
    """Rotate through 6 Etherscan keys to avoid rate limits."""
    global _etherscan_idx
    if not ETHERSCAN_KEYS:
        return ""
    key = ETHERSCAN_KEYS[_etherscan_idx % len(ETHERSCAN_KEYS)]
    _etherscan_idx += 1
    return key


def _fetch(url: str, headers: dict | None = None, timeout: int = 10) -> Any:
    """Fetch JSON from URL."""
    hdrs = {"User-Agent": "CryptoScope/1.0"}
    if headers:
        hdrs.update(headers)
    req = urllib.request.Request(url, headers=hdrs)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


# ---------------------------------------------------------------------------
# 1. GMGN — Smart Money + Trending + New Pools
# Base: gmgn.ai (Cloudflare protected, use known endpoints)
# ---------------------------------------------------------------------------

def gmgn_trending(chain: str = "sol", period: str = "1h") -> list[dict]:
    """Get GMGN trending tokens ranked by smart money activity.

    Note: GMGN uses Cloudflare protection. This may return empty if blocked.
    Workaround: use browser cookie or proxy.

    Args:
        chain: sol, eth, base, bsc
        period: 1m, 5m, 1h, 6h, 24h
    """
    try:
        url = f"https://gmgn.ai/defi/quotation/v1/rank/{chain}/swaps/{period}"
        params = "orderby=smartmoney&direction=desc"
        # Try with browser-like headers to bypass Cloudflare
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://gmgn.ai/",
            "Origin": "https://gmgn.ai",
        }
        req = urllib.request.Request(f"{url}?{params}", headers=headers)
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
        tokens = data.get("data", {}).get("rank", [])
        results = []
        for t in tokens[:20]:
            results.append({
                "source": "gmgn",
                "symbol": t.get("symbol", ""),
                "address": t.get("address", ""),
                "chain": chain,
                "price": t.get("price"),
                "market_cap": t.get("market_cap"),
                "liquidity": t.get("liquidity"),
                "volume": t.get("volume"),
                "holder_count": t.get("holder_count"),
                "smart_buy_24h": t.get("smart_buy_24h", 0),
                "smart_sell_24h": t.get("smart_sell_24h", 0),
                "is_honeypot": t.get("is_honeypot"),
                "renounced": t.get("renounced"),
                "buy_tax": t.get("buy_tax"),
                "sell_tax": t.get("sell_tax"),
                "open_timestamp": t.get("open_timestamp"),
            })
        logger.info("gmgn_trending_fetched", chain=chain, count=len(results))
        return results
    except Exception as e:
        logger.warning("gmgn_trending_failed", error=str(e))
        return []


def gmgn_new_pairs(chain: str = "sol") -> list[dict]:
    """Get newest token pairs from GMGN."""
    try:
        url = f"https://gmgn.ai/defi/quotation/v1/pairs/{chain}/new_pairs"
        data = _fetch(url)
        pairs = data.get("data", {}).get("pairs", [])
        results = []
        for p in pairs[:30]:
            results.append({
                "source": "gmgn_new",
                "symbol": p.get("symbol", ""),
                "address": p.get("base_address", ""),
                "chain": chain,
                "liquidity": p.get("liquidity"),
                "market_cap": p.get("market_cap"),
                "open_timestamp": p.get("open_timestamp"),
            })
        return results
    except Exception as e:
        logger.warning("gmgn_new_pairs_failed", error=str(e))
        return []


# ---------------------------------------------------------------------------
# 2. DexScreener — Boosts + Trending + Search (free, 300rpm)
# ---------------------------------------------------------------------------

def dexscreener_top_boosts() -> list[dict]:
    """Get most boosted tokens (teams paying for promotion)."""
    try:
        data = _fetch("https://api.dexscreener.com/token-boosts/top/v1")
        if not isinstance(data, list):
            return []
        results = []
        for t in data[:30]:
            results.append({
                "source": "dexscreener_boost",
                "chain": t.get("chainId", ""),
                "address": t.get("tokenAddress", ""),
                "boost_amount": t.get("totalAmount", 0),
                "description": t.get("description", ""),
                "url": t.get("url", ""),
            })
        return results
    except Exception as e:
        logger.warning("dexscreener_boosts_failed", error=str(e))
        return []


def dexscreener_token_data(chain: str, address: str) -> dict | None:
    """Get detailed pair data for a specific token."""
    try:
        data = _fetch(f"https://api.dexscreener.com/token-pairs/v1/{chain}/{address}")
        pairs = data if isinstance(data, list) else data.get("pairs", [])
        if not pairs:
            return None
        # Return highest liquidity pair
        best = max(pairs, key=lambda p: (p.get("liquidity", {}) or {}).get("usd", 0) or 0)
        return {
            "symbol": (best.get("baseToken", {}) or {}).get("symbol", ""),
            "name": (best.get("baseToken", {}) or {}).get("name", ""),
            "price": best.get("priceUsd"),
            "market_cap": best.get("marketCap") or best.get("fdv"),
            "liquidity": (best.get("liquidity", {}) or {}).get("usd", 0),
            "volume_24h": (best.get("volume", {}) or {}).get("h24", 0),
            "change_5m": (best.get("priceChange", {}) or {}).get("m5"),
            "change_1h": (best.get("priceChange", {}) or {}).get("h1"),
            "change_24h": (best.get("priceChange", {}) or {}).get("h24"),
            "pair_created": best.get("pairCreatedAt"),
            "url": best.get("url", ""),
            "txns_buys_1h": (best.get("txns", {}).get("h1", {}) or {}).get("buys", 0),
            "txns_sells_1h": (best.get("txns", {}).get("h1", {}) or {}).get("sells", 0),
        }
    except Exception as e:
        logger.debug("dexscreener_token_failed", error=str(e))
        return None


# ---------------------------------------------------------------------------
# 3. RugCheck — Solana token security (free API)
# ---------------------------------------------------------------------------

def rugcheck_token(mint: str) -> dict | None:
    """Get RugCheck security report for a Solana token."""
    try:
        data = _fetch(f"https://api.rugcheck.xyz/v1/tokens/{mint}/report/summary")
        return {
            "source": "rugcheck",
            "score": data.get("score", 0),
            "score_normalized": data.get("score_normalised", 0),
            "risks": [
                {"name": r.get("name", ""), "level": r.get("level", ""), "description": r.get("description", "")}
                for r in data.get("risks", [])
            ],
            "rugged": data.get("rugged", False),
            "mint_authority": data.get("mintAuthority"),
            "freeze_authority": data.get("freezeAuthority"),
            "lp_locked_pct": data.get("lpLockedPct", 0),
            "top_holders": [
                {"address": h.get("address", ""), "pct": h.get("pct", 0), "insider": h.get("insider", False)}
                for h in (data.get("topHolders", []) or [])[:10]
            ],
        }
    except Exception as e:
        logger.debug("rugcheck_failed", mint=mint, error=str(e))
        return None


# ---------------------------------------------------------------------------
# 4. GoPlus — Multi-chain security (free, rate limited)
# ---------------------------------------------------------------------------

def goplus_check(chain_id: str, address: str) -> dict | None:
    """Quick GoPlus security check."""
    try:
        if chain_id == "solana":
            url = f"https://api.gopluslabs.io/api/v1/solana/token_security?contract_addresses={address}"
        else:
            chain_map = {"ethereum": 1, "bsc": 56, "base": 8453}
            cid = chain_map.get(chain_id, chain_id)
            url = f"https://api.gopluslabs.io/api/v1/token_security/{cid}?contract_addresses={address.lower()}"

        data = _fetch(url)
        result = data.get("result", {})
        token_data = result.get(address.lower(), result.get(address, {}))
        if not token_data:
            return None

        return {
            "source": "goplus",
            "is_honeypot": token_data.get("is_honeypot") in ("1", 1, True),
            "is_mintable": token_data.get("is_mintable") in ("1", 1, True),
            "can_take_back_ownership": token_data.get("can_take_back_ownership") in ("1", 1, True),
            "owner_change_balance": token_data.get("owner_change_balance") in ("1", 1, True),
            "hidden_owner": token_data.get("hidden_owner") in ("1", 1, True),
            "is_proxy": token_data.get("is_proxy") in ("1", 1, True),
            "buy_tax": token_data.get("buy_tax"),
            "sell_tax": token_data.get("sell_tax"),
            "holder_count": token_data.get("holder_count"),
        }
    except Exception as e:
        logger.debug("goplus_check_failed", error=str(e))
        return None


# ---------------------------------------------------------------------------
# 5. Debank — Whale portfolio (free API, limited)
# ---------------------------------------------------------------------------

def debank_wallet_tokens(address: str) -> list[dict]:
    """Get token holdings for a wallet via Debank."""
    try:
        url = f"https://api.debank.com/token/balance_list?user_addr={address}&chain=sol"
        data = _fetch(url)
        if not isinstance(data, list):
            return []
        return [
            {
                "symbol": t.get("symbol", ""),
                "amount": t.get("amount", 0),
                "price": t.get("price", 0),
                "usd_value": t.get("amount", 0) * t.get("price", 0),
            }
            for t in data[:20]
        ]
    except Exception as e:
        logger.debug("debank_failed", error=str(e))
        return []


# ---------------------------------------------------------------------------
# Master scanner — runs all tools and aggregates
# ---------------------------------------------------------------------------

def scan_all_sources(chain: str = "sol") -> dict:
    """Run all free meme tools and return aggregated intelligence.

    Returns:
        {
            "trending_smart_money": [...],  # GMGN smart money ranked
            "new_pairs": [...],             # GMGN new pairs
            "boosted_tokens": [...],        # DexScreener boosts
            "timestamp": "...",
        }
    """
    logger.info("meme_tools_scan_started", chain=chain)

    results = {
        "trending_smart_money": [],
        "new_pairs": [],
        "boosted_tokens": [],
        "dbot_hot": [],
        "dbot_meme_live": [],
        "ave_trending": [],
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "chain": chain,
    }

    # GMGN trending by smart money
    results["trending_smart_money"] = gmgn_trending(chain, "1h")

    # GMGN new pairs
    results["new_pairs"] = gmgn_new_pairs(chain)

    # DexScreener boosts
    results["boosted_tokens"] = dexscreener_top_boosts()

    # DBot hot pairs + live pump.fun
    results["dbot_hot"] = dbot_hot_pairs(chain)
    results["dbot_meme_live"] = dbot_pump_live(chain)

    # Ave.ai trending meme
    results["ave_trending"] = ave_trending(chain, "meme")

    total = sum(len(v) for v in results.values() if isinstance(v, list))
    logger.info("meme_tools_scan_done", total_items=total)

    return results


def analyze_token(chain: str, address: str) -> dict:
    """Deep analysis of a specific token using all available tools.

    Returns combined data from DexScreener + RugCheck + GoPlus.
    """
    result = {"chain": chain, "address": address}

    # DexScreener pair data
    dex_data = dexscreener_token_data(chain, address)
    if dex_data:
        result.update(dex_data)

    # Security checks
    if chain in ("sol", "solana"):
        rc = rugcheck_token(address)
        if rc:
            result["rugcheck"] = rc

    gp = goplus_check("solana" if chain in ("sol", "solana") else chain, address)
    if gp:
        result["goplus"] = gp

    return result


def format_scan_report(scan_data: dict) -> str:
    """Format scan results as Telegram HTML message."""
    lines = []
    lines.append(f"🔍 <b>打狗扫描报告</b>")
    lines.append(f"⏰ {scan_data.get('timestamp', '')[:19]}")
    lines.append("")

    # Smart money trending
    sm = scan_data.get("trending_smart_money", [])
    if sm:
        lines.append("━━ <b>聪明钱在买</b> (GMGN) ━━━")
        for i, t in enumerate(sm[:5], 1):
            sym = t.get("symbol", "?")
            mc = t.get("market_cap")
            sb = t.get("smart_buy_24h", 0)
            ss = t.get("smart_sell_24h", 0)
            net = sb - ss if sb and ss else 0
            mc_str = f"${mc/1000:.0f}K" if mc and mc < 1e6 else f"${mc/1e6:.1f}M" if mc else "?"
            net_emoji = "🟢" if net > 0 else "🔴" if net < 0 else "⚪"
            lines.append(f"  {i}. {net_emoji} ${sym} · MC {mc_str} · 聪明钱净买 {net:+d}")
        lines.append("")

    # Boosted tokens
    boosts = scan_data.get("boosted_tokens", [])
    sol_boosts = [b for b in boosts if b.get("chain") == "solana"]
    if sol_boosts:
        lines.append("━━ <b>Boost推广中</b> (DexScreener) ━━━")
        for b in sol_boosts[:5]:
            amt = b.get("boost_amount", 0)
            desc = b.get("description", "")[:30]
            addr = b.get("address", "")[:8]
            lines.append(f"  Boost:{amt} · {desc}... · {addr}")
        lines.append("")

    if not sm and not sol_boosts:
        lines.append("暂无数据")

    return "\n".join(lines)


def format_token_report(data: dict) -> str:
    """Format single token analysis as Telegram HTML."""
    sym = data.get("symbol", "?")
    name = data.get("name", "")
    price = data.get("price", "?")
    mc = data.get("market_cap")
    liq = data.get("liquidity")
    chg1h = data.get("change_1h")
    url = data.get("url", "")

    mc_str = f"${mc/1000:.0f}K" if mc and mc < 1e6 else f"${mc/1e6:.1f}M" if mc else "?"
    liq_str = f"${liq/1000:.0f}K" if liq and liq < 1e6 else f"${liq/1e6:.1f}M" if liq else "?"
    chg_str = f"{chg1h:+.0f}%" if chg1h else "?"

    lines = [f"🔍 <b>${sym}</b> ({name})", f"💰 MC {mc_str} · Liq {liq_str} · 1h {chg_str}", ""]

    # RugCheck
    rc = data.get("rugcheck")
    if rc:
        score = rc.get("score_normalized", rc.get("score", 0))
        rugged = rc.get("rugged", False)
        lp_locked = rc.get("lp_locked_pct", 0)
        risks = rc.get("risks", [])

        status = "❌ RUGGED" if rugged else f"评分 {score}/100"
        lines.append(f"🛡️ RugCheck: {status} · LP锁定 {lp_locked:.0f}%")
        for r in risks[:3]:
            level_emoji = "🔴" if r["level"] == "danger" else "🟡" if r["level"] == "warn" else "⚪"
            lines.append(f"  {level_emoji} {r['name']}: {r['description'][:50]}")

    # GoPlus
    gp = data.get("goplus")
    if gp:
        checks = []
        checks.append("✅" if not gp.get("is_honeypot") else "❌蜜罐")
        checks.append("✅" if not gp.get("is_mintable") else "❌增发")
        checks.append("✅" if not gp.get("is_proxy") else "⚠️代理")
        checks.append("✅" if not gp.get("hidden_owner") else "❌隐藏owner")
        lines.append(f"🔒 GoPlus: {' '.join(checks)}")
        if gp.get("buy_tax"):
            lines.append(f"  买税 {gp['buy_tax']} · 卖税 {gp.get('sell_tax', '?')}")

    # Top holders from RugCheck
    if rc and rc.get("top_holders"):
        insiders = [h for h in rc["top_holders"] if h.get("insider")]
        lines.append(f"👥 Top10持仓: {sum(h['pct'] for h in rc['top_holders'][:10]):.1f}% · 内部人: {len(insiders)}")

    if url:
        lines.append(f'\n<a href="{url}">DexScreener图表</a>')

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# 6. Ave.ai — Multi-chain token data + security + holders (free key)
# Base: https://prod.ave-api.com/v2
# Auth: X-API-KEY header (register at cloud.ave.ai)
# ---------------------------------------------------------------------------

AVE_KEY = os.environ.get("AVE_API_KEY", "")

def ave_token_security(address: str, chain: str = "solana") -> dict | None:
    """Get token security report from ave.ai — honeypot, taxes, holder distribution."""
    if not AVE_KEY:
        return None
    try:
        token_id = f"{address}-{chain}"
        data = _fetch(
            f"https://prod.ave-api.com/v2/contracts/{token_id}",
            headers={"X-API-KEY": AVE_KEY},
        )
        return data.get("data", data)
    except Exception as e:
        logger.debug("ave_security_failed", error=str(e))
        return None


def ave_trending(chain: str = "solana", topic: str = "hot") -> list[dict]:
    """Get trending tokens from ave.ai. Topics: hot, meme, gainers, losers, new."""
    if not AVE_KEY:
        return []
    try:
        data = _fetch(
            f"https://prod.ave-api.com/v2/ranks?topic={topic}&chain={chain}&offset=0&limit=20",
            headers={"X-API-KEY": AVE_KEY},
        )
        return data.get("data", [])
    except Exception as e:
        logger.debug("ave_trending_failed", error=str(e))
        return []


def ave_top_holders(address: str, chain: str = "solana") -> list[dict]:
    """Get top 100 holders with PnL from ave.ai."""
    if not AVE_KEY:
        return []
    try:
        token_id = f"{address}-{chain}"
        data = _fetch(
            f"https://prod.ave-api.com/v2/tokens/top100/{token_id}",
            headers={"X-API-KEY": AVE_KEY},
        )
        return data.get("data", [])
    except Exception as e:
        logger.debug("ave_holders_failed", error=str(e))
        return []


# ---------------------------------------------------------------------------
# 7. DBot/Dbotx — Wallet PnL, copy trade, meme data (free 200K credits/month)
# Base: https://api-data-v1.dbotx.com
# Auth: X-API-KEY header (register at dbotx.com/dashboard)
# ---------------------------------------------------------------------------

DBOT_KEY = os.environ.get("DBOT_API_KEY", "")

def dbot_wallet_pnl(address: str, chain: str = "solana") -> dict | None:
    """Get wallet holdings + PnL from DBot."""
    if not DBOT_KEY:
        return None
    try:
        data = _fetch(
            f"https://api-data-v1.dbotx.com/kline/wallet/holdings?chain={chain}&wallet={address}",
            headers={"X-API-KEY": DBOT_KEY},
        )
        if data.get("err"):
            return None
        return data.get("res")
    except Exception as e:
        logger.debug("dbot_wallet_failed", error=str(e))
        return None


def dbot_wallet_stats(address: str, chain: str = "solana") -> dict | None:
    """Get wallet trading statistics from DBot — win rate, total PnL."""
    if not DBOT_KEY:
        return None
    try:
        data = _fetch(
            f"https://api-data-v1.dbotx.com/kline/wallet/statistics?chain={chain}&wallet={address}",
            headers={"X-API-KEY": DBOT_KEY},
        )
        if data.get("err"):
            return None
        return data.get("res")
    except Exception as e:
        logger.debug("dbot_stats_failed", error=str(e))
        return None


def dbot_hot_pairs(chain: str = "solana") -> list[dict]:
    """Get hot/trending pairs from DBot."""
    if not DBOT_KEY:
        return []
    try:
        data = _fetch(
            f"https://api-data-v1.dbotx.com/kline/hot?chain={chain}",
            headers={"X-API-KEY": DBOT_KEY},
        )
        if data.get("err"):
            return []
        return data.get("res", [])
    except Exception as e:
        logger.debug("dbot_hot_failed", error=str(e))
        return []


def dbot_new_pairs(chain: str = "solana") -> list[dict]:
    """Get newly created pairs from DBot."""
    if not DBOT_KEY:
        return []
    try:
        data = _fetch(
            f"https://api-data-v1.dbotx.com/kline/new?chain={chain}",
            headers={"X-API-KEY": DBOT_KEY},
        )
        if data.get("err"):
            return []
        return data.get("res", [])
    except Exception as e:
        logger.debug("dbot_new_failed", error=str(e))
        return []


def dbot_meme_tokens(chain: str = "solana", status: str = "live") -> list[dict]:
    """Get meme tokens from DBot. Status: live, graduated, dead."""
    if not DBOT_KEY:
        return []
    try:
        data = _fetch(
            f"https://api-data-v1.dbotx.com/kline/meme?chain={chain}&status={status}",
            headers={"X-API-KEY": DBOT_KEY},
        )
        if data.get("err"):
            return []
        return data.get("res", [])
    except Exception as e:
        logger.debug("dbot_meme_failed", error=str(e))
        return []


def dbot_token_holders(address: str, chain: str = "solana") -> list[dict]:
    """Get top 100 holders from DBot."""
    if not DBOT_KEY:
        return []
    try:
        data = _fetch(
            f"https://api-data-v1.dbotx.com/kline/holders?chain={chain}&token={address}",
            headers={"X-API-KEY": DBOT_KEY},
        )
        if data.get("err"):
            return []
        return data.get("res", [])
    except Exception as e:
        logger.debug("dbot_holders_failed", error=str(e))
        return []


def dbot_pump_live(chain: str = "solana") -> list[dict]:
    """Get live streaming Pump.fun tokens from DBot."""
    if not DBOT_KEY:
        return []
    try:
        data = _fetch(
            f"https://api-data-v1.dbotx.com/kline/pump_live?chain={chain}",
            headers={"X-API-KEY": DBOT_KEY},
        )
        if data.get("err"):
            return []
        return data.get("res", [])
    except Exception as e:
        logger.debug("dbot_pump_live_failed", error=str(e))
        return []


# ---------------------------------------------------------------------------
# 8. Solscan — Solana token holders and transfers (API key required)
# ---------------------------------------------------------------------------

def solscan_token_holders(mint: str, limit: int = 20) -> list[dict]:
    """Get top token holders from Solscan Pro API."""
    if not SOLSCAN_KEY:
        return []
    try:
        url = f"https://pro-api.solscan.io/v2.0/token/holders?address={mint}&page=1&page_size={limit}"
        data = _fetch(url, headers={"token": SOLSCAN_KEY})
        holders = data.get("data", {}).get("items", []) if isinstance(data.get("data"), dict) else data.get("data", [])
        return [
            {
                "address": h.get("address", h.get("owner", "")),
                "amount": h.get("amount", 0),
                "decimals": h.get("decimals", 0),
                "rank": h.get("rank", i + 1),
            }
            for i, h in enumerate(holders[:limit])
        ]
    except Exception as e:
        logger.debug("solscan_holders_failed", mint=mint, error=str(e))
        return []


def solscan_token_meta(mint: str) -> dict | None:
    """Get token metadata from Solscan."""
    if not SOLSCAN_KEY:
        return None
    try:
        url = f"https://pro-api.solscan.io/v2.0/token/meta?address={mint}"
        data = _fetch(url, headers={"token": SOLSCAN_KEY})
        d = data.get("data", {})
        return {
            "symbol": d.get("symbol", ""),
            "name": d.get("name", ""),
            "supply": d.get("supply", 0),
            "decimals": d.get("decimals", 0),
            "holder_count": d.get("holder", 0),
            "price": d.get("price", 0),
            "market_cap": d.get("market_cap", 0),
        }
    except Exception as e:
        logger.debug("solscan_meta_failed", error=str(e))
        return None


def solscan_token_transfers(mint: str, limit: int = 20) -> list[dict]:
    """Get recent token transfers from Solscan."""
    if not SOLSCAN_KEY:
        return []
    try:
        url = f"https://pro-api.solscan.io/v2.0/token/transfer?address={mint}&page=1&page_size={limit}"
        data = _fetch(url, headers={"token": SOLSCAN_KEY})
        items = data.get("data", {}).get("items", []) if isinstance(data.get("data"), dict) else data.get("data", [])
        return [
            {
                "from": t.get("from_address", ""),
                "to": t.get("to_address", ""),
                "amount": t.get("amount", 0),
                "time": t.get("block_time", 0),
                "tx": t.get("trans_id", t.get("signature", "")),
            }
            for t in items[:limit]
        ]
    except Exception as e:
        logger.debug("solscan_transfers_failed", error=str(e))
        return []


# ---------------------------------------------------------------------------
# 7. Alchemy Solana RPC — Fast transaction fetching
# ---------------------------------------------------------------------------

def alchemy_get_signatures(address: str, limit: int = 20) -> list[dict]:
    """Get recent transaction signatures for a wallet via Alchemy RPC."""
    try:
        payload = json.dumps({
            "jsonrpc": "2.0", "id": 1,
            "method": "getSignaturesForAddress",
            "params": [address, {"limit": limit}],
        })
        req = urllib.request.Request(
            SOLANA_RPC,
            data=payload.encode(),
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read().decode())
        return result.get("result", [])
    except Exception as e:
        logger.debug("alchemy_signatures_failed", address=address[:12], error=str(e))
        return []


def alchemy_get_token_accounts(address: str) -> list[dict]:
    """Get all SPL token accounts for a wallet."""
    try:
        payload = json.dumps({
            "jsonrpc": "2.0", "id": 1,
            "method": "getTokenAccountsByOwner",
            "params": [
                address,
                {"programId": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"},
                {"encoding": "jsonParsed"},
            ],
        })
        req = urllib.request.Request(
            SOLANA_RPC,
            data=payload.encode(),
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read().decode())
        accounts = result.get("result", {}).get("value", [])
        tokens = []
        for acc in accounts:
            info = acc.get("account", {}).get("data", {}).get("parsed", {}).get("info", {})
            amount_info = info.get("tokenAmount", {})
            if float(amount_info.get("uiAmount", 0) or 0) > 0:
                tokens.append({
                    "mint": info.get("mint", ""),
                    "amount": float(amount_info.get("uiAmount", 0)),
                    "decimals": amount_info.get("decimals", 0),
                })
        return tokens
    except Exception as e:
        logger.debug("alchemy_token_accounts_failed", error=str(e))
        return []


# ---------------------------------------------------------------------------
# 8. Etherscan V2 — EVM smart money with key rotation
# ---------------------------------------------------------------------------

def etherscan_token_txs(address: str, chain_id: int = 1, limit: int = 30) -> list[dict]:
    """Get recent token transactions with Etherscan key rotation."""
    key = _get_etherscan_key()
    if not key:
        return []
    try:
        url = (
            f"https://api.etherscan.io/v2/api?chainid={chain_id}"
            f"&module=account&action=tokentx&address={address}"
            f"&startblock=0&endblock=99999999&sort=desc&offset={limit}"
            f"&apikey={key}"
        )
        data = _fetch(url)
        result = data.get("result", [])
        return result if isinstance(result, list) else []
    except Exception as e:
        logger.debug("etherscan_failed", address=address[:12], error=str(e))
        return []


# ---------------------------------------------------------------------------
# 9. Dune Analytics — SQL queries on on-chain data
# ---------------------------------------------------------------------------

def dune_execute_query(query_id: int) -> dict | None:
    """Execute a pre-saved Dune query and get results."""
    if not DUNE_KEY:
        return None
    try:
        # Execute query
        exec_url = f"https://api.dune.com/api/v1/query/{query_id}/execute"
        req = urllib.request.Request(
            exec_url,
            method="POST",
            headers={"X-Dune-API-Key": DUNE_KEY, "Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            exec_data = json.loads(resp.read().decode())

        execution_id = exec_data.get("execution_id")
        if not execution_id:
            return None

        # Poll for results (max 30 seconds)
        for _ in range(6):
            time.sleep(5)
            status_url = f"https://api.dune.com/api/v1/execution/{execution_id}/results"
            req2 = urllib.request.Request(
                status_url,
                headers={"X-Dune-API-Key": DUNE_KEY},
            )
            with urllib.request.urlopen(req2, timeout=10) as resp2:
                result = json.loads(resp2.read().decode())
            if result.get("state") == "QUERY_STATE_COMPLETED":
                return result.get("result", {})

        return None
    except Exception as e:
        logger.debug("dune_query_failed", query_id=query_id, error=str(e))
        return None


# ---------------------------------------------------------------------------
# Enhanced analyze_token with all tools
# ---------------------------------------------------------------------------

def analyze_token_deep(chain: str, address: str) -> dict:
    """Deep analysis using ALL available tools: DexScreener + RugCheck + GoPlus + Solscan."""
    result = analyze_token(chain, address)

    # Add Solscan data if Solana
    if chain in ("sol", "solana") and SOLSCAN_KEY:
        meta = solscan_token_meta(address)
        if meta:
            result["solscan_meta"] = meta
            result["holder_count"] = meta.get("holder_count", result.get("holder_count"))

        holders = solscan_token_holders(address, 10)
        if holders:
            result["top_holders"] = holders
            total_top10 = sum(h.get("amount", 0) for h in holders[:10])
            result["top10_concentration"] = total_top10

        transfers = solscan_token_transfers(address, 10)
        if transfers:
            result["recent_transfers"] = len(transfers)

    return result


if __name__ == "__main__":
    import re

    # Load env
    from pathlib import Path
    env_path = Path(".env")
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ[k.strip()] = v.strip()

    # Test: scan all sources
    print("=== 扫描聪明钱 ===")
    data = scan_all_sources("sol")
    report = format_scan_report(data)
    print(re.sub(r'<[^>]+>', '', report))
