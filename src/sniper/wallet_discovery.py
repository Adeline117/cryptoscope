"""Wallet Discovery — automatically finds profitable wallets to track.

Methods:
1. GMGN top traders: scrape top 100 traders for trending tokens
2. DexScreener top holders: find early buyers of tokens that did 10x+
3. Cross-reference: wallets that appear in multiple token top-holder lists = consistent alpha

Runs periodically to grow the watchlist from 41 → 200+ wallets.
"""

from __future__ import annotations

import json
import os
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml
import structlog

logger = structlog.get_logger()

WALLETS_CONFIG = Path("config/smart_money_wallets.yaml")


def _fetch(url: str, headers: dict | None = None, timeout: int = 12) -> Any:
    hdrs = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Accept": "application/json", "Referer": "https://gmgn.ai/"}
    if headers:
        hdrs.update(headers)
    req = urllib.request.Request(url, headers=hdrs)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


def discover_from_gmgn_trending(chain: str = "sol", limit: int = 100) -> list[dict]:
    """Get top traders from GMGN trending tokens.

    GMGN returns tokens with their trading data. We can identify
    wallets that consistently appear as top buyers.
    """
    discovered = []

    # Get trending tokens across multiple timeframes
    for period in ["1h", "6h", "24h"]:
        try:
            url = f"https://gmgn.ai/defi/quotation/v1/rank/{chain}/swaps/{period}"
            params = "orderby=volume&direction=desc"
            data = _fetch(f"{url}?{params}")
            tokens = data.get("data", {}).get("rank", [])

            for t in tokens[:20]:
                addr = t.get("address", "")
                sym = t.get("symbol", "?")
                mc = t.get("market_cap", 0) or 0
                sb = t.get("smart_buy_24h", 0) or 0
                ss = t.get("smart_sell_24h", 0) or 0

                # Only tokens with smart money activity
                if sb > 0:
                    discovered.append({
                        "source": f"gmgn_trending_{period}",
                        "token": sym,
                        "token_address": addr,
                        "smart_buys": sb,
                        "smart_sells": ss,
                        "mc": mc,
                    })

            time.sleep(1)  # Rate limit
        except Exception as e:
            logger.debug("gmgn_trending_failed", period=period, error=str(e))

    return discovered


def discover_top_holders(token_address: str, chain: str = "sol") -> list[dict]:
    """Get top holders of a specific token via GMGN."""
    try:
        url = f"https://gmgn.ai/defi/quotation/v1/tokens/top_buyers/{chain}/{token_address}"
        data = _fetch(url)
        buyers = data.get("data", [])
        if not isinstance(buyers, list):
            buyers = data.get("data", {}).get("holders", [])

        results = []
        for b in (buyers if isinstance(buyers, list) else [])[:50]:
            addr = b.get("address", b.get("wallet_address", ""))
            if addr and len(addr) > 20:
                results.append({
                    "address": addr,
                    "pnl": b.get("realized_profit", b.get("profit", 0)),
                    "unrealized": b.get("unrealized_profit", 0),
                    "buy_amount": b.get("buy_amount_cur", b.get("total_cost", 0)),
                })
        return results
    except Exception as e:
        logger.debug("top_holders_failed", token=token_address[:12], error=str(e))
        return []


def discover_wallets_from_hot_tokens(chain: str = "sol", max_tokens: int = 10) -> dict[str, dict]:
    """Main discovery: find wallets that appear as top buyers across multiple hot tokens.

    A wallet that's a top buyer in 3+ different trending tokens = likely smart money.
    """
    logger.info("wallet_discovery_started")

    # Step 1: Get trending tokens
    trending = discover_from_gmgn_trending(chain)
    token_addresses = list({t["token_address"] for t in trending if t["token_address"]})[:max_tokens]

    # Step 2: For each token, get top buyers
    wallet_appearances: dict[str, dict] = {}  # address -> {count, total_pnl, tokens}

    for token_addr in token_addresses:
        holders = discover_top_holders(token_addr, chain)
        token_sym = next((t["token"] for t in trending if t["token_address"] == token_addr), "?")

        for h in holders:
            addr = h["address"]
            if addr not in wallet_appearances:
                wallet_appearances[addr] = {
                    "address": addr,
                    "appearances": 0,
                    "tokens": [],
                    "total_pnl": 0,
                }
            wallet_appearances[addr]["appearances"] += 1
            wallet_appearances[addr]["tokens"].append(token_sym)
            wallet_appearances[addr]["total_pnl"] += float(h.get("pnl", 0) or 0)

        time.sleep(1.5)  # Rate limit

    # Step 3: Filter — wallets appearing in 2+ token top-holder lists
    smart_wallets = {
        addr: info for addr, info in wallet_appearances.items()
        if info["appearances"] >= 2
    }

    logger.info("wallet_discovery_done",
                tokens_checked=len(token_addresses),
                wallets_found=len(wallet_appearances),
                smart_wallets=len(smart_wallets))

    return smart_wallets


def add_discovered_wallets(
    discovered: dict[str, dict],
    min_appearances: int = 2,
    max_add: int = 50,
) -> int:
    """Add discovered wallets to the config file.

    Only adds wallets not already tracked. Returns count added.
    """
    # Load existing
    if WALLETS_CONFIG.exists():
        with open(WALLETS_CONFIG) as f:
            data = yaml.safe_load(f) or {}
    else:
        data = {"wallets": []}

    existing_addrs = {w["address"] for w in data.get("wallets", [])}

    # Filter and sort by appearances (quality signal)
    candidates = [
        info for addr, info in discovered.items()
        if info["appearances"] >= min_appearances
        and info["address"] not in existing_addrs
        and len(info["address"]) > 20  # Valid address
    ]
    candidates.sort(key=lambda x: (x["appearances"], x["total_pnl"]), reverse=True)

    added = 0
    for c in candidates[:max_add]:
        token_list = ", ".join(c["tokens"][:3])
        data["wallets"].append({
            "address": c["address"],
            "label": f"Auto #{len(data['wallets'])+1} ({c['appearances']}x in {token_list})",
            "chain": "solana",
            "tier": 2 if c["appearances"] >= 3 else 3,
            "notes": f"Auto-discovered: {c['appearances']} token overlaps, PnL ${c['total_pnl']:,.0f}",
        })
        added += 1

    if added > 0:
        with open(WALLETS_CONFIG, "w") as f:
            yaml.dump(data, f, allow_unicode=True, default_flow_style=False)
        logger.info("wallets_added", count=added, total=len(data["wallets"]))

    return added


async def run_wallet_discovery() -> dict:
    """Full discovery pipeline: find → filter → add to watchlist."""
    discovered = discover_wallets_from_hot_tokens("sol", max_tokens=8)
    added = add_discovered_wallets(discovered, min_appearances=2, max_add=30)

    # Count total
    with open(WALLETS_CONFIG) as f:
        total = len(yaml.safe_load(f).get("wallets", []))

    result = {
        "discovered": len(discovered),
        "added": added,
        "total_wallets": total,
    }

    if added > 0:
        try:
            from src.distribution.telegram_sender import send_meme_alert
            await send_meme_alert(
                f"🔍 <b>钱包自动发现</b>\n\n"
                f"发现 {len(discovered)} 个潜在聪明钱\n"
                f"新增 {added} 个到追踪列表\n"
                f"总追踪: {total} 个钱包"
            )
        except Exception:
            pass

    return result


if __name__ == "__main__":
    import re
    for line in open(".env").readlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ[k.strip()] = v.strip()

    print("🔍 自动发现聪明钱钱包...")
    discovered = discover_wallets_from_hot_tokens("sol", max_tokens=5)
    print(f"\n发现 {len(discovered)} 个出现在2+热门代币的钱包:")
    for addr, info in sorted(discovered.items(), key=lambda x: x[1]["appearances"], reverse=True)[:10]:
        print(f"  {addr[:16]}... | {info['appearances']}个代币 | PnL ${info['total_pnl']:,.0f} | {', '.join(info['tokens'][:3])}")
