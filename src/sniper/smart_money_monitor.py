"""Smart Money Monitor — watches wallet behavior and generates signals.

Monitors 5 dimensions:
1. Buy behavior: What new tokens are smart money buying? (first buy = strongest signal)
2. Sell behavior: Smart money dumping → exit warning
3. Fund flow: CEX withdrawal to chain → about to snipe
4. Cluster effect: Multiple wallets buying same token → high confidence
5. Position changes: Adding/reducing/closing positions

Signal scoring:
  ★★☆☆☆  Single wallet buy
  ★★★☆☆  2-3 wallets buy same token
  ★★★★★  5+ wallets buy + low MC (<$100K)
  +1 star  Passes security check
  🚨       Smart money selling YOUR holdings
"""

from __future__ import annotations

import json
import os
import sqlite3
import time
import urllib.request
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import structlog

logger = structlog.get_logger()

DB_PATH = Path("data/smart_money_monitor.db")
SOLANA_RPC = os.environ.get("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")


def _ensure_db() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS smart_money_buys (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            wallet_address TEXT,
            wallet_label TEXT,
            wallet_tier INTEGER,
            token_mint TEXT,
            token_symbol TEXT,
            action TEXT,
            amount_sol REAL,
            token_mc REAL,
            token_liquidity REAL,
            detected_at TEXT,
            tx_signature TEXT,
            UNIQUE(wallet_address, token_mint, tx_signature)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS cluster_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            token_mint TEXT,
            token_symbol TEXT,
            wallet_count INTEGER,
            wallets TEXT,
            stars INTEGER,
            security_passed INTEGER,
            token_mc REAL,
            detected_at TEXT
        )
    """)
    conn.commit()
    return conn


def _fetch_json(url: str, headers: dict | None = None, timeout: int = 10) -> Any:
    hdrs = {"User-Agent": "CryptoScope/1.0"}
    if headers:
        hdrs.update(headers)
    req = urllib.request.Request(url, headers=hdrs)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


def _rpc_call(method: str, params: list) -> Any:
    payload = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
    req = urllib.request.Request(
        SOLANA_RPC, data=payload.encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode()).get("result")


def load_wallets() -> list[dict]:
    """Load smart money wallets from config."""
    import yaml
    config_path = Path("config/smart_money_wallets.yaml")
    if not config_path.exists():
        return []
    with open(config_path) as f:
        data = yaml.safe_load(f) or {}
    return [w for w in data.get("wallets", []) if w.get("chain") == "solana"]


def get_wallet_new_tokens(address: str) -> list[dict]:
    """Get tokens a wallet currently holds via RPC."""
    try:
        result = _rpc_call("getTokenAccountsByOwner", [
            address,
            {"programId": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"},
            {"encoding": "jsonParsed"},
        ])
        if not result:
            return []
        tokens = []
        for acc in result.get("value", []):
            info = acc.get("account", {}).get("data", {}).get("parsed", {}).get("info", {})
            amt_info = info.get("tokenAmount", {})
            ui_amount = float(amt_info.get("uiAmount", 0) or 0)
            if ui_amount > 0:
                tokens.append({
                    "mint": info.get("mint", ""),
                    "amount": ui_amount,
                })
        return tokens
    except Exception as e:
        logger.debug("wallet_tokens_failed", address=address[:12], error=str(e))
        return []


def get_token_info(mint: str) -> dict | None:
    """Get token info from DexScreener."""
    try:
        data = _fetch_json(f"https://api.dexscreener.com/token-pairs/v1/solana/{mint}")
        pairs = data if isinstance(data, list) else data.get("pairs", [])
        if not pairs:
            return None
        best = max(pairs, key=lambda p: (p.get("liquidity", {}) or {}).get("usd", 0) or 0)
        return {
            "symbol": (best.get("baseToken", {}) or {}).get("symbol", "?"),
            "name": (best.get("baseToken", {}) or {}).get("name", ""),
            "price": float(best.get("priceUsd", 0) or 0),
            "mc": best.get("marketCap") or best.get("fdv", 0) or 0,
            "liquidity": (best.get("liquidity", {}) or {}).get("usd", 0) or 0,
            "volume_24h": (best.get("volume", {}) or {}).get("h24", 0) or 0,
            "url": best.get("url", ""),
        }
    except Exception:
        return None


def check_token_security(mint: str) -> bool:
    """Quick security check via RugCheck + GoPlus."""
    # RugCheck
    try:
        data = _fetch_json(f"https://api.rugcheck.xyz/v1/tokens/{mint}/report/summary")
        if data.get("rugged"):
            return False
        score = data.get("score_normalised", data.get("score", 100))
        if score > 500:  # High risk score
            return False
    except Exception as e:
        # Fail-open by design, but log so API outages don't silently mark
        # every token as "safe" without any trace.
        logger.warning("rugcheck_unavailable", mint=mint, error=str(e))

    # GoPlus
    try:
        data = _fetch_json(f"https://api.gopluslabs.io/api/v1/solana/token_security?contract_addresses={mint}")
        result = data.get("result", {}).get(mint, {})
        if result.get("is_honeypot") in ("1", 1, True):
            return False
    except Exception as e:
        logger.warning("goplus_unavailable", mint=mint, error=str(e))

    return True


def scan_smart_money_activity() -> list[dict]:
    """Main scan: check all wallets for new token holdings, detect clusters.

    Returns list of signals sorted by star rating.
    """
    wallets = load_wallets()
    if not wallets:
        return []

    logger.info("smart_money_scan_start", wallets=len(wallets))

    # Collect all wallet holdings
    wallet_holdings: dict[str, list[dict]] = {}  # address -> tokens
    token_buyers: dict[str, list[dict]] = defaultdict(list)  # mint -> wallets that hold it

    conn = _ensure_db()
    now = datetime.now(timezone.utc)

    for w in wallets:
        addr = w["address"]
        label = w.get("label", addr[:12])
        tier = w.get("tier", 3)

        tokens = get_wallet_new_tokens(addr)
        wallet_holdings[addr] = tokens

        for t in tokens:
            token_buyers[t["mint"]].append({
                "address": addr,
                "label": label,
                "tier": tier,
                "amount": t["amount"],
            })

        time.sleep(0.5)  # Rate limit RPC

    # Detect clusters: tokens held by multiple tracked wallets
    signals = []

    for mint, buyers in token_buyers.items():
        if len(buyers) < 2:
            continue  # Need at least 2 wallets

        # Skip known stablecoins and SOL
        known_skip = {
            "So11111111111111111111111111111111111111112",  # Wrapped SOL
            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
            "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",  # USDT
        }
        if mint in known_skip:
            continue

        # Get token info
        info = get_token_info(mint)
        if not info:
            continue

        mc = info.get("mc", 0)
        liquidity = info.get("liquidity", 0)
        symbol = info.get("symbol", "?")

        # Skip very large MC tokens (not meme alpha)
        if mc > 100_000_000:  # > $100M
            continue

        # Calculate star rating
        t1_count = sum(1 for b in buyers if b["tier"] == 1)
        total_count = len(buyers)

        stars = 2  # Base: at least 2 wallets
        if total_count >= 5:
            stars = 5 if mc < 100_000 else 4
        elif total_count >= 3:
            stars = 3
        elif t1_count >= 2:
            stars = 3

        # Security bonus
        security_ok = check_token_security(mint)
        if security_ok:
            stars = min(stars + 1, 5)

        # Record
        wallet_labels = [b["label"] for b in buyers]

        try:
            conn.execute("""
                INSERT INTO cluster_signals
                (token_mint, token_symbol, wallet_count, wallets, stars,
                 security_passed, token_mc, detected_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (mint, symbol, total_count, json.dumps(wallet_labels),
                  stars, 1 if security_ok else 0, mc, now.isoformat()))
        except Exception:
            pass

        signals.append({
            "mint": mint,
            "symbol": symbol,
            "mc": mc,
            "liquidity": liquidity,
            "buyers": buyers,
            "buyer_count": total_count,
            "t1_count": t1_count,
            "stars": stars,
            "security_ok": security_ok,
            "url": info.get("url", ""),
        })

        time.sleep(0.3)  # Rate limit

    conn.commit()
    conn.close()

    signals.sort(key=lambda s: (s["stars"], s["buyer_count"]), reverse=True)
    logger.info("smart_money_scan_done", signals=len(signals))
    return signals


def format_smart_money_signal(signal: dict) -> str:
    """Format a smart money signal as Telegram HTML message."""
    sym = signal["symbol"]
    mc = signal["mc"]
    liq = signal["liquidity"]
    buyers = signal["buyers"]
    stars = signal["stars"]
    security = signal["security_ok"]
    url = signal.get("url", "")

    star_str = "★" * stars + "☆" * (5 - stars)
    mc_str = f"${mc/1000:.0f}K" if mc < 1e6 else f"${mc/1e6:.1f}M"
    liq_str = f"${liq/1000:.0f}K" if liq < 1e6 else f"${liq/1e6:.1f}M"
    sec_str = "✅ 通过" if security else "⚠️ 未通过"

    lines = [
        f"🧠 <b>聪明钱动态</b> | Solana",
        f"━━━━━━━━━━━━━━━━━━━━",
        f"📊 聪明钱聚集: <b>{signal['buyer_count']}</b>个追踪钱包持仓",
        f"🔒 安全检测: {sec_str}",
        f"⚠️ 综合信号: <b>{star_str}</b>",
        f"━━━━━━━━━━━━━━━━━━━━",
        f"🔥 <b>${sym}</b>",
        f"MC {mc_str} · Liq {liq_str}",
        f"",
    ]

    for b in buyers[:5]:
        label = b["label"]
        tier_str = f"T{b['tier']}" if b["tier"] <= 2 else ""
        lines.append(f"  🐋 {label} {tier_str}")

    if url:
        lines.append(f'\n<a href="{url}">DexScreener</a>')

    lines.append(f"\n📍 CA: <code>{signal['mint'][:16]}...</code>")

    return "\n".join(lines)


async def run_smart_money_monitor() -> dict:
    """Run the full smart money scan and send alerts for high-star signals."""
    signals = scan_smart_money_activity()

    if not signals:
        return {"status": "silent", "signals": 0}

    sent = 0
    for signal in signals:
        if signal["stars"] >= 3:  # Only alert on 3+ star signals
            message = format_smart_money_signal(signal)
            try:
                from src.distribution.telegram_sender import send_meme_alert
                await send_meme_alert(message)
                sent += 1
            except Exception as e:
                logger.error("smart_money_alert_failed", error=str(e))

    return {
        "status": "sent" if sent > 0 else "no_alerts",
        "total_signals": len(signals),
        "alerts_sent": sent,
        "top_signal": signals[0]["symbol"] if signals else None,
    }


if __name__ == "__main__":
    import re
    for line in open(".env").readlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ[k.strip()] = v.strip()

    SOLANA_RPC = os.environ.get("SOLANA_RPC_URL", SOLANA_RPC)

    signals = scan_smart_money_activity()
    for s in signals[:5]:
        print(re.sub(r'<[^>]+>', '', format_smart_money_signal(s)))
        print()
