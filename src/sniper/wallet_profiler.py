"""Wallet Profiler — builds win rate, PnL, and trading history for tracked wallets.

For each smart money wallet, maintains:
- Total trades, win rate, avg ROI
- Recent token buys and sells
- Current holdings with P&L
- Historical accuracy (did their buys go up?)

Uses: Alchemy Solana RPC + DexScreener for price data.
"""

from __future__ import annotations

import json
import os
import sqlite3
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import structlog

logger = structlog.get_logger()

DB_PATH = Path("data/wallet_profiles.db")
SOLANA_RPC = os.environ.get("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")


def _ensure_db() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS wallet_profiles (
            address TEXT PRIMARY KEY,
            label TEXT,
            chain TEXT DEFAULT 'solana',
            tier INTEGER DEFAULT 3,
            total_trades INTEGER DEFAULT 0,
            wins INTEGER DEFAULT 0,
            losses INTEGER DEFAULT 0,
            win_rate REAL DEFAULT 0,
            total_pnl_usd REAL DEFAULT 0,
            avg_roi_pct REAL DEFAULT 0,
            last_active TEXT,
            updated_at TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS wallet_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            wallet_address TEXT,
            token_mint TEXT,
            token_symbol TEXT,
            action TEXT,
            amount_sol REAL,
            price_at_trade REAL,
            price_now REAL,
            pnl_pct REAL,
            tx_signature TEXT,
            trade_time TEXT,
            checked INTEGER DEFAULT 0
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS wallet_holdings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            wallet_address TEXT,
            token_mint TEXT,
            token_symbol TEXT,
            amount REAL,
            entry_price_est REAL,
            current_price REAL,
            pnl_pct REAL,
            updated_at TEXT,
            UNIQUE(wallet_address, token_mint)
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
    """Make a Solana RPC call."""
    payload = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
    req = urllib.request.Request(
        SOLANA_RPC,
        data=payload.encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode()).get("result")


def get_wallet_token_holdings(address: str) -> list[dict]:
    """Get all SPL token accounts for a wallet via Solana RPC."""
    try:
        result = _rpc_call("getTokenAccountsByOwner", [
            address,
            {"programId": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"},
            {"encoding": "jsonParsed"},
        ])
        if not result:
            return []

        holdings = []
        for acc in result.get("value", []):
            info = acc.get("account", {}).get("data", {}).get("parsed", {}).get("info", {})
            amount_info = info.get("tokenAmount", {})
            ui_amount = float(amount_info.get("uiAmount", 0) or 0)
            if ui_amount > 0:
                holdings.append({
                    "mint": info.get("mint", ""),
                    "amount": ui_amount,
                    "decimals": amount_info.get("decimals", 0),
                })
        return holdings
    except Exception as e:
        logger.debug("holdings_fetch_failed", address=address[:12], error=str(e))
        return []


def get_wallet_recent_txs(address: str, limit: int = 20) -> list[dict]:
    """Get recent transaction signatures for a wallet."""
    try:
        result = _rpc_call("getSignaturesForAddress", [address, {"limit": limit}])
        return result or []
    except Exception as e:
        logger.debug("tx_fetch_failed", address=address[:12], error=str(e))
        return []


def get_token_price(mint: str) -> float | None:
    """Get current token price from DexScreener."""
    try:
        data = _fetch_json(f"https://api.dexscreener.com/token-pairs/v1/solana/{mint}")
        pairs = data if isinstance(data, list) else data.get("pairs", [])
        if pairs:
            best = max(pairs, key=lambda p: (p.get("liquidity", {}) or {}).get("usd", 0) or 0)
            price = best.get("priceUsd")
            return float(price) if price else None
    except Exception:
        pass
    return None


def profile_wallet(address: str, label: str = "", tier: int = 3) -> dict:
    """Build a complete profile for a wallet — holdings, recent activity, stats."""
    profile = {
        "address": address,
        "label": label,
        "tier": tier,
        "chain": "solana",
        "holdings": [],
        "recent_txs": 0,
        "total_holdings_usd": 0,
    }

    # Get holdings
    holdings = get_wallet_token_holdings(address)
    profile["holdings_count"] = len(holdings)

    # Get prices for top holdings (limit API calls)
    total_usd = 0
    enriched = []
    for h in holdings[:20]:  # Top 20 tokens
        mint = h["mint"]
        price = get_token_price(mint)
        usd_value = h["amount"] * price if price else 0
        enriched.append({
            "mint": mint,
            "amount": h["amount"],
            "price": price,
            "usd_value": round(usd_value, 2),
        })
        total_usd += usd_value
        time.sleep(0.3)  # Rate limit DexScreener

    enriched.sort(key=lambda x: x["usd_value"], reverse=True)
    profile["holdings"] = enriched[:10]  # Top 10 by value
    profile["total_holdings_usd"] = round(total_usd, 2)

    # Get recent transactions
    txs = get_wallet_recent_txs(address, 10)
    profile["recent_txs"] = len(txs)
    if txs:
        latest_slot = txs[0].get("slot", 0)
        profile["last_active_slot"] = latest_slot

    # Save to DB
    conn = _ensure_db()
    try:
        now = datetime.now(timezone.utc).isoformat()
        conn.execute("""
            INSERT OR REPLACE INTO wallet_profiles
            (address, label, chain, tier, updated_at)
            VALUES (?, ?, 'solana', ?, ?)
        """, (address, label, tier, now))

        for h in enriched[:10]:
            conn.execute("""
                INSERT OR REPLACE INTO wallet_holdings
                (wallet_address, token_mint, amount, current_price, updated_at)
                VALUES (?, ?, ?, ?, ?)
            """, (address, h["mint"], h["amount"], h["price"], now))

        conn.commit()
    finally:
        conn.close()

    return profile


def format_wallet_profile(profile: dict) -> str:
    """Format wallet profile as Telegram HTML."""
    addr = profile["address"]
    label = profile.get("label", addr[:12])
    total_usd = profile.get("total_holdings_usd", 0)
    holdings = profile.get("holdings", [])
    tx_count = profile.get("recent_txs", 0)

    lines = [
        f"🐋 <b>{label}</b>",
        f"📍 {addr[:8]}...{addr[-6:]}",
        f"💰 持仓总值: ${total_usd:,.0f}",
        f"📊 代币数: {profile.get('holdings_count', 0)} · 近期TX: {tx_count}",
        "",
    ]

    if holdings:
        lines.append("<b>Top持仓:</b>")
        for i, h in enumerate(holdings[:5], 1):
            mint = h["mint"][:8]
            usd = h.get("usd_value", 0)
            amt = h.get("amount", 0)
            if usd > 0:
                lines.append(f"  {i}. {mint}... · ${usd:,.0f}")

    return "\n".join(lines)


def auto_discover_smart_wallets(token_mint: str, min_multiplier: float = 10.0) -> list[dict]:
    """Auto-discover wallets that bought a token early and got 10x+.

    Looks at current top holders and estimates their entry timing.
    Returns list of wallet addresses worth tracking.
    """
    # This is a placeholder for the full implementation
    # Full version would:
    # 1. Get top 100 holders of a token that did 10x+
    # 2. Check when each holder first bought
    # 3. If they bought in first 1h and held through 10x, they're "smart money"
    # 4. Add them to the watchlist
    logger.info("auto_discover_placeholder", token=token_mint)
    return []


if __name__ == "__main__":
    import sys
    # Load env
    for line in open(".env").readlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ[k.strip()] = v.strip()

    # Update SOLANA_RPC from env
    SOLANA_RPC = os.environ.get("SOLANA_RPC_URL", SOLANA_RPC)

    # Test with a known wallet
    addr = sys.argv[1] if len(sys.argv) > 1 else "HWdeC9mMkYzqqfhxkLYmKFq5bgFYNuUxTD2TDJK4pump"
    print(f"Profiling {addr[:16]}...")
    import re
    profile = profile_wallet(addr, "Test Wallet", 1)
    print(re.sub(r'<[^>]+>', '', format_wallet_profile(profile)))
