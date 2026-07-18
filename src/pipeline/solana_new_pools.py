"""Solana new-pool coverage across ALL venues, not just pump.fun.

The launch stream only watches pump.fun, so the signal feed was blind to every
other Solana launch venue — Raydium, Meteora, Pumpswap, and whatever launches
next. Rather than build a fragile per-program WS stream + instruction parser for
each (and fight free-RPC rate limits), poll GeckoTerminal's free, indexed
new-pools endpoint, which already covers every Solana DEX. This is a leads source
for the signal feed, not the fail-closed launch-evidence lane — so an aggregator
read is the right tool.

A dedicated poller accumulates into a small DB (deduped by token, pruned) so the
feed sees every recent pool, not just the ~20 newest at feed time.
"""
from __future__ import annotations

import json
import sqlite3
import urllib.request
from datetime import datetime, timezone

from src.config import DATA_DIR

DB = DATA_DIR / "solana_new_pools.db"
NEW_POOLS_URL = "https://api.geckoterminal.com/api/v2/networks/solana/new_pools?page={page}"
RETAIN_HOURS = 6
# Quote sides that are never the "new token".
_QUOTES = {
    "So11111111111111111111111111111111111111112",           # WSOL
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",           # USDC
    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",           # USDT
}


def _num(value):
    try:
        out = float(value)
        return out if out == out else None
    except (TypeError, ValueError):
        return None


def _strip_chain(token_id) -> str | None:
    """GeckoTerminal ids are 'solana_<address>' — return the bare address."""
    if not isinstance(token_id, str) or not token_id.startswith("solana_"):
        return None
    addr = token_id[len("solana_"):].strip()
    return addr or None


def parse_pool(raw) -> dict | None:
    """Pure: one GeckoTerminal pool object → a launch-shaped candidate, or None."""
    if not isinstance(raw, dict):
        return None
    attr = raw.get("attributes") or {}
    rel = raw.get("relationships") or {}
    base = _strip_chain((rel.get("base_token", {}).get("data") or {}).get("id"))
    quote = _strip_chain((rel.get("quote_token", {}).get("data") or {}).get("id"))
    if not base or base in _QUOTES:
        # If the base side is a known quote, the real new token is the other side.
        base = quote if quote and quote not in _QUOTES else None
    if not base:
        return None
    dex = (rel.get("dex", {}).get("data") or {}).get("id") or "?"
    created = attr.get("pool_created_at")
    if not created:
        return None
    name = attr.get("name") or ""
    symbol = name.split("/")[0].strip() if "/" in name else (name.strip() or "?")
    return {
        "token": base, "symbol": symbol or "?", "dex": str(dex),
        "pool": attr.get("address"), "created_at": str(created),
        "fdv_usd": _num(attr.get("fdv_usd")),
        "liquidity_usd": _num(attr.get("reserve_in_usd")),
        "vol_m5": _num((attr.get("volume_usd") or {}).get("m5")),
    }


def _conn() -> sqlite3.Connection:
    DB.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB), timeout=10)
    conn.execute("""CREATE TABLE IF NOT EXISTS pools(
        token TEXT PRIMARY KEY, symbol TEXT, dex TEXT, pool TEXT,
        created_at TEXT NOT NULL, detected_at TEXT NOT NULL, detected_ts REAL NOT NULL,
        fdv_usd REAL, liquidity_usd REAL, vol_m5 REAL)""")
    return conn


def fetch_new_pools(pages: int = 2) -> list:
    """Fetch the newest Solana pools across all DEXs (GeckoTerminal, keyless)."""
    out = []
    for page in range(1, pages + 1):
        req = urllib.request.Request(
            NEW_POOLS_URL.format(page=page),
            headers={"User-Agent": "CryptoScope/1.0", "Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=20) as r:
            out.extend((json.loads(r.read()).get("data")) or [])
    return out


def record(raws, *, now, conn: sqlite3.Connection | None = None) -> dict:
    """Freeze first-seen new pools (deduped by token). First detection wins."""
    stamp = now.astimezone(timezone.utc)
    owned = conn is None
    conn = conn or _conn()
    inserted = 0
    try:
        for raw in raws or []:
            p = parse_pool(raw)
            if not p:
                continue
            if conn.execute("SELECT 1 FROM pools WHERE token=?", (p["token"],)).fetchone():
                continue
            conn.execute(
                "INSERT OR IGNORE INTO pools VALUES (?,?,?,?,?,?,?,?,?,?)",
                (p["token"], p["symbol"], p["dex"], p["pool"], p["created_at"],
                 stamp.isoformat(), stamp.timestamp(),
                 p["fdv_usd"], p["liquidity_usd"], p["vol_m5"]))
            inserted += 1
        conn.execute("DELETE FROM pools WHERE detected_ts < ?",
                     (stamp.timestamp() - RETAIN_HOURS * 3600,))
        conn.commit()
        total = conn.execute("SELECT COUNT(*) FROM pools").fetchone()[0]
    finally:
        if owned:
            conn.close()
    return {"inserted": inserted, "total": total}


def recent(*, minutes: int = 120, now=None, conn: sqlite3.Connection | None = None) -> list:
    """Launch-shaped events for the signal feed (Solana, all non-pump.fun venues too)."""
    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    owned = conn is None
    conn = conn or _conn()
    try:
        rows = conn.execute(
            "SELECT token,symbol,dex,detected_at,liquidity_usd,fdv_usd FROM pools "
            "WHERE detected_ts >= ? ORDER BY detected_ts DESC",
            (current.timestamp() - minutes * 60,),
        ).fetchall()
    finally:
        if owned:
            conn.close()
    return [{"token": t, "symbol": s, "chain": "solana", "detected_at": d,
             "dex": dex, "liquidity_usd": liq, "fdv": fdv}
            for t, s, dex, d, liq, fdv in rows]


def run(*, now=None) -> dict:
    """Poll GeckoTerminal and accumulate Solana new pools. Free, keyless."""
    current = now or datetime.now(timezone.utc)
    try:
        raws = fetch_new_pools()
    except Exception as exc:
        return {"inserted": 0, "error": type(exc).__name__}
    return record(raws, now=current)


if __name__ == "__main__":
    print(json.dumps(run(), ensure_ascii=False, indent=2))
