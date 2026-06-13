"""Funder graph — the missing input that makes Sybil clustering work.

A whale spreads accumulation across many fresh wallets, but those wallets have to
be *funded* (gas / first deposit) from somewhere. The first funder of an address
is a strong "same entity" link: dozens of holders sharing one funder are almost
certainly one actor.

This module resolves the first funder of an address:
  - EVM: the sender of the address's first incoming native transfer (Etherscan
    V2 `txlist`, ascending). Funders are immutable, so results are cached in
    SQLite forever. The 6-key Etherscan pool is rotated to spread rate limits.
  - Solana: best-effort / not implemented in the free MVP (first-funder requires
    parsing full tx history); returns {} so clustering falls back to co-buy +
    label exclusion on Solana.

Output feeds `entity_clustering.effective_concentration(..., funders=...)`.
"""

from __future__ import annotations

import json
import os
import sqlite3
import urllib.parse
import urllib.request
from pathlib import Path

import structlog

from src.config import DATA_DIR

logger = structlog.get_logger()

DB_PATH = DATA_DIR / "funder_graph.db"

_EVM_CHAIN_IDS = {
    "ethereum": 1, "eth": 1, "base": 8453, "bsc": 56,
    "arbitrum": 42161, "optimism": 10, "polygon": 137,
}


def _keys() -> list[str]:
    """Etherscan key pool (ETHERSCAN_API_KEYS csv, else single ETHERSCAN_API_KEY)."""
    pool = os.environ.get("ETHERSCAN_API_KEYS", "")
    keys = [k.strip() for k in pool.split(",") if k.strip()]
    if not keys:
        single = os.environ.get("ETHERSCAN_API_KEY", "")
        keys = [single] if single else []
    return keys


def _connect(db_path: Path = DB_PATH) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS funders (
            address TEXT NOT NULL,
            chain TEXT NOT NULL,
            funder TEXT,
            PRIMARY KEY (address, chain)
        )
    """)
    return conn


def _cache_get(addrs: list[str], chain: str, db_path: Path) -> dict[str, str | None]:
    conn = _connect(db_path)
    try:
        out: dict[str, str | None] = {}
        for a in addrs:
            row = conn.execute(
                "SELECT funder FROM funders WHERE address = ? AND chain = ?", (a, chain)
            ).fetchone()
            if row is not None:
                out[a] = row[0]
        return out
    finally:
        conn.close()


def _cache_put(address: str, chain: str, funder: str | None, db_path: Path) -> None:
    conn = _connect(db_path)
    try:
        conn.execute(
            "INSERT OR REPLACE INTO funders (address, chain, funder) VALUES (?, ?, ?)",
            (address, chain, funder),
        )
        conn.commit()
    finally:
        conn.close()


def _fetch_first_funder_evm(address: str, chain_id: int, key: str, timeout: int = 15) -> str | None:
    """Return the sender of the address's first incoming native transfer, or None."""
    url = (
        f"https://api.etherscan.io/v2/api?chainid={chain_id}&module=account&action=txlist"
        f"&address={address}&startblock=0&endblock=99999999&page=1&offset=20"
        f"&sort=asc&apikey={key}"
    )
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "CryptoScope/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode())
        if data.get("status") != "1":
            return None
        for tx in data.get("result", []):
            to = (tx.get("to") or "").lower()
            frm = (tx.get("from") or "").lower()
            try:
                value = int(tx.get("value", "0"))
            except (ValueError, TypeError):
                value = 0
            # First incoming funding transfer → `from` is the funder.
            if to == address.lower() and value > 0 and frm:
                return frm
        return None
    except Exception as e:
        logger.debug("funder_fetch_failed", address=address, error=str(e))
        return None


def get_funders(
    addresses: list[str], chain: str, max_lookups: int = 40, db_path: Path = DB_PATH
) -> dict[str, str]:
    """Resolve first-funders for a set of addresses (cached, rate-limited).

    Returns address -> funder for those that resolve. Unknown/None funders are
    cached too (as NULL) to avoid repeat lookups. Solana returns {} for now.
    """
    addrs = [a.lower() for a in addresses if a]
    if chain in ("solana", "sol") or not addrs:
        return {}
    chain_id = _EVM_CHAIN_IDS.get(chain, 1)
    keys = _keys()
    if not keys:
        return {}

    cached = _cache_get(addrs, chain, db_path)
    result: dict[str, str] = {a: f for a, f in cached.items() if f}

    todo = [a for a in addrs if a not in cached][:max_lookups]
    for i, addr in enumerate(todo):
        key = keys[i % len(keys)]  # rotate the pool to spread rate limits
        funder = _fetch_first_funder_evm(addr, chain_id, key)
        _cache_put(addr, chain, funder, db_path)
        if funder:
            result[addr] = funder
    logger.info(
        "funders_resolved", chain=chain, requested=len(addrs),
        looked_up=len(todo), resolved=len(result),
    )
    return result
