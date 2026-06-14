"""Arkham Intelligence entity clustering — ground-truth for effective concentration.

The hardest, most valuable part of the accumulation system is knowing which
addresses are the SAME entity (the whale's many wallets). Arkham already does
this at scale. Their API maps any address to its entity, so we can replace the
heuristic clustering with Arkham's ground-truth grouping:

    GET /intelligence/address/{address}?chain=...  → arkhamEntity

This module resolves address→entity (cached forever — entities are stable —
and rate-limited), and exposes `entity_map()` which the clustering layer uses
as the preferred grouping. Without ARKHAM_API_KEY it returns {} and the system
falls back to the heuristic clustering.

Get a key at https://intel.arkm.com/api and set ARKHAM_API_KEY.
"""

from __future__ import annotations

import json
import os
import sqlite3
import time
import urllib.parse
import urllib.request
from pathlib import Path

import structlog

from src.config import DATA_DIR

logger = structlog.get_logger()

BASE = "https://api.arkm.com"
DB_PATH = DATA_DIR / "arkham_entities.db"
# chain → Arkham chain slug
_CHAIN = {
    "ethereum": "ethereum", "eth": "ethereum", "base": "base", "bsc": "bsc",
    "arbitrum": "arbitrum_one", "optimism": "optimism", "polygon": "polygon",
    "solana": "solana", "sol": "solana",
}


def has_key() -> bool:
    return bool(os.environ.get("ARKHAM_API_KEY"))


def _connect(db_path: Path = DB_PATH) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), timeout=10)
    conn.execute("PRAGMA busy_timeout=10000")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS address_entity (
            address TEXT NOT NULL,
            chain TEXT NOT NULL,
            entity TEXT,          -- entity id/name, or NULL if Arkham has none
            label TEXT,
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
                "SELECT entity FROM address_entity WHERE address = ? AND chain = ?",
                (a, chain),
            ).fetchone()
            if row is not None:
                out[a] = row[0]
        return out
    finally:
        conn.close()


def _cache_put(address: str, chain: str, entity: str | None, label: str | None,
               db_path: Path) -> None:
    conn = _connect(db_path)
    try:
        conn.execute(
            "INSERT OR REPLACE INTO address_entity (address, chain, entity, label) "
            "VALUES (?, ?, ?, ?)",
            (address, chain, entity, label),
        )
        conn.commit()
    finally:
        conn.close()


def _fetch_entity(address: str, chain: str, timeout: int = 12) -> tuple[str | None, str | None]:
    """Return (entity_id, label) for an address, or (None, None)."""
    key = os.environ.get("ARKHAM_API_KEY", "")
    slug = _CHAIN.get(chain, chain)
    url = f"{BASE}/intelligence/address/{urllib.parse.quote(address)}?chain={slug}"
    req = urllib.request.Request(url, headers={"API-Key": key, "User-Agent": "CryptoScope/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode())
    except Exception as e:
        logger.debug("arkham_fetch_failed", address=address, error=str(e))
        return None, None
    ent = data.get("arkhamEntity") or {}
    entity_id = ent.get("id") or ent.get("name")
    label = data.get("arkhamLabel", {}).get("name") if isinstance(data.get("arkhamLabel"), dict) else None
    return entity_id, label


def entity_map(addresses: list[str], chain: str, max_lookups: int = 300,
               rate_per_sec: float = 18.0, db_path: Path = DB_PATH) -> dict[str, str]:
    """Resolve address→entity for a holder set (cached, rate-limited).

    Returns only addresses that map to a known entity. Addresses with no Arkham
    entity are cached as NULL (so we don't re-query) and omitted from the result
    — the clustering layer leaves them as singletons.
    """
    if not has_key() or not addresses:
        return {}
    addrs = [a for a in addresses if a]
    cached = _cache_get(addrs, chain, db_path)
    result: dict[str, str] = {a: e for a, e in cached.items() if e}

    todo = [a for a in addrs if a not in cached][:max_lookups]
    delay = 1.0 / rate_per_sec if rate_per_sec > 0 else 0
    for a in todo:
        entity, label = _fetch_entity(a, chain)
        _cache_put(a, chain, entity, label, db_path)
        if entity:
            result[a] = entity
        if delay:
            time.sleep(delay)
    logger.info("arkham_entities_resolved", chain=chain, requested=len(addrs),
                looked_up=len(todo), mapped=len(result))
    return result
