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

# Etherscan's free tier only covers Ethereum mainnet ("Free API access is not
# supported for this chain" on chainid!=1). For other EVM chains we use Moralis
# (free tier) when a key is present — this is what lets BSC funder clustering run.
_MORALIS_CHAINS = {
    "bsc": "bsc", "base": "base", "arbitrum": "arbitrum",
    "optimism": "optimism", "polygon": "polygon", "ethereum": "eth", "eth": "eth",
}
_BROWSER_UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
               "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")

# Minimum native value (wei) for an incoming transfer to count as REAL funding.
# `value > 0` is not enough: address-poisoning spam sends dust (observed 2e10 wei =
# 0.00000002 BNB) from a vanity look-alike of the target — value>0 but not funding.
# Counting it clusters the VICTIM with the POISONER (the MAME false link). Real gas
# funding is orders of magnitude larger (the MAME operator's funder sent 60-101 BNB);
# 1e14 wei (0.0001 native ≈ a few txs of gas) sits 5000x above the dust, far below any
# genuine funding. Chain-agnostic: poisoning is ~zero-value on every EVM chain.
MIN_FUNDER_VALUE_WEI = 10**14


def _fetch_first_funder_moralis(address: str, chain: str, timeout: int = 20) -> str | None:
    """First incoming native transfer's sender via Moralis (free tier, multi-key
    rotated). Covers BSC and other EVM chains Etherscan's free tier locks out."""
    from src.onchain import moralis_client
    mchain = _MORALIS_CHAINS.get(chain)
    if not moralis_client.available() or not mchain:
        return None
    data = moralis_client.get(f"{address}?chain={mchain}&order=ASC&limit=50", timeout)
    if not data:
        return None
    for tx in data.get("result", []):
        to = (tx.get("to_address") or "").lower()
        frm = (tx.get("from_address") or "").lower()
        try:
            value = int(tx.get("value", "0") or 0)
        except (ValueError, TypeError):
            value = 0
        if to == address.lower() and value >= MIN_FUNDER_VALUE_WEI and frm:
            return frm
    return None


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
    conn = sqlite3.connect(str(db_path), timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=10000")
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
            # First incoming funding transfer → `from` is the funder. Dust-poisoning
            # spam (value>0 but ~zero) must not count, or the victim clusters with the
            # poisoner — see MIN_FUNDER_VALUE_WEI.
            if to == address.lower() and value >= MIN_FUNDER_VALUE_WEI and frm:
                return frm
        return None
    except Exception as e:
        logger.debug("funder_fetch_failed", address=address, error=str(e))
        return None


def _rpc(url: str, method: str, params: list, timeout: int = 15) -> dict:
    req = urllib.request.Request(
        url,
        data=json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


def _fetch_first_funder_solana(address: str, max_pages: int = 2, timeout: int = 15) -> str | None:
    """Return the source of the address's first incoming SOL transfer, or None.

    Paginates getSignaturesForAddress back to the oldest signature (fresh
    accumulation/Sybil wallets have few txs, so 1-2 pages reach the true oldest);
    capped at max_pages=2 for speed so a Solana scan doesn't crawl (each wallet was
    ~1-1.5s × pages). Then parses that tx for the SOL transfer to the address.
    """
    rpc = os.environ.get("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")
    try:
        oldest = None
        before = None
        for _ in range(max_pages):
            params: list = [address, {"limit": 1000}]
            if before:
                params[1]["before"] = before
            sigs = _rpc(rpc, "getSignaturesForAddress", params, timeout).get("result", [])
            if not sigs:
                break
            oldest = sigs[-1].get("signature")
            if len(sigs) < 1000:
                break  # reached the last (oldest) page
            before = oldest
        if not oldest:
            return None

        tx = _rpc(
            rpc, "getTransaction",
            [oldest, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}],
            timeout,
        ).get("result", {})
        instrs = (
            tx.get("transaction", {}).get("message", {}).get("instructions", [])
        )
        for ins in instrs:
            parsed = ins.get("parsed", {})
            if isinstance(parsed, dict) and parsed.get("type") in ("transfer", "createAccount"):
                info = parsed.get("info", {})
                dest = info.get("destination") or info.get("newAccount")
                if dest == address:
                    src = info.get("source") or info.get("lamports") and info.get("source")
                    if src:
                        return src
        # Fallback: fee payer (first account key) usually funded a fresh wallet.
        keys = tx.get("transaction", {}).get("message", {}).get("accountKeys", [])
        if keys:
            first = keys[0]
            payer = first.get("pubkey") if isinstance(first, dict) else first
            if payer and payer != address:
                return payer
        return None
    except Exception as e:
        logger.debug("solana_funder_fetch_failed", address=address, error=str(e))
        return None


def get_funders(
    addresses: list[str], chain: str, max_lookups: int = 40, db_path: Path = DB_PATH
) -> dict[str, str]:
    """Resolve first-funders for a set of addresses (cached, rate-limited).

    Returns address -> funder for those that resolve. Unknown/None funders are
    cached too (as NULL) to avoid repeat lookups. EVM via Etherscan, Solana via
    Helius/RPC signature pagination.
    """
    is_solana = chain in ("solana", "sol")
    # Solana addresses are case-sensitive (base58); EVM are not.
    addrs = [a if is_solana else a.lower() for a in addresses if a]
    if not addrs:
        return {}

    keys = _keys()
    chain_id = _EVM_CHAIN_IDS.get(chain, 1)
    # Etherscan free works only for ETH; other EVM chains route through Moralis, with
    # Covalent/GoldRush as the keyed free fallback when Moralis is parked/unavailable.
    from src.onchain import covalent_client, moralis_client
    use_moralis = (not is_solana and chain_id != 1 and moralis_client.usable())
    use_covalent = (not is_solana and chain_id != 1 and not use_moralis
                    and covalent_client.available())
    if not is_solana and not keys and not use_moralis and not use_covalent:
        return {}

    cached = _cache_get(addrs, chain, db_path)
    result: dict[str, str] = {a: f for a, f in cached.items() if f}

    todo = [a for a in addrs if a not in cached][:max_lookups]
    for i, addr in enumerate(todo):
        if is_solana:
            funder = _fetch_first_funder_solana(addr)
        elif use_moralis:
            funder = _fetch_first_funder_moralis(addr, chain)
        elif use_covalent:
            funder = covalent_client.first_funder(addr, chain_id)
        else:
            funder = _fetch_first_funder_evm(addr, chain_id, keys[i % len(keys)])
        _cache_put(addr, chain, funder, db_path)
        if funder:
            result[addr] = funder
    logger.info(
        "funders_resolved", chain=chain, requested=len(addrs),
        looked_up=len(todo), resolved=len(result),
    )
    return result
