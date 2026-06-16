"""Holder distribution snapshots — the foundation of accumulation detection.

A token must be snapshotted *from birth*; with no snapshot history the past is
permanently lost. This module:

1. Records token birth (first time we see it).
2. Fetches the current holder distribution (top holders) per chain.
3. Persists timestamped snapshots to SQLite.
4. Computes *nominal* concentration metrics (top-N %, holder count, gini).

The nominal metrics here are deliberately the "naive" view. The *effective*
concentration (after Sybil clustering) lives in `entity_clustering.py`; the
accumulation signal is the divergence between the two (see
`src/signals/accumulation_divergence.py`).

Fetchers are best-effort and free-tier:
- Solana: Helius / Solana RPC `getTokenLargestAccounts` (top accounts) + owner
  resolution. (Solscan free keys cannot access holders — HTTP 401.)
- EVM: reconstruct balances from Etherscan V2 `tokentx` over a recent window.

Full holder enumeration is a later upgrade; top-holder coverage is sufficient
to compute concentration and feed clustering on the significant holders.
"""

from __future__ import annotations

import json
import os
import sqlite3
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import structlog

from src.config import DATA_DIR

logger = structlog.get_logger()

DB_PATH = DATA_DIR / "holder_snapshots.db"


# --------------------------------------------------------------------------
# Pure logic — concentration metrics (unit-tested, no I/O)
# --------------------------------------------------------------------------

def top_n_pct(balances: list[float], n: int = 10) -> float:
    """Share of total supply held by the largest `n` holders (0-100)."""
    total = sum(balances)
    if total <= 0:
        return 0.0
    top = sorted(balances, reverse=True)[:n]
    return round(sum(top) / total * 100, 4)


def gini(balances: list[float]) -> float:
    """Gini coefficient of the balance distribution (0 = equal, 1 = concentrated).

    Returns 0.0 for empty/degenerate input.
    """
    vals = sorted(b for b in balances if b > 0)
    n = len(vals)
    if n == 0:
        return 0.0
    total = sum(vals)
    if total <= 0:
        return 0.0
    # Gini via the ordered-cumulative formula.
    cum = 0.0
    for i, v in enumerate(vals, start=1):
        cum += i * v
    return round((2 * cum) / (n * total) - (n + 1) / n, 4)


def concentration_metrics(holders: list[dict[str, Any]]) -> dict[str, Any]:
    """Compute nominal concentration metrics from a holder list.

    Each holder dict must have an "address" and a numeric "balance".
    """
    balances = [float(h.get("balance", 0) or 0) for h in holders]
    return {
        "holder_count": len([b for b in balances if b > 0]),
        "top10_pct": top_n_pct(balances, 10),
        "top25_pct": top_n_pct(balances, 25),
        "gini": gini(balances),
        "total_supply_observed": round(sum(balances), 6),
    }


# --------------------------------------------------------------------------
# Storage
# --------------------------------------------------------------------------

def _connect(db_path: Path = DB_PATH) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=10000")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS token_birth (
            token TEXT NOT NULL,
            chain TEXT NOT NULL,
            first_seen_at TEXT NOT NULL,
            source TEXT,
            PRIMARY KEY (token, chain)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS holder_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            token TEXT NOT NULL,
            chain TEXT NOT NULL,
            snapshot_at TEXT NOT NULL,
            holder_count INTEGER,
            top10_pct REAL,
            top25_pct REAL,
            gini REAL,
            total_supply_observed REAL,
            holders_json TEXT
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_snap_token "
        "ON holder_snapshots(token, chain, snapshot_at)"
    )
    return conn


def record_token_birth(
    token: str, chain: str, source: str = "", db_path: Path = DB_PATH
) -> None:
    """Record the first time a token enters the universe (idempotent)."""
    conn = _connect(db_path)
    try:
        conn.execute(
            "INSERT OR IGNORE INTO token_birth (token, chain, first_seen_at, source) "
            "VALUES (?, ?, ?, ?)",
            (token, chain, datetime.now(timezone.utc).isoformat(), source),
        )
        conn.commit()
    finally:
        conn.close()


def save_snapshot(
    token: str,
    chain: str,
    holders: list[dict[str, Any]],
    snapshot_at: str | None = None,
    db_path: Path = DB_PATH,
) -> dict[str, Any]:
    """Persist a holder snapshot and return the computed metrics."""
    metrics = concentration_metrics(holders)
    ts = snapshot_at or datetime.now(timezone.utc).isoformat()
    conn = _connect(db_path)
    try:
        conn.execute(
            """INSERT INTO holder_snapshots
               (token, chain, snapshot_at, holder_count, top10_pct, top25_pct,
                gini, total_supply_observed, holders_json)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                token, chain, ts, metrics["holder_count"], metrics["top10_pct"],
                metrics["top25_pct"], metrics["gini"],
                metrics["total_supply_observed"], json.dumps(holders),
            ),
        )
        conn.commit()
    finally:
        conn.close()
    logger.info("holder_snapshot_saved", token=token, chain=chain, **metrics)
    return {**metrics, "snapshot_at": ts}


def get_snapshots(
    token: str, chain: str, limit: int = 100, db_path: Path = DB_PATH
) -> list[dict[str, Any]]:
    """Return snapshots for a token, oldest first (time series)."""
    conn = _connect(db_path)
    try:
        rows = conn.execute(
            """SELECT snapshot_at, holder_count, top10_pct, top25_pct, gini,
                      total_supply_observed
               FROM holder_snapshots WHERE token = ? AND chain = ?
               ORDER BY snapshot_at ASC LIMIT ?""",
            (token, chain, limit),
        ).fetchall()
    finally:
        conn.close()
    cols = ["snapshot_at", "holder_count", "top10_pct", "top25_pct", "gini",
            "total_supply_observed"]
    return [dict(zip(cols, r)) for r in rows]


def list_tokens(db_path: Path = DB_PATH) -> list[tuple[str, str]]:
    """Return [(token, chain)] for every token with at least one snapshot."""
    conn = _connect(db_path)
    try:
        rows = conn.execute(
            "SELECT DISTINCT token, chain FROM holder_snapshots"
        ).fetchall()
    finally:
        conn.close()
    return [(t, c) for t, c in rows]


def get_holders_history(
    token: str, chain: str, limit: int = 100, since: str | None = None,
    db_path: Path = DB_PATH,
) -> list[tuple[str, list[dict[str, Any]]]]:
    """Return [(snapshot_at, holders)] oldest-first, for recomputing series.

    `since` (ISO timestamp) bounds the window so a *realtime* signal is not
    polluted by stale snapshots from months ago — the same class of bug that
    let a 3-month-old item leak into the 2h highlight. Pass it to keep the
    accumulation slope reflecting recent activity only.
    """
    conn = _connect(db_path)
    try:
        if since is not None:
            rows = conn.execute(
                """SELECT snapshot_at, holders_json FROM holder_snapshots
                   WHERE token = ? AND chain = ? AND snapshot_at >= ?
                   ORDER BY snapshot_at ASC LIMIT ?""",
                (token, chain, since, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                """SELECT snapshot_at, holders_json FROM holder_snapshots
                   WHERE token = ? AND chain = ? ORDER BY snapshot_at ASC LIMIT ?""",
                (token, chain, limit),
            ).fetchall()
    finally:
        conn.close()
    out: list[tuple[str, list[dict[str, Any]]]] = []
    for ts, hj in rows:
        try:
            out.append((ts, json.loads(hj) if hj else []))
        except (json.JSONDecodeError, TypeError):
            out.append((ts, []))
    return out


# --------------------------------------------------------------------------
# I/O — chain fetchers (best-effort, free-tier)
# --------------------------------------------------------------------------

def _rpc(url: str, method: str, params: list, timeout: int = 15) -> dict:
    payload = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
    req = urllib.request.Request(
        url, data=payload.encode(), headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


def _rpc_das(url: str, method: str, params: dict, timeout: int = 20) -> dict:
    """Helius DAS RPC (params is an object, not a list)."""
    payload = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
    req = urllib.request.Request(
        url, data=payload.encode(), headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


def fetch_holders_solana(mint: str, max_pages: int = 10, timeout: int = 20) -> list[dict[str, Any]]:
    """Fetch the FULL Solana holder set via Helius DAS `getTokenAccounts`.

    Paginates all token accounts (1000/page) and aggregates by OWNER (one owner
    may hold many token accounts). Raw amounts are fine for concentration since
    every account shares the mint's decimals. Caps at `max_pages` (~10k accounts)
    — beyond that is dust that doesn't move concentration.

    Falls back to the top-20 `getTokenLargestAccounts` only if Helius is absent.
    Best-effort: returns [] on total failure.
    """
    rpc = os.environ.get("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")
    has_helius = "helius" in rpc

    if has_helius:
        owner_bal: dict[str, float] = {}
        try:
            for p in range(1, max_pages + 1):
                data = _rpc_das(
                    rpc, "getTokenAccounts",
                    {"mint": mint, "page": p, "limit": 1000}, timeout,
                )
                accts = data.get("result", {}).get("token_accounts", [])
                if not accts:
                    break
                for a in accts:
                    owner = a.get("owner")
                    amt = float(a.get("amount") or 0)
                    if owner and amt > 0:
                        owner_bal[owner] = owner_bal.get(owner, 0.0) + amt
                if len(accts) < 1000:
                    break
            if owner_bal:
                return [{"address": o, "balance": b} for o, b in owner_bal.items()]
        except Exception as e:
            logger.warning("solana_das_fetch_failed", mint=mint, error=str(e))
            # fall through to the largest-accounts fallback

    # Fallback: top-20 token accounts (coarse, but better than nothing).
    try:
        data = _rpc(rpc, "getTokenLargestAccounts", [mint], timeout)
        accounts = data.get("result", {}).get("value", [])
    except Exception as e:
        logger.warning("solana_holders_fetch_failed", mint=mint, error=str(e))
        return []
    holders: list[dict[str, Any]] = []
    for acc in accounts:
        amount = float(acc.get("uiAmount") or 0)
        if amount > 0:
            holders.append({"address": acc.get("address", ""), "balance": amount})
    return holders


# chain → Alchemy network subdomain
_ALCHEMY_NET = {
    1: "eth-mainnet", 8453: "base-mainnet", 42161: "arb-mainnet",
    10: "opt-mainnet", 137: "polygon-mainnet", 56: "bnb-mainnet",
}


_MORALIS_CHAINS = {1: "eth", 56: "bsc", 8453: "base", 42161: "arbitrum",
                   10: "optimism", 137: "polygon"}
_BROWSER_UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
               "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")


def fetch_holders_moralis(
    token: str, chain_id: int, max_pages: int = 5, timeout: int = 25
) -> list[dict[str, Any]]:
    """Token holders via Moralis (free tier) — the ranked owner list with balances.
    This is the path for BSC and other EVM chains where Alchemy/Etherscan free
    don't work. Cloudflare blocks the default urllib UA (1010), so a browser UA is
    required. Paginates by cursor; max_pages*100 holders."""
    key = os.environ.get("MORALIS_API_KEY")
    mchain = _MORALIS_CHAINS.get(chain_id)
    if not key or not mchain:
        return []
    holders: list[dict[str, Any]] = []
    cursor = None
    zero = "0x0000000000000000000000000000000000000000"
    try:
        for _ in range(max_pages):
            url = (f"https://deep-index.moralis.io/api/v2.2/erc20/{token}/owners"
                   f"?chain={mchain}&order=DESC&limit=100")
            if cursor:
                url += f"&cursor={cursor}"
            req = urllib.request.Request(
                url, headers={"X-API-Key": key, "accept": "application/json",
                              "User-Agent": _BROWSER_UA})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                data = json.loads(resp.read().decode())
            for r in data.get("result", []):
                addr = (r.get("owner_address") or "").lower()
                try:
                    bal = float(r.get("balance_formatted") or 0)
                except (ValueError, TypeError):
                    bal = 0.0
                if addr and addr != zero and bal > 0:
                    holders.append({"address": addr, "balance": round(bal, 8)})
            cursor = data.get("cursor")
            if not cursor:
                break
    except Exception as e:
        logger.warning("moralis_holders_failed", token=token, error=str(e))
    return holders


def fetch_holders_evm(
    token: str, chain_id: int = 1, max_pages: int = 25, timeout: int = 25
) -> list[dict[str, Any]]:
    """Reconstruct EVM holder balances from FULL transfer history via Alchemy.

    Paginates `alchemy_getAssetTransfers` (all ERC-20 transfers of the token,
    1000/page, value already decimal-adjusted) and nets per address — the real
    holder set, not just recent transactors. Caps at `max_pages` for very large
    tokens (logged when truncated). Falls back to Etherscan recent-window if no
    Alchemy key. Best-effort: returns [] on total failure.

    Non-ETH chains (BSC etc.) try Moralis first — Alchemy/Etherscan free don't
    cover them.
    """
    # Moralis owner list is the reliable free path for non-ETH EVM chains.
    if chain_id != 1 and os.environ.get("MORALIS_API_KEY"):
        m = fetch_holders_moralis(token, chain_id, max_pages=min(max_pages, 5), timeout=timeout)
        if m:
            return m

    key = os.environ.get("ALCHEMY_API_KEY", "")
    net = _ALCHEMY_NET.get(chain_id)
    if key and net:
        balances: dict[str, float] = {}
        page_key = None
        truncated = False
        try:
            for p in range(max_pages):
                params = {
                    "fromBlock": "0x0", "toBlock": "latest",
                    "contractAddresses": [token], "category": ["erc20"],
                    "withMetadata": False, "maxCount": "0x3e8", "order": "asc",
                }
                if page_key:
                    params["pageKey"] = page_key
                data = _rpc_das(
                    f"https://{net}.g.alchemy.com/v2/{key}",
                    "alchemy_getAssetTransfers", [params], timeout,
                )
                result = data.get("result", {})
                for t in result.get("transfers", []):
                    try:
                        amount = float(t.get("value") or 0)
                    except (ValueError, TypeError):
                        continue
                    frm = (t.get("from") or "").lower()
                    to = (t.get("to") or "").lower()
                    if frm:
                        balances[frm] = balances.get(frm, 0.0) - amount
                    if to:
                        balances[to] = balances.get(to, 0.0) + amount
                page_key = result.get("pageKey")
                if not page_key:
                    break
            else:
                truncated = True
            if truncated:
                logger.info("evm_holders_truncated", token=token, max_pages=max_pages)
            zero = "0x0000000000000000000000000000000000000000"
            holders = [
                {"address": a, "balance": round(b, 8)}
                for a, b in balances.items() if b > 1e-9 and a != zero
            ]
            if holders:
                return holders
        except Exception as e:
            logger.warning("evm_alchemy_fetch_failed", token=token, error=str(e))
            # fall through to Etherscan

    return _fetch_holders_evm_etherscan(token, chain_id, timeout)


def _fetch_holders_evm_etherscan(token: str, chain_id: int, timeout: int = 20) -> list[dict[str, Any]]:
    """Fallback: approximate balances from a recent Etherscan tokentx window."""
    key = os.environ.get("ETHERSCAN_API_KEY", "")
    if not key:
        return []
    url = (
        f"https://api.etherscan.io/v2/api?chainid={chain_id}&module=account"
        f"&action=tokentx&contractaddress={token}&page=1&offset=10000"
        f"&sort=desc&apikey={key}"
    )
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "CryptoScope/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode())
        if data.get("status") != "1":
            return []
        txs = data.get("result", [])
    except Exception as e:
        logger.warning("evm_holders_fetch_failed", token=token, error=str(e))
        return []

    balances: dict[str, float] = {}
    decimals: int | None = None
    for tx in txs:
        if decimals is None:
            try:
                decimals = int(tx.get("tokenDecimal", 18))
            except (ValueError, TypeError):
                decimals = 18
        try:
            raw = float(tx.get("value", 0))
        except (ValueError, TypeError):
            continue
        amount = raw / (10 ** (decimals or 18))
        frm = (tx.get("from") or "").lower()
        to = (tx.get("to") or "").lower()
        if frm:
            balances[frm] = balances.get(frm, 0.0) - amount
        if to:
            balances[to] = balances.get(to, 0.0) + amount

    zero = "0x0000000000000000000000000000000000000000"
    return [
        {"address": a, "balance": round(b, 8)}
        for a, b in balances.items()
        if b > 0 and a != zero
    ]


def snapshot_token(
    token: str, chain: str, source: str = "", db_path: Path = DB_PATH
) -> dict[str, Any] | None:
    """End-to-end: record birth, fetch holders for the chain, save snapshot.

    Returns the snapshot metrics, or None if no holders could be fetched.
    """
    record_token_birth(token, chain, source, db_path)

    if chain in ("solana", "sol"):
        holders = fetch_holders_solana(token)
    else:
        chain_ids = {"ethereum": 1, "eth": 1, "base": 8453, "arbitrum": 42161, "optimism": 10}
        holders = fetch_holders_evm(token, chain_ids.get(chain, 1))

    if not holders:
        logger.info("snapshot_skipped_no_holders", token=token, chain=chain)
        return None
    return save_snapshot(token, chain, holders, db_path=db_path)
