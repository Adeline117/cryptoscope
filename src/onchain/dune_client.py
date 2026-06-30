"""Reusable Dune Analytics client — verified on-chain data via SQL.

Dune's labeled datasets are a RELIABLE source for things we must not guess:
exchange/router/bridge address labels, holder/transfer data at scale, etc. The
key auto-loads via src.config (DUNE_API_KEY). Pattern: create-or-reuse a query →
execute → poll results.

Critical discipline baked in (learned building the BSC CEX list): Dune's exchange
labels INCLUDE DEX aggregators and bridges — a transfer to a router/bridge is a
swap/bridge, NOT a CEX deposit. ALWAYS filter those out before using a label as a
"CEX" signal, or the #1 dump signal false-fires. See bsc_cex_addresses().

    from src.onchain.dune_client import run_sql
    rows = run_sql("select address, custody_owner from labels.owner_addresses "
                   "where blockchain = 'bnb' limit 10")
"""

from __future__ import annotations

import json
import os
import time
import urllib.request

import structlog

logger = structlog.get_logger()

_BASE = "https://api.dune.com/api/v1"
_ROUTER_BRIDGE_HINTS = ("dex", "aggregator", "bridge", "stargate", "router", "swap")


def available() -> bool:
    return bool(os.environ.get("DUNE_API_KEY"))


def _req(method: str, path: str, body: dict | None = None, timeout: int = 25) -> dict | None:
    key = os.environ.get("DUNE_API_KEY")
    if not key:
        return None
    try:
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(
            f"{_BASE}{path}", data=data, method=method,
            headers={"X-Dune-Api-Key": key, "Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode())
    except Exception as e:
        logger.debug("dune_req_failed", path=path, error=str(e)[:80])
        return None


def run_sql(sql: str, *, query_id: int | None = None, poll_s: int = 5,
            max_polls: int = 18) -> list[dict]:
    """Run an ad-hoc SQL query on Dune and return result rows (or [] on failure).

    Reuses a query_id if given (PATCH the SQL) to avoid hitting the private-query
    cap; otherwise creates a public query. Never raises."""
    if not available():
        return []
    qid = query_id
    if qid is None:
        created = _req("POST", "/query",
                       {"name": "cryptoscope_adhoc", "query_sql": sql, "is_private": False})
        qid = (created or {}).get("query_id")
        if not qid:
            return []
    else:
        _req("PATCH", f"/query/{qid}", {"query_sql": sql})
    ex = _req("POST", f"/query/{qid}/execute")
    eid = (ex or {}).get("execution_id")
    if not eid:
        return []
    for _ in range(max_polls):
        time.sleep(poll_s)
        res = _req("GET", f"/execution/{eid}/results")
        state = (res or {}).get("state")
        if state == "QUERY_STATE_COMPLETED":
            return (res or {}).get("result", {}).get("rows", []) or []
        if state == "QUERY_STATE_FAILED":
            logger.debug("dune_query_failed", error=str((res or {}).get("error"))[:120])
            return []
    return []


def bsc_cex_addresses(query_id: int | None = 7852861) -> dict[str, str]:
    """Verified BSC exchange hot/deposit wallets from Dune labels, with routers and
    bridges FILTERED OUT (they are not CEX deposits). Returns {address_lower: label}.
    Used to refresh cex_addresses.BSC_CEX_SUPPLEMENT; cheap to re-run periodically."""
    rows = run_sql(
        "select address, custody_owner, owner_key from labels.owner_addresses "
        "where blockchain = 'bnb' and ("
        "lower(owner_key) like '%binance%' or lower(owner_key) like '%okx%' or "
        "lower(owner_key) like '%gate%' or lower(owner_key) like '%mexc%' or "
        "lower(owner_key) like '%bitget%' or lower(owner_key) like '%kucoin%' or "
        "lower(owner_key) like '%bybit%' or lower(owner_key) like '%htx%' or "
        "lower(owner_key) like '%kraken%' or lower(owner_key) like '%coinbase%') limit 200",
        query_id=query_id)
    out: dict[str, str] = {}
    for r in rows:
        key = (r.get("owner_key") or "").lower()
        owner = r.get("custody_owner") or r.get("owner_key") or ""
        addr = (r.get("address") or "").lower()
        if not addr or any(h in key for h in _ROUTER_BRIDGE_HINTS) \
                or any(h in owner.lower() for h in _ROUTER_BRIDGE_HINTS):
            continue
        out[addr] = owner if owner and owner[:1].isupper() else key
    return out
