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


# Main quote/major tokens to exclude from volume-ranked discovery — they dominate
# DEX volume but are never the operator target. Downstream filters would drop them
# anyway, but excluding here stops them from eating the top-N candidate slots.
_QUOTE_MAJORS = (
    # BNB chain
    "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c",  # WBNB
    "0x55d398326f99059ff775485246999027b3197955",  # USDT
    "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d",  # USDC
    "0xe9e7cea3dedca5984780bafc599bd69add087d56",  # BUSD
    "0x2170ed0880ac9a755fd29b2688956bd959f933f8",  # ETH
    "0x7130d2a12b9bcbfae4f2634d864a1ee1ce3ead9c",  # BTCB
    # Base
    "0x4200000000000000000000000000000000000006",  # WETH
    "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",  # USDC
    "0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca",  # USDbC
)


def top_dex_tokens(chain_map: dict[str, str] | None = None, hours: int = 24,
                   per_chain: int = 120, query_id: int | None = None) -> dict[str, list[str]]:
    """Volume-ranked candidate tokens per chain from dex.trades — a paid, rate-limit-
    free replacement for the GeckoTerminal free-tier discovery that 429s and starves
    coverage. EVM-only (dex.trades has no Solana). Returns {hunt_chain: [address,...]}.

    chain_map maps Dune blockchain -> our hunt chain id, e.g. {'bnb':'bsc','base':'base'}.
    Majors/quote tokens are excluded so they don't eat the top-N slots; everything
    else (age, liquidity band, non-operator veto) is left to the hunt's own filters."""
    chain_map = chain_map or {"bnb": "bsc", "base": "base"}
    quotes = ", ".join(_QUOTE_MAJORS)
    duned = "', '".join(chain_map.keys())
    rows = run_sql(
        f"select blockchain, token_bought_address as token, sum(amount_usd) as vol "
        f"from dex.trades "
        f"where block_time > now() - interval '{int(hours)}' hour "
        f"and blockchain in ('{duned}') and amount_usd between 100 and 5000000 "
        f"and token_bought_address not in ({quotes}) "
        f"group by 1, 2 having sum(amount_usd) > 50000 "
        f"order by blockchain, vol desc",
        query_id=query_id)
    _skip = {"0x0000000000000000000000000000000000000000",
             "0x000000000000000000000000000000000000dead"}
    out: dict[str, list[str]] = {v: [] for v in chain_map.values()}
    for r in rows:
        hunt_chain = chain_map.get(r.get("blockchain"))
        tok = (r.get("token") or "").lower()
        if hunt_chain and tok and tok not in _skip and len(out[hunt_chain]) < per_chain:
            out[hunt_chain].append(tok)
    return out


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
