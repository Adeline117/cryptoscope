"""Graph-based operator clustering (stronger than funder-linking).

Ports the transfer-graph approach from pre-airdrop-detection (07_graph_features /
ArtemisNet) to the token domain. Builds an address graph from a token's transfer
history and finds the densest non-custodial community = candidate operator.

Edges (each a coordination signal):
  - direct transfer between two holders (they moved the token to each other)
  - shared funder (common root funder, validated >=N batch threshold)
  - co-acquisition (received the token in the same short window)
Communities = connected components over these edges; the operator is the
highest-internal-density non-CEX component holding meaningful supply.

DATA REALITY (honest): needs the token's transfer graph. Free Alchemy
getAssetTransfers provides this on ETH (and Alchemy-supported EVM). On BSC the
free getLogs response-size limit makes the full graph impractical, and there is
no free per-address funder data — so this runs on ETH-class chains only. It is
UNVALIDATED against ground truth: the one labeled operator (SIREN) is on BSC,
whose graph cannot be freely reconstructed. Supervised training is not possible
with n=1 labeled token; this is the unsupervised detector to use until a labeled
dataset (Arkham clusters for many tokens) exists.
"""

from __future__ import annotations

import os
import urllib.request
import json

import structlog

from src.onchain.entity_clustering import _UnionFind, _norm, _exchange_addresses

logger = structlog.get_logger()

_ALCHEMY_NET = {"ethereum": "eth-mainnet", "eth": "eth-mainnet", "base": "base-mainnet",
                "arbitrum": "arb-mainnet", "optimism": "opt-mainnet", "polygon": "polygon-mainnet"}


def _fetch_transfers(token: str, chain: str, max_pages: int = 30) -> list[dict]:
    key = os.environ.get("ALCHEMY_API_KEY", "")
    net = _ALCHEMY_NET.get(chain)
    if not key or not net:
        return []
    url = f"https://{net}.g.alchemy.com/v2/{key}"
    out, page_key = [], None
    for _ in range(max_pages):
        params = {"fromBlock": "0x0", "toBlock": "latest", "contractAddresses": [token],
                  "category": ["erc20"], "withMetadata": False, "maxCount": "0x3e8", "order": "asc"}
        if page_key:
            params["pageKey"] = page_key
        try:
            req = urllib.request.Request(url, data=json.dumps({
                "jsonrpc": "2.0", "id": 1, "method": "alchemy_getAssetTransfers",
                "params": [params]}).encode(), headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(req, timeout=30) as r:
                data = json.loads(r.read().decode())
        except Exception as e:
            logger.debug("graph_transfers_failed", token=token, error=str(e))
            break
        res = data.get("result", {})
        for t in res.get("transfers", []):
            out.append({"from": (t.get("from") or "").lower(),
                        "to": (t.get("to") or "").lower(),
                        "block": int(t.get("blockNum", "0x0"), 16),
                        "value": float(t.get("value") or 0)})
        page_key = res.get("pageKey")
        if not page_key:
            break
    return out


def find_operator_via_graph(token: str, chain: str, funders: dict | None = None,
                            co_buy_window: int = 50) -> dict | None:
    """Build the transfer graph and return the densest non-CEX community.

    Returns {operator: [addrs], component_count, edges, n_addresses} or None.
    """
    transfers = _fetch_transfers(token, chain)
    if len(transfers) < 30:
        return None
    exclude = {a.lower() for a in _exchange_addresses()}
    zero = "0x0000000000000000000000000000000000000000"

    uf = _UnionFind()
    edges = 0
    # Edge: direct holder→holder transfer (skip mints/burns/CEX).
    for t in transfers:
        f, to = t["from"], t["to"]
        if f in (zero, "") or to in (zero, "") or f in exclude or to in exclude:
            continue
        uf.union(f, to)
        edges += 1

    # Edge: shared funder (if provided, e.g. from funder_graph on ETH).
    if funders:
        by_f: dict[str, list[str]] = {}
        for a, fdr in funders.items():
            al, fl = _norm(a), _norm(fdr or "")
            if fl and al not in exclude and fl not in exclude:
                by_f.setdefault(fl, []).append(al)
        for grp in by_f.values():
            if len(grp) >= 5:  # validated batch threshold
                for o in grp[1:]:
                    uf.union(grp[0], o)

    # Edge: co-acquisition (received token within `co_buy_window` blocks).
    recv_by_block = sorted(
        ((t["block"], t["to"]) for t in transfers
         if t["to"] not in exclude and t["to"] not in (zero, "")),
    )
    for i in range(1, len(recv_by_block)):
        if recv_by_block[i][0] - recv_by_block[i - 1][0] <= co_buy_window:
            uf.union(recv_by_block[i - 1][1], recv_by_block[i][1])

    # Components → pick the largest non-trivial one.
    comp: dict[str, list[str]] = {}
    for t in transfers:
        for a in (t["from"], t["to"]):
            if a and a not in exclude and a != zero:
                comp.setdefault(uf.find(a), []).append(a)
    comp = {r: list(set(m)) for r, m in comp.items()}
    if not comp:
        return None
    biggest = max(comp.values(), key=len)
    return {
        "operator": biggest,
        "component_count": len(comp),
        "edges": edges,
        "n_addresses": len(set(a for m in comp.values() for a in m)),
        "operator_size": len(biggest),
    }
