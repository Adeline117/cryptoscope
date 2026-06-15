"""Autonomous operator finder — identify the controlling cluster WITHOUT Arkham.

The ETH falsification showed raw concentration has no signal; the linchpin is
knowing WHICH addresses are the operator. Arkham gives that, but to be fully
self-serve we try to recover it ourselves:

  1. Reconstruct the token's current holder set (top holders by balance).
  2. Resolve each top holder's first-funder (funder_graph).
  3. Cluster via the same heuristics (shared root funder + similar balance),
     excluding CEX/pool addresses.
  4. The largest non-custodial entity cluster = the candidate operator.

This is weaker than Arkham (sophisticated operators fund each wallet from a fresh
CEX withdrawal to defeat funder-clustering), but it's free and fully autonomous.
Returns the candidate operator addresses to feed into the operator curve.

EVM only (needs funder resolution via Etherscan, which is ETH-only on free keys).
"""

from __future__ import annotations

import structlog

logger = structlog.get_logger()


def find_operator_cluster(token: str, chain: str, top_n: int = 60,
                          min_cluster: int = 3) -> dict | None:
    """Find the largest funder/balance-linked holder cluster for a token.

    Returns {operator: [addresses], cluster_count, top_holders, share_pct} or None.
    """
    if chain not in ("ethereum", "eth"):
        logger.info("operator_finder_evm_only", chain=chain)
        return None

    from src.onchain.holder_snapshot import fetch_holders_evm
    from src.onchain.entity_clustering import cluster_addresses, _exchange_addresses, _norm
    from src.onchain.funder_graph import get_funders

    holders = fetch_holders_evm(token, chain_id=1, max_pages=20)
    if len(holders) < 10:
        return None
    holders.sort(key=lambda h: h["balance"], reverse=True)
    top = holders[:top_n]
    addrs = [_norm(h["address"]) for h in top]
    bal_map = {_norm(h["address"]): h["balance"] for h in top}
    total = sum(h["balance"] for h in holders)

    # Resolve funders for the top holders (cached, rotated keys).
    funders = get_funders(addrs, "ethereum", max_lookups=top_n)

    # Cluster (shared root funder + similar balance), excluding CEX/pool.
    exclude = _exchange_addresses()
    mapping = cluster_addresses(addrs, funders=funders, exclude=exclude, balances=bal_map)

    # Group addresses by entity, pick the largest by combined balance.
    by_entity: dict[str, list[str]] = {}
    for a, ent in mapping.items():
        by_entity.setdefault(ent, []).append(a)
    if not by_entity:
        return None
    best_ent = max(by_entity, key=lambda e: sum(bal_map.get(a, 0) for a in by_entity[e]))
    operator = by_entity[best_ent]
    op_bal = sum(bal_map.get(a, 0) for a in operator)

    result = {
        "operator": operator,
        "cluster_count": len(operator),
        "share_pct": round(op_bal / total * 100, 2) if total > 0 else 0,
        "top_holders_examined": len(top),
    }
    logger.info("operator_cluster_found", token=token, **{k: result[k] for k in
                ("cluster_count", "share_pct")})
    # Only meaningful if the cluster is more than a lone address.
    return result if len(operator) >= min_cluster else result | {"weak": True}
