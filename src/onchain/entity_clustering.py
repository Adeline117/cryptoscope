"""Sybil / entity clustering → effective concentration.

This is the core edge of the accumulation system. The naive concentration
metric (top-N holder %) is a *fake signal*: a sophisticated whale spreads its
position across dozens of fresh addresses, so the nominal holder count rises
and top-N % looks flat or falls — while the position is actually highly
concentrated.

We cluster addresses that are likely the same entity, then recompute
concentration over *entities* instead of raw addresses. The gap between
effective concentration (clustered) and nominal concentration (raw) is the
divergence the accumulation signal keys on.

MVP heuristics (no graph ML yet):
  1. Common funder       — addresses first funded by the same source wallet.
  2. Temporal co-buying  — addresses that bought the same token in the same
                           short window (passed in as co-buy groups).
  3. Label propagation   — known CEX/MM/bridge addresses are tagged and EXCLUDED
                           from "whale" concentration (they are custodial noise,
                           not a single accumulating entity).

The clustering is a union-find over the heuristic edges. A heavier
implementation (e.g. HasciDB-style Sybil clustering across many airdrops) can
be ported in later behind the same `cluster_addresses` interface.
"""

from __future__ import annotations

from typing import Any, Iterable

import structlog

logger = structlog.get_logger()


# Known custodial / non-entity address labels to exclude from whale clustering.
# Seeded from whale_tracker.KNOWN_EXCHANGES; extend as needed. These represent
# omnibus wallets (many users), not a single accumulating actor.
def _exchange_addresses() -> set[str]:
    try:
        from src.collectors.whale_tracker import WhaleTrackerCollector

        return {a.lower() for a in WhaleTrackerCollector.KNOWN_EXCHANGES}
    except Exception:
        return set()


class _UnionFind:
    def __init__(self) -> None:
        self.parent: dict[str, str] = {}

    def find(self, x: str) -> str:
        self.parent.setdefault(x, x)
        root = x
        while self.parent[root] != root:
            root = self.parent[root]
        # path compression
        while self.parent[x] != root:
            self.parent[x], x = root, self.parent[x]
        return root

    def union(self, a: str, b: str) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[ra] = rb


def cluster_addresses(
    addresses: Iterable[str],
    funders: dict[str, str] | None = None,
    co_buy_groups: list[list[str]] | None = None,
    exclude: set[str] | None = None,
) -> dict[str, str]:
    """Cluster addresses into entities via union-find over heuristic edges.

    Args:
        addresses: all addresses under consideration (lowercased internally).
        funders: address -> funder address. Addresses sharing a funder merge.
        co_buy_groups: lists of addresses that co-bought in the same window;
            each group merges together.
        exclude: addresses to keep as singletons (e.g. CEX/MM). Defaults to
            known exchange addresses.

    Returns:
        address -> entity_id (the union-find root).
    """
    if exclude is None:
        exclude = _exchange_addresses()
    exclude = {a.lower() for a in exclude}

    addrs = [a.lower() for a in addresses]
    uf = _UnionFind()
    for a in addrs:
        uf.find(a)  # ensure present

    # Edge type 1: common funder (skip excluded addresses).
    if funders:
        by_funder: dict[str, list[str]] = {}
        for addr, funder in funders.items():
            al, fl = addr.lower(), (funder or "").lower()
            if not fl or al in exclude or fl in exclude:
                continue
            by_funder.setdefault(fl, []).append(al)
        for group in by_funder.values():
            for other in group[1:]:
                uf.union(group[0], other)

    # Edge type 2: temporal co-buying.
    if co_buy_groups:
        for group in co_buy_groups:
            members = [a.lower() for a in group if a.lower() not in exclude]
            for other in members[1:]:
                uf.union(members[0], other)

    return {a: uf.find(a) for a in addrs if a not in exclude}


def effective_concentration(
    holders: list[dict[str, Any]],
    funders: dict[str, str] | None = None,
    co_buy_groups: list[list[str]] | None = None,
    exclude: set[str] | None = None,
    top_n: int = 10,
) -> dict[str, Any]:
    """Compute effective (entity-level) vs nominal (address-level) concentration.

    Returns a dict with:
        nominal_top_n_pct      — top-N by raw address
        effective_top_n_pct    — top-N by clustered entity
        nominal_holder_count   — raw address count (balance > 0)
        entity_count           — clustered entity count
        concentration_gap      — effective - nominal (the divergence, in pts)
        largest_entity_pct     — share held by the single largest entity
    """
    pos = [
        {"address": str(h["address"]).lower(), "balance": float(h.get("balance", 0) or 0)}
        for h in holders
        if float(h.get("balance", 0) or 0) > 0
    ]
    total = sum(h["balance"] for h in pos)
    if total <= 0:
        return {
            "nominal_top_n_pct": 0.0, "effective_top_n_pct": 0.0,
            "nominal_holder_count": 0, "entity_count": 0,
            "concentration_gap": 0.0, "largest_entity_pct": 0.0,
        }

    # Nominal: top-N by raw address.
    addr_bal = sorted((h["balance"] for h in pos), reverse=True)
    nominal_top = round(sum(addr_bal[:top_n]) / total * 100, 4)

    # Effective: cluster, then top-N by entity.
    mapping = cluster_addresses(
        (h["address"] for h in pos), funders, co_buy_groups, exclude
    )
    excluded = set(h["address"] for h in pos) - set(mapping)
    entity_bal: dict[str, float] = {}
    for h in pos:
        if h["address"] in excluded:
            continue  # custodial noise: not part of whale concentration
        ent = mapping[h["address"]]
        entity_bal[ent] = entity_bal.get(ent, 0.0) + h["balance"]

    ent_sorted = sorted(entity_bal.values(), reverse=True)
    effective_top = round(sum(ent_sorted[:top_n]) / total * 100, 4)
    largest = round((ent_sorted[0] / total * 100), 4) if ent_sorted else 0.0

    result = {
        "nominal_top_n_pct": nominal_top,
        "effective_top_n_pct": effective_top,
        "nominal_holder_count": len(pos),
        "entity_count": len(entity_bal),
        "concentration_gap": round(effective_top - nominal_top, 4),
        "largest_entity_pct": largest,
    }
    logger.debug("effective_concentration", **result)
    return result
