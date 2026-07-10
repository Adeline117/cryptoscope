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


def _norm(addr: str) -> str:
    """Normalize an address for matching: lowercase EVM (0x…) only.

    Solana / other base58 addresses are case-sensitive and must be preserved.
    """
    s = str(addr)
    return s.lower() if s.startswith("0x") else s


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


def _root_funder(addr: str, funders: dict[str, str], exclude: set[str], max_depth: int = 4) -> str | None:
    """Follow the funding chain up to its root (stopping at CEX/unknown).

    Catches the 'fresh wallet funded by another fresh wallet' pattern: even when
    each hop differs, addresses sharing a deeper common ancestor merge.
    """
    seen = set()
    cur = _norm(addr)
    root = None
    for _ in range(max_depth):
        f = funders.get(cur) or funders.get(addr)
        if not f:
            break
        fl = _norm(f)
        if fl in exclude or fl in seen:
            break
        seen.add(fl)
        root = fl
        cur = fl
    return root


def _similar_balance_groups(
    balances: dict[str, float], min_members: int = 3, sig: int = 3
) -> list[list[str]]:
    """Group addresses whose balances are near-identical (a split-position tell).

    Buckets by balance rounded to `sig` significant figures; a bucket with
    >= min_members non-trivial equal balances is very unlikely by chance.
    """
    import math

    buckets: dict[float, list[str]] = {}
    for addr, bal in balances.items():
        if not bal or bal <= 0:
            continue
        # round to `sig` significant figures
        digits = sig - int(math.floor(math.log10(abs(bal)))) - 1
        key = round(bal, digits)
        buckets.setdefault(key, []).append(_norm(addr))
    return [members for members in buckets.values() if len(members) >= min_members]


def batch_funder_flags(
    addresses: Iterable[str], funders: dict[str, str],
    min_batch: int = 5, exclude: set[str] | None = None,
) -> dict[str, dict]:
    """Per-address batch-funder coordination signal (ported from bw_flag).

    For each address returns {funder_wallet_count, has_batch_funder}:
      - funder_wallet_count: how many in-scope wallets share this address's
        non-CEX root funder (siblings incl. self).
      - has_batch_funder: 1 if that count >= min_batch (the validated coordination
        flag — a funder that batch-funded >= min_batch wallets is an operator).
    """
    if exclude is None:
        exclude = _exchange_addresses()
    exclude = {a.lower() for a in exclude}
    addrs = [_norm(a) for a in addresses]

    by_root: dict[str, list[str]] = {}
    addr_root: dict[str, str] = {}
    for a in addrs:
        if a in exclude:
            continue
        root = _root_funder(a, funders, exclude)
        if root and root not in exclude:
            by_root.setdefault(root, []).append(a)
            addr_root[a] = root

    out: dict[str, dict] = {}
    for a in addrs:
        root = addr_root.get(a)
        count = len(by_root.get(root, [])) if root else 0
        out[a] = {"funder_wallet_count": count,
                  "has_batch_funder": 1 if count >= min_batch else 0}
    return out


def cluster_addresses(
    addresses: Iterable[str],
    funders: dict[str, str] | None = None,
    co_buy_groups: list[list[str]] | None = None,
    exclude: set[str] | None = None,
    balances: dict[str, float] | None = None,
    entity_map: dict[str, str] | None = None,
    min_batch_funder: int = 1,
) -> dict[str, str]:
    """Cluster addresses into entities via union-find over edges.

    Edge types (each a "same entity" signal), strongest first:
      0. Arkham entity — ground-truth address→entity (when an API key is set).
      1. Batch funder — addresses sharing a common non-CEX funder (chain-aware
         root). `min_batch_funder` is the validated coordination threshold: a
         funder must have funded >= this many in-scope wallets to count as one
         entity (ported from pre-airdrop-detection's bw_flag, validated at 5).
      2. Temporal co-acquisition — addresses that received the token together.
      3. Similar balance — CORROBORATOR ONLY. Near-identical balances confirm a
         funder-proposed link; they never create one on their own (an equal-tier
         airdrop otherwise fabricates a single large "entity" out of retail).
    CEX/MM addresses (`exclude`) are kept as singletons — custodial noise.
    """
    if exclude is None:
        exclude = _exchange_addresses()
    exclude = {a.lower() for a in exclude}

    addrs = [_norm(a) for a in addresses]
    uf = _UnionFind()
    for a in addrs:
        uf.find(a)  # ensure present

    # Edge type 0: Arkham ground-truth entity (strongest). Addresses sharing an
    # Arkham entity merge directly.
    if entity_map:
        by_entity: dict[str, list[str]] = {}
        for addr, ent in entity_map.items():
            al = _norm(addr)
            if al in exclude or not ent:
                continue
            by_entity.setdefault(str(ent), []).append(al)
        for group in by_entity.values():
            for other in group[1:]:
                uf.union(group[0], other)

    # Edge type 1: shared root funder (chain-aware, skip excluded addresses).
    if funders:
        by_root: dict[str, list[str]] = {}
        for addr in funders:
            al = _norm(addr)
            if al in exclude:
                continue
            root = _root_funder(addr, funders, exclude)
            if root and root not in exclude:
                by_root.setdefault(root, []).append(al)
        for group in by_root.values():
            # Validated rule: only treat a shared funder as an entity link if it
            # batch-funded >= min_batch_funder wallets (avoids coincidental pairs).
            if len(group) < min_batch_funder:
                continue
            for other in group[1:]:
                uf.union(group[0], other)

    # Edge type 2: temporal co-acquisition.
    if co_buy_groups:
        for group in co_buy_groups:
            members = [_norm(a) for a in group if _norm(a) not in exclude]
            for other in members[1:]:
                uf.union(members[0], other)

    # Edge type 3: similar-balance split positions — CORROBORATOR ONLY.
    #
    # This edge used to union on balance alone. An equal-tier airdrop or a staking
    # program pays N wallets the identical amount, so 40 unrelated retail addresses
    # fused into one fabricated 20% "entity" → cluster_confidence 83 → a top-of-tree
    # `live_operator` verdict built on nothing. No downstream fix can undo it: the
    # signal arrives pre-merged.
    #
    # A shared balance is now only allowed to CONFIRM a link that a shared root funder
    # already proposes — it can no longer create one. Without funder data the edge is
    # silently dropped (unknown ≠ same entity).
    if balances:
        for group in _similar_balance_groups(
            {a: b for a, b in balances.items() if _norm(a) not in exclude}
        ):
            if not funders:
                continue           # no corroboration available → refuse to merge
            by_root_bal: dict[str, list[str]] = {}
            for a in group:
                root = _root_funder(a, funders, exclude)
                if root and root not in exclude:
                    by_root_bal.setdefault(root, []).append(a)
            for same_root in by_root_bal.values():
                if len(same_root) < 2:
                    continue       # equal balance + a funder nobody else shares = noise
                for other in same_root[1:]:
                    uf.union(same_root[0], other)

    return {a: uf.find(a) for a in addrs if a not in exclude}


def effective_concentration(
    holders: list[dict[str, Any]],
    funders: dict[str, str] | None = None,
    co_buy_groups: list[list[str]] | None = None,
    exclude: set[str] | None = None,
    top_n: int = 10,
    exclude_share_above: float | None = None,
    entity_map: dict[str, str] | None = None,
    min_batch_funder: int = 1,
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
        {"address": _norm(h["address"]), "balance": float(h.get("balance", 0) or 0)}
        for h in holders
        if float(h.get("balance", 0) or 0) > 0
    ]
    # Drop pool/treasury accounts: on an established, distributed token no single
    # REAL holder holds an outsized share — that account is the AMM vault / curve.
    if exclude_share_above is not None and pos:
        gross = sum(h["balance"] for h in pos)
        if gross > 0:
            pos = [h for h in pos if h["balance"] / gross <= exclude_share_above]
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

    # Effective: cluster (incl. similar-balance split detection), then top-N.
    bal_map = {h["address"]: h["balance"] for h in pos}
    mapping = cluster_addresses(
        (h["address"] for h in pos), funders, co_buy_groups, exclude,
        balances=bal_map, entity_map=entity_map, min_batch_funder=min_batch_funder,
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
