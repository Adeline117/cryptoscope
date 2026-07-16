"""Perp-universe signal scanner — runs the tradeable dump/accumulation signals over
the shortable/longable coin set (perp_universe), so every hit is something you can
actually act on (long or short with leverage).

⚠️ scan_unlocks IS NOT TRUSTWORTHY — DO NOT WIRE TO ALERTS. Verified 2026-07:
the free DefiLlama unlock layer (via catalyst_feed) FABRICATES schedules for
loosely-matched protocols — MAGIC/EDEN/AI on three different chains all report an
identical 8.9% unlock, GRASS 105%, etc. Filtering can't fix a source that lies; it
only makes the garbage look plausible (more dangerous). A real unlock signal needs
a paid feed (Token Unlocks / CryptoRank). The scan framework (universe iteration +
sanity gates) is kept for reuse; the unlock signal itself is parked.

The reliable dump precursor — top-holder → CEX deposit — runs on infrastructure we
control and cross-checked exactly (Dune holder reconstruction + cex_flow), NOT on a
third-party feed. That is the next scanner (scan_cex_deposits, forthcoming).
"""

from __future__ import annotations

import re

import structlog

logger = structlog.get_logger()

_VERIFIED_SYMBOL = re.compile(r"^[A-Z0-9]{1,32}$")
_VERIFIED_CHAINS = frozenset({
    "ethereum", "bsc", "solana", "base", "arbitrum", "optimism",
    "polygon", "avalanche",
})
_EVM_VERIFIED_CHAINS = _VERIFIED_CHAINS - {"solana"}
_EVM_ADDRESS = re.compile(r"^0x[0-9a-fA-F]{40}$")
_SOLANA_ADDRESS = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")
_BASE58_INDEX = {
    char: index
    for index, char in enumerate(
        "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
    )
}
_MAX_VERIFIED_UNIVERSE_ROWS = 20_000


def _verified_address_matches_chain(chain: object, address: object) -> bool:
    if not isinstance(address, str):
        return False
    if chain in _EVM_VERIFIED_CHAINS:
        return _EVM_ADDRESS.fullmatch(address) is not None
    if chain != "solana" or _SOLANA_ADDRESS.fullmatch(address) is None:
        return False

    # Base58 leading ``1`` characters encode zero bytes.  Counting those plus the
    # minimal byte length of the remaining integer avoids accepting a merely
    # well-shaped string that is not a 32-byte Solana public key.
    value = 0
    for char in address:
        value = value * 58 + _BASE58_INDEX[char]
    leading_zero_bytes = len(address) - len(address.lstrip("1"))
    decoded_bytes = leading_zero_bytes + (value.bit_length() + 7) // 8
    return decoded_bytes == 32


def validated_verified_universe(value: object) -> dict | None:
    """Return a defensive copy only when every actionable row is verified.

    ``None`` means the whole envelope is malformed.  Callers must fail closed rather
    than silently dropping a bad row and scanning the remainder: partial acceptance
    would make a forged asset identity indistinguishable from a verified one.
    """
    if (
        not isinstance(value, dict)
        or len(value) > _MAX_VERIFIED_UNIVERSE_ROWS
    ):
        return None
    verified: dict[str, dict] = {}
    for symbol, row in value.items():
        if (
            not isinstance(symbol, str)
            or _VERIFIED_SYMBOL.fullmatch(symbol) is None
            or not isinstance(row, dict)
            or row.get("actionability") != "verified"
            or row.get("chain") not in _VERIFIED_CHAINS
            or not _verified_address_matches_chain(
                row.get("chain"), row.get("address"),
            )
        ):
            return None
        verified[symbol] = dict(row)
    return verified


def scan_unlocks(within_days: int = 30, limit: int | None = None) -> list[dict]:
    """Scan the perp universe for near-term token unlocks. Returns a list of
    short candidates, soonest/biggest first:
      [{symbol, chain, address, days_until, pct_of_max_supply, usd, severity, detail}]
    Only coins with a MATERIAL upcoming unlock are returned."""
    from src.onchain.catalyst_feed import catalyst_for
    from src.onchain.perp_universe import load

    universe = load()
    if not universe:
        logger.warning("perp_scan_no_universe", note="run perp_universe.refresh() first")
        return []

    items = list(universe.items())
    if limit:
        items = items[:limit]

    hits: list[dict] = []
    for symbol, rec in items:
        chain, addr = rec["chain"], rec["address"]
        try:
            cat = catalyst_for(addr, chain, symbol=symbol)
        except Exception as e:
            logger.debug("catalyst_failed", symbol=symbol, error=str(e)[:60])
            continue
        if "unlock" not in cat.get("kinds", []):
            continue
        unlocks = [u for u in cat.get("unlocks", []) if u.get("days_until", 999) <= within_days]
        # Sanity guard. The DefiLlama layer FABRICATES events for loosely-matched
        # protocols (verified: EDEN & MAGIC both emit an identical 69.6%, GRASS 105%,
        # all with noOfTokens=None) — so a single-event share above what any real
        # cliff reaches (>35% of supply) is a data error, dropped. 15-35% is flagged
        # suspect (verify manually before trading). This keeps only plausible cliffs.
        unlocks = [u for u in unlocks
                   if u.get("pct_of_max_supply") is not None
                   and 0 < u["pct_of_max_supply"] <= 0.35]
        if not unlocks:
            continue
        nxt = unlocks[0]
        suspect = (nxt.get("pct_of_max_supply") or 0) > 0.15
        days = nxt.get("days_until")
        hits.append({
            "symbol": symbol, "chain": chain, "address": addr,
            "days_until": days,
            "pct_of_max_supply": nxt.get("pct_of_max_supply"),
            "usd": nxt.get("usd"),
            "severity": "imminent" if (days is not None and days <= 7) else "near",
            "suspect": suspect,
            "detail": cat.get("detail", ""),
        })

    def _rank(h):
        # soonest + biggest first
        return (h["days_until"] if h["days_until"] is not None else 999,
                -(h["pct_of_max_supply"] or 0))

    hits.sort(key=_rank)
    return hits


def scan_cex_deposits(chains: tuple[str, ...] = ("ethereum", "bsc", "base", "arbitrum"),
                      top_n: int = 10, min_share: float = 2.0,
                      limit: int | None = None, *,
                      verified_universe: dict | None = None) -> list[dict]:
    """Dump precursor on infrastructure WE control: for each perp coin, take its top
    non-exchange non-contract holders (team/treasury/whales) and check whether they
    are moving tokens TO a CEX deposit address (cex_flow). A confirmed
    big-holder → CEX deposit precedes the sell = short candidate.

    EVM only (cex_flow's Solana path is separate). Returns hits soonest-strongest:
      [{symbol, chain, address, holder, pct, cex_outflow, pct_of_holder, detail}]
    """
    from src.onchain.cex_addresses import evm_exchanges
    from src.onchain.cex_flow import cex_outflow_signal
    from src.onchain.entity_classify import classify_address
    from src.onchain.holder_snapshot import fetch_holders_evm
    from src.onchain.perp_universe import load

    _CHAIN_ID = {"ethereum": 1, "bsc": 56, "base": 8453, "arbitrum": 42161}
    candidate_universe = load() if verified_universe is None else verified_universe
    source_universe = validated_verified_universe(candidate_universe)
    if source_universe is None:
        logger.warning(
            "perp_cex_scan_universe_invalid",
            reason_code="verified_universe_contract_invalid",
        )
        return []
    universe = [(s, r) for s, r in source_universe.items() if r["chain"] in chains]
    if limit:
        universe = universe[:limit]
    cex = evm_exchanges()
    hits: list[dict] = []

    for symbol, rec in universe:
        chain, addr = rec["chain"], rec["address"]
        cid = _CHAIN_ID.get(chain)
        try:
            holders = fetch_holders_evm(addr, chain_id=cid, max_pages=3) or []
        except Exception:
            continue
        if not holders:
            continue
        holders.sort(key=lambda h: -float(h.get("balance", 0) or 0))
        supply = sum(float(h.get("balance", 0) or 0) for h in holders[:50]) or 1
        # significant NON-exchange, NON-contract holders = the movers worth watching
        watch = []
        for h in holders[:top_n]:
            a = str(h.get("address", "")).lower()
            bal = float(h.get("balance", 0) or 0)
            share = bal / supply * 100
            if a in cex or share < min_share:
                continue
            if classify_address(a, chain).get("type") in ("eoa", "multisig"):
                watch.append((a, share))
        if not watch:
            continue
        cxf = cex_outflow_signal(addr, chain, [a for a, _ in watch])
        if cxf.get("has_signal") and cxf.get("complete"):
            hits.append({
                "symbol": symbol, "chain": chain, "address": addr,
                "watched_holders": len(watch),
                "cex_outflow": cxf.get("cex_outflow"),
                "pct_of_cluster": cxf.get("pct_of_cluster"),
                "detail": cxf.get("detail", ""),
            })

    hits.sort(key=lambda h: -(h.get("pct_of_cluster") or 0))
    return hits


def scan_accumulation(chains: tuple[str, ...] = ("ethereum", "bsc", "base", "arbitrum"),
                      min_confidence: int = 55, limit: int | None = None) -> list[dict]:
    """Accumulation-anomaly (LONG / 埋伏) scan over the perp universe. For each coin,
    run effective_concentration_signal — the hidden-coordinated-holding detector
    (CEX/contracts already excluded, cluster_confidence validated 27/27) — and flag
    coins where a coordinated entity holds a meaningful, high-confidence share = a
    'someone is quietly building here' setup worth a small patient position.

    ⚠️ PARKED — DO NOT WIRE TO ALERTS on the perp universe. Verified 2026-07: on
    large CEX-listed coins, high concentration = the ISSUER/foundation/staking, not
    a hidden accumulator. Top hit was OKB at 93% (that's OKX holding its own exchange
    token), SSV/AT = foundation holdings. These are structural, not "someone building
    to pump" — false positives for a long setup. The concentration detector that
    finds micro-cap operators finds issuers on large caps. Confirms the original
    caution: on-chain structure does not predict pumps on established coins. Kept for
    reuse (e.g. a filtered small-cap sub-universe with issuer labels) but not run.

    Returns [{symbol, chain, address, largest_entity_pct, concentration_gap,
    cluster_confidence, wallets}] by confidence desc."""
    from src.onchain.holder_snapshot import fetch_holders_evm
    from src.onchain.perp_universe import load
    from src.pipeline.anomaly_screener import effective_concentration_signal

    _CHAIN_ID = {"ethereum": 1, "bsc": 56, "base": 8453, "arbitrum": 42161}
    universe = [(s, r) for s, r in load().items() if r["chain"] in chains]
    if limit:
        universe = universe[:limit]
    hits: list[dict] = []

    for symbol, rec in universe:
        chain, addr = rec["chain"], rec["address"]
        cid = _CHAIN_ID.get(chain)
        try:
            holders = fetch_holders_evm(addr, chain_id=cid, max_pages=4) or []
            if not holders:
                continue
            conc = effective_concentration_signal(holders, addr, chain)
        except Exception:
            continue
        if not conc:
            continue
        conf = conc.get("cluster_confidence") or 0
        if conf < min_confidence:
            continue
        hits.append({
            "symbol": symbol, "chain": chain, "address": addr,
            "largest_entity_pct": conc.get("largest_entity_pct"),
            "concentration_gap": conc.get("concentration_gap"),
            "cluster_confidence": conf,
            "wallets": conc.get("dominant_cluster_wallets") or [],
        })

    hits.sort(key=lambda h: -h["cluster_confidence"])
    return hits
