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

import structlog

logger = structlog.get_logger()


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
