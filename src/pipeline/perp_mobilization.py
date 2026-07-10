"""Widen the EVENT SURFACE on coins you can actually short.

The kill-line exposed the structural problem: every event we had accrued sat on a
BSC micro-cap with no perpetual future. An edge measured there cannot be monetised.
`scan_cex_deposits` is the only signal aimed at the perp universe, and its first
full run produced zero hits — so at that density the 120-event decision never
arrives.

This module adds two more MOMENT-shaped events on the same 190 shortable coins:

  1. mobilization  — a top holder gas-funds an ammo wallet, or approves a DEX router.
                     Selling requires both; neither is visible in a price chart.
  2. lp_unlock     — an LP position transitions locked -> unlocked, i.e. the pool can
                     now be pulled. The existing RUG alarm is post-mortem (liquidity
                     already -30%); this is the logistics step that precedes it.

Both are 戒备 (escalation) events, not entry signals: they show the CAPABILITY and the
PREPARATION, never the intent or the timing. They are recorded so the thesis can be
tested; they are not advice.

FAIL-TO-UNKNOWN, everywhere. A scan that did not complete reports `complete=False`
and contributes NO event. Reading a failed scan as "quiet" is how this codebase kept
turning missing data into confident conclusions.

    python -m src.pipeline.perp_mobilization --limit 20
"""

from __future__ import annotations

import argparse

import structlog

logger = structlog.get_logger()

_CID = {"ethereum": 1, "bsc": 56, "base": 8453, "arbitrum": 42161}
MIN_HOLDER_SHARE = 1.5      # % of supply — below this a holder can't move the price
TOP_N = 10


def _whales(token: str, chain: str) -> tuple[list[str], bool]:
    """Top non-CEX, non-contract holders with a material share.

    Returns (wallets, complete). complete=False when the holder list or the supply
    could not be read — the caller must then emit NO event for this coin rather than
    an empty one."""
    from src.onchain.cex_addresses import evm_exchanges
    from src.onchain.entity_classify import classify_address
    from src.onchain.evm_archive import ArchiveRPC
    from src.onchain.holder_snapshot import fetch_holders_evm
    try:
        supply = ArchiveRPC(chain).total_supply(token)
        if not supply:
            return [], False
        holders = fetch_holders_evm(token, chain_id=_CID[chain], max_pages=2) or []
        if not holders:
            return [], False
    except Exception as e:
        logger.debug("whales_failed", token=token, error=str(e)[:60])
        return [], False

    cex = evm_exchanges()
    out = []
    for h in holders[:TOP_N]:
        a = str(h.get("address", "")).lower()
        bal = float(h.get("balance", 0) or 0)
        if a in cex or (bal / supply * 100) < MIN_HOLDER_SHARE:
            continue
        if classify_address(a, chain).get("type") in ("eoa", "multisig"):
            out.append(a)
    return out, True


def scan_mobilization(limit: int | None = None,
                      chains: tuple[str, ...] = ("ethereum", "bsc", "base", "arbitrum"),
                      prev_state: dict | None = None) -> tuple[list[dict], dict]:
    """Router approvals + gas top-ups by perp-coin whales.

    `prev_state` carries per-coin cursors ({coin_key: {mobil_block, native_bal}}); the
    first pass ARMS the gas baseline and emits no gas event, exactly as the sentinel
    does. Returns (events, new_state).
    """
    from src.onchain.mobilization import approval_scan, gas_topup_scan
    from src.onchain.perp_universe import load as perp_load

    state = dict(prev_state or {})
    events: list[dict] = []
    universe = [(s, r) for s, r in sorted(perp_load().items())
                if r["chain"] in chains and r["chain"] in _CID]
    if limit:
        universe = universe[:limit]

    scanned = incomplete = 0
    for symbol, rec in universe:
        chain, token = rec["chain"], rec["address"]
        key = f"{chain}:{token}"
        wallets, complete = _whales(token, chain)
        if not complete:
            incomplete += 1
            continue                       # data failure != no event
        if not wallets:
            continue
        scanned += 1
        st = state.setdefault(key, {})

        ap = approval_scan(token, chain, wallets, st.get("mobil_block"))
        if ap.get("complete"):
            st["mobil_block"] = ap.get("to_block")
            routers = [a for a in ap["approvals"] if a["spender_kind"] == "router"]
            if routers:
                events.append({"symbol": symbol, "chain": chain, "address": token,
                               "kind": "授权路由", "n": len(routers),
                               "detail": f"{len(routers)}个大户授权DEX路由 → 卖出前置动作"})

        gs = gas_topup_scan(chain, wallets, st.get("native_bal"))
        if gs.get("armed"):
            st["native_bal"] = gs["balances"]
            if gs["topups"]:
                events.append({"symbol": symbol, "chain": chain, "address": token,
                               "kind": "注入gas", "n": len(gs["topups"]),
                               "detail": f"{len(gs['topups'])}个大户钱包被注入手续费 → 准备发交易"})

    logger.info("perp_mobilization_scanned", coins=scanned, incomplete=incomplete,
                events=len(events))
    return events, state


def scan_lp_unlock(limit: int | None = None,
                   prev_state: dict | None = None) -> tuple[list[dict], dict]:
    """LP positions transitioning locked -> unlocked (the pool CAN now be pulled).

    Only a transition is an event. A pool that has always been unlocked is a standing
    condition, not a moment — and a standing condition can never be early to anything
    (the same lesson the constant `distributing` verdict taught).
    """
    from src.onchain.goplus_client import token_security
    from src.onchain.perp_universe import load as perp_load

    state = dict(prev_state or {})
    events: list[dict] = []
    universe = [(s, r) for s, r in sorted(perp_load().items()) if r["chain"] in _CID]
    if limit:
        universe = universe[:limit]

    unchecked = 0
    for symbol, rec in universe:
        chain, token = rec["chain"], rec["address"]
        sec = token_security(token, chain)
        if not sec.get("available"):
            unchecked += 1
            continue                       # unchecked != safe, and != an event
        lp = sec["lp"]
        if lp["all_locked"] is None:
            unchecked += 1
            continue                       # no LP data → unknown, not "unlocked"
        key = f"{chain}:{token}"
        prev = state.get(key, {}).get("lp_all_locked")
        state.setdefault(key, {})["lp_all_locked"] = lp["all_locked"]
        # locked -> unlocked is the moment. None (first sight) arms, never fires.
        if prev is True and lp["all_locked"] is False:
            events.append({"symbol": symbol, "chain": chain, "address": token,
                           "kind": "LP解锁", "n": lp["n_holders_seen"] - lp["n_locked"],
                           "detail": f"LP 由锁定转为未锁定({lp['n_locked']}/"
                                     f"{lp['n_holders_seen']} 仍锁)→ 可撤池"})
    logger.info("perp_lp_scanned", unchecked=unchecked, events=len(events))
    return events, state


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=20)
    args = ap.parse_args()
    ev, _ = scan_mobilization(limit=args.limit)
    lp, _ = scan_lp_unlock(limit=args.limit)
    for e in ev + lp:
        print(f"{e['symbol']:10} [{e['chain']:8}] {e['kind']:8} {e['detail']}")
    print(f"\n{len(ev)} mobilization + {len(lp)} lp events")


if __name__ == "__main__":
    main()
