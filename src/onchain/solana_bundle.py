"""Solana bundle / same-entity edges — the cleanest 庄 signal Solana offers.

On EVM the strongest "same actor" links we can cheaply build are *funder* and
mempool-timing inference (noisy). Solana hands us something far sharper:

  1. SAME-SLOT / SAME-BUNDLE CO-BUYS. Jito bundles execute up to 5 transactions
     atomically inside one slot. A launch operator ("bundler"/"sniper") fires a
     bundle that buys a large share of supply across several fresh wallets in the
     SAME slot — cryptographically guaranteed to land together, so same-slot
     co-buyers near launch are near-deterministically ONE entity. This catches
     dev-controlled wallets even after they disperse balances to dodge holder
     maps (the holder snapshot sees N small wallets; the bundle edge sees they
     bought as one).
  2. SHARED feePayer / ATA-rent payer. Fresh wallets have to be created and their
     associated-token-accounts rent-funded. The payer of a wallet's earliest
     transaction (account creation / first funding) is a strong same-entity link,
     the Solana analogue of EVM first-funder.

WHAT IS RELIABLY FEASIBLE ON THE FREE HELIUS TIER (and what is bounded):
  - We CAN parse swaps via the Helius Enhanced Transactions API (typed SWAP feed,
    already decoded: slot, feePayer, tokenTransfers, nativeTransfers) and the
    earliest-tx feePayer per wallet. Both are used below.
  - We do NOT reconstruct full Jito bundles (that needs the block's ordered tx
    list + a tip transfer to a Jito tip account to prove atomicity). Instead we
    use the practical proxy: same-slot co-buy, ELEVATED to a proven `jito_bundle`
    edge only when a Jito tip transfer is observed in that slot's transactions.
  - The Enhanced API pages newest->oldest. For a FRESH token launch is reached in
    a page or two; for an established/hot token the default analyzes the most
    recent window (still surfaces same-slot co-buy clusters, just not at t=0).
    Pass `before` + raise `max_pages` to walk back toward launch. All call counts
    are bounded so a scan never crawls.

Output is confidence-bearing edges:
    {"cluster_wallets": [...], "edge_type": "same_slot_cobuy"|"jito_bundle"|
     "shared_feepayer", "confidence": float, "detail": str, ...}

Best-effort throughout: every network path is guarded; functions return empty
results (never raise) on failure, matching holder_snapshot / funder_graph.
"""

from __future__ import annotations

import json
import os
import urllib.request
from collections import defaultdict
from typing import Any

import structlog

import src.config  # noqa: F401  — side-effect: loads .env (HELIUS_API_KEY, RPC)

logger = structlog.get_logger()

# Canonical Jito tip accounts. A native-SOL transfer to any of these inside a tx
# proves that tx was submitted as part of a Jito bundle → same-slot co-buyers that
# also tip Jito are atomic-by-construction = one entity (highest confidence).
JITO_TIP_ACCOUNTS = frozenset({
    "96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5",
    "HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe",
    "Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY",
    "ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49",
    "DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh",
    "ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt",
    "DttWaMuVvTiduytNA2dyDsvDLE7VuaA38m6APFhXVRLB",
    "3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT",
})

_UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
       "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")

# --------------------------------------------------------------------------
# Low-level helpers
# --------------------------------------------------------------------------


def _helius_key() -> str:
    """Helius API key, or "" if absent (callers degrade to empty results)."""
    return os.environ.get("HELIUS_API_KEY", "")


def _rpc_url() -> str:
    return os.environ.get("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")


def _rpc(method: str, params: list, timeout: int = 20) -> dict:
    """JSON-RPC call against the (Helius) Solana RPC endpoint."""
    url = _rpc_url()
    payload = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
    req = urllib.request.Request(
        url, data=payload.encode(),
        headers={"Content-Type": "application/json", "User-Agent": _UA},
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


def _enhanced_address_txns(
    address: str, key: str, *, limit: int = 100, before: str | None = None,
    tx_type: str | None = None, timeout: int = 30,
) -> list[dict[str, Any]]:
    """Helius Enhanced Transactions for an address (already-parsed, newest-first).

    Returns [] on any failure. `before` is a signature cursor for older pages.
    """
    url = (f"https://api.helius.xyz/v0/addresses/{address}/transactions"
           f"?api-key={key}&limit={limit}")
    if tx_type:
        url += f"&type={tx_type}"
    if before:
        url += f"&before={before}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": _UA})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode())
        return data if isinstance(data, list) else []
    except Exception as e:
        logger.debug("helius_enhanced_failed", address=address, error=str(e))
        return []


def _enhanced_parse_sigs(
    signatures: list[str], key: str, timeout: int = 30
) -> list[dict[str, Any]]:
    """Batch-parse up to 100 signatures via Helius POST /v0/transactions. []-safe."""
    out: list[dict[str, Any]] = []
    url = f"https://api.helius.xyz/v0/transactions?api-key={key}"
    for i in range(0, len(signatures), 100):
        chunk = signatures[i:i + 100]
        try:
            req = urllib.request.Request(
                url, data=json.dumps({"transactions": chunk}).encode(),
                headers={"Content-Type": "application/json", "User-Agent": _UA},
            )
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                data = json.loads(resp.read().decode())
            if isinstance(data, list):
                out.extend(data)
        except Exception as e:
            logger.debug("helius_parse_sigs_failed", error=str(e))
    return out


# --------------------------------------------------------------------------
# Pure parsing logic (no I/O — unit-testable)
# --------------------------------------------------------------------------


def _net_buyer(tx: dict[str, Any], mint: str) -> str | None:
    """Owner who NET-acquired `mint` in this tx (the buyer), or None.

    Sums tokenTransfers of the target mint per owner (received +, sent -). The
    owner with the largest positive net is the buyer; pools/sellers net negative.
    Using net token flow — not feePayer — is essential because Solana swaps are
    frequently gasless (feePayer is a relayer, not the buyer).
    """
    net: dict[str, float] = defaultdict(float)
    for tt in tx.get("tokenTransfers", []) or []:
        if tt.get("mint") != mint:
            continue
        amt = tt.get("tokenAmount") or 0
        try:
            amt = float(amt)
        except (TypeError, ValueError):
            continue
        to = tt.get("toUserAccount")
        frm = tt.get("fromUserAccount")
        if to:
            net[to] += amt
        if frm:
            net[frm] -= amt
    net.pop(None, None)  # type: ignore[arg-type]
    if not net:
        return None
    owner, amount = max(net.items(), key=lambda kv: kv[1])
    return owner if amount > 0 else None


def _has_jito_tip(tx: dict[str, Any]) -> bool:
    """True if the tx contains a native-SOL transfer to a Jito tip account."""
    for nt in tx.get("nativeTransfers", []) or []:
        if nt.get("toUserAccount") in JITO_TIP_ACCOUNTS:
            return True
    return False


# --------------------------------------------------------------------------
# Public API
# --------------------------------------------------------------------------


def same_slot_cobuyers(
    mint: str, *, max_pages: int = 3, page_limit: int = 100,
    before: str | None = None, max_cluster: int = 25,
) -> list[dict[str, Any]]:
    """Find wallets that bought `mint` in the SAME slot — a strong same-entity edge.

    Pulls the Helius typed-SWAP feed for the mint (newest-first, `max_pages` pages
    of `page_limit`), derives each swap's buyer via net token flow, groups buyers
    by slot, and emits an edge for every slot with >=2 DISTINCT buyers. A slot in
    which any tx tips a Jito tip account is upgraded to a proven `jito_bundle`
    edge (atomic-by-construction → highest confidence); otherwise it is a
    `same_slot_cobuy` edge (strong but not atomicity-proven).

    To target LAUNCH on a hot token, pass `before` (a signature) and raise
    `max_pages` to walk older; on a fresh token the first page already covers
    launch. Bounded to `max_pages` API calls so a scan never crawls.

    Returns edges sorted by slot ascending; [] on no key / no clusters / failure.
    """
    key = _helius_key()
    if not key or not mint:
        logger.debug("same_slot_cobuyers_skipped", reason="no_key_or_mint")
        return []

    # slot -> {buyer -> {"sigs": set, "tip": bool}}
    slot_buyers: dict[int, dict[str, dict[str, Any]]] = defaultdict(dict)
    cursor = before
    pages = 0
    try:
        while pages < max_pages:
            txns = _enhanced_address_txns(
                mint, key, limit=page_limit, before=cursor, tx_type="SWAP")
            if not txns:
                break
            for tx in txns:
                slot = tx.get("slot")
                if slot is None:
                    continue
                buyer = _net_buyer(tx, mint)
                if not buyer:
                    continue
                tip = _has_jito_tip(tx)
                rec = slot_buyers[slot].setdefault(buyer, {"sigs": set(), "tip": False})
                if tx.get("signature"):
                    rec["sigs"].add(tx["signature"])
                rec["tip"] = rec["tip"] or tip
            cursor = txns[-1].get("signature")
            pages += 1
            if len(txns) < page_limit:
                break  # reached the oldest available page (launch)
    except Exception as e:
        logger.warning("same_slot_cobuyers_failed", mint=mint, error=str(e))

    edges: list[dict[str, Any]] = []
    for slot in sorted(slot_buyers):
        buyers = slot_buyers[slot]
        if len(buyers) < 2:
            continue
        wallets = list(buyers)[:max_cluster]
        sigs: list[str] = []
        for w in wallets:
            sigs.extend(buyers[w]["sigs"])
        slot_tipped = any(buyers[w]["tip"] for w in wallets)
        if slot_tipped:
            edge_type = "jito_bundle"
            # Atomic-by-construction: a Jito tip in the slot proves bundling.
            confidence = round(min(0.95, 0.85 + 0.02 * len(wallets)), 3)
            detail = (f"{len(buyers)} distinct buyers in slot {slot}; Jito tip "
                      f"observed → bundle atomicity proven")
        else:
            edge_type = "same_slot_cobuy"
            # Same slot but atomicity unproven: a slot can hold independent txns,
            # so this is strong-but-not-certain. More co-buyers => more confident.
            confidence = round(min(0.8, 0.5 + 0.06 * (len(buyers) - 1)), 3)
            detail = (f"{len(buyers)} distinct buyers co-bought in slot {slot} "
                      f"(no Jito tip seen; atomicity not proven)")
        edges.append({
            "cluster_wallets": wallets,
            "edge_type": edge_type,
            "confidence": confidence,
            "detail": detail,
            "slot": slot,
            "signatures": sigs[:max_cluster],
        })
    logger.info("same_slot_cobuyers", mint=mint, pages=pages,
                slots=len(slot_buyers), edges=len(edges))
    return edges


def _earliest_fee_payer(
    wallet: str, key: str, *, max_pages: int = 2, timeout: int = 20
) -> str | None:
    """feePayer of `wallet`'s EARLIEST transaction (its creation / first funding).

    Paginates getSignaturesForAddress to the oldest signature (fresh wallets have
    few txns, so 1-2 pages reach the true oldest), then parses that tx's feePayer
    via the Helius enhanced parser. The earliest-tx payer is the rent/funding
    payer — the Solana analogue of EVM first-funder. None on failure.
    """
    oldest = None
    before = None
    try:
        for _ in range(max_pages):
            params: list = [wallet, {"limit": 1000}]
            if before:
                params[1]["before"] = before
            sigs = _rpc("getSignaturesForAddress", params, timeout).get("result", [])
            if not sigs:
                break
            oldest = sigs[-1].get("signature")
            if len(sigs) < 1000:
                break  # reached the oldest page
            before = oldest
        if not oldest:
            return None
        parsed = _enhanced_parse_sigs([oldest], key, timeout)
        if parsed:
            fp = parsed[0].get("feePayer")
            return fp or None
    except Exception as e:
        logger.debug("earliest_fee_payer_failed", wallet=wallet, error=str(e))
    return None


def shared_fee_payer(
    wallets: list[str], *, max_wallets: int = 30, max_pages: int = 2,
) -> list[dict[str, Any]]:
    """Cluster `wallets` by the payer of their earliest (creation/funding) tx.

    Wallets whose first transaction was paid by the SAME feePayer were created /
    rent-funded by one actor = one entity. Returns a `shared_feepayer` edge per
    payer that links >=2 wallets. Bounded to `max_wallets` lookups (each 1-2 RPC
    pages + one parse). [] on no key / no shared payer / failure.

    Caveat (documented limitation): a few wallets in a swap-derived set may share
    a *gasless relayer* as feePayer rather than a real funder; that inflates a
    cluster. Pair this with `same_slot_cobuyers` (net-flow based, relayer-immune)
    rather than trusting feePayer clusters alone.
    """
    key = _helius_key()
    seen: set[str] = set()
    uniq = [w for w in wallets if w and not (w in seen or seen.add(w))]
    if not key or not uniq:
        logger.debug("shared_fee_payer_skipped", reason="no_key_or_wallets")
        return []

    payer_to_wallets: dict[str, list[str]] = defaultdict(list)
    for wallet in uniq[:max_wallets]:
        fp = _earliest_fee_payer(wallet, key, max_pages=max_pages)
        if fp and fp != wallet:
            payer_to_wallets[fp].append(wallet)

    edges: list[dict[str, Any]] = []
    for payer, members in payer_to_wallets.items():
        if len(members) < 2:
            continue
        # More wallets funded by one payer => stronger entity evidence.
        confidence = round(min(0.92, 0.75 + 0.04 * (len(members) - 1)), 3)
        edges.append({
            "cluster_wallets": members,
            "edge_type": "shared_feepayer",
            "confidence": confidence,
            "detail": (f"{len(members)} wallets' earliest tx paid by {payer} "
                       f"(shared creation/rent payer)"),
            "fee_payer": payer,
        })
    logger.info("shared_fee_payer", requested=len(uniq),
                looked_up=min(len(uniq), max_wallets), edges=len(edges))
    return edges


def bundle_clusters(
    mint: str, *, max_pages: int = 3, page_limit: int = 100,
    before: str | None = None, feepayer_check: bool = True,
    max_feepayer_wallets: int = 30,
) -> dict[str, Any]:
    """End-to-end same-entity edge map for a Solana mint.

    1. `same_slot_cobuyers` — same-slot / Jito-bundle co-buy edges.
    2. (optional) `shared_fee_payer` over the unique co-buyer wallets — links
       wallets created by one payer, catching dev clusters that bought in
       DIFFERENT slots but were funded by one hand.

    Returns:
        {"mint", "edges": [...all edges, confidence-bearing...],
         "cobuy_edges", "feepayer_edges", "n_cobuy_wallets", "summary"}
    Never raises; empty fields on failure / no Helius key.
    """
    cobuy = same_slot_cobuyers(
        mint, max_pages=max_pages, page_limit=page_limit, before=before)

    cobuy_wallets: list[str] = []
    seen: set[str] = set()
    for e in cobuy:
        for w in e["cluster_wallets"]:
            if w not in seen:
                seen.add(w)
                cobuy_wallets.append(w)

    feepayer: list[dict[str, Any]] = []
    if feepayer_check and cobuy_wallets:
        feepayer = shared_fee_payer(cobuy_wallets, max_wallets=max_feepayer_wallets)

    edges = cobuy + feepayer
    summary = (f"{len(cobuy)} same-slot/bundle co-buy edge(s), "
               f"{len(feepayer)} shared-feePayer edge(s) over "
               f"{len(cobuy_wallets)} co-buyer wallet(s)")
    logger.info("bundle_clusters", mint=mint, cobuy=len(cobuy),
                feepayer=len(feepayer), cobuy_wallets=len(cobuy_wallets))
    return {
        "mint": mint,
        "edges": edges,
        "cobuy_edges": cobuy,
        "feepayer_edges": feepayer,
        "n_cobuy_wallets": len(cobuy_wallets),
        "summary": summary,
    }
