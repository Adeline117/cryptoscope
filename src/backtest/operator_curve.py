"""Operator accumulation curve — the cleanest validation of the whole thesis.

Given the address cluster of a token's controlling entity (e.g. Arkham's
"SIREN控盘者" = 131 addresses) and the token mint, this reconstructs the
operator's COMBINED holding over time from on-chain data, and reports whether
their position rose (accumulation) and decelerated (nearing saturation) — i.e.
whether the accumulation actually preceded the launch.

This sidesteps full holder reconstruction: we only track a known set of
addresses, so it's cheap and exact. Arkham supplies the ground-truth cluster
(grabbed once, free, from the web); we supply the free on-chain replay.

Solana: replay the mint's txs (Helius) and, at checkpoints, sum the latest known
balance of the operator addresses vs the total observed supply.
"""

from __future__ import annotations

import json
import os
import urllib.request

import structlog

logger = structlog.get_logger()


def _sol_rpc(method: str, params: list, timeout: int = 25) -> dict:
    rpc = os.environ.get("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")
    req = urllib.request.Request(
        rpc, data=json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


def operator_curve_solana(
    mint: str, operator_addresses: list[str], n_points: int = 12,
    max_txs: int = 4000, max_sig_pages: int = 30,
) -> dict | None:
    """Reconstruct the operator cluster's combined holding share over time.

    Returns {
        share_series: [% of observed supply held by the operator at each point],
        operator_balance_series: [absolute combined balance],
        block_times: [unix ts per checkpoint],
        n_txs, n_operator_addresses, n_operator_seen
    } or None.
    """
    op = {a.strip() for a in operator_addresses if a and a.strip()}
    if not op or not mint:
        return None

    # Walk signatures oldest-first (cheap), take the early/accumulation window.
    all_sigs: list[dict] = []
    before = None
    for _ in range(max_sig_pages):
        params: list = [mint, {"limit": 1000}]
        if before:
            params[1]["before"] = before
        sigs = _sol_rpc("getSignaturesForAddress", params).get("result", [])
        if not sigs:
            break
        all_sigs.extend(sigs)
        before = sigs[-1].get("signature")
        if len(sigs) < 1000:
            break
    if len(all_sigs) < 20:
        return None

    early = list(reversed(all_sigs))[:max_txs]
    checkpoints = {int(len(early) / n_points * i) for i in range(1, n_points + 1)}

    balances: dict[str, float] = {}           # latest known balance per owner
    share_series, op_bal_series, block_times = [], [], []
    op_seen: set[str] = set()

    for idx, s in enumerate(early):
        sig = s.get("signature")
        if not sig:
            continue
        try:
            tx = _sol_rpc(
                "getTransaction",
                [sig, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}],
            ).get("result", {})
        except Exception:
            continue
        meta = (tx or {}).get("meta", {}) or {}
        for b in meta.get("postTokenBalances", []):
            if b.get("mint") != mint:
                continue
            owner = b.get("owner")
            amt = b.get("uiTokenAmount", {}).get("uiAmount")
            if owner and amt is not None:
                balances[owner] = float(amt)
                if owner in op:
                    op_seen.add(owner)
        if idx in checkpoints:
            # Observed supply = all non-pool holders (exclude >30% vault accounts).
            vals = [v for v in balances.values() if v > 0]
            gross = sum(vals)
            real = [v for v in vals if gross <= 0 or v / gross <= 0.30]
            total = sum(real) or 1.0
            op_bal = sum(balances.get(a, 0.0) for a in op)
            share_series.append(round(op_bal / total * 100, 4))
            op_bal_series.append(round(op_bal, 4))
            block_times.append(s.get("blockTime"))

    if len(share_series) < 4:
        return None
    return {
        "share_series": share_series,
        "operator_balance_series": op_bal_series,
        "block_times": block_times,
        "n_txs": len(early),
        "n_operator_addresses": len(op),
        "n_operator_seen": len(op_seen),
    }


def analyze_curve(curve: dict) -> dict:
    """Judge whether the operator curve shows accumulation that preceded launch.

    Looks at: did the operator's share rise, did the rise decelerate (saturation),
    and how high did it peak. Returns a verdict dict.
    """
    from src.signals.accumulation_divergence import _slope, is_decelerating

    share = curve.get("share_series") or []
    if len(share) < 4:
        return {"verdict": "insufficient", "detail": "too few points"}

    rose = _slope(share) > 0
    decel = is_decelerating(share)
    peak = max(share)
    start, end = share[0], share[-1]
    return {
        "verdict": "accumulation" if (rose and end > start) else "no_accumulation",
        "share_start_pct": round(start, 2),
        "share_peak_pct": round(peak, 2),
        "share_end_pct": round(end, 2),
        "rising": rose,
        "decelerating_saturation": decel,
        "operator_addresses_seen": f"{curve['n_operator_seen']}/{curve['n_operator_addresses']}",
        "slope": round(_slope(share), 3),
    }
