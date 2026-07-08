"""Canonical operator discovery — the ONE procedure to answer "is there a 庄, and
what is it doing" for any token. Built to fix a recurring methodological failure:
answering that question from the CURRENT holder snapshot alone, which is blind to
the most important case — an operator that accumulated cheap, ran the price, and
EXITED (they are simply not in the current holder graph anymore).

Two time dimensions, always both:
  1. CURRENT graph — hidden-Sybil clustering on the full live holder set
     (effective_concentration_signal): finds a LIVE loaded operator.
  2. HISTORICAL ledger — per-wallet cumulative INFLOW vs current NET holding over
     the token's whole life (Dune erc20 transfers): a wallet that took in a large
     amount but holds little now = accumulated-then-distributed = an EXITED operator
     fingerprint that the snapshot cannot see.

Output is a GRADED verdict with explicit unknowns — never a bare story like "dead"
or "loaded 庄". `historical.available=False` (e.g. Dune down) means the exited-
operator question is UNANSWERED, not "no operator".
"""

from __future__ import annotations

import structlog

logger = structlog.get_logger()

_ERC20 = {"bsc": "erc20_bnb", "ethereum": "erc20_ethereum", "base": "erc20_base"}
_BURN = {"0x0000000000000000000000000000000000000000",
         "0x000000000000000000000000000000000000dead",
         "0x0000000000000000000000000000000000000001"}


def _historical_ledger(token: str, chain: str, decimals: int) -> dict:
    """Per-wallet total-inflow vs net-now over full history (Dune). Flags wallets that
    took in a lot but hold little now = accumulated-then-distributed (exited operator).
    Returns {available, exited, holding} — available=False on Dune failure (UNKNOWN,
    not 'none')."""
    from src.onchain.cex_addresses import evm_exchanges
    from src.onchain.dune_client import available, run_sql

    table = _ERC20.get(chain)
    if not table or not available():
        return {"available": False, "exited": [], "holding": []}
    tok = token.lower()
    rows = run_sql(
        "with t as ("
        "select \"to\" as a, cast(value as double)/1e%d as amt, 1 as sgn "
        "from %s.evt_Transfer where contract_address = %s "
        "union all "
        "select \"from\", cast(value as double)/1e%d, -1 "
        "from %s.evt_Transfer where contract_address = %s) "
        "select a, sum(case when sgn=1 then amt else 0 end) as total_in, "
        "sum(sgn*amt) as net_now from t group by 1 "
        "order by total_in desc limit 60" % (decimals, table, tok, decimals, table, tok),
        poll_s=6, max_polls=50)
    if not rows:
        return {"available": False, "exited": [], "holding": []}   # Dune failed → UNKNOWN

    cex = evm_exchanges()
    exited, holding = [], []
    for r in rows:
        a = str(r.get("a", "")).lower()
        tin = float(r.get("total_in") or 0)
        net = float(r.get("net_now") or 0)
        if a in _BURN or a in cex or tin <= 0:
            continue
        dist_ratio = 1 - max(net, 0) / tin       # how much of what they took in is gone
        rec = {"address": a, "total_in": tin, "net_now": net, "distributed": round(dist_ratio, 2)}
        if dist_ratio >= 0.7 and tin > 0:        # took in a lot, dumped most → exited op
            exited.append(rec)
        elif net > 0 and dist_ratio < 0.3:        # took in a lot, still holds → live whale/op
            holding.append(rec)
    return {"available": True, "exited": exited[:15], "holding": holding[:15]}


def identify_operator(token: str, chain: str) -> dict:
    """The canonical verdict. Never raises. Shape:
      {verdict, confidence, current:{...}, historical:{...}, evidence, caveats}
    verdict ∈ live_operator | exited_operator | treasury | dispersed | unknown."""
    out = {"token": token, "chain": chain, "verdict": "unknown", "confidence": 0,
           "current": {}, "historical": {}, "evidence": "", "caveats": []}
    try:
        from src.onchain.holder_snapshot import fetch_holders_evm
        from src.pipeline.anomaly_screener import effective_concentration_signal
        cid = {"bsc": 56, "ethereum": 1, "base": 8453, "arbitrum": 42161}.get(chain)
        holders = fetch_holders_evm(token, chain_id=cid, max_pages=8) or []
        conc = effective_concentration_signal(holders, token, chain) or {}
        out["current"] = {
            "cluster_confidence": conc.get("cluster_confidence"),
            "largest_entity_pct": conc.get("largest_entity_pct"),
            "concentration_gap": conc.get("concentration_gap"),
            "dominant_wallets": len(conc.get("dominant_cluster_wallets") or []),
        }
    except Exception as e:
        out["caveats"].append(f"current-graph failed: {str(e)[:60]}")
        conc = {}

    try:
        from src.onchain.evm_archive import ArchiveRPC
        dec = ArchiveRPC(chain).token_decimals(token)
        out["historical"] = _historical_ledger(token, chain, dec)
    except Exception as e:
        out["historical"] = {"available": False, "exited": [], "holding": []}
        out["caveats"].append(f"historical failed: {str(e)[:60]}")

    conf = conc.get("cluster_confidence") or 0
    hist = out["historical"]
    exited = hist.get("exited") or []
    # Verdict logic — the whole point is NOT to collapse to a story the evidence
    # doesn't support. Current-snapshot silence + unavailable history = UNKNOWN.
    if conf >= 55:
        out["verdict"] = "live_operator"
        out["confidence"] = conf
        out["evidence"] = f"当前隐藏簇 cluster_confidence={conf}"
    elif hist.get("available") and exited:
        out["verdict"] = "exited_operator"
        out["confidence"] = min(90, 40 + 10 * len(exited))
        big = max(exited, key=lambda e: e["total_in"])
        out["evidence"] = (f"{len(exited)}个钱包大量吸入后已派发≥70%(最大吸入"
                           f"{big['total_in']:,.0f}, 现存比{1-big['distributed']:.0%})= 操盘已离场")
    elif not hist.get("available"):
        out["verdict"] = "unknown"
        out["evidence"] = ("当前无隐藏簇,但历史台账不可得(Dune不可用)→ "
                           "'拉盘前吸筹后离场'未验证,不能断言无庄")
        out["caveats"].append("历史维度缺失 = 结论未完成")
    else:
        # current dispersed AND history checked with no exited-accumulator
        lg = conc.get("largest_entity_pct") or 0
        out["verdict"] = "treasury" if lg >= 15 else "dispersed"
        out["confidence"] = 60
        out["evidence"] = (f"当前分散(最大{lg:.0f}%)且历史无'吸入后派发'的操盘足迹 → "
                           f"{'集中于金库/长持' if lg>=15 else '散户盘'},无操盘证据")
    return out
