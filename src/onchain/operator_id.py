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


_TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
_ETHERSCAN_CHAINID = {"ethereum": 1}          # free Etherscan V2 = ETH only (BSC/Base need paid)


def _events_etherscan(token: str, chainid: int, decimals: int, max_pages: int = 60):
    """Full transfer history via Etherscan V2 getLogs (free = ETH only). Returns
    (inflow, net) per wallet, or (None, None) on failure. Fast (~1k logs/2s)."""
    import json
    import os
    import urllib.request
    from collections import defaultdict
    keys = [k.strip() for k in os.environ.get("ETHERSCAN_API_KEYS", "").split(",") if k.strip()]
    if not keys:
        return None, None
    inflow, net = defaultdict(float), defaultdict(float)
    frm, scale = 0, float(10 ** decimals)
    for p in range(max_pages):
        u = (f"https://api.etherscan.io/v2/api?chainid={chainid}&module=logs&action=getLogs"
             f"&address={token}&topic0={_TRANSFER_TOPIC}&fromBlock={frm}&toBlock=latest"
             f"&page=1&offset=1000&apikey={keys[p % len(keys)]}")
        try:
            r = json.loads(urllib.request.urlopen(
                urllib.request.Request(u, headers={"User-Agent": "Mozilla/5.0"}), timeout=25
            ).read().decode())
        except Exception:
            return (inflow, net) if inflow else (None, None)
        res = r.get("result")
        if not isinstance(res, list) or not res:
            break
        for lg in res:
            tp = lg.get("topics", [])
            if len(tp) < 3:
                continue
            a_from = "0x" + tp[1][-40:].lower()
            a_to = "0x" + tp[2][-40:].lower()
            try:
                amt = int(lg.get("data", "0x0"), 16) / scale
            except (ValueError, TypeError):
                continue
            inflow[a_to] += amt
            net[a_to] += amt
            net[a_from] -= amt
        if len(res) < 1000:
            break
        frm = int(res[-1]["blockNumber"], 16) + 1
    return inflow, net


def _early_inflow_moralis(token: str, chain: str, decimals: int, max_pages: int = 60):
    """EARLY-accumulation inflow per wallet via Moralis, walking OLDEST-first
    (order=ASC). A busy token's full history is too big to page newest-first (can't
    reach genesis) — but we don't need it: who accumulated in the token's EARLY life
    is the operator-accumulation window; whether they EXITED is then a cheap current-
    balance lookup (done by the caller). Returns {wallet: early_inflow} or None."""
    import time
    from collections import defaultdict

    from src.onchain import moralis_client
    mchain = {"bsc": "bsc", "base": "base", "ethereum": "eth"}.get(chain)
    if not moralis_client.usable() or not mchain:
        return None
    inflow: dict[str, float] = defaultdict(float)
    cursor, scale, got = None, float(10 ** decimals), False
    for pg in range(max_pages):
        if pg:
            time.sleep(0.25)
        path = (f"erc20/{token}/transfers?chain={mchain}&order=ASC&limit=100"
                + (f"&cursor={cursor}" if cursor else ""))
        d = moralis_client.get(path)
        if not d:
            break
        for t in (d.get("result", []) if isinstance(d, dict) else []):
            got = True
            try:
                amt = float(t.get("value", 0)) / scale
            except (ValueError, TypeError):
                continue
            a_to = (t.get("to_address") or "").lower()
            if a_to:
                inflow[a_to] += amt
        cursor = d.get("cursor") if isinstance(d, dict) else None
        if not cursor:
            break
    return inflow if got else None


def _historical_ledger(token: str, chain: str, decimals: int) -> dict:
    """Per-wallet total-inflow vs net-now over full history — DUNE-FREE. Flags wallets
    that took in a lot but hold ~nothing now = accumulated-then-distributed (exited
    operator), the fingerprint the current snapshot can't see. Source: Etherscan V2
    (ETH) / Moralis (BSC). Candidates are entity-FILTERED (a wallet that took a lot
    and dumped could be the LP/deployer/MM/CEX, not an operator). available=False on
    fetch failure → UNKNOWN, never 'none'."""
    tok = token.lower()
    cid = _ETHERSCAN_CHAINID.get(chain)
    inflow, net = (_events_etherscan(tok, cid, decimals) if cid else (None, None))

    if inflow is not None and net is not None:
        # ETH: full block-walk gives complete inflow AND net → use net directly.
        # Completeness guard: negative net among top holders = partial pull → refuse.
        ranked = sorted(inflow.items(), key=lambda kv: -kv[1])
        if [a for a, _ in ranked[:40] if net.get(a, 0) < -max(1.0, 1e-6 * (inflow.get(a) or 0))]:
            logger.warning("historical_ledger_incomplete", token=token, note="partial ETH window")
            return {"available": False, "exited": [], "holding": [], "incomplete": True}
        net_of = lambda a: net.get(a, 0.0)
    else:
        # BSC/other: early-accumulation window (oldest-first) + CURRENT balance to
        # decide exit — avoids needing the (too-large) full history.
        inflow = _early_inflow_moralis(tok, chain, decimals)
        if not inflow:
            return {"available": False, "exited": [], "holding": []}
        from src.onchain.evm_archive import ArchiveRPC
        rpc = ArchiveRPC(chain)
        ranked = sorted(inflow.items(), key=lambda kv: -kv[1])
        _bal_cache: dict[str, float] = {}

        def net_of(a):
            if a not in _bal_cache:
                b = rpc.balance_of(token, a)
                _bal_cache[a] = b if b is not None else 0.0
            return _bal_cache[a]

    from src.onchain.cex_addresses import evm_exchanges
    from src.onchain.entity_classify import classify_address
    cex = evm_exchanges()
    exited, holding = [], []
    for a, tin in ranked[:40]:
        if a in _BURN or a in cex or tin <= 0:
            continue
        if classify_address(a, chain).get("type") in ("contract",):   # LP/router/vesting
            continue
        nn = net_of(a)
        dist = 1 - max(nn, 0) / tin
        rec = {"address": a, "total_in": round(tin), "net_now": round(nn),
               "distributed": round(dist, 2)}
        if dist >= 0.7:
            exited.append(rec)
        elif nn > 0 and dist < 0.3:
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

    # Price context — the sell-vs-move discriminator. A mass SELL (real exit) sends
    # tokens INTO the LP pool → price craters. If wallets emptied but price is NOT
    # down, the tokens were MOVED wallet-to-wallet (rotation/obfuscation), NOT sold —
    # the operator likely still controls them via other addresses. Without this gate,
    # "balance→0" was wrongly read as "exited" (the EVAA catch: +660%, price intact,
    # yet 15 wallets 'fully distributed' — impossible if they'd actually sold).
    px_change = None
    try:
        from src.pipeline.operator_sentinel import _dex
        dx = _dex(token, chain)
        px_change = dx.get("h24")   # recent trend; a real mass exit shows big negative
    except Exception:
        pass
    out["current"]["price_change_24h"] = px_change

    conf = conc.get("cluster_confidence") or 0
    hist = out["historical"]
    exited = hist.get("exited") or []
    price_crashed = (px_change is not None and px_change <= -25)
    if conf >= 55:
        out["verdict"] = "live_operator"
        out["confidence"] = conf
        out["evidence"] = f"当前隐藏簇 cluster_confidence={conf}"
    elif hist.get("available") and exited and price_crashed:
        out["verdict"] = "exited_operator"
        out["confidence"] = min(90, 40 + 10 * len(exited))
        big = max(exited, key=lambda e: e["total_in"])
        out["evidence"] = (f"{len(exited)}个钱包吸入后派发≥70% 且价24h{px_change:.0f}% "
                           f"(最大吸入{big['total_in']:,.0f})= 操盘卖出离场")
    elif hist.get("available") and exited and not price_crashed:
        # accumulation gone from wallets BUT price intact = MOVED not sold
        out["verdict"] = "operator_present_rotating"
        out["confidence"] = min(85, 45 + 8 * len(exited))
        big = max(exited, key=lambda e: e["total_in"])
        out["evidence"] = (f"{len(exited)}个早期重仓钱包已清空(最大{big['total_in']:,.0f}) "
                           f"但价格未崩(24h{px_change}) → 币是转走非卖出 = 操盘换钱包/仍控盘,未离场")
        out["caveats"].append("需追转出去向确认:转LP/CEX=真卖,转EOA=换钱包(操盘仍在)")
    elif not hist.get("available"):
        out["verdict"] = "unknown"
        out["evidence"] = ("当前无隐藏簇,但历史台账取数失败(数据源额度耗尽,如Moralis每日/"
                           "Etherscan不覆盖该链)→ '拉盘前吸筹后离场'未验证,不能断言无庄")
        out["caveats"].append("历史维度缺失 = 结论未完成(换源/额度恢复后重跑)")
    else:
        # current dispersed AND history checked with no exited-accumulator
        lg = conc.get("largest_entity_pct") or 0
        out["verdict"] = "treasury" if lg >= 15 else "dispersed"
        out["confidence"] = 60
        out["evidence"] = (f"当前分散(最大{lg:.0f}%)且历史无'吸入后派发'的操盘足迹 → "
                           f"{'集中于金库/长持' if lg>=15 else '散户盘'},无操盘证据")
    return out
