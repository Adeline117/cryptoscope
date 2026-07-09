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


# Verdicts that must NEVER auto-register a sentinel (spec process-gate): unproven or
# non-operator states. identify_operator is the only intended promotion path.
NON_PROMOTABLE = {"too_young_to_judge", "indeterminate_emptied", "none", "dispersed",
                  "treasury_only", "unknown"}


def promotable(verdict: dict, min_confidence: int = 55) -> bool:
    """Whether a verdict may promote to a tracked sentinel. Gates out unproven /
    non-operator / low-confidence states — no register() bypass (MAME lesson)."""
    return (verdict.get("verdict") not in NON_PROMOTABLE
            and (verdict.get("confidence") or 0) >= min_confidence)


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


def _infra(chain: str) -> dict:
    """Router/bridge/disperse/burn labels (destination classification). Routers ARE
    sell venues — a sell usually routes THROUGH the router, not straight to the pair,
    so missing them undercounts sells."""
    import json

    from src.config import DATA_DIR
    try:
        d = json.loads((DATA_DIR / "research" / "labels" / f"infra_{chain}.json").read_text())
        return {"routers": set(d.get("routers", {})), "bridges": set(d.get("bridges", {})),
                "disperse": set(d.get("disperse", {})), "burn": set(d.get("burn", {}))}
    except Exception:
        return {"routers": set(), "bridges": set(), "disperse": set(), "burn": set()}


def _token_pairs(token: str, chain: str) -> set[str]:
    """LP pair addresses (sell-into-pool destinations) from DexScreener + total liq."""
    import json
    import urllib.request
    pairs: set[str] = set()
    try:
        req = urllib.request.Request(
            f"https://api.dexscreener.com/latest/dex/tokens/{token}",
            headers={"User-Agent": "Mozilla/5.0"})
        d = json.loads(urllib.request.urlopen(req, timeout=15).read().decode())
        for p in (d.get("pairs") or []):
            pa = (p.get("pairAddress") or "").lower()
            if pa:
                pairs.add(pa)
    except Exception:
        pass
    return pairs


def _exit_destinations(token: str, chain: str, wallet: str, member_set: set,
                       pairs: set, cex: dict, max_pages: int = 6) -> dict:
    """Where did `wallet` send `token`? Classify each destination (SELL vs MOVE) —
    the sell-vs-move referee. Sold: → LP pair / CEX. Moved: → cluster member (internal)
    or a plain EOA (rotation-unproven). Returns aggregate amounts by class."""
    import time

    from src.onchain import moralis_client
    from src.onchain.entity_classify import classify_address
    mchain = {"bsc": "bsc", "base": "base", "ethereum": "eth"}.get(chain)
    if not mchain:
        return {}
    infra = _infra(chain)
    sell_venues = pairs | infra["routers"]      # routers ARE sell venues
    agg = {"sell_dex": 0.0, "sell_cex": 0.0, "move_member": 0.0, "move_eoa": 0.0,
           "to_contract": 0.0, "resolved": False}
    cursor = None
    tl = token.lower()
    for pg in range(max_pages):
        if pg:
            time.sleep(0.25)
        path = f"{wallet}/erc20/transfers?chain={mchain}&order=DESC&limit=100" + (f"&cursor={cursor}" if cursor else "")
        d = moralis_client.get(path)
        if not d:
            break
        for t in (d.get("result", []) if isinstance(d, dict) else []):
            if (t.get("address") or t.get("token_address") or "").lower() != tl:
                continue
            if (t.get("from_address") or "").lower() != wallet.lower():
                continue
            agg["resolved"] = True
            try:
                amt = float(t.get("value_decimal") or (float(t.get("value", 0)) / 1e18))
            except (ValueError, TypeError):
                amt = 0.0
            to = (t.get("to_address") or "").lower()
            if to in sell_venues:
                agg["sell_dex"] += amt
            elif to in cex:
                agg["sell_cex"] += amt
            elif to in member_set:
                agg["move_member"] += amt
            elif classify_address(to, chain).get("type") == "contract":
                agg["to_contract"] += amt
            else:
                agg["move_eoa"] += amt
        cursor = d.get("cursor") if isinstance(d, dict) else None
        if not cursor:
            break
    return agg


def _wallet_outflow_map(token: str, chain: str, wallet: str, max_pages: int = 6) -> dict:
    """{to_lower: amount} for `wallet`'s outbound `token` transfers (Moralis DESC)."""
    import time
    from collections import defaultdict

    from src.onchain import moralis_client
    mchain = {"bsc": "bsc", "base": "base", "ethereum": "eth"}.get(chain)
    if not mchain:
        return {}
    out: dict[str, float] = defaultdict(float)
    cursor, tl = None, token.lower()
    for pg in range(max_pages):
        if pg:
            time.sleep(0.25)
        path = f"{wallet}/erc20/transfers?chain={mchain}&order=DESC&limit=100" + (f"&cursor={cursor}" if cursor else "")
        d = moralis_client.get(path)
        if not d:
            break
        for t in (d.get("result", []) if isinstance(d, dict) else []):
            if (t.get("address") or t.get("token_address") or "").lower() != tl:
                continue
            if (t.get("from_address") or "").lower() != wallet.lower():
                continue
            try:
                amt = float(t.get("value_decimal") or (float(t.get("value", 0)) / 1e18))
            except (ValueError, TypeError):
                amt = 0.0
            to = (t.get("to_address") or "").lower()
            if to:
                out[to] += amt
        cursor = d.get("cursor") if isinstance(d, dict) else None
        if not cursor:
            break
    return out


def _rotation_frontier(token: str, chain: str, seed: list, pairs: set, cex: dict,
                       depth: int = 2, max_wallets: int = 25) -> dict:
    """Follow the rotation frontier: emptied operator wallets → their MOVE
    destinations → recurse (bounded). Aggregate where the stack ULTIMATELY goes:
    sold (→pool/CEX anywhere in the frontier) vs still parked in fresh EOAs (held/
    dormant). Answers 'the rotated stack — did it eventually get sold, or is it a
    loaded threat sitting in new wallets?'"""
    from src.onchain.entity_classify import classify_address
    sell_venues = pairs | _infra(chain)["routers"] | set(cex)
    seen = set(w.lower() for w in seed)
    frontier = list(seen)
    sold = 0.0
    parked_terminal = 0.0
    visited_edges = 0
    for lvl in range(depth):
        nxt = []
        for w in frontier:
            if visited_edges >= max_wallets:
                break
            visited_edges += 1
            for to, amt in _wallet_outflow_map(token, chain, w).items():
                if to in sell_venues:
                    sold += amt                      # reached a sell venue → sold
                elif classify_address(to, chain).get("type") == "contract":
                    sold += amt * 0                  # contract: ignore (LP/staking ambiguous)
                elif to not in seen:
                    seen.add(to)
                    if lvl < depth - 1:
                        nxt.append(to)               # keep chasing
                    else:
                        parked_terminal += amt       # frontier edge: parked in a wallet
        frontier = nxt
        if not frontier:
            break
    return {"sold_via_frontier": round(sold), "parked_in_wallets": round(parked_terminal),
            "wallets_walked": visited_edges}


def _cluster_holds_onchain(token: str, chain: str, wallets: list) -> bool:
    """INV-4 / SYN fix: confirm the snapshot-derived cluster ACTUALLY holds on-chain
    (balance_of > 0) before calling it loaded. SYN's snapshot said '5 wallets hold
    10%' but all 5 had a real zero balance = stale/mid-transit snapshot. A loaded
    verdict on a cluster that holds nothing is a false positive."""
    if not wallets:
        return False
    try:
        from src.onchain.evm_archive import ArchiveRPC
        rpc = ArchiveRPC(chain)
        total = 0.0
        got = False
        for w in wallets[:10]:
            b = rpc.balance_of(token, w)
            if b is not None:
                got = True
                total += b
        return got and total > 0
    except Exception:
        return False


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
    # expose the cluster ADDRESSES so callers (sentinel registration) can act on a
    # verdict — counts alone aren't registrable.
    out["current"]["cluster_wallets"] = list(conc.get("dominant_cluster_wallets") or [])

    try:
        from src.onchain.evm_archive import ArchiveRPC
        dec = ArchiveRPC(chain).token_decimals(token)
        out["historical"] = _historical_ledger(token, chain, dec)
    except Exception as e:
        out["historical"] = {"available": False, "exited": [], "holding": []}
        out["caveats"].append(f"historical failed: {str(e)[:60]}")

    # AGE GATE (MAME fix): a token too young has no pump→distribute lifecycle to
    # judge — youth routes out BEFORE the operator taxonomy so a busy young token is
    # never cornered into an operator label.
    try:
        from src.pipeline.operator_sentinel import _token_age_days
        age = _token_age_days(token, chain)
    except Exception:
        age = None
    out["current"]["token_age_days"] = age

    conf = conc.get("cluster_confidence") or 0
    hist = out["historical"]
    exited = hist.get("exited") or []

    if age is not None and age < 14 and conf < 55:
        out["verdict"] = "too_young_to_judge"
        out["confidence"] = 0
        out["evidence"] = f"代币仅{age:.0f}天(<14d):拉盘→派发生命周期无法计算,不下操盘定性"
        out["caveats"].append("年龄门:<14d非可判")
        return out

    holding = hist.get("holding") or []
    sum_hold = sum(h.get("net_now", 0) for h in holding)
    sum_exit_in = sum(e.get("total_in", 0) for e in exited)

    lg = conc.get("largest_entity_pct") or 0
    dom = out["current"].get("dominant_wallets") or 0

    if conf >= 55:
        out["verdict"] = "live_operator"
        out["confidence"] = conf
        out["evidence"] = f"当前隐藏簇 cluster_confidence={conf}"
    elif dom >= 5 and lg >= 10 and _cluster_holds_onchain(token, chain, conc.get("dominant_cluster_wallets") or []):
        # LOADED-LIVE via the CURRENT holder graph (BASED): a coordinated CLUSTER
        # (>=5 wallets, not a single whale) holds a meaningful share of LIVE supply.
        # INV-4 GUARD (SYN catch): the concentration signal comes from a fetched holder
        # list that can be STALE — _cluster_holds_onchain RPC-verifies live balances>0
        # before calling it loaded (SYN's "5 wallets/10%" held 0 on-chain = false).
        out["verdict"] = "loaded_live_operator"
        out["confidence"] = min(78, 45 + int(lg))
        out["evidence"] = (f"当前活簇 {dom}个钱包协同持有 {lg:.0f}% 流通供应(链上余额已核实>0),"
                           f"早期未大规模离场 = 操盘装弹持有(拉盘候选)")
    elif hist.get("available") and len(holding) >= 3 and sum_hold >= max(sum_exit_in, 1):
        # LOADED-LIVE fix (BASED): the coordinated cluster still HOLDS more than it
        # emptied — an operator loaded and sitting, not one that left. Checked BEFORE
        # the emptied-wallet path so a live loaded cluster isn't read as 'indeterminate'.
        out["verdict"] = "loaded_live_operator"
        out["confidence"] = min(80, 45 + 6 * len(holding))
        out["evidence"] = (f"{len(holding)}个早期重仓钱包仍持有(合计{sum_hold:,.0f} > "
                           f"已清空{sum_exit_in:,.0f}) = 操盘装弹持有,未离场未派发")
    elif hist.get("available") and exited:
        # SELL-vs-MOVE REFEREE (destination-grounded, replaces the price guess).
        # For the emptied early-heavy wallets, trace WHERE their tokens went.
        from src.onchain.cex_addresses import evm_exchanges
        member_set = {e["address"] for e in exited} | {e["address"] for e in (hist.get("holding") or [])}
        pairs = _token_pairs(token, chain)
        cex = evm_exchanges()
        tot = {"sell_dex": 0.0, "sell_cex": 0.0, "move_member": 0.0, "move_eoa": 0.0,
               "to_contract": 0.0}
        resolved_n = 0
        for e in exited[:8]:                       # bounded
            a = _exit_destinations(token, chain, e["address"], member_set, pairs, cex)
            if a.get("resolved"):
                resolved_n += 1
                for kk in tot:
                    tot[kk] += a.get(kk, 0.0)
        sold = tot["sell_dex"] + tot["sell_cex"]
        moved_internal = tot["move_member"]
        moved_eoa = tot["move_eoa"]
        total_out = sold + moved_internal + moved_eoa + tot["to_contract"]
        out["current"]["exit_destinations"] = {**tot, "resolved_wallets": resolved_n}

        if resolved_n == 0 or total_out <= 0:
            out["verdict"] = "indeterminate_emptied"
            out["confidence"] = 25
            out["evidence"] = f"{len(exited)}个早期重仓钱包已清空,但转出去向取数失败 → 卖/移不可判"
            out["caveats"].append("去向未解析 = 不得断言操盘去留")
        elif sold >= 0.5 * total_out:
            out["verdict"] = "exited_by_selling"
            out["confidence"] = min(85, 50 + 5 * resolved_n)
            out["evidence"] = (f"{len(exited)}个早期重仓钱包清空,转出{sold/total_out*100:.0f}%进"
                               f"LP池/CEX(卖{sold:,.0f}) = 操盘卖出离场")
        elif moved_internal >= 0.5 * total_out:
            # moved-to-member → follow the frontier: PARKED (loaded threat) = real
            # rotation; SOLD downstream = distribution/churn, NOT a loaded operator.
            fr = _rotation_frontier(token, chain, [e["address"] for e in exited[:8]], pairs, cex)
            out["current"]["rotation_frontier"] = fr
            if fr["parked_in_wallets"] > fr["sold_via_frontier"] and fr["parked_in_wallets"] > 0:
                out["verdict"] = "present_rotating_confirmed"   # EVAA: parked = loaded threat
                out["confidence"] = min(85, 50 + 5 * resolved_n)
                out["evidence"] = (f"{len(exited)}个钱包清空,{moved_internal/total_out*100:.0f}%回流簇内且"
                                   f"下游{fr['parked_in_wallets']:,.0f}仍停新钱包 = 换钱包装弹,随时可砸")
            elif fr["sold_via_frontier"] > 0:
                # rotated then dumped downstream — distribution, and if no still-holding
                # coordinated cluster this is more likely CHURN than an operator (MAME).
                out["verdict"] = "distributing_or_churn"
                out["confidence"] = 45
                out["evidence"] = (f"{len(exited)}个钱包清空,回流簇内但下游已卖{fr['sold_via_frontier']:,.0f} "
                                   f"→ 派发或散户刷币(非装弹操盘);无仍持有的协同簇=倾向churn")
                out["caveats"].append("需genesis/degen判别区分'操盘派发'vs'散户churn'")
            else:
                out["verdict"] = "indeterminate_emptied"
                out["confidence"] = 30
                out["evidence"] = f"{len(exited)}个钱包清空回流簇内,下游去向未解析 → 不可判"
        elif sold >= 0.2 * total_out or sold >= 10_000_000:
            # DISTRIBUTING fix (SIREN): meaningful selling into pool/CEX = distribution
            # even if some also moved. A real bleed doesn't need a 50% majority.
            out["verdict"] = "distributing"
            out["confidence"] = min(75, 45 + 5 * resolved_n)
            out["evidence"] = (f"{len(exited)}个钱包清空,已向LP池/CEX卖出{sold:,.0f}"
                               f"({sold/total_out*100:.0f}%) = 操盘在派发出货")
        else:
            out["verdict"] = "indeterminate_emptied"
            out["confidence"] = 35
            out["evidence"] = (f"{len(exited)}个钱包清空,转出主要去向为普通新EOA"
                               f"(EOA{moved_eoa:,.0f}),卖出仅{sold:,.0f} → 卖vs换钱包不可判")
            out["caveats"].append("去向多为featureless新EOA:自托管vs换钱包vs中介卖出,不可判")
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
