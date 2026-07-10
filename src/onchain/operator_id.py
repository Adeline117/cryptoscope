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
                  "treasury", "treasury_only", "unknown"}

# F8: only an ACCUMULATING loaded cluster is a long signal. `loaded_dormant` is a state
# description, not a reason to buy — it stays promotable (worth watching) but callers
# must not render it as 拉盘候选.
LONG_ACTIONABLE = {"loaded_accumulating"}
SHORT_ACTIONABLE = {"distributing", "exited_by_selling", "present_rotating_confirmed"}


def promotable(verdict: dict, min_confidence: int = 55) -> bool:
    """Whether a verdict may promote to a tracked sentinel. Gates out unproven /
    non-operator / low-confidence states — no register() bypass (MAME lesson).

    F12: a verdict flagged `borderline` sits within jitter distance of a category
    cliff. Refuse promotion rather than commit to a side that may flip next run."""
    if any(str(c).startswith("borderline") for c in (verdict.get("caveats") or [])):
        return False
    return (verdict.get("verdict") not in NON_PROMOTABLE
            and (verdict.get("confidence") or 0) >= min_confidence)


_TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
_ETHERSCAN_CHAINID = {"ethereum": 1}          # free Etherscan V2 = ETH only (BSC/Base need paid)


_CONFIRMATIONS = 12          # F7: freeze the walk window below the moving tip


def _events_etherscan(token: str, chainid: int, decimals: int, max_pages: int = 60,
                      head: int | None = None):
    """Full transfer history via Etherscan V2 getLogs (free = ETH only). Returns
    (inflow, net, complete) per wallet, or (None, None, False) on failure.

    F7: `toBlock` is PINNED to a head captured once. With `toBlock=latest` re-evaluated
    per page the tip moved mid-walk, so a sell landing between pages split a wallet's
    in/out across the boundary → transient net<0 → the whole historical dimension was
    discarded. `complete` reports whether the walk genuinely TERMINATED (a final short
    page) rather than hitting the page ceiling — the caller keys its guard off that."""
    import json
    import os
    import urllib.request
    from collections import defaultdict
    keys = [k.strip() for k in os.environ.get("ETHERSCAN_API_KEYS", "").split(",") if k.strip()]
    if not keys:
        return None, None, False
    if head is None:
        try:
            from src.onchain.evm_archive import ArchiveRPC
            head = ArchiveRPC("ethereum").latest_block()
        except Exception:
            head = None
    to_block = str(head - _CONFIRMATIONS) if head else "latest"
    inflow, net = defaultdict(float), defaultdict(float)
    frm, scale, complete = 0, float(10 ** decimals), False
    for p in range(max_pages):
        u = (f"https://api.etherscan.io/v2/api?chainid={chainid}&module=logs&action=getLogs"
             f"&address={token}&topic0={_TRANSFER_TOPIC}&fromBlock={frm}&toBlock={to_block}"
             f"&page=1&offset=1000&apikey={keys[p % len(keys)]}")
        try:
            r = json.loads(urllib.request.urlopen(
                urllib.request.Request(u, headers={"User-Agent": "Mozilla/5.0"}), timeout=25
            ).read().decode())
        except Exception:
            return ((inflow, net, False) if inflow else (None, None, False))
        res = r.get("result")
        if not isinstance(res, list) or not res:
            complete = True          # empty page = end of history, not a failure
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
            complete = True          # short final page = the walk really terminated
            break
        frm = int(res[-1]["blockNumber"], 16) + 1
    return inflow, net, complete


def _before(row: dict, as_of_block: int | None) -> bool:
    """Replay cutoff: is this transfer at or before the as-of block?

    A backtest that sees even one transfer past its own cutoff is measuring hindsight,
    not prediction. A row with no block_number is UNDATABLE — exclude it under replay
    (dropping a real row understates activity; including an undated one may leak the
    future, and only the latter can manufacture a fake edge)."""
    if as_of_block is None:
        return True
    try:
        return int(row.get("block_number")) <= as_of_block
    except (TypeError, ValueError):
        return False


def _early_inflow_moralis(token: str, chain: str, decimals: int, max_pages: int = 60,
                          as_of_block: int | None = None):
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
        rows = (d.get("result", []) if isinstance(d, dict) else [])
        past_cutoff = False
        for t in rows:
            if not _before(t, as_of_block):
                past_cutoff = True       # ASC order → everything after is later too
                continue
            got = True
            try:
                amt = float(t.get("value", 0)) / scale
            except (ValueError, TypeError):
                continue
            a_to = (t.get("to_address") or "").lower()
            if a_to:
                inflow[a_to] += amt
        if past_cutoff:
            break                        # oldest-first: no need to page further
        cursor = d.get("cursor") if isinstance(d, dict) else None
        if not cursor:
            break
    return inflow if got else None


_BASKET_CACHE: dict[str, set] = {}
_MM_BASKET_SIZE = 60          # F10: a desk/serial-degen holds a huge generic basket


def _wallet_basket(wallet: str, chain: str) -> set[str] | None:
    """The set of ERC20s a wallet currently holds. An operator's basket is small and
    idiosyncratic; a market-maker's / serial-degen's is huge and generic. None on a
    fetch failure — an empty set would falsely read as 'clean single-token operator'."""
    from src.onchain import moralis_client
    mchain = {"bsc": "bsc", "base": "base", "ethereum": "eth"}.get(chain)
    key = f"{chain}:{wallet.lower()}"
    if key in _BASKET_CACHE:
        return _BASKET_CACHE[key]
    if not moralis_client.usable() or not mchain:
        return None
    try:
        d = moralis_client.get(f"{wallet}/erc20?chain={mchain}")
    except Exception:
        return None
    if d is None:
        return None
    rows = d if isinstance(d, list) else (d.get("result") or [])
    b = {(r.get("token_address") or "").lower() for r in rows if r.get("token_address")}
    _BASKET_CACHE[key] = b
    return b


def _is_mm_like(wallet: str, chain: str) -> bool:
    """F10: market-maker / serial-degen fingerprint. These pass every candidate
    exclusion (EOA, not CEX, not a pair) and get misread as `exited_by_selling`
    operators. Unknown basket → False (don't exclude on missing data)."""
    b = _wallet_basket(wallet, chain)
    return b is not None and len(b) >= _MM_BASKET_SIZE


def same_entity(a: str, b: str, chain: str, funders: dict | None = None) -> dict:
    """F10: is `a` the same actor as `b`? Two independent corroborators —

      1. shared root funder that is NOT a CEX and NOT a disperser (a disperser or an
         exchange hot wallet links thousands of unrelated wallets: the falsified
         "family root" lesson);
      2. co-held low-cap basket overlap (Jaccard) — small idiosyncratic overlap is an
         operator fingerprint; a huge basket on either side means MM/degen and the
         overlap is meaningless, so we refuse rather than assert.

    Returns {same, why, jaccard} — `same` only when a corroborator is POSITIVE, never
    merely 'not contradicted'."""
    al, bl = a.lower(), b.lower()
    if al == bl:
        return {"same": True, "why": "identical", "jaccard": 1.0}
    why, jac = [], None
    try:
        from src.onchain.cex_addresses import evm_exchanges
        from src.pipeline.anomaly_screener import funder_disperser_verdict
        if funders is None:
            from src.onchain.funder_graph import get_funders
            funders = get_funders([al, bl], chain)
        fa = str(funders.get(al) or "").lower()
        fb = str(funders.get(bl) or "").lower()
        cex = evm_exchanges()
        # `is False` (affirmative): an unverifiable funder must not fabricate a link.
        if (fa and fa == fb and fa not in cex
                and funder_disperser_verdict(fa, chain) is False):
            why.append(f"shared_funder:{fa[:10]}")
    except Exception:
        pass
    ba, bb = _wallet_basket(al, chain), _wallet_basket(bl, chain)
    if ba is not None and bb is not None and ba and bb:
        if len(ba) >= _MM_BASKET_SIZE or len(bb) >= _MM_BASKET_SIZE:
            why.append("mm_like_basket:inconclusive")     # refuse, don't assert
        else:
            jac = round(len(ba & bb) / float(len(ba | bb)), 3)
            if jac >= 0.5 and len(ba & bb) >= 2:
                why.append(f"co_held_basket:{jac}")
    same = any(w.startswith(("shared_funder", "co_held_basket")) for w in why)
    return {"same": same, "why": why, "jaccard": jac}


def _historical_ledger(token: str, chain: str, decimals: int,
                       as_of_block: int | None = None) -> dict:
    """Per-wallet total-inflow vs net-now over full history — DUNE-FREE. Flags wallets
    that took in a lot but hold ~nothing now = accumulated-then-distributed (exited
    operator), the fingerprint the current snapshot can't see. Source: Etherscan V2
    (ETH) / Moralis (BSC). Candidates are entity-FILTERED (a wallet that took a lot
    and dumped could be the LP/deployer/MM/CEX, not an operator). available=False on
    fetch failure → UNKNOWN, never 'none'."""
    tok = token.lower()
    cid = _ETHERSCAN_CHAINID.get(chain)
    # F7 already pins toBlock; under replay the pin IS the cutoff.
    inflow, net, complete = (_events_etherscan(tok, cid, decimals,
                                               head=(as_of_block + _CONFIRMATIONS)
                                               if as_of_block else None) if cid
                             else (None, None, False))

    if inflow is not None and complete and not inflow:
        # Walk terminated with ZERO transfers. Every real ERC20 has at least its mint
        # transfers — an empty history means wrong address/chain or an indexer error,
        # and treating it as "checked, no operator footprint" produced a confident
        # `dispersed` verdict for a nonexistent token. No data ≠ clean history.
        logger.warning("historical_ledger_empty", token=token,
                       note="0 transfers on a terminated walk = wrong address/chain?")
        return {"available": False, "exited": [], "holding": [], "empty_history": True}

    if inflow is not None and net is not None and complete:
        # ETH: a TERMINATED full block-walk gives complete inflow AND net → use net.
        # F7: the guard is keyed on real termination (above), not on a transient
        # net<0 — which fired on every busy token (a fee/reflection token legitimately
        # shows small negatives) and discarded exactly the tokens operators target.
        # A large unexplained negative still means the pull is broken, so keep that
        # as a hard check with a tolerance proportional to the wallet's own inflow.
        ranked = sorted(inflow.items(), key=lambda kv: -kv[1])
        bad = [a for a, _ in ranked[:40]
               if net.get(a, 0) < -max(1.0, 0.02 * (inflow.get(a) or 0))]
        if bad:
            logger.warning("historical_ledger_incomplete", token=token,
                           note="ETH walk terminated but net<0 beyond tolerance")
            return {"available": False, "exited": [], "holding": [], "incomplete": True}
        net_of = lambda a: net.get(a, 0.0)
    else:
        if inflow is not None and not complete:
            # F7: page-ceiling truncation on a busy ETH token — don't refuse; fall
            # through to the early-window + live-balance path that BSC already uses.
            logger.warning("eth_walk_truncated", token=token, note="falling back to early-window")
        # BSC/other: early-accumulation window (oldest-first) + balance AT the as-of
        # block to decide exit — avoids needing the (too-large) full history.
        inflow = _early_inflow_moralis(tok, chain, decimals, as_of_block=as_of_block)
        if not inflow:
            return {"available": False, "exited": [], "holding": []}
        from src.onchain.evm_archive import ArchiveRPC
        rpc = ArchiveRPC(chain)
        ranked = sorted(inflow.items(), key=lambda kv: -kv[1])
        _bal_cache: dict[str, float] = {}

        def net_of(a):
            # THE MAIN REPLAY LEAK: this read was pinned to "latest", so a backtest
            # replaying block B saw balances as of TODAY — a wallet that emptied after
            # B looked already-exited at B. Every "exited_by_selling" would then be
            # trivially foreseen. Under replay, read the balance AT the as-of block.
            if a not in _bal_cache:
                b = rpc.balance_of(token, a, as_of_block or "latest")
                _bal_cache[a] = b if b is not None else 0.0
            return _bal_cache[a]

    from src.onchain.cex_addresses import evm_exchanges
    from src.onchain.entity_classify import classify_address
    cex = evm_exchanges()
    exited, holding = [], []
    for a, tin in ranked[:40]:
        if a in _BURN or a in cex or tin <= 0:
            continue
        # F6 (red-team): a real trading operator is confidently EOA; 'unknown' (getCode
        # flake / BSC RPC down) must not let an LP/bridge/migration lock into the exited
        # path. Keep multisig (Safe operators) — only drop contract/unknown.
        if classify_address(a, chain).get("type") in ("contract", "unknown"):
            continue
        nn = net_of(a)
        dist = 1 - max(nn, 0) / tin
        rec = {"address": a, "total_in": round(tin), "net_now": round(nn),
               "distributed": round(dist, 2)}
        # F4 (red-team dead-zone fix): 0.35–0.7 was in NEITHER list → the verdict fell
        # through to 'treasury' (a big-residual mid-distributor like SIREN mislabeled
        # passive). Anything that has shed >=35% of what it took in goes through the
        # exit referee so real distribution is seen.
        if dist >= 0.35:
            exited.append(rec)
        elif nn > 0 and dist < 0.3:
            holding.append(rec)
    # F10: drop market-maker / serial-degen wallets from the EXITED path. They are
    # confidently-EOA, non-CEX, non-pair — they clear every other exclusion — and a
    # desk cycling inventory then reads as "operator sold and left" (exited_by_selling
    # conf 85). Bounded to the wallets the referee will actually walk.
    # Red-team: a FAILED basket read is not a PASSED check — track unchecked wallets so
    # the caller caps confidence instead of emitting conf-85 as if the MM screen ran.
    mm_dropped, mm_unchecked = [], []
    for rec in exited[:10]:
        b = _wallet_basket(rec["address"], chain)
        if b is None:
            mm_unchecked.append(rec["address"])
        elif len(b) >= _MM_BASKET_SIZE:
            mm_dropped.append(rec["address"])
    if mm_dropped:
        exited = [e for e in exited if e["address"] not in set(mm_dropped)]
        logger.info("mm_candidates_dropped", token=token, n=len(mm_dropped))
    return {"available": True, "exited": exited[:15], "holding": holding[:15],
            "mm_dropped": mm_dropped, "mm_unchecked": mm_unchecked}


def _token_age_onchain(token: str, chain: str, as_of_block: int | None = None) -> float | None:
    """F2: age from the token's FIRST on-chain transfer (Moralis order=ASC, 1 row) —
    reproducible, unlike the network-flaky `_token_age_days` heuristic. Returns days,
    or None if unresolvable (the caller must branch on that, never fall through).

    Under replay, age is measured to the AS-OF block's timestamp, not to today — a
    token that was 3 days old at the replay block must trip the youth gate then, even
    though it is a year old now."""
    from datetime import datetime, timezone

    from src.onchain import moralis_client
    mchain = {"bsc": "bsc", "base": "base", "ethereum": "eth"}.get(chain)
    if not moralis_client.usable() or not mchain:
        return None
    try:
        d = moralis_client.get(f"erc20/{token}/transfers?chain={mchain}&order=ASC&limit=1")
        rows = (d or {}).get("result") or []
        ts = rows[0].get("block_timestamp") if rows else None
        if not ts:
            return None
        t0 = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    except Exception:
        return None
    now = datetime.now(timezone.utc)
    if as_of_block:
        try:
            from src.onchain.evm_archive import ArchiveRPC
            bt = ArchiveRPC(chain).block_time(as_of_block)
            if not bt:
                return None          # can't date the cutoff → age UNKNOWN, not "today"
            now = datetime.fromtimestamp(bt, timezone.utc)
        except Exception:
            return None
    return (now - t0).total_seconds() / 86400.0


_INFRA_FILE = {"ethereum": "eth"}     # chain name → label-file suffix


def _infra(chain: str) -> dict:
    """Router/bridge/disperse/burn labels (destination classification). Routers ARE
    sell venues — a sell usually routes THROUGH the router, not straight to the pair,
    so missing them undercounts sells.

    Silent-miss bug (fixed): the file for ethereum is `infra_eth.json`, but the
    lookup built `infra_ethereum.json` → every ETH label set was EMPTY, so sells
    routed through Uniswap were scored as move_eoa/to_contract instead of sell_dex,
    understating distribution on every ETH sentinel. Empty labels are now LOUD."""
    import json

    from src.config import DATA_DIR
    suffix = _INFRA_FILE.get(chain, chain)
    try:
        d = json.loads((DATA_DIR / "research" / "labels" / f"infra_{suffix}.json").read_text())
        if not d.get("routers"):
            logger.warning("infra_labels_empty", chain=chain,
                           note="无路由器标签 → 经路由卖出会被低估为move")
        return {"routers": set(d.get("routers", {})), "bridges": set(d.get("bridges", {})),
                "disperse": set(d.get("disperse", {})), "burn": set(d.get("burn", {}))}
    except Exception as e:
        logger.warning("infra_labels_missing", chain=chain, error=str(e)[:60],
                       note="标签文件缺失 → 卖出会被系统性低估")
        return {"routers": set(), "bridges": set(), "disperse": set(), "burn": set()}


_DS_CACHE: dict[str, dict] = {}


def _dexscreener(token: str) -> dict:
    """One DexScreener fetch per token per process (pairs + market both need it)."""
    import json
    import urllib.request
    tl = token.lower()
    if tl in _DS_CACHE:
        return _DS_CACHE[tl]
    d = {}
    try:
        req = urllib.request.Request(
            f"https://api.dexscreener.com/latest/dex/tokens/{token}",
            headers={"User-Agent": "Mozilla/5.0"})
        d = json.loads(urllib.request.urlopen(req, timeout=15).read().decode()) or {}
    except Exception:
        d = {}
    _DS_CACHE[tl] = d
    return d


def _token_pairs(token: str, chain: str) -> set[str]:
    """LP pair addresses (sell-into-pool destinations) from DexScreener."""
    pairs: set[str] = set()
    for p in (_dexscreener(token).get("pairs") or []):
        pa = (p.get("pairAddress") or "").lower()
        if pa:
            pairs.add(pa)
    return pairs


def _token_market(token: str) -> dict:
    """F11: liquidity / 24h volume / recent price change — fields the pair fetch
    already returned and threw away. Used for the TERMINAL gate: a token whose pool
    is drained, volume is ~zero and price already collapsed is a post-event corpse.
    Calling it '操盘在派发' there is a misfire — the event already happened."""
    best, liq, vol = None, 0.0, 0.0
    for p in (_dexscreener(token).get("pairs") or []):
        l = float(((p.get("liquidity") or {}).get("usd")) or 0)
        v = float(p.get("volume", {}).get("h24") or 0)
        liq += l
        vol += v
        if best is None or l > best[0]:
            best = (l, p)
    if best is None:
        return {"available": False}
    p = best[1]
    pc = p.get("priceChange") or {}
    return {"available": True, "liquidity_usd": round(liq), "volume_h24": round(vol),
            "price_change_h24": pc.get("h24"), "price_change_h6": pc.get("h6"),
            "price_usd": p.get("priceUsd")}


def _is_terminal(mkt: dict) -> bool:
    """Post-collapse corpse: no liquidity left AND no trading. Both must hold — a
    thin-liquidity token that still trades is a live operator's playground, and a
    deep pool with no volume is merely quiet, not dead."""
    if not mkt.get("available"):
        return False
    liq = mkt.get("liquidity_usd") or 0
    vol = mkt.get("volume_h24") or 0
    return liq < 15_000 and vol < 2_000


def _exit_destinations(token: str, chain: str, wallet: str, member_set: set,
                       pairs: set, cex: dict, max_pages: int = 30,
                       seed_funders: set | None = None,
                       expected_out: float | None = None,
                       as_of_block: int | None = None) -> dict:
    """Where did `wallet` send `token`? Classify each destination (SELL vs MOVE) —
    the sell-vs-move referee. Sold: → LP pair / CEX. Moved: → cluster member OR (F10)
    a wallet sharing an operator seed-funder = same-entity rotation; else plain EOA
    (rotation-unproven). Returns aggregate amounts by class.

    F9: paging is CONVERGENCE-bounded, not a recency slice. `expected_out` (≈ total_in
    − net_now) is how much this wallet must have sent; we page DESC until the resolved
    outflow accounts for it, then stop. The old fixed 6-page window only saw the ~600
    most recent transfers, so for an early operator whose sells happened early the
    window slid off the real exit as dust accrued — SIREN's `distributing` decayed to
    `indeterminate_emptied` with nothing but wall-clock. `coverage` reports how much of
    the expected outflow we actually accounted for; the caller must not referee on a
    thin slice."""
    import time
    from collections import defaultdict

    from src.onchain import moralis_client
    from src.onchain.entity_classify import classify_address
    mchain = {"bsc": "bsc", "base": "base", "ethereum": "eth"}.get(chain)
    if not mchain:
        return {}
    infra = _infra(chain)
    sell_venues = pairs | infra["routers"]      # routers ARE sell venues
    agg = {"sell_dex": 0.0, "sell_cex": 0.0, "move_member": 0.0, "move_eoa": 0.0,
           "to_contract": 0.0, "resolved": False, "coverage": None, "converged": None}
    eoa_dests: dict = defaultdict(float)        # deferred: funder-checked at end (F10)
    cursor, cum_out = None, 0.0
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
            if not _before(t, as_of_block):     # replay cutoff
                continue
            agg["resolved"] = True
            try:
                amt = float(t.get("value_decimal") or (float(t.get("value", 0)) / 1e18))
            except (ValueError, TypeError):
                amt = 0.0
            cum_out += amt
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
                eoa_dests[to] += amt              # defer: funder-check below
        cursor = d.get("cursor") if isinstance(d, dict) else None
        if not cursor:
            agg["converged"] = True              # history exhausted = fully accounted
            break
        if expected_out and expected_out > 0 and cum_out >= 0.98 * expected_out:
            agg["converged"] = True              # convergence bound reached, stop early
            break
    if expected_out and expected_out > 0:
        agg["coverage"] = round(min(cum_out / expected_out, 1.0), 3)
        if agg["converged"] is None:
            agg["converged"] = agg["coverage"] >= 0.9
    # F10: an EOA destination that shares an operator SEED FUNDER is same-entity
    # rotation (move_member), not an unknown EOA — automates the manual EVAA trace.
    if eoa_dests and seed_funders:
        try:
            from src.onchain.funder_graph import get_funders
            fmap = get_funders(list(eoa_dests), chain)
        except Exception:
            fmap = {}
        for to, amt in eoa_dests.items():
            if str(fmap.get(to) or "").lower() in seed_funders:
                agg["move_member"] += amt
            else:
                agg["move_eoa"] += amt
    else:
        for amt in eoa_dests.values():
            agg["move_eoa"] += amt
    return agg


def _wallet_outflow_map(token: str, chain: str, wallet: str, max_pages: int = 6,
                        as_of_block: int | None = None) -> dict:
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
            if not _before(t, as_of_block):     # replay cutoff
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
                       depth: int = 2, max_wallets: int = 25,
                       seed_funders: set | None = None,
                       as_of_block: int | None = None) -> dict:
    """Follow the rotation frontier: emptied operator wallets → their MOVE
    destinations → recurse (bounded). Aggregate where the stack ULTIMATELY goes:
    sold (→pool/CEX anywhere in the frontier) vs still parked in fresh EOAs (held/
    dormant). Answers 'the rotated stack — did it eventually get sold, or is it a
    loaded threat sitting in new wallets?'

    H4/F5: `parked_in_wallets` counts a terminal wallet as OPERATOR ammo only if it is
    funder-linked to the seed. Without that, a stack sold OTC into a buyer's wallet
    looks identical to self-custody rotation, and the buyer's balance would be scored
    as the operator's remaining ammo → false `present_rotating_confirmed`. Unlinked
    terminals go to their own bucket, which the caller must not read as 'loaded'."""
    from src.onchain.entity_classify import classify_address
    sell_venues = pairs | _infra(chain)["routers"] | set(cex)
    # F1 (red-team determinism fix): deterministic ordered worklist + weight-priority
    # cap. The old `list(set(...))` was PYTHONHASHSEED-random and the count-cap then
    # stopped after a hash-random subset → EVAA flipped rotating↔churn between runs.
    # Now: at each level sort candidates by (-amount, address) and take the top-K by
    # VALUE, so the dropped tail is the lowest-value wallets that can't move the verdict.
    seen = set(w.lower() for w in seed)
    frontier = sorted(seen)
    sold = 0.0
    parked_terminal = 0.0        # funder-LINKED terminals = operator ammo
    parked_unlinked = 0.0        # unlinked terminals = OTC-buyer / unprovable
    to_contract = 0.0
    walked = 0
    terminals: dict[str, float] = {}
    for lvl in range(depth):
        level_dests: dict[str, float] = {}
        for w in frontier[:max_wallets]:
            walked += 1
            for to, amt in _wallet_outflow_map(token, chain, w,
                                               as_of_block=as_of_block).items():
                level_dests[to] = level_dests.get(to, 0.0) + amt
        nxt = []
        for to, amt in sorted(level_dests.items(), key=lambda kv: (-kv[1], kv[0])):
            if to in sell_venues:
                sold += amt
            elif classify_address(to, chain).get("type") == "contract":
                to_contract += amt                   # explicit bucket, not silent 0
            elif to not in seen:
                seen.add(to)
                if lvl < depth - 1:
                    nxt.append(to)
                else:
                    terminals[to] = terminals.get(to, 0.0) + amt
        frontier = sorted(nxt)[:max_wallets]         # deterministic, value-independent order OK (already netted)
        if not frontier:
            break
    # H4: split terminals into funder-linked (operator ammo) vs unlinked (unprovable).
    if terminals:
        fmap = {}
        if seed_funders:
            try:
                from src.onchain.funder_graph import get_funders
                ranked_t = sorted(terminals.items(), key=lambda kv: (-kv[1], kv[0]))[:25]
                fmap = get_funders([t for t, _ in ranked_t], chain)
            except Exception:
                fmap = {}
        for to, amt in terminals.items():
            if seed_funders and str(fmap.get(to) or "").lower() in seed_funders:
                parked_terminal += amt
            else:
                parked_unlinked += amt
    return {"sold_via_frontier": round(sold), "parked_in_wallets": round(parked_terminal),
            "parked_unlinked": round(parked_unlinked), "to_contract": round(to_contract),
            "wallets_walked": walked, "terminals": len(terminals)}


_MINT_SOURCES = _BURN          # first inflow from 0x0 = minted, not bought


def acquisition_mode(token: str, chain: str, wallets: list, max_wallets: int = 8) -> dict:
    """Did this cluster BUY its position, or was it ALLOCATED?

    The single sharpest operator-vs-issuer discriminator, and the one this codebase
    kept getting wrong ("concentration = the issuer"). An operator accumulates FROM
    THE MARKET: its wallets' first inflow of the token arrives from an LP pair or a
    router. A project allocates: the first inflow arrives from the zero address (a
    mint) or from one distributor wallet.

    Measured 2026-07-10 on the two concentrated coins in a 45-coin survey of shortable
    small caps: AT had 5 of 6 wallets minted straight from 0x0; CHIP had 7 of 8 funded
    by one distributor. Buys: zero. Both were issuers wearing an operator's shape —
    23 wallets, one funder, 67% of supply, all EOAs, owner renounced.

    Returns {available, bought, allocated, unresolved, verdict, top_source}.
    `verdict` is 'bought' | 'allocated' | 'mixed' | 'unknown' — never a silent default.
    """
    from src.onchain import moralis_client
    from src.onchain.entity_classify import classify_address
    mchain = {"bsc": "bsc", "base": "base", "ethereum": "eth",
              "arbitrum": "arbitrum"}.get(chain)
    if not mchain or not wallets or not moralis_client.usable():
        return {"available": False, "verdict": "unknown", "reason": "no source"}

    pairs = _token_pairs(token, chain)
    routers = _infra(chain)["routers"]
    creator = ""
    try:
        from src.onchain.goplus_client import token_security
        sec = token_security(token, chain)
        creator = (sec.get("creator_address") or "") if sec.get("available") else ""
    except Exception:
        creator = ""

    tl = token.lower()
    bought = allocated = unresolved = 0
    sources: dict[str, int] = {}
    for w in [str(x).lower() for x in wallets[:max_wallets]]:
        try:
            d = moralis_client.get(f"{w}/erc20/transfers?chain={mchain}&order=ASC&limit=25")
            rows = [r for r in ((d or {}).get("result") or [])
                    if (r.get("address") or r.get("token_address") or "").lower() == tl
                    and (r.get("to_address") or "").lower() == w]
        except Exception:
            rows = []
        if not rows:
            unresolved += 1                 # can't date the acquisition → unknown
            continue
        frm = (rows[0].get("from_address") or "").lower()
        sources[frm] = sources.get(frm, 0) + 1
        if frm in pairs or frm in routers:
            bought += 1
        elif (frm in _MINT_SOURCES or frm == creator.lower()
              or classify_address(frm, chain).get("type") == "contract"):
            allocated += 1
        else:
            allocated += 1                  # a plain wallet handing out the float
    resolved = bought + allocated
    if not resolved:
        verdict = "unknown"
    elif allocated > bought:
        verdict = "allocated"
    elif bought > allocated:
        verdict = "bought"
    else:
        verdict = "mixed"
    top = max(sources.items(), key=lambda kv: kv[1])[0] if sources else None
    return {"available": True, "bought": bought, "allocated": allocated,
            "unresolved": unresolved, "verdict": verdict, "top_source": top}


def _cluster_holds_onchain(token: str, chain: str, wallets: list,
                           as_of_block: int | None = None) -> bool:
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
            b = rpc.balance_of(token, w, as_of_block or "latest")
            if b is not None:
                got = True
                total += b
        return got and total > 0
    except Exception:
        return False


def _cluster_velocity_30d(token: str, chain: str, wallets: list,
                          total_supply: float | None, days: int = 30,
                          as_of_block: int | None = None) -> dict | None:
    """F8: is the loaded cluster ACCUMULATING or just sitting there? Net Δ of the
    cluster's combined balance over the last `days`, as % of supply.

    A 6-month-flat fossil and a cluster that added +40% this week were emitting the
    identical '拉盘候选' string — correct state label, useless trade signal (BASED).
    Only a positive Δ is an actionable long.

    Uses strict=True: if ANY endpoint balance read fails the answer is None (unknown),
    never a fabricated Δ. A missing archive node must not look like a sell-off.
    """
    if not wallets or not total_supply:
        return None
    try:
        from src.onchain.evm_archive import ArchiveRPC, combined_balance_at
        rpc = ArchiveRPC(chain)
        if not rpc.available():
            return None
        spb = rpc.seconds_per_block()
        if not spb or spb <= 0:
            return None
        head = as_of_block if as_of_block else (rpc.latest_block() - _CONFIRMATIONS)
        then = int(head - (days * 86400) / spb)
        if then <= 0:
            return None
        addrs = list(wallets[:8])
        b_then = combined_balance_at(token, addrs, chain, then, rpc=rpc, strict=True)
        b_now = combined_balance_at(token, addrs, chain, head, rpc=rpc, strict=True)
    except Exception:
        return None
    if b_then is None or b_now is None:
        return None                       # archive gap → UNKNOWN, not a fake delta
    delta = b_now - b_then
    return {"delta_tokens": round(delta), "balance_then": round(b_then),
            "balance_now": round(b_now), "days": days,
            "delta_pct_supply": round(100.0 * delta / total_supply, 2)}


def identify_operator(token: str, chain: str, as_of_block: int | None = None) -> dict:
    """The canonical verdict. Never raises. Shape:
      {verdict, confidence, current:{...}, historical:{...}, evidence, caveats}
    verdict ∈ live_operator | exited_operator | treasury | dispersed | unknown.

    `as_of_block` REPLAYS the verdict as it would have been at that block — the basis
    of the walk-forward backtest. Everything downstream is cut off there. Two
    dimensions genuinely cannot time-travel and are therefore DISABLED under replay
    rather than silently served stale:
      - the CURRENT holder graph (`fetch_holders_evm` returns today's holders; there
        is no historical holder list), so a replayed verdict is historical-ledger-only;
      - the DexScreener market/terminal gate (today's liquidity and price).
    Serving either at a past block would leak the future into the backtest and
    manufacture an edge that does not exist. Replay is honestly weaker than live.
    """
    replay = as_of_block is not None
    out = {"token": token, "chain": chain, "verdict": "unknown", "confidence": 0,
           "current": {}, "historical": {}, "evidence": "", "caveats": [],
           "as_of_block": as_of_block}
    conc: dict = {}
    if replay:
        out["current"]["holders_fetched"] = 0
        out["current"]["current_graph_available"] = False
        out["caveats"].append(
            "replay:当前持仓图无法回溯(holder快照只有今天)→ 本次判决仅基于历史台账")
    else:
        try:
            from src.onchain.holder_snapshot import fetch_holders_evm
            from src.pipeline.anomaly_screener import effective_concentration_signal
            cid = {"bsc": 56, "ethereum": 1, "base": 8453, "arbitrum": 42161}.get(chain)
            holders = fetch_holders_evm(token, chain_id=cid, max_pages=8) or []
            conc = effective_concentration_signal(holders, token, chain) or {}
            out["current"]["holders_fetched"] = len(holders)
            out["current"] |= {
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
        out["historical"] = _historical_ledger(token, chain, dec, as_of_block=as_of_block)
    except Exception as e:
        out["historical"] = {"available": False, "exited": [], "holding": []}
        out["caveats"].append(f"historical failed: {str(e)[:60]}")

    # ---- supply + market context (F3 percent-of-supply floor, F11 terminal gate) ----
    total_supply = None
    try:
        from src.onchain.evm_archive import ArchiveRPC
        total_supply = ArchiveRPC(chain).total_supply(token)   # supply is ~static
    except Exception:
        total_supply = None
    if total_supply is None:
        out["caveats"].append("totalSupply取数失败:占比门无法执行(不以0代供应量)")
    # DexScreener is today-only: under replay a "terminal/dead pool" call would be
    # made with knowledge the past did not have. Disable it rather than back-date it.
    mkt = {"available": False, "reason": "replay"} if replay else _token_market(token)
    out["current"]["market"] = mkt
    terminal = False if replay else _is_terminal(mkt)

    # ---- age (F2): on-chain-derived, with an EXPLICIT unknown branch ----
    # The old `_token_age_days` was a flaky network call and `age is not None` made the
    # whole youth gate evaporate on a timeout — silently dropping a 5-day token into
    # the operator taxonomy. Unknown age is now its own flagged state, never a
    # fall-through.
    age = _token_age_onchain(token, chain, as_of_block=as_of_block)
    age_src = "onchain_first_transfer"
    if age is None:
        try:
            from src.pipeline.operator_sentinel import _token_age_days
            age = _token_age_days(token, chain)
            age_src = "heuristic"
        except Exception:
            age = None
    if age is None:
        age_src = "unverified"
        out["caveats"].append("age_unverified:年龄未能链上确认,年龄门未执行(结论按未验证年龄处理)")
    out["current"]["token_age_days"] = round(age, 1) if age is not None else None
    out["current"]["token_age_source"] = age_src

    conf = conc.get("cluster_confidence") or 0
    hist = out["historical"]
    exited = hist.get("exited") or []
    holding = hist.get("holding") or []
    sum_hold = sum(h.get("net_now", 0) for h in holding)
    sum_exit_in = sum(e.get("total_in", 0) for e in exited)
    lg = conc.get("largest_entity_pct") or 0
    dom = out["current"].get("dominant_wallets") or 0
    cluster_w = list(conc.get("dominant_cluster_wallets") or [])

    # F12 hysteresis: quantize noisy inputs BEFORE the cliffs, so run-to-run jitter
    # across 54↔56 or a 0.50 boundary can't flip the category. Near a cliff we emit a
    # `borderline` caveat and refuse promotion rather than commit to a side.
    conf_q = int(round(conf / 5.0) * 5)
    if abs(conf - 55) <= 3:
        out["caveats"].append(f"borderline:cluster_confidence={conf}贴近55门槛,类别不稳定")

    # F3: percent-of-supply floor for the historical loaded path. `max(sum_exit_in,1)`
    # let 3 retail diamond-hands holding 1 token clear the bar (MAME-shaped FP).
    hold_pct = (100.0 * sum_hold / total_supply) if (total_supply and sum_hold) else None
    out["current"]["holding_pct_supply"] = round(hold_pct, 2) if hold_pct else None

    def _loaded_split(wallets: list, base_conf: int, base_ev: str) -> None:
        """F8: a loaded cluster is only an actionable LONG if it is ACCUMULATING.
        A 6-month-flat fossil and a cluster that added 40% this week were both emitting
        '拉盘候选' — the BASED failure: right state label, useless trade signal."""
        vel = _cluster_velocity_30d(token, chain, wallets, total_supply,
                                    as_of_block=as_of_block)
        out["current"]["velocity_30d"] = vel
        if vel is None:
            out["verdict"] = "loaded_live_operator"
            out["confidence"] = min(base_conf, 60)
            out["evidence"] = base_ev + " | 30d速度不可得(缺archive)→不判吸筹/休眠"
            out["caveats"].append("velocity_unavailable:无法区分吸筹vs休眠,不构成做多信号")
            return
        d = vel["delta_pct_supply"]
        if d > 2:
            out["verdict"] = "loaded_accumulating"
            out["confidence"] = min(80, base_conf + 10)
            out["evidence"] = base_ev + f" | 近30d净吸筹 +{d:.1f}%供应 = 唯一可操作的做多形态"
        elif d < -2:
            out["verdict"] = "distributing"
            out["confidence"] = min(75, base_conf)
            out["evidence"] = base_ev + f" | 但近30d净减仓 {d:.1f}%供应 = 实为派发中"
        else:
            out["verdict"] = "loaded_dormant"
            out["confidence"] = min(65, base_conf)
            out["evidence"] = base_ev + f" | 近30d几乎不动({d:+.1f}%供应)= 装弹但休眠,非拉盘信号"
            out["caveats"].append("dormant:仅状态描述,不是买入理由")

    # ---- VERIFIED-LOADED runs BEFORE the youth gate (F2) ----
    # A cluster whose live on-chain balances are RPC-verified is judgeable at any age.
    # Youth only blocks the exited/distribute LIFECYCLE inference (which genuinely
    # needs a pump→distribute history to exist). The old order forced a fast-loaded
    # 9-day operator into `too_young_to_judge`.
    # `largest_entity_pct` is a share of REAL totalSupply only when supply_verified.
    # Otherwise it is a share of the fetched holder subset — shrink the list and the
    # number inflates. Gating a loaded verdict on that would manufacture operators out
    # of small holder lists, which is precisely what the WOO ghost did.
    supply_ok = bool(conc.get("supply_verified"))
    if not supply_ok and (lg or dom):
        out["caveats"].append("supply_unverified:集中度是子集比例而非供应占比,装弹门槛不执行")

    # OPERATOR vs ISSUER. A concentrated cluster that was ALLOCATED (minted to, or
    # handed the float by one distributor) is a treasury/team, not an operator — it
    # never bought, so it has no cost basis to defend and no reason to run the price.
    # Two coins in a 45-coin survey looked exactly like operators (23 wallets, one
    # funder, 67% of supply, all EOAs, owner renounced) and both were issuers.
    # Skipped under replay: today's first-inflow lookup is not the past's.
    acq = {"available": False, "verdict": "unknown"}
    if cluster_w and not replay:
        acq = acquisition_mode(token, chain, cluster_w)
        out["current"]["acquisition"] = acq
        if acq.get("verdict") == "allocated":
            out["caveats"].append(
                f"issuer_allocation:簇成员首笔入账来自铸造/单一分发地址"
                f"(买{acq['bought']}/分配{acq['allocated']}) → 集中度=发行方,非操盘")
    is_operator_acq = acq.get("verdict") != "allocated"

    loaded_cluster = (supply_ok and dom >= 5 and lg >= 10 and is_operator_acq
                      and _cluster_holds_onchain(token, chain, cluster_w, as_of_block))

    if conf_q >= 55 and acq.get("verdict") == "allocated":
        # A funder-linked cluster with high confidence that was ALLOCATED (minted /
        # single distributor) is a team/treasury, not a trading operator. This path
        # fired live_operator on MERC and DETO — both issuer allocations — because it
        # only saw the cluster SHAPE, never how the tokens were acquired.
        out["verdict"] = "treasury"
        out["confidence"] = 60
        out["evidence"] = (f"高置信隐藏簇(conf{conf})但成员为分配/铸造所得"
                           f"(买{acq['bought']}/分配{acq['allocated']})= 发行方金库,非交易操盘")
    elif conf_q >= 55:
        out["verdict"] = "live_operator"
        out["confidence"] = conf
        out["evidence"] = f"当前隐藏簇 cluster_confidence={conf}(从市场买入)"
    elif loaded_cluster:
        # LOADED-LIVE via the CURRENT holder graph (BASED): a coordinated CLUSTER
        # (>=5 wallets, not a single whale) holds a meaningful share of LIVE supply.
        # INV-4 GUARD (SYN catch): the concentration signal comes from a fetched holder
        # list that can be STALE — _cluster_holds_onchain RPC-verifies live balances>0
        # before calling it loaded (SYN's "5 wallets/10%" held 0 on-chain = false).
        _loaded_split(cluster_w, min(78, 45 + int(lg)),
                      f"当前活簇 {dom}个钱包协同持有 {lg:.0f}% 流通供应(链上余额已核实>0)")
    elif age is not None and age < 14 and conf_q < 55:
        # AGE GATE: a token too young has no pump→distribute lifecycle to judge.
        out["verdict"] = "too_young_to_judge"
        out["confidence"] = 0
        out["evidence"] = f"代币仅{age:.0f}天(<14d):拉盘→派发生命周期无法计算,不下操盘定性"
        out["caveats"].append("年龄门:<14d非可判")
        return out
    elif (hist.get("available") and len(holding) >= 5 and sum_hold > 1.5 * sum_exit_in
          and hold_pct is not None and hold_pct >= 8
          and _cluster_holds_onchain(token, chain, [h["address"] for h in holding],
                                     as_of_block)):
        # LOADED-LIVE (BASED): a coordinated set still HOLDS clearly more than it emptied.
        # F3 (red-team): >=5 wallets, sum_hold > 1.5x emptied, >=8% of REAL totalSupply
        # (not the old `max(...,1)` floor), + RPC-verified live balances — so retail
        # diamond-hands holding dust can't fabricate a loaded operator.
        # Red-team round 2: the floor must be AFFIRMATIVELY satisfied. `hold_pct is
        # None` (totalSupply read failed) used to pass the gate — a node hiccup
        # re-opened the exact dust-holder FP this branch exists to kill. Fail closed:
        # no denominator = size unproven = fall through to the referee.
        _loaded_split([h["address"] for h in holding], min(80, 45 + 5 * len(holding)),
                      f"{len(holding)}个早期重仓钱包仍持有(合计{sum_hold:,.0f}"
                      + (f",占供应{hold_pct:.1f}%" if hold_pct else "")
                      + f" > 已清空{sum_exit_in:,.0f})")
    elif hist.get("available") and exited:
        # SELL-vs-MOVE REFEREE (destination-grounded, replaces the price guess).
        # For the emptied early-heavy wallets, trace WHERE their tokens went.
        from src.onchain.cex_addresses import evm_exchanges
        # F5 (circularity): a destination is an INTERNAL MOVE only if funder-verified,
        # not merely any early-cohort wallet.
        verified_cluster = {a.lower() for a in (conc.get("dominant_cluster_wallets") or [])}
        member_set = verified_cluster or (
            {e["address"] for e in exited} | {e["address"] for e in (hist.get("holding") or [])})
        # F10-core (red-team): compute the SEED operator funders once, so a rotation
        # destination that shares an operator funder counts as a same-entity MOVE —
        # automating the manual EVAA funder-trace (dest funded by 0x661213676e = still
        # the operator). CEX/disperser funders are voided (they link unrelated wallets).
        # F13: a DISPERSER funder links thousands of unrelated wallets — using it as a
        # seed would let any two retail wallets read as "same operator". Void both
        # CEX hot wallets and dispersers, deterministically ordered.
        seed_funders = set()
        try:
            from src.onchain.funder_graph import get_funders
            from src.pipeline.anomaly_screener import funder_disperser_verdict
            cexset = evm_exchanges()
            fmap = get_funders([e["address"] for e in exited[:6]], chain)
            for f in sorted({str(v or "").lower() for v in fmap.values()} - {""}):
                # Red-team: admission requires an AFFIRMATIVE non-disperser verdict.
                # `is False` — None means the fan-out couldn't be evaluated, and an
                # unverifiable funder used as an operator seed links unrelated retail
                # (the falsified family-root over-claim). Fail closed.
                if f not in cexset and funder_disperser_verdict(f, chain) is False:
                    seed_funders.add(f)
        except Exception:
            pass
        out["current"]["seed_funders"] = sorted(seed_funders)
        pairs = _token_pairs(token, chain)
        cex = evm_exchanges()
        tot = {"sell_dex": 0.0, "sell_cex": 0.0, "move_member": 0.0, "move_eoa": 0.0,
               "to_contract": 0.0}
        resolved_n = 0
        cov_num, cov_den = 0.0, 0.0
        for e in exited[:8]:                       # bounded
            # F9: how much this wallet MUST have sent out — the convergence target.
            exp_out = max(e.get("total_in", 0) - max(e.get("net_now", 0), 0), 0.0)
            a = _exit_destinations(token, chain, e["address"], member_set, pairs, cex,
                                   seed_funders=seed_funders, expected_out=exp_out,
                                   as_of_block=as_of_block)
            if a.get("resolved"):
                resolved_n += 1
                for kk in tot:
                    tot[kk] += a.get(kk, 0.0)
                if a.get("coverage") is not None and exp_out > 0:
                    cov_num += a["coverage"] * exp_out
                    cov_den += exp_out
        sold = tot["sell_dex"] + tot["sell_cex"]
        moved_internal = tot["move_member"]
        moved_eoa = tot["move_eoa"]
        total_out = sold + moved_internal + moved_eoa + tot["to_contract"]
        # F9: value-weighted share of the expected exit we actually accounted for.
        coverage = round(cov_num / cov_den, 3) if cov_den > 0 else None
        out["current"]["exit_destinations"] = {**tot, "resolved_wallets": resolved_n,
                                               "coverage": coverage}
        # F11: how much ammo is LEFT. SIREN with 45% remaining and SIREN with 3%
        # remaining are opposite trades, and the old verdict said "distributing" to both.
        rem_pct = (100.0 * sum_hold / total_supply) if (total_supply and sum_hold) else None
        out["current"]["remaining_operator_float_pct"] = round(rem_pct, 2) if rem_pct else None
        # F12 (corrected in red-team round 2): quantize-then-compare merely RELOCATED
        # the cliff (raw 0.4749 vs 0.4751 still flipped the verdict) and the caveat
        # band only covered the above-cliff side. Real protection: RAW ratios with
        # symmetric borderline bands centered on each ACTUAL decision boundary. Inside
        # a band the verdict may still flip run-to-run, but `borderline` blocks
        # promotion/push on BOTH sides, so no user-visible signal ever flips.
        sold_frac = (sold / total_out) if total_out > 0 else 0.0
        mv_frac = (moved_internal / total_out) if total_out > 0 else 0.0
        if total_out > 0:
            if abs(sold_frac - 0.5) < 0.05:
                out["caveats"].append("borderline:卖出占比贴近50%门槛,卖/移定性不稳定")
            elif abs(sold_frac - 0.2) < 0.03:
                out["caveats"].append("borderline:卖出占比贴近20%派发门槛,定性不稳定")
            if abs(mv_frac - 0.5) < 0.05:
                out["caveats"].append("borderline:簇内回流占比贴近50%门槛,换仓定性不稳定")

        if resolved_n == 0 or total_out <= 0:
            out["verdict"] = "indeterminate_emptied"
            out["confidence"] = 25
            out["evidence"] = f"{len(exited)}个早期重仓钱包已清空,但转出去向取数失败 → 卖/移不可判"
            out["caveats"].append("去向未解析 = 不得断言操盘去留")
        elif coverage is not None and coverage < 0.6:
            # F9: we only saw a thin recent slice of the exit — the sell/move ratio on
            # that slice is not representative of where the stack actually went.
            out["verdict"] = "indeterminate_emptied"
            out["confidence"] = 25
            out["evidence"] = (f"{len(exited)}个钱包清空,但只追回{coverage*100:.0f}%的应转出量 "
                               f"→ 卖/移比例取样不足,不下定性")
            out["caveats"].append(f"coverage={coverage}:转出未追平,拒绝在薄样本上裁决")
        elif sold_frac >= 0.5:
            out["verdict"] = "exited_by_selling"
            out["confidence"] = min(85, 50 + 5 * resolved_n)
            out["evidence"] = (f"{len(exited)}个早期重仓钱包清空,转出{sold/total_out*100:.0f}%进"
                               f"LP池/CEX(卖{sold:,.0f}) = 操盘卖出离场")
            # Red-team: a FAILED MM-basket read is not a passed MM screen. If any
            # exited candidate couldn't be basket-checked, this conf-85 verdict may
            # be a market-maker desk cycling inventory — cap and say so.
            if hist.get("mm_unchecked"):
                out["confidence"] = min(out["confidence"], 60)
                out["caveats"].append(
                    f"mm_check_unavailable:{len(hist['mm_unchecked'])}个候选钱包篮子取数失败,"
                    f"MM/串行degen未排除,置信度受限")
        elif mv_frac >= 0.5:
            # moved-to-member → follow the frontier: PARKED (loaded threat) = real
            # rotation; SOLD downstream = distribution/churn, NOT a loaded operator.
            fr = _rotation_frontier(token, chain, [e["address"] for e in exited[:8]],
                                    pairs, cex, seed_funders=seed_funders,
                                    as_of_block=as_of_block)
            out["current"]["rotation_frontier"] = fr
            # H4: only FUNDER-LINKED terminals are operator ammo. `parked_unlinked` may
            # be an OTC buyer's wallet — counting it as "the operator is still loaded"
            # is the same over-claim as calling a balance→0 a sell.
            if fr["parked_in_wallets"] > fr["sold_via_frontier"] and fr["parked_in_wallets"] > 0:
                out["verdict"] = "present_rotating_confirmed"   # EVAA: parked = loaded threat
                out["confidence"] = min(85, 50 + 5 * resolved_n)
                out["evidence"] = (f"{len(exited)}个钱包清空,{mv_frac*100:.0f}%回流簇内且"
                                   f"下游{fr['parked_in_wallets']:,.0f}停在同funder新钱包 = 换钱包装弹,随时可砸")
            elif fr["sold_via_frontier"] > 0:
                # rotated then dumped downstream — distribution, and if no still-holding
                # coordinated cluster this is more likely CHURN than an operator (MAME).
                out["verdict"] = "distributing_or_churn"
                out["confidence"] = 45
                out["evidence"] = (f"{len(exited)}个钱包清空,回流簇内但下游已卖{fr['sold_via_frontier']:,.0f} "
                                   f"→ 派发或散户刷币(非装弹操盘);无仍持有的协同簇=倾向churn")
                out["caveats"].append("需genesis/degen判别区分'操盘派发'vs'散户churn'")
            elif fr.get("parked_unlinked", 0) > 0:
                out["verdict"] = "indeterminate_emptied"
                out["confidence"] = 30
                out["evidence"] = (f"{len(exited)}个钱包清空回流,下游{fr['parked_unlinked']:,.0f}停在"
                                   f"与操盘无funder关联的钱包 → 场外卖出vs自托管换钱包,不可判")
                out["caveats"].append("parked_unlinked:终点钱包非同funder,不得记为操盘余弹")
            else:
                out["verdict"] = "indeterminate_emptied"
                out["confidence"] = 30
                out["evidence"] = f"{len(exited)}个钱包清空回流簇内,下游去向未解析 → 不可判"
        elif sold_frac >= 0.2 or sold >= 10_000_000:
            # DISTRIBUTING fix (SIREN): meaningful selling into pool/CEX = distribution
            # even if some also moved. A real bleed doesn't need a 50% majority.
            out["verdict"] = "distributing"
            out["confidence"] = min(75, 45 + 5 * resolved_n)
            ammo = (f",操盘余弹约{rem_pct:.1f}%供应" if rem_pct else "")
            out["evidence"] = (f"{len(exited)}个钱包清空,已向LP池/CEX卖出{sold:,.0f}"
                               f"({sold_frac*100:.0f}%) = 操盘在派发出货{ammo}")
        else:
            out["verdict"] = "indeterminate_emptied"
            out["confidence"] = 35
            out["evidence"] = (f"{len(exited)}个钱包清空,转出主要去向为普通新EOA"
                               f"(EOA{moved_eoa:,.0f}),卖出仅{sold:,.0f} → 卖vs换钱包不可判")
            out["caveats"].append("去向多为featureless新EOA:自托管vs换钱包vs中介卖出,不可判")
    elif not hist.get("available"):
        out["verdict"] = "unknown"
        if hist.get("empty_history"):
            out["evidence"] = "全历史0条转账:地址/链可能错误或索引失败 → 无数据,不下任何定性"
        else:
            out["evidence"] = ("当前无隐藏簇,但历史台账取数失败(数据源额度耗尽,如Moralis每日/"
                               "Etherscan不覆盖该链)→ '拉盘前吸筹后离场'未验证,不能断言无庄")
        out["caveats"].append("历史维度缺失 = 结论未完成(换源/额度恢复后重跑)")
    elif not (out["current"].get("holders_fetched") or 0):
        # History says no operator footprint, but the CURRENT holder snapshot fetch
        # returned nothing (Dune 402 / Moralis parked). "Dispersed" is a claim about
        # the current graph — it cannot be asserted from zero holder data (INV-4).
        out["verdict"] = "unknown"
        out["evidence"] = "历史无操盘足迹,但当前holder快照取数失败(0条)→ '分散'不可断言"
        out["caveats"].append("当前维度缺失:holder快照为空 = 数据失败,非散户盘证据")
    else:
        # current graph fetched AND history checked with no exited-accumulator.
        # F12 (FN-2): before dumping a big single holder into `treasury`, check if it
        # is a confidently-EOA live holder — a lone trading wallet, not a Safe/vesting
        # contract. It stays a STATE observation (dormant, no coordination proof), so
        # velocity decides whether it's actually accumulating; it never claims the
        # coordinated-cluster confidence.
        lg = conc.get("largest_entity_pct") or 0
        single_op = False
        if supply_ok and lg >= 15 and dom < 5 and cluster_w:
            try:
                from src.onchain.entity_classify import classify_address
                single_op = (all(classify_address(w, chain).get("type") == "eoa"
                                 for w in cluster_w[:3])
                             and _cluster_holds_onchain(token, chain, cluster_w, as_of_block))
            except Exception:
                single_op = False
        if single_op:
            _loaded_split(cluster_w, 55,
                          f"单一/双钱包EOA持有 {lg:.0f}% 流通供应(链上已核实,非多签金库),"
                          f"历史无出货足迹")
            out["caveats"].append("single_operator:单钱包集中,无协同簇/无历史足迹,"
                                  "巨鲸vs操盘不可区分,置信度受限")
            out["confidence"] = min(out["confidence"], 60)
        else:
            out["verdict"] = "treasury" if lg >= 15 else "dispersed"
            out["confidence"] = 60
            out["evidence"] = (f"当前分散(最大{lg:.0f}%)且历史无'吸入后派发'的操盘足迹 → "
                               f"{'集中于金库/长持' if lg>=15 else '散户盘'},无操盘证据")

    # CODE RISK — a SEPARATE dimension, never folded into cluster_confidence and never
    # a gate. "Is there an operator and what are they doing" (behavioral) and "can this
    # contract rug by code" (structural) are orthogonal questions; mixing them corrupts
    # a clean behavioral signal. A missing GoPlus read reports available=False
    # ("unchecked"), never "clean". Skipped under replay: today's contract state is not
    # the past's.
    if not replay:
        try:
            from src.onchain.goplus_client import rug_risk
            out["rug_risk"] = rug_risk(token, chain)
        except Exception as e:
            out["rug_risk"] = {"available": False, "reason": str(e)[:50],
                               "note": "代码风险未检查 — 不等于安全"}
    else:
        out["rug_risk"] = {"available": False, "reason": "replay"}

    # F11 TERMINAL GATE: the pool is drained and nothing trades — whatever the operator
    # did, it already happened. Emitting a live "在派发" here is a misfire on a corpse;
    # cap confidence and say so rather than implying a tradeable event ahead.
    # Red-team: FORWARD-LOOKING loaded verdicts are exempt — a still-loaded operator
    # over a thin pool (present_rotating / loaded_*) is the most dangerous live setup,
    # not a corpse; capping it below 55 silenced the exact EVAA-class dump threat.
    # The corpse call applies to backward-looking outcomes (distributing/exited/…).
    _FORWARD = {"present_rotating_confirmed", "loaded_accumulating", "loaded_dormant",
                "loaded_live_operator", "live_operator"}
    if terminal and out["verdict"] not in _FORWARD | {"too_young_to_judge", "unknown"}:
        out["confidence"] = min(out["confidence"], 40)
        out["caveats"].append(
            f"terminal:流动性${mkt.get('liquidity_usd'):,}/24h量${mkt.get('volume_h24'):,} "
            f"→ 盘已死,event已发生,不构成前瞻交易信号")
    return out
