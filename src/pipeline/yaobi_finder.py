"""妖币发现器 — the offense the repo was built for: find operator tokens, early.

The goal from day one is to FIND 妖币 (operator-manipulated tokens that run), not to
avoid losing money. Everything the night verified now serves that: real holders (not
ghosts), supply-verified concentration, and — the sharp one — bought-from-market vs
issuer-allocated. So when this surfaces an operator, it's a REAL one.

Why a wide continuous scan and not a one-shot: a one-shot of 79 trending tokens found
exactly one operator, and it was one we already had. Two reasons —
  · trending is TOO LATE: by the time a token trends, the operator is distributing.
  · the operator signature is RARE (~1% of tokens), so you must scan MANY to catch a
    few, in the accumulation window (young, not-yet-trending).
So this pages the NEW-pool feeds across chains, filters to the accumulation window
(days old, tradeable, some volume), analyses each, and PERSISTS finds to a watchlist
that accumulates over runs — with the first-seen price, so forward performance is
measurable, not asserted.

The honest boundary, stated in the output: this FINDS real operators (a watchlist).
It does NOT claim which will pump — a real operator can sit dormant (BASED is one, and
it's flat). Timing is the user's call on top of a clean, verified list.

    python -m src.pipeline.yaobi_finder            # scan + print the watchlist
"""

from __future__ import annotations

import json
import sqlite3
import time
import urllib.request
from datetime import datetime, timezone

import structlog

from src.config import DATA_DIR

logger = structlog.get_logger()

DB_PATH = DATA_DIR / "yaobi_watchlist.db"
_CID = {"bsc": 56, "base": 8453, "ethereum": 1, "arbitrum": 42161}
# The accumulation window. MIN was 1.0 day, which excluded the ENTIRE new_pools feed
# (it lists pools hours old) — a scan pre-filtered to 1 candidate. The window starts at
# a few hours (past the instant-launch churn) and runs to 21 days; tokens caught young
# are persisted and re-analysed as they age INTO the accumulation signature.
MIN_AGE_DAYS, MAX_AGE_DAYS = 0.2, 21.0
MIN_LIQ_USD = 25_000         # tradeable
MIN_VOL24_USD = 15_000       # actually trading, not a dead pool


def _conn() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(str(DB_PATH), timeout=10)
    c.execute("""CREATE TABLE IF NOT EXISTS finds (
        token TEXT, chain TEXT, symbol TEXT,
        first_seen TEXT, price0 REAL, liq0 REAL, mcap0 REAL, age_days0 REAL,
        op_score REAL, shape TEXT, acquisition TEXT, largest_pct REAL, gap REAL,
        cluster_n INTEGER, direction TEXT, signals TEXT,
        PRIMARY KEY (token, chain))""")
    if "direction" not in {r[1] for r in c.execute("PRAGMA table_info(finds)")}:
        c.execute("ALTER TABLE finds ADD COLUMN direction TEXT")
        c.execute("ALTER TABLE finds ADD COLUMN signals TEXT")
    # CAPTURE-AND-MONITOR funnel: the operator signature is ~1% and develops over
    # DAYS, so a single scan of the sparse young-tradeable window finds ~0. Instead,
    # every fresh candidate that clears the cheap pre-filter is captured here, and
    # re-analysed on later scans as it ages INTO the accumulation window. This widens
    # the funnel from a handful per scan to everything that has passed through the
    # window recently — with full verification kept (real holders, supply, acquisition).
    c.execute("""CREATE TABLE IF NOT EXISTS monitor (
        token TEXT, chain TEXT, symbol TEXT, first_seen TEXT, last_checked TEXT,
        checks INTEGER DEFAULT 0, promoted INTEGER DEFAULT 0,
        PRIMARY KEY (token, chain))""")
    return c


def _monitor_add(cands: list[dict]) -> None:
    """Record fresh candidates for later re-analysis as they mature."""
    c = _conn()
    now = datetime.now(timezone.utc).isoformat()
    try:
        for r in cands:
            c.execute("INSERT OR IGNORE INTO monitor (token, chain, symbol, first_seen, "
                      "last_checked) VALUES (?,?,?,?,?)",
                      (r["address"].lower(), r["chain"], r.get("symbol", "?"), now, now))
        c.commit()
    finally:
        c.close()


def _monitor_due(max_age_days: float = MAX_AGE_DAYS, limit: int = 80) -> list[dict]:
    """Monitored tokens still inside the accumulation window and not yet promoted,
    least-recently-checked first (so re-analysis rotates through the pool)."""
    from datetime import timedelta
    cutoff = (datetime.now(timezone.utc) - timedelta(days=max_age_days)).isoformat()
    c = _conn()
    try:
        rows = c.execute(
            "SELECT token, chain, symbol FROM monitor WHERE promoted=0 AND first_seen>=? "
            "ORDER BY last_checked ASC LIMIT ?", (cutoff, limit)).fetchall()
    finally:
        c.close()
    return [{"token": r[0], "chain": r[1], "symbol": r[2]} for r in rows]


def _monitor_touch(token: str, chain: str, promoted: bool = False) -> None:
    c = _conn()
    try:
        c.execute("UPDATE monitor SET last_checked=?, checks=checks+1, promoted=? "
                  "WHERE token=? AND chain=?",
                  (datetime.now(timezone.utc).isoformat(), 1 if promoted else 0,
                   token.lower(), chain))
        c.commit()
    finally:
        c.close()


def _gather_young(chains=("bsc", "base"), pages: int = 3) -> list[dict]:
    """Fresh-pool candidates across chains, pre-filtered CHEAPLY by age/liq/volume
    before any expensive holder analysis. new_pools is the accumulation-window feed;
    trending is deliberately excluded (it's the post-pump feed)."""
    from src.pipeline.anomaly_screener import _gt_base_addresses
    _NET = {"bsc": "bsc", "base": "base", "ethereum": "eth", "arbitrum": "arbitrum_one"}
    seen: set = set()
    addrs: list[tuple] = []
    for ch in chains:
        net = _NET.get(ch)
        if not net:
            continue
        for pg in range(1, pages + 1):
            # new_pools = fresh launches; volume-desc = active young movers not yet
            # trending. Both feed the age filter below; trending is excluded (post-pump).
            for feed in (f"networks/{net}/new_pools?page={pg}",
                         f"networks/{net}/pools?page={pg}&sort=h24_volume_usd_desc"):
                for a in _gt_base_addresses(feed):
                    if a.lower() not in seen:
                        seen.add(a.lower())
                        addrs.append((a, ch))
                time.sleep(1.4)
    # cheap DexScreener enrich + filter to the accumulation window
    out = []
    for a, ch in addrs:
        try:
            req = urllib.request.Request(
                f"https://api.dexscreener.com/tokens/v1/{ch}/{a}",
                headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=12) as response:
                d = json.loads(response.read())
        except Exception:
            continue
        pr = max((x for x in (d if isinstance(d, list) else [])),
                 key=lambda x: float((x.get("liquidity") or {}).get("usd") or 0), default=None)
        if not pr:
            continue
        liq = float((pr.get("liquidity") or {}).get("usd") or 0)
        vol = float(pr.get("volume", {}).get("h24") or 0)
        ms = pr.get("pairCreatedAt") or 0
        age = (time.time() * 1000 - ms) / 86_400_000 if ms else None
        if age is None or not (MIN_AGE_DAYS <= age <= MAX_AGE_DAYS):
            continue
        if liq < MIN_LIQ_USD or vol < MIN_VOL24_USD:
            continue
        out.append({"address": a, "chain": ch,
                    "symbol": (pr.get("baseToken") or {}).get("symbol", "?"),
                    "price": float(pr.get("priceUsd") or 0), "liq": liq, "vol": vol,
                    "age_days": round(age, 1),
                    "mcap": float(pr.get("marketCap") or pr.get("fdv") or 0),
                    "ch24": (pr.get("priceChange") or {}).get("h24"),
                    "txns": pr.get("txns") or {}})
        time.sleep(0.25)
    return out


def _buy_pressure(cand: dict) -> dict:
    """Leading-ish demand proxy from trade counts: buy/sell ratio + whether buying is
    ACCELERATING in the last hour. Holder growth itself lags (it IS the pump); buy
    pressure that is rising is the earliest cheap read of demand building."""
    tx = cand.get("txns") or {}
    b1, s1 = tx.get("h1", {}).get("buys", 0), tx.get("h1", {}).get("sells", 0)
    b6 = tx.get("h6", {}).get("buys", 0)
    ratio_h1 = b1 / max(s1, 1)
    accelerating = b1 * 6 > b6 * 1.2 and b1 >= 10
    return {"ratio_h1": round(ratio_h1, 2), "accelerating": accelerating,
            "buys_h1": b1, "sells_h1": s1}


def classify(cand: dict) -> dict | None:
    """DUAL classifier — 会砸(short) or 会涨(long) or None.

    The key insight the research forced: concentration is a DUMP tell, not a run tell.
    A hidden operator cluster distributes into the first pump; the tokens that RUN have
    HEALTHY distribution + smart money entering + buy pressure building. So concentration
    routes to SHORT, and a separate healthy+smart-money profile routes to LONG. Same
    verification either way (real holders, supply-verified, bought-vs-allocated).

      SHORT (会砸): a VERIFIED operator cluster (concentration, bought-not-allocated) —
                   loaded to dump — or heavy sell pressure on a still-concentrated bag.
      LONG  (会涨): healthy distribution (largest < 20%, no hidden cluster) + buy
                   pressure (ratio > 1.5, accelerating) + smart-money convergence +
                   clean contract. Concentration here is a DISQUALIFIER.
    """
    from src.onchain.holder_snapshot import fetch_holders_evm
    from src.onchain.operator_id import acquisition_mode
    from src.pipeline.anomaly_screener import effective_concentration_signal
    ch, tok = cand["chain"], cand["address"]
    cid = _CID.get(ch)
    if not cid:
        return None
    try:
        holders = fetch_holders_evm(tok, chain_id=cid, max_pages=3) or []
        if not holders:
            return None
        sig = effective_concentration_signal(holders, tok, ch) or {}
    except Exception:
        return None
    if not sig.get("supply_verified"):
        # subset ratio ≠ supply share. But a supply-RPC OUTAGE makes EVERY token fail
        # this, and an empty scan then looks like 'no operators found' when it's really
        # 'couldn't check'. Log it so a run of these is visible as an outage, not read
        # as a clean scan (the failure-becomes-conclusion trap).
        logger.info("yaobi_supply_unverified", token=cand.get("address"), chain=ch)
        return None
    lg = sig.get("largest_entity_pct") or 0
    gap = sig.get("concentration_gap") or 0
    cw = sig.get("dominant_cluster_wallets") or []
    bp = _buy_pressure(cand)

    base = {**cand, "largest_pct": round(lg, 1), "gap": round(gap, 1),
            "cluster_n": len(cw), "buy_pressure": bp}

    # ---- SHORT (会砸): verified operator concentration = loaded to dump ----
    if gap >= 6 or (lg >= 15 and len(cw) >= 3):
        acq = acquisition_mode(tok, ch, cw) if cw else {"verdict": "unknown"}
        if acq.get("verdict") == "allocated":
            return None                        # issuer treasury, not a tradeable setup
        score = gap * 4 + min(lg, 30) * 0.5
        if acq.get("verdict") == "bought":
            score += 20
        # "already selling" bonus only when there is REAL sell data — a monitored token
        # with missing txns has ratio 0, which is < 0.7 but means 'no data', not
        # 'heavy selling'. Guarding on sells_h1 > 0 stops a missing read inflating the
        # short score and biasing the whole ranking.
        if bp["sells_h1"] > 0 and bp["ratio_h1"] < 0.7:
            score += 15
        shape = "隐藏簇·装弹" if gap >= 6 else "高集中·装弹"
        return {**base, "direction": "short", "op_score": round(score, 1),
                "shape": shape, "acquisition": acq.get("verdict"),
                "signals": f"集中{lg:.0f}% gap{gap:.0f} 买卖比{bp['ratio_h1']}"}

    # ---- LONG (会涨): healthy + demand + smart money + clean ----
    if lg >= 15:                                # a single ~15%+ entity ≠ healthy for a
        return None                             # clean run (tightened from 20 — the
                                                # research floor is "top holder well
                                                # under 20%").
    # demand gate (cheap pre-filter before the expensive smart-money check): a token
    # is worth the smart-money lookup if buying is ACCELERATING, or buy dominance is
    # simply strong (ratio >= 2.5) — a 4.5 buy/sell ratio is real demand even if steady,
    # and the trace showed the accelerating-only rule wrongly rejecting it. Smart money
    # (gate 5) remains the real discriminator; this only gates who reaches it.
    if not (bp["ratio_h1"] >= 1.5 and (bp["accelerating"] or bp["ratio_h1"] >= 2.5)):
        return None                            # no demand building → not a long yet
    # clean contract gate (cheap-ish) before the expensive smart-money check
    try:
        from src.onchain.goplus_client import rug_risk
        rr = rug_risk(tok, ch)
        if rr.get("available"):
            f = rr.get("flags") or {}
            # STRUCTURED flags, not Chinese substring matching — the substring form had
            # "可增发且" which never matched the real fact "可增发(...", so mintable
            # tokens slipped through the long gate. A LONG is something you'd buy, so
            # any of these disqualifies:
            crit = (f.get("is_honeypot") == 1 or f.get("owner_change_balance") == 1
                    or f.get("transfer_pausable") == 1 or f.get("can_take_back_ownership") == 1
                    or (f.get("is_mintable") == 1 and rr.get("owner_renounced") is not True)
                    or (rr.get("owner_renounced") is None
                        and any("不可信" in x for x in rr.get("facts", []))))
            if crit:
                return None                    # rug/mint/pause risk disqualifies a long
    except Exception:
        pass
    # smart money — the LEADING signal, expensive, run last only for long-eligible
    try:
        from src.onchain.smart_money import convergence
        conv = convergence(tok, ch, max_check=12)
    except Exception:
        conv = {"verdict": "unknown", "skilled_entities": 0}
    sk = conv.get("skilled_entities", 0)          # behavior-deduped independent actors
    if conv.get("verdict") != "convergence":
        # Require TRUE convergence (>=3 INDEPENDENT actors), not "some" (1-2). One
        # skilled buyer is noise, and — the CZBULL lesson — a wallet farm churning its
        # own token collapses to a single actor that would otherwise read as "some".
        # A LONG only earns the label when several independent proven-profitable
        # wallets converge; that is the whole "聪明钱进场" thesis.
        return None
    score = 20 + sk * 15 + min(bp["ratio_h1"], 5) * 4
    if cand["age_days"] <= 10:
        score += 10
    return {**base, "direction": "long", "op_score": round(score, 1),
            "shape": f"健康分散·聪明钱进场×{sk}", "acquisition": "-",
            "signals": f"分散(最大{lg:.0f}%) 买卖比{bp['ratio_h1']} 聪明钱{sk} "
                       f"({conv.get('verdict')})"}


def _dex_candidate(token: str, chain: str) -> dict | None:
    """Re-fetch a monitored token's current market data so analyze() can run on it."""
    try:
        req = urllib.request.Request(
            f"https://api.dexscreener.com/tokens/v1/{chain}/{token}",
            headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=12) as response:
            d = json.loads(response.read())
    except Exception:
        return None
    pr = max((x for x in (d if isinstance(d, list) else [])),
             key=lambda x: float((x.get("liquidity") or {}).get("usd") or 0), default=None)
    if not pr:
        return None
    ms = pr.get("pairCreatedAt") or 0
    age = (time.time() * 1000 - ms) / 86_400_000 if ms else None
    liq = float((pr.get("liquidity") or {}).get("usd") or 0)
    if age is None or liq < MIN_LIQ_USD:
        return None
    return {"address": token, "chain": chain,
            "symbol": (pr.get("baseToken") or {}).get("symbol", "?"),
            "price": float(pr.get("priceUsd") or 0), "liq": liq,
            "txns": pr.get("txns") or {},      # BUG FIX: without this every MONITORED
            # token re-analysed by the funnel had buy_pressure 0 → the long path was
            # structurally unreachable for the entire capture-and-monitor mechanism.
            "vol": float(pr.get("volume", {}).get("h24") or 0), "age_days": round(age, 1),
            "mcap": float(pr.get("marketCap") or pr.get("fdv") or 0),
            "ch24": (pr.get("priceChange") or {}).get("h24")}


def scan(chains=("bsc", "base"), pages: int = 3, max_analyze: int = 40) -> list[dict]:
    """One pass of the CAPTURE-AND-MONITOR funnel:
      1. gather fresh young candidates, capture them to the monitor pool;
      2. analyse the fresh ones AND the maturing monitored ones;
      3. promote any that now show a verified operator signature to the watchlist.
    The signature develops over days, so re-checking the pool is what turns a sparse
    per-scan yield into an accumulating list."""
    fresh = _gather_young(chains, pages)
    _monitor_add(fresh)                       # capture everything young for later
    due = _monitor_due()                      # maturing tokens to re-check
    logger.info("yaobi_gathered", fresh=len(fresh), monitored_due=len(due))

    finds = []
    analyzed = 0
    # fresh first, then rotate through the monitor pool up to the budget
    work = [(c, False) for c in fresh] + [(m, True) for m in due]
    seen: set = set()
    for item, from_monitor in work:
        if analyzed >= max_analyze:
            break
        tok, ch = item.get("address") or item.get("token"), item["chain"]
        if (tok.lower(), ch) in seen:
            continue
        seen.add((tok.lower(), ch))
        cand = _dex_candidate(tok, ch) if from_monitor else item
        if not cand:
            continue
        analyzed += 1
        r = classify(cand)
        if from_monitor:
            _monitor_touch(tok, ch, promoted=bool(r))
        if r:
            finds.append(r)
            _persist(r)
        time.sleep(0.2)
    finds.sort(key=lambda r: -r["op_score"])
    logger.info("yaobi_scan_done", analyzed=analyzed, found=len(finds),
                monitor_pool=_monitor_size())
    return finds


def _monitor_size() -> int:
    c = _conn()
    try:
        return c.execute("SELECT COUNT(*) FROM monitor WHERE promoted=0").fetchone()[0]
    finally:
        c.close()


def _persist(r: dict) -> None:
    c = _conn()
    try:
        c.execute("""INSERT OR IGNORE INTO finds
            (token, chain, symbol, first_seen, price0, liq0, mcap0, age_days0,
             op_score, shape, acquisition, largest_pct, gap, cluster_n, direction, signals)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                  (r["address"].lower(), r["chain"], r["symbol"],
                   datetime.now(timezone.utc).isoformat(), r["price"], r["liq"],
                   r["mcap"], r["age_days"], r["op_score"], r["shape"],
                   r.get("acquisition"), r["largest_pct"], r["gap"], r["cluster_n"],
                   r.get("direction"), r.get("signals")))
        c.commit()
    finally:
        c.close()


def watchlist(limit: int = 40) -> list[dict]:
    """The accumulated find list, newest-scored first."""
    c = _conn()
    try:
        cols = [d[0] for d in c.execute("SELECT * FROM finds LIMIT 0").description]
        rows = c.execute("SELECT * FROM finds ORDER BY first_seen DESC LIMIT ?",
                         (limit,)).fetchall()
    finally:
        c.close()
    return [dict(zip(cols, r)) for r in rows]


def main() -> None:
    finds = scan()
    longs = [r for r in finds if r.get("direction") == "long"]
    shorts = [r for r in finds if r.get("direction") == "short"]
    print(f"本轮: 🟢会涨(多) {len(longs)}  🔴会砸(空) {len(shorts)}\n")
    for r in longs[:10]:
        print(f"  🟢 {r['symbol']:10} [{r['chain']:4}] 龄{r['age_days']}d mc${r['mcap']/1e6:.1f}M {r['signals']}")
    for r in shorts[:10]:
        print(f"  🔴 {r['symbol']:10} [{r['chain']:4}] 龄{r['age_days']}d mc${r['mcap']/1e6:.1f}M {r['signals']}")
    wl = watchlist()
    print(f"\n累计名单(共 {len(wl)}): 多 {sum(1 for r in wl if r.get('direction')=='long')} / "
          f"空 {sum(1 for r in wl if r.get('direction')=='short')}")
    print("\n会涨=健康分散+聪明钱进场+买压;会砸=核实操盘装弹。均已核实真holder/供应,无幽灵无发行方。")
    print("这是【发现】,不是保证。时机与仓位是你在干净名单之上的判断。")


if __name__ == "__main__":
    main()
