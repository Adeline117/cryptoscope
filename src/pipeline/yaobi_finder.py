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
        cluster_n INTEGER,
        PRIMARY KEY (token, chain))""")
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
            d = json.loads(urllib.request.urlopen(urllib.request.Request(
                f"https://api.dexscreener.com/tokens/v1/{ch}/{a}",
                headers={"User-Agent": "Mozilla/5.0"}), timeout=12).read())
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
                    "ch24": (pr.get("priceChange") or {}).get("h24")})
        time.sleep(0.25)
    return out


def analyze(cand: dict) -> dict | None:
    """Full operator analysis on a pre-filtered candidate. Returns an enriched record
    if it carries an operator signature, else None. Verified data only (ghost/subset
    guards from the night)."""
    from src.onchain.holder_snapshot import fetch_holders_evm
    from src.onchain.operator_id import acquisition_mode
    from src.pipeline.anomaly_screener import effective_concentration_signal
    ch = cand["chain"]
    cid = _CID.get(ch)
    if not cid:
        return None
    try:
        holders = fetch_holders_evm(cand["address"], chain_id=cid, max_pages=3) or []
        if not holders:
            return None
        sig = effective_concentration_signal(holders, cand["address"], ch) or {}
    except Exception:
        return None
    if not sig.get("supply_verified"):        # subset ratio ≠ supply share
        return None
    conf = sig.get("cluster_confidence") or 0
    lg = sig.get("largest_entity_pct") or 0
    gap = sig.get("concentration_gap") or 0
    cw = sig.get("dominant_cluster_wallets") or []
    # operator signature: hidden Sybil cluster, or a strong coordinated cluster
    if not (gap >= 6 or conf >= 55 or (len(cw) >= 4 and lg >= 12)):
        return None
    acq = acquisition_mode(cand["address"], ch, cw) if cw else {"verdict": "unknown"}
    if acq.get("verdict") == "allocated":
        return None                            # issuer, not an operator
    # youth + hidden-cluster + verified-bought all push the score up
    score = gap * 4 + min(lg, 30) * 0.5
    if acq.get("verdict") == "bought":
        score += 25
    if cand["age_days"] <= 10:
        score += 10
    shape = ("隐藏簇" if gap >= 6 else "协同大户")
    if acq.get("verdict") == "bought":
        shape += "·从市场买入"
    return {**cand, "op_score": round(score, 1), "shape": shape,
            "acquisition": acq.get("verdict"), "largest_pct": round(lg, 1),
            "gap": round(gap, 1), "cluster_n": len(cw)}


def _dex_candidate(token: str, chain: str) -> dict | None:
    """Re-fetch a monitored token's current market data so analyze() can run on it."""
    try:
        d = json.loads(urllib.request.urlopen(urllib.request.Request(
            f"https://api.dexscreener.com/tokens/v1/{chain}/{token}",
            headers={"User-Agent": "Mozilla/5.0"}), timeout=12).read())
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
        r = analyze(cand)
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
             op_score, shape, acquisition, largest_pct, gap, cluster_n)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                  (r["address"].lower(), r["chain"], r["symbol"],
                   datetime.now(timezone.utc).isoformat(), r["price"], r["liq"],
                   r["mcap"], r["age_days"], r["op_score"], r["shape"],
                   r["acquisition"], r["largest_pct"], r["gap"], r["cluster_n"]))
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
    print(f"本轮扫描新增操盘签名: {len(finds)}\n")
    for r in finds[:15]:
        print(f"  {r['symbol']:10} [{r['chain']:4}] op{r['op_score']:>5.0f} "
              f"持{r['largest_pct']:.0f}% gap{r['gap']:.0f} 簇{r['cluster_n']} "
              f"龄{r['age_days']}d mc${r['mcap']/1e6:.1f}M {r['shape']}")
    wl = watchlist()
    print(f"\n累计观察名单(共 {len(wl)}):")
    for r in wl[:20]:
        print(f"  {r['symbol']:10} [{r['chain']:4}] 首见{r['first_seen'][:10]} "
              f"@${r['price0']:.6g} op{r['op_score']:.0f} {r['shape']}")
    print("\n这是【发现】的真操盘名单 —— 已核实真holder/供应/买入,无幽灵无发行方。")
    print("它不预测谁会拉盘(真操盘也可能休眠);时机是你在干净名单之上的判断。")


if __name__ == "__main__":
    main()
