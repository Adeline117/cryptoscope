"""Falsifier-board data exporter — renders the board's pre-shaped JSON views.

The board is the read surface of the falsifier engine: cryptoscope answers "is this
token's structure an operator, an issuer bag, or a wash farm"; the Arena side answers
"is this trader's record repeatable skill or tail luck". This module renders both
into small per-view JSON files (growthepie pattern: one pre-shaped file per view,
frontend does ZERO aggregation) and pushes them to Vercel Blob.

Honesty rules baked in (non-negotiable):
  · every payload carries schema_version / generated_at / next_expected_at, so the
    page can show a staleness banner instead of impersonating freshness;
  · every concentration number carries its supply_verified flag;
  · trader verdicts are GATES-FIRST (insufficient_data before anything) — the
    literature says real skill is a <1-2% tail, so the default verdict is honest
    ignorance, never a score;
  · anything we cannot compute from the public Arena API is explicitly
    "unavailable", never imputed.

    python -m src.pipeline.board_export            # render + push
    python -m src.pipeline.board_export --dry-run  # render only, print paths
"""

from __future__ import annotations

import json
import os
import urllib.request
from datetime import datetime, timedelta, timezone

import structlog

from src.config import DATA_DIR

logger = structlog.get_logger()

SCHEMA_VERSION = 1
EXPORT_DIR = DATA_DIR / "board_export"
REFRESH_MIN = 45                      # scheduler cadence; drives next_expected_at

# v1 endpoint: cross-platform in one call, flat fields, no required platform param
# (v2 requires platform=). Verified live: max_drawdown and win_rate arrive as PERCENT
# values (mdd 0.49..59.6 continuous, win_rate 1.1..99.9) — no unit heuristics.
ARENA_API = "https://www.arenafi.org/api/rankings"


# --------------------------------------------------------------------------- #
# operators view — from the sentinel verdict engine
# --------------------------------------------------------------------------- #
def _analyze_sentinel(v: dict) -> dict:
    """Verdict record for one sentinel. Never raises."""
    from src.onchain.operator_id import identify_operator
    tok, ch, sym = v.get("token"), v.get("chain"), v.get("symbol", "?")
    rec = {"symbol": sym, "chain": ch, "token": tok}
    try:
        r = identify_operator(tok, ch)
        cur = r.get("current", {}) or {}
        mkt = cur.get("market", {}) or {}
        acq = cur.get("acquisition", {}) or {}
        rec.update({
            "ok": True,
            "verdict": r.get("verdict"), "confidence": r.get("confidence"),
            "acquisition": acq.get("verdict"),
            "current_graph_available": cur.get("current_graph_available"),
            "supply_verified": cur.get("supply_verified"),
            "largest_entity_pct": cur.get("largest_entity_pct"),
            "largest_address_pct": cur.get("largest_address_pct"),
            "dominant_wallets": cur.get("dominant_wallets"),
            "entity_count": cur.get("entity_count"),
            "liquidity_usd": mkt.get("liquidity_usd"),
            "volume_h24": mkt.get("volume_h24"),
            "caveats": (r.get("caveats") or [])[:3],
        })
        rec.update(_dex_direction(tok, ch))
    except Exception as e:
        rec.update({"ok": False, "error": str(e)[:120]})
    return rec


def render_operators() -> dict:
    """One record per registered sentinel, from identify_operator. Each verdict is
    minutes of RPC; run at concurrency 3 (proven safe vs rate limits) so 12 sentinels
    finish in ~one identify_operator's wall time, not twelve. Runs in the scheduler's
    export job, never in a request path."""
    from concurrent.futures import ThreadPoolExecutor

    sf = DATA_DIR / "operator_sentinels.json"
    sentinels = json.loads(sf.read_text()) if sf.exists() else {}
    targets = [v for v in sentinels.values() if v.get("token")]
    with ThreadPoolExecutor(max_workers=3) as ex:
        records = list(ex.map(_analyze_sentinel, targets))
    return _envelope({"operators": records})


def _rug_flag(token: str, chain: str) -> dict:
    """GoPlus (keyless) → an AVOID/caution/clean badge. #5 folded into opportunities:
    a token smart money is buying that's ALSO a honeypot is a trap, not a find. Never
    calls a failed check 'safe'."""
    try:
        from src.onchain.goplus_client import rug_risk
        rr = rug_risk(token, chain)
    except Exception as e:
        return {"level": "unchecked", "facts": [], "reason": str(e)[:60]}
    if not rr.get("available"):
        return {"level": "unchecked", "facts": [], "reason": rr.get("reason")}
    facts = rr.get("facts") or []
    hard = any(any(w in f for w in ("蜜罐", "owner 可直接改", "可暂停", "可收回", "隐藏 owner"))
               for f in facts)
    level = "avoid" if hard else ("caution" if facts else "clean")
    return {"level": level, "facts": facts[:4], "lp_locked": rr.get("lp_all_locked")}


def _cielo_smart_buys(chains: str = "eth,bsc,base,solana", list_id: int = 75168,
                      min_usd: int = 500, limit: int = 100) -> list | None:
    """If CIELO_API_KEY is set, pull recent BUYS by Cielo's curated Smart Money list
    (id 75168) and aggregate by token = 'N curated smart wallets bought X recently'.
    Returns None when no key (caller falls back to the home-grown radar). Research-
    verified endpoint; stays dormant until a free key is added to .env."""
    key = os.environ.get("CIELO_API_KEY", "")
    if not key:
        return None
    try:
        url = (f"https://feed-api.cielo.finance/api/v1/feed?list={list_id}"
               f"&txTypes=swap&newTrades=true&chains={chains}&minUSD={min_usd}&limit={limit}")
        req = urllib.request.Request(url, headers={"X-API-KEY": key,
                                                   "User-Agent": "falsifier-board/0.1"})
        d = json.loads(urllib.request.urlopen(req, timeout=20).read())
    except Exception as e:
        logger.warning("cielo_fetch_failed", error=str(e)[:100])
        return None
    items = d.get("data", {}).get("items") or d.get("items") or d.get("data") or []
    by_token: dict = {}
    for r in items if isinstance(items, list) else []:
        # a BUY = the wallet received token1 paying token0; Cielo marks direction, but
        # be defensive about field names across versions.
        tok = (r.get("token1_address") or r.get("token_address")
               or (r.get("token1") or {}).get("address"))
        sym = (r.get("token1_symbol") or r.get("symbol")
               or (r.get("token1") or {}).get("symbol") or "?")
        ch = r.get("chain") or r.get("network")
        wallet = r.get("wallet") or r.get("from")
        usd = float(r.get("token1_amount_usd") or r.get("amount_usd") or r.get("value_usd") or 0)
        if not tok:
            continue
        e = by_token.setdefault(tok.lower(), {"symbol": sym, "chain": ch, "token": tok,
                                              "wallets": set(), "usd": 0.0})
        if wallet:
            e["wallets"].add(str(wallet).lower())
        e["usd"] += usd
    out = []
    for e in by_token.values():
        n = len(e["wallets"])
        out.append({"symbol": e["symbol"], "chain": e["chain"], "token": e["token"],
                    "smart_actors": n, "smart_wallets_seen": n, "farm_collapsed": 0,
                    "usd_bought": round(e["usd"]), "strength": "强" if n >= 3 else "弱",
                    "sample_wallets": list(e["wallets"])[:4],
                    "rug": _rug_flag(e["token"], e["chain"]) if e["chain"] in ("bsc", "base", "ethereum") else {"level": "unchecked", "facts": []}})
    out.sort(key=lambda x: -x["smart_actors"])
    return out


def render_opportunities(chains=("bsc", "base", "ethereum", "arbitrum"),
                         max_scan: int = 30) -> dict:
    """THE offense view: fresh low-float tokens that PROVEN-PROFITABLE, INDEPENDENT
    wallets are buying right now — category #1 (get in early on the diffusion curve).

    Reuses the hardened pieces: _gather_young (fresh-pool discovery), convergence
    (realized-PnL skilled wallets, with the wallet-farm collapse so a churn farm can't
    fake it), _buy_pressure. Shows the actual buyer wallets so 'what are they buying'
    is literal and verifiable. Ranked by how many INDEPENDENT skilled actors are in,
    then freshness. Honest strength: 强 = >=3 independent actors (true convergence),
    弱 = 1-2 (a lead, not a conviction).
    """
    from src.onchain.smart_money import convergence
    from src.pipeline.yaobi_finder import _buy_pressure, _gather_young

    # #1 STRONGEST PATH: GMGN's curated smart-money rank via FlareSolverr — ALL chains
    # incl Solana, with bot/sniper reverse-tells + native honeypot/tax/LP safety, one
    # call per chain. Far better than the slow home-grown convergence. Dormant when
    # FlareSolverr is down (returns None → fall through).
    try:
        from src.onchain import gmgn
        gm = gmgn.opportunities(chains=("sol", "bsc", "base", "eth"), min_smart=2)
    except Exception as e:
        logger.warning("gmgn_failed", error=str(e)[:100])
        gm = None
    if gm is not None:
        # normalize to the card shape (add token alias + smart_actors for the UI)
        for o in gm:
            o["token"] = o.get("address")
            o["smart_actors"] = o.get("smart_money")
            o["age_days"] = (o.get("age_hours") or 0) / 24 if o.get("age_hours") else None
            o["liq"] = o.get("liquidity")
        return _envelope({"opportunities": gm, "scanned": len(gm), "source": "GMGN 策展聪明钱(全链)",
                          "note": ("GMGN 策展的聪明钱正在买的新币,按聪明钱数排,全链含 Solana。"
                                   "同时显示狙击/机器人数(反指标)和合约安全。诚实:你看到时已落后其入场,多数会归零。")})

    # #1 second path: Cielo curated list (if key present).
    cielo = _cielo_smart_buys()
    if cielo is not None:
        return _envelope({"opportunities": cielo, "scanned": None, "source": "Cielo 策展聪明钱名单",
                          "note": ("Cielo 策展的已验证聪明钱正在买入的币,按买入钱包数排。"
                                   "已带 GoPlus 避雷。诚实边界:你看到时已落后其入场,多数会归零。")})

    try:
        fresh = _gather_young(chains, pages=2)
    except Exception as e:
        return _envelope({"opportunities": [], "scanned": 0, "scan_error": str(e)[:120],
                          "source": "自建雷达"})

    out = []
    scanned = 0
    for cand in fresh[:max_scan]:
        tok, ch = cand.get("address"), cand["chain"]
        if not tok:
            continue
        scanned += 1
        try:
            conv = convergence(tok, ch, max_check=12)
        except Exception:
            continue
        actors = conv.get("skilled_entities", 0)          # behavior-deduped
        if actors < 1:                                     # no proven-profitable buyer → skip
            continue
        bp = _buy_pressure(cand)
        wallets = [w.get("wallet") for w in (conv.get("skilled_wallets") or [])][:4]
        out.append({
            "symbol": cand.get("symbol", "?"), "chain": ch, "token": tok,
            "age_days": cand.get("age_days"),
            "price": cand.get("price"), "liq": cand.get("liq") or cand.get("liquidity"),
            "mcap": cand.get("mcap"),
            "smart_actors": actors,                        # independent proven-profit wallets
            "smart_wallets_seen": conv.get("skilled_wallets_n", actors),
            "farm_collapsed": (conv.get("skilled_wallets_n", actors) or 0) - actors,
            "buy_ratio_h1": bp.get("ratio_h1"),
            "buys_h1": bp.get("buys_h1"), "sells_h1": bp.get("sells_h1"),
            "strength": "强" if actors >= 3 else "弱",
            "sample_wallets": wallets,
            "rug": _rug_flag(tok, ch),          # #5 avoid-flag folded in
        })
    out.sort(key=lambda x: (-(x["smart_actors"] or 0), x["age_days"] if x["age_days"] is not None else 999))
    return _envelope({"opportunities": out, "scanned": scanned, "source": "自建雷达(慢/稀疏)",
                      "note": ("聪明钱=有已实现盈利历史、且相互独立(非同一钱包农场)的钱包正在买入。"
                               "强=≥3个独立主体收敛;弱=1-2个,只是线索。已带 GoPlus 避雷。"
                               "接 Cielo key 可换成策展聪明钱(更强)。诚实边界:你看到时已落后其入场价,多数会归零。")})


def render_stats(opportunities: dict | None) -> dict:
    """Measurement layer: log this round's opp picks (with entry price), resolve any
    due, and emit the honest per-lane hit rate. The board shows '不可判' until the
    sample supports a number — the same discipline that killed the fake 44%."""
    try:
        from src.pipeline import board_outcomes
        ops = (opportunities or {}).get("opportunities", [])
        picks = [{"symbol": o.get("symbol"), "chain": o.get("chain"),
                  "token": o.get("token") or o.get("address"),
                  "price0": o.get("price"), "liquidity": o.get("liq"),
                  "metric": o.get("smart_money")}
                 for o in ops[:12] if o.get("price")]
        # _price_at resolves via GeckoTerminal, which covers Solana + EVM. A pick whose
        # horizon price can't be fetched simply stays unresolved and retires — never a
        # fake number.
        board_outcomes.log_picks("opp", picks)
        board_outcomes.resolve()
        return _envelope({"lanes": board_outcomes.lane_stats(),
                          "note": "每条线 pick 的事后命中率(对照滑点门槛,4h/24h,GeckoTerminal取价)。"
                                  "样本<20 显示'不可判'——不拿不足的样本报假数字(44%教训)。刚开始积累。"})
    except Exception as e:
        logger.warning("render_stats_failed", error=str(e)[:120])
        return _envelope({"lanes": {}, "error": str(e)[:120]})


def render_watch() -> dict:
    """The EARLIEST lane: tokens that WATCHED proven wallets JUST bought (minutes ago),
    before it aggregates into any rank. Convergence (>=2 watched wallets, same token,
    same window) is the strongest early signal a dashboard can honestly give."""
    from src.onchain.smart_wallets import fresh_smart_buys, watchlist
    try:
        buys = fresh_smart_buys(chain_codes=("sol", "bsc", "base", "eth"), window_min=45)
    except Exception as e:
        return _envelope({"watch": [], "watched_wallets": 0, "scan_error": str(e)[:120]})
    if buys is None:
        return _envelope({"watch": [], "watched_wallets": len(watchlist()),
                          "note": "FlareSolverr 未运行 — 实时监听需要它"})
    return _envelope({"watch": buys, "watched_wallets": len(watchlist()),
                      "note": ("盯着一批已验证盈利的钱包,它们刚买入的币(分钟级,早于排名聚合)。"
                               "收敛=≥2个独立聪明钱同窗买同一个=最强早信号。仍晚于创建区块内部人。")})


def render_perps() -> dict:
    """Structure #2 (trend ignition) + #3 (liquidation-cascade right side) from
    Hyperliquid — keyless, live, no home-grown detection needed."""
    from src.onchain.hyperliquid import perp_signals
    try:
        sigs = perp_signals()
        return _envelope({"perps": sigs, "source": "Hyperliquid (keyless)",
                          "note": ("拥挤=资金费极端,那一侧在付费死扛=清算燃料(防它被清算的方向)。"
                                   "点火=未平仓与价同向放量=新杠杆进场,趋势有燃料。第2轮起才有点火(需OI历史)。"
                                   "诚实边界:抬高的是概率不是时点,不是买卖指令。")})
    except Exception as e:
        return _envelope({"perps": [], "source": "Hyperliquid", "scan_error": str(e)[:120]})


def _dex_direction(token: str, chain: str) -> dict:
    """Cheap tape context: price change + buy/sell counts (DexScreener, keyless)."""
    try:
        u = f"https://api.dexscreener.com/token-pairs/v1/{chain}/{token}"
        req = urllib.request.Request(u, headers={"User-Agent": "CryptoScope/1.0"})
        d = json.loads(urllib.request.urlopen(req, timeout=15).read().decode())
        pairs = d if isinstance(d, list) else d.get("pairs", [])
        if not pairs:
            return {}
        p = max(pairs, key=lambda x: (x.get("liquidity", {}) or {}).get("usd", 0) or 0)
        pc, tx = p.get("priceChange", {}) or {}, p.get("txns", {}) or {}
        return {"pc_h1": pc.get("h1"), "pc_h24": pc.get("h24"),
                "buys_h24": (tx.get("h24", {}) or {}).get("buys"),
                "sells_h24": (tx.get("h24", {}) or {}).get("sells")}
    except Exception:
        return {}


# --------------------------------------------------------------------------- #
# traders view — Arena public API + gates-first falsifier verdicts (v0)
# --------------------------------------------------------------------------- #
# v0 computes only what the public rankings API actually supports. Per the research:
# per-trade t-stats / PnL-HHI / martingale regression need position-level history the
# public API doesn't expose — those fields ship as explicit nulls with
# "unavailable" reasons, NEVER imputed. Skill certification (repeatable_skill)
# additionally needs the multi-period + DSR machinery (P3); v0 therefore never
# emits repeatable_skill — its strongest positive claim is "not yet falsified".
MIN_TRADES = 30          # below → insufficient_data (per-trade t needs >=30 round-trips)
MIN_MDD_DAYS = 60        # MDD uninformative under ~60d (E[MDD] grows with T)
LEV_LOTTERY = 20.0       # median leverage above this = lottery flag (Binance's own
                         # "high leverage" tag; liquidated cohort averaged ~60x)
MDD_BLOWUP = 40.0        # 30d MtM drawdown beyond this = lottery risk class


def _fetch_arena(window: str = "90d", limit: int = 100) -> list[dict]:
    u = f"{ARENA_API}?window={window}&limit={limit}"
    req = urllib.request.Request(u, headers={"User-Agent": "falsifier-board/0.1"})
    with urllib.request.urlopen(req, timeout=20) as r:
        d = json.loads(r.read().decode())
    data = d.get("data") or {}
    return data.get("traders") or d.get("traders") or []


def falsify_trader(t: dict) -> dict:
    """Gates-first verdict from the fields the rankings API exposes.

    Cascade: insufficient_data → lottery_leverage → tail_luck-flags → unfalsified.
    sniper_timing needs on-chain entry data (P4); repeatable_skill needs multi-period
    persistence + DSR (P3) — v0 honestly refuses to emit either."""
    trades = t.get("trades_count") or 0
    mdd = t.get("max_drawdown")        # PERCENT (verified live: 0.49..59.6 continuous)
    win_rate = t.get("win_rate")       # PERCENT (verified live: 1.1..99.9)

    if trades < MIN_TRADES:
        return {"verdict": "insufficient_data",
                "why": f"仅{trades}单(<{MIN_TRADES}),任何统计都是噪声"}
    if mdd is not None and abs(float(mdd)) >= MDD_BLOWUP:
        return {"verdict": "lottery_leverage",
                "why": f"回撤{abs(float(mdd)):.0f}%≥{MDD_BLOWUP}%:已证实的爆仓风险类"}
    # near-100% win rate is the martingale tell (steady tiny wins, hidden tail)
    if win_rate is not None and float(win_rate) >= 95:
        return {"verdict": "lottery_leverage",
                "why": "胜率≈100%:典型 martingale 形态(小赢连串,大亏隐藏)"}
    return {"verdict": "unfalsified",
            "why": "通过 v0 可算的门;技能认证需多期存活+DSR(未实现),狙击判别需链上入场数据(未实现)"}


def render_traders(window: str = "90d") -> dict:
    try:
        raw = _fetch_arena(window=window, limit=100)
        source_ok = True
        err = None
    except Exception as e:
        raw, source_ok, err = [], False, str(e)[:120]
    records = []
    for t in raw:
        f = falsify_trader(t)
        records.append({
            "source": t.get("platform") or t.get("source"),
            "trader_id": t.get("trader_key") or t.get("source_trader_id") or t.get("id"),
            "handle": t.get("display_name") or t.get("handle"),
            "rank": t.get("rank"), "arena_score": t.get("arena_score") or t.get("score"),
            "roi": t.get("roi"), "pnl": t.get("pnl"), "win_rate": t.get("win_rate"),
            "max_drawdown": t.get("max_drawdown"), "sharpe": t.get("sharpe_ratio"),
            "trades_count": t.get("trades_count"),
            "falsifier": f,
            # explicit unavailability — never imputed (research: absence must be
            # distinguishable from a passing check)
            "unavailable": ["pnl_hhi", "martingale_coef", "sniper_timing",
                            "cross_period_survival"],
        })
    return _envelope({
        "window": window, "source": "arenafi.org public API",
        "source_ok": source_ok, "source_error": err,
        "note": ("v0:门先于分。真技能是<1-2%尾巴,本版最强的正面判决只有"
                 "'未被证伪',绝不发'可跟单'。缺链上入场/逐笔数据的指标显式标 unavailable。"),
        "traders": records,
    })


# --------------------------------------------------------------------------- #
# envelope / write / push
# --------------------------------------------------------------------------- #
def _envelope(body: dict) -> dict:
    now = datetime.now(timezone.utc)
    return {"schema_version": SCHEMA_VERSION,
            "generated_at": now.isoformat(),
            "next_expected_at": (now + timedelta(minutes=REFRESH_MIN)).isoformat(),
            **body}


def write_views(**views: dict) -> list:
    """views: {view_name: payload} → writes <view_name>.json for each non-None."""
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    paths = []
    for name, payload in views.items():
        if payload is None:
            continue
        p = EXPORT_DIR / f"{name}.json"
        p.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
        paths.append(p)
    meta = _envelope({"views": [n for n, v in views.items() if v is not None]})
    mp = EXPORT_DIR / "meta.json"
    mp.write_text(json.dumps(meta, ensure_ascii=False, separators=(",", ":")))
    paths.append(mp)
    return paths


def push_to_blob(paths: list) -> int:
    """PUT each view to Vercel Blob at a FIXED pathname (allowOverwrite) so the
    board polls one stable URL per view. Requires BLOB_READ_WRITE_TOKEN in .env.
    Fails loud in logs, returns count pushed; a failed push leaves the previous
    blob serving (stale-if-error keeps the board alive)."""
    token = os.environ.get("BLOB_READ_WRITE_TOKEN", "")
    if not token:
        logger.warning("blob_push_skipped", reason="no BLOB_READ_WRITE_TOKEN")
        return 0
    pushed = 0
    for p in paths:
        try:
            url = f"https://blob.vercel-storage.com/{p.name}"
            req = urllib.request.Request(url, data=p.read_bytes(), method="PUT", headers={
                "Authorization": f"Bearer {token}",
                "x-api-version": "7",
                "x-content-type": "application/json",
                "x-allow-overwrite": "1",
                "x-add-random-suffix": "0",
                "x-cache-control-max-age": "60",
            })
            with urllib.request.urlopen(req, timeout=30) as r:
                resp = json.loads(r.read().decode())
            logger.info("blob_pushed", view=p.name, url=resp.get("url"))
            pushed += 1
        except Exception as e:
            logger.error("blob_push_failed", view=p.name, error=str(e)[:120])
    return pushed


def run(push: bool = True, include_operators: bool = True) -> dict:
    """Render the money-making lanes → Blob. perps (#2/#3) is keyless & fast;
    opportunities (#1) is the home-grown smart-money radar; operators (庄) is the
    verdict engine (slow). Each lane fails independently to null, never blocks."""
    perps = render_perps()                              # #2 ignition + #3 cascade
    opportunities = render_opportunities()              # #1 early smart-money buys
    operators = render_operators() if include_operators else None
    stats = render_stats(opportunities)                 # measurement: log + resolve + hit rate
    paths = write_views(perps=perps, opportunities=opportunities, operators=operators, stats=stats)
    n = push_to_blob(paths) if push else 0
    return {"views_written": len(paths), "views_pushed": n,
            "perps": len((perps or {}).get("perps", [])),
            "opportunities": len((opportunities or {}).get("opportunities", [])),
            "export_dir": str(EXPORT_DIR)}


if __name__ == "__main__":
    import sys
    from dotenv import load_dotenv

    from src.config import PROJECT_ROOT
    load_dotenv(PROJECT_ROOT / ".env")
    res = run(push="--dry-run" not in sys.argv)
    print(json.dumps(res, ensure_ascii=False, indent=1))
