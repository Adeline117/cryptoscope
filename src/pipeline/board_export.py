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


def write_views(operators: dict | None = None, traders: dict | None = None) -> list:
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    paths = []
    views = {"operators.json": operators, "traders.json": traders}
    for name, payload in views.items():
        if payload is None:
            continue
        p = EXPORT_DIR / name
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
    operators = render_operators() if include_operators else None
    traders = render_traders()
    paths = write_views(operators, traders)
    n = push_to_blob(paths) if push else 0
    return {"views_written": len(paths), "views_pushed": n,
            "export_dir": str(EXPORT_DIR)}


if __name__ == "__main__":
    import sys
    from dotenv import load_dotenv

    from src.config import PROJECT_ROOT
    load_dotenv(PROJECT_ROOT / ".env")
    res = run(push="--dry-run" not in sys.argv)
    print(json.dumps(res, ensure_ascii=False, indent=1))
