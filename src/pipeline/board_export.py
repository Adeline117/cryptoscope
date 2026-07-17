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
import re
import threading
import urllib.request
from datetime import datetime, timedelta, timezone

import structlog

from src.config import DATA_DIR

logger = structlog.get_logger()

SCHEMA_VERSION = 1
EXPORT_DIR = DATA_DIR / "board_export"
VIEW_FRESHNESS = {
    # (actual scheduler cadence minutes, tolerated grace minutes)
    # Discovery is a real three-minute market scan. The separate 30-second quote
    # job publishes only when it actually obtains an eligible fresh assessment;
    # an idle heartbeat must not impersonate a new market observation.
    "launch": (3, 1), "structure": (2, 3),
    "watch": (15, 5), "perps": (5, 5),
    "airdrop": (60, 15), "opportunities": (60, 15),
    "operators": (60, 30), "stats": (60, 15),
    "traders": (24 * 60, 60), "meta": (60, 15),
}
_WRITE_LOCK = threading.Lock()

_PERP_IDENTITY_CACHE_TTL_SECONDS = 26 * 60 * 60
_PERP_IDENTITY_STATUSES = frozenset({
    "verified", "research_only", "blocked", "invalid", "stale", "unavailable",
})
_PERP_IDENTITY_SYMBOL = re.compile(r"^[A-Z0-9]{1,32}$")
_PERP_IDENTITY_REASON = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_PERP_IDENTITY_MAX_ROWS = 20_000
_PERP_IDENTITY_MAX_MARKETS = 100_000
_PERP_IDENTITY_MAX_SOURCES = 64

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
            # live event from the sentinel's last fired phase. 'deposit' = operator
            # moving inventory to an exchange right now — the LEADING dump signal, its
            # own measured episode (generic shorts hit 1/16, so short is a candidate;
            # AVOID is the defensible read — don't be exit liquidity). Board gates on
            # flow_scan_ts freshness so a stale phase isn't shown as happening now.
            "live_phase": v.get("last_phase"),
            "flow_scan_ts": v.get("flow_scan_ts"),
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
    return _envelope({"operators": records}, view="operators")


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
        with urllib.request.urlopen(req, timeout=20) as response:
            d = json.loads(response.read())
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


def _normalize_legacy_opportunity_rows(rows: list[dict], source: str) -> list[dict]:
    """Give heterogeneous discovery fallbacks one honest, UI-safe row contract."""
    for row in rows:
        row["token"] = row.get("token") or row.get("address")
        row["address"] = row.get("address") or row.get("token")
        actors = row.get("smart_money")
        if actors is None:
            actors = row.get("smart_actors") or 0
        row["smart_money"] = actors
        row["smart_actors"] = row.get("smart_actors", actors)
        row["renowned"] = row.get("renowned") or 0
        if row.get("age_hours") is None and row.get("age_days") is not None:
            row["age_hours"] = row["age_days"] * 24
        row["age_days"] = (
            row["age_hours"] / 24 if row.get("age_hours") is not None
            else row.get("age_days"))
        row["confirmed_fresh"] = bool(
            row.get("confirmed_fresh")
            or (row.get("age_hours") is not None and row["age_hours"] <= 12))
        row["liq"] = row.get("liq") if row.get("liq") is not None else row.get("liquidity")
        if not isinstance(row.get("exit_risk"), dict):
            row["exit_risk"] = {
                "level": "unknown", "score": None,
                "reasons": ["备用源未提供可验证的接盘风险字段"],
            }
        if not isinstance(row.get("manipulation"), dict):
            row["manipulation"] = {
                "level": "unknown", "reasons": [],
            }
        if not isinstance(row.get("rug"), dict):
            row["rug"] = {"level": "unchecked", "facts": []}
        row["evidence_source"] = source
    return rows


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
    # call per chain. Far better than the slow home-grown convergence. Every chain's
    # response is status-bearing so a challenge page can never publish as "zero".
    gm_health = {"state": "failed", "error_kind": "not_attempted", "chains": []}
    try:
        from src.onchain import gmgn
        gm_result = gmgn.opportunities_result(
            chains=("sol", "bsc", "base", "eth"), min_smart=2)
        gm = gm_result["opportunities"]
        gm_health = gm_result["source_health"]
    except Exception as e:
        logger.warning("gmgn_failed", error=str(e)[:100])
        gm = []
        gm_health = {"state": "failed", "error_kind": "internal_error",
                     "detail": str(e)[:120], "chains": []}
    if gm_health["state"] == "ok" or (gm_health["state"] == "partial" and gm):
        _normalize_legacy_opportunity_rows(gm, "gmgn")
        payload = {"opportunities": gm, "scanned": len(gm),
                   "source": ("GMGN 策展聪明钱(全链)"
                              if gm_health["state"] == "ok"
                              else "GMGN 策展聪明钱(部分链)"),
                   "source_health": gm_health,
                   "note": ("GMGN 策展的聪明钱正在买的新币,按聪明钱数排,全链含 Solana。"
                            "同时显示狙击/机器人数(反指标)和合约安全。诚实:你看到时已落后其入场,多数会归零。")}
        if gm_health["state"] == "partial":
            payload["scan_error"] = gm_health["error_kind"]
        return _envelope(payload, view="opportunities")

    # #1 second path: Cielo curated list (if key present).
    cielo = _cielo_smart_buys()
    if cielo:
        _normalize_legacy_opportunity_rows(cielo, "cielo")
        return _envelope({"opportunities": cielo, "scanned": None, "source": "Cielo 策展聪明钱名单",
                          "upstream_source_health": {"gmgn": gm_health},
                          "fallback_source_health": {
                              "source": "cielo", "state": "ok",
                              "observed": len(cielo), "failed": 0,
                          },
                          "note": ("Cielo 策展的已验证聪明钱正在买入的币,按买入钱包数排。"
                                   "已带 GoPlus 避雷。诚实边界:你看到时已落后其入场,多数会归零。")},
                         view="opportunities")

    try:
        fresh = _gather_young(chains, pages=2)
    except Exception as e:
        logger.warning(
            "all_opportunity_sources_failed", gmgn_error=gm_health.get("error_kind"),
            fallback_error=str(e)[:120])
        # Preserve the last-good local/Blob view. A newly fresh empty payload would
        # impersonate a verified zero after every source failed.
        raise RuntimeError("all opportunity sources failed") from e
    if not fresh:
        logger.warning(
            "opportunity_fallback_unverified_empty",
            gmgn_error=gm_health.get("error_kind"))
        # _gather_young's legacy HTTP helpers collapse total provider failure to [].
        # With no candidate-level proof of a successful scan, preserve last-good.
        raise RuntimeError("opportunity fallback returned an unverified empty result")

    out = []
    scanned = 0
    convergence_available = 0
    convergence_unavailable = 0
    for cand in fresh[:max_scan]:
        tok, ch = cand.get("address"), cand["chain"]
        if not tok:
            continue
        scanned += 1
        try:
            conv = convergence(tok, ch, max_check=12)
        except Exception:
            convergence_unavailable += 1
            continue
        if conv.get("available") is not True:
            convergence_unavailable += 1
            continue
        convergence_available += 1
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
    if scanned == 0 or convergence_available == 0:
        logger.warning(
            "opportunity_convergence_unavailable", candidates=len(fresh),
            scanned=scanned, unavailable=convergence_unavailable)
        raise RuntimeError("opportunity convergence source unavailable")
    out.sort(key=lambda x: (-(x["smart_actors"] or 0), x["age_days"] if x["age_days"] is not None else 999))
    _normalize_legacy_opportunity_rows(out, "self_hosted")
    fallback_health = {
        "source": "self_hosted_convergence",
        "state": "partial" if convergence_unavailable else "ok",
        "observed": convergence_available, "failed": convergence_unavailable,
        "requested": scanned,
    }
    payload = {"opportunities": out, "scanned": scanned, "source": "自建雷达(慢/稀疏)",
               "upstream_source_health": {"gmgn": gm_health},
               "fallback_source_health": fallback_health,
               "note": ("聪明钱=有已实现盈利历史、且相互独立(非同一钱包农场)的钱包正在买入。"
                        "强=≥3个独立主体收敛;弱=1-2个,只是线索。已带 GoPlus 避雷。"
                        "接 Cielo key 可换成策展聪明钱(更强)。诚实边界:你看到时已落后其入场价,多数会归零。")}
    if fallback_health["state"] == "partial":
        payload["scan_error"] = "fallback_convergence_partial"
    return _envelope(payload, view="opportunities")


def render_launch() -> dict:
    """The dedicated low-float event lane.

    Ingestion runs independently every few minutes. Export must stay read-only so a
    slow board refresh cannot make the first-observed price later than the event.
    """
    try:
        from src.pipeline.launch_radar import view
        return _envelope(view(), view="launch")
    except Exception as e:
        logger.warning("render_launch_failed", error=str(e)[:120])
        # Never turn a ledger/read failure into a newly-fresh empty market.  Every
        # scheduled caller treats this exception as stale-if-error and leaves the
        # existing local/Blob launch view untouched.
        raise


def render_structure() -> dict:
    """Public listing/flow/unlock events, kept distinct from directional calls."""
    try:
        from src.pipeline.structure_radar import view
        return _envelope(view(), view="structure")
    except Exception as e:
        logger.warning("render_structure_failed", error=str(e)[:120])
        # A ledger/read-model failure is not evidence that there are zero events.
        # Scheduled callers preserve the prior local/Blob payload when rendering
        # raises, so its freshness clock can expire visibly instead of being reset.
        raise


def render_airdrop() -> dict:
    """User-curated official campaigns; missing wallet evidence remains unknown."""
    try:
        from src.pipeline.airdrop_radar import sync
        return _envelope(sync(), view="airdrop")
    except Exception as e:
        logger.warning("render_airdrop_failed", error=str(e)[:120])
        # A watchlist/ledger failure is unknown, never a newly observed empty set.
        # Let the scheduler's stale-if-error path retain the last good publication.
        raise


def render_stats(opportunities: dict | None) -> dict:
    """Read-only measurement export for legacy and canonical lane scorecards.

    The old smart-money view is now a non-actionable discovery aid, so its frozen
    historical scorecard is displayed but no longer creates or resolves new picks.
    Canonical five-lane outcomes are ingested by their event jobs and resolved by the
    dedicated hourly scheduler.  Rendering must never perform slow price backfills.
    """
    try:
        from src.pipeline import board_outcomes
        lanes = board_outcomes.lane_stats()
        # The five-lane ledger has its own immutable first-seen snapshots and is
        # resolved hourly by the scheduler. Rendering stays read-only: a dashboard
        # refresh must never choose the observation time or trigger a large backfill.
        from src.pipeline.opportunity_outcomes import lane_stats as opportunity_lane_stats
        lanes.update(opportunity_lane_stats())
        from src.pipeline.validation_overview import build_validation_overview
        validation_overview = build_validation_overview(lanes)
        return _envelope({"lanes": lanes,
                          "validation_overview": validation_overview,
                          "note": "只有 Launch/Cascade 方向事件按首次发现价做固定 1h/24h/7d "
                                  "纸面测量并扣冻结的估算成本；Structure 不计算方向命中率；"
                                  "Airdrop 只汇总语义、受益人、奖励与实际成本全部核验的完整领取；"
                                  "Carry 使用独立代理账本且不冒充实盘收益。Launch 头部统计只认追加式"
                                  "精确池结果；优势只认当前预注册协议的前向固定 look、连续 UTC 日历与 "
                                  "SPA/Reality Check。旧聪明钱分数和 "
                                  "picks 为冻结历史，只作描述且永远不可据此判定优势。"},
                         view="stats")
    except Exception as e:
        logger.warning("render_stats_failed", error=str(e)[:120])
        raise


def render_watch() -> dict:
    """The EARLIEST lane: tokens that WATCHED proven wallets JUST bought (minutes ago),
    before it aggregates into any rank. Convergence (>=2 watched wallets, same token,
    same window) is the strongest early signal a dashboard can honestly give."""
    from src.onchain.smart_wallets import fresh_smart_buys_result
    try:
        result = fresh_smart_buys_result(
            chain_codes=("sol", "bsc", "base", "eth"), window_min=45)
    except Exception as e:
        return _envelope({"watch": [], "watched_wallets": 0, "scan_error": str(e)[:120]},
                         view="watch")
    buys = result["buys"]
    health = result["source_health"]
    payload = {"watch": buys, "watched_wallets": health["configured_wallets"],
               "source_health": health,
               "note": ("盯着一批已验证盈利的钱包,它们刚买入的币(分钟级,早于排名聚合)。"
                        "收敛=≥2个独立聪明钱同窗买同一个=最强早信号。仍晚于创建区块内部人。")}
    if health["state"] == "failed":
        payload["scan_error"] = health["error_kind"]
    return _envelope(payload, view="watch")


def render_perps() -> dict:
    """Structure #2 (trend ignition) + #3 (liquidation-cascade right side) from
    Hyperliquid — keyless, live, no home-grown detection needed."""
    from src.onchain.hyperliquid import (fetch_ctxs_result, _store_and_diff, carry_scorecard,
                                         perp_signals, scan_carry)
    from src.pipeline.carry_paper import open_symbols as paper_open_symbols
    from src.pipeline.carry_paper import run as paper_run
    try:
        hl_result = fetch_ctxs_result()
        rows = hl_result["rows"]
        snapshot_health = {"state": "ok"}
        if rows:
            try:
                _store_and_diff(rows)      # one snapshot shared by both screens
            except Exception as e:
                logger.warning("perp_snapshot_store_failed", error=str(e)[:120])
                snapshot_health = {"state": "error", "error_kind": "store_failed",
                                   "error": str(e)[:120]}
        sigs = perp_signals(rows) if rows else []
        cascade_events = []
        try:
            from src.pipeline.cascade_radar import record_signals, view as cascade_view
            record_signals(sigs)
            cascade_events = cascade_view().get("events", [])
        except Exception as e:
            logger.debug("cascade_ledger_failed", error=str(e)[:80])
        paper_pre_error = None
        try:
            active_symbols = paper_open_symbols()
        except Exception as exc:
            active_symbols = []
            paper_pre_error = str(exc)[:120]
        carry_scan = scan_carry(rows, priority_symbols=active_symbols,
                                hl_health=hl_result["health"])
        carry = carry_scan["signals"]
        scorecard_health = {"state": "ok"}
        try:
            scorecard = carry_scorecard()
        except Exception as e:
            logger.warning("carry_scorecard_failed", error=str(e)[:120])
            scorecard = {"available": False, "verdict": "不可判",
                         "error_kind": "scorecard_failed"}
            scorecard_health = {"state": "error", "error_kind": "scorecard_failed",
                                "error": str(e)[:120]}
        paper = {}
        paper_health = {"state": "ok"}
        if paper_pre_error:
            paper_health = {"state": "error", "error_kind": "open_symbols_failed",
                            "error": paper_pre_error}
        else:
            try:
                paper = paper_run(
                    carry,
                    observations=carry_scan.get(
                        "paper_observations", carry_scan["open_observations"]),
                )
                if (paper.get("ledger_sync") or {}).get("status") == "error":
                    paper_health = {"state": "partial", "error_kind": "ledger_sync_failed"}
            except Exception as e:
                logger.debug("carry_paper_failed", error=str(e)[:80])
                paper_health = {"state": "error", "error_kind": "tracker_failed",
                                "error": str(e)[:120]}
        carry_health = dict(carry_scan["source_health"])
        carry_health["paper"] = paper_health
        carry_health["snapshot_store"] = snapshot_health
        carry_health["scorecard"] = scorecard_health
        if (paper_health["state"] != "ok" or snapshot_health["state"] != "ok"
                or scorecard_health["state"] != "ok") and carry_health["state"] == "ok":
            carry_health["state"] = "partial"
        return _envelope({"perps": sigs, "carry": carry, "cascade_events": cascade_events, "carry_scorecard": scorecard,
                          "carry_paper": paper,
                          "carry_open_status": carry_scan["open_status"],
                          "carry_source_health": carry_health,
                          "source": "Hyperliquid + OKX (keyless)",
                          "note": ("Carry 是待验证假设：优先观测空 HL、多 OKX 的两永续配对差价，"
                                   "当前只形成报价率与已知成本组件的纸面代理，不代表已证明正 EV、完整成本或实盘收益。"
                                   "费率翻负、basis 扩张、保证金与 ADL 都可能吞掉代理优势。拥挤/点火仅作方向防御观测，"
                                   "不是买卖指令。")},
                         view="perps")
    except Exception as e:
        logger.warning("render_perps_failed", error=str(e)[:120])
        # Total render/source failure cannot prove an empty perp market.  Keep the
        # last-good payload and let its public freshness deadline expose the outage.
        # Component-level failures above remain explicit partial-health states when
        # the live market rows themselves were read successfully.
        raise


def _dex_direction(token: str, chain: str) -> dict:
    """Cheap tape context: price change + buy/sell counts (DexScreener, keyless)."""
    try:
        u = f"https://api.dexscreener.com/token-pairs/v1/{chain}/{token}"
        req = urllib.request.Request(u, headers={"User-Agent": "CryptoScope/1.0"})
        with urllib.request.urlopen(req, timeout=15) as response:
            d = json.loads(response.read().decode())
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
    }, view="traders")


# --------------------------------------------------------------------------- #
# envelope / write / push
# --------------------------------------------------------------------------- #
def _envelope(body: dict, *, view: str) -> dict:
    if view not in VIEW_FRESHNESS:
        raise ValueError(f"unknown board view freshness policy: {view}")
    cadence_min, grace_min = VIEW_FRESHNESS[view]
    now = datetime.now(timezone.utc)
    return {"schema_version": SCHEMA_VERSION,
            "view": view,
            "generated_at": now.isoformat(),
            "refresh_cadence_min": cadence_min,
            "freshness_grace_min": grace_min,
            "next_expected_at": (now + timedelta(minutes=cadence_min)).isoformat(),
            "stale_after_at": (now + timedelta(minutes=cadence_min + grace_min)).isoformat(),
            **body}


def _risk_budget() -> dict:
    """Publish the static manual-probe discipline budget, never live usage.

    Committed/remaining amounts are computed client-side from the same
    validated launch rows the browser already gates: a server-side count
    would go stale between exports while quote clocks expire in the browser.
    """
    from src.contract.launch_probe import (
        MAX_CONCURRENT_MANUAL_PROBES,
        MAX_PROBE_NOTIONAL_USD,
        RISK_BUDGET_BASIS,
    )

    return {
        "version": 1,
        "auto_execution_allowed": False,
        "per_probe_cap_usd": MAX_PROBE_NOTIONAL_USD,
        "max_concurrent_probes": MAX_CONCURRENT_MANUAL_PROBES,
        "max_concurrent_notional_usd": (
            MAX_PROBE_NOTIONAL_USD * MAX_CONCURRENT_MANUAL_PROBES
        ),
        "basis": RISK_BUDGET_BASIS,
    }


def _hlp(*, now: datetime | None = None, max_age_seconds: float = 3600.0) -> dict:
    """Project the HLP vault money view from the scheduler-refreshed state file.

    The tracker job writes data/hlp_state.json every 30 min; board export only
    reads it (never fetches) so a slow API can never stall a render. Missing,
    unreadable, malformed, or stale state fails closed to available=False.
    """
    from src.pipeline.hlp_tracker import STATE_FILE

    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    try:
        state = json.loads(STATE_FILE.read_text())
    except (OSError, json.JSONDecodeError):
        return {"available": False, "reason": "hlp_state_unavailable"}
    if not isinstance(state, dict) or not isinstance(state.get("available"), bool):
        return {"available": False, "reason": "hlp_state_malformed"}
    stamp = state.get("generated_at")
    try:
        generated = datetime.fromisoformat(str(stamp).replace("Z", "+00:00"))
        if generated.tzinfo is None:
            raise ValueError("naive hlp clock")
        age = (current - generated.astimezone(timezone.utc)).total_seconds()
    except (TypeError, ValueError):
        return {"available": False, "reason": "hlp_state_clock_invalid"}
    if age < -5 or age > max_age_seconds:
        return {"available": False, "reason": "hlp_state_stale",
                "generated_at": str(stamp)}
    return state


def _runtime_safety() -> dict:
    """Project only bounded runtime truth needed to gate manual actionability."""
    from collections.abc import Mapping

    from src.ops import health
    from src.pipeline import evm_factory_stream, evm_launch_bridge, stream_health

    try:
        disk_raw = health._disk_health()
    except Exception:
        disk_raw = None
    storage_pressure = (
        disk_raw.get("state")
        if isinstance(disk_raw, Mapping)
        and disk_raw.get("state") in {"ok", "warn", "critical", "unknown"}
        else "unknown"
    )

    try:
        raw_rows = stream_health.snapshot()
        rows = raw_rows if isinstance(raw_rows, list) else None
    except Exception:
        rows = None
    try:
        raw_specs = evm_factory_stream.configured_specs()
        specs = tuple(raw_specs)
    except Exception:
        specs = None
    try:
        raw_evm_health = evm_launch_bridge.configured_stream_health()
        evm_health = raw_evm_health if isinstance(raw_evm_health, list) else None
    except Exception:
        evm_health = None

    def matches(source: str, stream: str) -> list[Mapping]:
        if rows is None:
            return []
        return [
            row for row in rows
            if isinstance(row, Mapping)
            and row.get("source") == source and row.get("stream") == stream
        ]

    def stream_state(row: Mapping | None) -> str:
        if row is None:
            return "unknown"
        status, stale, gaps = row.get("status"), row.get("stale"), row.get("open_gaps")
        if (status not in {"live", "degraded", "disconnected", "stale"}
                or not isinstance(stale, bool)
                or isinstance(gaps, bool) or not isinstance(gaps, int) or gaps < 0):
            return "unknown"
        return "healthy" if status == "live" and not stale and gaps == 0 else "blocked"

    solana_configured = 1
    solana_rows = matches("solana", "pump_fun_launches")
    if rows is None or len(solana_rows) != 1:
        solana_state, solana_live = "unknown", None
    else:
        solana_state = stream_state(solana_rows[0])
        solana_live = (1 if solana_state == "healthy" else 0
                       if solana_state == "blocked" else None)

    maintenance_rows = matches("solana", "pump_fun_maintenance")
    if rows is None or len(maintenance_rows) != 1:
        maintenance = "unknown"
    else:
        maintenance = stream_state(maintenance_rows[0])

    evm_live: int | None
    evm_configured: int | None
    if specs is None or evm_health is None:
        evm_state, evm_live = "unknown", None
        evm_configured = len(specs) if specs is not None else None
    else:
        evm_configured = len(specs)
        evm_live = 0
        evm_unknown = evm_configured == 0
        seen: set[tuple[str, str]] = set()
        for spec in specs:
            source, stream = getattr(spec, "chain", None), getattr(spec, "stream", None)
            identity = (source, stream)
            if (not isinstance(source, str) or not source
                    or not isinstance(stream, str) or not stream
                    or identity in seen):
                evm_unknown = True
                continue
            seen.add(identity)
            observed = [
                row for row in evm_health
                if isinstance(row, Mapping)
                and row.get("source") == source and row.get("stream") == stream
            ]
            if len(observed) > 1:
                evm_unknown = True
                continue
            if not observed:
                evm_unknown = True
                continue
            item = observed[0]
            coverage = item.get("coverage_verified")
            if not isinstance(coverage, bool):
                evm_unknown = True
                continue
            observed_state = stream_state(item)
            if observed_state == "unknown":
                evm_unknown = True
            elif observed_state == "healthy" and coverage:
                evm_live += 1
        if evm_unknown:
            evm_state, evm_live = "unknown", None
        elif evm_live == evm_configured:
            evm_state = "healthy"
        elif evm_live == 0:
            evm_state = "blocked"
        else:
            evm_state = "degraded"

    retention_rows = matches("hyperliquid", "raw_trade_retention")
    retention = "unknown"
    if rows is not None and len(retention_rows) == 1:
        row = retention_rows[0]
        details = row.get("details")
        retained = details.get("raw_trades_retained") if isinstance(details, Mapping) else None
        measurement_failed = (
            details.get("measurement_failed") if isinstance(details, Mapping) else None
        )
        if (row.get("stale") is False
                and row.get("status") in {"live", "degraded"}
                and isinstance(retained, bool) and measurement_failed is False):
            retention = "retained" if retained else "shed"

    unavailable = (storage_pressure == "unknown" or solana_state == "unknown"
                   or maintenance == "unknown" or evm_state == "unknown"
                   or retention == "unknown")
    reasons = ["runtime_health_unavailable"] if unavailable else []
    if storage_pressure == "warn":
        reasons.append("storage_pressure_warn")
    elif storage_pressure == "critical":
        reasons.append("storage_pressure_critical")
    if solana_state == "blocked":
        reasons.append("solana_streams_unhealthy")
    if maintenance == "blocked":
        reasons.append("solana_maintenance_unhealthy")
    if evm_state in {"degraded", "blocked"}:
        reasons.append("evm_streams_unhealthy")
    if retention == "shed":
        reasons.append("hyperliquid_raw_trade_retention_shed")

    if unavailable:
        state, blocks = "unknown", True
    elif (storage_pressure == "critical" or solana_state == "blocked"
          or maintenance == "blocked"):
        state, blocks = "blocked", True
    elif (storage_pressure == "warn" or evm_state != "healthy"
          or retention == "shed"):
        state, blocks = "degraded", False
    else:
        state, blocks = "healthy", False

    return {
        "version": 1,
        "state": state,
        "blocks_actionability": blocks,
        "auto_execution_allowed": False,
        "storage_pressure": storage_pressure,
        "reason_codes": reasons,
        "streams": {
            "solana": {
                "state": solana_state, "live": solana_live,
                "configured": solana_configured, "maintenance": maintenance,
            },
            "evm": {
                "state": evm_state, "live": evm_live,
                "configured": evm_configured,
            },
        },
        "hyperliquid_raw_trade_retention": retention,
    }


def _perp_identity_fallback(status: str, reason_code: str) -> dict:
    """Return the bounded public fail-closed shape, never source diagnostics."""
    return {
        "version": 1,
        "status": status,
        "blocks_identity_dependent_scans": True,
        "auto_execution_allowed": False,
        "reason_codes": [reason_code],
        "market_count": 0,
        "research_mapped": 0,
        "actionable_identity_count": 0,
        "independent_source_count": 0,
        "observed_path_count": 0,
        "cache_age_seconds": None,
        "cache_ttl_seconds": _PERP_IDENTITY_CACHE_TTL_SECONDS,
    }


def _perp_identity_policy(*, _now: datetime | None = None) -> dict:
    """Project one local cache read into a scoped public identity-policy gate.

    This is deliberately separate from ``runtime_safety``: an unverified
    token-to-contract join blocks only scans that depend on that identity.  It does
    not gate direct Hyperliquid/OKX observations, Launch, or any other board lane.
    """
    try:
        from src.onchain.perp_universe import CACHE_TTL_SECONDS, load_result

        if CACHE_TTL_SECONDS != _PERP_IDENTITY_CACHE_TTL_SECONDS:
            return _perp_identity_fallback("invalid", "identity_projection_invalid")
        now = _now if _now is not None else datetime.now(timezone.utc)
        if (
            not isinstance(now, datetime)
            or now.tzinfo is None
            or now.utcoffset() != timedelta(0)
        ):
            return _perp_identity_fallback("invalid", "identity_projection_invalid")
        # load_result is cache-only.  Supplying the same clock used for age
        # projection avoids an expiry race between validation and serialization.
        result = load_result(_now=now)
    except Exception:
        # Exception text can contain a URL, token, or local path.  None is public.
        return _perp_identity_fallback("unavailable", "identity_load_failed")

    if type(result) is not dict:
        return _perp_identity_fallback("invalid", "identity_projection_invalid")
    status = result.get("status")
    reasons = result.get("reason_codes")
    if (
        type(status) is not str
        or status not in _PERP_IDENTITY_STATUSES
        or type(reasons) is not list
        or len(reasons) > 8
        or any(
            type(reason) is not str
            or _PERP_IDENTITY_REASON.fullmatch(reason) is None
            for reason in reasons
        )
        or len(reasons) != len(set(reasons))
    ):
        return _perp_identity_fallback("invalid", "identity_projection_invalid")

    research = result.get("research_universe")
    actionable = result.get("actionable_universe")
    if (
        type(research) is not dict
        or len(research) > _PERP_IDENTITY_MAX_ROWS
        or any(
            type(symbol) is not str
            or _PERP_IDENTITY_SYMBOL.fullmatch(symbol) is None
            or type(row) is not dict
            or row.get("actionability") != "research_only"
            for symbol, row in research.items()
        )
        or type(actionable) is not dict
        or len(actionable) > _PERP_IDENTITY_MAX_ROWS
    ):
        return _perp_identity_fallback("invalid", "identity_projection_invalid")

    # The actionable side requires the full chain/address identity contract, not
    # merely a count or an ``actionability`` string.
    try:
        from src.pipeline.perp_scanner import validated_verified_universe

        verified = validated_verified_universe(actionable)
    except Exception:
        verified = None
    if verified is None:
        return _perp_identity_fallback("invalid", "identity_projection_invalid")

    if status in {"blocked", "invalid", "stale", "unavailable"}:
        if research or verified or not reasons:
            return _perp_identity_fallback("invalid", "identity_projection_invalid")
        public_reason = {
            "blocked": "identity_collection_blocked",
            "invalid": "identity_cache_invalid",
            "stale": "identity_cache_stale",
            "unavailable": "identity_cache_unavailable",
        }[status]
        return _perp_identity_fallback(status, public_reason)

    market_count = result.get("market_count")
    source_counts = result.get("source_counts")
    independent = (
        source_counts.get("independent_source_count")
        if type(source_counts) is dict else None
    )
    observed_paths = (
        source_counts.get("observed_path_count")
        if type(source_counts) is dict else None
    )
    if (
        type(market_count) is not int
        or not 0 < market_count <= _PERP_IDENTITY_MAX_MARKETS
        or len(research) + len(verified) > market_count
        or type(independent) is not int
        or not 0 < independent <= _PERP_IDENTITY_MAX_SOURCES
        or type(observed_paths) is not int
        or not independent <= observed_paths <= _PERP_IDENTITY_MAX_SOURCES
    ):
        return _perp_identity_fallback("invalid", "identity_projection_invalid")

    try:
        generated_raw = result["generated_at"]
        expires_raw = result["expires_at"]
        if type(generated_raw) is not str or type(expires_raw) is not str:
            raise TypeError("identity cache clocks must be strings")
        generated_at = datetime.fromisoformat(generated_raw)
        expires_at = datetime.fromisoformat(expires_raw)
        clocks_valid = (
            generated_at.tzinfo is not None
            and generated_at.utcoffset() == timedelta(0)
            and generated_at.isoformat() == generated_raw
            and expires_at.tzinfo is not None
            and expires_at.utcoffset() == timedelta(0)
            and expires_at.isoformat() == expires_raw
            and expires_at == generated_at + timedelta(
                seconds=_PERP_IDENTITY_CACHE_TTL_SECONDS,
            )
            and generated_at <= now < expires_at
        )
    except (KeyError, TypeError, ValueError):
        clocks_valid = False
    if not clocks_valid:
        return _perp_identity_fallback("invalid", "identity_projection_invalid")
    cache_age = int((now - generated_at).total_seconds())
    if not 0 <= cache_age < _PERP_IDENTITY_CACHE_TTL_SECONDS:
        return _perp_identity_fallback("invalid", "identity_projection_invalid")

    if status == "research_only":
        if (
            reasons != ["heuristic_mapping_not_actionable"]
            or not research
            or verified
        ):
            return _perp_identity_fallback("invalid", "identity_projection_invalid")
        public_reasons = ["heuristic_mapping_not_actionable"]
        blocks = True
    else:  # verified
        if reasons or research or not verified:
            return _perp_identity_fallback("invalid", "identity_projection_invalid")
        public_reasons = []
        blocks = False

    return {
        "version": 1,
        "status": status,
        "blocks_identity_dependent_scans": blocks,
        "auto_execution_allowed": False,
        "reason_codes": public_reasons,
        "market_count": market_count,
        "research_mapped": len(research),
        "actionable_identity_count": len(verified),
        "independent_source_count": independent,
        "observed_path_count": observed_paths,
        "cache_age_seconds": cache_age,
        "cache_ttl_seconds": _PERP_IDENTITY_CACHE_TTL_SECONDS,
    }


def _atomic_json(path, payload: dict) -> None:
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(payload, ensure_ascii=False, allow_nan=False,
                               separators=(",", ":")))
    temp.replace(path)


def write_views(**views: dict) -> list:
    """Atomically write views and merge their clocks into a durable manifest."""
    from src.contract.board_view import launch_protocol_join, validate_board_view

    prepared = []
    # Validate the complete batch before touching any file. A later malformed view
    # must not leave an earlier view (or the manifest) half-updated.
    for name, payload in views.items():
        if payload is None:
            continue
        try:
            cadence_min, grace_min = VIEW_FRESHNESS[name]
        except KeyError as exc:
            raise ValueError(f"unknown board view freshness policy: {name}") from exc
        validate_board_view(name, payload, cadence_min=cadence_min,
                            grace_min=grace_min)
        prepared.append((name, payload))

    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    paths = []
    with _WRITE_LOCK:
        prepared_by_name = dict((name, payload) for name, payload in prepared)

        def joined_payload(name: str) -> dict | None:
            if name in prepared_by_name:
                return prepared_by_name[name]
            path = EXPORT_DIR / f"{name}.json"
            try:
                value = json.loads(path.read_text())
                return value if isinstance(value, dict) else None
            except (OSError, json.JSONDecodeError):
                return None

        protocol_join = launch_protocol_join(
            joined_payload("launch"), joined_payload("stats"),
        )
        if ({"launch", "stats"}.issubset(prepared_by_name)
                and protocol_join["state"] == "identity_mismatch"):
            raise ValueError("launch/stats protocol identity mismatch in one write batch")

        mp = EXPORT_DIR / "meta.json"
        try:
            previous = json.loads(mp.read_text()) if mp.exists() else {}
        except (OSError, json.JSONDecodeError):
            previous = {}
        manifest = dict(previous.get("view_status") or {})
        for name, payload in prepared:
            manifest[name] = {
                key: payload.get(key) for key in (
                    "generated_at", "next_expected_at", "stale_after_at",
                    "refresh_cadence_min", "freshness_grace_min")
            }
        meta = _envelope({
            "views": sorted(manifest), "view_status": manifest,
            "launch_protocol_join": protocol_join,
            "runtime_safety": _runtime_safety(),
            "perp_identity_policy": _perp_identity_policy(),
            "risk_budget": _risk_budget(),
            "hlp": _hlp(),
        }, view="meta")
        cadence_min, grace_min = VIEW_FRESHNESS["meta"]
        validate_board_view(
            "meta", meta, cadence_min=cadence_min, grace_min=grace_min,
        )
        # Runtime projection and the complete next manifest are validated before
        # replacing any prior view, preserving the batch's fail-closed boundary.
        for name, payload in prepared:
            p = EXPORT_DIR / f"{name}.json"
            _atomic_json(p, payload)
            paths.append(p)
        _atomic_json(mp, meta)
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


def run(push: bool = True, include_operators: bool = True,
        include_opportunities: bool = True, include_perps: bool = True,
        include_launch: bool = True) -> dict:
    """Render the money-making lanes → Blob. perps (#2/#3) is keyless & fast;
    opportunities (#1) is the home-grown smart-money radar; operators (庄) is the
    verdict engine (slow). Independent scheduler jobs can omit their separately-owned
    views without overwriting the last successful export."""
    perps = render_perps() if include_perps else None   # independent five-minute job
    # The scheduler's independent scan/quote jobs own the Launch observation clock.
    # Manual full exports may opt in, but regular exports must not synthesize a fresh
    # delivery timestamp without a source observation.
    launch = render_launch() if include_launch else None
    structure = render_structure()                      # public listings / later unlocks
    airdrop = render_airdrop()                          # official, owned-wallet workbench
    opportunities = render_opportunities() if include_opportunities else None
    operators = render_operators() if include_operators else None
    # With opportunities disabled, render_stats remains a read of the existing board
    # cohorts plus the five-lane ledger; it does not manufacture a new empty cohort.
    stats = render_stats(opportunities)
    paths = write_views(launch=launch, structure=structure, airdrop=airdrop, perps=perps, opportunities=opportunities, operators=operators, stats=stats)
    n = push_to_blob(paths) if push else 0
    return {"views_written": len(paths), "views_pushed": n,
            "perps": len((perps or {}).get("perps", [])),
            "launch": len((launch or {}).get("events", [])),
            "structure": len((structure or {}).get("events", [])),
            "airdrop": len((airdrop or {}).get("events", [])),
            "opportunities": len((opportunities or {}).get("opportunities", [])),
            "export_dir": str(EXPORT_DIR)}


if __name__ == "__main__":
    import sys
    from dotenv import load_dotenv

    from src.config import PROJECT_ROOT
    load_dotenv(PROJECT_ROOT / ".env")
    res = run(push="--dry-run" not in sys.argv)
    print(json.dumps(res, ensure_ascii=False, indent=1))
