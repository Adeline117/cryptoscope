"""Hyperliquid perp signals — keyless, one POST gives OI + funding + mark + vol for
every coin. Powers two get-rich structures on the board:

  · CASCADE / crowding (#3): extreme funding = one side is crowded and paying to stay
    in — that side is the fuel. High + funding → longs crowded → down-cascade risk
    (position short-side / avoid longs). High − funding → shorts crowded → squeeze-up
    risk. This is computable from a SINGLE snapshot (no history needed).
  · IGNITION (#2): open interest rising WITH price + volume = new leveraged money
    entering the move → the trend has fuel (vs OI falling on a move = short-covering,
    weaker). Needs OI change over time, so we persist a snapshot each run and diff.

Honest limits shipped with the signal: funding/OI raise cascade/ignition PROBABILITY,
they do NOT time it (312/LUNA/FTX are survivorship-flavored); thresholds are regime-
dependent — we score |funding| annualized and OI change, and label strength, never
'certain'. Nothing here is a trade instruction.
"""

from __future__ import annotations

import json
import math
import sqlite3
import urllib.request
from datetime import datetime, timezone

import structlog

from src.config import DATA_DIR
from src.onchain.okx_symbols import candidates as okx_symbol_candidates

logger = structlog.get_logger()

INFO_URL = "https://api.hyperliquid.xyz/info"
SNAP_DB = DATA_DIR / "perp_snapshots.db"

# thresholds (heuristics, labeled as such — annualized funding %)
FUNDING_CROWDED_ANN = 50.0     # |ann funding| above this = a crowded, paying side
MIN_OI_USD = 500_000           # ignore illiquid coins (noise)
IGNITION_OI_JUMP = 0.03        # +3% OI since last snapshot = leverage piling in
SNAPSHOT_MIN_GAP_MIN = 12      # diff against a snapshot at least this old (15-min job)


def _fetch_ctxs() -> list[dict]:
    """[{name, markPx, openInterest, funding, dayNtlVlm, prevDayPx, oi_usd, funding_ann,
    price_chg_24h, vol24}] for every perp. Never raises → [] on failure."""
    try:
        req = urllib.request.Request(
            INFO_URL, data=json.dumps({"type": "metaAndAssetCtxs"}).encode(),
            headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=20) as response:
            d = json.loads(response.read())
    except Exception as e:
        logger.warning("hyperliquid_fetch_failed", error=str(e)[:100])
        return []
    universe, ctxs = d[0].get("universe", []), d[1]
    out = []
    for u, c in zip(universe, ctxs):
        try:
            px = float(c.get("markPx") or 0)
            oi = float(c.get("openInterest") or 0) * px
            fund = float(c.get("funding") or 0)          # hourly
            vol = float(c.get("dayNtlVlm") or 0)
            prev = float(c.get("prevDayPx") or 0)
            out.append({
                "name": u.get("name"), "markPx": px, "oi_usd": oi,
                "funding_ann": fund * 24 * 365 * 100,     # annualized %
                "vol24": vol,
                "price_chg_24h": ((px / prev - 1) * 100) if prev else None,
            })
        except Exception:
            continue
    return out


def _conn() -> sqlite3.Connection:
    SNAP_DB.parent.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(str(SNAP_DB), timeout=10)
    c.execute("""CREATE TABLE IF NOT EXISTS snaps(
        coin TEXT, ts TEXT, oi_usd REAL, mark REAL, vol24 REAL)""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_coin_ts ON snaps(coin, ts)")
    # funding_ann persisted per snapshot so carry_signals can score PERSISTENCE (the
    # fraction of recent snapshots funding stayed positive) — the #1 carry risk is
    # funding flipping negative and you paying instead of collecting.
    if "funding_ann" not in {r[1] for r in c.execute("PRAGMA table_info(snaps)")}:
        c.execute("ALTER TABLE snaps ADD COLUMN funding_ann REAL")
    return c


def _store_and_diff(rows: list[dict]) -> None:
    """Persist this snapshot and annotate each row with oi_chg/price_chg since the last
    snapshot ≥ SNAPSHOT_MIN_GAP_MIN old (None on first run — honestly absent, not 0)."""
    now = datetime.now(timezone.utc)
    c = _conn()
    try:
        # read most-recent prior snapshot per coin within a sane window
        prev = {}
        for coin, ts, oi, mark in c.execute(
                "SELECT coin, ts, oi_usd, mark FROM snaps ORDER BY ts DESC"):
            if coin in prev:
                continue
            try:
                age = (now - datetime.fromisoformat(ts)).total_seconds() / 60
            except Exception:
                continue
            if age >= SNAPSHOT_MIN_GAP_MIN:
                prev[coin] = (oi, mark)
        for r in rows:
            p = prev.get(r["name"])
            if p and p[0] and p[1]:
                r["oi_chg_pct"] = (r["oi_usd"] / p[0] - 1) * 100
                r["price_chg_since"] = (r["markPx"] / p[1] - 1) * 100
            else:
                r["oi_chg_pct"] = None
                r["price_chg_since"] = None
        # write current snapshot (incl funding_ann for carry persistence scoring)
        c.executemany(
            "INSERT INTO snaps(coin, ts, oi_usd, mark, vol24, funding_ann) VALUES (?,?,?,?,?,?)",
            [(r["name"], now.isoformat(), r["oi_usd"], r["markPx"], r["vol24"],
              r["funding_ann"]) for r in rows])
        # prune snapshots older than ~2 days
        c.execute("DELETE FROM snaps WHERE ts < ?",
                  ((now.replace(microsecond=0)).isoformat()[:10] + "T00:00:00+00:00",))
        c.commit()
    finally:
        c.close()


IGNITION_MIN_OI_USD = 200_000  # ignition tolerates smaller coins than cascade (a real
                               # OI+price surge on a $250k coin is signal, not noise)


def _signal(r: dict) -> dict | None:
    """Classify a coin into a cascade or ignition signal, or None. Returns
    {kind, direction, strength, why}."""
    if r["oi_usd"] < IGNITION_MIN_OI_USD:
        return None
    fa = r["funding_ann"]
    oc = r.get("oi_chg_pct")
    # CASCADE — funding EXTREME marks the crowded/fragile side, but research is blunt:
    # the standing "high funding" state is NOT a short trigger (funding fights momentum,
    # R²≈0). The tradable event is the ROLLOVER — OI starting to FALL from the crowded
    # state = liquidation beginning. So: OI dropping while funding extreme = "firing"
    # (actionable); otherwise "crowded fragile" = context/defense only.
    if r["oi_usd"] >= MIN_OI_USD and abs(fa) >= FUNDING_CROWDED_ANN:
        side = "longs_crowded" if fa > 0 else "shorts_crowded"
        firing = oc is not None and oc <= -2      # OI unwinding
        base = (f"多头在付 {fa:.0f}%/年资金费=多头拥挤" if fa > 0
                else f"空头在付 {abs(fa):.0f}%/年资金费=空头拥挤")
        if firing:
            return {"kind": "cascade", "direction": side, "strength": "强",
                    "why": f"{base},且未平仓已回落{oc:.0f}% = 清算正在启动 → "
                           + ("向下级联,做空/避多" if fa > 0 else "轧空向上")}
        return {"kind": "cascade", "direction": side,
                "strength": "中" if abs(fa) >= 150 else "弱",
                "why": f"{base}(戒备,非做空触发——要等未平仓开始回落才是那一刻);"
                       + (f"另:正费率可做资金费套利(现货多+永续空)吃 {fa:.0f}%/年" if fa > 0 else "防轧空")}
    # IGNITION — OI rising with price (needs history)
    oc, pc = r.get("oi_chg_pct"), r.get("price_chg_since")
    if oc is not None and pc is not None and oc >= IGNITION_OI_JUMP * 100:
        if pc > 0.5:
            return {"kind": "ignition", "direction": "up",
                    "strength": "强" if oc >= 15 else "中",
                    "why": f"未平仓 +{oc:.0f}%、价 +{pc:.1f}% 同向放量 = 新杠杆进场推涨,趋势有燃料"}
        if pc < -0.5:
            return {"kind": "ignition", "direction": "down",
                    "strength": "强" if oc >= 15 else "中",
                    "why": f"未平仓 +{oc:.0f}%、价 {pc:.1f}% 同向 = 新空头进场推跌"}
    return None


def perp_signals(rows: list[dict] | None = None) -> list[dict]:
    """Ranked perp signals: cascade-crowding + ignition. Persists a snapshot each call
    so ignition (OI-delta) becomes available from the 2nd call on. Pass pre-fetched+
    stored `rows` to share one network call/snapshot with carry_signals."""
    if rows is None:
        rows = _fetch_ctxs()
        if not rows:
            return []
        _store_and_diff(rows)
    out = []
    for r in rows:
        sig = _signal(r)
        if not sig:
            continue
        out.append({
            "symbol": r["name"], "signal": sig["kind"], "direction": sig["direction"],
            "strength": sig["strength"], "why": sig["why"],
            "mark_price": r["markPx"], "funding_ann": round(r["funding_ann"], 1), "oi_usd": round(r["oi_usd"]),
            "vol24": round(r["vol24"]), "price_chg_24h": r["price_chg_24h"],
            "oi_chg_pct": r.get("oi_chg_pct"),
        })
    # rank: ignition first (actionable now), then cascade by |funding|, weighted by OI
    order = {"ignition": 0, "cascade": 1}
    out.sort(key=lambda x: (order.get(x["signal"], 2),
                            -abs(x["funding_ann"]) if x["signal"] == "cascade" else -(x["oi_chg_pct"] or 0)))
    return out


# ── Funding-carry screener (delta-neutral): the ONE replicable positive-EV core ──
# Research verdict (core-of-edge-provider-not-signal): the only edge an individual can
# actually run is being the COUNTERPARTY to leveraged longs — hold spot + short the
# perp, collect the funding they pay. This is CARRY (a risk premium), NOT free arb:
# funding can flip negative (you pay), the short leg can get squeezed/ADL'd, the venue
# can blow up or the price de-peg (2025-10-10: USDe hit $0.65 on Binance, liquidating
# solvent accounts). So we screen for SUSTAINED positive funding + flag every risk,
# and never quote a fabricated net — funding is gross of fees/slippage.
CARRY_MIN_OI_USD = 1_000_000   # need a deep perp + a real spot market to run the pair
CARRY_MIN_ANN = 8.0            # gross ann funding floor — below this, fees eat the carry
CARRY_WINDOW_H = 48            # persistence window
# clean majors: deep spot + perp, far lower de-peg/liquidity-squeeze risk than an alt
CARRY_MAJORS = {"BTC", "ETH", "SOL", "BNB", "XRP", "DOGE", "AVAX", "LTC", "LINK",
                "ADA", "SUI", "APT", "ARB", "OP", "TON", "TRX"}

# ── Net-of-cost model for the cross-venue carry ──────────────────────────────────────
# The gross differential is a %/yr RATE; the entry/exit cost is a ONE-TIME % that
# amortizes over how long you hold. Short holds (differential flips fast) = crippling
# drag. Every number below is a stated, arguable COST assumption — conservative, never a
# favorable fudge. This is the calculation that turns an exciting gross number into the
# truth: after costs, most differentials are thin or NEGATIVE, and only the fat ones on
# a long-enough hold survive.
CARRY_ROUNDTRIP_COST_PCT = 0.35   # both legs × (in+out): ~maker fees + realistic cross-
                                  # leg slippage (research: 20-50bps/leg). One-time.
CARRY_REBALANCE_DRAG_ANN = 1.5    # ongoing %/yr to keep the two legs delta-neutral as
                                  # price moves (periodic rebalances cost fee+slippage).
CARRY_DEFAULT_HOLD_DAYS = 14      # amortize the one-time cost over this hold. This is the
                                  # single biggest lever — and it EQUALS how long the
                                  # differential stays positive (measured, not assumed).
OKX_FUNDING_REQUEST_CAP = 45


def _carry_net_ann(gross_edge_ann: float, hold_days: float = CARRY_DEFAULT_HOLD_DAYS) -> float:
    """Net %/yr after amortized entry/exit cost + ongoing rebalance drag. hold_days is the
    lever: at 14d a 0.35% round-trip = ~9%/yr drag, so a +7% gross differential nets
    NEGATIVE — only fat differentials survive a short hold. Longer persistence → less drag."""
    amortized_drag = CARRY_ROUNDTRIP_COST_PCT / max(hold_days / 365.0, 1e-6)
    return gross_edge_ann - amortized_drag - CARRY_REBALANCE_DRAG_ANN


def _hl_spot_tokens() -> set[str]:
    """Coin names with a USDC spot market on Hyperliquid — the carry can be run
    single-venue there (buy HL spot, short HL perp). Keyless. Empty set on failure so we
    fall back to major-only executability, NEVER a false 'hedgeable'. Note BTC/ETH/SOL
    are perp-only on HL — their spot lives on CEXes, handled via the is_major path."""
    try:
        req = urllib.request.Request(
            INFO_URL, data=json.dumps({"type": "spotMeta"}).encode(),
            headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=15) as response:
            d = json.loads(response.read())
        idx = {t["index"]: t["name"] for t in d.get("tokens", [])}
        bases = set()
        for u in d.get("universe", []):
            ts = u.get("tokens", [])
            if len(ts) == 2 and idx.get(ts[1]) == "USDC":
                bases.add(idx.get(ts[0]))
        bases.discard(None)
        return bases
    except Exception as e:
        logger.warning("hl_spot_fetch_failed", error=str(e)[:100])
        return set()


OKX_FUNDING_URL = "https://www.okx.com/api/v5/public/funding-rate?instId={}-USDT-SWAP"
OKX_FUNDING_MAX_AGE_MS = 5 * 60 * 1000


def _store_xdiff(diffs: dict[str, float]) -> None:
    """Persist each coin's HL-OKX differential per run, so xdiff_stats can measure how
    long the differential actually STAYS positive — that persistence IS the hold period,
    which is the single biggest lever on net-of-cost. Never raises."""
    if not diffs:
        return
    now = datetime.now(timezone.utc).isoformat()
    try:
        c = _conn()
        c.execute("CREATE TABLE IF NOT EXISTS xdiff(coin TEXT, ts TEXT, diff_ann REAL)")
        c.executemany("INSERT INTO xdiff(coin, ts, diff_ann) VALUES (?,?,?)",
                      [(k, now, v) for k, v in diffs.items()])
        # prune > 30d
        from datetime import timedelta
        cut = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
        c.execute("DELETE FROM xdiff WHERE ts < ?", (cut,))
        c.commit()
        c.close()
    except Exception as e:
        logger.debug("xdiff_store_failed", error=str(e)[:80])


def xdiff_stats(window_h: int = 7 * 24) -> dict[str, dict]:
    """Per coin over the window: {coin: {pos_frac, mean, n, span_h}} for the HL-OKX
    differential. Empty until history accrues — honestly absent, not faked."""
    from datetime import timedelta
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=window_h)).isoformat()
    agg: dict[str, list] = {}
    try:
        c = _conn()
        c.execute("CREATE TABLE IF NOT EXISTS xdiff(coin TEXT, ts TEXT, diff_ann REAL)")
        for coin, ts, dv in c.execute(
                "SELECT coin, ts, diff_ann FROM xdiff WHERE ts >= ?", (cutoff,)):
            agg.setdefault(coin, []).append((ts, dv))
        c.close()
    except Exception:
        return {}
    out = {}
    for coin, pts in agg.items():
        vals = [v for _, v in pts]
        if len(vals) < 3:
            continue
        tss = sorted(t for t, _ in pts)
        try:
            span = (datetime.fromisoformat(tss[-1]) - datetime.fromisoformat(tss[0])).total_seconds() / 3600
        except Exception:
            span = 0
        out[coin] = {"pos_frac": sum(1 for v in vals if v > 0) / len(vals),
                     "mean": sum(vals) / len(vals), "n": len(vals), "span_h": round(span, 1)}
    return out


def _okx_funding_ann(row: dict, *, now_ms: int | None = None) -> float | None:
    """Annualize one period rate using OKX's actual scheduled interval."""
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000) if now_ms is None else now_ms
    try:
        observed_ms = int(row["ts"])
        funding_ms = int(row["fundingTime"])
        next_ms = int(row["nextFundingTime"])
        rate = float(row["fundingRate"])
    except (KeyError, TypeError, ValueError):
        return None
    age_ms = now_ms - observed_ms
    interval_h = (next_ms - funding_ms) / 3_600_000
    if (not math.isfinite(rate) or age_ms < -5_000 or age_ms > OKX_FUNDING_MAX_AGE_MS
            or not (0.5 <= interval_h <= 24)):
        return None
    return rate * (24 / interval_h) * 365 * 100


def okx_funding_map(coins: list[str], cap: int = 45, fetch=None) -> dict[str, float]:
    """{coin: OKX annualized funding %} for the given coins (bounded loop, keyless).
    OKX's interval can change by contract/regime, so each rate is annualized from the
    response's fundingTime→nextFundingTime interval. The cross-exchange EDGE lives here:
    higher than CEX (institutions can't touch the DEX leg), so HL_ann − OKX_ann is a
    delta-neutral two-perp carry (short the high-funding venue, long the low). Never
    raises; a coin absent from OKX or a failed fetch is simply omitted (→ no false diff).
    Binance/Bybit are geo-blocked (451/403) from here; OKX is the reachable CEX leg."""
    out: dict[str, float] = {}
    if fetch is None:
        def fetch(url):
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=8) as response:
                return json.loads(response.read())
    for c in coins[:cap]:
        for base in okx_symbol_candidates(c):
            try:
                d = fetch(OKX_FUNDING_URL.format(base))
                rows = d.get("data") or []
            except Exception:
                break
            if not rows:
                continue
            annualized = _okx_funding_ann(rows[0])
            if annualized is not None:
                out[c] = annualized
            break
    return out


def _funding_persistence(window_h: int = CARRY_WINDOW_H) -> dict[str, dict]:
    """Per coin over the last `window_h`: {coin: {mean_ann, pos_frac, n}} from the
    persisted snapshots. Empty until snapshots accrue — honestly absent, not faked."""
    from datetime import timedelta
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=window_h)).isoformat()
    c = _conn()
    agg: dict[str, list[float]] = {}
    try:
        for coin, fa in c.execute(
                "SELECT coin, funding_ann FROM snaps WHERE ts >= ? AND funding_ann IS NOT NULL",
                (cutoff,)):
            agg.setdefault(coin, []).append(fa)
    finally:
        c.close()
    out = {}
    for coin, xs in agg.items():
        if xs:
            out[coin] = {"mean_ann": sum(xs) / len(xs),
                         "pos_frac": sum(1 for x in xs if x > 0) / len(xs),
                         "n": len(xs)}
    return out


SCORECARD_WINDOW_H = 7 * 24
SCORECARD_MIN_SNAPS = 20       # same MIN_N discipline as the board measurement layer
SCORECARD_MIN_SPAN_H = 24      # need a real span, not 20 snapshots in one burst


def carry_scorecard(window_h: int = SCORECARD_WINDOW_H) -> dict:
    """Lane-level REALIZED-carry track record from the accumulated snapshots — the honest
    proof/disproof of the edge core. Not 'funding is 11% now' but 'holding the clean-major
    carry basket over the window actually realized X%/yr and stayed positive Z% of the
    time, worst instantaneous −W%'. Refuses a number until the sample supports one
    ('不可判'), exactly like board_outcomes — a realized carry is only honest with history.

    Scope = CARRY_MAJORS only: the executable, deep-spot subset. Alts' realized funding
    is dominated by de-peg/liquidation tail risk this can't capture, so quoting a basket
    number on them would overstate an individual's achievable carry."""
    from datetime import timedelta
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=window_h)).isoformat()
    c = _conn()
    pts: list[tuple[str, float]] = []      # (ts, funding_ann) across all major coins
    try:
        for ts, coin, fa in c.execute(
                "SELECT ts, coin, funding_ann FROM snaps "
                "WHERE ts >= ? AND funding_ann IS NOT NULL", (cutoff,)):
            if coin in CARRY_MAJORS:
                pts.append((ts, fa))
    finally:
        c.close()
    if len(pts) < SCORECARD_MIN_SNAPS:
        return {"available": False, "verdict": "不可判", "n": len(pts),
                "note": f"已实现carry战绩积累中({len(pts)}/{SCORECARD_MIN_SNAPS}个快照点)"}
    tss = sorted(t for t, _ in pts)
    try:
        span_h = (datetime.fromisoformat(tss[-1]) - datetime.fromisoformat(tss[0])).total_seconds() / 3600
    except Exception:
        span_h = 0
    if span_h < SCORECARD_MIN_SPAN_H:
        return {"available": False, "verdict": "不可判", "n": len(pts),
                "note": f"跨度仅{span_h:.0f}h(<{SCORECARD_MIN_SPAN_H}h),等更长历史"}
    fas = [fa for _, fa in pts]
    realized = sum(fas) / len(fas)
    pos_frac = sum(1 for x in fas if x > 0) / len(fas)
    return {"available": True, "verdict": "measured",
            "realized_ann": round(realized, 1), "pos_frac": round(pos_frac, 3),
            "worst_ann": round(min(fas), 1), "n": len(fas), "span_h": round(span_h, 1),
            "note": (f"主流carry篮子近{span_h:.0f}h已实现约 {realized:.0f}%/年(毛),"
                     f"{pos_frac*100:.0f}%时间为正,最差瞬时 {min(fas):.0f}%/年。"
                     "毛值未扣手续费/滑点,且不含脱锚/挤压尾部。")}


def carry_signals(rows: list[dict] | None = None, *,
                  okx_rates: dict[str, float] | None = None) -> list[dict]:
    """Funding-carry opportunities, with the CROSS-EXCHANGE differential (HL vs OKX) as
    the primary edge — the one research-validated play our breadth can capture: HL funding
    is structurally higher than CEX (institutions can't touch the DEX leg), so short the
    high-funding venue + long the low = a delta-neutral TWO-PERP carry, NO spot leg needed.
    Falls back to single-venue (spot-hedged) carry when OKX lacks the perp. Ranked by the
    cross-venue differential (or sustained HL funding), persistence-weighted, majors lifted.
    Pass pre-fetched+stored `rows` to share one call with perp_signals."""
    if rows is None:
        rows = _fetch_ctxs()
        if not rows:
            return []
        _store_and_diff(rows)      # also records funding for persistence history
    hist = _funding_persistence()
    hl_spot = _hl_spot_tokens()    # coins with a HL spot leg (single-venue hedge)
    xstats = xdiff_stats()         # measured persistence of the HL-OKX differential
    # candidates = coins with notable HL funding; fetch the OKX leg for just these.
    cands = [r["name"] for r in rows
             if r["oi_usd"] >= CARRY_MIN_OI_USD and r["funding_ann"] >= CARRY_MIN_ANN]
    okx = okx_rates if okx_rates is not None else okx_funding_map(cands)
    diffs_to_store: dict[str, float] = {}
    out = []
    for r in rows:
        fa = r["funding_ann"]
        if r["oi_usd"] < CARRY_MIN_OI_USD or fa < CARRY_MIN_ANN:
            continue
        h = hist.get(r["name"])
        sustained = h["mean_ann"] if h and h["n"] >= 3 else fa   # HL sustained funding
        pos_frac = h["pos_frac"] if h and h["n"] >= 3 else None
        is_major = r["name"] in CARRY_MAJORS
        has_hl_spot = r["name"] in hl_spot
        okx_ann = okx.get(r["name"])
        cross = okx_ann is not None                      # both venues have the perp
        # PRIMARY: cross-venue two-perp arb (no spot). FALLBACK: single-venue spot-hedge.
        if cross:
            observed_cross_diff = fa - okx_ann            # same-window observation
            cross_diff = sustained - okx_ann              # persistence-weighted score
            edge_ann = cross_diff
            hedge = "跨所两永续(空HL多OKX)· 无需现货"
            diffs_to_store[r["name"]] = observed_cross_diff
        elif is_major:
            edge_ann = sustained; hedge = "外部现货(CEX)可对冲"
        elif has_hl_spot:
            edge_ann = sustained; hedge = "HL现货可单所对冲"
        else:
            # no OKX perp AND no spot leg → can't run delta-neutral. A naked short is a
            # directional bet, not a carry — its extreme funding shows in the heatmap.
            continue
        if cross and edge_ann < 3.0:
            continue                                     # differential too thin to bother
        flags = []
        if not is_major:
            flags.append("非主流:清算/脱锚/深度风险高")
        if r["oi_usd"] < 3_000_000:
            flags.append("深度薄:滑点+空腿易被挤压")
        if pos_frac is not None and pos_frac < 0.8:
            flags.append(f"费率不稳:仅{pos_frac*100:.0f}%时间为正,会倒付")
        if pos_frac is None:
            flags.append("持续性积累中(需多轮快照)")
        flags.append("尾部:ADL级联(2025-10-10)会强平对冲腿——为崩盘sizing")
        # hold_days = how long the differential actually STAYS positive (measured), which
        # sets the cost amortization. Until history accrues, use the conservative default
        # and say so — never assume a long hold we haven't observed.
        xs = xstats.get(r["name"]) if cross else None
        if xs and xs["n"] >= 6 and xs["pos_frac"] >= 0.8:
            hold_days = min(max(xs["span_h"] / 24.0, 1.0), 30.0)
            hold_measured = True
        else:
            hold_days = CARRY_DEFAULT_HOLD_DAYS
            hold_measured = False
        # NET after costs — the number that matters. Rank by this, not the gross edge.
        net_ann = _carry_net_ann(edge_ann, hold_days)
        if net_ann <= 0:
            flags.insert(0, f"净额≤0:毛{edge_ann:.0f}%扣成本后不划算(需差价更肥或持仓更久)")
        if cross and not hold_measured:
            flags.append("差价持续性未测(净额按14天假设,真值取决于差价能撑多久)")
        pf = pos_frac if pos_frac is not None else 0.5
        quality = net_ann * pf * (1.3 if is_major else 1.0)
        note = (f"跨所差(毛){edge_ann:.0f}%:空HL(+{sustained:.0f}%)、多OKX({okx_ann:+.0f}%)。"
                f"扣成本(往返~{CARRY_ROUNDTRIP_COST_PCT}%按{CARRY_DEFAULT_HOLD_DAYS}天摊+再平衡{CARRY_REBALANCE_DRAG_ANN}%)"
                f"净约 {net_ann:.0f}%/年。两永续delta中性,不需现货。持仓越久摊得越薄(=差价持续多久,测量中)。"
                if cross else
                f"现货多+永续空,毛{sustained:.0f}%,扣成本净约{net_ann:.0f}%/年。对冲:{hedge}。")
        out.append({
            "symbol": r["name"], "funding_ann": round(fa, 1),
            "sustained_ann": round(sustained, 1), "pos_frac": pos_frac,
            "okx_ann": round(okx_ann, 1) if cross else None,
            "cross_diff": round(cross_diff, 1) if cross else None, "cross": cross,
            "edge_ann": round(edge_ann, 1), "score_edge_ann": round(edge_ann, 1),
            "observed_edge_ann": (observed_cross_diff if cross else None),
            "current_hl_ann": fa, "net_ann": round(net_ann, 1),
            "hold_days": round(hold_days, 1), "hold_measured": (hold_measured if cross else None),
            "diff_pos_frac": (round(xs["pos_frac"], 2) if xs else None),
            "trade": ("空HL·多OKX" if cross else "现货多·永续空"),
            "oi_usd": round(r["oi_usd"]), "is_major": is_major,
            "tier": "主流" if is_major else "山寨", "hedge": hedge,
            "flags": flags, "n_snaps": h["n"] if h else 0, "_q": quality, "note": note,
        })
    _store_xdiff(diffs_to_store)   # persist this run's differentials for persistence stats
    out.sort(key=lambda x: -x["_q"])
    for x in out:
        del x["_q"]
    return out


def scan_carry(rows: list[dict] | None = None, *,
               priority_symbols: list[str] | None = None) -> dict:
    """Return entry signals separately from current observations of open episodes."""
    fetched_here = rows is None
    if rows is None:
        rows = _fetch_ctxs()
        if rows:
            _store_and_diff(rows)
    priorities = list(dict.fromkeys(str(s) for s in (priority_symbols or []) if s))
    scan_at = datetime.now(timezone.utc).isoformat()
    if not rows:
        statuses = [{"symbol": symbol, "status": "hl_source_unavailable"}
                    for symbol in priorities]
        return {
            "signals": [], "open_observations": [], "open_status": statuses,
            "source_health": {
                "state": "unavailable", "scan_at": scan_at,
                "hl_rows": 0, "open_requested": len(priorities), "open_observed": 0,
                "okx_requested": 0, "okx_observed": 0, "entry_deferred_by_cap": 0,
                "fetched_here": fetched_here,
            },
        }

    by_symbol = {r.get("name"): r for r in rows if r.get("name")}
    entry_symbols = [r["name"] for r in rows
                     if r.get("oi_usd", 0) >= CARRY_MIN_OI_USD
                     and r.get("funding_ann", 0) >= CARRY_MIN_ANN]
    requested = list(dict.fromkeys(priorities + entry_symbols))
    limited = requested[:OKX_FUNDING_REQUEST_CAP]
    okx = okx_funding_map(limited, cap=len(limited)) if limited else {}
    limited_set = set(limited)
    signal_rows = [r for r in rows if r.get("name") in limited_set]
    signals = carry_signals(signal_rows, okx_rates=okx)

    observations = []
    statuses = []
    for symbol in priorities:
        row = by_symbol.get(symbol)
        if row is None:
            status = "hl_symbol_unavailable"
        elif symbol not in limited_set:
            status = "okx_request_cap"
        elif symbol not in okx:
            status = "okx_rate_unavailable"
        else:
            observed_edge = float(row["funding_ann"]) - float(okx[symbol])
            observation = {
                "symbol": symbol, "status": "observed", "cross": True,
                "observation_version": 1, "observed_at": scan_at,
                "hl_ann": float(row["funding_ann"]), "okx_ann": float(okx[symbol]),
                "observed_edge_ann": observed_edge, "edge_ann": observed_edge,
            }
            observations.append(observation)
            status = "observed"
        statuses.append({"symbol": symbol, "status": status})

    observed_n = len(observations)
    state = "ok" if observed_n == len(priorities) else "partial"
    return {
        "signals": signals, "open_observations": observations, "open_status": statuses,
        "source_health": {
            "state": state, "scan_at": scan_at, "hl_rows": len(rows),
            "open_requested": len(priorities), "open_observed": observed_n,
            "okx_requested": len(limited), "okx_observed": len(okx),
            "entry_deferred_by_cap": sum(s not in limited_set for s in entry_symbols),
            "fetched_here": fetched_here,
        },
    }


if __name__ == "__main__":
    from dotenv import load_dotenv

    from src.config import PROJECT_ROOT
    load_dotenv(PROJECT_ROOT / ".env")
    sigs = perp_signals()
    print(f"{len(sigs)} perp signals")
    for s in sigs[:15]:
        print(f"  {s['symbol']:9} {s['signal']:8} {s['direction']:14} [{s['strength']}] "
              f"fund {s['funding_ann']:+.0f}% OI ${s['oi_usd']/1e6:.1f}M — {s['why'][:60]}")
    carry = scan_carry()["signals"]
    print(f"\n{len(carry)} funding-carry opportunities (delta-neutral)")
    for s in carry[:15]:
        print(f"  {s['symbol']:9} [{s['tier']}] ann {s['funding_ann']:+.0f}% sustained "
              f"{s['sustained_ann']:+.0f}% OI ${s['oi_usd']/1e6:.1f}M {s['hedge']} "
              f"{'· '.join(s['flags']) if s['flags'] else 'clean'}")
    sc = carry_scorecard()
    print(f"\ncarry scorecard: {sc.get('note')}")
