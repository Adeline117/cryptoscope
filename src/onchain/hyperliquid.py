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
from concurrent.futures import ThreadPoolExecutor, wait
from datetime import datetime, timezone
from time import monotonic

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


def fetch_ctxs_result(fetch=None) -> dict:
    """Typed Hyperliquid snapshot result; transport failure is not an empty market."""
    attempted_at = datetime.now(timezone.utc).isoformat()
    try:
        if fetch is None:
            req = urllib.request.Request(
                INFO_URL, data=json.dumps({"type": "metaAndAssetCtxs"}).encode(),
                headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(req, timeout=20) as response:
                data = json.loads(response.read())
        else:
            data = fetch()
    except Exception as e:
        logger.warning("hyperliquid_fetch_failed", error=str(e)[:100])
        return {"rows": [], "health": {"state": "unavailable", "rows": 0,
                "attempted_at": attempted_at, "error_kind": "request_failed",
                "error": str(e)[:120]}}
    if (not isinstance(data, list) or len(data) < 2 or not isinstance(data[0], dict)
            or not isinstance(data[0].get("universe"), list)
            or not isinstance(data[1], list)):
        return {"rows": [], "health": {"state": "unavailable", "rows": 0,
                "attempted_at": attempted_at, "error_kind": "malformed_response"}}
    universe, ctxs = data[0]["universe"], data[1]
    out = []
    invalid_rows = abs(len(universe) - len(ctxs))
    for u, c in zip(universe, ctxs):
        try:
            name = str(u["name"])
            px = float(c["markPx"])
            open_interest = float(c["openInterest"])
            fund = float(c["funding"])
            vol = float(c["dayNtlVlm"])
            prev = float(c.get("prevDayPx") or 0)
            if (not name or not all(math.isfinite(x) for x in
                                    (px, open_interest, fund, vol, prev))
                    or px <= 0 or open_interest < 0 or vol < 0 or prev < 0):
                raise ValueError("invalid market row")
            oi = open_interest * px
            out.append({
                "name": name, "markPx": px, "oi_usd": oi,
                "funding_ann": fund * 24 * 365 * 100,     # annualized %
                "vol24": vol,
                "price_chg_24h": ((px / prev - 1) * 100) if prev else None,
            })
        except Exception:
            invalid_rows += 1
            continue
    if not out:
        return {"rows": [], "health": {"state": "unavailable", "rows": 0,
                "attempted_at": attempted_at, "error_kind": "no_valid_rows",
                "invalid_rows": invalid_rows}}
    return {"rows": out, "health": {
            "state": "partial" if invalid_rows else "ok", "rows": len(out),
            "attempted_at": attempted_at, "last_success_at": attempted_at,
            "invalid_rows": invalid_rows, "expected_rows": len(universe)}}


def _fetch_ctxs() -> list[dict]:
    """Compatibility wrapper for callers that only consume rows."""
    return fetch_ctxs_result()["rows"]


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


# ── Funding-carry screener (delta-neutral candidate hypothesis) ────────────────────
# Carry can collect a funding differential while hedging direction, but this repository
# has not established a real-fill, all-in positive edge. It remains a risk-premium proxy:
# funding can flip negative (you pay), the short leg can get squeezed/ADL'd, the venue
# can blow up or the price de-peg (2025-10-10: USDe hit $0.65 on Binance, liquidating
# solvent accounts). We screen sustained quotes and expose every missing cost component.
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
# favorable fudge. The output is a partial model proxy, not an all-in net return.
CARRY_ROUNDTRIP_COST_PCT = 0.35   # both legs × (in+out): ~maker fees + realistic cross-
                                  # leg slippage (research: 20-50bps/leg). One-time.
CARRY_REBALANCE_DRAG_ANN = 1.5    # ongoing %/yr to keep the two legs delta-neutral as
                                  # price moves (periodic rebalances cost fee+slippage).
CARRY_MODEL_HOLD_DAYS_ASSUMPTION = 14  # disclosed scenario input, never inferred from
                                       # sparse quote-history coverage
OKX_FUNDING_REQUEST_CAP = 256
OKX_FUNDING_MAX_WORKERS = 1
OKX_FUNDING_SCAN_TIMEOUT_S = 15.0
CARRY_ENTRY_PRIORITY_METHOD = "current_hl_funding_desc_then_oi_desc_v1"


def _carry_partial_model_proxy_ann(gross_edge_ann: float) -> float:
    """Fixed-scenario proxy, not an all-in or realized annual return.

    Quote-history coverage never changes the assumed 14-day amortization. Basis, account
    fees, collateral, transfers, rebalancing paths and fills remain unknown.
    """
    assumed_years = CARRY_MODEL_HOLD_DAYS_ASSUMPTION / 365.0
    amortized_drag = CARRY_ROUNDTRIP_COST_PCT / assumed_years
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


OKX_FUNDING_BULK_URL = (
    "https://www.okx.com/api/v5/public/funding-rate?instId=ANY"
)
OKX_FUNDING_MAX_AGE_MS = 5 * 60 * 1000


def _store_xdiff(diffs: dict[str, float]) -> None:
    """Persist sparse HL-OKX quote differences for descriptive coverage statistics."""
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
    """Describe sparse quote coverage; never infer a continuous holding period from it."""
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
        out[coin] = {
            "positive_fraction": sum(1 for v in vals if v > 0) / len(vals),
            "mean_ann": sum(vals) / len(vals),
            "point_count": len(vals),
            "coverage_span_h": round(span, 1),
        }
    return out


def _okx_funding_value(row: dict, *, now_ms: int | None = None) -> tuple[float | None, str]:
    """Return annualized rate plus observed/stale/invalid classification."""
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000) if now_ms is None else now_ms
    try:
        observed_ms = int(row["ts"])
        funding_ms = int(row["fundingTime"])
        next_ms = int(row["nextFundingTime"])
        rate = float(row["fundingRate"])
    except (KeyError, TypeError, ValueError):
        return None, "rate_invalid"
    age_ms = now_ms - observed_ms
    interval_h = (next_ms - funding_ms) / 3_600_000
    if age_ms < -5_000 or age_ms > OKX_FUNDING_MAX_AGE_MS:
        return None, "rate_stale"
    if not math.isfinite(rate) or not (0.5 <= interval_h <= 24):
        return None, "rate_invalid"
    annualized = rate * (24 / interval_h) * 365 * 100
    return (annualized, "observed") if math.isfinite(annualized) else (None, "rate_invalid")


def _okx_funding_ann(row: dict, *, now_ms: int | None = None) -> float | None:
    """Compatibility wrapper returning only a verified current annualized rate."""
    value, status = _okx_funding_value(row, now_ms=now_ms)
    return value if status == "observed" else None


def okx_funding_scan(coins: list[str], cap: int = OKX_FUNDING_REQUEST_CAP,
                     fetch=None, *, max_workers: int = OKX_FUNDING_MAX_WORKERS,
                     scan_timeout_s: float = OKX_FUNDING_SCAN_TIMEOUT_S) -> dict:
    """Map the requested HL universe from one bounded OKX ``instId=ANY`` snapshot.

    OKX documents ``ANY`` as all perpetual/X-Perps funding rows. One snapshot removes
    the old 45-symbol coverage truncation and avoids a burst of per-instrument requests.
    A source failure applies to every requested symbol and can never become
    ``unsupported``. Output key order remains identical to the input.
    """
    symbols = list(dict.fromkeys(str(c) for c in coins if c))
    attempted_at = datetime.now(timezone.utc).isoformat()
    started = monotonic()
    if fetch is None:
        def fetch(url):
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=8) as response:
                return json.loads(response.read())
    rates: dict[str, float] = {}
    statuses: dict[str, str] = {}
    try:
        request_cap = max(0, int(cap))
    except (TypeError, ValueError):
        request_cap = OKX_FUNDING_REQUEST_CAP
    limited = symbols[:request_cap]
    actual_workers = 1 if limited else 0
    upstream_requests = 0
    bulk_rows = 0
    bulk_usdt_swap_rows = 0
    bulk_invalid_rows = 0
    source_error_kind = None
    source_error = None
    data = None
    if limited:
        executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="okx-funding-bulk",
        )
        future = executor.submit(fetch, OKX_FUNDING_BULK_URL)
        upstream_requests = 1
        try:
            done, pending = wait(
                [future], timeout=max(float(scan_timeout_s), 0.0),
            )
            if pending:
                source_error_kind = "request_timeout"
                source_error = "OKX bulk funding snapshot timed out"
                future.cancel()
            elif done:
                try:
                    data = future.result()
                except Exception as exc:
                    reason = getattr(exc, "reason", None)
                    timed_out = isinstance(exc, TimeoutError) or isinstance(
                        reason, TimeoutError,
                    )
                    source_error_kind = (
                        "request_timeout" if timed_out else "request_failed"
                    )
                    source_error = str(exc)[:160]
        finally:
            executor.shutdown(wait=False, cancel_futures=True)

    index: dict[str, dict] = {}
    duplicate_ids: set[str] = set()
    if limited and source_error_kind is None:
        if not isinstance(data, dict) or str(data.get("code")) != "0":
            source_error_kind = "request_failed"
            source_error = (
                f"OKX bulk funding response code {data.get('code')}"
                if isinstance(data, dict) else "malformed OKX bulk funding response"
            )
        elif not isinstance(data.get("data"), list) or not data["data"]:
            source_error_kind = "request_failed"
            source_error = "OKX bulk funding response has no non-empty row list"
        else:
            bulk_rows = len(data["data"])
            for row in data["data"]:
                inst_id = row.get("instId") if isinstance(row, dict) else None
                if not isinstance(inst_id, str) or not inst_id:
                    bulk_invalid_rows += 1
                    continue
                if inst_id in index:
                    duplicate_ids.add(inst_id)
                    bulk_invalid_rows += 1
                    continue
                index[inst_id] = row
            for inst_id in duplicate_ids:
                index.pop(inst_id, None)
            bulk_usdt_swap_rows = sum(
                inst_id.endswith("-USDT-SWAP")
                and row.get("instType") == "SWAP"
                for inst_id, row in index.items()
            )
            if not bulk_usdt_swap_rows:
                source_error_kind = "request_failed"
                source_error = "OKX bulk funding response has no valid USDT swaps"

    if source_error_kind is not None:
        for symbol in limited:
            statuses[symbol] = source_error_kind
    else:
        for symbol in limited:
            status = "rate_invalid" if bulk_invalid_rows else "unsupported"
            value = None
            for base in okx_symbol_candidates(symbol):
                inst_id = f"{base}-USDT-SWAP"
                if inst_id in duplicate_ids:
                    status = "rate_invalid"
                    break
                row = index.get(inst_id)
                if row is None:
                    continue
                if row.get("instType") != "SWAP":
                    status = "rate_invalid"
                    break
                value, status = _okx_funding_value(row)
                break
            statuses[symbol] = status
            if status == "observed" and value is not None:
                rates[symbol] = value
    for symbol in symbols[len(limited):]:
        statuses[symbol] = "request_cap"
    counts = {name: sum(value == name for value in statuses.values()) for name in (
        "observed", "unsupported", "request_failed", "request_timeout", "rate_stale",
        "rate_invalid", "request_cap")}
    bad = (counts["request_failed"] + counts["request_timeout"]
           + counts["rate_stale"] + counts["rate_invalid"])
    if not symbols:
        state = "not_needed"
    elif bad and not rates and counts["unsupported"] == 0:
        state = "unavailable"
    elif bad or counts["request_cap"] or bulk_invalid_rows:
        state = "partial"
    else:
        state = "ok"
    return {"rates": rates, "status_by_symbol": statuses,
            "summary": {"state": state, "attempted_at": attempted_at,
                        "requested": len(symbols), **counts,
                        "duration_ms": round((monotonic() - started) * 1000),
                        "max_workers": actual_workers,
                        "transport_mode": "bulk_any",
                        "upstream_requests": upstream_requests,
                        "bulk_rows": bulk_rows,
                        "bulk_usdt_swap_rows": bulk_usdt_swap_rows,
                        "bulk_invalid_rows": bulk_invalid_rows,
                        "source_error_kind": source_error_kind,
                        "source_error": source_error}}


def okx_funding_map(coins: list[str], cap: int = OKX_FUNDING_REQUEST_CAP,
                    fetch=None) -> dict[str, float]:
    """{coin: OKX annualized funding %} for the given coins (bounded loop, keyless).
    OKX's interval can change by contract/regime, so each rate is annualized from the
    response's fundingTime→nextFundingTime interval. The cross-exchange EDGE lives here:
    higher than CEX (institutions can't touch the DEX leg), so HL_ann − OKX_ann is a
    delta-neutral two-perp carry (short the high-funding venue, long the low). Never
    raises; a coin absent from OKX or a failed fetch is simply omitted (→ no false diff).
    Binance/Bybit are geo-blocked (451/403) from here; OKX is the reachable CEX leg."""
    return okx_funding_scan(coins, cap=cap, fetch=fetch)["rates"]


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
    """Describe historical HL funding *quote snapshots*, never portfolio PnL.

    The series has no OKX leg, positions, settlement timestamps, basis accounting, fees,
    or fills. It is useful context for persistence only and cannot prove/disprove carry
    profitability on its own.
    """
    from datetime import timedelta
    base = {
        "measure_kind": "hl_funding_quote_snapshot_proxy",
        "is_realized_pnl": False,
        "includes_okx_leg": False,
        "includes_funding_settlements": False,
        "includes_basis_pnl": False,
        "includes_costs": False,
    }
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
        return {**base, "available": False, "verdict": "不可判", "n": len(pts),
                "note": f"HL 报价费率快照积累中({len(pts)}/{SCORECARD_MIN_SNAPS}个点)"}
    tss = sorted(t for t, _ in pts)
    try:
        span_h = (datetime.fromisoformat(tss[-1]) - datetime.fromisoformat(tss[0])).total_seconds() / 3600
    except Exception:
        span_h = 0
    if span_h < SCORECARD_MIN_SPAN_H:
        return {**base, "available": False, "verdict": "不可判", "n": len(pts),
                "note": f"跨度仅{span_h:.0f}h(<{SCORECARD_MIN_SPAN_H}h),等更长历史"}
    fas = [fa for _, fa in pts]
    quoted_proxy = sum(fas) / len(fas)
    pos_frac = sum(1 for x in fas if x > 0) / len(fas)
    return {**base, "available": True, "verdict": "measured",
            "quoted_hl_funding_rate_proxy_ann": round(quoted_proxy, 1),
            "positive_quote_fraction": round(pos_frac, 3),
            "worst_quoted_rate_ann": round(min(fas), 1),
            "n": len(fas), "span_h": round(span_h, 1),
            "note": (f"主流币近{span_h:.0f}h HL 资金费报价快照均值约 "
                     f"{quoted_proxy:.0f}%/年，{pos_frac*100:.0f}% 快照为正，"
                     f"最差瞬时报价率 {min(fas):.0f}%/年。未包含 OKX 腿、"
                     "实际资金费结算、basis、成交或成本。")}


def carry_signals(rows: list[dict] | None = None, *,
                  okx_rates: dict[str, float] | None = None) -> list[dict]:
    """Funding-carry opportunities, with the CROSS-EXCHANGE differential (HL vs OKX) as
    primary hypothesis. Short the high-funding venue + long the low is directionally
    hedged in concept, but basis, liquidation, venue and execution risks remain. Single-
    venue spot-hedged rows are kept as a separate candidate scope and never enter the
    cross-venue paper ledger. Pass pre-fetched `rows` to share one market snapshot."""
    if rows is None:
        rows = _fetch_ctxs()
        if not rows:
            return []
        _store_and_diff(rows)      # also records funding for persistence history
    hist = _funding_persistence()
    hl_spot = _hl_spot_tokens()    # coins with a HL spot leg (single-venue hedge)
    xstats = xdiff_stats()         # sparse quote coverage, not a measured holding period
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
        xs = xstats.get(r["name"]) if cross else None
        partial_proxy = _carry_partial_model_proxy_ann(edge_ann)
        if partial_proxy <= 0:
            flags.insert(0, f"部分模型代理≤0:毛{edge_ann:.0f}%不足以覆盖当前情景成本")
        if cross:
            flags.append(
                f"持有期未验证(模型固定按{CARRY_MODEL_HOLD_DAYS_ASSUMPTION}天假设;"
                "历史仅为稀疏报价覆盖)"
            )
        pf = pos_frac if pos_frac is not None else 0.5
        quality = partial_proxy * pf * (1.3 if is_major else 1.0)
        note = (f"跨所差(毛){edge_ann:.0f}%:空HL(+{sustained:.0f}%)、多OKX({okx_ann:+.0f}%)。"
                f"部分成本情景(往返~{CARRY_ROUNDTRIP_COST_PCT}%按"
                f"{CARRY_MODEL_HOLD_DAYS_ASSUMPTION}天假设摊销+再平衡"
                f"{CARRY_REBALANCE_DRAG_ANN}%)代理约 {partial_proxy:.0f}%/年。"
                "持有期未验证；历史跨度只表示稀疏报价覆盖。"
                if cross else
                f"现货多+永续空,毛{sustained:.0f}%,部分成本情景代理约"
                f"{partial_proxy:.0f}%/年。对冲:{hedge}；不进入跨所双腿纸面账本。")
        out.append({
            "symbol": r["name"], "funding_ann": round(fa, 1),
            "sustained_ann": round(sustained, 1), "pos_frac": pos_frac,
            "okx_ann": round(okx_ann, 1) if cross else None,
            "cross": cross,
            "gross_funding_diff_ann_pct": round(edge_ann, 1),
            "ranking_metric": "gross_funding_diff_ann_pct",
            "current_paired_funding_diff_ann_pct": (
                round(observed_cross_diff, 1) if cross else None
            ),
            "current_hl_ann": fa,
            "partial_model_proxy_ann_pct": round(partial_proxy, 1),
            "all_in_net_ann_pct": None,
            "cost_completeness": "partial", "is_realized": False,
            "model_hold_days_assumption": CARRY_MODEL_HOLD_DAYS_ASSUMPTION,
            "hold_period_verified": False,
            "coverage_span_h": xs["coverage_span_h"] if xs else None,
            "coverage_point_count": xs["point_count"] if xs else 0,
            "coverage_positive_fraction": (
                round(xs["positive_fraction"], 2) if xs else None),
            "partial_model_method": "fixed_14d_roundtrip_and_rebalance_proxy_v1",
            "candidate_scope": ("cross_venue_two_perp" if cross
                                else "single_venue_spot_perp"),
            "paper_measurement_eligible": cross,
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
               priority_symbols: list[str] | None = None,
               hl_health: dict | None = None) -> dict:
    """Return entry signals plus versioned current-pair paper observations."""
    fetched_here = rows is None
    if rows is None:
        result = fetch_ctxs_result()
        rows = result["rows"]
        hl_health = result["health"]
        if rows:
            _store_and_diff(rows)
    elif hl_health is None:
        hl_health = {"state": "ok" if rows else "unavailable", "rows": len(rows),
                     "attempted_at": datetime.now(timezone.utc).isoformat(),
                     **({} if rows else {"error_kind": "rows_unavailable"})}
    priorities = list(dict.fromkeys(str(s) for s in (priority_symbols or []) if s))
    scan_at = datetime.now(timezone.utc).isoformat()
    if not rows:
        statuses = [{"symbol": symbol, "role": "open",
                     "status": "hl_source_unavailable"}
                    for symbol in priorities]
        return {
            "signals": [], "open_observations": [], "entry_observations": [],
            "paper_observations": [], "open_status": statuses,
            "source_health": {
                "schema_version": 1, "state": "unavailable", "scan_at": scan_at,
                "hl": hl_health, "okx": {"state": "not_needed", "requested": 0,
                                           "observed": 0, "unsupported": 0,
                                           "request_failed": 0, "request_timeout": 0,
                                           "rate_stale": 0,
                                           "rate_invalid": 0, "request_cap": 0},
                "open_requested": len(priorities), "open_observed": 0,
                "entry_observed": 0,
                "entry_deferred_by_cap": 0,
                "entry_priority_method": CARRY_ENTRY_PRIORITY_METHOD,
                "fetched_here": fetched_here,
            },
        }

    by_symbol = {r.get("name"): r for r in rows if r.get("name")}
    # Existing episodes own the scarce observation budget: losing their paired quote
    # would manufacture a source gap and can hide a real exit.  For the remaining slots,
    # rank only by fields already validated in the current HL snapshot.  Current funding
    # is the observable right-tail upper bound before the OKX leg is fetched; OI breaks
    # ties in favour of the more measurable book.  The later paired OKX quote still gates
    # whether a row becomes a cross-venue signal, so this order is not an edge claim.
    entry_rows = sorted(
        (r for r in rows
         if r.get("oi_usd", 0) >= CARRY_MIN_OI_USD
         and r.get("funding_ann", 0) >= CARRY_MIN_ANN),
        key=lambda r: (-float(r["funding_ann"]), -float(r["oi_usd"]), str(r["name"])),
    )
    entry_symbols = list(dict.fromkeys(r["name"] for r in entry_rows))
    priority_set = set(priorities)
    requested = priorities + [symbol for symbol in entry_symbols
                              if symbol not in priority_set]
    limited = requested[:OKX_FUNDING_REQUEST_CAP]
    okx_result = okx_funding_scan(requested, cap=OKX_FUNDING_REQUEST_CAP)
    okx = okx_result["rates"]
    okx_status = okx_result["status_by_symbol"]
    okx_health = okx_result["summary"]
    limited_set = set(limited)
    # A failed/stale/invalid OKX response is unknown, not evidence that the contract is
    # absent. Only a live paired quote or a verified unsupported result may feed entry.
    signal_rows = [
        r for r in rows
        if r.get("name") in limited_set
        and okx_status.get(r.get("name")) in {"observed", "unsupported"}
    ]
    signals = carry_signals(signal_rows, okx_rates=okx)

    def paired_observation(symbol: str) -> dict | None:
        row = by_symbol.get(symbol)
        if row is None or okx_status.get(symbol) != "observed":
            return None
        observed_edge = float(row["funding_ann"]) - float(okx[symbol])
        return {
            "symbol": symbol, "status": "observed", "cross": True,
            "observation_version": 1, "observed_at": scan_at,
            "hl_ann": float(row["funding_ann"]), "okx_ann": float(okx[symbol]),
            "paired_funding_diff_ann_pct": observed_edge,
            "current_partial_model_proxy_ann_pct":
                _carry_partial_model_proxy_ann(observed_edge),
        }

    open_observations = []
    statuses = []
    for symbol in priorities:
        row = by_symbol.get(symbol)
        if row is None:
            status = "hl_symbol_unavailable"
        elif okx_status.get(symbol) != "observed":
            status = f"okx_{okx_status.get(symbol) or 'request_failed'}"
        else:
            open_observations.append(paired_observation(symbol))
            status = "observed"
        statuses.append({"symbol": symbol, "role": "open", "status": status})

    entry_observations = []
    for signal in signals:
        if not signal.get("cross") or not signal.get("paper_measurement_eligible"):
            continue
        observation = paired_observation(signal["symbol"])
        if observation is not None:
            entry_observations.append(observation)
    paper_observations_by_symbol = {
        observation["symbol"]: observation
        for observation in open_observations + entry_observations
    }
    paper_observations = list(paper_observations_by_symbol.values())
    observed_n = len(open_observations)
    if hl_health.get("state") == "unavailable" or okx_health["state"] == "unavailable":
        state = "unavailable"
    elif (hl_health.get("state") == "partial" or okx_health["state"] == "partial"
          or observed_n != len(priorities)):
        state = "partial"
    else:
        state = "ok"
    return {
        "signals": signals, "open_observations": open_observations,
        "entry_observations": entry_observations,
        "paper_observations": paper_observations, "open_status": statuses,
        "source_health": {
            "schema_version": 1, "state": state, "scan_at": scan_at,
            "hl": hl_health, "okx": okx_health,
            "open_requested": len(priorities), "open_observed": observed_n,
            "entry_observed": len(entry_observations),
            "entry_deferred_by_cap": sum(s not in limited_set for s in entry_symbols),
            "entry_priority_method": CARRY_ENTRY_PRIORITY_METHOD,
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
