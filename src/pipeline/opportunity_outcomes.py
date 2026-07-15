"""Forward outcome measurement for the five-lane opportunity ledger.

An event card is not evidence of edge. This module settles the immutable first-seen
snapshot at 1h/24h/7d, subtracts the cost model frozen at discovery, and keeps the
result in the same ledger. It deliberately separates three claims:

* ``measured``: enough outcomes exist to describe the distribution;
* ``edge_verdict``: a comparable control exists and survives uncertainty;
* real execution: never claimed here -- Launch/Cascade costs are paper estimates.

Only lanes with a directional entry can be price-settled. Structure events remain
WATCH-only until they have a separately specified hypothesis; Airdrop and Carry use
their own reward/cost and delta-neutral accounting respectively.
"""
from __future__ import annotations

import json
import statistics
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Callable

import structlog

from src.pipeline import opportunity_ledger

logger = structlog.get_logger()

HORIZONS = (("1h", 1), ("24h", 24), ("7d", 7 * 24))
MIN_N = 20
MAX_PRICE_LOOKUPS = 20       # hard network/resource budget per hourly resolver run
UNRESOLVABLE_DAYS = 21       # 7d horizon plus a generous historical-data grace period
SUPPORTED_LANES = {"launch", "cascade"}

PriceAt = Callable[[dict, datetime], float | None]


def _aware(value: str) -> datetime:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _direction(row: dict) -> str | None:
    if row.get("lane") == "launch":
        return "long"
    if row.get("lane") == "cascade":
        side = str((row.get("payload") or {}).get("side") or "").upper()
        return "short" if side == "SHORT" else "long" if side == "LONG" else None
    return None


def _cost(row: dict) -> tuple[float, str]:
    """Return the discovery-time paper cost estimate, with a legacy fallback."""
    try:
        frozen = float(row.get("cost_pct_est"))
        if frozen >= 0:
            return frozen, str(row.get("cost_model") or "discovery_snapshot")
    except (TypeError, ValueError):
        pass
    payload = row.get("payload") or {}
    # Never trust a refreshable payload as the discovery-time cost. Legacy rows have
    # NULL frozen columns; after deployment a later scan may enrich their payload with
    # today's liquidity. Treat those rows as reconstructed even if that field appears.
    if row.get("lane") == "launch":
        # Old ledger rows predate the frozen field. Reconstruct from other immutable
        # discovery facts and label it as a reconstruction, never as a real fill.
        from src.pipeline.slippage import price_impact
        try:
            liq = float(payload.get("liquidity_usd") or 0)
            size = float(row.get("max_notional_usd") or 0)
            if liq > 0 and size > 0:
                return (round(2 * price_impact(liq, size) + 0.60, 3),
                        "legacy_reconstructed_constant_product_plus_0.60pct_buffer")
        except (TypeError, ValueError):
            pass
    return (0.20, "legacy_perp_roundtrip_0.20pct_buffer")


def _hyperliquid_price_at(row: dict, when: datetime) -> float | None:
    """Hourly Hyperliquid close nearest the requested horizon (public, keyless)."""
    coin = row.get("token") or row.get("symbol")
    if not coin:
        return None
    start = int((when - timedelta(hours=2)).timestamp() * 1000)
    end = int((when + timedelta(hours=2)).timestamp() * 1000)
    body = {"type": "candleSnapshot", "req": {"coin": coin, "interval": "1h",
                                                 "startTime": start, "endTime": end}}
    req = urllib.request.Request(
        "https://api.hyperliquid.xyz/info", data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json", "User-Agent": "CryptoScope/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=12) as response:
            candles = json.loads(response.read())
        usable = [c for c in candles if c.get("c") is not None and c.get("t") is not None]
        if not usable:
            return None
        best = min(usable, key=lambda c: abs(int(c["t"]) / 1000 - when.timestamp()))
        if abs(int(best["t"]) / 1000 - when.timestamp()) > 2 * 3600:
            return None
        return float(best["c"])
    except Exception:
        return None


def _default_price_at(row: dict, when: datetime) -> float | None:
    if row.get("lane") == "launch":
        from src.pipeline.outcome_tracker import _price_at
        return _price_at(row.get("token"), row.get("chain"), when)
    if row.get("lane") == "cascade":
        return _hyperliquid_price_at(row, when)
    return None


def _settled(entry: float, price: float, direction: str, cost_pct: float) -> dict:
    raw = (price / entry - 1.0) * 100
    gross = -raw if direction == "short" else raw
    net = gross - cost_pct
    return {"price": price, "gross_return_pct": round(gross, 4),
            "net_return_pct_est": round(net, 4), "positive_after_cost": net > 0}


def resolve(*, now: datetime | None = None, price_at: PriceAt | None = None,
            max_lookups: int = MAX_PRICE_LOOKUPS) -> dict:
    """Resolve at most one missing horizon per event within a hard lookup budget.

    One-horizon-per-event makes a backlog fair and bounds file descriptors/network
    work. Failed lookups are persisted as attempts and sorted to the back next run,
    preventing one dead pool from starving every newer event.
    """
    now = now or datetime.now(timezone.utc)
    price_at = price_at or _default_price_at
    rows = opportunity_ledger.outcome_rows(open_only=True)
    rows.sort(key=lambda r: (r.get("outcome") or {}).get("last_attempt_at") or "")
    lookups = settled = retired = 0
    for row in rows:
        if lookups >= max(0, max_lookups):
            break
        if row.get("lane") not in SUPPORTED_LANES or not _direction(row):
            continue
        try:
            entry = float(row.get("entry_price") or 0)
            t0 = _aware(row["detected_at"])
        except (TypeError, ValueError, KeyError):
            continue
        if entry <= 0:
            continue
        outcome = dict(row.get("outcome") or {})
        horizons = dict(outcome.get("horizons") or {})
        unavailable = set(outcome.get("unavailable_horizons") or [])
        cost_pct, cost_model = _cost(row)
        outcome.update({"version": 1, "direction": _direction(row),
                        "cost_pct_est": cost_pct, "cost_model": cost_model,
                        "cost_is_real_fill": False, "horizons": horizons})
        missing = next(((name, hours) for name, hours in HORIZONS
                        if now >= t0 + timedelta(hours=hours)
                        and name not in horizons and name not in unavailable), None)
        if not missing:
            continue
        name, hours = missing
        lookups += 1
        outcome["last_attempt_at"] = now.isoformat()
        attempts = dict(outcome.get("attempts") or {})
        attempts[name] = int(attempts.get(name) or 0) + 1
        outcome["attempts"] = attempts
        price = price_at(row, t0 + timedelta(hours=hours))
        state = "open"
        if price is not None and price > 0:
            horizons[name] = _settled(entry, float(price), _direction(row), cost_pct)
            settled += 1
        elif (now - t0).total_seconds() >= UNRESOLVABLE_DAYS * 86400:
            # Retire this horizon, not the whole event: a missing 1h candle must not
            # prevent a valid 24h/7d outcome from entering the evidence sample.
            unavailable.add(name)
            outcome["unavailable_horizons"] = sorted(unavailable)
        complete = all(n in horizons or n in unavailable for n, _ in HORIZONS)
        if complete:
            state = "resolved" if "24h" in horizons else "unresolvable"
            if state == "unresolvable":
                outcome["unresolvable_reason"] = "24h historical price unavailable after grace"
                retired += 1
        outcome["updated_at"] = now.isoformat()
        opportunity_ledger.save_outcome(row["id"], outcome, state)
    result = {"lookups": lookups, "settled": settled, "retired": retired,
              "pending_events": sum(1 for r in rows if r.get("lane") in SUPPORTED_LANES)}
    logger.info("opportunity_outcomes_resolved", **result)
    return result


def _percentile(values: list[float], p: float) -> float | None:
    if not values:
        return None
    xs = sorted(values)
    pos = (len(xs) - 1) * p
    lo, hi = int(pos), min(int(pos) + 1, len(xs) - 1)
    return xs[lo] + (xs[hi] - xs[lo]) * (pos - lo)


def _cohort(rows: list[dict], decision: str) -> dict:
    vals = []
    for row in rows:
        if row.get("decision") != decision or (row.get("cohort_version") or 0) < 2:
            continue
        point = ((row.get("outcome") or {}).get("horizons") or {}).get("24h")
        if point and point.get("net_return_pct_est") is not None:
            vals.append(float(point["net_return_pct_est"]))
    positives = sum(v > 0 for v in vals)
    return {"n": len(vals), "positives": positives,
            "positive_rate": positives / len(vals) if vals else None,
            "median_net_24h": statistics.median(vals) if vals else None}


def lane_stats() -> dict:
    """Honest 24h distribution and, only where possible, a control comparison."""
    from src.pipeline.evidence import wilson

    rows = opportunity_ledger.outcome_rows()
    out: dict[str, dict] = {}
    for lane in sorted({r["lane"] for r in rows}):
        lane_rows = [r for r in rows if r["lane"] == lane]
        measurable = [r for r in lane_rows if _direction(r) and r.get("entry_price")]
        if not measurable:
            note = ("公开结构事件没有方向假设,不把事后涨跌伪装成命中率" if lane == "structure"
                    else "本线使用非价格结果账本,不套用方向命中率")
            out[lane] = {"verdict": "not_directional", "n_events": len(lane_rows),
                         "pending": 0, "note": note}
            continue
        points = []
        for row in measurable:
            point = ((row.get("outcome") or {}).get("horizons") or {}).get("24h")
            if point and point.get("net_return_pct_est") is not None:
                points.append(float(point["net_return_pct_est"]))
        n, positives = len(points), sum(v > 0 for v in points)
        pending = sum(r.get("outcome_state") == "open" for r in measurable)
        unresolvable = sum(r.get("outcome_state") == "unresolvable" for r in measurable)
        common = {"n": n, "hits": positives, "pending": pending,
                  "unresolvable": unresolvable, "metric": "positive_after_estimated_cost",
                  "cost_is_real_fill": False}
        if lane == "launch":
            common["legacy_unfrozen_n"] = sum((r.get("cohort_version") or 0) < 2
                                                for r in measurable)
        if n < MIN_N:
            out[lane] = {**common, "verdict": "不可判",
                         "edge_verdict": "不可判", "note": f"24h样本 {n}/{MIN_N},继续积累"}
            continue
        rate = positives / n
        lo, hi = wilson(positives, n)
        stat = {**common, "verdict": "measured", "rate": round(rate, 3),
                "lo": round(lo, 3), "hi": round(hi, 3),
                "median_net_24h": round(statistics.median(points), 3),
                "p90_net_24h": round(_percentile(points, 0.90) or 0, 3),
                "p99_net_24h": round(_percentile(points, 0.99) or 0, 3),
                "max_net_24h": round(max(points), 3),
                "edge_verdict": "不可判", "edge_note": "缺少同期可比对照"}
        if lane == "launch":
            probe, watch = _cohort(measurable, "SMALL_PROBE"), _cohort(measurable, "WATCH")
            stat["probe"] = probe
            stat["control"] = watch
            if probe["n"] >= MIN_N and watch["n"] >= MIN_N:
                plo, phi = wilson(probe["positives"], probe["n"])
                wlo, whi = wilson(watch["positives"], watch["n"])
                probe["lo"], probe["hi"] = round(plo, 3), round(phi, 3)
                watch["lo"], watch["hi"] = round(wlo, 3), round(whi, 3)
                if plo > whi and probe["median_net_24h"] > watch["median_net_24h"]:
                    stat["edge_verdict"] = "有edge迹象"
                    stat["edge_note"] = "SMALL_PROBE成本后正收益率区间高于WATCH对照"
                elif (probe["positive_rate"] <= watch["positive_rate"] and
                      probe["median_net_24h"] <= watch["median_net_24h"]):
                    stat["edge_verdict"] = "无edge/负"
                    stat["edge_note"] = "执行门未优于同期WATCH对照"
                else:
                    stat["edge_note"] = "试验组与WATCH对照区间仍重叠"
            else:
                stat["edge_note"] = (f"同期对照不足: SMALL_PROBE {probe['n']}/{MIN_N},"
                                     f"WATCH {watch['n']}/{MIN_N}")
        out[lane] = stat
    return out


if __name__ == "__main__":
    from dotenv import load_dotenv
    from src.config import PROJECT_ROOT

    load_dotenv(PROJECT_ROOT / ".env")
    print(json.dumps({"resolve": resolve(), "lanes": lane_stats()}, ensure_ascii=False, indent=1))
