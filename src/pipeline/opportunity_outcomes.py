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
HORIZON_PRIORITY = {"24h": 0, "1h": 1, "7d": 2}
MIN_N = 20
MAX_PRICE_LOOKUPS = 20       # hard network/resource budget per hourly resolver run
UNRESOLVABLE_DAYS = 21       # 7d horizon plus a generous historical-data grace period
SUPPORTED_LANES = {"launch", "cascade"}
LAUNCH_V3_COST_METHOD = "constant_product_roundtrip_plus_0.60pct_buffer_v1"

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
        return _price_at(row.get("token"), row.get("chain"), when,
                         raise_rate_limit=True)
    if row.get("lane") == "cascade":
        return _hyperliquid_price_at(row, when)
    return None


def _settled(entry: float, price: float, direction: str, cost_pct: float) -> dict:
    raw = (price / entry - 1.0) * 100
    gross = -raw if direction == "short" else raw
    net = gross - cost_pct
    return {"price": price, "gross_return_pct": round(gross, 4),
            "net_return_pct_est": round(net, 4), "positive_after_cost": net > 0}


def _next_due_task(row: dict, now: datetime) -> dict | None:
    """Choose one due horizon for an event, prioritizing the 24h evidence gate."""
    try:
        t0 = _aware(row["detected_at"])
    except (TypeError, ValueError, KeyError):
        return None
    outcome = row.get("outcome") or {}
    horizons = outcome.get("horizons") or {}
    unavailable = set(outcome.get("unavailable_horizons") or [])
    attempted_at = outcome.get("attempted_at") or {}
    candidates = []
    for name, hours in HORIZONS:
        due_at = t0 + timedelta(hours=hours)
        if now < due_at or name in horizons or name in unavailable:
            continue
        candidates.append({
            "row": row, "name": name, "hours": hours, "due_at": due_at,
            "key": (HORIZON_PRIORITY[name], attempted_at.get(name) or "",
                    due_at.isoformat()),
        })
    return min(candidates, key=lambda task: task["key"], default=None)


def _select_due_tasks(rows: list[dict], now: datetime, budget: int) -> list[dict]:
    """Reserve one slot per live lane, then fill the remaining oldest priorities."""
    tasks = []
    for row in rows:
        if row.get("lane") not in SUPPORTED_LANES or not _direction(row):
            continue
        try:
            if float(row.get("entry_price") or 0) <= 0:
                continue
        except (TypeError, ValueError):
            continue
        task = _next_due_task(row, now)
        if task:
            tasks.append(task)
    budget = max(0, budget)
    if not tasks or budget == 0:
        return []
    lane_heads = []
    for lane in sorted({task["row"]["lane"] for task in tasks}):
        lane_heads.append(min((task for task in tasks if task["row"]["lane"] == lane),
                              key=lambda task: task["key"]))
    selected = sorted(lane_heads, key=lambda task: task["key"])[:budget]
    selected_ids = {task["row"]["id"] for task in selected}
    for task in sorted(tasks, key=lambda item: item["key"]):
        if len(selected) >= budget:
            break
        if task["row"]["id"] in selected_ids:
            continue
        selected.append(task)
        selected_ids.add(task["row"]["id"])
    return selected


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
    tasks = _select_due_tasks(rows, now, max_lookups)
    lookups = settled = retired = 0
    source_backoff = None
    by_lane: dict[str, int] = {}
    by_horizon: dict[str, int] = {}
    for task in tasks:
        row, name, hours = task["row"], task["name"], task["hours"]
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
        lookups += 1
        by_lane[row["lane"]] = by_lane.get(row["lane"], 0) + 1
        by_horizon[name] = by_horizon.get(name, 0) + 1
        outcome["last_attempt_at"] = now.isoformat()
        attempted_at = dict(outcome.get("attempted_at") or {})
        attempted_at[name] = now.isoformat()
        outcome["attempted_at"] = attempted_at
        attempts = dict(outcome.get("attempts") or {})
        attempts[name] = int(attempts.get(name) or 0) + 1
        outcome["attempts"] = attempts
        try:
            price = price_at(row, t0 + timedelta(hours=hours))
        except Exception as exc:
            from src.pipeline.evidence import OhlcvRateLimited
            if not isinstance(exc, OhlcvRateLimited):
                raise
            source_backoff = str(exc)[:120]
            outcome["price_source_backoff"] = {
                "at": now.isoformat(), "horizon": name, "reason": source_backoff}
            outcome["updated_at"] = now.isoformat()
            opportunity_ledger.save_outcome(row["id"], outcome, "open")
            break
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
              "lookups_by_lane": by_lane, "lookups_by_horizon": by_horizon,
              "source_backoff": source_backoff,
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
        contract = row.get("cost_contract") or {}
        if (row.get("decision") != decision or row.get("cohort_version") != 3
                or row.get("cost_contract_version") != 1
                or contract.get("purpose") != "discovery_outcome"
                or contract.get("method") != LAUNCH_V3_COST_METHOD):
            continue
        point = ((row.get("outcome") or {}).get("horizons") or {}).get("24h")
        if point and point.get("net_return_pct_est") is not None:
            vals.append(float(point["net_return_pct_est"]))
    positives = sum(v > 0 for v in vals)
    return {"n": len(vals), "positives": positives,
            "positive_rate": positives / len(vals) if vals else None,
            "median_net_24h": statistics.median(vals) if vals else None}


def _carry_stats(rows: list[dict]) -> dict:
    """Describe valid v3 quote proxies while keeping real-edge evidence at zero."""
    from src.pipeline.evidence import wilson
    from src.pipeline.carry_paper import proxy_exclusion_reasons

    closed = []
    total_closed = 0
    excluded: dict[str, int] = {}
    for row in rows:
        outcome = row.get("outcome") or {}
        if outcome.get("kind") != "delta_neutral_carry_paper":
            continue
        if row.get("outcome_state") != "resolved":
            continue
        total_closed += 1
        reasons = proxy_exclusion_reasons(outcome)
        if reasons:
            for reason in reasons:
                excluded[reason] = excluded.get(reason, 0) + 1
            continue
        value = outcome.get("net_proxy_after_book_quotes_and_modeled_fee_pct")
        closed.append(float(value))
    n = len(closed)
    positives = sum(value > 0 for value in closed)
    common = {
        "n": n, "n_proxy": n, "hits": positives, "real_edge_n": 0,
        "total_closed": total_closed,
        "excluded_closed": total_closed - n,
        "excluded_by_reason": excluded,
        "pending": sum(r.get("outcome_state") == "open" for r in rows),
        "metric": "quote_rate_integral_minus_book_quotes_and_modeled_fee_proxy",
        "cohort_kind": "descriptive_quote_proxy",
        "cost_completeness": "partial", "all_in_total_pct": None,
        "cost_is_real_fill": False,
        "execution_mode": "paper_orderbook_measurement",
        "real_edge_eligible": False,
    }
    if n < MIN_N:
        return {**common, "verdict": "不可判", "edge_verdict": "不可判",
                "note": (f"有效v3报价代理关闭样本 {n}/{MIN_N};另隔离 "
                         f"{total_closed - n} 个旧版/错误退出/盘口不全样本。"
                         "真实优势样本仍为0")}
    lo, hi = wilson(positives, n)
    return {
        **common, "verdict": "measured", "edge_verdict": "不可判",
        "positive_rate": round(positives / n, 3),
        "lo": round(lo, 3), "hi": round(hi, 3),
        "mean_net_proxy_pct": round(statistics.mean(closed), 4),
        "median_net_proxy_pct": round(statistics.median(closed), 4),
        "worst_net_proxy_pct": round(min(closed), 4),
        "note": ("已有描述性报价代理分布；仍缺实际资金费结算、basis、完整成本、"
                 "真实双腿成交与样本外验证，不能据此判定正EV"),
    }


def _airdrop_stats(rows: list[dict]) -> dict:
    """Sum verified claims while refusing a success-only hit rate."""
    claims = []
    for row in rows:
        outcome = row.get("outcome") or {}
        if (row.get("outcome_state") == "resolved"
                and outcome.get("kind") == "airdrop_claim"
                and outcome.get("reward_is_claimed") is True
                and outcome.get("cost_is_actual") is True):
            claims.append(outcome)
    common = {
        "n_events": len(rows), "n_claimed": len(claims),
        "pending": sum(r.get("outcome_state") == "open" for r in rows),
        "metric": "verified_claim_net_usd", "edge_verdict": "不可判",
    }
    if not claims:
        return {**common, "verdict": "不可判",
                "note": "尚无带交易证据、实际奖励和实际成本的领取结果"}
    nets = [float(c["net_reward_usd"]) for c in claims]
    return {
        **common, "verdict": "realized_claims",
        "gross_reward_usd": round(sum(float(c["gross_reward_usd"]) for c in claims), 2),
        "actual_cost_usd": round(sum(float(c["actual_cost_usd"]) for c in claims), 2),
        "net_reward_usd": round(sum(nets), 2),
        "median_net_reward_usd": round(statistics.median(nets), 2),
        "note": "仅汇总已核验领取;缺参与失败/资格未命中分母,不计算命中率或edge",
    }


def _horizon_24h_progress(rows: list[dict], *, now: datetime | None = None) -> dict:
    now = now or datetime.now(timezone.utc)
    progress = {"resolved_24h": 0, "not_due_24h": 0, "due_24h": 0,
                "attempted_unpriced_24h": 0, "unavailable_24h": 0,
                "oldest_due_24h_hours": None}
    overdue = []
    for row in rows:
        try:
            due_at = _aware(row["detected_at"]) + timedelta(hours=24)
        except (TypeError, ValueError, KeyError):
            continue
        outcome = row.get("outcome") or {}
        if "24h" in (outcome.get("horizons") or {}):
            progress["resolved_24h"] += 1
        elif "24h" in set(outcome.get("unavailable_horizons") or []):
            progress["unavailable_24h"] += 1
        elif now < due_at:
            progress["not_due_24h"] += 1
        else:
            progress["due_24h"] += 1
            overdue.append((now - due_at).total_seconds() / 3600)
            if int((outcome.get("attempts") or {}).get("24h") or 0) > 0:
                progress["attempted_unpriced_24h"] += 1
    if overdue:
        progress["oldest_due_24h_hours"] = round(max(overdue), 1)
    return progress


def actionability_gate(lane: str) -> dict:
    """Translate measured evidence into a fail-closed live action gate.

    The recorded cohort decision remains immutable so paper outcomes can accumulate.
    This gate only controls whether a current quote may be presented as actionable.
    A technical route is not an edge: promotion requires a cost-after 24h cohort and
    a contemporaneous WATCH control whose Wilson intervals no longer overlap.
    """
    if lane not in SUPPORTED_LANES:
        return {"state": "not_applicable", "lane": lane}
    try:
        stat = lane_stats().get(lane) or {}
    except Exception as exc:
        return {"state": "blocked", "lane": lane, "edge_verdict": "不可判",
                "reason": f"evidence read failed: {str(exc)[:80]}"}
    edge = stat.get("edge_verdict") or "不可判"
    common = {"lane": lane, "edge_verdict": edge, "minimum_n": MIN_N,
              "measured_n": int(stat.get("n") or 0),
              "cost_is_real_fill": stat.get("cost_is_real_fill", False)}
    if edge == "有edge迹象":
        return {**common, "state": "pass", "reason": stat.get("edge_note")}
    if edge == "无edge/负":
        return {**common, "state": "blocked", "reason": stat.get("edge_note")}
    if lane == "launch":
        probe, control = stat.get("probe") or {}, stat.get("control") or {}
        return {**common, "state": "collecting",
                "probe_n": int(probe.get("n") or 0),
                "control_n": int(control.get("n") or 0),
                "reason": (f"成本后24h同期对照不足: SMALL_PROBE "
                           f"{int(probe.get('n') or 0)}/{MIN_N}, WATCH "
                           f"{int(control.get('n') or 0)}/{MIN_N}")}
    return {**common, "state": "collecting",
            "reason": "Cascade 尚无同期可比 WATCH 对照；仅继续纸面测量"}


def lane_stats() -> dict:
    """Honest 24h distribution and, only where possible, a control comparison."""
    from src.pipeline.evidence import wilson

    rows = opportunity_ledger.outcome_rows()
    out: dict[str, dict] = {}
    # A configured-but-empty workbench is still a measurable state. Always expose
    # Airdrop's zero-event scorecard so the UI cannot hide missing discovery behind
    # an absent JSON key.
    for lane in sorted({r["lane"] for r in rows} | {"airdrop"}):
        lane_rows = [r for r in rows if r["lane"] == lane]
        if lane == "carry":
            out[lane] = _carry_stats(lane_rows)
            continue
        if lane == "airdrop":
            out[lane] = _airdrop_stats(lane_rows)
            continue
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
        common.update(_horizon_24h_progress(measurable))
        if lane == "launch":
            common["legacy_unfrozen_n"] = sum((r.get("cohort_version") or 0) < 2
                                                for r in measurable)
            common["legacy_v2_descriptive_n"] = sum(r.get("cohort_version") == 2
                                                     for r in measurable)
            common["v3_cost_method"] = LAUNCH_V3_COST_METHOD
            common["probe"] = _cohort(measurable, "SMALL_PROBE")
            common["control"] = _cohort(measurable, "WATCH")
        if n < MIN_N:
            out[lane] = {**common, "verdict": "不可判", "edge_verdict": "不可判",
                         "note": (f"24h样本 {n}/{MIN_N};到期待结算"
                                  f" {common['due_24h']},继续积累")}
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
            probe, watch = stat["probe"], stat["control"]
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
