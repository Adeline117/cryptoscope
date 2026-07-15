"""Pre-registered forward validation for Launch selection quality.

This module is deliberately stricter than a normal dashboard statistic.  It is the
only path from paper outcomes to an evidence-gate pass, and it fails closed when the
protocol, outcome coverage, calendar overlap, or validator dependency is incomplete.

The primary endpoint is frozen before the cohort begins: cost-adjusted 24-hour log
growth utility for ``SMALL_PROBE`` versus contemporaneous ``WATCH`` candidates.  The
comparison is aggregated by UTC day and tested with ``arch.bootstrap.SPA`` using a
stationary bootstrap.  SPA is a Reality-Check family test intended to resist model
selection/data-snooping.  A Bonferroni budget across the six pre-declared sample looks
also prevents the board's repeated refreshes from turning optional stopping into an
edge claim.

Passing this protocol means "forward paper edge evidence worth a manual experiment".
It is never real-fill or live-PnL evidence and never enables automatic execution.
"""
from __future__ import annotations

import math
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

import numpy as np


PROTOCOL_ID = "launch-forward-spa-v1"
COHORT_VERSION = 4
PROTOCOL_START_AT = "2026-07-15T19:00:00+00:00"
PRIMARY_HORIZON = "24h"
PRIMARY_METRIC = "daily_mean_cost_adjusted_log_growth_utility"
LOOK_SIZES = (100, 200, 400, 800, 1_600, 3_200)
FAMILY_ALPHA = 0.05
LOOK_ALPHA = FAMILY_ALPHA / len(LOOK_SIZES)
MIN_OUTCOME_COVERAGE = 0.95
MAX_COVERAGE_GAP = 0.05
MIN_SHARED_DAYS = 14
MIN_SHARED_EVENT_FRACTION = 0.80
MIN_MEAN_UTILITY_LIFT = 0.02
SPA_REPS = 10_000
SPA_BLOCK_SIZE_DAYS = 3
SPA_SEED = 20_260_715
LAUNCH_COST_METHOD = "constant_product_roundtrip_plus_0.60pct_buffer_v1"
ARMS = ("SMALL_PROBE", "WATCH")


def _aware(value: Any) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def is_protocol_event(row: dict) -> bool:
    """Return whether discovery was made under the frozen forward protocol."""
    detected = _aware(row.get("detected_at"))
    contract = row.get("cost_contract") or {}
    return bool(
        row.get("lane") == "launch"
        and row.get("decision") in ARMS
        and row.get("cohort_version") == COHORT_VERSION
        and row.get("cost_contract_version") == 1
        and contract.get("purpose") == "discovery_outcome"
        and contract.get("method") == LAUNCH_COST_METHOD
        and detected is not None
        and detected >= datetime.fromisoformat(PROTOCOL_START_AT)
    )


def _point_state(row: dict) -> tuple[str, float | None]:
    outcome = row.get("outcome") or {}
    point = (outcome.get("horizons") or {}).get(PRIMARY_HORIZON)
    if point is not None:
        try:
            value = float(point.get("net_return_pct_est"))
        except (AttributeError, TypeError, ValueError):
            return "invalid", None
        return ("resolved", value) if math.isfinite(value) else ("invalid", None)
    if PRIMARY_HORIZON in set(outcome.get("unavailable_horizons") or []):
        return "unavailable", None
    return "pending", None


def _utility(net_return_pct: float) -> float:
    # A 1% residual value floor keeps malformed/sub-minus-100 paper returns finite.
    # Log utility reduces one moonshot's power to manufacture a pass while preserving
    # the right-tail ordering that the product is trying to discover.
    return math.log1p(max(-99.0, net_return_pct) / 100.0)


def _arm_summary(rows: list[dict]) -> dict:
    states = defaultdict(int)
    values = []
    for row in rows:
        state, value = _point_state(row)
        states[state] += 1
        if state == "resolved" and value is not None:
            values.append(value)
    n = len(rows)
    return {
        "eligible_n": n,
        "resolved_n": states["resolved"],
        "pending_n": states["pending"],
        "unavailable_n": states["unavailable"],
        "invalid_n": states["invalid"],
        "coverage": states["resolved"] / n if n else 0.0,
        "positive_rate": (sum(value > 0 for value in values) / len(values)
                          if values else None),
        "mean_net_24h": float(np.mean(values)) if values else None,
        "median_net_24h": float(np.median(values)) if values else None,
    }


def _daily_utility(rows: list[dict]) -> tuple[dict[str, float], int]:
    by_day: dict[str, list[float]] = defaultdict(list)
    resolved = 0
    for row in rows:
        state, value = _point_state(row)
        detected = _aware(row.get("detected_at"))
        if state != "resolved" or value is None or detected is None:
            continue
        resolved += 1
        by_day[detected.date().isoformat()].append(_utility(value))
    return ({day: float(np.mean(values)) for day, values in by_day.items()}, resolved)


def _base_result(eligible: dict[str, list[dict]]) -> dict:
    counts = {arm: len(rows) for arm, rows in eligible.items()}
    next_look = next((size for size in LOOK_SIZES
                      if min(counts.values(), default=0) < size), None)
    return {
        "protocol_id": PROTOCOL_ID,
        "protocol_start_at": PROTOCOL_START_AT,
        "cohort_version": COHORT_VERSION,
        "primary_horizon": PRIMARY_HORIZON,
        "primary_metric": PRIMARY_METRIC,
        "planned_looks": list(LOOK_SIZES),
        "family_alpha": FAMILY_ALPHA,
        "look_alpha": round(LOOK_ALPHA, 8),
        "spa_reps": SPA_REPS,
        "spa_block_size_days": SPA_BLOCK_SIZE_DAYS,
        "spa_seed": SPA_SEED,
        "cost_is_real_fill": False,
        "real_edge_eligible": False,
        "eligible_n": counts,
        "next_look_n_per_arm": next_look,
        "edge_verdict": "不可判",
    }


def launch_forward_validation(rows: list[dict]) -> dict:
    """Run the current pre-registered look, or explain why no look is valid yet."""
    eligible = {arm: [] for arm in ARMS}
    for row in rows:
        if is_protocol_event(row):
            eligible[row["decision"]].append(row)
    for arm in ARMS:
        eligible[arm].sort(key=lambda row: (row.get("detected_at") or "", row.get("id") or ""))
    result = _base_result(eligible)
    min_n = min((len(eligible[arm]) for arm in ARMS), default=0)
    look_n = max((size for size in LOOK_SIZES if size <= min_n), default=None)
    if look_n is None:
        result.update({
            "state": "collecting",
            "reason": (f"前向协议每组至少需要 {LOOK_SIZES[0]} 个候选；当前 "
                       f"SMALL_PROBE {len(eligible['SMALL_PROBE'])}, "
                       f"WATCH {len(eligible['WATCH'])}"),
        })
        return result

    prefix = {arm: eligible[arm][:look_n] for arm in ARMS}
    summary = {arm: _arm_summary(prefix[arm]) for arm in ARMS}
    result.update({"look_n_per_arm": look_n, "arms": summary})
    pending = sum(summary[arm]["pending_n"] for arm in ARMS)
    if pending:
        result.update({
            "state": "awaiting_outcomes",
            "reason": f"第 {look_n} 次固定前缀仍有 {pending} 个 24h 结果待结算",
        })
        return result

    probe_coverage = summary["SMALL_PROBE"]["coverage"]
    watch_coverage = summary["WATCH"]["coverage"]
    if (min(probe_coverage, watch_coverage) < MIN_OUTCOME_COVERAGE
            or abs(probe_coverage - watch_coverage) > MAX_COVERAGE_GAP):
        result.update({
            "state": "coverage_blocked",
            "reason": ("固定前缀结果覆盖不足或组间缺失差异过大: "
                       f"SMALL_PROBE {probe_coverage:.1%}, WATCH {watch_coverage:.1%}"),
        })
        return result

    daily = {}
    resolved = {}
    for arm in ARMS:
        daily[arm], resolved[arm] = _daily_utility(prefix[arm])
    shared_days = sorted(set(daily["SMALL_PROBE"]) & set(daily["WATCH"]))
    shared_events = {
        arm: sum(1 for row in prefix[arm]
                 if _point_state(row)[0] == "resolved"
                 and _aware(row.get("detected_at")).date().isoformat() in shared_days)
        for arm in ARMS
    }
    shared_fraction = {
        arm: shared_events[arm] / resolved[arm] if resolved[arm] else 0.0
        for arm in ARMS
    }
    result.update({"shared_days": len(shared_days),
                   "shared_event_fraction": shared_fraction})
    if (len(shared_days) < MIN_SHARED_DAYS
            or min(shared_fraction.values()) < MIN_SHARED_EVENT_FRACTION):
        result.update({
            "state": "regime_overlap_blocked",
            "reason": (f"同期 UTC 日覆盖不足: {len(shared_days)}/{MIN_SHARED_DAYS} 天；"
                       f"共享日事件占比 SMALL_PROBE {shared_fraction['SMALL_PROBE']:.1%}, "
                       f"WATCH {shared_fraction['WATCH']:.1%}"),
        })
        return result

    probe = np.asarray([daily["SMALL_PROBE"][day] for day in shared_days])
    watch = np.asarray([daily["WATCH"][day] for day in shared_days])
    mean_lift = float(np.mean(probe - watch))
    result["mean_daily_log_utility_lift"] = round(mean_lift, 8)
    try:
        from arch.bootstrap import SPA

        benchmark_losses = -watch
        model_losses = (-probe)[:, None]
        spa = SPA(
            benchmark_losses,
            model_losses,
            block_size=SPA_BLOCK_SIZE_DAYS,
            reps=SPA_REPS,
            bootstrap="stationary",
            studentize=True,
            seed=SPA_SEED,
        )
        spa.compute()
        pvalues = {name: float(value) for name, value in spa.pvalues.items()}
    except Exception as exc:
        result.update({
            "state": "validator_unavailable",
            "reason": f"SPA validator failed closed: {type(exc).__name__}: {str(exc)[:100]}",
        })
        return result

    result.update({
        "spa_pvalues": {name: round(value, 8) for name, value in pvalues.items()},
        "spa_pvalue_used": "upper",
        "tested_model_count": 1,
    })
    p_upper = pvalues["upper"]
    if p_upper <= LOOK_ALPHA and mean_lift >= MIN_MEAN_UTILITY_LIFT:
        result.update({
            "state": "pass",
            "edge_verdict": "有前向纸面edge迹象",
            "reason": (f"预注册 {look_n}/组 look 通过: SPA upper p={p_upper:.5f} "
                       f"≤ {LOOK_ALPHA:.5f}, 日均log效用差 {mean_lift:.4f}"),
        })
    elif mean_lift <= 0:
        result.update({
            "state": "no_edge_observed",
            "edge_verdict": "无edge/负",
            "reason": f"固定 look 的日均log效用差 {mean_lift:.4f} 未优于 WATCH",
        })
    else:
        result.update({
            "state": "inconclusive",
            "reason": (f"效应为正但未越过预注册门槛: SPA upper p={p_upper:.5f}, "
                       f"日均log效用差 {mean_lift:.4f}"),
        })
    return result
