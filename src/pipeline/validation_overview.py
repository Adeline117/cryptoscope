"""Small, fail-closed five-lane validation projection for the public board.

Input is one ``stats.lanes`` snapshot.  The projection never reads the faster perps
lifecycle view and never promotes Launch's quarantined legacy distribution.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from typing import Any


VERSION = 1
LANE_ORDER = ("launch", "cascade", "carry", "airdrop", "structure")
MIN_N = 20


class _Invalid(ValueError):
    pass


def _obj(value: Any, path: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise _Invalid(f"{path}_missing_or_not_object")
    return value


def _str(value: Any, path: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise _Invalid(f"{path}_missing_or_invalid")
    return value.strip()


def _uint(value: Any, path: str, *, positive: bool = False) -> int:
    if (isinstance(value, bool) or not isinstance(value, int) or value < 0
            or positive and value == 0):
        raise _Invalid(f"{path}_invalid_integer")
    return value


def _num(value: Any, path: str, *, optional: bool = False,
         probability: bool = False) -> float | None:
    if value is None and optional:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise _Invalid(f"{path}_invalid_number")
    number = float(value)
    if not math.isfinite(number) or probability and not 0 <= number <= 1:
        raise _Invalid(f"{path}_invalid_number")
    return number


def _safe_claims(value: Any, path: str) -> None:
    """A projection can fail closed but can never echo an execution claim."""
    if isinstance(value, Mapping):
        for key, item in value.items():
            child = f"{path}.{key}"
            if key in {
                "auto_execution_allowed", "execution_edge_eligible",
                "real_edge_eligible", "edge_eligible", "cost_is_real_fill",
                "is_real_fill",
            } and item is not False:
                raise _Invalid(f"{child}_must_be_false")
            if key == "real_edge_n" and (
                    isinstance(item, bool) or not isinstance(item, int) or item != 0):
                raise _Invalid(f"{child}_must_be_zero")
            _safe_claims(item, child)
    elif isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            _safe_claims(item, f"{path}[{index}]")


def _sample(valid: int | None, target: int | None, unit: str,
            arms: dict | None = None) -> dict:
    return {"valid_n": valid, "target_n": target, "target_unit": unit,
            "launch_arms": arms}


def _result(kind: str, summary: str, primary: float | None = None,
            comparison: float | None = None, unit: str = "not_applicable") -> dict:
    return {"evidence_type": kind, "summary": summary,
            "primary_value": primary, "comparison_value": comparison,
            "unit": unit, "is_real_fill": False}


def _uncertainty(kind: str, state: str, summary: str, *,
                 value: float | None = None, lower: float | None = None,
                 upper: float | None = None,
                 threshold: float | None = None) -> dict:
    return {"kind": kind, "state": state, "summary": summary, "value": value,
            "lower": lower, "upper": upper, "threshold": threshold}


def _row(lane: str, verdict: str, sample: dict, result: dict,
         benchmark_kind: str, benchmark_summary: str, uncertainty: dict,
         reason: str | None) -> dict:
    return {"lane": lane, "verdict": verdict, "sample": sample,
            "result": result,
            "benchmark": {"kind": benchmark_kind, "summary": benchmark_summary},
            "uncertainty": uncertainty, "blocking_reason": reason,
            "real_edge_n": 0, "execution_edge_eligible": False,
            "auto_execution_allowed": False}


def _invalid_row(lane: str, reason: str) -> dict:
    return _row(
        lane, "unverifiable", _sample(None, None, "unverifiable"),
        _result("unverifiable", "验证输入缺失或合同无效，未发布结果"),
        "unverifiable", "基准合同不可验证",
        _uncertainty("unverifiable", "unavailable", "不确定性合同不可验证"),
        reason,
    )


def _launch(value: Any) -> dict:
    lane = _obj(value, "launch")
    _safe_claims(lane, "launch")
    if (lane.get("metric") !=
            "append_only_exact_pool_24h_positive_after_frozen_full_paper_cost"
            or lane.get("sample_kind") != "forward_paper_selector"):
        raise _Invalid("launch_evidence_identity_invalid")
    probe, control = _obj(lane.get("probe"), "launch.probe"), _obj(
        lane.get("control"), "launch.control")
    pe, ce = _uint(probe.get("eligible_n"), "launch.probe.eligible_n"), _uint(
        control.get("eligible_n"), "launch.control.eligible_n")
    pr, cr = _uint(probe.get("resolved_n"), "launch.probe.resolved_n"), _uint(
        control.get("resolved_n"), "launch.control.resolved_n")
    if pr > pe or cr > ce or _uint(lane.get("n"), "launch.n") != pr + cr:
        raise _Invalid("launch_arm_counts_invalid")
    # Validate but never publish interim medians before a frozen look.
    _num(probe.get("median_net_24h"), "launch.probe.median", optional=True)
    _num(control.get("median_net_24h"), "launch.control.median", optional=True)

    ev = _obj(lane.get("edge_validation"), "launch.edge_validation")
    eligible = _obj(ev.get("eligible_n"), "launch.edge_validation.eligible_n")
    if (_uint(eligible.get("SMALL_PROBE"), "launch.ev.probe") != pe
            or _uint(eligible.get("WATCH"), "launch.ev.control") != ce):
        raise _Invalid("launch_eligible_counts_drifted")
    state, reason = _str(ev.get("state"), "launch.ev.state"), _str(
        ev.get("reason"), "launch.ev.reason")
    allowed = {"protocol_integrity_blocked", "collecting", "awaiting_outcomes",
               "coverage_blocked", "regime_overlap_blocked", "invalid_evidence",
               "no_edge_observed", "validator_unavailable", "pass", "inconclusive"}
    if state not in allowed:
        raise _Invalid("launch_ev_state_invalid")
    from src.pipeline.edge_validation import FAMILY_ALPHA, LOOK_ALPHA, LOOK_SIZES

    if (ev.get("planned_looks") != list(LOOK_SIZES)
            or _num(ev.get("family_alpha"), "launch.ev.family_alpha",
                    probability=True) != FAMILY_ALPHA
            or _num(ev.get("look_alpha"), "launch.ev.look_alpha",
                    probability=True) != round(LOOK_ALPHA, 8)):
        raise _Invalid("launch_sequential_test_policy_drifted")
    expected_next = next(
        (size for size in LOOK_SIZES if min(pe, ce) < size), None,
    )
    raw_next = ev.get("next_look_n_per_arm")
    if expected_next is None:
        if raw_next is not None:
            raise _Invalid("launch_next_look_drifted")
        target = _uint(
            ev.get("look_n_per_arm"), "launch.ev.look_n", positive=True,
        )
        if target not in LOOK_SIZES:
            raise _Invalid("launch_look_not_preregistered")
    else:
        if _uint(raw_next, "launch.ev.next_look", positive=True) != expected_next:
            raise _Invalid("launch_next_look_drifted")
        target = expected_next
    alpha = round(LOOK_ALPHA, 8)

    primary = comparison = None
    if state in {"pass", "no_edge_observed", "inconclusive"}:
        arms = _obj(ev.get("arms"), "launch.ev.arms")
        primary = _num(_obj(arms.get("SMALL_PROBE"), "launch.ev.arms.probe").get(
            "median_net_24h"), "launch.ev.arms.probe.median", optional=True)
        comparison = _num(_obj(arms.get("WATCH"), "launch.ev.arms.control").get(
            "median_net_24h"), "launch.ev.arms.control.median", optional=True)
    summary = (f"固定 look：SMALL_PROBE 中位 {primary:.4f}% · "
               f"WATCH 中位 {comparison:.4f}%"
               if primary is not None and comparison is not None
               else "固定 look 的冻结成本后 24h 结果尚不可判")

    if ev.get("spa_pvalues") is not None:
        if ev.get("spa_pvalue_used") != "upper":
            raise _Invalid("launch_spa_name_invalid")
        p = _num(_obj(ev["spa_pvalues"], "launch.ev.spa").get("upper"),
                 "launch.ev.spa.upper", probability=True)
        uncertainty = _uncertainty(
            "sequential_spa_upper_p", "available",
            f"sequential SPA upper p={p:.8f}；预注册门槛 {alpha:.8f}",
            value=p, threshold=alpha)
    else:
        if state == "pass":
            raise _Invalid("launch_pass_missing_spa")
        pending = state in {"protocol_integrity_blocked", "collecting",
                            "awaiting_outcomes"}
        uncertainty = _uncertainty(
            "sequential_spa_upper_p", "not_due" if pending else "unavailable",
            "固定 look 尚未到，未运行 sequential SPA" if pending
            else "sequential SPA 当前不可用或证据未通过", threshold=alpha)
    verdict = {"pass": "paper_signal", "no_edge_observed": "no_edge_observed",
               "inconclusive": "inconclusive", "collecting": "collecting",
               "awaiting_outcomes": "collecting"}.get(state, "blocked")
    return _row(
        "launch", verdict,
        _sample(min(pr, cr), target, "per_arm", {
            "probe": {"eligible_n": pe, "resolved_n": pr},
            "control": {"eligible_n": ce, "resolved_n": cr}}),
        _result("append_only_frozen_cost_24h_paper_selector", summary,
                primary, comparison, "pct"),
        "contemporaneous_watch",
        "同期 WATCH 对照；两臂使用同一 entry UTC 日历和冻结成本规则",
        uncertainty, None if state == "pass" else reason)


def _wilson(lane: Mapping[str, Any], rate_key: str, path: str, *,
            hits: int, n: int) -> dict:
    rate = _num(lane.get(rate_key), f"{path}.{rate_key}", probability=True)
    lo = _num(lane.get("lo"), f"{path}.lo", probability=True)
    hi = _num(lane.get("hi"), f"{path}.hi", probability=True)
    from src.pipeline.evidence import wilson

    expected_lo, expected_hi = wilson(hits, n)
    expected = (
        round(hits / n, 3), round(expected_lo, 3), round(expected_hi, 3),
    )
    if (rate, lo, hi) != expected:
        raise _Invalid(f"{path}_positive_rate_or_wilson_drifted")
    return _uncertainty(
        "wilson_95_positive_rate", "available",
        f"成本后为正率 {rate:.1%}；Wilson 95% CI [{lo:.1%}, {hi:.1%}]",
        value=rate, lower=lo, upper=hi)


def _descriptive(value: Any, *, lane_name: str, metric: str,
                 rate_key: str, value_key: str, comparison_key: str | None,
                 evidence_type: str, result_label: str,
                 target_unit: str, benchmark_kind: str,
                 benchmark_summary: str) -> dict:
    lane = _obj(value, lane_name)
    _safe_claims(lane, lane_name)
    if (lane.get("metric") != metric
            or lane.get("edge_verdict") != "不可判"
            or lane.get("cost_is_real_fill") is not False):
        raise _Invalid(f"{lane_name}_evidence_identity_invalid")
    n, hits = _uint(lane.get("n"), f"{lane_name}.n"), _uint(
        lane.get("hits"), f"{lane_name}.hits")
    if hits > n:
        raise _Invalid(f"{lane_name}_hits_invalid")
    if lane_name == "carry" and _uint(lane.get("n_proxy"), "carry.n_proxy") != n:
        raise _Invalid("carry_n_proxy_invalid")
    expected = "measured" if n >= MIN_N else "不可判"
    if lane.get("verdict") != expected:
        raise _Invalid(f"{lane_name}_verdict_invalid")
    reason = _str(lane.get("edge_note") or lane.get("note"), f"{lane_name}.note")
    primary = _num(lane.get(value_key), f"{lane_name}.{value_key}", optional=True)
    comparison = (_num(lane.get(comparison_key), f"{lane_name}.{comparison_key}",
                       optional=True) if comparison_key else None)
    if expected == "measured":
        if primary is None or comparison_key and comparison is None:
            raise _Invalid(f"{lane_name}_measured_result_missing")
        uncertainty = _wilson(
            lane, rate_key, lane_name, hits=hits, n=n,
        )
        summary = (f"{result_label} {primary:.4f}%"
                   + (f" · 中位 {comparison:.4f}%" if comparison is not None else "")
                   + "（非实盘成交）")
        verdict = "descriptive_only"
    else:
        uncertainty = _uncertainty(
            "wilson_95_positive_rate", "not_due",
            "有效样本未到 20，不发布为正率或 Wilson 95% CI")
        summary, primary, comparison, verdict = (
            "有效样本未到 20，不发布净结果分布", None, None, "collecting")
    return _row(
        lane_name, verdict, _sample(n, MIN_N, target_unit),
        _result(evidence_type, summary, primary, comparison, "pct"),
        benchmark_kind, benchmark_summary, uncertainty, reason)


def _cascade(value: Any) -> dict:
    lane = _obj(value, "cascade")
    common = {
        "n", "hits", "pending", "unresolvable", "metric",
        "cost_is_real_fill", "resolved_24h", "not_due_24h", "due_24h",
        "attempted_unpriced_24h", "unavailable_24h",
        "oldest_due_24h_hours", "verdict", "edge_verdict",
    }
    measured = {
        "rate", "lo", "hi", "median_net_24h", "p90_net_24h",
        "p99_net_24h", "max_net_24h", "edge_note",
    }
    expected_keys = common | (measured if lane.get("verdict") == "measured"
                              else {"note"})
    if set(lane) != expected_keys:
        raise _Invalid("cascade_schema_invalid")
    return _descriptive(
        lane, lane_name="cascade", metric="positive_after_estimated_cost",
        rate_key="rate", value_key="median_net_24h", comparison_key=None,
        evidence_type="estimated_cost_adjusted_24h_not_real_fill",
        result_label="估算成本后 24h 中位", target_unit="events",
        benchmark_kind="missing_contemporaneous_control",
        benchmark_summary="缺少同期可比 WATCH 对照，描述性分布不能判定优势")


def _carry(value: Any) -> dict:
    lane = _obj(value, "carry")
    n = _uint(lane.get("n"), "carry.n")
    excluded = _uint(lane.get("excluded_closed"), "carry.excluded_closed")
    total = _uint(lane.get("total_closed"), "carry.total_closed")
    if (lane.get("cohort_kind") != "descriptive_quote_proxy"
            or lane.get("cost_completeness") != "partial"
            or lane.get("all_in_total_pct") is not None
            or lane.get("execution_mode") != "paper_orderbook_measurement"
            or total != n + excluded):
        raise _Invalid("carry_proxy_contract_invalid")
    return _descriptive(
        lane, lane_name="carry",
        metric="quote_rate_integral_minus_book_quotes_and_modeled_fee_proxy",
        rate_key="positive_rate", value_key="mean_net_proxy_pct",
        comparison_key="median_net_proxy_pct",
        evidence_type="partial_quote_rate_and_book_cost_proxy",
        result_label="净报价代理均值",
        target_unit="valid_quote_proxy_closures",
        benchmark_kind="missing_real_settlement_and_full_cost_control",
        benchmark_summary=(
            "缺实际结算、basis、完整成本、真实双腿成交与样本外对照"
        ))


def _airdrop(value: Any) -> dict:
    lane = _obj(value, "airdrop")
    _safe_claims(lane, "airdrop")
    common_keys = {
        "n_events", "n_claimed", "n_transaction_verified",
        "n_claim_semantics_verified", "n_reward_valued",
        "n_fully_verified_claims", "pending", "metric", "edge_verdict",
        "verdict", "note",
    }
    amount_keys = {
        "gross_reward_usd", "actual_cost_usd", "net_reward_usd",
        "median_net_reward_usd",
    }
    expected_keys = common_keys | (
        amount_keys if lane.get("verdict") == "realized_claims" else set()
    )
    if (lane.get("metric") != "fully_verified_claim_net_usd"
            or lane.get("edge_verdict") != "不可判"
            or set(lane) != expected_keys):
        raise _Invalid("airdrop_identity_invalid")
    events = _uint(lane.get("n_events"), "airdrop.events")
    transactions = _uint(
        lane.get("n_transaction_verified"), "airdrop.transactions",
    )
    semantics = _uint(
        lane.get("n_claim_semantics_verified"), "airdrop.semantics",
    )
    valued = _uint(lane.get("n_reward_valued"), "airdrop.valued")
    full = _uint(lane.get("n_fully_verified_claims"), "airdrop.full")
    claimed = _uint(lane.get("n_claimed"), "airdrop.claimed")
    pending = _uint(lane.get("pending"), "airdrop.pending")
    if (not full == claimed <= valued <= semantics <= transactions <= events
            or full + pending > events):
        raise _Invalid("airdrop_verification_counts_invalid")
    reason = _str(lane.get("note"), "airdrop.note")
    net = _num(lane.get("net_reward_usd"), "airdrop.net", optional=True)
    if lane.get("verdict") == "realized_claims":
        gross = _num(lane.get("gross_reward_usd"), "airdrop.gross")
        cost = _num(lane.get("actual_cost_usd"), "airdrop.cost")
        _num(
            lane.get("median_net_reward_usd"), "airdrop.median_net",
        )
        if (full == 0 or net is None or gross < 0 or cost < 0
                or not math.isclose(
                    net, gross - cost, rel_tol=0, abs_tol=0.02,
                )):
            raise _Invalid("airdrop_realized_result_invalid")
        summary = f"完整核验领取净回报合计 ${net:.2f}；不代表策略 edge"
    elif (lane.get("verdict") == "不可判" and full == 0 and net is None
          and all(lane.get(field) is None for field in (
              "gross_reward_usd", "actual_cost_usd", "median_net_reward_usd",
          ))):
        summary = "尚无完整核验领取与实际成本，不计入净回报"
    else:
        raise _Invalid("airdrop_verdict_invalid")
    return _row(
        "airdrop", "descriptive_only",
        _sample(full, None, "fully_verified_claims_no_edge_denominator"),
        _result("fully_verified_claim_net_usd", summary, net, unit="usd"),
        "missing_participation_failure_denominator",
        "缺参与失败与资格未命中分母，不能计算命中率或优势",
        _uncertainty("not_available_without_denominator", "unavailable",
                     "没有完整分母，置信区间不适用"), reason)


def _structure(value: Any) -> dict:
    lane = _obj(value, "structure")
    _safe_claims(lane, "structure")
    if (set(lane) != {"verdict", "n_events", "pending", "note"}
            or lane.get("verdict") != "not_directional"):
        raise _Invalid("structure_verdict_invalid")
    events = _uint(lane.get("n_events"), "structure.n_events")
    reason = _str(lane.get("note"), "structure.note")
    return _row(
        "structure", "not_applicable",
        _sample(events, None, "observed_events_not_directional_trials"),
        _result("non_directional_structure_observation",
                "结构事件不计算方向收益或命中率"),
        "not_applicable", "没有方向假设，因此不设置价格基准",
        _uncertainty("not_applicable", "not_applicable",
                     "非方向事件不适用收益置信区间"), reason)


_BUILDERS = {"launch": _launch, "cascade": _cascade, "carry": _carry,
             "airdrop": _airdrop, "structure": _structure}


def build_validation_overview(lanes: Any) -> dict:
    """Project a fixed five-row overview; malformed input remains non-actionable."""
    source = lanes if isinstance(lanes, Mapping) else {}
    rows, invalid = [], []
    for lane in LANE_ORDER:
        try:
            rows.append(_BUILDERS[lane](source.get(lane)))
        except (KeyError, TypeError, ValueError, OverflowError) as exc:
            rows.append(_invalid_row(lane, str(exc) or f"{lane}_input_invalid"))
            invalid.append(lane)
    return {"version": VERSION,
            "state": "unverifiable" if invalid else "no_execution_edge",
            "lane_order": list(LANE_ORDER),
            "reason_codes": [f"{lane}_unverifiable" for lane in invalid],
            "real_edge_n": 0, "execution_edge_eligible": False,
            "auto_execution_allowed": False, "rows": rows}
