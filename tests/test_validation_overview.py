from __future__ import annotations

from copy import deepcopy

import pytest

from src.pipeline.validation_overview import (
    LANE_ORDER,
    build_validation_overview,
)


def _launch() -> dict:
    return {
        "metric": "append_only_exact_pool_24h_positive_after_frozen_full_paper_cost",
        "sample_kind": "forward_paper_selector",
        "selection_stage": "discovery_rule_before_security_and_route",
        "cost_is_real_fill": False,
        "real_edge_n": 0,
        "real_edge_eligible": False,
        "execution_edge_eligible": False,
        "auto_execution_allowed": False,
        "n": 0,
        "probe": {
            "eligible_n": 0, "resolved_n": 0, "median_net_24h": None,
        },
        "control": {
            "eligible_n": 0, "resolved_n": 0, "median_net_24h": None,
        },
        "edge_validation": {
            "state": "protocol_integrity_blocked",
            "reason": "protocol admission scheduled; source readiness blocked",
            "eligible_n": {"SMALL_PROBE": 0, "WATCH": 0},
            "next_look_n_per_arm": 100,
            "look_n_per_arm": None,
            "planned_looks": [100, 200, 400, 800, 1_600, 3_200],
            "family_alpha": 0.05,
            "look_alpha": 0.00833333,
            "cost_is_real_fill": False,
            "real_edge_n": 0,
            "real_edge_eligible": False,
            "execution_edge_eligible": False,
            "auto_execution_allowed": False,
        },
        # Deliberately large: this frozen mutable cohort must never enter the v6
        # sample, target, verdict or uncertainty projection.
        "legacy_distribution": {
            "n": 999, "rate": 0.99,
            "sample_kind": "legacy_mutable_outcome_descriptive_only",
            "edge_eligible": False,
            "real_edge_n": 0,
            "real_edge_eligible": False,
            "execution_edge_eligible": False,
            "auto_execution_allowed": False,
        },
    }


def _cascade(n: int = 16) -> dict:
    measured = n >= 20
    value = {
        "n": n, "hits": 12 if measured else 8,
        "pending": 1, "unresolvable": 0,
        "metric": "positive_after_estimated_cost",
        "cost_is_real_fill": False,
        "verdict": "measured" if measured else "不可判",
        "edge_verdict": "不可判",
        "resolved_24h": n, "not_due_24h": 1, "due_24h": 0,
        "attempted_unpriced_24h": 0, "unavailable_24h": 0,
        "oldest_due_24h_hours": None,
    }
    if measured:
        value.update({
            "rate": 0.6, "lo": 0.387, "hi": 0.781,
            "median_net_24h": -0.25,
            "p90_net_24h": 4.0, "p99_net_24h": 8.0,
            "max_net_24h": 9.0,
            "edge_note": "缺少同期可比 WATCH 对照",
        })
    else:
        value["note"] = f"24h样本 {n}/20;继续积累"
    return value


def _carry(n: int = 16) -> dict:
    measured = n >= 20
    excluded = 44
    value = {
        "n": n, "n_proxy": n, "hits": 12 if measured else 0,
        "real_edge_n": 0, "total_closed": n + excluded,
        "excluded_closed": excluded,
        "excluded_by_reason": {"legacy_episode": excluded},
        "pending": 14,
        "metric": "quote_rate_integral_minus_book_quotes_and_modeled_fee_proxy",
        "cohort_kind": "descriptive_quote_proxy",
        "cost_completeness": "partial", "all_in_total_pct": None,
        "cost_is_real_fill": False, "real_edge_eligible": False,
        "execution_edge_eligible": False, "auto_execution_allowed": False,
        "execution_mode": "paper_orderbook_measurement",
        "verdict": "measured" if measured else "不可判",
        "edge_verdict": "不可判",
        "note": (f"有效报价代理关闭 {n}/20；隔离 {excluded}；"
                 "缺真实结算、完整成本与真实双腿成交"),
    }
    if measured:
        value.update({
            "positive_rate": 0.6, "lo": 0.387, "hi": 0.781,
            "mean_net_proxy_pct": -0.4,
            "median_net_proxy_pct": -0.5,
            "worst_net_proxy_pct": -0.9,
        })
    return value


def _airdrop() -> dict:
    return {
        "n_events": 2, "n_claimed": 0,
        "n_transaction_verified": 0,
        "n_claim_semantics_verified": 0,
        "n_reward_valued": 0,
        "n_fully_verified_claims": 0,
        "pending": 2,
        "metric": "fully_verified_claim_net_usd",
        "edge_verdict": "不可判", "verdict": "不可判",
        "note": "缺参与失败与资格未命中分母，不能判断优势",
    }


def _structure() -> dict:
    return {
        "verdict": "not_directional", "n_events": 96, "pending": 0,
        "note": "公开结构事件没有方向假设，不计算方向命中率",
    }


def _lanes() -> dict:
    return {
        "launch": _launch(), "cascade": _cascade(), "carry": _carry(),
        "airdrop": _airdrop(), "structure": _structure(),
        # The frozen legacy board lane is intentionally out of scope.
        "opp": {"n": 50_000, "verdict": "measured", "rate": 1.0},
    }


def _row(overview: dict, lane: str) -> dict:
    return next(row for row in overview["rows"] if row["lane"] == lane)


def test_overview_is_fixed_to_five_lanes_and_never_grants_execution():
    overview = build_validation_overview(_lanes())

    assert overview["state"] == "no_execution_edge"
    assert overview["lane_order"] == list(LANE_ORDER)
    assert [row["lane"] for row in overview["rows"]] == list(LANE_ORDER)
    assert overview["real_edge_n"] == 0
    assert overview["execution_edge_eligible"] is False
    assert overview["auto_execution_allowed"] is False
    assert all(row["real_edge_n"] == 0 for row in overview["rows"])
    assert all(row["execution_edge_eligible"] is False for row in overview["rows"])
    assert all(row["auto_execution_allowed"] is False for row in overview["rows"])
    assert "opp" not in overview["lane_order"]


def test_launch_uses_only_current_two_arm_protocol_and_quarantines_legacy():
    overview = build_validation_overview(_lanes())
    launch = _row(overview, "launch")

    assert launch["verdict"] == "blocked"
    assert launch["sample"] == {
        "valid_n": 0, "target_n": 100, "target_unit": "per_arm",
        "launch_arms": {
            "probe": {"eligible_n": 0, "resolved_n": 0},
            "control": {"eligible_n": 0, "resolved_n": 0},
        },
    }
    assert "999" not in str(launch)
    assert launch["uncertainty"]["kind"] == "sequential_spa_upper_p"
    assert launch["uncertainty"]["state"] == "not_due"
    assert launch["result"]["primary_value"] is None
    assert "source readiness blocked" in launch["blocking_reason"]


def test_carry_overview_uses_only_stats_lane_not_faster_paper_snapshot():
    lanes = _lanes()
    independent_perps_snapshot = {"carry_paper": {"n_proxy_closed": 20}}

    overview = build_validation_overview(lanes)

    assert independent_perps_snapshot["carry_paper"]["n_proxy_closed"] == 20
    assert _row(overview, "carry")["sample"]["valid_n"] == 16
    assert _row(overview, "carry")["sample"]["target_n"] == 20


def test_wilson_is_named_as_positive_rate_uncertainty_not_return_interval():
    lanes = _lanes()
    lanes["cascade"] = _cascade(20)
    lanes["carry"] = _carry(20)

    overview = build_validation_overview(lanes)

    for lane in ("cascade", "carry"):
        uncertainty = _row(overview, lane)["uncertainty"]
        assert uncertainty == {
            "kind": "wilson_95_positive_rate",
            "state": "available",
            "summary": "成本后为正率 60.0%；Wilson 95% CI [38.7%, 78.1%]",
            "value": 0.6, "lower": 0.387, "upper": 0.781,
            "threshold": None,
        }
        assert "收益" not in uncertainty["summary"]


def test_misordered_wilson_interval_fails_closed():
    lanes = _lanes()
    lanes["carry"] = _carry(20)
    lanes["carry"]["hi"] = 0.5

    overview = build_validation_overview(lanes)

    assert overview["state"] == "unverifiable"
    assert overview["reason_codes"] == ["carry_unverifiable"]
    assert _row(overview, "carry")["verdict"] == "unverifiable"


def test_launch_spa_projection_uses_upper_p_and_keeps_pass_paper_only():
    lanes = _lanes()
    launch = lanes["launch"]
    launch.update({
        "n": 200,
        "probe": {"eligible_n": 110, "resolved_n": 100,
                  "median_net_24h": 1.2},
        "control": {"eligible_n": 108, "resolved_n": 100,
                    "median_net_24h": -0.5},
    })
    launch["edge_validation"].update({
        "state": "pass", "reason": "frozen paper look passed",
        "eligible_n": {"SMALL_PROBE": 110, "WATCH": 108},
        "look_n_per_arm": 100, "next_look_n_per_arm": 200,
        "arms": {
            "SMALL_PROBE": {"median_net_24h": 1.2},
            "WATCH": {"median_net_24h": -0.5},
        },
        "spa_pvalues": {"lower": 0.001, "consistent": 0.002, "upper": 0.005},
        "spa_pvalue_used": "upper",
    })

    overview = build_validation_overview(lanes)
    row = _row(overview, "launch")

    assert overview["state"] == "no_execution_edge"
    assert row["verdict"] == "paper_signal"
    assert row["sample"]["valid_n"] == 100
    assert row["sample"]["target_n"] == 200
    assert row["result"]["primary_value"] == 1.2
    assert row["result"]["comparison_value"] == -0.5
    assert row["uncertainty"]["kind"] == "sequential_spa_upper_p"
    assert row["uncertainty"]["value"] == 0.005
    assert row["uncertainty"]["threshold"] == 0.00833333
    assert row["execution_edge_eligible"] is False
    assert row["auto_execution_allowed"] is False


@pytest.mark.parametrize(
    "mutate, lane",
    [
        (lambda lanes: lanes.pop("structure"), "structure"),
        (lambda lanes: lanes["cascade"].update(n=float("nan")), "cascade"),
        (lambda lanes: lanes["airdrop"].update(auto_execution_allowed=True),
         "airdrop"),
        (lambda lanes: lanes["launch"].update(execution_edge_eligible=True),
         "launch"),
        (lambda lanes: lanes["launch"]["edge_validation"].update(
            next_look_n_per_arm=1, look_alpha=1.0,
         ), "launch"),
        (lambda lanes: lanes["cascade"].update(edge_verdict="已获真实优势"),
         "cascade"),
        (lambda lanes: lanes["structure"].update(
            edge_verdict="保证暴富", realized_return_pct=999_999,
         ), "structure"),
        (lambda lanes: lanes["airdrop"].update(
            n_fully_verified_claims=999, n_claimed=999,
            verdict="realized_claims", gross_reward_usd=1,
            actual_cost_usd=1_000, net_reward_usd=1_000_000_000,
            median_net_reward_usd=1_000_000_000,
         ), "airdrop"),
    ],
)
def test_missing_malicious_or_illegal_input_fails_closed(mutate, lane):
    lanes = _lanes()
    mutate(lanes)

    overview = build_validation_overview(lanes)

    assert overview["state"] == "unverifiable"
    assert f"{lane}_unverifiable" in overview["reason_codes"]
    assert _row(overview, lane)["verdict"] == "unverifiable"
    assert overview["execution_edge_eligible"] is False
    assert overview["auto_execution_allowed"] is False


def test_stats_contract_recomputes_overview_and_rejects_projection_drift(
        tmp_path, monkeypatch):
    from src.contract.board_view import BoardViewContractError, validate_board_view
    from src.pipeline import board_export, board_outcomes, opportunity_ledger

    monkeypatch.setattr(board_outcomes, "DB", tmp_path / "board_picks.db")
    monkeypatch.setattr(opportunity_ledger, "DB", tmp_path / "ledger.db")
    payload = board_export.render_stats(None)
    cadence, grace = board_export.VIEW_FRESHNESS["stats"]
    validate_board_view("stats", payload, cadence_min=cadence, grace_min=grace)

    bad = deepcopy(payload)
    bad["validation_overview"]["rows"][0]["sample"]["valid_n"] = 444
    with pytest.raises(
            BoardViewContractError,
            match="validation_overview does not match its lanes snapshot"):
        validate_board_view("stats", bad, cadence_min=cadence, grace_min=grace)

    type_drift = deepcopy(payload)
    type_drift["validation_overview"]["auto_execution_allowed"] = 0
    with pytest.raises(
            BoardViewContractError,
            match="validation_overview does not match its lanes snapshot"):
        validate_board_view(
            "stats", type_drift, cadence_min=cadence, grace_min=grace,
        )


@pytest.mark.parametrize("lane,mutate", [
    ("cascade", lambda value: value.update(
        n=20, hits=0, verdict="measured", rate=1.0, lo=1.0, hi=1.0,
        median_net_24h=999.0, edge_verdict="已获真实优势",
    )),
    ("airdrop", lambda value: value.update(
        n_events=0, n_claimed=999, n_transaction_verified=0,
        n_claim_semantics_verified=0, n_reward_valued=0,
        n_fully_verified_claims=999, pending=0, verdict="realized_claims",
        gross_reward_usd=1.0, actual_cost_usd=1_000.0,
        net_reward_usd=1_000_000_000.0,
        median_net_reward_usd=1_000_000_000.0,
    )),
    ("launch", lambda value: value["edge_validation"].update(
        next_look_n_per_arm=1, look_alpha=1.0,
    )),
    ("structure", lambda value: value.update(
        edge_verdict="保证暴富", realized_return_pct=999_999,
    )),
])
def test_stats_contract_rejects_present_forged_lane_even_with_reprojection(
        tmp_path, monkeypatch, lane, mutate):
    from src.contract.board_view import BoardViewContractError, validate_board_view
    from src.pipeline import board_export, board_outcomes, opportunity_ledger

    monkeypatch.setattr(board_outcomes, "DB", tmp_path / "board_picks.db")
    monkeypatch.setattr(opportunity_ledger, "DB", tmp_path / "ledger.db")
    payload = board_export.render_stats(None)
    if lane not in payload["lanes"]:
        payload["lanes"][lane] = deepcopy(_lanes()[lane])
    mutate(payload["lanes"][lane])
    payload["validation_overview"] = build_validation_overview(payload["lanes"])
    cadence, grace = board_export.VIEW_FRESHNESS["stats"]

    with pytest.raises(
            BoardViewContractError,
            match="invalid present validation lanes"):
        validate_board_view(
            "stats", payload, cadence_min=cadence, grace_min=grace,
        )
