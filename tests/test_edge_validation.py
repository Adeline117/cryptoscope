"""The Launch edge gate is forward-only, pre-registered and fail closed."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone


def _row(index: int, arm: str, net: float | None, *, day: int | None = None,
         cohort_version: int = 5, before_start: bool = False,
         unavailable: bool = False) -> dict:
    from src.pipeline import edge_validation as ev

    start = datetime.fromisoformat(ev.PROTOCOL_START_AT)
    detected = start + timedelta(days=index % 20 if day is None else day,
                                 minutes=index)
    if before_start:
        detected = start - timedelta(seconds=1)
    outcome: dict = {"horizons": {}}
    if net is not None:
        outcome["horizons"]["24h"] = {"net_return_pct_est": net}
    elif unavailable:
        outcome["unavailable_horizons"] = ["24h"]
    return {
        "id": f"{arm}-{index}", "lane": "launch", "chain": "solana",
        "token": f"token-{arm}-{index}", "decision": arm,
        "detected_at": detected.isoformat(), "cohort_version": cohort_version,
        "cost_contract_version": 1,
        "cost_contract": {
            "purpose": "discovery_outcome", "method": ev.LAUNCH_COST_METHOD,
        },
        "outcome": outcome,
    }


def _complete_cohort(*, probe_shift: float = 12.0,
                     watch_shift: float = -4.0) -> list[dict]:
    rows = []
    for i in range(100):
        # Twenty shared days and deterministic within-day variation avoid a
        # zero-variance/studentization shortcut in the statistical test.
        rows.append(_row(i, "SMALL_PROBE", probe_shift + (i % 7) * 0.4))
        rows.append(_row(i, "WATCH", watch_shift + (i % 5) * 0.3))
    return rows


def test_protocol_rejects_legacy_and_pre_registration_rows():
    from src.pipeline import edge_validation as ev

    valid = _row(0, "SMALL_PROBE", 5.0)
    assert ev.is_protocol_event(valid)
    assert not ev.is_protocol_event({**valid, "cohort_version": 3})
    assert not ev.is_protocol_event({**valid, "cohort_version": 4})
    assert not ev.is_protocol_event(_row(
        1, "SMALL_PROBE", 5.0, before_start=True
    ))
    assert not ev.is_protocol_event({
        **valid,
        "cost_contract": {**valid["cost_contract"], "method": "retuned_after_outcome"},
    })


def test_no_pre_registered_look_means_collecting_not_edge():
    from src.pipeline import edge_validation as ev

    rows = [_row(i, "SMALL_PROBE", 20.0) for i in range(99)]
    rows += [_row(i, "WATCH", -10.0) for i in range(99)]
    got = ev.launch_forward_validation(rows)

    assert got["state"] == "collecting"
    assert got["edge_verdict"] == "不可判"
    assert got["next_look_n_per_arm"] == 100
    assert got["eligible_n"] == {"SMALL_PROBE": 99, "WATCH": 99}
    assert "spa_pvalues" not in got


def test_fixed_prefix_waits_for_pending_outcome_instead_of_cherry_picking_later_row():
    from src.pipeline import edge_validation as ev

    rows = _complete_cohort()
    first = min((row for row in rows if row["decision"] == "SMALL_PROBE"),
                key=lambda row: (row["detected_at"], row["id"]))
    first["outcome"] = {"horizons": {}}
    rows.append(_row(500, "SMALL_PROBE", 100.0, day=21))

    got = ev.launch_forward_validation(rows)

    assert got["look_n_per_arm"] == 100
    assert got["state"] == "awaiting_outcomes"
    assert got["arms"]["SMALL_PROBE"]["pending_n"] == 1
    assert "spa_pvalues" not in got


def test_outcome_attrition_and_market_regime_mismatch_fail_closed():
    from src.pipeline import edge_validation as ev

    low_coverage = _complete_cohort()
    probe = [row for row in low_coverage if row["decision"] == "SMALL_PROBE"]
    for row in probe[:6]:
        row["outcome"] = {"horizons": {}, "unavailable_horizons": ["24h"]}
    got = ev.launch_forward_validation(low_coverage)
    assert got["state"] == "coverage_blocked"
    assert got["edge_verdict"] == "不可判"
    assert got["arms"]["SMALL_PROBE"]["coverage"] == 0.94

    disjoint = []
    for i in range(100):
        disjoint.append(_row(i, "SMALL_PROBE", 20.0 + i % 3, day=i % 10))
        disjoint.append(_row(i, "WATCH", -5.0 + i % 3, day=20 + i % 10))
    got = ev.launch_forward_validation(disjoint)
    assert got["state"] == "regime_overlap_blocked"
    assert got["shared_days"] == 0
    assert "spa_pvalues" not in got


def test_selective_attrition_inside_old_tolerance_cannot_manufacture_pass():
    """Five missing probe rugs used to pass the old 95% coverage threshold."""
    from src.pipeline import edge_validation as ev

    rows = _complete_cohort()
    probe = [row for row in rows if row["decision"] == "SMALL_PROBE"]
    watch = [row for row in rows if row["decision"] == "WATCH"]
    for row in probe[:5]:
        row["outcome"] = {"horizons": {}, "unavailable_horizons": ["24h"]}
    watch[0]["outcome"] = {"horizons": {}, "unavailable_horizons": ["24h"]}

    got = ev.launch_forward_validation(rows)

    assert got["state"] == "coverage_blocked"
    assert got["edge_verdict"] == "不可判"
    assert got["required_outcome_coverage"] == 1.0
    assert got["arms"]["SMALL_PROBE"]["coverage"] == 0.95
    assert got["arms"]["WATCH"]["coverage"] == 0.99
    assert "spa_pvalues" not in got


def test_nonshared_bad_days_are_not_dropped_from_the_primary_endpoint():
    from src.pipeline import edge_validation as ev

    rows = []
    for i in range(80):
        rows.append(_row(i, "SMALL_PROBE", 12.0 + (i % 3) * 0.1, day=i % 20))
        rows.append(_row(i, "WATCH", -4.0 + (i % 3) * 0.1, day=i % 20))
    for i in range(20):
        rows.append(_row(80 + i, "SMALL_PROBE", -99.0, day=20 + i))
        rows.append(_row(80 + i, "WATCH", -4.0, day=40 + i))

    got = ev.launch_forward_validation(rows)

    assert got["shared_days"] == 20
    assert got["calendar_days"] == 60
    assert got["shared_event_fraction"] == {
        "SMALL_PROBE": 0.8, "WATCH": 0.8,
    }
    assert got["calendar_policy"] == "union_days_absent_arm_is_cash"
    assert got["mean_event_log_utility"]["SMALL_PROBE"] < 0
    assert got["mean_event_log_utility_lift"] < 0
    assert got["state"] == "no_edge_observed"
    assert got["edge_verdict"] == "无edge/负"
    assert "spa_pvalues" not in got


def test_strong_forward_effect_passes_deterministic_conservative_spa():
    from src.pipeline import edge_validation as ev

    first = ev.launch_forward_validation(_complete_cohort())
    second = ev.launch_forward_validation(_complete_cohort())

    assert first == second
    assert first["state"] == "pass"
    assert first["edge_verdict"] == "有前向纸面edge迹象"
    assert first["look_n_per_arm"] == 100
    assert first["shared_days"] == 20
    assert first["spa_pvalue_used"] == "upper"
    assert first["spa_pvalues"]["upper"] <= ev.LOOK_ALPHA
    assert first["mean_daily_log_utility_lift"] >= ev.MIN_MEAN_UTILITY_LIFT
    assert first["cost_is_real_fill"] is False
    assert first["real_edge_eligible"] is False


def test_negative_fixed_look_never_becomes_a_positive_edge():
    from src.pipeline import edge_validation as ev

    got = ev.launch_forward_validation(_complete_cohort(
        probe_shift=-8.0, watch_shift=5.0
    ))

    assert got["state"] == "no_edge_observed"
    assert got["edge_verdict"] == "无edge/负"
    assert got["mean_daily_log_utility_lift"] < 0
