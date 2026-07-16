"""The v6 Launch gate must derive paper results from immutable evidence only."""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone

import pytest


@pytest.fixture(autouse=True)
def _explicit_test_source_authorities(monkeypatch):
    """Unit rows use a deterministic authority; integration tests cover SQLite."""
    from src.pipeline import edge_validation as ev

    monkeypatch.setattr(
        ev, "_candidate_source_proof",
        lambda row, _snapshot: deepcopy(row["_expected_reconciliation_proof"]),
    )
    monkeypatch.setattr(
        ev, "_protocol_admission_state",
        lambda: {"state": "open", "enrollment_open": True, "reason_codes": []},
    )


def _stored_observation(row: dict, price: float, **overrides) -> dict:
    """Build the exact merged shape returned by ledger.outcome_rows()."""
    from src.pipeline import opportunity_ledger as ledger

    anchor = datetime.fromisoformat(row["entry_observation"]["observed_at"])
    target = anchor + timedelta(hours=24)
    created = target + timedelta(minutes=5)
    observation = {
        "version": 1,
        "provider": "geckoterminal_public_v2",
        "network": "solana",
        "chain": "solana",
        "token": row["token"],
        "token_side": "base",
        "pair": row["entry_observation"]["pair"],
        "pool": row["entry_observation"]["pair"],
        "currency": "usd",
        "field": "close",
        "target_at": target.isoformat(),
        "candle_at": target.isoformat(),
        "distance_seconds": 0.0,
        "price": price,
        "retrieved_at": created.isoformat(),
        "identity_verified": True,
    }
    observation.update(overrides)
    observation.update({
        "opportunity_id": row["id"],
        "horizon": "24h",
        "entry_observation_hash": ledger._json_hash(row["entry_observation"]),
        "cost_contract_hash": ledger._json_hash(row["cost_contract"]),
    })
    observation_id = ledger.hashlib.sha256(
        f"{row['id']}:24h:{ledger._canonical_json(observation)}".encode()
    ).hexdigest()[:32]
    return {
        **observation,
        "observation_id": observation_id,
        "created_at": created.isoformat(),
    }


def _row(
        index: int, arm: str, net: float | None, *, day: int | None = None,
        unavailable: bool = False, outcome_missing: bool = False,
        detected_at: datetime | None = None, observation_overrides: dict | None = None,
) -> dict:
    from src.pipeline import edge_validation as ev
    from src.contract.launch_selector import (
        evaluate_selector_snapshot, freeze_selector_snapshot, freeze_source_snapshot,
    )
    from src.pipeline.execution_cost import solana_launch_full_paper_contract

    anchor = datetime.fromisoformat(ev.PROTOCOL_START_AT) + timedelta(
        days=index % 20 if day is None else day,
        minutes=index,
    )
    token = f"token-{arm}-{index}"
    row_detected = (detected_at or anchor).astimezone(timezone.utc)
    event_at = anchor - timedelta(minutes=30)
    selector_snapshot = freeze_selector_snapshot(
        pool_created_at=event_at.isoformat(), liquidity_usd=8_000,
        fdv_usd=100_000, volume_m5_usd=500 if arm == "SMALL_PROBE" else 0,
        buys_m5=5 if arm == "SMALL_PROBE" else 1, sells_m5=2,
    )
    selector = evaluate_selector_snapshot(
        selector_snapshot, event_at=event_at.isoformat(), decision_at=anchor.isoformat(),
    )
    cap = selector["max_notional_usd"]
    contract = solana_launch_full_paper_contract(
        notional_usd=cap,
        modeled_route_roundtrip_pct=selector["modeled_route_roundtrip_pct"],
        method=ev.LAUNCH_COST_METHOD,
    )
    proof = {
        "version": 1, "epoch_id": "1" * 32,
        "from_slot": 0, "to_slot": 1_000, "status": "sealed_clean",
        "checked_at": anchor.isoformat(),
        "live_provider": "solana_rpc:live.example",
        "archive_provider": "solana_rpc:archive.example",
        "genesis_hash": "mainnet-genesis", "evidence_hash": "e" * 64,
        "finalized_head": 1_000,
        "live_captured_at": row_detected.isoformat(),
        "live_observation_hash": "a" * 64,
        "archive_observation_hash": "a" * 64,
        "hydration_identity_hash": "b" * 64,
    }
    row = {
        "id": f"{arm}-{index}",
        "lane": "launch",
        "chain": "solana",
        "token": token,
        "source": "Pump.fun standard logs + DEX Screener pool",
        "primary_evidence": {"creator": "creator"},
        "decision": arm,
        "event_at": event_at.isoformat(),
        "detected_at": row_detected.isoformat(),
        "decision_at": anchor.isoformat(),
        "created_at": (anchor + timedelta(seconds=1)).isoformat(),
        "state": "live",
        "entry_price": 100.0,
        "entry_observation_version": 1,
        "entry_observation": {
            "version": 1,
            "provider": "dexscreener_token_pairs_v1",
            "observed_at": anchor.isoformat(),
            "chain": "solana",
            "base_token": token,
            "quote_token": "So11111111111111111111111111111111111111112",
            "pair": f"pool-{token}",
            "price": 100.0,
            "currency": "usd",
            "field": "priceUsd",
            "identity_verified": True,
            "token_side": "base",
            "selector_snapshot": selector_snapshot,
            "source_snapshot": freeze_source_snapshot(
                signature=f"signature-{arm}-{index}", slot=index,
                event_type="pump_fun_createv2", detected_at=row_detected.isoformat(),
                captured_at=row_detected.isoformat(), decision_at=anchor.isoformat(),
                mint=token, raw_payload_hash="a" * 64,
                hydration_payload_hash="b" * 64,
                capture_mode="live_ws",
                source_provider="solana_rpc:live.example",
                reconciliation_state="verified_live",
                reconciled_at=anchor.isoformat(), reconciliation_proof=proof,
            ),
        },
        "cohort_version": ev.COHORT_VERSION,
        "cost_contract_version": 1,
        "cost_contract": contract,
        "max_notional_usd": cap,
        "cost_pct_est": contract["all_in_total_pct"],
        "cost_model": ev.LAUNCH_COST_METHOD,
        "outcome": {"horizons": {}},
        "price_observations": {},
        "_expected_reconciliation_proof": deepcopy(proof),
    }
    if net is not None:
        gross = net + contract["all_in_total_pct"]
        price = row["entry_price"] * (1.0 + gross / 100.0)
        stored = _stored_observation(row, price, **(observation_overrides or {}))
        row["price_observations"] = {"24h": stored}
        gross_truth = (price / row["entry_price"] - 1.0) * 100.0
        net_truth = gross_truth - contract["all_in_total_pct"]
        if outcome_missing:
            row["outcome"] = {}
        else:
            row["outcome"]["horizons"]["24h"] = {
                "price_observation_id": stored["observation_id"],
                "price": price,
                "gross_return_pct": round(gross_truth, 4),
                "net_return_pct_est": round(net_truth, 4),
                "target_at": stored["target_at"],
                "outcome_anchor_at": anchor.isoformat(),
                "positive_after_cost": net_truth > 0,
            }
    elif unavailable:
        row["outcome"]["unavailable_horizons"] = ["24h"]
    return row


def _complete_cohort(
        *, probe_shift: float = 12.0, watch_shift: float = -4.0,
) -> list[dict]:
    rows = []
    for index in range(100):
        rows.append(_row(
            index, "SMALL_PROBE", probe_shift + (index % 7) * 0.4,
        ))
        rows.append(_row(
            index, "WATCH", watch_shift + (index % 5) * 0.3,
        ))
    return rows


def test_protocol_snapshot_exposes_normalized_complete_paper_contract():
    from src.pipeline import edge_validation as ev

    row = _row(0, "SMALL_PROBE", 5.0)
    snapshot = ev.protocol_snapshot(row)

    assert ev.PROTOCOL_ID == "launch-forward-spa-v3"
    assert ev.COHORT_VERSION == 6
    assert ev.PROTOCOL_START_AT == "2026-08-03T00:00:00+00:00"
    assert snapshot == {
        "anchor_at": row["entry_observation"]["observed_at"],
        "entry_observation": row["entry_observation"],
        "cost_contract": row["cost_contract"],
        "all_in_cost_pct": row["cost_contract"]["all_in_total_pct"],
        "cost_is_real_fill": False,
        "ledger_created_at": row["created_at"],
        "selector_snapshot": row["entry_observation"]["selector_snapshot"],
        "source_snapshot": row["entry_observation"]["source_snapshot"],
    }
    assert ev.is_protocol_event(row)
    assert ev.protocol_exclusion_reasons(row) == []


def test_protocol_rechecks_source_database_and_fails_closed(monkeypatch):
    from src.pipeline import edge_validation as ev

    row = _row(0, "SMALL_PROBE", 5.0)

    def unavailable(*_args, **_kwargs):
        raise OSError("source database unavailable")

    monkeypatch.setattr(ev, "_candidate_source_proof", unavailable)
    assert "source_proof_unverifiable" in ev.protocol_exclusion_reasons(row)
    assert ev.protocol_snapshot(row) is None

    monkeypatch.setattr(
        ev, "_candidate_source_proof",
        lambda *_args, **_kwargs: {
            **row["_expected_reconciliation_proof"], "evidence_hash": "f" * 64,
        },
    )
    assert "source_proof_mismatch" in ev.protocol_exclusion_reasons(row)


def test_breached_global_admission_blocks_entire_edge_verdict(monkeypatch):
    from src.pipeline import edge_validation as ev

    monkeypatch.setattr(
        ev, "_protocol_admission_state",
        lambda: {
            "state": "breached", "enrollment_open": False,
            "reason_codes": ["source_readiness_breached_after_open"],
        },
    )

    got = ev.launch_forward_validation([_row(0, "SMALL_PROBE", 5.0)])

    assert got["state"] == "protocol_integrity_blocked"
    assert got["protocol_admission"]["state"] == "breached"
    assert "admission" in got["reason"]


def test_post_boundary_wrong_cohort_label_cannot_escape_integrity_denominator():
    from src.pipeline import edge_validation as ev

    row = _row(0, "SMALL_PROBE", 5.0)
    row["cohort_version"] = ev.COHORT_VERSION - 1

    assert ev.is_protocol_enrollment_candidate(row) is True
    assert "cohort_version_mismatch" in ev.protocol_exclusion_reasons(row)
    got = ev.launch_forward_validation([row])
    assert got["state"] == "protocol_integrity_blocked"
    assert got["integrity_invalid_n"] == 1
    assert got["integrity_invalid_by_reason"]["cohort_version_mismatch"] == 1


def test_protocol_rejects_v5_preboundary_partial_wrong_ceiling_and_snapshot_drift():
    from src.pipeline import edge_validation as ev
    from src.pipeline.execution_cost import (
        discovery_contract,
        solana_launch_full_paper_contract,
    )

    valid = _row(1, "SMALL_PROBE", 5.0)
    cases = []

    legacy = deepcopy(valid)
    legacy["cohort_version"] = 5
    cases.append((legacy, "cohort_version_mismatch"))

    before = deepcopy(valid)
    old = datetime.fromisoformat(ev.PROTOCOL_START_AT) - timedelta(seconds=1)
    before["detected_at"] = before["decision_at"] = old.isoformat()
    before["entry_observation"]["observed_at"] = old.isoformat()
    cases.append((before, "entry_before_protocol_start"))

    partial = deepcopy(valid)
    partial["cost_contract"] = discovery_contract(
        notional_usd=25, modeled_roundtrip_pct=1,
        method=ev.LAUNCH_COST_METHOD,
    )
    partial["cost_pct_est"] = 1
    cases.append((partial, "cost_incomplete"))

    wrong_ceiling = deepcopy(valid)
    wrong_ceiling["cost_contract"] = solana_launch_full_paper_contract(
        notional_usd=25, modeled_route_roundtrip_pct=1,
        method=ev.LAUNCH_COST_METHOD, network_fee_ceiling_usd=1,
    )
    wrong_ceiling["cost_pct_est"] = wrong_ceiling["cost_contract"]["all_in_total_pct"]
    cases.append((wrong_ceiling, "network_fee_ceiling_mismatch"))

    cost_drift = deepcopy(valid)
    cost_drift["cost_pct_est"] += 0.01
    cases.append((cost_drift, "row_cost_pct_mismatch"))

    model_drift = deepcopy(valid)
    model_drift["cost_model"] = "friendly_after_outcome"
    cases.append((model_drift, "row_cost_model_mismatch"))

    clock_drift = deepcopy(valid)
    clock_drift["decision_at"] = (
        datetime.fromisoformat(clock_drift["decision_at"]) + timedelta(seconds=1)
    ).isoformat()
    cases.append((clock_drift, "decision_entry_clock_mismatch"))

    provider_drift = deepcopy(valid)
    provider_drift["entry_observation"]["provider"] = "unfrozen_provider"
    cases.append((provider_drift, "entry_provider_mismatch"))

    source_drift = deepcopy(valid)
    source_drift["source"] = "DEX Screener token profiles"
    cases.append((source_drift, "source_universe_mismatch"))

    source_snapshot_missing = deepcopy(valid)
    source_snapshot_missing["entry_observation"].pop("source_snapshot")
    cases.append((source_snapshot_missing, "source_snapshot_invalid"))

    for row, expected_reason in cases:
        reasons = ev.protocol_exclusion_reasons(row)
        assert expected_reason in reasons
        assert ev.protocol_snapshot(row) is None
        assert not ev.is_protocol_event(row)


def test_protocol_recomputes_selector_arm_position_and_route_cost():
    from src.pipeline import edge_validation as ev
    from src.pipeline.execution_cost import solana_launch_full_paper_contract

    valid = _row(0, "SMALL_PROBE", None)

    arm_drift = deepcopy(valid)
    arm_drift["entry_observation"]["selector_snapshot"]["volume_m5_usd"] = 0
    assert "selector_decision_mismatch" in ev.protocol_exclusion_reasons(arm_drift)

    cap_drift = deepcopy(valid)
    cap_drift["max_notional_usd"] = 1_000_000_000
    cap_drift["cost_contract"] = solana_launch_full_paper_contract(
        notional_usd=1_000_000_000, modeled_route_roundtrip_pct=0,
        method=ev.LAUNCH_COST_METHOD,
    )
    cap_drift["cost_pct_est"] = cap_drift["cost_contract"]["all_in_total_pct"]
    reasons = ev.protocol_exclusion_reasons(cap_drift)
    assert "position_cap_rule_mismatch" in reasons
    assert "modeled_route_rule_mismatch" in reasons

    route_drift = deepcopy(valid)
    route_drift["cost_contract"] = solana_launch_full_paper_contract(
        notional_usd=valid["max_notional_usd"], modeled_route_roundtrip_pct=0.6,
        method=ev.LAUNCH_COST_METHOD,
    )
    route_drift["cost_pct_est"] = route_drift["cost_contract"]["all_in_total_pct"]
    assert "modeled_route_rule_mismatch" in ev.protocol_exclusion_reasons(route_drift)


def test_protocol_requires_prompt_immutable_ledger_enrollment_clock():
    from src.pipeline import edge_validation as ev

    missing = _row(0, "SMALL_PROBE", None)
    missing.pop("created_at")
    assert "ledger_created_clock_invalid" in ev.protocol_exclusion_reasons(missing)

    late = _row(1, "SMALL_PROBE", None)
    late["created_at"] = (
        datetime.fromisoformat(late["decision_at"])
        + timedelta(seconds=ev.MAX_LEDGER_COMMIT_DELAY_SECONDS + 1)
    ).isoformat()
    assert "ledger_commit_delay_exceeded" in ev.protocol_exclusion_reasons(late)


def test_protocol_rejects_survivors_discovered_after_source_latency_budget():
    from src.pipeline import edge_validation as ev

    on_boundary = _row(0, "SMALL_PROBE", None)
    decision = datetime.fromisoformat(on_boundary["decision_at"])
    detected = decision - timedelta(seconds=ev.MAX_SOURCE_TO_DECISION_SECONDS)
    on_boundary["detected_at"] = detected.isoformat()
    on_boundary["entry_observation"]["source_snapshot"]["detected_at"] = \
        detected.isoformat()
    assert "source_to_decision_delay_exceeded" not in \
        ev.protocol_exclusion_reasons(on_boundary)

    late = deepcopy(on_boundary)
    detected -= timedelta(microseconds=1)
    late["detected_at"] = detected.isoformat()
    late["entry_observation"]["source_snapshot"]["detected_at"] = detected.isoformat()
    assert "source_to_decision_delay_exceeded" in ev.protocol_exclusion_reasons(late)


def test_append_only_observation_resolves_without_mutable_outcome_and_recomputes_net():
    from src.pipeline import edge_validation as ev

    row = _row(0, "SMALL_PROBE", 7.25, outcome_missing=True)

    assert row["outcome"] == {}
    assert ev.protocol_point_state(row) == ("resolved", 7.25)


def test_finite_prices_whose_ratio_overflows_are_invalid_not_a_moonshot():
    from src.pipeline import edge_validation as ev

    row = _row(0, "SMALL_PROBE", None)
    row["entry_price"] = 5e-324
    row["entry_observation"]["price"] = 5e-324
    row["outcome"] = {}
    row["price_observations"] = {
        "24h": _stored_observation(row, 1e308),
    }

    assert ev._point_state_with_reason(row) == (
        "invalid", None, "recomputed_point_non_finite",
    )
    assert ev.protocol_point_state(row) == ("invalid", None)


def test_mutable_outcome_without_append_only_evidence_is_invalid():
    from src.pipeline import edge_validation as ev

    row = _row(0, "SMALL_PROBE", None)
    row["outcome"]["horizons"]["24h"] = {
        "price": 1_000_000,
        "net_return_pct_est": 999_999,
    }

    assert ev.protocol_point_state(row) == ("invalid", None)


@pytest.mark.parametrize("field,bad_value", [
    ("price_observation_id", "forged"),
    ("price", 999_999),
    ("gross_return_pct", 999_999),
    ("net_return_pct_est", 999_999),
    ("target_at", "2026-07-19T00:00:00+00:00"),
    ("outcome_anchor_at", "2026-07-18T00:00:00+00:00"),
    ("positive_after_cost", False),
])
def test_mutable_outcome_must_exactly_match_append_only_truth(field, bad_value):
    from src.pipeline import edge_validation as ev

    row = _row(0, "SMALL_PROBE", 5.0)
    row["outcome"]["horizons"]["24h"][field] = bad_value

    assert ev.protocol_point_state(row) == ("invalid", None)


@pytest.mark.parametrize("overrides", [
    {"provider": "another_valid_provider"},
    {"network": "eth"},
    {"pair": "another-pair-label"},
])
def test_outcome_price_source_and_exact_identity_are_frozen(overrides):
    from src.pipeline import edge_validation as ev

    row = _row(
        0, "SMALL_PROBE", 5.0,
        outcome_missing=True, observation_overrides=overrides,
    )

    # The append shape itself remains internally valid; v6's stricter source/side
    # preregistration is what excludes it.
    assert ev.protocol_point_state(row) == ("invalid", None)


def test_exact_frozen_token_may_be_geckoterminal_quote_side():
    from src.pipeline import edge_validation as ev

    row = _row(
        0, "SMALL_PROBE", 5.0,
        outcome_missing=True, observation_overrides={"token_side": "quote"},
    )

    assert ev.protocol_point_state(row) == ("resolved", 5.0)


def test_hindsight_survivor_with_late_decision_blocks_protocol_integrity():
    from src.pipeline import edge_validation as ev

    start = datetime.fromisoformat(ev.PROTOCOL_START_AT)
    rows = [_row(0, "SMALL_PROBE", None)]
    rows += [_row(i, "SMALL_PROBE", 10.0, day=1 + i % 20) for i in range(1, 100)]
    # This event was detected first but its actual decision/entry anchor is last.  A
    # detected_at sort would cherry-pick it into the 100-row prefix and drop pending 0.
    survivor = _row(500, "SMALL_PROBE", 100.0, day=100)
    survivor["detected_at"] = start.isoformat()
    source = survivor["entry_observation"]["source_snapshot"]
    source["detected_at"] = source["captured_at"] = start.isoformat()
    source["reconciliation_proof"]["live_captured_at"] = start.isoformat()
    survivor["_expected_reconciliation_proof"] = deepcopy(
        source["reconciliation_proof"]
    )
    rows.append(survivor)
    rows += [_row(i, "WATCH", -3.0) for i in range(100)]

    got = ev.launch_forward_validation(rows)

    assert got["state"] == "protocol_integrity_blocked"
    assert got["integrity_invalid_by_reason"] == {
        "source_snapshot_invalid": 1,
        "source_to_decision_delay_exceeded": 1,
    }
    assert "look_n_per_arm" not in got


def test_unavailable_or_invalid_prefix_evidence_blocks_complete_coverage():
    from src.pipeline import edge_validation as ev

    rows = _complete_cohort()
    probe = [row for row in rows if row["decision"] == "SMALL_PROBE"]
    for index, row in enumerate(probe[:5]):
        row["price_observations"] = {}
        row["outcome"] = {
            "horizons": {}, "unavailable_horizons": ["24h"],
        }
    probe[5]["outcome"]["horizons"]["24h"]["net_return_pct_est"] = 999_999

    got = ev.launch_forward_validation(rows)

    assert got["state"] == "coverage_blocked"
    assert got["edge_verdict"] == "不可判"
    assert got["required_outcome_coverage"] == 1.0
    assert got["arms"]["SMALL_PROBE"]["unavailable_n"] == 5
    assert got["arms"]["SMALL_PROBE"]["invalid_n"] == 1
    assert got["arms"]["SMALL_PROBE"]["coverage"] == 0.94
    assert "spa_pvalues" not in got


def test_union_calendar_and_daily_buckets_use_entry_anchor():
    from src.pipeline import edge_validation as ev

    rows = _complete_cohort()
    boundary = datetime.fromisoformat(ev.PROTOCOL_START_AT)
    for row in rows:
        detected = max(
            boundary,
            datetime.fromisoformat(row["decision_at"]) - timedelta(minutes=1),
        )
        row["detected_at"] = detected.isoformat()
        source = row["entry_observation"]["source_snapshot"]
        source["detected_at"] = source["captured_at"] = detected.isoformat()
        source["reconciliation_proof"]["live_captured_at"] = detected.isoformat()
        row["_expected_reconciliation_proof"] = deepcopy(
            source["reconciliation_proof"]
        )
        price = row["price_observations"]["24h"]["price"]
        stored = _stored_observation(row, price)
        row["price_observations"]["24h"] = stored
        row["outcome"]["horizons"]["24h"]["price_observation_id"] = \
            stored["observation_id"]

    got = ev.launch_forward_validation(rows)

    assert got["shared_days"] == 20
    assert got["calendar_days"] == 20
    assert got["time_anchor_policy"] == "entry_observation_utc"
    assert got["state"] == "pass"


def test_nonshared_bad_days_stay_in_union_calendar_and_cannot_manufacture_pass():
    from src.pipeline import edge_validation as ev

    rows = []
    for index in range(80):
        rows.append(_row(
            index, "SMALL_PROBE", 12.0 + (index % 3) * 0.1, day=index % 20,
        ))
        rows.append(_row(
            index, "WATCH", -4.0 + (index % 3) * 0.1, day=index % 20,
        ))
    for index in range(20):
        rows.append(_row(80 + index, "SMALL_PROBE", -99.0, day=20 + index))
        rows.append(_row(80 + index, "WATCH", -4.0, day=40 + index))

    got = ev.launch_forward_validation(rows)

    assert got["shared_days"] == 20
    assert got["calendar_days"] == 60
    assert got["shared_event_fraction"] == {
        "SMALL_PROBE": 0.8, "WATCH": 0.8,
    }
    assert got["calendar_policy"] == "continuous_utc_calendar_absent_arm_is_cash"
    assert got["mean_event_log_utility"]["SMALL_PROBE"] < 0
    assert got["state"] == "no_edge_observed"
    assert "spa_pvalues" not in got


def test_strong_spa_pass_remains_paper_selector_only_and_never_authorizes_execution():
    from src.pipeline import edge_validation as ev

    got = ev.launch_forward_validation(_complete_cohort())

    assert got["state"] == "pass"
    assert got["edge_verdict"] == "有前向纸面selector edge迹象"
    assert got["look_n_per_arm"] == 100
    assert got["spa_pvalue_used"] == "upper"
    assert got["spa_pvalues"]["upper"] <= ev.LOOK_ALPHA
    assert got["mean_daily_log_utility_lift"] >= ev.MIN_MEAN_UTILITY_LIFT
    assert got["sample_kind"] == "forward_paper_selector"
    assert got["selection_stage"] == "discovery_rule_before_security_and_route"
    assert got["cost_is_real_fill"] is False
    assert got["real_edge_n"] == 0
    assert got["real_edge_eligible"] is False
    assert got["execution_edge_eligible"] is False
    assert got["auto_execution_allowed"] is False
    assert "append_only" in got["price_evidence_policy"]
    assert "complete_paper" in got["cost_evidence_policy"]


def test_collecting_result_exposes_exclusions_and_all_fixed_safety_fields():
    from src.pipeline import edge_validation as ev

    rows = [_row(index, "SMALL_PROBE", 5.0) for index in range(3)]
    legacy = _row(9, "WATCH", 5.0)
    legacy["cohort_version"] = 5
    before = datetime.fromisoformat(ev.PROTOCOL_START_AT) - timedelta(seconds=1)
    legacy["detected_at"] = legacy["decision_at"] = before.isoformat()
    legacy["entry_observation"]["observed_at"] = before.isoformat()
    rows.append(legacy)

    got = ev.launch_forward_validation(rows)

    assert got["state"] == "collecting"
    assert got["eligible_n"] == {"SMALL_PROBE": 3, "WATCH": 0}
    assert got["excluded_n"] == 1
    assert got["excluded_by_reason"]["cohort_version_mismatch"] == 1
    assert got["edge_verdict"] == "不可判"
    assert got["real_edge_n"] == 0
    assert got["execution_edge_eligible"] is False
    assert got["auto_execution_allowed"] is False


def test_post_boundary_bad_snapshot_blocks_protocol_instead_of_shrinking_denominator():
    from src.pipeline import edge_validation as ev

    rows = _complete_cohort()
    rows[0]["cost_model"] = "friendlier_after_result"

    got = ev.launch_forward_validation(rows)

    assert got["state"] == "protocol_integrity_blocked"
    assert got["integrity_invalid_n"] == 1
    assert got["integrity_invalid_by_reason"]["row_cost_model_mismatch"] == 1
    assert got["edge_verdict"] == "不可判"
    assert got["auto_execution_allowed"] is False


def test_preboundary_quarantine_is_descriptive_not_a_protocol_integrity_failure():
    from src.pipeline import edge_validation as ev

    row = _row(0, "SMALL_PROBE", 5.0)
    before = datetime.fromisoformat(ev.PROTOCOL_START_AT) - timedelta(seconds=1)
    row["detected_at"] = row["decision_at"] = before.isoformat()
    row["entry_observation"]["observed_at"] = before.isoformat()

    got = ev.launch_forward_validation([row])

    assert got["state"] == "collecting"
    assert got["integrity_invalid_n"] == 0
    assert got["excluded_n"] == 1
    assert got["excluded_by_reason"]["detected_before_protocol_start"] == 1


def test_duplicate_event_id_blocks_protocol_integrity():
    from src.pipeline import edge_validation as ev

    row = _row(0, "SMALL_PROBE", 5.0)
    duplicate = deepcopy(row)
    duplicate["entry_observation"]["observed_at"] = (
        datetime.fromisoformat(duplicate["entry_observation"]["observed_at"])
        + timedelta(minutes=1)
    ).isoformat()
    duplicate["decision_at"] = duplicate["entry_observation"]["observed_at"]
    duplicate["detected_at"] = duplicate["decision_at"]
    source = duplicate["entry_observation"]["source_snapshot"]
    source["detected_at"] = source["captured_at"] = duplicate["detected_at"]
    source["reconciled_at"] = duplicate["decision_at"]
    source["reconciliation_proof"]["live_captured_at"] = duplicate["detected_at"]
    source["reconciliation_proof"]["checked_at"] = duplicate["decision_at"]
    duplicate["_expected_reconciliation_proof"] = deepcopy(
        source["reconciliation_proof"]
    )
    duplicate["created_at"] = (
        datetime.fromisoformat(duplicate["decision_at"]) + timedelta(seconds=1)
    ).isoformat()
    duplicate["outcome"] = {"horizons": {}}
    duplicate["price_observations"] = {}

    got = ev.launch_forward_validation([row, duplicate])

    assert got["state"] == "protocol_integrity_blocked"
    assert got["integrity_invalid_by_reason"] == {"duplicate_event_id": 1}


def test_source_invalidation_after_append_blocks_protocol_instead_of_dropping_row():
    from src.pipeline import edge_validation as ev

    row = _row(0, "SMALL_PROBE", 5.0)
    row["state"] = "invalidated"

    got = ev.launch_forward_validation([row])

    assert got["state"] == "protocol_integrity_blocked"
    assert got["integrity_invalid_n"] == 1
    assert got["integrity_invalid_by_reason"] == {"source_event_invalidated": 1}


def test_continuous_calendar_keeps_empty_utc_days_as_cash():
    from src.pipeline import edge_validation as ev

    rows = []
    for index in range(100):
        # Fourteen shared active days spread over 27 true calendar days.
        day = (index % 14) * 2
        rows.append(_row(index, "SMALL_PROBE", 12.0, day=day))
        rows.append(_row(index, "WATCH", -4.0, day=day))

    got = ev.launch_forward_validation(rows)

    assert got["shared_days"] == 14
    assert got["calendar_days"] == 27
    assert got["calendar_policy"] == "continuous_utc_calendar_absent_arm_is_cash"


def test_nonfinite_spa_pvalues_fail_closed(monkeypatch):
    import arch.bootstrap
    from src.pipeline import edge_validation as ev

    class NonFiniteSpa:
        def __init__(self, *_args, **_kwargs):
            self.pvalues = {"lower": 0.0, "consistent": 0.0, "upper": float("nan")}

        def compute(self):
            return None

    monkeypatch.setattr(arch.bootstrap, "SPA", NonFiniteSpa)

    got = ev.launch_forward_validation(_complete_cohort())

    assert got["state"] == "validator_unavailable"
    assert got["edge_verdict"] == "不可判"
    assert "invalid p-values" in got["reason"]
    assert got["auto_execution_allowed"] is False
