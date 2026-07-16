"""Execution measurements are append-only and separate from discovery snapshots."""
from datetime import datetime, timedelta, timezone

import pytest


@pytest.fixture(autouse=True)
def _explicit_test_source_authorities(monkeypatch):
    """Unit rows use deterministic authorities; SQLite integration is separate."""
    from src.pipeline import edge_validation as ev

    monkeypatch.setattr(
        ev, "_candidate_source_proof",
        lambda _row, snapshot: dict(snapshot["reconciliation_proof"]),
    )
    monkeypatch.setattr(
        ev, "_protocol_admission_state",
        lambda: {"state": "open", "enrollment_open": True, "reason_codes": []},
    )


def _freeze_test_source_snapshot(*, detected_at: str, decision_at: str) -> dict:
    from src.contract.launch_selector import freeze_source_snapshot

    proof = {
        "version": 1, "epoch_id": "1" * 32,
        "from_slot": 0, "to_slot": 1_000, "status": "sealed_clean",
        "checked_at": detected_at,
        "live_provider": "solana_rpc:live.example",
        "archive_provider": "solana_rpc:archive.example",
        "genesis_hash": "mainnet-genesis", "evidence_hash": "e" * 64,
        "finalized_head": 1_000, "live_captured_at": detected_at,
        "live_observation_hash": "a" * 64,
        "archive_observation_hash": "a" * 64,
        "hydration_identity_hash": "b" * 64,
    }
    return freeze_source_snapshot(
        signature="signature-token", slot=1, event_type="pump_fun_createv2",
        detected_at=detected_at, captured_at=detected_at,
        decision_at=decision_at, mint="token", raw_payload_hash="a" * 64,
        hydration_payload_hash="b" * 64, capture_mode="live_ws",
        source_provider="solana_rpc:live.example",
        reconciliation_state="verified_live", reconciled_at=detected_at,
        reconciliation_proof=proof,
    )


def _setup(tmp_path, monkeypatch, *, decision="WATCH", invalidation_price=0.7):
    from src.pipeline import opportunity_ledger as ledger
    from src.pipeline import edge_validation
    from src.contract.launch_selector import (
        evaluate_selector_snapshot, freeze_selector_snapshot,
    )
    from src.pipeline.execution_cost import solana_launch_full_paper_contract

    monkeypatch.setattr(ledger, "DB", tmp_path / "ledger.db")
    at = datetime.now(timezone.utc).replace(microsecond=0)
    monkeypatch.setattr(
        edge_validation, "PROTOCOL_START_AT", (at - timedelta(hours=1)).isoformat()
    )
    event_at = at - timedelta(minutes=30)
    selector_snapshot = freeze_selector_snapshot(
        pool_created_at=event_at.isoformat(), liquidity_usd=8_000,
        fdv_usd=100_000, volume_m5_usd=500 if decision == "SMALL_PROBE" else 0,
        buys_m5=5 if decision == "SMALL_PROBE" else 1, sells_m5=2,
    )
    selector = evaluate_selector_snapshot(
        selector_snapshot, event_at=event_at.isoformat(), decision_at=at.isoformat(),
    )
    contract = solana_launch_full_paper_contract(
        notional_usd=selector["max_notional_usd"],
        modeled_route_roundtrip_pct=selector["modeled_route_roundtrip_pct"],
        method=edge_validation.LAUNCH_COST_METHOD,
    )
    detected_at = at.isoformat()
    ident, _ = ledger.record({
        "lane": "launch", "chain": "solana", "token": "token", "symbol": "T",
        "source": "Pump.fun standard logs + DEX Screener pool", "decision": decision,
        "state": "live", "entry_price": 1.0,
        "invalidation_price": invalidation_price, "liquidity_usd": 8_000,
        "max_notional_usd": selector["max_notional_usd"],
        "event_at": event_at.isoformat(),
        "detected_at": detected_at, "decision_at": detected_at,
        "entry_observation": {
            "version": 1, "provider": "dexscreener_token_pairs_v1",
            "observed_at": detected_at, "chain": "solana",
            "base_token": "token", "quote_token": "SOL", "pair": "pool",
            "price": 1.0, "currency": "usd", "field": "priceUsd",
            "identity_verified": True,
            "selector_snapshot": selector_snapshot,
            "source_snapshot": _freeze_test_source_snapshot(
                detected_at=detected_at, decision_at=detected_at,
            ),
        },
        "roundtrip_cost_pct_est": contract["all_in_total_pct"],
        "cost_model": edge_validation.LAUNCH_COST_METHOD, "cost_contract": contract,
        "cohort_version": edge_validation.COHORT_VERSION,
    })
    return ledger, ident


def _protocol_time() -> datetime:
    from src.pipeline.edge_validation import PROTOCOL_START_AT

    return datetime.fromisoformat(PROTOCOL_START_AT) + timedelta(hours=1)


def _assessment(at, **overrides):
    from src.pipeline.execution_cost import route_contract

    item = {
        "kind": "read_only_quote", "chain": "solana", "token": "token",
        "assessed_at": at.isoformat(),
        "security_state": "pass", "security_at": at.isoformat(),
        "security_expires_at": (at + timedelta(minutes=5)).isoformat(),
        "security_gate": {
            "state": "pass", "chain": "solana", "token": "token",
            "source": "GoPlus Solana + finalized Solana RPC",
            "checked_at": at.isoformat(),
            "hard_flags": [], "cautions": [], "unknown_fields": [],
            "providers": {
                "goplus": {"state": "pass", "source": "GoPlus Solana"},
                "solana_rpc": {
                    "state": "pass", "source": "Solana finalized getAccountInfo",
                },
            },
        },
        "route_state": "quoted", "quote_source": "Jupiter", "quote_mode": "keyless",
        "quote_at": at.isoformat(), "quote_expires_at": (at + timedelta(seconds=60)).isoformat(),
        "expires_at": (at + timedelta(seconds=60)).isoformat(), "notional_usd": 25,
        "entry_reference_price": 1.1, "invalidation_reference_price": 0.77,
        "roundtrip_back_usd": 24.5,
        "execution_probe": {
            "state": "quoted", "chain": "solana", "token": "token",
            "source": "Jupiter", "checked_at": at.isoformat(),
            "notional_usd": 25, "entry_reference_price": 1.1,
            "invalidation_reference_price": 0.77, "roundtrip_back_usd": 24.5,
            "roundtrip_loss_pct": 2.0, "network_fees_included": False,
            "read_only": True, "is_real_fill": False,
        },
        "cost_contract": route_contract(notional_usd=25, route_loss_pct=2.0,
                                        method="jupiter_worst_threshold_roundtrip"),
        "is_real_fill": False,
    }
    item.update(overrides)
    return item


def _passing_edge_gate(lane="launch"):
    from src.pipeline.edge_validation import PROTOCOL_ID

    return {
        "state": "pass", "lane": lane, "protocol_id": PROTOCOL_ID,
        "protocol_state": "pass", "cost_is_real_fill": False,
        "edge_verdict": "有前向纸面selector edge迹象", "minimum_n": 100,
        "measured_n": 200, "look_n_per_arm": 100,
        "sample_kind": "forward_paper_selector",
        "selection_stage": "discovery_rule_before_security_and_route",
        "real_edge_n": 0, "real_edge_eligible": False,
        "execution_edge_eligible": False, "auto_execution_allowed": False,
        "reason": "pre-registered forward look passed in test fixture",
    }


def test_two_quotes_are_retained_and_latest_is_selected(tmp_path, monkeypatch):
    ledger, ident = _setup(tmp_path, monkeypatch)
    first = datetime(2026, 7, 16, 12, tzinfo=timezone.utc)
    second = first + timedelta(minutes=1)

    first_id, inserted = ledger.append_execution_assessment(ident, _assessment(first))
    assert inserted is True
    second_id, inserted = ledger.append_execution_assessment(
        ident, _assessment(second, entry_reference_price=1.2))
    assert inserted is True and second_id != first_id
    latest = ledger.latest_execution_assessment(ident)
    assert latest["assessment_id"] == second_id
    assert latest["entry_reference_price"] == 1.2
    assert latest["cost_contract"]["all_in_total_pct"] is None

    c = ledger._conn()
    try:
        assert c.execute("SELECT COUNT(*) FROM execution_assessments").fetchone()[0] == 2
    finally:
        c.close()


def test_duplicate_assessment_is_idempotent(tmp_path, monkeypatch):
    ledger, ident = _setup(tmp_path, monkeypatch)
    at = _protocol_time()
    assessment = _assessment(at)
    first = ledger.append_execution_assessment(ident, assessment)
    second = ledger.append_execution_assessment(ident, assessment)
    assert first[0] == second[0]
    assert first[1] is True and second[1] is False


@pytest.mark.parametrize("value", [True, 1, "true"])
def test_assessment_rejects_any_attempt_to_enable_auto_execution(
        tmp_path, monkeypatch, value):
    ledger, ident = _setup(tmp_path, monkeypatch)
    at = datetime.now(timezone.utc)

    with pytest.raises(ValueError, match="cannot allow automatic execution"):
        ledger.append_execution_assessment(
            ident, _assessment(at, auto_execution_allowed=value)
        )


def test_assessment_table_rejects_mutation(tmp_path, monkeypatch):
    import sqlite3

    ledger, ident = _setup(tmp_path, monkeypatch)
    at = _protocol_time()
    assessment_id, _ = ledger.append_execution_assessment(ident, _assessment(at))
    c = ledger._conn()
    try:
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            c.execute("UPDATE execution_assessments SET route_state='fake' "
                      "WHERE assessment_id=?", (assessment_id,))
    finally:
        c.close()


def test_assessment_rejects_bad_clocks_notional_and_fill_claims(tmp_path, monkeypatch):
    ledger, ident = _setup(tmp_path, monkeypatch)
    at = datetime(2026, 7, 16, 12, tzinfo=timezone.utc)
    with pytest.raises(ValueError, match="timezone"):
        ledger.append_execution_assessment(
            ident, _assessment(at, assessed_at="2026-07-16T12:00:00"))
    with pytest.raises(ValueError, match="expiry"):
        ledger.append_execution_assessment(
            ident, _assessment(at, expires_at=(at - timedelta(seconds=1)).isoformat()))
    with pytest.raises(ValueError, match="notional disagrees"):
        ledger.append_execution_assessment(ident, _assessment(at, notional_usd=30))
    with pytest.raises(ValueError, match="real_fill"):
        ledger.append_execution_assessment(
            ident, _assessment(at, kind="real_fill", is_real_fill=False))


def test_legacy_event_has_no_invented_assessment(tmp_path, monkeypatch):
    ledger, ident = _setup(tmp_path, monkeypatch)
    assert ledger.latest_execution_assessment(ident) is None


def test_discovery_cost_contract_is_stored_on_immutable_row(tmp_path, monkeypatch):
    from src.pipeline import opportunity_ledger as ledger
    from src.pipeline.execution_cost import discovery_contract

    monkeypatch.setattr(ledger, "DB", tmp_path / "ledger.db")
    contract = discovery_contract(notional_usd=25, modeled_roundtrip_pct=1.2,
                                  method="constant_product_v1")
    ledger.record({"lane": "launch", "chain": "solana", "token": "token",
                   "symbol": "T", "decision": "WATCH", "state": "live",
                   "entry_price": 1.0, "max_notional_usd": 25,
                   "roundtrip_cost_pct_est": 1.2, "cost_contract": contract,
                   "cohort_version": 3})
    row = ledger.outcome_rows()[0]
    assert row["cohort_version"] == 3
    assert row["cost_contract_version"] == 1
    assert row["cost_contract"]["purpose"] == "discovery_outcome"
    assert ledger.active("launch")[0]["cost_contract"]["known_total_pct"] == 1.2


def test_fresh_partial_quote_resolves_to_paper_ready_not_actionable(tmp_path, monkeypatch):
    ledger, ident = _setup(tmp_path, monkeypatch)
    now = _protocol_time()
    ledger.append_execution_assessment(ident, _assessment(now))
    row = ledger.active("launch", now=now)[0]
    assert row["action_level"] == "A2_PAPER_READY"
    assert row["actionable_now"] is False
    assert row["auto_execution_allowed"] is False
    assert "all_in_cost_incomplete" in row["action_reason_codes"]
    assert row["current_assessment"]["entry_reference_price"] == 1.1
    assert row["current_assessment"]["security_gate"]["state"] == "pass"
    assert row["current_assessment"]["execution_probe"]["state"] == "quoted"
    assert row["current_assessment"]["execution_probe"]["roundtrip_loss_pct"] == 2.0


def test_expired_v6_quote_downgrades_immediately_to_watch(tmp_path, monkeypatch):
    ledger, ident = _setup(tmp_path, monkeypatch)
    at = _protocol_time()
    ledger.append_execution_assessment(ident, _assessment(at))
    row = ledger.active("launch", now=at + timedelta(seconds=61))[0]
    assert row["action_level"] == "A1_WATCH"
    assert "quote_clock_invalid" in row["action_reason_codes"]


def test_known_untradeable_reverse_route_is_blocked(tmp_path, monkeypatch):
    ledger, ident = _setup(tmp_path, monkeypatch)
    at = _protocol_time()
    ledger.append_execution_assessment(ident, _assessment(at, route_state="untradeable"))
    row = ledger.active("launch", now=at)[0]
    assert row["action_level"] == "A0_BLOCKED"
    assert row["effective_decision"] == "AVOID"


def test_manual_probe_stays_a2_until_quote_and_delivery_verifiers_exist(
        tmp_path, monkeypatch):
    from src.pipeline.execution_cost import route_contract
    from src.pipeline import opportunity_outcomes

    ledger, ident = _setup(tmp_path, monkeypatch, decision="SMALL_PROBE")
    at = _protocol_time()
    complete = route_contract(notional_usd=25, route_loss_pct=2.0,
                              network_fee_pct=0.02, method="complete_test")
    ledger.append_execution_assessment(ident, _assessment(
        at, cost_contract=complete, delivery_sla_state="pass"))
    monkeypatch.setattr(opportunity_outcomes, "actionability_gate",
                        lambda lane: _passing_edge_gate(lane))
    row = ledger.active("launch", now=at)[0]
    assert row["action_level"] == "A2_PAPER_READY"
    assert row["actionable_now"] is False
    assert row["auto_execution_allowed"] is False
    assert row["current_assessment"]["auto_execution_allowed"] is False
    assert "quote_not_promotable" in row["action_reason_codes"]
    assert "delivery_readback_verifier_unavailable" in row["action_reason_codes"]


@pytest.mark.parametrize(("case", "reason"), [
    ("long_quote_ttl", "quote_clock_invalid"),
    ("long_security_ttl", "security_clock_invalid"),
    ("paper_fill", "assessment_not_read_only_quote"),
    ("missing_quote_source", "quote_source_missing"),
])
def test_ledger_shared_a3_contract_downgrades_invalid_current_windows(
        tmp_path, monkeypatch, case, reason):
    from src.pipeline.execution_cost import route_contract
    from src.pipeline import opportunity_outcomes

    ledger, ident = _setup(tmp_path, monkeypatch, decision="SMALL_PROBE")
    at = _protocol_time()
    assessment = _assessment(
        at,
        cost_contract=route_contract(
            notional_usd=25, route_loss_pct=2.0, network_fee_pct=0.02,
            method="complete_shared_contract_test",
        ),
        delivery_sla_state="pass",
    )
    if case == "long_quote_ttl":
        assessment["quote_expires_at"] = assessment["expires_at"] = (
            at + timedelta(seconds=61)
        ).isoformat()
    elif case == "long_security_ttl":
        assessment["security_expires_at"] = (at + timedelta(seconds=301)).isoformat()
    elif case == "paper_fill":
        assessment["kind"] = "paper_fill"
    elif case == "missing_quote_source":
        assessment["quote_source"] = None
    ledger.append_execution_assessment(ident, assessment)
    monkeypatch.setattr(
        opportunity_outcomes, "actionability_gate",
        lambda lane: _passing_edge_gate(lane),
    )

    row = ledger.active("launch", now=at)[0]

    assert row["action_level"] == "A1_WATCH"
    assert row["actionable_now"] is False
    assert reason in row["action_reason_codes"]


def test_ledger_shared_a3_contract_rejects_missing_public_plan_fields(
        tmp_path, monkeypatch):
    from src.pipeline.execution_cost import route_contract
    from src.pipeline import opportunity_outcomes

    ledger, ident = _setup(
        tmp_path, monkeypatch, decision="SMALL_PROBE", invalidation_price=None,
    )
    at = _protocol_time()
    ledger.append_execution_assessment(
        ident,
        _assessment(
            at,
            cost_contract=route_contract(
                notional_usd=25, route_loss_pct=2.0, network_fee_pct=0.02,
                method="complete_missing_plan_test",
            ),
            delivery_sla_state="pass",
        ),
    )
    monkeypatch.setattr(
        opportunity_outcomes, "actionability_gate",
        lambda lane: _passing_edge_gate(lane),
    )

    row = ledger.active("launch", now=at)[0]

    assert row["action_level"] == "A2_PAPER_READY"
    assert row["actionable_now"] is False
    assert "discovery_invalidation_invalid" in row["action_reason_codes"]


def test_real_ledger_a3_crosses_the_public_board_contract(tmp_path, monkeypatch):
    from src.pipeline.execution_cost import route_contract
    from src.pipeline import board_export, edge_validation, opportunity_outcomes

    at = datetime.now(timezone.utc).replace(microsecond=0)
    monkeypatch.setattr(
        edge_validation, "PROTOCOL_START_AT", (at - timedelta(hours=1)).isoformat()
    )
    ledger, ident = _setup(tmp_path, monkeypatch, decision="SMALL_PROBE")
    complete = route_contract(
        notional_usd=25, route_loss_pct=2.0, network_fee_pct=0.02,
        method="complete_public_contract_test",
    )
    ledger.append_execution_assessment(
        ident, _assessment(at, cost_contract=complete, delivery_sla_state="pass")
    )
    monkeypatch.setattr(
        opportunity_outcomes, "actionability_gate",
        lambda lane: _passing_edge_gate(lane),
    )
    row = ledger.active("launch", now=at)[0]
    monkeypatch.setattr(board_export, "EXPORT_DIR", tmp_path / "board")

    paths = board_export.write_views(
        launch=board_export._envelope({"events": [row]}, view="launch")
    )

    assert {path.name for path in paths} == {"launch.json", "meta.json"}
