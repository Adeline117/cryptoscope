"""Public board writes fail closed before replacing known-good JSON."""
from __future__ import annotations

import json
from copy import deepcopy
from datetime import datetime, timedelta, timezone

import pytest


def _launch_event(**overrides):
    event = {
        "id": "launch-1", "lane": "launch", "action_level": "A1_WATCH",
        "actionable_now": False, "auto_execution_allowed": False,
        "effective_decision": "WATCH",
    }
    event.update(overrides)
    return event


def _source_readiness(*, ready: bool = False) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    live_provider = "solana_rpc:live.example"
    archive_provider = "solana_rpc:archive.example" if ready else None
    epoch = ({
        "epoch_id": "1" * 32, "from_slot": 100, "to_slot": 103,
        "status": "sealed_clean", "live_provider": live_provider,
        "archive_provider": archive_provider, "checked_at": now,
        "missing_live": 0, "extra_live": 0, "finalized_head": 103,
    } if ready else None)
    stream = {"status": "live", "open_gaps": 0}
    return {
        "state": "ready" if ready else "blocked", "ready": ready,
        "live_provider": live_provider, "archive_provider": archive_provider,
        "required_clean_epochs": 1_440, "observed_epochs": 1_440 if ready else 0,
        "max_age_seconds": 300.0, "latest_age_seconds": 0.0 if ready else None,
        "max_finalized_lag_slots": 256,
        "latest_sealed_lag_slots": 0 if ready else None,
        "latest_runtime_lag_slots": 0 if ready else None,
        "latest_epoch": epoch,
        "runtime": {
            "live": stream if ready else None,
            "maintenance": stream if ready else None,
        },
        "reason_codes": [] if ready else ["archive_provider_not_configured"],
    }


def _protocol_admission(*, state: str = "scheduled") -> dict:
    from src.contract.launch_protocol import (
        COHORT_VERSION, PROTOCOL_ID, PROTOCOL_START_AT,
    )

    now = datetime.now(timezone.utc).isoformat()
    return {
        "protocol_id": PROTOCOL_ID, "cohort_version": COHORT_VERSION,
        "protocol_start_at": PROTOCOL_START_AT, "state": state,
        "enrollment_open": state == "open",
        "armed_at": now if state in {"armed", "open"} else None,
        "opened_at": now if state == "open" else None,
        "breached_at": now if state == "breached" else None,
        "reason_codes": [] if state == "open" else [
            "source_readiness_breached_after_open" if state == "breached"
            else "protocol_start_not_reached"
        ],
        "readiness_hash": "a" * 64, "created_at": now, "updated_at": now,
        "auto_execution_allowed": False,
    }


def _launch_body(events: list[dict], *, ready: bool = False,
                 admission_state: str = "scheduled") -> dict:
    from src.pipeline.launch_radar import _public_research_protocol

    readiness = _source_readiness(ready=ready)
    admission = _protocol_admission(state=admission_state)
    return {
        "events": events,
        "research_protocol": _public_research_protocol(readiness, admission),
        "primary_sources": {
            "solana": {
                "available": True, "source_readiness": readiness,
                "protocol_admission": admission,
            },
            "evm": {"available": True, "streams": []},
        },
    }


def _open_launch_body(events: list[dict]) -> dict:
    return _launch_body(events, ready=True, admission_state="open")


def _a3_event(**overrides):
    from src.contract.launch_selector import (
        evaluate_selector_snapshot, freeze_selector_snapshot, freeze_source_snapshot,
    )
    from src.pipeline.edge_validation import (
        COHORT_VERSION, LAUNCH_COST_METHOD, PROTOCOL_ID, PROTOCOL_START_AT,
    )
    from src.pipeline.execution_cost import (
        route_contract,
        solana_launch_full_paper_contract,
    )

    now = datetime.now(timezone.utc)
    assessment = {
        "assessment_id": "assessment-1", "kind": "read_only_quote",
        "opportunity_id": "launch-1", "chain": "solana", "token": "Mint111",
        "assessed_at": now.isoformat(),
        "security_state": "pass", "security_at": now.isoformat(),
        "security_expires_at": (now + timedelta(minutes=5)).isoformat(),
        "security_gate": {
            "state": "pass", "checked_at": now.isoformat(),
            "expires_at": (now + timedelta(minutes=5)).isoformat(),
            "hard_flags": [], "cautions": [], "unknown_fields": [],
            "chain": "solana", "token": "Mint111",
            "source": "GoPlus Solana + finalized Solana RPC",
            "providers": {
                "goplus": {"state": "pass", "source": "GoPlus Solana"},
                "solana_rpc": {
                    "state": "pass", "source": "Solana finalized getAccountInfo",
                },
            },
        },
        "route_state": "quoted", "quote_source": "Jupiter Swap v2 order",
        "quote_mode": "keyed_v2", "quote_at": now.isoformat(),
        "quote_expires_at": (now + timedelta(minutes=1)).isoformat(),
        "expires_at": (now + timedelta(minutes=1)).isoformat(),
        "notional_usd": 25.0, "entry_reference_price": 1.1,
        "invalidation_reference_price": 0.77, "roundtrip_back_usd": 24.55,
        "execution_probe": {
            "state": "quoted", "source": "Jupiter Swap v2 order",
            "api_mode": "keyed_v2", "promotion_eligible": True,
            "quote_contract_verified": True,
            "provider_contract": {
                "version": 1, "provider": "jupiter", "api_version": "v2",
                "operation": "order",
                "endpoint": "https://api.jup.ag/swap/v2/order",
                "auth_mode": "x_api_key", "slippage_bps": 100,
                "swap_mode": "ExactIn", "read_only": True,
                "taker_supplied": False, "transaction_built": False,
            },
            "checked_at": now.isoformat(),
            "chain": "solana", "token": "Mint111",
            "read_only": True, "is_real_fill": False, "network_fees_included": True,
            "notional_usd": 25.0, "roundtrip_loss_pct": 1.8,
            "roundtrip_back_usd": 24.55,
            "entry_reference_price": 1.1, "invalidation_reference_price": 0.77,
        },
        "cost_contract": route_contract(
            notional_usd=25, route_loss_pct=1.8, network_fee_pct=0.02,
            method="complete_board_contract_test",
        ),
        "delivery_sla_state": "pass", "is_real_fill": False,
        "auto_execution_allowed": False,
    }
    detected = datetime.fromisoformat(PROTOCOL_START_AT) + timedelta(seconds=1)
    detected_at = detected.isoformat()
    event_at = detected - timedelta(minutes=30)
    selector_snapshot = freeze_selector_snapshot(
        pool_created_at=event_at.isoformat(), liquidity_usd=8_000,
        fdv_usd=100_000, volume_m5_usd=500, buys_m5=5, sells_m5=2,
    )
    selector = evaluate_selector_snapshot(
        selector_snapshot,
        event_at=event_at.isoformat(),
        decision_at=detected_at,
    )
    discovery_cost = solana_launch_full_paper_contract(
        notional_usd=selector["max_notional_usd"],
        modeled_route_roundtrip_pct=selector["modeled_route_roundtrip_pct"],
        method=LAUNCH_COST_METHOD,
    )
    event = _launch_event(
        chain="solana", token="Mint111", symbol="T",
        source="Pump.fun standard logs + DEX Screener pool",
        state="live", outcome_state="open", is_expired=False,
        action_level="A3_MANUAL_PROBE", actionable_now=True,
        effective_decision="SMALL_PROBE", decision="SMALL_PROBE",
        recorded_decision="SMALL_PROBE", entry_price=1.0,
        invalidation_price=0.7, liquidity_usd=8_000.0, max_notional_usd=25.0,
        event_at=event_at.isoformat(), detected_at=detected_at,
        decision_at=detected_at,
        created_at=(detected + timedelta(seconds=1)).isoformat(),
        entry_observation={
            "version": 1, "provider": "dexscreener_token_pairs_v1",
            "observed_at": detected_at, "chain": "solana",
            "base_token": "Mint111", "quote_token": "SOL", "pair": "pool",
            "price": 1.0, "currency": "usd", "field": "priceUsd",
            "identity_verified": True,
            "selector_snapshot": selector_snapshot,
            "source_snapshot": freeze_source_snapshot(
                signature="signature-Mint111", slot=1,
                event_type="pump_fun_createv2", detected_at=detected_at,
                captured_at=detected_at, decision_at=detected_at,
                mint="Mint111", raw_payload_hash="a" * 64,
                hydration_payload_hash="b" * 64, capture_mode="live_ws",
                source_provider="solana_rpc:live.example",
                reconciliation_state="verified_live", reconciled_at=detected_at,
                reconciliation_proof={
                    "version": 1, "epoch_id": "1" * 32,
                    "from_slot": 0, "to_slot": 100, "status": "sealed_clean",
                    "checked_at": detected_at,
                    "live_provider": "solana_rpc:live.example",
                    "archive_provider": "solana_rpc:archive.example",
                    "genesis_hash": "mainnet-genesis", "evidence_hash": "e" * 64,
                    "finalized_head": 100, "live_captured_at": detected_at,
                    "live_observation_hash": "a" * 64,
                    "archive_observation_hash": "a" * 64,
                    "hydration_identity_hash": "b" * 64,
                },
            ),
        },
        cohort_version=COHORT_VERSION, cost_contract_version=1,
        entry_observation_version=1,
        cost_pct_est=discovery_cost["all_in_total_pct"],
        cost_model=LAUNCH_COST_METHOD, cost_contract=discovery_cost,
        evidence_gate={
            "state": "pass", "lane": "launch", "protocol_id": PROTOCOL_ID,
            "protocol_state": "pass", "cost_is_real_fill": False,
            "edge_verdict": "有前向纸面selector edge迹象", "minimum_n": 100,
            "measured_n": 200, "look_n_per_arm": 100,
            "sample_kind": "forward_paper_selector",
            "selection_stage": "discovery_rule_before_security_and_route",
            "real_edge_n": 0, "real_edge_eligible": False,
            "execution_edge_eligible": False, "auto_execution_allowed": False,
            "reason": "pre-registered forward look passed in contract fixture",
        },
        current_assessment=assessment,
    )
    event.update(overrides)
    return event


def _enable_started_protocol(monkeypatch) -> None:
    from src.contract import launch_protocol
    from src.pipeline import edge_validation, launch_radar

    boundary = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    monkeypatch.setattr(launch_protocol, "PROTOCOL_START_AT", boundary)
    monkeypatch.setattr(edge_validation, "PROTOCOL_START_AT", boundary)
    monkeypatch.setattr(launch_radar, "PROTOCOL_START_AT", boundary)


def _view(board_export, view, body):
    return board_export._envelope(body, view=view)


def test_wrong_view_name_rejects_write_and_preserves_existing_files(tmp_path, monkeypatch):
    from src.pipeline import board_export

    monkeypatch.setattr(board_export, "EXPORT_DIR", tmp_path)
    good = _view(board_export, "launch", _launch_body([_launch_event()]))
    board_export.write_views(launch=good)
    before_launch = (tmp_path / "launch.json").read_bytes()
    before_meta = (tmp_path / "meta.json").read_bytes()
    wrong = {**good, "view": "structure"}

    with pytest.raises(ValueError, match="view name mismatch"):
        board_export.write_views(launch=wrong)

    assert (tmp_path / "launch.json").read_bytes() == before_launch
    assert (tmp_path / "meta.json").read_bytes() == before_meta
    assert not list(tmp_path.glob("*.tmp"))


def test_batch_preflight_rejects_nan_without_partial_update(tmp_path, monkeypatch):
    from src.pipeline import board_export

    monkeypatch.setattr(board_export, "EXPORT_DIR", tmp_path)
    old_launch = _view(board_export, "launch", _launch_body([_launch_event()]))
    old_structure = _view(board_export, "structure", {
        "events": [], "product_metadata_at": None,
        "product_metadata_time_semantics": (
            "current_inventory_metadata_not_event_time_evidence"
        ),
    })
    board_export.write_views(launch=old_launch, structure=old_structure)
    before = {path.name: path.read_bytes() for path in tmp_path.glob("*.json")}

    new_launch = _view(
        board_export, "launch", _launch_body([_launch_event(symbol="NEW")]),
    )
    bad_structure = _view(board_export, "structure", {
        "events": [], "coverage_ratio": float("nan"), "product_metadata_at": None,
        "product_metadata_time_semantics": (
            "current_inventory_metadata_not_event_time_evidence"
        ),
    })
    with pytest.raises(ValueError, match="Out of range float values"):
        board_export.write_views(launch=new_launch, structure=bad_structure)

    assert {path.name: path.read_bytes() for path in tmp_path.glob("*.json")} == before
    assert not list(tmp_path.glob("*.tmp"))


def test_future_generated_clock_cannot_make_quotes_live_for_years(tmp_path, monkeypatch):
    from src.pipeline import board_export

    monkeypatch.setattr(board_export, "EXPORT_DIR", tmp_path)
    payload = _view(board_export, "launch", _launch_body([]))
    generated = datetime(2100, 1, 1, tzinfo=timezone.utc)
    cadence = timedelta(minutes=payload["refresh_cadence_min"])
    grace = timedelta(minutes=payload["freshness_grace_min"])
    payload.update({
        "generated_at": generated.isoformat(),
        "next_expected_at": (generated + cadence).isoformat(),
        "stale_after_at": (generated + cadence + grace).isoformat(),
    })

    with pytest.raises(ValueError, match="wall clock"):
        board_export.write_views(launch=payload)

    assert not (tmp_path / "launch.json").exists()


def test_launch_context_is_mandatory_and_preserves_last_good(tmp_path, monkeypatch):
    from src.pipeline import board_export

    monkeypatch.setattr(board_export, "EXPORT_DIR", tmp_path)
    good = _view(board_export, "launch", _launch_body([_launch_event()]))
    board_export.write_views(launch=good)
    before = (tmp_path / "launch.json").read_bytes()
    before_meta = (tmp_path / "meta.json").read_bytes()

    bad_bodies = [
        {"events": [_launch_event()]},
        {**_launch_body([_launch_event()]), "research_protocol": None},
        {
            **_launch_body([_launch_event()]),
            "primary_sources": {"solana": {"protocol_admission": _protocol_admission()}},
        },
        {
            **_launch_body([_launch_event()]),
            "primary_sources": {"solana": {"source_readiness": _source_readiness()}},
        },
    ]
    for body in bad_bodies:
        with pytest.raises(ValueError):
            board_export.write_views(launch=_view(board_export, "launch", body))
        assert (tmp_path / "launch.json").read_bytes() == before
        assert (tmp_path / "meta.json").read_bytes() == before_meta


def test_open_launch_context_rejects_every_readiness_and_admission_contradiction(
        tmp_path, monkeypatch):
    from src.pipeline import board_export

    monkeypatch.setattr(board_export, "EXPORT_DIR", tmp_path)
    _enable_started_protocol(monkeypatch)
    good = _view(board_export, "launch", _open_launch_body([_launch_event()]))
    board_export.write_views(launch=good)
    before = (tmp_path / "launch.json").read_bytes()

    def readiness(body):
        return body["primary_sources"]["solana"]["source_readiness"]

    def admission(body):
        return body["primary_sources"]["solana"]["protocol_admission"]

    mutators = (
        lambda body: readiness(body).update(ready=False),
        lambda body: readiness(body)["reason_codes"].append("hidden_block"),
        lambda body: readiness(body).update(observed_epochs=1),
        lambda body: readiness(body).update(latest_age_seconds=301),
        lambda body: readiness(body).update(latest_runtime_lag_slots=257),
        lambda body: readiness(body)["latest_epoch"].update(status="sealed_breached"),
        lambda body: readiness(body)["latest_epoch"].update(missing_live=1),
        lambda body: readiness(body)["runtime"]["live"].update(open_gaps=1),
        lambda body: readiness(body).update(
            archive_provider="solana_rpc:live.example:9999"
        ),
        lambda body: admission(body).update(protocol_id="retuned-protocol"),
        lambda body: admission(body).update(enrollment_open=False),
        lambda body: admission(body).update(auto_execution_allowed=True),
        lambda body: body["research_protocol"].update(enrollment_open=False),
        lambda body: body["research_protocol"].update(enrollment_state="blocked"),
    )
    for mutate in mutators:
        bad = deepcopy(good)
        mutate(bad)
        with pytest.raises(ValueError):
            board_export.write_views(launch=bad)
        assert (tmp_path / "launch.json").read_bytes() == before


def test_blocked_protocol_never_publishes_forged_manual_window(tmp_path, monkeypatch):
    from src.pipeline import board_export

    monkeypatch.setattr(board_export, "EXPORT_DIR", tmp_path)
    forged = _launch_event(
        action_level="A3_MANUAL_PROBE", actionable_now=True,
        effective_decision="SMALL_PROBE",
    )
    with pytest.raises(ValueError, match="enrollment is blocked"):
        board_export.write_views(
            launch=_view(board_export, "launch", _launch_body([forged]))
        )
    assert not (tmp_path / "launch.json").exists()


def test_a3_expiring_during_render_cannot_cross_the_write_boundary(
        tmp_path, monkeypatch):
    from src.pipeline import board_export

    monkeypatch.setattr(board_export, "EXPORT_DIR", tmp_path)
    _enable_started_protocol(monkeypatch)
    event = _a3_event()
    stale = datetime.now(timezone.utc) - timedelta(seconds=61)
    event["current_assessment"].update({
        "assessed_at": stale.isoformat(), "quote_at": stale.isoformat(),
        "quote_expires_at": (stale + timedelta(seconds=60)).isoformat(),
        "expires_at": (stale + timedelta(seconds=60)).isoformat(),
    })
    event["current_assessment"]["execution_probe"]["checked_at"] = stale.isoformat()

    with pytest.raises(ValueError, match="quote_clock_invalid"):
        board_export.write_views(
            launch=_view(board_export, "launch", _open_launch_body([event]))
        )

    assert not (tmp_path / "launch.json").exists()


@pytest.mark.parametrize("changes", [
    {"actionable_now": False},
    {"auto_execution_allowed": True},
    {"effective_decision": "WATCH"},
])
def test_false_a3_cannot_cross_public_boundary(tmp_path, monkeypatch, changes):
    from src.pipeline import board_export

    monkeypatch.setattr(board_export, "EXPORT_DIR", tmp_path)
    _enable_started_protocol(monkeypatch)
    event = _a3_event()
    event.update(changes)

    with pytest.raises(ValueError):
        board_export.write_views(
            launch=_view(board_export, "launch", _open_launch_body([event]))
        )
    assert not (tmp_path / "launch.json").exists()


def test_a3_cannot_cross_until_public_delivery_readback_exists(tmp_path, monkeypatch):
    from src.pipeline import board_export

    monkeypatch.setattr(board_export, "EXPORT_DIR", tmp_path)
    _enable_started_protocol(monkeypatch)
    event = _a3_event()

    with pytest.raises(ValueError, match="delivery_readback_missing"):
        board_export.write_views(
            launch=_view(board_export, "launch", _open_launch_body([event]))
        )

    assert not (tmp_path / "launch.json").exists()


@pytest.mark.parametrize("case", [
    "assessment_missing", "security_unknown", "route_unknown", "quote_clock_missing",
    "quote_expired", "security_clock_expired", "entry_missing", "invalidation_negative",
    "notional_zero", "partial_cost", "evidence_blocked", "delivery_unverified",
    "auto_execution_unspecified", "assessment_auto_execution", "public_entry_missing",
    "public_invalidation_missing", "public_notional_missing", "notional_above_cap",
    "cost_notional_mismatch", "future_quote", "future_security_check",
    "outside_protocol", "wrong_evidence_protocol", "stale_long_quote",
    "long_quote_ttl", "long_security_ttl", "assessment_not_read_only",
    "assessment_id_missing", "quote_source_missing", "public_security_missing",
    "public_route_missing", "short_side", "chain_missing", "source_unknown",
    "assessment_opportunity_mismatch", "assessment_clock_missing",
    "evidence_shape_incomplete", "security_hard_flag", "route_source_mismatch",
    "route_loss_negative", "route_clock_mismatch", "network_cost_missing",
    "route_cost_missing", "all_in_cost_above_limit", "discovery_notional_mismatch",
    "discovery_cost_total_mismatch",
    "unsupported_chain", "event_not_live", "outcome_invalidated", "absolute_cap",
    "security_reason", "route_reason", "nested_notional_mismatch",
    "quote_source_unknown", "current_method_missing", "discovery_real_fill",
    "discovery_cost_complete",
    "bsc_jupiter", "solana_zerox", "assessment_asset_mismatch",
    "security_source_missing", "security_provider_missing", "route_token_mismatch",
    "liquidity_missing", "cap_liquidity_mismatch", "quote_below_cap",
])
def test_incomplete_a3_never_replaces_last_known_good_view(
        tmp_path, monkeypatch, case):
    from src.pipeline import board_export
    from src.pipeline.execution_cost import route_contract

    monkeypatch.setattr(board_export, "EXPORT_DIR", tmp_path)
    _enable_started_protocol(monkeypatch)
    old = _view(
        board_export, "launch", _launch_body([_launch_event(symbol="KNOWN_GOOD")]),
    )
    board_export.write_views(launch=old)
    before = (tmp_path / "launch.json").read_bytes()
    before_meta = (tmp_path / "meta.json").read_bytes()
    event = deepcopy(_a3_event())
    assessment = event["current_assessment"]
    if case == "assessment_missing":
        event["current_assessment"] = None
    elif case == "security_unknown":
        assessment["security_state"] = "unknown"
    elif case == "route_unknown":
        assessment["route_state"] = "unknown"
    elif case == "quote_clock_missing":
        assessment["quote_at"] = None
    elif case == "quote_expired":
        assessment["expires_at"] = "2020-01-01T00:00:00+00:00"
    elif case == "security_clock_expired":
        assessment["security_expires_at"] = "2020-01-01T00:00:00+00:00"
    elif case == "entry_missing":
        assessment["entry_reference_price"] = None
    elif case == "invalidation_negative":
        assessment["invalidation_reference_price"] = -1
    elif case == "notional_zero":
        assessment["notional_usd"] = 0
    elif case == "partial_cost":
        assessment["cost_contract"] = route_contract(
            notional_usd=25, route_loss_pct=1.8, method="partial_board_contract_test"
        )
    elif case == "evidence_blocked":
        event["evidence_gate"] = {"state": "blocked"}
    elif case == "delivery_unverified":
        assessment["delivery_sla_state"] = "unverified"
    elif case == "auto_execution_unspecified":
        event["auto_execution_allowed"] = None
    elif case == "assessment_auto_execution":
        assessment["auto_execution_allowed"] = True
    elif case == "public_entry_missing":
        event["entry_price"] = None
    elif case == "public_invalidation_missing":
        event["invalidation_price"] = None
    elif case == "public_notional_missing":
        event["max_notional_usd"] = None
    elif case == "notional_above_cap":
        assessment["notional_usd"] = 50
        assessment["cost_contract"] = route_contract(
            notional_usd=50, route_loss_pct=1.8, network_fee_pct=0.02,
            method="oversize_board_contract_test",
        )
    elif case == "cost_notional_mismatch":
        assessment["notional_usd"] = 20
    elif case == "future_quote":
        assessment["quote_at"] = "2100-01-01T00:00:00+00:00"
    elif case == "future_security_check":
        assessment["security_at"] = "2100-01-01T00:00:00+00:00"
    elif case == "outside_protocol":
        event["cohort_version"] = 4
    elif case == "wrong_evidence_protocol":
        event["evidence_gate"]["protocol_id"] = "retuned-after-outcomes"
    elif case == "stale_long_quote":
        assessment["quote_at"] = "2020-01-01T00:00:00+00:00"
    elif case == "long_quote_ttl":
        expiry = datetime.now(timezone.utc) + timedelta(minutes=2)
        assessment["quote_expires_at"] = assessment["expires_at"] = expiry.isoformat()
    elif case == "long_security_ttl":
        assessment["security_expires_at"] = (
            datetime.now(timezone.utc) + timedelta(minutes=10)
        ).isoformat()
    elif case == "assessment_not_read_only":
        assessment["kind"] = "paper_fill"
    elif case == "assessment_id_missing":
        assessment["assessment_id"] = None
    elif case == "quote_source_missing":
        assessment["quote_source"] = None
    elif case == "public_security_missing":
        assessment["security_gate"] = None
    elif case == "public_route_missing":
        assessment["execution_probe"] = None
    elif case == "short_side":
        event["side"] = "SHORT"
    elif case == "chain_missing":
        event["chain"] = ""
    elif case == "source_unknown":
        event["source"] = "unknown"
    elif case == "assessment_opportunity_mismatch":
        assessment["opportunity_id"] = "another-launch-event"
    elif case == "assessment_clock_missing":
        assessment["assessed_at"] = None
    elif case == "evidence_shape_incomplete":
        event["evidence_gate"].pop("reason")
    elif case == "security_hard_flag":
        assessment["security_gate"]["hard_flags"] = ["is_honeypot"]
    elif case == "route_source_mismatch":
        assessment["execution_probe"]["source"] = "unrelated router"
    elif case == "route_loss_negative":
        assessment["execution_probe"]["roundtrip_loss_pct"] = -99
    elif case == "route_clock_mismatch":
        assessment["execution_probe"]["checked_at"] = "2100-01-01T00:00:00+00:00"
    elif case == "network_cost_missing":
        contract = assessment["cost_contract"]
        contract["components"] = [contract["components"][0]]
        contract["known_total_pct"] = contract["all_in_total_pct"] = 1.8
    elif case == "route_cost_missing":
        contract = assessment["cost_contract"]
        contract["components"] = [contract["components"][1]]
        contract["known_total_pct"] = contract["all_in_total_pct"] = 0.02
    elif case == "all_in_cost_above_limit":
        assessment["cost_contract"] = route_contract(
            notional_usd=25, route_loss_pct=5.0, network_fee_pct=0.02,
            method="over_limit_board_contract_test",
        )
    elif case == "discovery_notional_mismatch":
        from src.pipeline.edge_validation import LAUNCH_COST_METHOD
        from src.pipeline.execution_cost import discovery_contract

        event["cost_contract"] = discovery_contract(
            notional_usd=1, modeled_roundtrip_pct=1.2, method=LAUNCH_COST_METHOD,
        )
    elif case == "discovery_cost_total_mismatch":
        event["cost_pct_est"] = 9.9
    elif case == "unsupported_chain":
        event["chain"] = "mars"
    elif case == "event_not_live":
        event["state"] = "reorg_removed"
    elif case == "outcome_invalidated":
        event["outcome_state"] = "invalidated"
    elif case == "absolute_cap":
        event["max_notional_usd"] = assessment["notional_usd"] = 1_000
        assessment["roundtrip_back_usd"] = 982
        assessment["execution_probe"].update({
            "notional_usd": 1_000, "roundtrip_back_usd": 982,
        })
        assessment["cost_contract"] = route_contract(
            notional_usd=1_000, route_loss_pct=1.8, network_fee_pct=0.02,
            method="oversize_absolute_cap_test",
        )
        from src.pipeline.edge_validation import LAUNCH_COST_METHOD
        from src.pipeline.execution_cost import discovery_contract

        event["cost_contract"] = discovery_contract(
            notional_usd=1_000, modeled_roundtrip_pct=1.2, method=LAUNCH_COST_METHOD,
        )
    elif case == "security_reason":
        assessment["security_gate"]["reason"] = "honeypot detected"
    elif case == "route_reason":
        assessment["execution_probe"]["reason"] = "no sell route"
    elif case == "nested_notional_mismatch":
        assessment["execution_probe"]["notional_usd"] = 999
    elif case == "quote_source_unknown":
        assessment["quote_source"] = assessment["execution_probe"]["source"] = "unknown"
    elif case == "current_method_missing":
        assessment["cost_contract"]["method"] = ""
    elif case == "discovery_real_fill":
        event["cost_contract"]["is_real_fill"] = True
    elif case == "discovery_cost_complete":
        contract = event["cost_contract"]
        contract["components"][1] = {
            "name": "network_fee", "pct": 0.0, "status": "included",
        }
        contract["all_in_total_pct"] = contract["known_total_pct"] = 1.2
        contract["completeness"] = "complete"
    elif case == "bsc_jupiter":
        event["chain"] = assessment["chain"] = "bsc"
        assessment["security_gate"]["chain"] = "bsc"
        assessment["execution_probe"]["chain"] = "bsc"
    elif case == "solana_zerox":
        assessment["quote_source"] = assessment["execution_probe"]["source"] = (
            "0x indicative price v2"
        )
    elif case == "assessment_asset_mismatch":
        assessment["token"] = "AnotherMint"
    elif case == "security_source_missing":
        assessment["security_gate"]["source"] = None
    elif case == "security_provider_missing":
        assessment["security_gate"]["providers"] = {}
    elif case == "route_token_mismatch":
        assessment["execution_probe"]["token"] = "AnotherMint"
    elif case == "liquidity_missing":
        event["liquidity_usd"] = None
    elif case == "cap_liquidity_mismatch":
        event["liquidity_usd"] = 100_000
    elif case == "quote_below_cap":
        assessment["notional_usd"] = 1
        assessment["roundtrip_back_usd"] = 0.982
        assessment["cost_contract"] = route_contract(
            notional_usd=1, route_loss_pct=1.8, network_fee_pct=0.02,
            method="undersized_quote_test",
        )
        assessment["execution_probe"].update({
            "notional_usd": 1, "roundtrip_back_usd": 0.982,
        })

    with pytest.raises(ValueError):
        board_export.write_views(
            launch=_view(board_export, "launch", _open_launch_body([event]))
        )

    assert (tmp_path / "launch.json").read_bytes() == before
    assert (tmp_path / "meta.json").read_bytes() == before_meta
    assert not list(tmp_path.glob("*.tmp"))


def test_fail_closed_launch_and_carry_views_remain_serializable(tmp_path, monkeypatch):
    from src.pipeline import board_export

    monkeypatch.setattr(board_export, "EXPORT_DIR", tmp_path)
    observed_at = datetime.now(timezone.utc).isoformat()
    launch = _view(board_export, "launch", _launch_body([_launch_event()]))
    perps = _view(board_export, "perps", {
        "perps": [], "carry": [], "cascade_events": [{
            "id": "cascade-1", "lane": "cascade", "chain": "hyperliquid",
            "token": "BTC", "symbol": "BTC", "source": "Hyperliquid",
            "detected_at": observed_at, "decision_at": observed_at,
            "event_at": observed_at, "direction": "down", "side": "SHORT",
            "actionable_now": False,
            "effective_decision": "WATCH", "auto_execution_allowed": False,
        }],
    })

    paths = board_export.write_views(launch=launch, perps=perps)

    assert {path.name for path in paths} == {"launch.json", "perps.json", "meta.json"}
    assert json.loads((tmp_path / "launch.json").read_text())["events"][0][
        "action_level"
    ] == "A1_WATCH"


def test_meta_protocol_join_ignores_non_safety_admission_churn(tmp_path, monkeypatch):
    from src.pipeline import board_export, opportunity_ledger

    monkeypatch.setattr(board_export, "EXPORT_DIR", tmp_path)
    monkeypatch.setattr(opportunity_ledger, "DB", tmp_path / "ledger.db")
    launch = _view(board_export, "launch", _launch_body([_launch_event()]))
    stats = board_export.render_stats(None)
    launch_admission = launch["primary_sources"]["solana"]["protocol_admission"]
    stats_admission = stats["lanes"]["launch"]["edge_validation"][
        "protocol_admission"
    ]
    # The gate state and immutable clocks are the safety truth. Observation clocks,
    # readiness hashes and explanatory reasons legitimately differ by render cadence.
    launch_admission.update({
        key: stats_admission.get(key) for key in (
            "state", "enrollment_open", "armed_at", "opened_at", "breached_at",
            "auto_execution_allowed",
        )
    })
    launch_admission.update({
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "readiness_hash": "b" * 64,
        "reason_codes": ["archive_provider_not_configured"],
    })
    launch["research_protocol"]["persistent_admission_state"] = stats_admission["state"]
    launch["research_protocol"]["enrollment_state"] = stats_admission["state"]
    launch["research_protocol"]["reason_codes"] = sorted(set(
        launch["research_protocol"]["reason_codes"]
        + launch_admission["reason_codes"]
    ))

    board_export.write_views(launch=launch, stats=stats)
    meta = json.loads((tmp_path / "meta.json").read_text())

    assert meta["launch_protocol_join"]["state"] == "consistent"
    assert meta["launch_protocol_join"]["cross_view_edge_usable"] is True


def test_single_view_protocol_transition_marks_sync_pending_then_recovers():
    from src.contract.board_view import launch_protocol_join

    scheduled = {
        "generated_at": "2026-07-16T00:00:00+00:00",
        **_launch_body([]),
    }
    stats_admission = deepcopy(scheduled["primary_sources"]["solana"]["protocol_admission"])
    stats_payload = {
        "generated_at": "2026-07-16T00:00:00+00:00",
        "lanes": {"launch": {"edge_validation": {
            **{field: scheduled["research_protocol"][field] for field in (
                "protocol_id", "cohort_version", "protocol_start_at",
            )},
            "protocol_admission": stats_admission,
        }}},
    }
    armed = deepcopy(scheduled)
    later = (datetime.fromisoformat(stats_admission["updated_at"])
             + timedelta(minutes=1)).isoformat()
    armed["generated_at"] = later
    armed_admission = armed["primary_sources"]["solana"]["protocol_admission"]
    armed_admission.update({
        "state": "armed", "enrollment_open": False,
        "armed_at": later, "updated_at": later,
    })

    pending = launch_protocol_join(armed, stats_payload)
    assert pending["state"] == "sync_pending"
    assert pending["cross_view_edge_usable"] is False

    stats_payload["lanes"]["launch"]["edge_validation"][
        "protocol_admission"
    ] = deepcopy(armed_admission)
    recovered = launch_protocol_join(armed, stats_payload)
    assert recovered["state"] == "consistent"
    assert recovered["cross_view_edge_usable"] is True


@pytest.mark.parametrize("newer_view", ["launch", "stats"])
def test_prestart_armed_to_scheduled_transition_is_sync_pending(newer_view):
    from src.contract.board_view import launch_protocol_join

    older_clock = "2026-08-02T23:58:00+00:00"
    newer_clock = "2026-08-02T23:59:00+00:00"
    armed = _protocol_admission(state="armed")
    armed.update({"armed_at": older_clock, "updated_at": older_clock})
    scheduled = deepcopy(armed)
    scheduled.update({
        "state": "scheduled", "enrollment_open": False,
        "armed_at": None, "updated_at": newer_clock,
    })
    launch_admission = scheduled if newer_view == "launch" else armed
    stats_admission = scheduled if newer_view == "stats" else armed
    launch = {
        "generated_at": launch_admission["updated_at"],
        "research_protocol": {
            field: launch_admission[field] for field in (
                "protocol_id", "cohort_version", "protocol_start_at",
            )
        },
        "primary_sources": {"solana": {"protocol_admission": launch_admission}},
    }
    stats = {
        "generated_at": stats_admission["updated_at"],
        "lanes": {"launch": {"edge_validation": {
            **{field: stats_admission[field] for field in (
                "protocol_id", "cohort_version", "protocol_start_at",
            )},
            "protocol_admission": stats_admission,
        }}},
    }

    joined = launch_protocol_join(launch, stats)

    assert joined["state"] == "sync_pending"
    assert joined["reason_codes"] == ["admission_state_not_yet_joined"]
    assert joined["cross_view_edge_usable"] is False


@pytest.mark.parametrize(
    ("older_state", "newer_state", "newer_clock"),
    [
        ("armed", "scheduled", "2026-08-03T00:00:00+00:00"),
        ("open", "armed", "2026-08-03T00:02:00+00:00"),
        ("breached", "open", "2026-08-03T00:02:00+00:00"),
    ],
)
def test_terminal_or_post_boundary_admission_relaxation_is_contradiction(
        older_state, newer_state, newer_clock):
    from src.contract.board_view import launch_protocol_join

    older_clock = "2026-08-02T23:59:00+00:00"
    older = _protocol_admission(state=older_state)
    newer = _protocol_admission(state=newer_state)
    older["updated_at"] = older_clock
    newer["updated_at"] = newer_clock
    launch = {
        "generated_at": older_clock,
        "research_protocol": {
            field: older[field] for field in (
                "protocol_id", "cohort_version", "protocol_start_at",
            )
        },
        "primary_sources": {"solana": {"protocol_admission": older}},
    }
    stats = {
        "generated_at": newer_clock,
        "lanes": {"launch": {"edge_validation": {
            **{field: newer[field] for field in (
                "protocol_id", "cohort_version", "protocol_start_at",
            )},
            "protocol_admission": newer,
        }}},
    }

    joined = launch_protocol_join(launch, stats)

    assert joined["state"] == "contradiction"
    assert joined["reason_codes"] == ["admission_state_regressed"]
    assert joined["cross_view_edge_usable"] is False


def test_same_batch_protocol_identity_mismatch_preserves_every_file(
        tmp_path, monkeypatch):
    from src.contract import board_view
    from src.pipeline import board_export

    monkeypatch.setattr(board_export, "EXPORT_DIR", tmp_path)
    old = {"keep": "launch"}
    old_stats = {"keep": "stats"}
    old_meta = {"keep": "meta"}
    for name, payload in (("launch", old), ("stats", old_stats), ("meta", old_meta)):
        (tmp_path / f"{name}.json").write_text(json.dumps(payload))
    before = {path.name: path.read_bytes() for path in tmp_path.glob("*.json")}
    monkeypatch.setattr(board_view, "validate_board_view", lambda *_args, **_kwargs: None)
    launch_admission = _protocol_admission()
    launch_admission.update({
        "protocol_id": "v3", "cohort_version": 6,
        "protocol_start_at": "2026-08-03T00:00:00+00:00",
    })
    stats_admission = deepcopy(launch_admission)
    stats_admission.update({
        "protocol_id": "v2", "cohort_version": 5,
        "protocol_start_at": "2026-07-20T00:00:00+00:00",
    })
    launch = {
        "generated_at": "2026-07-16T00:00:00+00:00",
        "research_protocol": {
            "protocol_id": "v3", "cohort_version": 6,
            "protocol_start_at": "2026-08-03T00:00:00+00:00",
        },
        "primary_sources": {"solana": {"protocol_admission": launch_admission}},
    }
    stats = {
        "generated_at": "2026-07-16T00:00:00+00:00",
        "lanes": {"launch": {"edge_validation": {
            "protocol_id": "v2", "cohort_version": 5,
            "protocol_start_at": "2026-07-20T00:00:00+00:00",
            "protocol_admission": stats_admission,
        }}},
    }

    with pytest.raises(ValueError, match="protocol identity mismatch"):
        board_export.write_views(launch=launch, stats=stats)

    assert {path.name: path.read_bytes() for path in tmp_path.glob("*.json")} == before


def test_stats_view_quarantines_legacy_and_rejects_execution_edge_claims(
        tmp_path, monkeypatch):
    from src.pipeline import board_export, opportunity_ledger

    monkeypatch.setattr(board_export, "EXPORT_DIR", tmp_path)
    monkeypatch.setattr(opportunity_ledger, "DB", tmp_path / "ledger.db")
    good = board_export.render_stats(None)
    board_export.write_views(stats=good)
    before = (tmp_path / "stats.json").read_bytes()

    mutators = (
        lambda launch: launch.update(real_edge_n=99),
        lambda launch: launch.update(auto_execution_allowed=1),
        lambda launch: launch.update(n=1),
        lambda launch: launch.pop("source_membership_policy"),
        lambda launch: launch["edge_validation"].update(
            auto_execution_allowed=True
        ),
        lambda launch: launch["edge_validation"].pop("source_membership_policy"),
        lambda launch: launch["edge_validation"].update(
            state="pass", edge_verdict="有前向纸面selector edge迹象"
        ),
        lambda launch: launch["current_protocol"].update(
            protocol_admission={"state": "open"}
        ),
        lambda launch: launch["legacy_distribution"].update(edge_eligible=True),
    )
    missing_launch = deepcopy(good)
    missing_launch["lanes"].pop("launch")
    with pytest.raises(ValueError, match="stats.lanes.launch is required"):
        board_export.write_views(stats=missing_launch)
    assert (tmp_path / "stats.json").read_bytes() == before
    for mutate in mutators:
        bad = deepcopy(good)
        mutate(bad["lanes"]["launch"])
        with pytest.raises(ValueError):
            board_export.write_views(stats=bad)
        assert (tmp_path / "stats.json").read_bytes() == before


def test_full_export_render_failure_never_replaces_last_good_launch(tmp_path, monkeypatch):
    from src.pipeline import board_export

    monkeypatch.setattr(board_export, "EXPORT_DIR", tmp_path)
    old = _view(board_export, "launch", _launch_body([_launch_event()]))
    board_export.write_views(launch=old)
    before = {path.name: path.read_bytes() for path in tmp_path.glob("*.json")}
    monkeypatch.setattr(board_export, "render_launch", lambda: (_ for _ in ()).throw(
        RuntimeError("launch ledger unavailable")))
    pushed = []
    monkeypatch.setattr(board_export, "push_to_blob",
                        lambda paths: pushed.extend(paths) or len(paths))

    with pytest.raises(RuntimeError, match="ledger unavailable"):
        board_export.run(push=True)

    assert {path.name: path.read_bytes() for path in tmp_path.glob("*.json")} == before
    assert pushed == []
