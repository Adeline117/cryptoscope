"""Frozen Launch selector and independent source proof contracts."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone

import pytest


def _proof(*, checked_at: str, captured_at: str) -> dict:
    return {
        "version": 1,
        "epoch_id": "1" * 32,
        "from_slot": 100,
        "to_slot": 227,
        "status": "sealed_clean",
        "checked_at": checked_at,
        "live_provider": "solana_rpc:live.example",
        "archive_provider": "solana_rpc:archive.example",
        "genesis_hash": "mainnet-genesis",
        "evidence_hash": "2" * 64,
        "finalized_head": 250,
        "live_captured_at": captured_at,
        "live_observation_hash": "a" * 64,
        "archive_observation_hash": "a" * 64,
        "hydration_identity_hash": "b" * 64,
    }


def _source_snapshot():
    from src.contract.launch_selector import freeze_source_snapshot

    detected = datetime(2026, 8, 3, 12, tzinfo=timezone.utc)
    reconciled = detected + timedelta(seconds=30)
    decision = detected + timedelta(seconds=60)
    return freeze_source_snapshot(
        signature="signature", slot=101, event_type="pump_fun_createv2",
        detected_at=detected.isoformat(), captured_at=detected.isoformat(),
        decision_at=decision.isoformat(),
        mint="mint", raw_payload_hash="a" * 64,
        hydration_payload_hash="b" * 64, capture_mode="live_ws",
        source_provider="solana_rpc:live.example",
        reconciliation_state="verified_live",
        reconciled_at=reconciled.isoformat(),
        reconciliation_proof=_proof(
            checked_at=reconciled.isoformat(), captured_at=detected.isoformat(),
        ),
    ), detected, decision


def test_selector_snapshot_reproduces_arm_and_position_cap():
    from src.contract.launch_selector import (
        evaluate_selector_snapshot, freeze_selector_snapshot,
    )

    event = datetime(2026, 8, 3, 12, tzinfo=timezone.utc)
    decision = event + timedelta(minutes=1)
    snapshot = freeze_selector_snapshot(
        pool_created_at=event.isoformat(), liquidity_usd=20_000,
        fdv_usd=100_000, volume_m5_usd=1_000, buys_m5=10, sells_m5=3,
    )

    got = evaluate_selector_snapshot(
        snapshot, event_at=event.isoformat(), decision_at=decision.isoformat(),
    )

    assert got["decision"] == "SMALL_PROBE"
    assert got["max_notional_usd"] == 60.0
    assert got["modeled_route_roundtrip_pct"] > 0


def test_source_snapshot_freezes_clean_independent_epoch_before_decision():
    from src.contract.launch_selector import validate_source_snapshot

    snapshot, detected, decision = _source_snapshot()

    assert snapshot["version"] == 2
    assert snapshot["capture_mode"] == "live_ws"
    assert snapshot["reconciliation_state"] == "verified_live"
    assert snapshot["reconciliation_proof"]["status"] == "sealed_clean"
    assert validate_source_snapshot(
        snapshot, token="mint", detected_at=detected.isoformat(),
        decision_at=decision.isoformat(),
    ) == snapshot


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda value: value.update(capture_mode="gap_backfill"), "capture mode"),
        (lambda value: value.update(reconciliation_state="unverified"),
         "not verified"),
        (lambda value: value.update(source_provider="solana_rpc:other.example"),
         "source provider"),
        (lambda value: value.update(captured_at="2026-08-03T12:00:01+00:00"),
         "detection clock"),
        (lambda value: value["reconciliation_proof"].update(status="sealed_breached"),
         "not clean"),
        (lambda value: value["reconciliation_proof"].update(
            archive_provider="solana_rpc:live.example"), "independent"),
        (lambda value: value["reconciliation_proof"].update(
            archive_provider="solana_rpc:live.example:8899",
            live_provider="solana_rpc:live.example:9900"), "independent"),
        (lambda value: value["reconciliation_proof"].update(from_slot=102),
         "outside"),
        (lambda value: value["reconciliation_proof"].update(evidence_hash="bad"),
         "evidence_hash"),
        (lambda value: value["reconciliation_proof"].update(
            archive_observation_hash="c" * 64), "observation hashes"),
        (lambda value: value["reconciliation_proof"].update(finalized_head=200),
         "before epoch end"),
    ],
)
def test_source_snapshot_rejects_unverified_or_mutated_proof(mutate, message):
    from src.contract.launch_selector import freeze_source_snapshot

    snapshot, _detected, decision = _source_snapshot()
    candidate = deepcopy(snapshot)
    mutate(candidate)

    with pytest.raises(ValueError, match=message):
        freeze_source_snapshot(
            signature=candidate["signature"], slot=candidate["slot"],
            event_type=candidate["event_type"],
            detected_at=candidate["detected_at"],
            captured_at=candidate["captured_at"], decision_at=decision.isoformat(),
            mint=candidate["mint"], raw_payload_hash=candidate["raw_payload_hash"],
            hydration_payload_hash=candidate["hydration_payload_hash"],
            capture_mode=candidate["capture_mode"],
            source_provider=candidate["source_provider"],
            reconciliation_state=candidate["reconciliation_state"],
            reconciled_at=candidate["reconciled_at"],
            reconciliation_proof=candidate["reconciliation_proof"],
        )


def test_source_proof_cannot_be_added_after_the_decision():
    from src.contract.launch_selector import freeze_source_snapshot

    detected = datetime(2026, 8, 3, 12, tzinfo=timezone.utc)
    decision = detected + timedelta(seconds=30)
    reconciled = decision + timedelta(seconds=1)

    with pytest.raises(ValueError, match="between detection and decision"):
        freeze_source_snapshot(
            signature="signature", slot=101, event_type="pump_fun_create",
            detected_at=detected.isoformat(), captured_at=detected.isoformat(),
            decision_at=decision.isoformat(),
            mint="mint", raw_payload_hash="a" * 64,
            hydration_payload_hash="b" * 64, capture_mode="live_ws",
            source_provider="solana_rpc:live.example",
            reconciliation_state="verified_live",
            reconciled_at=reconciled.isoformat(),
            reconciliation_proof=_proof(
                checked_at=reconciled.isoformat(), captured_at=detected.isoformat(),
            ),
        )


def test_source_event_type_is_an_exact_frozen_universe_member():
    from src.contract.launch_selector import freeze_source_snapshot

    snapshot, _detected, decision = _source_snapshot()
    with pytest.raises(ValueError, match="not a Pump.fun create"):
        freeze_source_snapshot(
            signature=snapshot["signature"], slot=snapshot["slot"],
            event_type="pump_fun_create_future_guess",
            detected_at=snapshot["detected_at"],
            captured_at=snapshot["captured_at"], decision_at=decision.isoformat(),
            mint=snapshot["mint"], raw_payload_hash=snapshot["raw_payload_hash"],
            hydration_payload_hash=snapshot["hydration_payload_hash"],
            capture_mode=snapshot["capture_mode"],
            source_provider=snapshot["source_provider"],
            reconciliation_state=snapshot["reconciliation_state"],
            reconciled_at=snapshot["reconciled_at"],
            reconciliation_proof=snapshot["reconciliation_proof"],
        )


def test_source_snapshot_enforces_preregistered_decision_deadline():
    from src.contract.launch_selector import freeze_source_snapshot

    snapshot, detected, _decision = _source_snapshot()
    late = detected + timedelta(seconds=601)
    with pytest.raises(ValueError, match="deadline exceeded"):
        freeze_source_snapshot(
            signature=snapshot["signature"], slot=snapshot["slot"],
            event_type=snapshot["event_type"], detected_at=snapshot["detected_at"],
            captured_at=snapshot["captured_at"], decision_at=late.isoformat(),
            mint=snapshot["mint"], raw_payload_hash=snapshot["raw_payload_hash"],
            hydration_payload_hash=snapshot["hydration_payload_hash"],
            capture_mode=snapshot["capture_mode"],
            source_provider=snapshot["source_provider"],
            reconciliation_state=snapshot["reconciliation_state"],
            reconciled_at=snapshot["reconciled_at"],
            reconciliation_proof=snapshot["reconciliation_proof"],
        )
