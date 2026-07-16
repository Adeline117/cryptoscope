"""Finalized archive reconciliation is the source-completeness gate."""
import sqlite3
from datetime import datetime, timezone

import pytest


PUMP = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"


def _payload(signature="sig-create", slot=100):
    logs = [
        f"Program {PUMP} invoke [1]",
        "Program log: Instruction: CreateV2",
        f"Program {PUMP} success",
    ]
    return {
        "kind": "launch", "signature": signature, "slot": slot,
        "transaction_index": None, "program": PUMP,
        "event_type": "pump_fun_createv2", "logs": logs,
    }


def _transaction(signature="sig-create", slot=100):
    payload = _payload(signature, slot)
    return {
        "slot": slot, "blockTime": 1_784_553_600,
        "transaction": {"signatures": [signature], "message": {
            "accountKeys": [
                {"pubkey": "creator", "signer": True},
                {"pubkey": "mint", "signer": True},
                {"pubkey": "curve", "signer": False},
            ],
            "instructions": [{"programId": PUMP,
                              "accounts": ["mint", "curve", "creator"],
                              "data": "raw"}],
        }},
        "meta": {"err": None, "logMessages": payload["logs"]},
    }


class FakeRpc:
    def __init__(self, endpoint, *, signatures=None, head=200):
        self.endpoint = endpoint
        self.signatures = signatures if signatures is not None else [
            {"signature": "sig-create", "slot": 100, "err": None},
            {"signature": "older", "slot": 99, "err": None},
        ]
        self.head = head

    def call(self, method, params):
        if method == "getGenesisHash":
            return "genesis-mainnet"
        if method == "getSlot":
            return self.head
        if method == "getFirstAvailableBlock":
            return 0
        if method == "getBlocks":
            return [params[0]]
        if method == "getSignaturesForAddress":
            return self.signatures
        if method == "getTransaction":
            return _transaction(params[0], 100)
        raise AssertionError(method)


@pytest.fixture
def reconcile(tmp_path, monkeypatch):
    from src.pipeline import solana_launch_reconcile as module
    from src.pipeline import solana_launch_stream as stream
    from src.pipeline import stream_health

    monkeypatch.setattr(stream, "DB", tmp_path / "launches.db")
    monkeypatch.setattr(stream_health, "DB", tmp_path / "health.db")
    monkeypatch.setenv("SOLANA_STREAM_RPC_URL", "https://live.example")
    monkeypatch.setenv(
        "SOLANA_RECONCILIATION_RPC_URL", "https://archive.example"
    )
    return module, stream, stream_health


def test_independent_archive_seals_exact_live_epoch(reconcile):
    module, stream, _health = reconcile
    now = datetime(2026, 7, 20, 12, tzinfo=timezone.utc)
    stream.persist(
        _payload(), transaction=_transaction(), capture_mode="live_ws",
        captured_at=now, source_provider="solana_rpc:live.example",
    )

    got = module.reconcile_next_epoch(
        FakeRpc("https://live.example"), FakeRpc("https://archive.example"),
        now=now, epoch_slots=4, safety_slots=0, start_slot=100,
    )

    assert got["state"] == "sealed_clean"
    assert got["canonical_launches"] == got["live_launches"] == 1
    assert got["missing_live"] == got["extra_live"] == 0
    connection = stream._conn()
    try:
        state = connection.execute(
            "SELECT capture_mode,reconciliation_state,reconciliation_epoch_id "
            "FROM raw_launches"
        ).fetchone()
    finally:
        connection.close()
    assert state[0] == "live_ws" and state[1] == "verified_live" and state[2]
    proof = module.reconciliation_epoch_proof(
        state[2], slot=100, reconciled_at=got["checked_at"],
    )
    assert proof == {
        "version": 1, "epoch_id": state[2], "from_slot": 100, "to_slot": 103,
        "status": "sealed_clean", "checked_at": got["checked_at"],
        "live_provider": "solana_rpc:live.example",
        "archive_provider": "solana_rpc:archive.example",
        "genesis_hash": "genesis-mainnet", "evidence_hash": got["evidence_hash"],
        "finalized_head": 200,
    }
    candidate = module.candidate_reconciliation_proof(
        "sig-create", slot=100, mint="mint", creator="creator",
    )
    connection = stream._conn()
    try:
        raw_hash, identity_hash = connection.execute(
            "SELECT raw_payload_hash,hydration_payload_hash FROM raw_launches"
        ).fetchone()
    finally:
        connection.close()
    assert candidate == {
        **proof,
        "live_captured_at": now.isoformat(),
        "live_observation_hash": raw_hash,
        "archive_observation_hash": raw_hash,
        "hydration_identity_hash": identity_hash,
    }


def test_archive_only_launch_is_backfill_and_permanent_epoch_breach(reconcile):
    module, stream, _health = reconcile
    now = datetime(2026, 7, 20, 12, tzinfo=timezone.utc)

    got = module.reconcile_next_epoch(
        FakeRpc("https://live.example"), FakeRpc("https://archive.example"),
        now=now, epoch_slots=4, safety_slots=0, start_slot=100,
    )

    assert got["state"] == "sealed_breached" and got["missing_live"] == 1
    connection = stream._conn()
    try:
        row = connection.execute(
            """SELECT capture_mode,reconciliation_state,qualification_state,
                      detected_at FROM raw_launches"""
        ).fetchone()
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            connection.execute(
                "UPDATE reconciliation_epochs SET status='sealed_clean'"
            )
    finally:
        connection.close()
    assert row == (
        "finalized_reconciliation", "reconciled_backfill",
        "provenance_conflict", now.isoformat(),
    )
    with pytest.raises(module.ReconciliationError, match="not clean"):
        module.reconciliation_epoch_proof(got["epoch_id"], slot=100)


def test_live_only_launch_is_extra_and_blocks_readiness(reconcile):
    module, stream, health = reconcile
    now = datetime(2026, 7, 20, 12, tzinfo=timezone.utc)
    stream.persist(
        _payload(), transaction=_transaction(), capture_mode="live_ws",
        captured_at=now, source_provider="solana_rpc:live.example",
    )
    archive = FakeRpc(
        "https://archive.example",
        signatures=[{"signature": "older", "slot": 99, "err": None}],
    )

    got = module.reconcile_next_epoch(
        FakeRpc("https://live.example"), archive, now=now,
        epoch_slots=4, safety_slots=0, start_slot=100,
    )
    health.observe(
        "solana", "pump_fun_launches", cursor=200, received_at=now,
        expect_contiguous=True,
    )
    health.report_worker(
        "solana", stream.MAINTENANCE_STREAM, status="live", at=now,
    )
    readiness = module.source_readiness(now=now, required_clean_epochs=1)

    assert got["extra_live"] == 1
    assert readiness["ready"] is False
    assert "reconciliation_epoch_breached" in readiness["reason_codes"]


def test_one_clean_epoch_and_live_health_can_satisfy_test_burn_in(reconcile):
    module, stream, health = reconcile
    now = datetime(2026, 7, 20, 12, tzinfo=timezone.utc)
    stream.persist(
        _payload(), transaction=_transaction(), capture_mode="live_ws",
        captured_at=now, source_provider="solana_rpc:live.example",
    )
    module.reconcile_next_epoch(
        FakeRpc("https://live.example"), FakeRpc("https://archive.example"),
        now=now, epoch_slots=4, safety_slots=0, start_slot=100,
    )
    health.observe(
        "solana", "pump_fun_launches", cursor=200, received_at=now,
        expect_contiguous=True,
    )
    health.report_worker(
        "solana", stream.MAINTENANCE_STREAM, status="live", at=now,
    )

    got = module.source_readiness(now=now, required_clean_epochs=1)

    assert got["ready"] is True and got["state"] == "ready"


def test_same_provider_or_incomplete_signature_page_fails_closed(reconcile):
    module, _stream, _health = reconcile
    with pytest.raises(module.ReconciliationError, match="different hosts"):
        module.reconcile_next_epoch(
            FakeRpc("https://same.example"), FakeRpc("https://same.example"),
            epoch_slots=4, safety_slots=0, start_slot=100,
        )
    with pytest.raises(module.ReconciliationError, match="different hosts"):
        module.reconcile_next_epoch(
            FakeRpc("https://same.example:8899"),
            FakeRpc("https://same.example:9900"),
            epoch_slots=4, safety_slots=0, start_slot=100,
        )

    malformed = FakeRpc("https://archive.example", signatures={"result": []})
    with pytest.raises(module.ReconciliationError, match="non-list"):
        module.reconcile_next_epoch(
            FakeRpc("https://live.example"), malformed,
            epoch_slots=4, safety_slots=0, start_slot=100,
        )


def test_readiness_is_scoped_to_configured_archive_and_recent_evidence(
        reconcile, monkeypatch):
    module, stream, _health = reconcile
    now = datetime(2026, 7, 20, 12, tzinfo=timezone.utc)
    stream.persist(
        _payload(), transaction=_transaction(), capture_mode="live_ws",
        captured_at=now, source_provider="solana_rpc:live.example",
    )
    module.reconcile_next_epoch(
        FakeRpc("https://live.example"), FakeRpc("https://archive.example"),
        now=now, epoch_slots=4, safety_slots=0, start_slot=100,
    )

    stale = module.source_readiness(
        now=now.replace(minute=6), required_clean_epochs=1,
        require_runtime_health=False,
    )
    assert stale["ready"] is False
    assert "reconciliation_evidence_stale" in stale["reason_codes"]

    monkeypatch.setenv(
        "SOLANA_RECONCILIATION_RPC_URL", "https://different-archive.example"
    )
    wrong_provider = module.source_readiness(
        now=now, required_clean_epochs=1, require_runtime_health=False,
    )
    assert wrong_provider["observed_epochs"] == 0
    assert "clean_epoch_burn_in_incomplete" in wrong_provider["reason_codes"]

    monkeypatch.delenv("SOLANA_RECONCILIATION_RPC_URL")
    unconfigured = module.source_readiness(
        now=now, required_clean_epochs=1, require_runtime_health=False,
    )
    assert unconfigured["archive_provider"] is None
    assert "archive_provider_not_configured" in unconfigured["reason_codes"]


def test_recent_checks_cannot_hide_a_large_finalized_slot_lag(reconcile):
    module, stream, _health = reconcile
    now = datetime(2026, 7, 20, 12, tzinfo=timezone.utc)
    stream.persist(
        _payload(), transaction=_transaction(), capture_mode="live_ws",
        captured_at=now, source_provider="solana_rpc:live.example",
    )
    module.reconcile_next_epoch(
        FakeRpc("https://live.example", head=1_000),
        FakeRpc("https://archive.example", head=1_000),
        now=now, epoch_slots=4, safety_slots=0, start_slot=100,
    )

    got = module.source_readiness(
        now=now, required_clean_epochs=1, require_runtime_health=False,
        max_finalized_lag_slots=256,
    )

    assert got["latest_age_seconds"] == 0
    assert got["latest_sealed_lag_slots"] == 897
    assert got["ready"] is False
    assert "reconciliation_slot_lag_exceeded" in got["reason_codes"]


def test_fabricated_mutable_flags_do_not_prove_candidate_membership(reconcile):
    module, stream, _health = reconcile
    now = datetime(2026, 7, 20, 12, tzinfo=timezone.utc)
    stream.persist(
        _payload(), transaction=_transaction(), capture_mode="live_ws",
        captured_at=now, source_provider="solana_rpc:live.example",
    )
    sealed = module.reconcile_next_epoch(
        FakeRpc("https://live.example"), FakeRpc("https://archive.example"),
        now=now, epoch_slots=4, safety_slots=0, start_slot=100,
    )
    connection = stream._conn()
    try:
        connection.execute(
            """INSERT INTO raw_launches(
                   signature,slot,program,event_type,creator,mint,detected_at,
                   captured_at,source_provider,raw_payload_hash,
                   hydration_payload_hash,logs,evidence_state,qualification_state,
                   capture_mode,reconciliation_state,reconciliation_epoch_id,
                   reconciled_at)
               VALUES ('fabricated',101,?,?,?,?,?,?,?,?,?,'[]','complete',
                       'raw_unqualified','live_ws','verified_live',?,?)""",
            (stream.PUMP_FUN_PROGRAM, "pump_fun_createv2", "creator", "fake-mint",
             now.isoformat(), now.isoformat(), "solana_rpc:live.example",
             "a" * 64, "b" * 64, sealed["epoch_id"], sealed["checked_at"]),
        )
        connection.commit()
    finally:
        connection.close()

    with pytest.raises(module.ReconciliationError, match="immutable"):
        module.candidate_reconciliation_proof(
            "fabricated", slot=101, mint="fake-mint", creator="creator",
        )


def test_epoch_and_candidate_marking_commit_atomically(reconcile, monkeypatch):
    module, stream, _health = reconcile
    now = datetime(2026, 7, 20, 12, tzinfo=timezone.utc)
    stream.persist(
        _payload(), transaction=_transaction(), capture_mode="live_ws",
        captured_at=now, source_provider="solana_rpc:live.example",
    )

    def fail_mark(*_args, **_kwargs):
        raise RuntimeError("comparison write failed")

    monkeypatch.setattr(module, "_mark_comparison", fail_mark)
    with pytest.raises(RuntimeError, match="comparison write failed"):
        module.reconcile_next_epoch(
            FakeRpc("https://live.example"), FakeRpc("https://archive.example"),
            now=now, epoch_slots=4, safety_slots=0, start_slot=100,
        )

    connection = stream._conn()
    try:
        epochs = connection.execute(
            "SELECT COUNT(*) FROM reconciliation_epochs"
        ).fetchone()[0]
        cursors = connection.execute(
            "SELECT COUNT(*) FROM reconciliation_cursor"
        ).fetchone()[0]
        candidate = connection.execute(
            "SELECT reconciliation_state,reconciliation_epoch_id FROM raw_launches"
        ).fetchone()
    finally:
        connection.close()
    assert epochs == cursors == 0
    assert candidate == ("unverified", None)
