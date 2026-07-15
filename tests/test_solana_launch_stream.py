"""Solana launch evidence remains distinct from a qualified opportunity."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest


@pytest.fixture
def sol(tmp_path, monkeypatch):
    from src.pipeline import solana_launch_stream
    from src.pipeline import stream_health

    monkeypatch.setattr(solana_launch_stream, "DB", tmp_path / "launches.db")
    monkeypatch.setattr(stream_health, "DB", tmp_path / "stream-health.db")
    return solana_launch_stream


def _notification(log="Program log: Instruction: CreateV2"):
    return {"jsonrpc": "2.0", "method": "logsNotification", "params": {"result": {
        "context": {"slot": 123}, "value": {"signature": "sig-1", "err": None,
        "logs": [log]},
    }}}


def _transaction():
    return {"slot": 123, "transaction": {"signatures": ["sig-1"], "message": {
        "accountKeys": [
            {"pubkey": "creator", "signer": True},
            {"pubkey": "mint", "signer": True},
            {"pubkey": "curve", "signer": False},
        ],
        "instructions": [{"programId":
                          "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P",
                          "accounts": ["mint", "curve", "creator"], "data": "raw"}],
    }}, "meta": {"err": None,
                  "logMessages": ["Program log: Instruction: CreateV2"]}}


def test_subscriptions_use_standard_solana_methods(sol):
    requests = sol.subscribe_requests()
    assert requests[0]["method"] == "logsSubscribe"
    assert requests[0]["params"][0] == {"mentions": [sol.PUMP_FUN_PROGRAM]}
    assert requests[1]["method"] == "slotSubscribe"
    assert all(request["method"] != "transactionSubscribe" for request in requests)


def test_creation_is_parsed_and_hydrated_as_raw_unqualified(sol):
    event = sol.parse_message(json.dumps(_notification()))
    assert event.cursor is None
    assert event.payload["event_type"] == "pump_fun_createv2"
    sol.persist(event.payload, transaction=_transaction())
    c = sol._conn()
    try:
        row = c.execute("SELECT event_type,creator,mint,evidence_state,"
                        "qualification_state,length(raw_payload_hash),"
                        "length(hydration_payload_hash) FROM raw_launches").fetchone()
        assert row == ("pump_fun_createv2", "creator", "mint", "complete",
                       "raw_unqualified", 64, 64)
    finally:
        c.close()


def test_generic_initialize_mint_is_not_enough_to_claim_launch(sol):
    assert sol.parse_message(_notification(
        "Program log: Instruction: InitializeMint2")) is None


def test_slot_notifications_supply_gap_cursor(sol):
    event = sol.parse_message({"jsonrpc": "2.0", "method": "slotNotification",
                               "params": {"result": {"slot": 456}}})
    assert event.cursor == 456
    assert event.payload == {"kind": "slot", "slot": 456}


def test_hydration_failure_keeps_raw_event_visible(sol):
    class BrokenRpc:
        def call(self, method, params):
            raise RuntimeError("rate limited")

    event = sol.parse_message(_notification())
    sol.persist(event.payload, rpc=BrokenRpc())
    c = sol._conn()
    try:
        row = c.execute("SELECT evidence_state,hydration_error,creator,mint "
                        "FROM raw_launches").fetchone()
        assert row[0] == "rpc_unavailable" and "rate limited" in row[1]
        assert row[2:] == (None, None)
    finally:
        c.close()


def test_ambiguous_signers_do_not_become_creator_claim(sol):
    tx = _transaction()
    tx["transaction"]["message"]["accountKeys"].append(
        {"pubkey": "sponsor", "signer": True})
    tx["transaction"]["message"]["instructions"][0]["accounts"].append("sponsor")
    event = sol.parse_message(_notification())
    sol.persist(event.payload, transaction=tx)
    c = sol._conn()
    try:
        assert c.execute("SELECT evidence_state,creator,mint FROM raw_launches").fetchone() \
            == ("incomplete", None, None)
    finally:
        c.close()


def test_inner_creation_instruction_can_prove_wrapped_launch(sol):
    tx = _transaction()
    creation = tx["transaction"]["message"]["instructions"].pop()
    tx["transaction"]["message"]["instructions"].append(
        {"programId": "router", "accounts": ["creator"], "data": "wrapper"})
    tx["meta"]["innerInstructions"] = [{"index": 0, "instructions": [creation]}]
    event = sol.parse_message(_notification())
    sol.persist(event.payload, transaction=tx)
    c = sol._conn()
    try:
        assert c.execute("SELECT evidence_state,creator,mint FROM raw_launches").fetchone() \
            == ("complete", "creator", "mint")
    finally:
        c.close()


def test_rpc_failed_launch_is_rehydrated_later(sol):
    class RecoveringRpc:
        def call(self, method, params):
            assert method == "getTransaction"
            return _transaction()

    event = sol.parse_message(_notification())
    sol.persist(event.payload)
    assert sol.rehydrate_pending(RecoveringRpc()) == {
        "attempted": 1, "completed": 1, "failed": 0}
    c = sol._conn()
    try:
        assert c.execute("SELECT evidence_state,creator,mint FROM raw_launches").fetchone() \
            == ("complete", "creator", "mint")
    finally:
        c.close()


def test_incomplete_identity_is_only_retried_during_startup_audit(sol):
    tx = _transaction()
    tx["transaction"]["message"]["accountKeys"].append(
        {"pubkey": "sponsor", "signer": True})
    tx["transaction"]["message"]["instructions"][0]["accounts"].append("sponsor")
    event = sol.parse_message(_notification())
    sol.persist(event.payload, transaction=tx)

    class Rpc:
        def __init__(self):
            self.calls = 0

        def call(self, method, params):
            self.calls += 1
            return _transaction()

    rpc = Rpc()
    assert sol.rehydrate_pending(rpc)["attempted"] == 0
    assert sol.rehydrate_pending(rpc, include_incomplete=True)["completed"] == 1
    assert rpc.calls == 1


def test_open_slot_gap_is_retried_and_resolved(sol):
    from src.pipeline import stream_health

    stream_health.observe("solana", "pump_fun_launches", cursor=10,
                          expect_contiguous=True)
    stream_health.observe("solana", "pump_fun_launches", cursor=12,
                          expect_contiguous=True)

    class Rpc:
        def call(self, method, params):
            assert method == "getBlock" and params[0] == 11
            return None

    assert sol.retry_open_gaps(Rpc()) == {
        "attempted": 1, "recovered": 1, "failed": 0}
    assert stream_health.open_gaps("solana", "pump_fun_launches") == []
    assert stream_health.snapshot(stale_after_seconds=60)[0]["status"] == "live"


def test_short_slot_gap_is_backfilled_and_long_gap_stays_open(sol):
    tx = _transaction()

    class Rpc:
        def __init__(self):
            self.slots = []

        def call(self, method, params):
            assert method == "getBlock"
            self.slots.append(params[0])
            return {"transactions": [tx]} if params[0] == 11 else None

    rpc = Rpc()
    assert sol.backfill_slots(10, 12, rpc=rpc) is True
    assert rpc.slots == [10, 11, 12]
    c = sol._conn()
    try:
        assert c.execute("SELECT signature,slot,transaction_index,evidence_state "
                         "FROM raw_launches").fetchone() == ("sig-1", 11, 0, "complete")
    finally:
        c.close()
    assert sol.backfill_slots(1, sol.MAX_BACKFILL_SLOTS + 1, rpc=rpc) is False


def test_subscription_error_fails_visible(sol):
    with pytest.raises(PermissionError, match="rejected"):
        sol.parse_message({"jsonrpc": "2.0", "id": 1,
                           "error": {"code": -32600, "message": "denied"}})


def test_qualification_batch_only_returns_recent_due_complete_rows(sol):
    now = datetime(2026, 7, 15, 12, tzinfo=timezone.utc)
    event = sol.parse_message(_notification())
    sol.persist(event.payload, transaction=_transaction())
    c = sol._conn()
    try:
        c.execute("UPDATE raw_launches SET detected_at=?", ((now - timedelta(minutes=5)).isoformat(),))
        c.commit()
    finally:
        c.close()

    due = sol.qualification_batch(now=now, retry_after_seconds=300)
    assert [row["mint"] for row in due] == ["mint"]
    sol.set_qualification("sig-1", "market_pending", error="pair not indexed", at=now)
    assert sol.qualification_batch(now=now + timedelta(seconds=299)) == []
    assert len(sol.qualification_batch(now=now + timedelta(seconds=301))) == 1


def test_qualification_state_keeps_raw_evidence_and_ledger_link(sol):
    now = datetime(2026, 7, 15, 12, tzinfo=timezone.utc)
    event = sol.parse_message(_notification())
    sol.persist(event.payload, transaction=_transaction())

    assert sol.set_qualification(
        "sig-1", "qualified_recorded", ledger_event_id="ledger-1", at=now)
    c = sol._conn()
    try:
        row = c.execute(
            "SELECT evidence_state,qualification_state,qualified_at,ledger_event_id "
            "FROM raw_launches WHERE signature='sig-1'"
        ).fetchone()
    finally:
        c.close()
    assert row == ("complete", "qualified_recorded", now.isoformat(), "ledger-1")
    summary = sol.qualification_summary(now=now)
    assert summary["raw_total"] == 1
    assert summary["evidence"] == {"complete": 1}
    assert summary["qualification"] == {"qualified_recorded": 1}


def test_unknown_qualification_state_is_rejected(sol):
    with pytest.raises(ValueError, match="unknown qualification state"):
        sol.set_qualification("sig-1", "pretend_success")
