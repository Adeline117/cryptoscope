"""Solana launch evidence remains distinct from a qualified opportunity."""
from __future__ import annotations

import json

import pytest


@pytest.fixture
def sol(tmp_path, monkeypatch):
    from src.pipeline import solana_launch_stream

    monkeypatch.setattr(solana_launch_stream, "DB", tmp_path / "launches.db")
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
