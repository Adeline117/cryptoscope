"""Solana launch evidence remains distinct from a qualified opportunity."""
from __future__ import annotations

import json
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
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
        "logs": [
            "Program 6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P invoke [1]",
            log,
            "Program 6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P success",
        ]},
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
    }}, "meta": {"err": None, "logMessages": [
        "Program 6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P invoke [1]",
        "Program log: Instruction: CreateV2",
        "Program 6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P success",
    ]}}


def _raw_transaction(*, loaded_program=False, loaded_sponsor=False):
    static_keys = ["creator", "mint", "curve"]
    writable = ["loaded-sponsor"] if loaded_sponsor else []
    if not loaded_program:
        static_keys.append("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P")
    readonly = (["6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"]
                if loaded_program else [])
    program_index = (len(static_keys) + len(writable)
                     if loaded_program else len(static_keys) - 1)
    accounts = [1, 2, 0]
    if loaded_sponsor:
        accounts.append(len(static_keys))
    return {
        "transaction": {
            "signatures": ["sig-1"],
            "message": {
                "header": {
                    "numRequiredSignatures": 2,
                    "numReadonlySignedAccounts": 0,
                    "numReadonlyUnsignedAccounts": 1,
                },
                "accountKeys": static_keys,
                "instructions": [{
                    "programIdIndex": program_index,
                    "accounts": accounts,
                    "data": "raw",
                }],
            },
        },
        "meta": {
            "err": None,
            "loadedAddresses": {"writable": writable, "readonly": readonly},
            "logMessages": [
                "Program 6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P invoke [1]",
                "Program log: Instruction: CreateV2",
            ],
        },
        "slot": 123,
    }


def _pump_swap_with_ata_create():
    tx = _raw_transaction()
    ata_program = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL"
    tx["transaction"]["message"]["accountKeys"].append(ata_program)
    tx["transaction"]["message"]["instructions"][0]["accounts"] = [0, 2]
    tx["meta"]["innerInstructions"] = [{
        "index": 0,
        "instructions": [{"programIdIndex": 4, "accounts": [0], "data": "ata"}],
    }]
    tx["meta"]["logMessages"] = [
        "Program 6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P invoke [1]",
        "Program log: Instruction: Buy",
        f"Program {ata_program} invoke [2]",
        "Program log: Instruction: Create",
        f"Program {ata_program} success",
        "Program 6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P success",
    ]
    return tx


def test_subscriptions_use_standard_solana_methods(sol):
    requests = sol.subscribe_requests()
    assert requests[0]["method"] == "logsSubscribe"
    assert requests[0]["params"][0] == {"mentions": [sol.PUMP_FUN_PROGRAM]}
    assert requests[1]["method"] == "slotSubscribe"
    assert all(request["method"] != "transactionSubscribe" for request in requests)


def test_concurrent_legacy_hydration_migration_is_idempotent(sol):
    legacy = sqlite3.connect(sol.DB)
    legacy.execute("""CREATE TABLE raw_launches(
        signature TEXT PRIMARY KEY, slot INTEGER NOT NULL, transaction_index INTEGER,
        program TEXT NOT NULL, event_type TEXT NOT NULL, creator TEXT, mint TEXT,
        detected_at TEXT NOT NULL, hydrated_at TEXT, raw_payload_hash TEXT NOT NULL,
        hydration_payload_hash TEXT, logs TEXT NOT NULL,
        evidence_state TEXT NOT NULL DEFAULT 'raw_only', hydration_error TEXT,
        qualification_state TEXT NOT NULL DEFAULT 'raw_unqualified')""")
    legacy.commit()
    legacy.close()

    workers = 12
    start = threading.Barrier(workers)

    def connect_and_read_schema() -> tuple[str, ...]:
        start.wait(timeout=5)
        connection = sol._conn()
        try:
            return tuple(
                row[1] for row in connection.execute("PRAGMA table_info(raw_launches)")
            )
        finally:
            connection.close()

    with ThreadPoolExecutor(max_workers=workers) as pool:
        schemas = list(pool.map(lambda _index: connect_and_read_schema(), range(workers)))

    expected = (
        "qualification_attempted_at", "qualification_error", "qualified_at",
        "ledger_event_id", "hydration_retry_count", "hydration_next_retry_at",
        "hydration_attempted_at", "hydration_last_rpc_error",
    )
    assert all(schema[-8:] == expected for schema in schemas)


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


def test_raw_json_transaction_indexes_prove_launch_identity(sol):
    event = sol.parse_message(json.dumps(_notification()))
    sol.persist(event.payload, transaction=_raw_transaction())

    c = sol._conn()
    try:
        row = c.execute(
            "SELECT creator,mint,evidence_state,hydration_error FROM raw_launches"
        ).fetchone()
    finally:
        c.close()
    assert row == ("creator", "mint", "complete", None)


def test_loaded_addresses_resolve_but_never_become_transaction_signers(sol):
    tx = _raw_transaction(loaded_program=True, loaded_sponsor=True)
    keys, signers = sol._account_keys(tx)
    assert keys == [
        "creator", "mint", "curve", "loaded-sponsor", sol.PUMP_FUN_PROGRAM,
    ]
    assert signers == {"creator", "mint"}

    event = sol.parse_message(json.dumps(_notification()))
    sol.persist(event.payload, transaction=tx)
    c = sol._conn()
    try:
        assert c.execute(
            "SELECT creator,mint,evidence_state FROM raw_launches"
        ).fetchone() == ("creator", "mint", "complete")
    finally:
        c.close()


def test_generic_initialize_mint_is_not_enough_to_claim_launch(sol):
    assert sol.parse_message(_notification(
        "Program log: Instruction: InitializeMint2")) is None


def test_ata_create_inside_pump_swap_is_not_a_launch_or_gap_blocker(sol):
    from src.pipeline import stream_health

    tx = _pump_swap_with_ata_create()
    logs = tx["meta"]["logMessages"]
    notification = _notification()
    notification["params"]["result"]["value"]["logs"] = logs
    assert sol.parse_message(notification) is None
    assert sol._launch_from_block_transaction(tx, 11, 0) is None

    stream_health.observe("solana", "pump_fun_launches", cursor=10,
                          expect_contiguous=True)
    stream_health.observe("solana", "pump_fun_launches", cursor=12,
                          expect_contiguous=True)

    class Rpc:
        def call(self, method, params):
            if method == "getSlot":
                return 100
            if method == "getFirstAvailableBlock":
                return 0
            if method == "getBlocks":
                return [11]
            assert method == "getBlock"
            return {"transactions": [tx]}

    assert sol.retry_open_gaps(Rpc()) == {
        "attempted": 1, "recovered": 1, "progressed": 0, "failed": 0,
    }
    assert stream_health.open_gaps("solana", "pump_fun_launches") == []
    c = sol._conn()
    try:
        assert c.execute("SELECT COUNT(*) FROM raw_launches").fetchone()[0] == 0
    finally:
        c.close()


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
        "attempted": 1, "completed": 1, "incomplete": 0,
        "rpc_failed": 0,
    }
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


def test_hydration_queue_is_fifo_so_new_launches_cannot_starve_old_rows(sol):
    base = datetime(2026, 7, 15, 12, tzinfo=timezone.utc)
    for index, signature in enumerate(("a-tie", "b-tie", "newest")):
        raw = _notification()
        raw["params"]["result"]["context"]["slot"] = 100 if index < 2 else 101
        raw["params"]["result"]["value"]["signature"] = signature
        sol.persist(sol.parse_message(raw).payload)
        c = sol._conn()
        try:
            c.execute(
                "UPDATE raw_launches SET detected_at=? WHERE signature=?",
                ((base if index < 2 else base + timedelta(seconds=1)).isoformat(),
                 signature),
            )
            c.commit()
        finally:
            c.close()
    c = sol._conn()
    try:
        c.execute(
            """UPDATE raw_launches SET evidence_state='rpc_unavailable',
                      hydration_retry_count=1,hydration_next_retry_at=?
               WHERE signature='b-tie'""",
            ((base - timedelta(seconds=1)).isoformat(),),
        )
        c.commit()
    finally:
        c.close()

    calls = []

    class Rpc:
        def call(self, method, params):
            assert method == "getTransaction"
            calls.append(params[0])
            tx = _transaction()
            tx["transaction"]["signatures"] = [params[0]]
            return tx

    result = sol.rehydrate_pending(Rpc(), limit=2, now=base + timedelta(minutes=1))

    assert calls == ["a-tie", "b-tie"]
    assert result == {
        "attempted": 2, "completed": 2, "incomplete": 0,
        "rpc_failed": 0,
    }
    c = sol._conn()
    try:
        assert c.execute(
            "SELECT signature FROM raw_launches WHERE evidence_state='raw_only'"
        ).fetchall() == [("newest",)]
    finally:
        c.close()


def test_hydration_rpc_failures_back_off_persistently(sol):
    base = datetime(2026, 7, 15, 12, tzinfo=timezone.utc)
    event = sol.parse_message(_notification())
    sol.persist(event.payload)
    c = sol._conn()
    try:
        c.execute("UPDATE raw_launches SET detected_at=?", (
            (base - timedelta(minutes=1)).isoformat(),
        ))
        c.commit()
    finally:
        c.close()

    class FlakyRpc:
        def __init__(self):
            self.calls = 0
            self.fail = True

        def call(self, method, params):
            self.calls += 1
            if self.fail:
                raise RuntimeError("rate limited")
            return _transaction()

    rpc = FlakyRpc()
    first = sol.rehydrate_pending(rpc, now=base)
    assert first == {
        "attempted": 1, "completed": 0, "incomplete": 0,
        "rpc_failed": 1,
    }
    c = sol._conn()
    try:
        retry = c.execute(
            "SELECT hydration_retry_count,hydration_next_retry_at FROM raw_launches"
        ).fetchone()
    finally:
        c.close()
    assert retry == (1, (base + timedelta(seconds=60)).isoformat())
    hydration = sol.qualification_summary(now=base)["hydration"]
    assert hydration == {
        "state": "backlogged", "pending_total": 1,
        "by_state": {"raw_only": 0, "rpc_unavailable": 1},
        "due": 0, "deferred": 1,
        "oldest_pending_at": (base - timedelta(minutes=1)).isoformat(),
        "oldest_pending_age_seconds": 60, "max_retry_count": 1,
    }

    assert sol.rehydrate_pending(rpc, now=base + timedelta(seconds=59))["attempted"] == 0
    second = sol.rehydrate_pending(rpc, now=base + timedelta(seconds=60))
    assert second["rpc_failed"] == 1 and rpc.calls == 2
    c = sol._conn()
    try:
        retry = c.execute(
            "SELECT hydration_retry_count,hydration_next_retry_at FROM raw_launches"
        ).fetchone()
    finally:
        c.close()
    assert retry == (2, (base + timedelta(seconds=180)).isoformat())

    rpc.fail = False
    recovered = sol.rehydrate_pending(rpc, now=base + timedelta(seconds=180))
    assert recovered == {
        "attempted": 1, "completed": 1, "incomplete": 0, "rpc_failed": 0,
    }
    c = sol._conn()
    try:
        reset = c.execute(
            "SELECT evidence_state,hydration_retry_count,hydration_next_retry_at "
            "FROM raw_launches"
        ).fetchone()
    finally:
        c.close()
    assert reset == ("complete", 0, None)


def test_hydration_outcome_does_not_count_ambiguous_identity_as_complete(sol):
    event = sol.parse_message(_notification())
    sol.persist(event.payload)
    ambiguous = _transaction()
    ambiguous["transaction"]["message"]["accountKeys"].append(
        {"pubkey": "sponsor", "signer": True})
    ambiguous["transaction"]["message"]["instructions"][0]["accounts"].append(
        "sponsor")

    class Rpc:
        def call(self, method, params):
            return ambiguous

    assert sol.rehydrate_pending(Rpc()) == {
        "attempted": 1, "completed": 0, "incomplete": 1,
        "rpc_failed": 0,
    }


def test_late_ambiguous_hydration_cannot_downgrade_complete_evidence(sol):
    event = sol.parse_message(_notification())
    sol.persist(event.payload, transaction=_transaction())
    ambiguous = _transaction()
    ambiguous["transaction"]["message"]["accountKeys"].append(
        {"pubkey": "sponsor", "signer": True})
    ambiguous["transaction"]["message"]["instructions"][0]["accounts"].append(
        "sponsor")
    c = sol._conn()
    try:
        before = c.execute(
            "SELECT creator,mint,hydration_payload_hash FROM raw_launches"
        ).fetchone()
    finally:
        c.close()

    assert sol._set_hydration("sig-1", ambiguous, None) == "complete"
    c = sol._conn()
    try:
        after = c.execute(
            "SELECT evidence_state,creator,mint,hydration_payload_hash FROM raw_launches"
        ).fetchone()
    finally:
        c.close()
    assert after == ("complete", before[0], before[1], before[2])


def test_failed_incomplete_audit_preserves_captured_transaction_evidence(sol):
    base = datetime(2026, 7, 15, 12, tzinfo=timezone.utc)
    ambiguous = _transaction()
    ambiguous["transaction"]["message"]["accountKeys"].append(
        {"pubkey": "sponsor", "signer": True})
    ambiguous["transaction"]["message"]["instructions"][0]["accounts"].append(
        "sponsor")
    event = sol.parse_message(_notification())
    sol.persist(event.payload, transaction=ambiguous)
    c = sol._conn()
    try:
        before = c.execute(
            "SELECT hydrated_at,hydration_payload_hash,hydration_error FROM raw_launches"
        ).fetchone()
    finally:
        c.close()

    class BrokenRpc:
        def call(self, method, params):
            raise RuntimeError("temporary outage")

    result = sol.rehydrate_pending(
        BrokenRpc(), include_incomplete=True, now=base,
    )
    assert result == {
        "attempted": 1, "completed": 0, "incomplete": 0, "rpc_failed": 1,
    }
    c = sol._conn()
    try:
        after = c.execute(
            """SELECT evidence_state,hydrated_at,hydration_payload_hash,
                      hydration_error,hydration_retry_count,hydration_attempted_at,
                      hydration_last_rpc_error
               FROM raw_launches"""
        ).fetchone()
    finally:
        c.close()
    assert after == (
        "incomplete", before[0], before[1], before[2], 1, base.isoformat(),
        "temporary outage",
    )


def test_open_slot_gap_is_retried_and_resolved(sol):
    from src.pipeline import stream_health

    stream_health.observe("solana", "pump_fun_launches", cursor=10,
                          expect_contiguous=True)
    stream_health.observe("solana", "pump_fun_launches", cursor=12,
                          expect_contiguous=True)

    class Rpc:
        def call(self, method, params):
            if method == "getSlot":
                return 12
            if method == "getFirstAvailableBlock":
                return 0
            assert method == "getBlocks" and params[:2] == [11, 11]
            return []

    assert sol.retry_open_gaps(Rpc()) == {
        "attempted": 1, "recovered": 1, "progressed": 0, "failed": 0}
    assert stream_health.open_gaps("solana", "pump_fun_launches") == []
    assert stream_health.snapshot(stale_after_seconds=60)[0]["status"] == "live"


def test_short_slot_gap_is_backfilled_and_long_gap_stays_open(sol):
    tx = _raw_transaction()

    class Rpc:
        def __init__(self):
            self.slots = []

        def call(self, method, params):
            if method == "getSlot":
                return 12
            if method == "getFirstAvailableBlock":
                return 0
            if method == "getBlocks":
                assert params[0] == params[1]
                return [11] if params[0] == 11 else []
            assert method == "getBlock"
            self.slots.append(params[0])
            assert params[1]["encoding"] == "json"
            return {"transactions": [tx]}

    rpc = Rpc()
    assert sol.backfill_slots(10, 12, rpc=rpc) is True
    assert rpc.slots == [11]
    c = sol._conn()
    try:
        assert c.execute("SELECT signature,slot,transaction_index,evidence_state "
                         "FROM raw_launches").fetchone() == ("sig-1", 11, 0, "complete")
    finally:
        c.close()
    assert sol.backfill_slots(1, sol.MAX_BACKFILL_SLOTS + 1, rpc=rpc) is False


def test_long_slot_gap_checkpoints_one_verified_slot_per_tick(sol):
    from src.pipeline import stream_health

    stream_health.observe("solana", "pump_fun_launches", cursor=10,
                          expect_contiguous=True)
    stream_health.observe("solana", "pump_fun_launches", cursor=30,
                          expect_contiguous=True)

    class Rpc:
        def __init__(self):
            self.blocks = []

        def call(self, method, params):
            if method == "getSlot":
                return 100
            if method == "getFirstAvailableBlock":
                return 0
            if method == "getBlocks":
                assert params[0] == params[1]
                return [params[0]]
            assert method == "getBlock"
            self.blocks.append(params[0])
            return {"transactions": []}

    rpc = Rpc()
    first = sol.retry_open_gaps(rpc, slot_budget=sol.MAX_BACKFILL_SLOTS)
    assert first == {"attempted": 1, "recovered": 0, "progressed": 1, "failed": 0}
    gap = stream_health.open_gaps("solana", "pump_fun_launches")[0]
    assert (gap["from_cursor"], gap["to_cursor"]) == (12, 29)
    assert rpc.blocks == [11]

    second = sol.retry_open_gaps(rpc, slot_budget=sol.MAX_BACKFILL_SLOTS)
    assert second == {"attempted": 1, "recovered": 0, "progressed": 1, "failed": 0}
    gap = stream_health.open_gaps("solana", "pump_fun_launches")[0]
    assert (gap["from_cursor"], gap["to_cursor"]) == (13, 29)
    assert rpc.blocks == [11, 12]


def test_default_gap_retry_budget_caps_block_rpc_load(sol):
    from src.pipeline import stream_health

    stream_health.observe("solana", "pump_fun_launches", cursor=10,
                          expect_contiguous=True)
    stream_health.observe("solana", "pump_fun_launches", cursor=30,
                          expect_contiguous=True)

    class Rpc:
        def __init__(self):
            self.blocks = []

        def call(self, method, params):
            if method == "getSlot":
                return 100
            if method == "getFirstAvailableBlock":
                return 0
            if method == "getBlocks":
                return list(range(params[0], params[1] + 1))
            assert method == "getBlock"
            self.blocks.append(params[0])
            return {"transactions": []}

    rpc = Rpc()
    result = sol.retry_open_gaps(rpc)
    assert result == {"attempted": 1, "recovered": 0, "progressed": 1, "failed": 0}
    assert rpc.blocks == [11]
    gap = stream_health.open_gaps("solana", "pump_fun_launches")[0]
    assert gap["from_cursor"] == 12


def test_failed_gap_recovery_is_deferred_but_remains_fail_visible(sol):
    from src.pipeline import stream_health

    stream_health.observe("solana", "pump_fun_launches", cursor=10,
                          expect_contiguous=True)
    stream_health.observe("solana", "pump_fun_launches", cursor=12,
                          expect_contiguous=True)

    class Rpc:
        def call(self, method, params):
            raise RuntimeError("public RPC response was truncated")

    assert sol.retry_open_gaps(Rpc()) == {
        "attempted": 1, "recovered": 0, "progressed": 0, "failed": 1,
    }
    assert stream_health.open_gaps("solana", "pump_fun_launches") == []
    health = stream_health.snapshot(stale_after_seconds=60)[0]
    assert health["status"] == "degraded"
    assert health["open_gaps"] == 1
    assert health["deferred_gaps"] == 1
    assert health["next_gap_retry_at"] is not None


def test_unresolved_raw_create_is_retained_and_does_not_block_the_gap(sol):
    from src.pipeline import stream_health

    stream_health.observe("solana", "pump_fun_launches", cursor=10,
                          expect_contiguous=True)
    stream_health.observe("solana", "pump_fun_launches", cursor=12,
                          expect_contiguous=True)
    unresolved = _raw_transaction()
    unresolved["transaction"]["message"]["instructions"][0]["programIdIndex"] = 999

    class Rpc:
        def call(self, method, params):
            if method == "getSlot":
                return 100
            if method == "getFirstAvailableBlock":
                return 0
            if method == "getBlocks":
                return [11]
            assert method == "getBlock"
            return {"transactions": [unresolved]}

    assert sol.retry_open_gaps(Rpc()) == {
        "attempted": 1, "recovered": 1, "progressed": 0, "failed": 0,
    }
    assert stream_health.open_gaps("solana", "pump_fun_launches") == []
    c = sol._conn()
    try:
        row = c.execute(
            "SELECT signature,evidence_state,creator,mint,hydration_error "
            "FROM raw_launches"
        ).fetchone()
    finally:
        c.close()
    assert row[:4] == ("sig-1", "incomplete", None, None)
    assert "did not prove" in row[4]

    class HydrationRpc:
        def call(self, method, params):
            assert method == "getTransaction"
            return _transaction()

    assert sol.rehydrate_pending(HydrationRpc(), include_incomplete=True) == {
        "attempted": 1, "completed": 1, "incomplete": 0,
        "rpc_failed": 0,
    }
    c = sol._conn()
    try:
        assert c.execute(
            "SELECT evidence_state,creator,mint FROM raw_launches"
        ).fetchone() == ("complete", "creator", "mint")
    finally:
        c.close()


def test_websocket_runner_never_backfills_or_hydrates_inline(sol):
    class Rpc:
        def __init__(self):
            self.calls = 0

        def call(self, method, params):
            self.calls += 1
            raise AssertionError("websocket reader must not call RPC")

    rpc = Rpc()
    runner = sol.build_runner(rpc=rpc, socket_factory=lambda: None)
    assert runner.backfill is None

    event = sol.parse_message(json.dumps(_notification()))
    runner.on_event(event.payload)
    assert rpc.calls == 0
    c = sol._conn()
    try:
        assert c.execute(
            "SELECT evidence_state,hydrated_at FROM raw_launches"
        ).fetchone() == ("raw_only", None)
    finally:
        c.close()


def test_maintenance_recovers_gap_before_bounded_hydration(sol, monkeypatch):
    order = []
    monkeypatch.setattr(
        sol, "retry_open_gaps",
        lambda rpc: order.append("gap") or {
            "attempted": 0, "recovered": 0, "progressed": 0, "failed": 0,
        },
    )

    def rehydrate(rpc, *, limit):
        order.append(("hydrate", limit))
        return {"attempted": 0, "completed": 0, "failed": 0}

    monkeypatch.setattr(sol, "rehydrate_pending", rehydrate)

    class StopAfterOneTick:
        def is_set(self):
            return False

        def wait(self, timeout):
            return True

    sol._rehydrate_loop(StopAfterOneTick(), object(), interval_seconds=1)
    assert order == ["gap", ("hydrate", 5)]


def test_unfinalized_or_pruned_slot_gap_stays_open(sol):
    class UnfinalizedRpc:
        def call(self, method, params):
            assert method == "getSlot"
            return 10

    assert sol.backfill_slots(11, 11, rpc=UnfinalizedRpc()) is False

    class PrunedRpc:
        def call(self, method, params):
            if method == "getSlot":
                return 100
            assert method == "getFirstAvailableBlock"
            return 20

    assert sol.backfill_slots(11, 11, rpc=PrunedRpc()) is False


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
    summary = sol.qualification_summary(
        now=now,
        ledger_readback=lambda ledger_id, mint: ledger_id == "ledger-1" and mint == "mint",
    )
    assert summary["raw_total"] == 1
    assert summary["evidence"] == {"complete": 1}
    assert summary["qualification"] == {"qualified_recorded": 1}
    assert summary["traceability"]["traceable_unique_ledger_events"] == 1
    assert summary["traceability"]["orphan_rows"] == 0


def test_qualification_summary_quarantines_unreadable_ledger_ids(sol):
    now = datetime(2026, 7, 15, 12, tzinfo=timezone.utc)
    first = sol.parse_message(_notification())
    sol.persist(first.payload, transaction=_transaction())
    second_raw = _notification()
    second_raw["params"]["result"]["value"]["signature"] = "sig-2"
    second = sol.parse_message(second_raw)
    second_tx = _transaction()
    second_tx["transaction"]["signatures"] = ["sig-2"]
    sol.persist(second.payload, transaction=second_tx)
    sol.set_qualification("sig-1", "qualified_recorded",
                          ledger_event_id="ledger-ok", at=now)
    sol.set_qualification("sig-2", "qualified_recorded",
                          ledger_event_id="ledger-missing", at=now)

    summary = sol.qualification_summary(
        now=now,
        ledger_readback=lambda ledger_id, _mint: ledger_id == "ledger-ok",
    )

    assert summary["raw_qualification_states"] == {"qualified_recorded": 2}
    assert summary["qualification"] == {
        "qualified_recorded": 1,
        "ledger_orphan": 1,
    }
    assert summary["traceability"] == {
        "state": "partial",
        "raw_marked_recorded_rows": 2,
        "traceable_rows": 1,
        "traceable_unique_ledger_events": 1,
        "orphan_rows": 1,
        "orphan_unique_ledger_ids": 1,
        "missing_ledger_id_rows": 0,
        "quarantined_state_rows": 0,
        "readback_error_rows": 0,
    }


def test_unknown_qualification_state_is_rejected(sol):
    with pytest.raises(ValueError, match="unknown qualification state"):
        sol.set_qualification("sig-1", "pretend_success")
