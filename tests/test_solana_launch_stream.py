"""Solana launch evidence remains distinct from a qualified opportunity."""
from __future__ import annotations

import io
import json
import sqlite3
import threading
import urllib.error
from copy import deepcopy
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import pytest


@pytest.fixture
def sol(tmp_path, monkeypatch):
    from src.ops.stream_disk_guard import DiskStateGuard
    from src.pipeline import solana_launch_stream
    from src.pipeline import stream_health

    monkeypatch.setattr(solana_launch_stream, "DB", tmp_path / "launches.db")
    monkeypatch.setattr(stream_health, "DB", tmp_path / "stream-health.db")
    monkeypatch.setattr(
        solana_launch_stream.stream_disk_guard, "GUARD",
        DiskStateGuard(probe=lambda: {"state": "ok"}),
    )
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


def _gap_stats(attempted: int, *, recovered: int = 0, progressed: int = 0,
               failed: int = 0, pressure_kind: str | None = None,
               retry_after_seconds: int | None = None,
               deadline_stopped: int = 0,
               deadline_exhausted: bool = False) -> dict:
    return {
        "attempted": attempted, "recovered": recovered,
        "progressed": progressed, "failed": failed,
        "pressure_kind": pressure_kind,
        "retry_after_seconds": retry_after_seconds,
        "deadline_stopped": deadline_stopped,
        "deadline_exhausted": deadline_exhausted,
    }


def _hydration_stats(attempted: int, *, completed: int = 0,
                     incomplete: int = 0, unavailable: int = 0,
                     rpc_failed: int = 0, pressure_failed: int = 0,
                     persistence_failed: int = 0,
                     persistence_error: str | None = None,
                     pressure_kind: str | None = None,
                     retry_after_seconds: int | None = None,
                     deadline_stopped: int = 0,
                     deadline_exhausted: bool = False) -> dict:
    return {
        "attempted": attempted, "completed": completed,
        "incomplete": incomplete, "unavailable": unavailable,
        "rpc_failed": rpc_failed, "pressure_failed": pressure_failed,
        "persistence_failed": persistence_failed,
        "persistence_error": persistence_error,
        "pressure_kind": pressure_kind,
        "retry_after_seconds": retry_after_seconds,
        "deadline_stopped": deadline_stopped,
        "deadline_exhausted": deadline_exhausted,
    }


def test_subscriptions_use_standard_solana_methods(sol):
    requests = sol.subscribe_requests()
    assert requests[0]["method"] == "logsSubscribe"
    assert requests[0]["params"][0] == {"mentions": [sol.PUMP_FUN_PROGRAM]}
    assert requests[1]["method"] == "slotSubscribe"
    assert all(request["method"] != "transactionSubscribe" for request in requests)


def test_critical_disk_guard_blocks_runner_before_solana_persist(sol, monkeypatch):
    from src.ops.stream_disk_guard import DiskStateGuard, StreamDiskCritical

    monkeypatch.setattr(
        sol.stream_disk_guard, "GUARD",
        DiskStateGuard(probe=lambda: {"state": "critical"}),
    )
    persisted = []
    monkeypatch.setattr(sol, "persist", lambda payload: persisted.append(payload))
    runner = sol.build_runner(rpc=object(), socket_factory=lambda: None)

    with pytest.raises(StreamDiskCritical):
        runner.on_event({"kind": "slot", "slot": 123})
    assert persisted == []


def test_critical_disk_guard_pauses_maintenance_before_rpc_or_db_work(
        sol, monkeypatch):
    from src.ops.stream_disk_guard import DiskStateGuard

    monkeypatch.setattr(
        sol.stream_disk_guard, "GUARD",
        DiskStateGuard(probe=lambda: {"state": "critical"}),
    )
    monkeypatch.setattr(
        sol, "retry_open_gaps",
        lambda *args, **kwargs: pytest.fail("critical maintenance read gap DB"),
    )
    monkeypatch.setattr(
        sol, "rehydrate_pending",
        lambda *args, **kwargs: pytest.fail("critical maintenance touched launch DB"),
    )
    reports = []
    monkeypatch.setattr(
        sol, "_report_maintenance",
        lambda status, error=None: reports.append((status, error)),
    )

    class Rpc:
        calls = 0

        def call(self, method, params):
            self.calls += 1
            raise AssertionError("critical maintenance called RPC")

    class StopAfterOneTick:
        waits = []

        def is_set(self):
            return False

        def wait(self, timeout):
            self.waits.append(timeout)
            return True

    stop = StopAfterOneTick()
    rpc = Rpc()
    sol._rehydrate_loop(stop, rpc, interval_seconds=1, monotonic=lambda: 0.0)

    assert rpc.calls == 0
    assert reports == [("degraded", "workspace disk critical; maintenance paused")]
    assert stop.waits == [1.0]


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
        "qualification_attempt_count", "qualification_lease_token",
        "qualification_lease_started_at", "qualification_lease_expires_at",
        "qualification_next_retry_at", "qualification_last_outcome_kind",
        "capture_mode", "captured_at", "block_time", "source_provider",
        "source_conflict_at", "source_conflict_reason", "reconciliation_state",
        "reconciliation_epoch_id", "reconciled_at",
    )
    assert all(schema[-len(expected):] == expected for schema in schemas)


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


def test_live_capture_and_exact_backfill_are_distinct_append_only_observations(sol):
    captured = datetime(2026, 7, 20, 12, tzinfo=timezone.utc)
    finalized = captured + timedelta(minutes=1)
    event = sol.parse_message(_notification())
    sol.persist(
        event.payload, transaction=_transaction(), capture_mode="live_ws",
        captured_at=captured, source_provider="solana_rpc:live.example",
    )
    c = sol._conn()
    try:
        first = c.execute(
            """SELECT detected_at,raw_payload_hash,hydration_payload_hash,
                      capture_mode,captured_at,transaction_index
               FROM raw_launches"""
        ).fetchone()
    finally:
        c.close()

    replay = deepcopy(event.payload)
    replay["transaction_index"] = 7
    sol.persist(
        replay, transaction=_transaction(), capture_mode="gap_backfill",
        captured_at=finalized, block_time=captured - timedelta(seconds=5),
        source_provider="solana_rpc:archive.example",
    )
    c = sol._conn()
    try:
        current = c.execute(
            """SELECT detected_at,raw_payload_hash,hydration_payload_hash,
                      capture_mode,captured_at,transaction_index,block_time
               FROM raw_launches"""
        ).fetchone()
        observations = c.execute(
            """SELECT capture_mode,source_provider,canonical_match
               FROM raw_launch_observations ORDER BY id"""
        ).fetchall()
        hydration_sources = c.execute(
            "SELECT source_provider FROM hydration_observations ORDER BY id"
        ).fetchall()
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            c.execute("UPDATE raw_launch_observations SET canonical_match=0")
    finally:
        c.close()

    assert current[:5] == first[:5]
    assert current[5:] == (
        7, (captured - timedelta(seconds=5)).isoformat(),
    )
    assert observations == [
        ("live_ws", "solana_rpc:live.example", 1),
        ("gap_backfill", "solana_rpc:archive.example", 1),
    ]
    assert hydration_sources == [
        ("solana_rpc:live.example",), ("solana_rpc:archive.example",),
    ]


def test_conflicting_signature_never_rewrites_first_raw_capture(sol):
    captured = datetime(2026, 7, 20, 12, tzinfo=timezone.utc)
    event = sol.parse_message(_notification())
    sol.persist(event.payload, transaction=_transaction(), captured_at=captured)
    c = sol._conn()
    try:
        before = c.execute(
            "SELECT slot,event_type,logs,raw_payload_hash FROM raw_launches"
        ).fetchone()
    finally:
        c.close()
    conflict = deepcopy(event.payload)
    conflict["logs"] = [*conflict["logs"], "Program log: forged"]

    with pytest.raises(sol.SourceEvidenceConflict, match="conflicting"):
        sol.persist(
            conflict, capture_mode="gap_backfill",
            captured_at=captured + timedelta(minutes=1),
            source_provider="solana_rpc:archive.example",
        )

    c = sol._conn()
    try:
        after = c.execute(
            """SELECT slot,event_type,logs,raw_payload_hash,evidence_state,
                      qualification_state FROM raw_launches"""
        ).fetchone()
        matches = c.execute(
            "SELECT canonical_match FROM raw_launch_observations ORDER BY id"
        ).fetchall()
    finally:
        c.close()
    assert after[:4] == before
    assert after[4:] == ("source_conflict", "provenance_conflict")
    assert matches == [(1,), (0,)]


def test_complete_hydration_identity_cannot_be_overwritten(sol):
    event = sol.parse_message(_notification())
    sol.persist(event.payload, transaction=_transaction())
    c = sol._conn()
    try:
        before = c.execute(
            "SELECT creator,mint,hydration_payload_hash FROM raw_launches"
        ).fetchone()
    finally:
        c.close()
    conflict = deepcopy(_transaction())
    conflict["transaction"]["message"]["accountKeys"][0]["pubkey"] = "attacker"
    conflict["transaction"]["message"]["instructions"][0]["accounts"][2] = "attacker"

    with pytest.raises(sol.SourceEvidenceConflict, match="identity conflicts"):
        sol._set_hydration("sig-1", conflict, None)

    c = sol._conn()
    try:
        after = c.execute(
            """SELECT creator,mint,hydration_payload_hash,evidence_state,
                      qualification_state FROM raw_launches"""
        ).fetchone()
        states = c.execute(
            "SELECT evidence_state FROM hydration_observations ORDER BY id"
        ).fetchall()
    finally:
        c.close()
    assert after[:3] == before
    assert after[3:] == ("source_conflict", "provenance_conflict")
    assert states == [("complete",), ("source_conflict",)]


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

    assert sol.retry_open_gaps(Rpc()) == _gap_stats(1, recovered=1)
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
    assert sol.rehydrate_pending(RecoveringRpc()) == _hydration_stats(
        1, completed=1,
    )
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
            tx["slot"] = 100
            return tx

    result = sol.rehydrate_pending(Rpc(), limit=2, now=base + timedelta(minutes=1))

    assert calls == ["a-tie", "b-tie"]
    assert result == _hydration_stats(2, completed=2)
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
                raise sol.RpcPressureError("rate limited", kind="rate_limited")
            return _transaction()

    rpc = FlakyRpc()
    first = sol.rehydrate_pending(rpc, now=base)
    assert first == _hydration_stats(
        1, pressure_failed=1, pressure_kind="rate_limited",
    )
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
        "by_state": {"raw_only": 0, "rpc_unavailable": 1, "incomplete": 0},
        "due": 0, "deferred": 1,
        "oldest_pending_at": (base - timedelta(minutes=1)).isoformat(),
        "oldest_pending_age_seconds": 60, "max_retry_count": 1,
    }

    assert sol.rehydrate_pending(rpc, now=base + timedelta(seconds=59))["attempted"] == 0
    second = sol.rehydrate_pending(rpc, now=base + timedelta(seconds=60))
    assert second["pressure_kind"] == "rate_limited" and rpc.calls == 2
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
    assert recovered == _hydration_stats(1, completed=1)
    c = sol._conn()
    try:
        reset = c.execute(
            "SELECT evidence_state,hydration_retry_count,hydration_next_retry_at "
            "FROM raw_launches"
        ).fetchone()
    finally:
        c.close()
    assert reset == ("complete", 0, None)


def test_incomplete_identity_is_visible_in_hydration_backlog(sol):
    now = datetime(2026, 7, 15, 12, tzinfo=timezone.utc)
    raw = sol.parse_message(_notification())
    sol.persist(raw.payload, transaction={"meta": {"err": None}})

    hydration = sol.qualification_summary(now=now)["hydration"]

    assert hydration["state"] == "backlogged"
    assert hydration["pending_total"] == 1
    assert hydration["by_state"] == {
        "raw_only": 0, "rpc_unavailable": 0, "incomplete": 1,
    }


def test_hydration_pressure_stops_batch_and_honors_retry_after(sol):
    base = datetime(2026, 7, 15, 12, tzinfo=timezone.utc)
    for signature in ("a-pressure", "b-unattempted"):
        raw = _notification()
        raw["params"]["result"]["value"]["signature"] = signature
        sol.persist(sol.parse_message(raw).payload)
    c = sol._conn()
    try:
        c.execute("UPDATE raw_launches SET detected_at=?", (base.isoformat(),))
        c.commit()
    finally:
        c.close()

    class Rpc:
        def __init__(self):
            self.calls = 0

        def call(self, method, params):
            self.calls += 1
            raise sol.RpcPressureError(
                "HTTP 429", kind="rate_limited", retry_after_seconds=180,
            )

    rpc = Rpc()
    result = sol.rehydrate_pending(rpc, limit=2, now=base)

    assert result == _hydration_stats(
        1, pressure_failed=1, pressure_kind="rate_limited",
        retry_after_seconds=180,
    )
    assert rpc.calls == 1
    c = sol._conn()
    try:
        rows = c.execute(
            """SELECT signature,evidence_state,hydration_next_retry_at
               FROM raw_launches ORDER BY signature"""
        ).fetchall()
    finally:
        c.close()
    assert rows == [
        ("a-pressure", "rpc_unavailable", (base + timedelta(seconds=180)).isoformat()),
        ("b-unattempted", "raw_only", None),
    ]


def test_hydration_pressure_survives_persistence_failure_and_opens_circuit(
        sol, monkeypatch):
    from src.pipeline import stream_health

    sol.persist(sol.parse_message(_notification()).payload)
    captured = []
    persistence_logs = []
    original_rehydrate = sol.rehydrate_pending

    def hydrate(*args, **kwargs):
        result = original_rehydrate(*args, **kwargs)
        captured.append(result)
        return result

    monkeypatch.setattr(
        sol, "retry_open_gaps", lambda _rpc, **kwargs: _gap_stats(0),
    )
    monkeypatch.setattr(sol, "rehydrate_pending", hydrate)
    monkeypatch.setattr(
        sol, "_set_hydration",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            RuntimeError("launch DB locked")
        ),
    )
    monkeypatch.setattr(
        sol.logger, "exception",
        lambda event, **kwargs: persistence_logs.append((event, kwargs)),
    )

    class Rpc:
        calls = 0

        def call(self, method, params):
            self.calls += 1
            raise sol.RpcPressureError(
                "HTTP 429", kind="rate_limited", retry_after_seconds=180,
            )

    class StopAfterOneTick:
        def is_set(self):
            return False

        def wait(self, timeout):
            return True

    rpc = Rpc()
    sol._rehydrate_loop(StopAfterOneTick(), rpc, interval_seconds=1)

    assert rpc.calls == 1
    assert captured == [_hydration_stats(
        1, pressure_failed=1, persistence_failed=1,
        persistence_error="launch DB locked", pressure_kind="rate_limited",
        retry_after_seconds=180,
    )]
    assert persistence_logs[0][0] == "solana_launch_hydration_persist_failed"
    maintenance = next(
        item for item in stream_health.snapshot()
        if item["stream"] == sol.MAINTENANCE_STREAM
    )
    assert maintenance["status"] == "degraded"
    assert maintenance["last_error"] == (
        "hydration RPC pressure: rate_limited; cooldown 180s"
    )


def test_unavailable_transaction_does_not_trip_pressure_or_stop_batch(sol):
    base = datetime(2026, 7, 15, 12, tzinfo=timezone.utc)
    for signature in ("a-missing", "b-complete"):
        raw = _notification()
        raw["params"]["result"]["value"]["signature"] = signature
        sol.persist(sol.parse_message(raw).payload)
    c = sol._conn()
    try:
        c.execute("UPDATE raw_launches SET detected_at=?", (base.isoformat(),))
        c.commit()
    finally:
        c.close()

    class Rpc:
        def __init__(self):
            self.calls = []

        def call(self, method, params):
            signature = params[0]
            self.calls.append(signature)
            if signature == "a-missing":
                return None
            tx = _transaction()
            tx["transaction"]["signatures"] = [signature]
            return tx

    rpc = Rpc()
    assert sol.rehydrate_pending(rpc, limit=2, now=base) == _hydration_stats(
        2, completed=1, unavailable=1,
    )
    assert rpc.calls == ["a-missing", "b-complete"]


def test_hydration_deadline_stops_before_starting_more_rows(sol):
    base = datetime(2026, 7, 15, 12, tzinfo=timezone.utc)
    for signature in ("a-first", "b-second", "c-not-started"):
        raw = _notification()
        raw["params"]["result"]["value"]["signature"] = signature
        sol.persist(sol.parse_message(raw).payload)
    c = sol._conn()
    try:
        c.execute("UPDATE raw_launches SET detected_at=?", (base.isoformat(),))
        c.commit()
    finally:
        c.close()

    class Clock:
        value = 0.0

        def __call__(self):
            return self.value

    clock = Clock()

    class Rpc:
        def __init__(self):
            self.calls = []

        def call(self, method, params):
            signature = params[0]
            self.calls.append(signature)
            clock.value += 11
            tx = _transaction()
            tx["transaction"]["signatures"] = [signature]
            return tx

    rpc = Rpc()
    result = sol.rehydrate_pending(
        rpc, limit=3, now=base, deadline=20, monotonic=clock,
    )
    assert result == _hydration_stats(
        2, completed=2, deadline_exhausted=True,
    )
    assert rpc.calls == ["a-first", "b-second"]


@pytest.mark.parametrize("outcome", ["unavailable", "rpc_failed"])
def test_hydration_failure_crossing_deadline_is_reported(sol, outcome):
    base = datetime(2026, 7, 15, 12, tzinfo=timezone.utc)
    sol.persist(sol.parse_message(_notification()).payload)

    class Clock:
        value = 0.0

        def __call__(self):
            return self.value

    clock = Clock()

    class Rpc:
        def call(self, method, params):
            assert method == "getTransaction"
            clock.value = 21
            if outcome == "unavailable":
                return None
            raise RuntimeError("temporary transport failure")

    expected = {outcome: 1}
    assert sol.rehydrate_pending(
        Rpc(), limit=1, now=base, deadline=20, monotonic=clock,
    ) == _hydration_stats(
        1, deadline_exhausted=True, **expected,
    )


def test_rpc_pressure_classification_is_narrow_and_retry_after_is_bounded(sol):
    now = datetime(2026, 7, 15, 12, tzinfo=timezone.utc)
    assert sol._retry_after_seconds("17", now=now) == 17
    assert sol._retry_after_seconds(
        "Wed, 15 Jul 2026 12:00:19 GMT", now=now,
    ) == 19
    assert sol._as_rpc_pressure(TimeoutError("timed out")).kind == "timeout"
    assert sol._as_rpc_pressure(
        urllib.error.URLError(TimeoutError("timed out")),
    ).kind == "timeout"
    assert sol._as_rpc_pressure(
        urllib.error.URLError("dns failure"),
    ).kind == "transport"
    ordinary_http = urllib.error.HTTPError(
        "https://rpc.invalid", 503, "unavailable", {}, None,
    )
    assert sol._as_rpc_pressure(ordinary_http).kind == "transport"
    assert sol._circuit_cooldown_seconds(None, 1) == 60
    assert sol._circuit_cooldown_seconds(None, 2) == 120
    assert sol._circuit_cooldown_seconds(180, 1) == 180


def test_deadline_rpc_clamps_production_network_timeout(sol, monkeypatch):
    timeouts = []

    def response(_request, timeout):
        timeouts.append(timeout)
        return io.BytesIO(b'{"jsonrpc":"2.0","result":42}')

    monkeypatch.setattr(sol.urllib.request, "urlopen", response)
    rpc = sol.JsonRpc("https://solana.invalid", timeout=15)
    assert sol._rpc_call(
        rpc, "getSlot", [], deadline=10, monotonic=lambda: 7,
    ) == 42
    assert timeouts == [3]
    with pytest.raises(sol.MaintenanceDeadlineExceeded):
        sol._rpc_call(
            rpc, "getSlot", [], deadline=10, monotonic=lambda: 10,
        )
    assert timeouts == [3]

    class Clock:
        value = 0.0

        def __call__(self):
            return self.value

    clock = Clock()

    def expires_during_call(method, params, *, timeout=None):
        clock.value = 20
        raise sol.RpcPressureError("socket timeout", kind="timeout")

    monkeypatch.setattr(rpc, "call", expires_during_call)
    with pytest.raises(sol.MaintenanceDeadlineExceeded) as expired:
        sol._rpc_call(
            rpc, "getBlock", [], deadline=20, monotonic=clock,
        )
    assert expired.value.work_started is True


def test_adaptive_budgets_ramp_only_after_full_clean_cycles_and_reset(sol):
    assert sol._adjust_gap_budget(
        1, 0, _gap_stats(1, recovered=1),
    ) == (1, 1)
    assert sol._adjust_gap_budget(
        1, 1, _gap_stats(1, recovered=1),
    ) == (2, 0)

    gap_budget = 1
    gap_streak = 0
    used_gap_budgets = []
    for _ in range(4):
        used_gap_budgets.append(gap_budget)
        gap_budget, gap_streak = sol._adjust_gap_budget(
            gap_budget, gap_streak,
            _gap_stats(gap_budget, progressed=gap_budget),
        )
    used_gap_budgets.append(gap_budget)
    assert used_gap_budgets == [1, 1, 2, 2, 4]
    assert sol._adjust_gap_budget(
        4, 1, _gap_stats(1, failed=1),
    ) == (1, 0)

    hydration_limit = 5
    hydration_streak = 0
    used_hydration_limits = []
    for _ in range(6):
        used_hydration_limits.append(hydration_limit)
        hydration_limit, hydration_streak = sol._adjust_hydration_limit(
            hydration_limit, hydration_streak,
            _hydration_stats(hydration_limit, completed=hydration_limit),
        )
    used_hydration_limits.append(hydration_limit)
    assert used_hydration_limits == [5, 5, 10, 10, 15, 15, 20]
    assert sol._adjust_hydration_limit(
        20, 1, _hydration_stats(1, rpc_failed=1),
    ) == (5, 0)


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

    assert sol.rehydrate_pending(Rpc()) == _hydration_stats(1, incomplete=1)


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
    assert result == _hydration_stats(1, rpc_failed=1)
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
            if method == "getBlocks":
                assert params[:2] == [11, 12]
                return [12]
            assert method == "getBlock" and params[0] == 12
            assert params[1]["transactionDetails"] == "none"
            return {"parentSlot": 10}

    assert sol.retry_open_gaps(Rpc()) == _gap_stats(1, recovered=1)
    assert stream_health.open_gaps("solana", "pump_fun_launches") == []
    assert stream_health.snapshot(stale_after_seconds=60)[0]["status"] == "live"


def test_small_gaps_are_served_before_a_large_backlog_gap(sol):
    from src.pipeline import stream_health

    t0 = datetime(2026, 7, 14, 12, tzinfo=timezone.utc)
    stream_health.observe("solana", "pump_fun_launches", cursor=10,
                          received_at=t0, expect_contiguous=True)
    stream_health.observe("solana", "pump_fun_launches", cursor=5011,
                          received_at=t0, expect_contiguous=True)
    stream_health.observe("solana", "pump_fun_launches", cursor=5013,
                          received_at=t0 + timedelta(seconds=1),
                          expect_contiguous=True)
    stream_health.observe("solana", "pump_fun_launches", cursor=5015,
                          received_at=t0 + timedelta(seconds=2),
                          expect_contiguous=True)

    class Rpc:
        def __init__(self):
            self.block_ranges = []

        def call(self, method, params):
            if method == "getSlot":
                return 5015
            if method == "getFirstAvailableBlock":
                return 0
            if method == "getBlocks":
                self.block_ranges.append(params[:2])
                assert params[0] in (5012, 5014)
                return [params[0] + 1]
            assert method == "getBlock" and params[0] in (5013, 5015)
            return {"parentSlot": params[0] - 2}

    rpc = Rpc()
    assert sol.retry_open_gaps(rpc, slot_budget=2) == _gap_stats(2, recovered=2)
    assert rpc.block_ranges == [[5012, 5015], [5014, 5015]]
    remaining = stream_health.open_gaps("solana", "pump_fun_launches")
    assert [(gap["from_cursor"], gap["to_cursor"]) for gap in remaining] == [
        (11, 5010)]


def test_four_skipped_slots_resolve_in_one_proof_budget_unit(sol):
    from src.pipeline import stream_health

    stream_health.observe("solana", "pump_fun_launches", cursor=10,
                          expect_contiguous=True)
    stream_health.observe("solana", "pump_fun_launches", cursor=15,
                          expect_contiguous=True)

    class Rpc:
        def __init__(self):
            self.calls = []

        def call(self, method, params):
            self.calls.append(method)
            if method == "getSlot":
                return 15
            if method == "getFirstAvailableBlock":
                return 0
            if method == "getBlocks":
                assert params[:2] == [11, 15]
                return [15]
            assert method == "getBlock" and params[0] == 15
            assert params[1]["transactionDetails"] == "none"
            return {"parentSlot": 10}

    rpc = Rpc()
    assert sol.retry_open_gaps(rpc) == _gap_stats(1, recovered=1)
    assert rpc.calls == [
        "getSlot", "getFirstAvailableBlock", "getBlocks", "getBlock",
    ]
    assert stream_health.open_gaps("solana", "pump_fun_launches") == []
    c = stream_health._conn()
    try:
        status, details = c.execute(
            "SELECT status,details FROM gaps"
        ).fetchone()
    finally:
        c.close()
    assert status == "resolved"
    proof = json.loads(details)
    assert proof["slot"] == 11
    assert proof["verified_through"] == 14
    assert proof["proof_next_slot"] == 15
    assert proof["proof_parent_slot"] == 10


def test_skipped_run_proof_is_clamped_to_the_open_gap_end(sol, monkeypatch):
    from src.pipeline import stream_health

    stream_health.observe("solana", "pump_fun_launches", cursor=10,
                          expect_contiguous=True)
    stream_health.observe("solana", "pump_fun_launches", cursor=13,
                          expect_contiguous=True)
    checkpoints = []
    advance = stream_health.advance_gap

    def record_checkpoint(gap_id, through_cursor, **kwargs):
        checkpoints.append(through_cursor)
        return advance(gap_id, through_cursor, **kwargs)

    monkeypatch.setattr(stream_health, "advance_gap", record_checkpoint)

    class Rpc:
        def call(self, method, params):
            if method == "getSlot":
                return 15
            if method == "getFirstAvailableBlock":
                return 0
            if method == "getBlocks":
                return [15]
            assert method == "getBlock" and params[0] == 15
            return {"parentSlot": 10}

    assert sol.retry_open_gaps(Rpc()) == _gap_stats(1, recovered=1)
    assert checkpoints == [12]
    c = stream_health._conn()
    try:
        details = c.execute("SELECT details FROM gaps").fetchone()[0]
    finally:
        c.close()
    assert json.loads(details)["verified_through"] == 14


def test_empty_getblocks_without_successor_proof_keeps_gap_open(sol):
    from src.pipeline import stream_health

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
            assert method == "getBlocks"
            return []

    assert sol.retry_open_gaps(Rpc()) == _gap_stats(1, failed=1)
    health = stream_health.snapshot(stale_after_seconds=60)[0]
    assert health["status"] == "degraded" and health["open_gaps"] == 1
    c = stream_health._conn()
    try:
        assert c.execute(
            "SELECT from_cursor,to_cursor,status FROM gaps"
        ).fetchone() == (11, 11, "open")
    finally:
        c.close()


def test_successor_parent_exposes_provider_omission_instead_of_resolving_gap(sol):
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
            if method == "getBlocks":
                return [12]
            assert method == "getBlock" and params[0] == 12
            return {"parentSlot": 11}

    assert sol.retry_open_gaps(Rpc()) == _gap_stats(1, failed=1)
    health = stream_health.snapshot(stale_after_seconds=60)[0]
    assert health["status"] == "degraded" and health["open_gaps"] == 1
    c = stream_health._conn()
    try:
        start, end, status, error = c.execute(
            "SELECT from_cursor,to_cursor,status,last_error FROM gaps"
        ).fetchone()
    finally:
        c.close()
    assert (start, end, status) == (11, 11, "open")
    assert "omitted produced" in error


def test_short_slot_gap_is_backfilled_and_long_gap_stays_open(sol):
    tx = _raw_transaction()

    class Rpc:
        def __init__(self):
            self.slots = []

        def call(self, method, params):
            if method == "getSlot":
                return 13
            if method == "getFirstAvailableBlock":
                return 0
            if method == "getBlocks":
                return [value for value in (11, 13)
                        if params[0] <= value <= params[1]]
            assert method == "getBlock"
            if params[1]["transactionDetails"] == "none":
                return {"parentSlot": 9 if params[0] == 11 else 11}
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


def test_long_slot_gap_checkpoints_each_verified_slot_within_budget(sol):
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
    first = sol.retry_open_gaps(rpc, slot_budget=sol.MAX_BACKFILL_SLOTS)
    assert first == _gap_stats(4, progressed=4)
    gap = stream_health.open_gaps("solana", "pump_fun_launches")[0]
    assert (gap["from_cursor"], gap["to_cursor"]) == (15, 29)
    assert rpc.blocks == [11, 12, 13, 14]

    second = sol.retry_open_gaps(rpc, slot_budget=sol.MAX_BACKFILL_SLOTS)
    assert second == _gap_stats(4, progressed=4)
    gap = stream_health.open_gaps("solana", "pump_fun_launches")[0]
    assert (gap["from_cursor"], gap["to_cursor"]) == (19, 29)
    assert rpc.blocks == [11, 12, 13, 14, 15, 16, 17, 18]


def test_gap_budget_stops_after_first_failure_without_touching_later_slots(
        sol, monkeypatch):
    from src.pipeline import stream_health

    stream_health.observe("solana", "pump_fun_launches", cursor=10,
                          expect_contiguous=True)
    stream_health.observe("solana", "pump_fun_launches", cursor=30,
                          expect_contiguous=True)
    calls = []

    def recover(slot, **kwargs):
        calls.append(slot)
        if slot == 12:
            raise RuntimeError("provider omitted block")
        return {
            "state": "produced", "slot": slot,
            "verified_through": slot, "launches": 0,
        }

    monkeypatch.setattr(sol, "_backfill_finalized_slot", recover)
    result = sol.retry_open_gaps(object(), slot_budget=4)

    assert result == _gap_stats(2, progressed=1, failed=1)
    assert calls == [11, 12]
    c = stream_health._conn()
    try:
        gap = c.execute(
            "SELECT from_cursor,to_cursor,status,last_error FROM gaps"
        ).fetchone()
    finally:
        c.close()
    assert gap[:3] == (12, 29, "open")
    assert "provider omitted" in gap[3]


def test_gap_deadline_checkpoints_completed_slot_then_stops(sol, monkeypatch):
    from src.pipeline import stream_health

    stream_health.observe("solana", "pump_fun_launches", cursor=10,
                          expect_contiguous=True)
    stream_health.observe("solana", "pump_fun_launches", cursor=30,
                          expect_contiguous=True)

    class Clock:
        value = 0.0

        def __call__(self):
            return self.value

    clock = Clock()
    calls = []

    def recover(slot, **kwargs):
        calls.append(slot)
        clock.value = 21
        return {
            "state": "produced", "slot": slot,
            "verified_through": slot, "launches": 0,
        }

    monkeypatch.setattr(sol, "_backfill_finalized_slot", recover)
    result = sol.retry_open_gaps(
        object(), slot_budget=4, deadline=20, monotonic=clock,
    )

    assert result == _gap_stats(
        1, progressed=1, deadline_exhausted=True,
    )
    assert calls == [11]
    assert stream_health.open_gaps(
        "solana", "pump_fun_launches",
    )[0]["from_cursor"] == 12


def test_gap_deadline_after_partial_slot_proof_remains_an_attempted_probe(sol):
    from src.pipeline import stream_health

    stream_health.observe("solana", "pump_fun_launches", cursor=10,
                          expect_contiguous=True)
    stream_health.observe("solana", "pump_fun_launches", cursor=12,
                          expect_contiguous=True)

    class Clock:
        value = 0.0

        def __call__(self):
            return self.value

    clock = Clock()

    class Rpc:
        calls = []

        def call(self, method, params):
            self.calls.append(method)
            assert method == "getSlot"
            clock.value = 20
            return 12

    rpc = Rpc()
    assert sol.retry_open_gaps(
        rpc, deadline=20, monotonic=clock,
    ) == _gap_stats(
        1, deadline_stopped=1, deadline_exhausted=True,
    )
    assert rpc.calls == ["getSlot"]
    assert stream_health.open_gaps(
        "solana", "pump_fun_launches",
    )[0]["from_cursor"] == 11


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
    assert result == _gap_stats(1, progressed=1)
    assert rpc.blocks == [11]
    gap = stream_health.open_gaps("solana", "pump_fun_launches")[0]
    assert gap["from_cursor"] == 12
    assert json.loads(gap["details"])["verified_through"] == 11


@pytest.mark.parametrize("evidence", [
    {"state": "produced", "slot": 11, "launches": 0},
    {"state": "produced", "slot": 11, "verified_through": True, "launches": 0},
    {"state": "produced", "slot": 11, "verified_through": 10, "launches": 0},
    {"state": "produced", "slot": 11, "verified_through": 11.0, "launches": 0},
    {"state": "unknown", "slot": 11, "verified_through": 11},
    {"state": "produced", "slot": 11, "verified_through": 12, "launches": 0},
    {"state": "produced", "slot": 12, "verified_through": 12, "launches": 0},
    {"state": "skipped_proven", "slot": 11, "verified_through": 14,
     "proof_next_slot": 15, "proof_parent_slot": 11},
    {"state": "skipped_proven", "slot": 11, "verified_through": 13,
     "proof_next_slot": 15, "proof_parent_slot": 10},
])
def test_gap_proof_requires_strict_monotonic_verified_through(
        sol, monkeypatch, evidence):
    from src.pipeline import stream_health

    stream_health.observe("solana", "pump_fun_launches", cursor=10,
                          expect_contiguous=True)
    stream_health.observe("solana", "pump_fun_launches", cursor=12,
                          expect_contiguous=True)
    monkeypatch.setattr(
        sol, "_backfill_finalized_slot", lambda _slot, **_kwargs: evidence,
    )

    assert sol.retry_open_gaps(object()) == _gap_stats(1, failed=1)
    c = stream_health._conn()
    try:
        start, end, status, error = c.execute(
            "SELECT from_cursor,to_cursor,status,last_error FROM gaps"
        ).fetchone()
    finally:
        c.close()
    assert (start, end, status) == (11, 11, "open")
    assert any(fragment in error for fragment in (
        "invalid verified_through", "crossed its slot",
        "proof state is invalid", "proof contract is inconsistent",
    ))


def test_failed_gap_recovery_is_deferred_but_remains_fail_visible(sol):
    from src.pipeline import stream_health

    stream_health.observe("solana", "pump_fun_launches", cursor=10,
                          expect_contiguous=True)
    stream_health.observe("solana", "pump_fun_launches", cursor=12,
                          expect_contiguous=True)

    class Rpc:
        def call(self, method, params):
            raise RuntimeError("public RPC response was truncated")

    assert sol.retry_open_gaps(Rpc()) == _gap_stats(1, failed=1)
    assert stream_health.open_gaps("solana", "pump_fun_launches") == []
    health = stream_health.snapshot(stale_after_seconds=60)[0]
    assert health["status"] == "degraded"
    assert health["open_gaps"] == 1
    assert health["deferred_gaps"] == 1
    assert health["next_gap_retry_at"] is not None


def test_gap_defer_failure_preserves_original_rpc_pressure(sol, monkeypatch):
    from src.pipeline import stream_health

    stream_health.observe("solana", "pump_fun_launches", cursor=10,
                          expect_contiguous=True)
    stream_health.observe("solana", "pump_fun_launches", cursor=12,
                          expect_contiguous=True)
    defer_errors = []
    monkeypatch.setattr(
        stream_health, "defer_gap",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            RuntimeError("health store locked")
        ),
    )
    monkeypatch.setattr(
        sol.logger, "exception",
        lambda event, **kwargs: defer_errors.append((event, kwargs)),
    )

    class Rpc:
        def call(self, method, params):
            raise sol.RpcPressureError(
                "HTTP 429", kind="rate_limited", retry_after_seconds=180,
            )

    assert sol.retry_open_gaps(Rpc()) == _gap_stats(
        1, failed=1, pressure_kind="rate_limited",
        retry_after_seconds=180,
    )
    assert defer_errors[0][0] == "solana_launch_gap_defer_failed"
    assert defer_errors[0][1]["pressure_kind"] == "rate_limited"


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

    assert sol.retry_open_gaps(Rpc()) == _gap_stats(1, recovered=1)
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
            tx = _transaction()
            tx["slot"] = 11
            return tx

    assert sol.rehydrate_pending(
        HydrationRpc(), include_incomplete=True,
    ) == _hydration_stats(1, completed=1)
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
        lambda rpc, **kwargs: order.append("gap") or _gap_stats(0),
    )

    def rehydrate(rpc, *, limit, **kwargs):
        order.append(("hydrate", limit))
        return _hydration_stats(0)

    monkeypatch.setattr(sol, "rehydrate_pending", rehydrate)

    class StopAfterOneTick:
        def is_set(self):
            return False

        def wait(self, timeout):
            return True

    sol._rehydrate_loop(StopAfterOneTick(), object(), interval_seconds=1)
    assert order == ["gap", ("hydrate", 5)]


def test_gap_lane_deadline_reserves_hydration_and_marks_degraded(sol, monkeypatch):
    from src.pipeline import stream_health

    hydration_calls = []

    def retry(_rpc, **kwargs):
        assert kwargs["deadline"] == sol.GAP_WORK_BUDGET_SECONDS
        return _gap_stats(1, progressed=1, deadline_exhausted=True)

    def hydrate(_rpc, *, limit, **kwargs):
        hydration_calls.append((limit, kwargs["deadline"]))
        return _hydration_stats(1, completed=1)

    monkeypatch.setattr(sol, "retry_open_gaps", retry)
    monkeypatch.setattr(
        sol, "rehydrate_pending", hydrate,
    )

    class Clock:
        value = 0.0

        def __call__(self):
            return self.value

    clock = Clock()

    class StopAfterOneTick:
        def is_set(self):
            return False

        def wait(self, timeout):
            return True

    sol._rehydrate_loop(
        StopAfterOneTick(), object(), interval_seconds=1, monotonic=clock,
    )
    assert hydration_calls == [(5, sol.MAINTENANCE_WORK_BUDGET_SECONDS)]
    maintenance = next(
        item for item in stream_health.snapshot()
        if item["stream"] == sol.MAINTENANCE_STREAM
    )
    assert maintenance["status"] == "degraded"
    assert "gap lane work budget exhausted" in maintenance["last_error"]


def test_total_deadline_exhaustion_skips_hydration_and_marks_degraded(
        sol, monkeypatch):
    from src.pipeline import stream_health

    hydration_calls = []

    class Clock:
        value = 0.0

        def __call__(self):
            return self.value

    clock = Clock()

    def retry(_rpc, **kwargs):
        clock.value = sol.MAINTENANCE_WORK_BUDGET_SECONDS + 1
        return _gap_stats(1, progressed=1, deadline_exhausted=True)

    monkeypatch.setattr(sol, "retry_open_gaps", retry)
    monkeypatch.setattr(
        sol, "rehydrate_pending",
        lambda *args, **kwargs: hydration_calls.append(True),
    )

    class StopAfterOneTick:
        def is_set(self):
            return False

        def wait(self, timeout):
            return True

    sol._rehydrate_loop(
        StopAfterOneTick(), object(), interval_seconds=1, monotonic=clock,
    )
    assert hydration_calls == []
    maintenance = next(
        item for item in stream_health.snapshot()
        if item["stream"] == sol.MAINTENANCE_STREAM
    )
    assert maintenance["status"] == "degraded"
    assert maintenance["last_error"] == "maintenance work budget exhausted"


def test_maintenance_hydration_limit_ramps_to_backlog_capacity(sol, monkeypatch):
    limits = []
    monkeypatch.setattr(
        sol, "retry_open_gaps",
        lambda rpc, **kwargs: _gap_stats(0),
    )

    def hydrate(_rpc, *, limit, **kwargs):
        limits.append(limit)
        return _hydration_stats(limit, completed=limit)

    monkeypatch.setattr(sol, "rehydrate_pending", hydrate)

    class Clock:
        value = 0.0

        def __call__(self):
            return self.value

    clock = Clock()

    class StopAfterSevenTicks:
        waits = 0

        def is_set(self):
            return False

        def wait(self, timeout):
            clock.value += timeout
            self.waits += 1
            return self.waits >= 7

    sol._rehydrate_loop(
        StopAfterSevenTicks(), object(), interval_seconds=60, monotonic=clock,
    )
    assert limits == [5, 5, 10, 10, 15, 15, 20]


def test_gap_pressure_skips_hydration_in_same_maintenance_cycle(sol, monkeypatch):
    from src.pipeline import stream_health

    hydration_calls = []
    monkeypatch.setattr(
        sol, "retry_open_gaps", lambda rpc, **kwargs: _gap_stats(
            1, failed=1, pressure_kind="rate_limited", retry_after_seconds=120,
        ),
    )
    monkeypatch.setattr(
        sol, "rehydrate_pending",
        lambda *args, **kwargs: hydration_calls.append(True),
    )

    class StopAfterOneTick:
        def is_set(self):
            return False

        def wait(self, timeout):
            return True

    sol._rehydrate_loop(StopAfterOneTick(), object(), interval_seconds=1)
    assert hydration_calls == []
    maintenance = next(
        item for item in stream_health.snapshot()
        if item["stream"] == sol.MAINTENANCE_STREAM
    )
    assert maintenance["status"] == "degraded"
    assert "gap RPC pressure" in maintenance["last_error"]


def test_escaped_gap_pressure_still_skips_hydration_and_opens_circuit(
        sol, monkeypatch):
    from src.pipeline import stream_health

    hydration_calls = []

    def retry(_rpc, **kwargs):
        raise sol.RpcPressureError(
            "HTTP 429", kind="rate_limited", retry_after_seconds=120,
        )

    monkeypatch.setattr(sol, "retry_open_gaps", retry)
    monkeypatch.setattr(
        sol, "rehydrate_pending",
        lambda *args, **kwargs: hydration_calls.append(True),
    )

    class StopAfterOneTick:
        def is_set(self):
            return False

        def wait(self, timeout):
            return True

    sol._rehydrate_loop(StopAfterOneTick(), object(), interval_seconds=1)
    assert hydration_calls == []
    maintenance = next(
        item for item in stream_health.snapshot()
        if item["stream"] == sol.MAINTENANCE_STREAM
    )
    assert maintenance["status"] == "degraded"
    assert "gap RPC pressure: rate_limited" in maintenance["last_error"]


def test_provider_circuit_half_open_uses_one_logical_work_unit(sol, monkeypatch):
    gap_calls = 0
    hydration_limits = []

    def retry(_rpc, **kwargs):
        nonlocal gap_calls
        gap_calls += 1
        if gap_calls == 1:
            return _gap_stats(
                1, failed=1, pressure_kind="rate_limited",
                retry_after_seconds=120,
            )
        return _gap_stats(0)

    def hydrate(_rpc, *, limit, **kwargs):
        hydration_limits.append(limit)
        return _hydration_stats(1, completed=1)

    monkeypatch.setattr(sol, "retry_open_gaps", retry)
    monkeypatch.setattr(sol, "rehydrate_pending", hydrate)

    class Clock:
        value = 0.0

        def __call__(self):
            return self.value

    clock = Clock()

    class StopAfterThreeTicks:
        waits = 0

        def is_set(self):
            return False

        def wait(self, timeout):
            clock.value += timeout
            self.waits += 1
            return self.waits >= 3

    sol._rehydrate_loop(
        StopAfterThreeTicks(), object(), interval_seconds=60, monotonic=clock,
    )

    assert gap_calls == 2
    assert hydration_limits == [1]


@pytest.mark.parametrize("failed_lane", ["gap", "gap_deadline", "hydration"])
def test_half_open_requires_a_valid_probe_before_reporting_recovery(
        sol, monkeypatch, failed_lane):
    from src.pipeline import stream_health

    gap_calls = 0
    hydration_calls = 0

    def retry(_rpc, **kwargs):
        nonlocal gap_calls
        gap_calls += 1
        if gap_calls == 1:
            return _gap_stats(
                1, failed=1, pressure_kind="rate_limited",
                retry_after_seconds=120,
            )
        if failed_lane == "gap":
            return _gap_stats(1, failed=1)
        if failed_lane == "gap_deadline":
            return _gap_stats(
                1, deadline_stopped=1, deadline_exhausted=True,
            )
        return _gap_stats(0)

    def hydrate(_rpc, *, limit, **kwargs):
        nonlocal hydration_calls
        hydration_calls += 1
        assert limit == 1
        return _hydration_stats(1, rpc_failed=1)

    monkeypatch.setattr(sol, "retry_open_gaps", retry)
    monkeypatch.setattr(sol, "rehydrate_pending", hydrate)

    class Clock:
        value = 0.0

        def __call__(self):
            return self.value

    clock = Clock()

    class StopAfterThreeTicks:
        waits = 0

        def is_set(self):
            return False

        def wait(self, timeout):
            clock.value += timeout
            self.waits += 1
            return self.waits >= 3

    sol._rehydrate_loop(
        StopAfterThreeTicks(), object(), interval_seconds=60, monotonic=clock,
    )

    assert gap_calls == 2
    assert hydration_calls == (1 if failed_lane == "hydration" else 0)
    maintenance = next(
        item for item in stream_health.snapshot()
        if item["stream"] == sol.MAINTENANCE_STREAM
    )
    assert maintenance["status"] == "degraded"
    assert maintenance["last_error"] == "RPC circuit half-open; probe failed"


def test_persistent_gap_lane_exception_never_starves_hydration(sol, monkeypatch):
    from src.pipeline import stream_health

    gap_calls = 0
    hydration_calls = 0
    errors = []

    def retry(_rpc, **kwargs):
        nonlocal gap_calls
        gap_calls += 1
        raise RuntimeError("persistent gap sqlite lock")

    def hydrate(_rpc, *, limit, **kwargs):
        nonlocal hydration_calls
        hydration_calls += 1
        return _hydration_stats(0)

    monkeypatch.setattr(sol, "retry_open_gaps", retry)
    monkeypatch.setattr(sol, "rehydrate_pending", hydrate)
    monkeypatch.setattr(
        sol.logger, "exception",
        lambda event, **kwargs: errors.append((event, kwargs)),
    )

    class StopAfterTwoTicks:
        waits = 0

        def is_set(self):
            return False

        def wait(self, timeout):
            self.waits += 1
            return self.waits >= 2

    sol._rehydrate_loop(StopAfterTwoTicks(), object(), interval_seconds=1)
    assert gap_calls == 2 and hydration_calls == 2
    assert [event for event, _kwargs in errors] == [
        "solana_launch_gap_lane_failed", "solana_launch_gap_lane_failed",
    ]
    maintenance = next(
        item for item in stream_health.snapshot()
        if item["stream"] == sol.MAINTENANCE_STREAM
    )
    assert maintenance["status"] == "degraded"
    assert "persistent gap sqlite lock" in maintenance["last_error"]


def test_maintenance_worker_continues_after_unexpected_hydration_exception(
        sol, monkeypatch):
    from src.pipeline import stream_health

    hydration_calls = 0
    errors = []
    monkeypatch.setattr(
        sol, "retry_open_gaps", lambda _rpc, **kwargs: _gap_stats(0),
    )

    def hydrate(_rpc, *, limit, **kwargs):
        nonlocal hydration_calls
        hydration_calls += 1
        if hydration_calls == 1:
            raise RuntimeError("temporary launch DB lock")
        return _hydration_stats(0)

    monkeypatch.setattr(sol, "rehydrate_pending", hydrate)
    monkeypatch.setattr(
        sol.logger, "exception",
        lambda event, **kwargs: errors.append((event, kwargs)),
    )

    class StopAfterTwoTicks:
        waits = 0

        def is_set(self):
            return False

        def wait(self, timeout):
            self.waits += 1
            return self.waits >= 2

    sol._rehydrate_loop(StopAfterTwoTicks(), object(), interval_seconds=1)
    assert hydration_calls == 2
    assert errors[0][0] == "solana_launch_maintenance_failed"
    maintenance = next(
        item for item in stream_health.snapshot()
        if item["stream"] == sol.MAINTENANCE_STREAM
    )
    assert maintenance["status"] == "live" and maintenance["last_error"] is None


def test_health_reporting_failure_cannot_kill_maintenance_worker(sol, monkeypatch):
    events = []
    monkeypatch.setattr(
        sol, "retry_open_gaps", lambda rpc, **kwargs: _gap_stats(0),
    )
    monkeypatch.setattr(
        sol, "rehydrate_pending",
        lambda rpc, limit, **kwargs: _hydration_stats(0),
    )
    monkeypatch.setattr(
        sol.stream_health, "report_worker",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            RuntimeError("health DB locked")
        ),
    )
    monkeypatch.setattr(
        sol.logger, "exception",
        lambda event, **kwargs: events.append((event, kwargs)),
    )

    class StopAfterOneTick:
        def is_set(self):
            return False

        def wait(self, timeout):
            return True

    sol._rehydrate_loop(StopAfterOneTick(), object(), interval_seconds=1)
    assert events[0][0] == "solana_launch_maintenance_health_failed"


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


def test_stream_reuses_configured_provider_instead_of_public_rpc(sol, monkeypatch):
    monkeypatch.delenv("SOLANA_STREAM_RPC_URL", raising=False)
    monkeypatch.delenv("SOLANA_STREAM_WS_URL", raising=False)
    monkeypatch.setenv(
        "SOLANA_RPC_URL", "https://mainnet.helius-rpc.com/?api-key=redacted"
    )

    assert sol.configured_rpc_endpoint() == (
        "https://mainnet.helius-rpc.com/?api-key=redacted"
    )
    assert sol.configured_ws_endpoint() == (
        "wss://mainnet.helius-rpc.com/?api-key=redacted"
    )

    monkeypatch.setenv("SOLANA_STREAM_RPC_URL", "https://stream.example/rpc")
    monkeypatch.setenv("SOLANA_STREAM_WS_URL", "wss://stream.example/ws")
    assert sol.configured_rpc_endpoint() == "https://stream.example/rpc"
    assert sol.configured_ws_endpoint() == "wss://stream.example/ws"


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


def test_qualification_claim_reserves_capacity_for_virgin_and_retry_rows(sol):
    now = datetime(2026, 7, 20, 12, tzinfo=timezone.utc)
    c = sol._conn()
    try:
        for index in range(8):
            signature = f"virgin-{index}"
            c.execute(
                """INSERT INTO raw_launches(
                       signature,slot,program,event_type,creator,mint,detected_at,
                       raw_payload_hash,hydration_payload_hash,logs,evidence_state,
                       qualification_state)
                   VALUES (?,?,?,?,?,?,?,?,?,?,'complete','raw_unqualified')""",
                (signature, index, sol.PUMP_FUN_PROGRAM, "pump_fun_createv2",
                 "creator", f"mint-{signature}",
                 (now - timedelta(minutes=9, seconds=index)).isoformat(),
                 "a" * 64, "b" * 64, "[]"),
            )
        for index in range(8):
            signature = f"retry-{index}"
            c.execute(
                """INSERT INTO raw_launches(
                       signature,slot,program,event_type,creator,mint,detected_at,
                       raw_payload_hash,hydration_payload_hash,logs,evidence_state,
                       qualification_state,qualification_attempted_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,'complete','market_pending',?)""",
                (signature, index + 20, sol.PUMP_FUN_PROGRAM, "pump_fun_createv2",
                 "creator", f"mint-{signature}",
                 (now - timedelta(minutes=9, seconds=index)).isoformat(),
                 "c" * 64, "d" * 64, "[]",
                 (now - timedelta(minutes=6)).isoformat()),
            )
        c.commit()
    finally:
        c.close()

    rows = sol.claim_qualification_batch(
        now=now, limit=4, virgin_fraction=0.5,
        protocol_start_at=(now - timedelta(hours=1)).isoformat(),
        max_source_to_decision_seconds=600,
    )

    assert sum(row["signature"].startswith("virgin-") for row in rows) == 2
    assert sum(row["signature"].startswith("retry-") for row in rows) == 2
    assert len({row["qualification_lease_token"] for row in rows}) == 4


def test_claim_quarantines_preboundary_and_late_rows_before_limit(sol):
    now = datetime(2026, 7, 20, 12, tzinfo=timezone.utc)
    boundary = now - timedelta(minutes=20)
    c = sol._conn()
    try:
        for signature, detected in (
            ("historical", boundary - timedelta(seconds=1)),
            ("late", now - timedelta(minutes=11)),
            ("live", now - timedelta(minutes=2)),
        ):
            c.execute(
                """INSERT INTO raw_launches(
                       signature,slot,program,event_type,creator,mint,detected_at,
                       raw_payload_hash,hydration_payload_hash,logs,evidence_state,
                       qualification_state)
                   VALUES (?,1,?,?,?,?,?,?,?,'[]','complete','raw_unqualified')""",
                (signature, sol.PUMP_FUN_PROGRAM, "pump_fun_createv2", "creator",
                 f"mint-{signature}", detected.isoformat(), "a" * 64, "b" * 64),
            )
        c.commit()
    finally:
        c.close()

    rows = sol.claim_qualification_batch(
        now=now, limit=1, protocol_start_at=boundary.isoformat(),
        max_source_to_decision_seconds=600,
    )

    assert [row["signature"] for row in rows] == ["live"]
    c = sol._conn()
    try:
        states = dict(c.execute(
            "SELECT signature,qualification_state FROM raw_launches"
        ).fetchall())
    finally:
        c.close()
    assert states == {
        "historical": "historical_raw_only",
        "late": "qualification_expired",
        "live": "raw_unqualified",
    }


def test_mutable_reconciliation_flags_cannot_enter_forward_protocol(sol):
    from src.pipeline import solana_launch_reconcile as reconcile

    now = datetime(2026, 7, 20, 12, tzinfo=timezone.utc)
    c = sol._conn()
    try:
        reconcile._ensure_schema(c)
        for signature, mode, reconciliation in (
            ("verified", "live_ws", "verified_live"),
            ("unverified", "live_ws", "unverified"),
            ("backfill", "finalized_reconciliation", "reconciled_backfill"),
        ):
            c.execute(
                """INSERT INTO raw_launches(
                       signature,slot,program,event_type,creator,mint,detected_at,
                       raw_payload_hash,hydration_payload_hash,logs,evidence_state,
                       qualification_state,capture_mode,reconciliation_state)
                   VALUES (?,1,?,?,?,?,?,?,?,'[]','complete','raw_unqualified',?,?)""",
                (signature, sol.PUMP_FUN_PROGRAM, "pump_fun_createv2", "creator",
                 f"mint-{signature}", (now - timedelta(minutes=1)).isoformat(),
                 "a" * 64, "b" * 64, mode, reconciliation),
            )
        c.commit()
    finally:
        c.close()

    claimed = sol.claim_qualification_batch(
        now=now, limit=3, protocol_start_at=(now - timedelta(hours=1)).isoformat(),
        max_source_to_decision_seconds=600, require_reconciled_live=True,
    )

    assert claimed == []


def test_qualification_lease_is_exclusive_and_crash_retries_after_cooldown(sol):
    now = datetime(2026, 7, 20, 12, tzinfo=timezone.utc)
    event = sol.parse_message(_notification())
    sol.persist(event.payload, transaction=_transaction())
    c = sol._conn()
    try:
        c.execute(
            "UPDATE raw_launches SET detected_at=?",
            ((now - timedelta(minutes=1)).isoformat(),),
        )
        c.commit()
    finally:
        c.close()

    def claim():
        return sol.claim_qualification_batch(
            now=now, limit=1, protocol_start_at=(now - timedelta(hours=1)).isoformat(),
            max_source_to_decision_seconds=600,
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        claims = list(pool.map(lambda _index: claim(), range(2)))
    winners = [rows[0] for rows in claims if rows]
    assert len(winners) == 1
    first = winners[0]
    assert sol.set_qualification(
        "sig-1", "market_pending", error="late result",
        lease_token=first["qualification_lease_token"],
        at=now + timedelta(seconds=121),
    ) is False
    retried = sol.claim_qualification_batch(
        now=now + timedelta(seconds=121), limit=1,
        protocol_start_at=(now - timedelta(hours=1)).isoformat(),
        max_source_to_decision_seconds=600,
    )
    assert len(retried) == 1
    assert retried[0]["qualification_lease_token"] != first["qualification_lease_token"]


def test_terminal_qualification_cannot_be_overwritten_by_stale_worker(sol):
    now = datetime(2026, 7, 20, 12, tzinfo=timezone.utc)
    event = sol.parse_message(_notification())
    sol.persist(event.payload, transaction=_transaction())
    c = sol._conn()
    try:
        c.execute(
            "UPDATE raw_launches SET detected_at=?",
            ((now - timedelta(minutes=1)).isoformat(),),
        )
        c.commit()
    finally:
        c.close()
    row = sol.claim_qualification_batch(
        now=now, limit=1, protocol_start_at=(now - timedelta(hours=1)).isoformat(),
        max_source_to_decision_seconds=600,
    )[0]
    token = row["qualification_lease_token"]

    assert sol.set_qualification(
        "sig-1", "qualified_recorded", ledger_event_id="ledger-1",
        lease_token=token, at=now,
    ) is True
    assert sol.set_qualification(
        "sig-1", "market_error", error="slow worker failed",
        lease_token=token, at=now + timedelta(seconds=1),
    ) is False
    assert sol.set_qualification(
        "sig-1", "qualified_recorded", ledger_event_id="ledger-1",
        at=now + timedelta(seconds=2),
    ) is True
    assert sol.set_qualification(
        "sig-1", "qualified_recorded", ledger_event_id="different",
        at=now + timedelta(seconds=2),
    ) is False
    c = sol._conn()
    try:
        with pytest.raises(sqlite3.IntegrityError, match="terminal qualification"):
            c.execute(
                "UPDATE raw_launches SET qualification_state='market_error' "
                "WHERE signature='sig-1'"
            )
    finally:
        c.close()


def test_claim_is_not_a_market_attempt_and_valid_empty_is_append_only(sol):
    now = datetime(2026, 7, 20, 12, tzinfo=timezone.utc)
    event = sol.parse_message(_notification())
    sol.persist(event.payload, transaction=_transaction())
    c = sol._conn()
    try:
        c.execute(
            "UPDATE raw_launches SET detected_at=?",
            ((now - timedelta(minutes=1)).isoformat(),),
        )
        c.commit()
    finally:
        c.close()
    row = sol.claim_qualification_batch(
        now=now, limit=1, protocol_start_at=(now - timedelta(hours=1)).isoformat(),
        max_source_to_decision_seconds=600,
    )[0]
    c = sol._conn()
    try:
        assert c.execute(
            "SELECT qualification_attempt_count,qualification_attempted_at "
            "FROM raw_launches"
        ).fetchone() == (0, None)
    finally:
        c.close()

    empty_hash = sol.hashlib.sha256(b"[]").hexdigest()
    assert sol.set_qualification(
        "sig-1", "market_pending", error="DEX pool not indexed yet",
        lease_token=row["qualification_lease_token"], outcome_kind="valid_empty",
        response_hash=empty_hash, at=now,
    ) is True
    c = sol._conn()
    try:
        state = c.execute(
            """SELECT qualification_attempt_count,qualification_attempted_at,
                      qualification_next_retry_at,qualification_last_outcome_kind
               FROM raw_launches"""
        ).fetchone()
        observation = c.execute(
            """SELECT attempt_id,outcome_kind,response_hash
               FROM qualification_observations"""
        ).fetchone()
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            c.execute("UPDATE qualification_observations SET outcome_kind='qualified'")
    finally:
        c.close()
    assert state == (
        1, now.isoformat(), (now + timedelta(seconds=300)).isoformat(), "valid_empty",
    )
    assert observation == (
        row["qualification_lease_token"], "valid_empty", empty_hash,
    )


def test_provider_circuit_is_persistent_and_half_opens(sol):
    now = datetime(2026, 7, 20, 12, tzinfo=timezone.utc)
    failed = sol.report_qualification_provider("error", error="bad schema", at=now)
    assert failed["ready"] is False and failed["circuit_state"] == "open"
    assert sol.qualification_provider_health(
        now=now + timedelta(seconds=59)
    )["ready"] is False
    half_open = sol.qualification_provider_health(now=now + timedelta(seconds=60))
    assert half_open["ready"] is True and half_open["circuit_state"] == "half_open"
    recovered = sol.report_qualification_provider(
        "ok", response_hash="a" * 64, at=now + timedelta(seconds=60),
    )
    assert recovered["circuit_state"] == "closed"
    assert recovered["failure_count"] == 0


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
