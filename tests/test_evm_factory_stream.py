"""EVM factory events are immutable raw evidence, not trade recommendations."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest


@pytest.fixture
def evm(tmp_path, monkeypatch):
    from src.pipeline import evm_factory_stream, stream_health

    monkeypatch.setattr(evm_factory_stream, "DB", tmp_path / "pools.db")
    monkeypatch.setattr(stream_health, "DB", tmp_path / "health.db")
    return evm_factory_stream


def _word(address: str) -> str:
    return "0" * 24 + address.lower().removeprefix("0x")


def _log(evm, *, removed=False, block=100):
    return {
        "address": evm.PANCAKE_V2_FACTORY, "topics": [
            evm.PAIR_CREATED_TOPIC,
            "0x" + _word("0x1111111111111111111111111111111111111111"),
            "0x" + _word("0x2222222222222222222222222222222222222222"),
        ],
        "data": "0x" + _word("0x3333333333333333333333333333333333333333")
        + f"{42:064x}",
        "blockNumber": hex(block), "blockHash": "0x" + "ab" * 32,
        "transactionHash": "0x" + "cd" * 32, "transactionIndex": "0x2",
        "logIndex": "0x3", "removed": removed,
    }


def _v3_log(evm, *, removed=False, block=100):
    item = _log(evm, removed=removed, block=block)
    item["address"] = evm.PANCAKE_V3_FACTORY
    item["topics"][0] = evm.POOL_CREATED_TOPIC
    item["topics"].append("0x" + f"{500:064x}")
    item["data"] = "0x" + f"{10:064x}" + _word(
        "0x4444444444444444444444444444444444444444")
    return item


def _aerodrome_log(evm, *, stable=False, block=100):
    item = _log(evm, block=block)
    item["address"] = evm.AERODROME_FACTORY
    item["topics"][0] = evm.AERODROME_POOL_TOPIC
    item["topics"].append("0x" + f"{int(stable):064x}")
    item["data"] = "0x" + _word(
        "0x5555555555555555555555555555555555555555") + f"{99:064x}"
    return item


def _notification(item):
    return {"jsonrpc": "2.0", "method": "eth_subscription",
            "params": {"subscription": "0xsub", "result": item}}


def test_subscribes_to_official_factory_event_and_heads(evm):
    spec = evm.bsc_pancake_v2_spec()
    requests = evm.subscribe_requests(spec)
    assert requests[0]["params"] == ["logs", {
        "address": evm.PANCAKE_V2_FACTORY, "topics": [evm.PAIR_CREATED_TOPIC]}]
    assert requests[1]["params"] == ["newHeads"]


def test_configured_specs_cover_official_bsc_base_and_ethereum_factories(evm):
    specs = {(spec.chain, spec.venue): spec for spec in evm.configured_specs()}
    assert set(specs) == {("bsc", "pancakeswap_v2"),
                          ("bsc", "pancakeswap_v3"),
                          ("base", "pancakeswap_v2"),
                          ("base", "aerodrome"),
                          ("ethereum", "pancakeswap_v2")}
    assert specs[("bsc", "pancakeswap_v2")].address == evm.PANCAKE_V2_FACTORY
    assert specs[("bsc", "pancakeswap_v3")].address == evm.PANCAKE_V3_FACTORY
    assert specs[("base", "pancakeswap_v2")].address == evm.PANCAKE_V2_BASE_FACTORY
    assert specs[("base", "aerodrome")].address == evm.AERODROME_FACTORY
    assert specs[("ethereum", "pancakeswap_v2")].address == evm.PANCAKE_V2_ETH_FACTORY
    assert all(spec.ws_urls and spec.rpc_urls for spec in specs.values())


def test_factory_specs_reject_malformed_chain_constants(evm):
    with pytest.raises(ValueError, match="address"):
        evm.FactorySpec("bsc", "bad", "0x1234", "pair_v2",
                        evm.PAIR_CREATED_TOPIC, ("wss://test",), ("https://test",))


def test_pair_created_decodes_raw_pool_evidence(evm):
    event = evm.parse_message(json.dumps(_notification(_log(evm))))
    assert event.cursor is None
    assert event.payload["token0"] == "0x1111111111111111111111111111111111111111"
    assert event.payload["token1"] == "0x2222222222222222222222222222222222222222"
    assert event.payload["pool"] == "0x3333333333333333333333333333333333333333"
    assert event.payload["pair_index"] == 42 and event.payload["block_number"] == 100


def test_v3_pool_created_decodes_fee_tick_and_pool(evm):
    spec = evm.bsc_pancake_v3_spec()
    event = evm.parse_message(_notification(_v3_log(evm)), spec=spec)
    assert event.payload["kind"] == "pool_v3"
    assert event.payload["fee"] == 500 and event.payload["tick_spacing"] == 10
    assert event.payload["pool"] == "0x4444444444444444444444444444444444444444"
    assert evm.subscribe_requests(spec)[0]["params"][1]["topics"] == [
        evm.POOL_CREATED_TOPIC]


def test_aerodrome_pool_decodes_stable_flag_pool_and_index(evm):
    spec = evm.base_aerodrome_spec()
    event = evm.parse_message(_notification(_aerodrome_log(evm, stable=True)), spec=spec)
    assert event.payload["kind"] == "aerodrome_pool"
    assert event.payload["stable"] is True and event.payload["pair_index"] == 99
    assert event.payload["pool"] == "0x5555555555555555555555555555555555555555"
    assert evm.subscribe_requests(spec)[0]["params"][1]["topics"] == [
        evm.AERODROME_POOL_TOPIC]


def test_new_head_supplies_contiguous_block_cursor_and_time(evm):
    event = evm.parse_message(_notification({"number": "0x64", "timestamp": "0x5"}))
    assert event.cursor == 100 and event.event_at.isoformat() == "1970-01-01T00:00:05+00:00"


def test_complete_pool_stays_raw_unqualified(evm):
    class Rpc:
        def call(self, method, params):
            assert method == "eth_getBlockByNumber" and params[0] == "0x64"
            return {"timestamp": "0x5"}

    event = evm.parse_message(_notification(_log(evm)))
    evm.persist(event.payload, rpc=Rpc())
    c = evm._conn()
    try:
        row = c.execute("SELECT token0,token1,pool,pair_index,block_at,removed,"
                        "evidence_state,qualification_state,length(raw_payload_hash) "
                        "FROM raw_pools").fetchone()
        assert row == ("0x1111111111111111111111111111111111111111",
                       "0x2222222222222222222222222222222222222222",
                       "0x3333333333333333333333333333333333333333", 42,
                       "1970-01-01T00:00:05+00:00", 0, "complete",
                       "raw_unqualified", 64)
    finally:
        c.close()


def test_complete_v3_pool_uses_separate_raw_schema(evm):
    class Rpc:
        def call(self, method, params):
            return {"number": "0x64", "timestamp": "0x5"}

    spec = evm.bsc_pancake_v3_spec()
    event = evm.parse_message(_notification(_v3_log(evm)), spec=spec)
    evm.persist(event.payload, rpc=Rpc())
    c = evm._conn()
    try:
        row = c.execute("SELECT venue,token0,token1,pool,fee,tick_spacing,"
                        "evidence_state,qualification_state FROM raw_v3_pools").fetchone()
        assert row == ("pancakeswap_v3",
                       "0x1111111111111111111111111111111111111111",
                       "0x2222222222222222222222222222222222222222",
                       "0x4444444444444444444444444444444444444444",
                       500, 10, "complete", "raw_unqualified")
        assert c.execute("SELECT COUNT(*) FROM raw_pools").fetchone()[0] == 0
    finally:
        c.close()


def test_aerodrome_pool_persists_stable_curve_without_qualification(evm):
    class Rpc:
        def call(self, method, params):
            return {"number": "0x64", "timestamp": "0x5"}

    spec = evm.base_aerodrome_spec()
    event = evm.parse_message(_notification(_aerodrome_log(evm, stable=True)), spec=spec)
    evm.persist(event.payload, rpc=Rpc())
    c = evm._conn()
    try:
        row = c.execute("SELECT venue,pool,pair_index,stable,evidence_state,"
                        "qualification_state FROM raw_pools").fetchone()
        assert row == ("aerodrome", "0x5555555555555555555555555555555555555555",
                       99, 1, "complete", "raw_unqualified")
    finally:
        c.close()


def test_removed_log_is_retained_as_reorg_evidence(evm):
    class Rpc:
        def call(self, method, params):
            return {"timestamp": "0x5"}

    original = evm.parse_message(_notification(_log(evm)))
    removed = evm.parse_message(_notification(_log(evm, removed=True)))
    evm.persist(original.payload, rpc=Rpc())
    evm.persist(removed.payload, rpc=Rpc())
    c = evm._conn()
    try:
        assert c.execute("SELECT removed,evidence_state,block_at,qualification_state "
                         "FROM raw_pools").fetchone() \
            == (1, "removed_reorg", "1970-01-01T00:00:05+00:00", "reorg_removed")
    finally:
        c.close()


def test_removed_log_arriving_first_is_never_qualification_candidate(evm):
    removed = evm.parse_message(_notification(_log(evm, removed=True)))
    evm.persist(removed.payload)
    c = evm._conn()
    try:
        assert c.execute("SELECT evidence_state,qualification_state FROM raw_pools").fetchone() \
            == ("removed_reorg", "reorg_removed")
    finally:
        c.close()


def test_reorg_invalidates_already_linked_ledger_event(evm, tmp_path, monkeypatch):
    from src.pipeline import opportunity_ledger as ledger

    monkeypatch.setattr(ledger, "DB", tmp_path / "ledger.db")
    original = evm.parse_message(_notification(_log(evm)))
    evm.persist(original.payload)
    ident, _ = ledger.record({"lane": "launch", "chain": "bsc", "token": "token",
                              "symbol": "T", "state": "live", "decision": "WATCH",
                              "entry_price": 1.0})
    c = evm._conn()
    try:
        c.execute("UPDATE raw_pools SET ledger_event_id=?,"
                  "qualification_state='qualified_recorded'", (ident,))
        c.commit()
    finally:
        c.close()

    removed = evm.parse_message(_notification(_log(evm, removed=True)))
    evm.persist(removed.payload)
    assert ledger.active("launch") == []
    assert ledger.outcome_rows()[0]["state"] == "reorg_removed"


def test_short_missing_block_range_is_backfilled_and_gap_resolved(evm):
    from src.pipeline import stream_health

    spec = evm.bsc_pancake_v2_spec()
    stream_health.observe("bsc", spec.stream, cursor=99, expect_contiguous=True)
    stream_health.observe("bsc", spec.stream, cursor=101, expect_contiguous=True)

    class Rpc:
        def call(self, method, params):
            if method == "eth_getLogs":
                assert params[0]["fromBlock"] == "0x64"
                return [_log(evm)]
            if method == "eth_getBlockByNumber":
                return {"timestamp": "0x5"}
            raise AssertionError(method)

    assert evm.retry_open_gaps(spec, Rpc()) == {
        "attempted": 1, "recovered": 1, "failed": 0}
    assert stream_health.open_gaps("bsc", spec.stream) == []
    c = evm._conn()
    try:
        assert c.execute("SELECT COUNT(*) FROM raw_pools").fetchone()[0] == 1
    finally:
        c.close()


def test_malformed_pair_data_fails_visible(evm):
    item = _log(evm)
    item["data"] = "0x1234"
    with pytest.raises(ValueError, match="two ABI words"):
        evm.parse_message(_notification(item))


def test_unsorted_tokens_and_non_boolean_reorg_flag_fail_visible(evm):
    item = _log(evm)
    item["topics"][1], item["topics"][2] = item["topics"][2], item["topics"][1]
    with pytest.raises(ValueError, match="sorted"):
        evm.parse_message(_notification(item))
    item = _log(evm)
    item["removed"] = "false"
    with pytest.raises(ValueError, match="boolean"):
        evm.parse_message(_notification(item))


def test_backfill_limit_counts_both_endpoints(evm):
    class Rpc:
        def call(self, method, params):
            raise AssertionError("oversized ranges must not call RPC")

    spec = evm.bsc_pancake_v2_spec()
    assert evm.backfill_blocks(1, evm.MAX_BACKFILL_BLOCKS + 1,
                               spec=spec, rpc=Rpc()) is False


def _persist_complete(evm):
    class Rpc:
        def call(self, method, params):
            return {"number": "0x64", "timestamp": "0x5"}

    event = evm.parse_message(_notification(_log(evm)))
    evm.persist(event.payload, rpc=Rpc())


def test_bridge_boundary_quarantines_historical_inventory(evm):
    _persist_complete(evm)
    now = datetime.now(timezone.utc) + timedelta(seconds=1)
    started = evm.ensure_bridge_started_at(at=now)
    assert started == now.isoformat()
    assert evm.qualification_batch(now=now) == []
    assert evm.qualification_summary()["qualification"] == {"historical_raw_only": 1}


def test_forward_factory_row_has_retryable_qualification_clock(evm):
    now = datetime.now(timezone.utc)
    evm.ensure_bridge_started_at(at=now - timedelta(minutes=1))
    _persist_complete(evm)
    c = evm._conn()
    try:
        c.execute("UPDATE raw_pools SET block_at=?,detected_at=?",
                  ((now - timedelta(seconds=10)).isoformat(),
                   (now - timedelta(seconds=9)).isoformat()))
        c.commit()
    finally:
        c.close()
    row = evm.qualification_batch(now=now, limit=1)[0]
    assert row["table_kind"] == "v2" and row["pool"].endswith("3333")
    assert evm.set_qualification(row, "market_pending", reason="not indexed",
                                 retry_after_seconds=180, at=now)
    assert evm.qualification_batch(now=now + timedelta(seconds=179)) == []
    retry = evm.qualification_batch(now=now + timedelta(seconds=181))[0]
    assert retry["qualification_attempts"] == 1


def test_ledger_orphan_is_terminal_and_retains_cross_database_trace(evm):
    now = datetime.now(timezone.utc)
    evm.ensure_bridge_started_at(at=now - timedelta(minutes=1))
    _persist_complete(evm)
    c = evm._conn()
    try:
        c.execute("UPDATE raw_pools SET block_at=?,detected_at=?",
                  ((now - timedelta(seconds=10)).isoformat(),
                   (now - timedelta(seconds=9)).isoformat()))
        c.commit()
    finally:
        c.close()
    row = evm.qualification_batch(now=now, limit=1)[0]
    target = "0x1111111111111111111111111111111111111111"
    assert evm.set_qualification(
        row, "ledger_orphan", reason="opportunity ledger ID failed exact read-back",
        target_token=target, ledger_event_id="ledger-mismatch", at=now,
    )

    assert evm.qualification_batch(now=now + timedelta(hours=1)) == []
    c = evm._conn()
    try:
        stored = c.execute(
            "SELECT qualification_state,qualification_reason,target_token,"
            "ledger_event_id,qualified_at FROM raw_pools"
        ).fetchone()
    finally:
        c.close()
    assert stored == (
        "ledger_orphan", "opportunity ledger ID failed exact read-back",
        target, "ledger-mismatch", None,
    )


def test_qualification_summary_revalidates_exact_unique_ledger_links(evm):
    token_a = "0x1111111111111111111111111111111111111111"
    token_b = "0x2222222222222222222222222222222222222222"
    token_c = "0x3333333333333333333333333333333333333333"
    token_d = "0x4444444444444444444444444444444444444444"
    now = datetime.now(timezone.utc).isoformat()
    c = evm._conn()
    try:
        def insert_v2(index, state, ledger_id, target):
            c.execute("""INSERT INTO raw_pools(
                chain,venue,factory,transaction_hash,log_index,block_number,
                token0,token1,pool,pair_index,detected_at,updated_at,
                raw_payload_hash,evidence_state,qualification_state,
                ledger_event_id,target_token
            ) VALUES ('bsc','pancakeswap_v2',?,?,?,?,?,?,?,?,?,?,?,
                      'complete',?,?,?)""",
                      (evm.PANCAKE_V2_FACTORY, "0x" + f"{index:064x}", index,
                       100 + index, token_a, token_b,
                       "0x" + f"{1000 + index:040x}", index, now, now,
                       f"{index:064x}", state, ledger_id, target))

        def insert_v3(index, state, ledger_id, target):
            c.execute("""INSERT INTO raw_v3_pools(
                chain,venue,factory,transaction_hash,log_index,block_number,
                token0,token1,pool,fee,tick_spacing,detected_at,updated_at,
                raw_payload_hash,evidence_state,qualification_state,
                ledger_event_id,target_token
            ) VALUES ('bsc','pancakeswap_v3',?,?,?,?,?,?,?,?,?,?,?, ?,
                      'complete',?,?,?)""",
                      (evm.PANCAKE_V3_FACTORY, "0x" + f"{index:064x}", index,
                       200 + index, token_a, token_b,
                       "0x" + f"{2000 + index:040x}", 500, 10, now, now,
                       f"{index + 10:064x}", state, ledger_id, target))

        insert_v2(1, "qualified_recorded", "ledger-good", token_a)
        insert_v2(2, "qualified_recorded", "ledger-good", token_a)
        insert_v3(3, "qualified_recorded", "ledger-bad", token_b)
        insert_v2(4, "qualified_recorded", None, token_c)
        insert_v3(5, "ledger_orphan", "ledger-terminal", token_d)
        c.commit()
    finally:
        c.close()

    calls = []

    def readback(ident, chain, token):
        calls.append((ident, chain, token))
        return ident == "ledger-good" and chain == "bsc" and token == token_a

    summary = evm.qualification_summary(ledger_readback=readback)
    assert summary["raw_total"] == 5
    assert summary["raw_qualification_states"] == {
        "ledger_orphan": 1, "qualified_recorded": 4}
    assert summary["qualification"] == {
        "ledger_orphan": 3, "qualified_recorded": 1}
    assert calls == [
        ("ledger-good", "bsc", token_a),
        ("ledger-good", "bsc", token_a),
        ("ledger-bad", "bsc", token_b),
    ]
    assert summary["traceability"] == {
        "state": "partial",
        "raw_marked_recorded_rows": 4,
        "traceable_rows": 2,
        "traceable_unique_ledger_events": 1,
        "orphan_rows": 3,
        "orphan_unique_ledger_ids": 2,
        "missing_ledger_id_rows": 1,
        "missing_identity_rows": 0,
        "quarantined_state_rows": 1,
        "readback_unavailable_rows": 0,
        "readback_error_rows": 0,
    }

    unavailable = evm.qualification_summary()
    assert unavailable["qualification"] == {
        "ledger_orphan": 2,
        "qualified_recorded": 0,
        "ledger_readback_unavailable": 3,
    }
    assert unavailable["traceability"]["state"] == "unavailable"
    assert unavailable["traceability"]["readback_unavailable_rows"] == 3


def test_factory_qualification_migrates_both_raw_tables(evm):
    c = evm._conn()
    try:
        for table in ("raw_pools", "raw_v3_pools"):
            columns = {row[1] for row in c.execute(f"PRAGMA table_info({table})")}
            assert {"qualification_attempted_at", "qualification_retry_at",
                    "qualification_reason", "qualification_attempts", "qualified_at",
                    "ledger_event_id", "target_token"} <= columns
    finally:
        c.close()


def test_unknown_factory_qualification_state_fails_closed(evm):
    with pytest.raises(ValueError, match="unknown qualification state"):
        evm.set_qualification({"table_kind": "v2"}, "magic_profit")
