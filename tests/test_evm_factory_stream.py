"""EVM factory events are immutable raw evidence, not trade recommendations."""
from __future__ import annotations

import json

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


def _notification(item):
    return {"jsonrpc": "2.0", "method": "eth_subscription",
            "params": {"subscription": "0xsub", "result": item}}


def test_subscribes_to_official_factory_event_and_heads(evm):
    spec = evm.bsc_pancake_v2_spec()
    requests = evm.subscribe_requests(spec)
    assert requests[0]["params"] == ["logs", {
        "address": evm.PANCAKE_V2_FACTORY, "topics": [evm.PAIR_CREATED_TOPIC]}]
    assert requests[1]["params"] == ["newHeads"]


def test_pair_created_decodes_raw_pool_evidence(evm):
    event = evm.parse_message(json.dumps(_notification(_log(evm))))
    assert event.cursor is None
    assert event.payload["token0"] == "0x1111111111111111111111111111111111111111"
    assert event.payload["token1"] == "0x2222222222222222222222222222222222222222"
    assert event.payload["pool"] == "0x3333333333333333333333333333333333333333"
    assert event.payload["pair_index"] == 42 and event.payload["block_number"] == 100


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
        assert c.execute("SELECT removed,evidence_state,block_at FROM raw_pools").fetchone() \
            == (1, "removed_reorg", "1970-01-01T00:00:05+00:00")
    finally:
        c.close()


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
