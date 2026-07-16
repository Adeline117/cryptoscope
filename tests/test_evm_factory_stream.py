"""EVM factory events are immutable raw evidence, not trade recommendations."""
from __future__ import annotations

import json
import hashlib
import io
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import pytest


def _pid(host: str) -> str:
    return "provider:" + hashlib.sha256(host.lower().encode()).hexdigest()


@pytest.fixture
def evm(tmp_path, monkeypatch):
    from src.ops.stream_disk_guard import DiskStateGuard
    from src.pipeline import evm_factory_stream, stream_health

    monkeypatch.setattr(evm_factory_stream, "DB", tmp_path / "pools.db")
    monkeypatch.setattr(stream_health, "DB", tmp_path / "health.db")
    evm_factory_stream._ACTIVE_WS_PROVIDERS.clear()
    monkeypatch.setattr(
        evm_factory_stream.stream_disk_guard, "GUARD",
        DiskStateGuard(probe=lambda: {"state": "ok"}),
    )
    return evm_factory_stream


class _CoverageRpc:
    def __init__(self, *, head: int, logs=None, provider="audit.example",
                 chain_id=56, failure: Exception | None = None,
                 logs_failure: Exception | None = None,
                 block_failure: Exception | None = None,
                 head_at: datetime | None = None,
                 head_hash: str = "0x" + "ab" * 32,
                 block_hashes: dict[int, str] | None = None):
        self.head = head
        self.logs = list(logs or [])
        self.provider = (
            provider if str(provider).startswith("provider:") else _pid(str(provider))
        )
        self.chain_id = chain_id
        self.failure = failure
        self.logs_failure = logs_failure
        self.block_failure = block_failure
        self.head_at = head_at or datetime.now(timezone.utc)
        self.head_hash = head_hash
        self.block_hashes = dict(block_hashes or {})
        self.calls = []

    def call_with_provider(self, method, params, *, exclude_provider_ids=frozenset()):
        self.calls.append(("select", method, params, frozenset(exclude_provider_ids)))
        if self.provider in exclude_provider_ids:
            from src.pipeline.evm_factory_stream import ProviderIndependenceError
            raise ProviderIndependenceError("same provider")
        if self.failure:
            raise self.failure
        assert method == "eth_getBlockByNumber" and params == ["finalized", False]
        return {
            "number": hex(self.head), "hash": self.head_hash,
            "timestamp": hex(int(self.head_at.timestamp())),
        }, self.provider

    def call_from_provider(self, identity, method, params):
        self.calls.append((identity, method, params))
        assert identity == self.provider
        if self.failure:
            raise self.failure
        if method == "eth_chainId":
            return hex(self.chain_id)
        if method == "eth_getLogs":
            if self.logs_failure:
                raise self.logs_failure
            return self.logs
        if method == "eth_getBlockByNumber":
            if self.block_failure:
                raise self.block_failure
            block = int(params[0], 16)
            return {
                "number": hex(block), "timestamp": hex(block),
                "hash": self.block_hashes.get(block, self.head_hash),
            }
        raise AssertionError(method)


class _CanonicalBlockRpc:
    def __init__(self, *, block_hash="0x" + "ab" * 32, timestamp=5):
        self.block_hash = block_hash
        self.timestamp = timestamp

    def call(self, method, params):
        assert method == "eth_getBlockByNumber"
        return {"number": params[0], "timestamp": hex(self.timestamp),
                "hash": self.block_hash}


def _install_legacy_coverage_schema(evm, *, with_epoch=False,
                                    with_verified_state=False):
    # Seed all unrelated current tables first so the concurrency test isolates the
    # coverage migration rather than older raw-table ALTER migrations.
    evm._conn().close()
    spec = evm.bsc_pancake_v2_spec()
    c = sqlite3.connect(str(evm.DB))
    try:
        c.execute("DROP INDEX IF EXISTS idx_coverage_epochs_spec")
        c.execute("DROP TABLE coverage_epochs")
        c.execute("""CREATE TABLE coverage_epochs(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chain TEXT NOT NULL,venue TEXT NOT NULL,factory TEXT NOT NULL,
            topic TEXT NOT NULL,from_block INTEGER NOT NULL,to_block INTEGER NOT NULL,
            checked_at TEXT NOT NULL,ws_provider_id TEXT NOT NULL,
            http_provider_id TEXT NOT NULL,provider_independent INTEGER NOT NULL,
            log_count INTEGER NOT NULL,evidence_digest TEXT NOT NULL,status TEXT NOT NULL,
            UNIQUE(chain,venue,factory,topic,from_block,to_block,http_provider_id))""")
        if with_epoch:
            c.execute("""INSERT INTO coverage_epochs(
                chain,venue,factory,topic,from_block,to_block,checked_at,
                ws_provider_id,http_provider_id,provider_independent,
                log_count,evidence_digest,status
            ) VALUES (?,?,?,?,90,100,?,'ws.example','audit.example',1,1,?,'sealed')""",
                      (*evm._coverage_key(spec), datetime.now(timezone.utc).isoformat(),
                       "a" * 64))
        if with_verified_state:
            c.execute("DROP TABLE coverage_state")
            c.execute("""CREATE TABLE coverage_state(
                chain TEXT NOT NULL,venue TEXT NOT NULL,factory TEXT NOT NULL,
                topic TEXT NOT NULL,coverage_started_block INTEGER,
                verified_through_block INTEGER,safe_head_block INTEGER,
                verified_at TEXT,ws_provider_id TEXT,http_provider_id TEXT,
                provider_independent INTEGER NOT NULL DEFAULT 0,status TEXT NOT NULL,
                consecutive_failures INTEGER NOT NULL DEFAULT 0,next_retry_at TEXT,
                last_error_kind TEXT,updated_at TEXT NOT NULL,
                PRIMARY KEY(chain,venue,factory,topic))""")
            now = datetime.now(timezone.utc).isoformat()
            c.execute("""INSERT INTO coverage_state(
                chain,venue,factory,topic,coverage_started_block,
                verified_through_block,safe_head_block,verified_at,
                ws_provider_id,http_provider_id,provider_independent,status,
                consecutive_failures,next_retry_at,last_error_kind,updated_at
            ) VALUES (?,?,?,?,90,100,100,?,'ws.example','audit.example',1,
                      'verified',0,NULL,NULL,?)""",
                      (*evm._coverage_key(spec), now, now))
        c.execute(
            "DELETE FROM bridge_meta WHERE key='provider_fingerprint_migration_v1'"
        )
        c.commit()
    finally:
        c.close()


def test_empty_legacy_coverage_schema_rebuilds_atomically(evm):
    _install_legacy_coverage_schema(evm)
    evm._conn().close()
    c = sqlite3.connect(str(evm.DB))
    try:
        columns = {row[1] for row in c.execute("PRAGMA table_info(coverage_epochs)")}
        legacy = c.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name LIKE 'coverage_epochs_legacy_v%'"
        ).fetchall()
    finally:
        c.close()
    assert evm._COVERAGE_EPOCH_COLUMNS.issubset(columns)
    assert legacy == []


def test_nonempty_legacy_epochs_are_quarantined_and_verified_state_revoked(evm):
    _install_legacy_coverage_schema(
        evm, with_epoch=True, with_verified_state=True,
    )
    evm._conn().close()
    c = sqlite3.connect(str(evm.DB))
    try:
        legacy_name = c.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name LIKE 'coverage_epochs_legacy_v%'"
        ).fetchone()[0]
        legacy_count = c.execute(f"SELECT COUNT(*) FROM {legacy_name}").fetchone()[0]
        current_count = c.execute("SELECT COUNT(*) FROM coverage_epochs").fetchone()[0]
        state = c.execute(
            "SELECT coverage_started_block,verified_through_block,status,"
            "provider_independent,last_error_kind FROM coverage_state"
        ).fetchone()
    finally:
        c.close()
    assert legacy_count == 1 and current_count == 0
    assert state == (90, 89, "blocked", 0, "provider_identity_reaudit_required")


def test_legacy_coverage_migration_is_safe_under_concurrent_startup(evm):
    _install_legacy_coverage_schema(
        evm, with_epoch=True, with_verified_state=True,
    )
    start = threading.Barrier(8)

    def open_and_close(_index):
        start.wait(timeout=5)
        evm._conn().close()

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(open_and_close, range(8)))
    c = sqlite3.connect(str(evm.DB))
    try:
        columns = {row[1] for row in c.execute("PRAGMA table_info(coverage_epochs)")}
        legacy_names = [row[0] for row in c.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name LIKE 'coverage_epochs_legacy_v%'"
        )]
        legacy_count = c.execute(
            f"SELECT COUNT(*) FROM {legacy_names[0]}"
        ).fetchone()[0]
        current_count = c.execute("SELECT COUNT(*) FROM coverage_epochs").fetchone()[0]
        state = c.execute(
            "SELECT coverage_started_block,verified_through_block,safe_head_block,"
            "status,provider_independent,last_error_kind FROM coverage_state"
        ).fetchone()
    finally:
        c.close()
    assert evm._COVERAGE_EPOCH_COLUMNS.issubset(columns)
    assert len(legacy_names) == 1 and legacy_count == 1 and current_count == 0
    assert state == (
        90, 89, None, "blocked", 0, "provider_identity_reaudit_required",
    )


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
    event = evm.parse_message(_notification({
        "number": "0x64", "timestamp": "0x5", "hash": "0x" + "ab" * 32,
    }))
    assert event.cursor == 100 and event.event_at.isoformat() == "1970-01-01T00:00:05+00:00"


def test_provider_identity_strips_credentials_and_matches_http_to_ws(evm):
    identity = evm.provider_id("wss://RPC.Example/path/token?api_key=secret")
    assert identity == evm.provider_id("https://rpc.example/another-secret")
    assert identity == _pid("rpc.example")
    assert "rpc.example" not in identity
    with pytest.raises(ValueError, match="endpoint"):
        evm.provider_id("not-an-endpoint")


def test_json_rpc_selects_only_provider_independent_from_websocket(evm, monkeypatch):
    rpc = evm.JsonRpc(("https://same.example/key", "https://audit.example/key"))
    called = []

    def request(endpoint, method, params):
        called.append(evm.provider_id(endpoint))
        return {"number": "0x1", "hash": "0x" + "ab" * 32}

    monkeypatch.setattr(rpc, "_call_endpoint", request)
    _result, identity = rpc.call_with_provider(
        "eth_getBlockByNumber", ["finalized", False],
        exclude_provider_ids={_pid("same.example")},
    )
    assert identity == _pid("audit.example")
    assert called == [_pid("audit.example")]

    same_only = evm.JsonRpc(("https://same.example/key",))
    with pytest.raises(evm.ProviderIndependenceError):
        same_only.call_with_provider(
            "eth_getBlockByNumber", ["finalized", False],
            exclude_provider_ids={_pid("same.example")},
        )


def test_rpc_timestamp_and_websocket_errors_never_leak_endpoint_secrets(
        evm, monkeypatch):
    secret_endpoint = "https://audit.example/private/token"
    rpc = evm.JsonRpc((secret_endpoint,))

    def fail_endpoint(_endpoint, _method, _params):
        raise RuntimeError(f"failed at {secret_endpoint}")

    monkeypatch.setattr(rpc, "_call_endpoint", fail_endpoint)
    with pytest.raises(RuntimeError) as all_failed:
        rpc.call_with_provider("eth_chainId", [])
    with pytest.raises(RuntimeError) as selected_failed:
        rpc.call_from_provider(_pid("audit.example"), "eth_chainId", [])
    assert "/private/token" not in str(all_failed.value)
    assert "/private/token" not in str(selected_failed.value)
    assert "error_kind=RuntimeError" in str(all_failed.value)

    records = []

    class Recorder:
        def warning(self, event, **kwargs):
            records.append((event, kwargs))

    class TimestampRpc:
        def call(self, _method, _params):
            raise RuntimeError(f"timestamp failed at {secret_endpoint}")

    monkeypatch.setattr(evm, "logger", Recorder())
    event = evm.parse_message(_notification(_log(evm)))
    assert evm.persist(event.payload, rpc=TimestampRpc()) == "timestamp_unavailable"
    assert "/private/token" not in repr(records)
    assert records[-1][1]["error_kind"] == "RuntimeError"

    spec = evm.FactorySpec(
        chain="bsc", venue="pancakeswap_v2", address=evm.PANCAKE_V2_FACTORY,
        event_kind="pair_v2", topic=evm.PAIR_CREATED_TOPIC,
        ws_urls=("wss://ws.example/private/token",),
        rpc_urls=("https://audit.example",),
    )
    runner = evm.build_runner(
        spec=spec, rpc=object(),
        socket_factory=lambda endpoint: (_ for _ in ()).throw(
            RuntimeError(f"connect failed at {endpoint}")),
    )
    with pytest.raises(ConnectionError) as ws_failed:
        runner.connect()
    assert "/private/token" not in str(ws_failed.value)
    assert "error_kind=RuntimeError" in str(ws_failed.value)


def test_runner_parser_requires_both_exact_subscription_acknowledgements(evm):
    spec = evm.bsc_pancake_v2_spec()
    parser = evm.EvmSubscriptionParser(spec)
    assert parser({"jsonrpc": "2.0", "id": 1, "result": "0xlogs"}) is None
    with pytest.raises(PermissionError, match="before both"):
        parser({"jsonrpc": "2.0", "method": "eth_subscription", "params": {
            "subscription": "0xlogs", "result": _log(evm),
        }})

    parser.reset()
    with pytest.raises(PermissionError, match="acknowledgement"):
        parser({"jsonrpc": "2.0", "id": 1, "result": None})
    assert parser({"jsonrpc": "2.0", "id": 1, "result": "0xlogs"}) is None
    assert parser({"jsonrpc": "2.0", "id": 2, "result": "0xheads"}) is None
    with pytest.raises(PermissionError, match="unacknowledged"):
        parser({"jsonrpc": "2.0", "method": "eth_subscription", "params": {
            "subscription": "0xother",
            "result": {"number": "0x64", "timestamp": "0x5",
                       "hash": "0x" + "ab" * 32},
        }})
    head = parser({"jsonrpc": "2.0", "method": "eth_subscription", "params": {
        "subscription": "0xheads",
        "result": {"number": "0x64", "timestamp": "0x5",
                   "hash": "0x" + "ab" * 32},
    }})
    assert head.cursor == 100
    launch = parser({"jsonrpc": "2.0", "method": "eth_subscription", "params": {
        "subscription": "0xlogs", "result": _log(evm),
    }})
    assert launch.payload["pool"] == "0x3333333333333333333333333333333333333333"


def test_critical_disk_guard_blocks_runner_before_factory_persist(evm, monkeypatch):
    from src.ops.stream_disk_guard import DiskStateGuard, StreamDiskCritical

    monkeypatch.setattr(
        evm.stream_disk_guard, "GUARD",
        DiskStateGuard(probe=lambda: {"state": "critical"}),
    )
    persisted = []
    monkeypatch.setattr(evm, "persist", lambda payload, **kwargs: persisted.append(payload))
    spec = evm.bsc_pancake_v2_spec()
    runner = evm.build_runner(spec=spec, rpc=object(), socket_factory=lambda _url: None)

    with pytest.raises(StreamDiskCritical):
        runner.on_event({"kind": "head", "block_number": 100})
    assert persisted == []


def test_critical_disk_guard_blocks_backfill_before_rpc_or_persist(evm, monkeypatch):
    from src.ops.stream_disk_guard import DiskStateGuard, StreamDiskCritical

    monkeypatch.setattr(
        evm.stream_disk_guard, "GUARD",
        DiskStateGuard(probe=lambda: {"state": "critical"}),
    )
    persisted = []
    monkeypatch.setattr(evm, "persist", lambda payload, **kwargs: persisted.append(payload))

    class Rpc:
        calls = 0

        def call(self, method, params):
            self.calls += 1
            return []

    rpc = Rpc()
    with pytest.raises(StreamDiskCritical):
        evm._backfill_blocks(100, 101, spec=evm.bsc_pancake_v2_spec(), rpc=rpc)
    assert rpc.calls == 0
    assert persisted == []


def test_critical_disk_blocks_coverage_before_db_or_rpc(evm, monkeypatch):
    from src.ops.stream_disk_guard import DiskStateGuard
    from src.pipeline import stream_health

    monkeypatch.setattr(
        evm.stream_disk_guard, "GUARD",
        DiskStateGuard(probe=lambda: {"state": "critical"}),
    )
    rpc = _CoverageRpc(head=100)
    assert not evm.DB.exists()
    state = evm.audit_finalized_coverage(
        evm.bsc_pancake_v2_spec(), rpc, ws_provider_id=_pid("ws.example"),
    )
    assert state["state"] == "blocked"
    assert state["last_error_kind"] == "disk_critical"
    assert rpc.calls == []
    assert not evm.DB.exists()  # bounded health is the only permitted write
    worker = next(
        row for row in stream_health.snapshot()
        if row["stream"] == evm.coverage_stream(evm.bsc_pancake_v2_spec())
    )
    assert worker["status"] == "degraded"


def test_complete_pool_stays_raw_unqualified(evm):
    class Rpc:
        def call(self, method, params):
            assert method == "eth_getBlockByNumber" and params[0] == "0x64"
            return {"number": "0x64", "timestamp": "0x5",
                    "hash": "0x" + "ab" * 32}

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
            return {"number": "0x64", "timestamp": "0x5",
                    "hash": "0x" + "ab" * 32}

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
            return {"number": "0x64", "timestamp": "0x5",
                    "hash": "0x" + "ab" * 32}

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
            return {"number": "0x64", "timestamp": "0x5",
                    "hash": "0x" + "ab" * 32}

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
                return {"number": "0x64", "timestamp": "0x5",
                        "hash": "0x" + "ab" * 32}
            raise AssertionError(method)

    assert evm.retry_open_gaps(spec, Rpc()) == {
        "attempted": 1, "advanced": 0, "recovered": 1, "failed": 0}
    assert stream_health.open_gaps("bsc", spec.stream) == []
    c = evm._conn()
    try:
        assert c.execute("SELECT COUNT(*) FROM raw_pools").fetchone()[0] == 1
    finally:
        c.close()


def test_contiguous_heads_do_not_impersonate_factory_log_coverage(evm):
    """The P0 regression: heads can be perfect while the log lane drops an event."""
    from src.pipeline import stream_health

    spec = evm.bsc_pancake_v2_spec()
    for block in (98, 99, 100):
        stream_health.observe(
            spec.chain, spec.stream, cursor=block, expect_contiguous=True,
        )
    transport = stream_health.snapshot()[0]
    assert transport["status"] == "live" and transport["open_gaps"] == 0

    rpc = _CoverageRpc(head=100, logs=[_log(evm, block=99)])
    state = evm.audit_finalized_coverage(
        spec, rpc, ws_provider_id=_pid("ws.example"), initial_lookback_blocks=3,
    )
    assert state["state"] == "verified"
    assert state["coverage_started_block"] == 98
    assert state["verified_through_block"] == 100
    assert state["provider_independent"] is True
    c = evm._conn()
    try:
        assert c.execute("SELECT COUNT(*) FROM raw_pools").fetchone()[0] == 1
        assert c.execute(
            "SELECT from_block,to_block,log_count,status FROM coverage_epochs"
        ).fetchone() == (98, 100, 1, "open")
    finally:
        c.close()


def test_coverage_watermark_advances_only_after_raw_evidence_writer(evm, monkeypatch):
    spec = evm.bsc_pancake_v2_spec()
    rpc = _CoverageRpc(head=100, logs=[_log(evm, block=100)])
    observed_during_write = []
    original = evm._persist_stream_event

    def persist_then_check(payload, **kwargs):
        observed_during_write.append(
            evm.coverage_snapshot(spec)["verified_through_block"]
        )
        return original(payload, **kwargs)

    monkeypatch.setattr(evm, "_persist_stream_event", persist_then_check)
    state = evm.audit_finalized_coverage(
        spec, rpc, ws_provider_id=_pid("ws.example"), initial_lookback_blocks=1,
    )
    assert observed_during_write == [99]  # frozen boundary, not audited block 100
    assert state["verified_through_block"] == 100


def test_coverage_persistence_failure_never_advances_prior_watermark(evm, monkeypatch):
    from src.pipeline import stream_health

    spec = evm.bsc_pancake_v2_spec()
    now = datetime.now(timezone.utc)
    first = evm.audit_finalized_coverage(
        spec, _CoverageRpc(head=100), ws_provider_id=_pid("ws.example"), now=now,
        initial_lookback_blocks=1,
    )
    assert first["state"] == "verified" and first["verified_through_block"] == 100

    def fail_writer(*_args, **_kwargs):
        raise OSError("disk writer failed")

    monkeypatch.setattr(evm, "_persist_stream_event", fail_writer)
    failed = evm.audit_finalized_coverage(
        spec, _CoverageRpc(head=101, logs=[_log(evm, block=101)]),
        ws_provider_id=_pid("ws.example"), now=now + timedelta(seconds=1),
    )
    assert failed["state"] == "blocked"
    assert failed["verified_through_block"] == 100
    # Failed observations never replace the last trusted safe-head checkpoint.
    assert failed["safe_head_block"] == 100
    assert failed["provider_independent"] is False
    assert failed["last_error_kind"] == "OSError"
    c = evm._conn()
    try:
        assert c.execute("SELECT COUNT(*) FROM coverage_epochs").fetchone()[0] == 1
    finally:
        c.close()
    health = next(
        row for row in stream_health.snapshot()
        if row["stream"] == evm.coverage_stream(spec)
    )
    assert health["status"] == "degraded"
    assert health["details"]["state"] == "blocked"


def test_first_safe_head_freezes_boundary_across_long_getlogs_outage(evm):
    spec = evm.bsc_pancake_v2_spec()
    now = datetime.now(timezone.utc)
    failed_rpc = _CoverageRpc(
        head=100, logs_failure=TimeoutError("archive unavailable"),
    )
    failed = evm.audit_finalized_coverage(
        spec, failed_rpc, ws_provider_id=_pid("ws.example"), now=now,
        initial_lookback_blocks=3,
    )
    assert failed["state"] == "blocked"
    assert failed["coverage_started_block"] == 98
    assert failed["verified_through_block"] == 97

    recovered_rpc = _CoverageRpc(head=5_000, head_at=now + timedelta(hours=2))
    recovered = evm.audit_finalized_coverage(
        spec, recovered_rpc, ws_provider_id=_pid("ws.example"),
        now=now + timedelta(hours=2), initial_lookback_blocks=3,
    )
    requested = next(
        call[2][0] for call in recovered_rpc.calls if call[1] == "eth_getLogs"
    )
    assert requested["fromBlock"] == hex(98)
    assert requested["toBlock"] == hex(1_999)
    assert recovered["coverage_started_block"] == 98
    assert recovered["verified_through_block"] == 1_999
    assert recovered["state"] == "catching_up"


def test_incomplete_timestamp_evidence_blocks_then_retries_same_watermark(evm):
    spec = evm.bsc_pancake_v2_spec()
    now = datetime.now(timezone.utc)
    item = _log(evm, block=100)
    incomplete = evm.audit_finalized_coverage(
        spec, _CoverageRpc(
            head=100, logs=[item],
            block_failure=TimeoutError("timestamp unavailable"),
        ),
        ws_provider_id=_pid("ws.example"), now=now, initial_lookback_blocks=1,
    )
    assert incomplete["state"] == "blocked"
    assert incomplete["coverage_started_block"] == 100
    assert incomplete["verified_through_block"] == 99
    c = evm._conn()
    try:
        assert c.execute(
            "SELECT evidence_state,block_at FROM raw_pools"
        ).fetchone() == ("timestamp_unavailable", None)
        assert c.execute("SELECT COUNT(*) FROM coverage_epochs").fetchone()[0] == 0
    finally:
        c.close()

    complete = evm.audit_finalized_coverage(
        spec, _CoverageRpc(head=100, logs=[item]),
        ws_provider_id=_pid("ws.example"), now=now + timedelta(seconds=61),
        initial_lookback_blocks=1,
    )
    assert complete["state"] == "verified"
    assert complete["verified_through_block"] == 100
    c = evm._conn()
    try:
        state, block_at = c.execute(
            "SELECT evidence_state,block_at FROM raw_pools"
        ).fetchone()
    finally:
        c.close()
    assert state == "complete" and block_at is not None


@pytest.mark.parametrize("block_response", [
    {"timestamp": "0x5", "hash": "0x" + "ab" * 32},
    {"number": "0x65", "timestamp": "0x5", "hash": "0x" + "ab" * 32},
])
def test_missing_or_wrong_block_number_never_advances_coverage(evm, block_response):
    spec = evm.bsc_pancake_v2_spec()

    class BadBlockRpc(_CoverageRpc):
        def call_from_provider(self, identity, method, params):
            if method == "eth_getBlockByNumber":
                self.calls.append((identity, method, params))
                return block_response
            return super().call_from_provider(identity, method, params)

    state = evm.audit_finalized_coverage(
        spec, BadBlockRpc(head=100, logs=[_log(evm, block=100)]),
        ws_provider_id=_pid("ws.example"), initial_lookback_blocks=1,
    )
    assert state["state"] == "blocked"
    assert state["coverage_started_block"] == 100
    assert state["verified_through_block"] == 99
    c = evm._conn()
    try:
        assert c.execute(
            "SELECT evidence_state,block_at FROM raw_pools"
        ).fetchone() == ("timestamp_unavailable", None)
        assert c.execute("SELECT COUNT(*) FROM coverage_epochs").fetchone()[0] == 0
    finally:
        c.close()


def test_complete_evidence_never_regresses_on_timestamp_replay(evm):
    event = evm.parse_message(_notification(_log(evm, block=100)))

    class GoodRpc:
        def call(self, method, params):
            assert method == "eth_getBlockByNumber"
            return {"number": params[0], "timestamp": "0x5",
                    "hash": "0x" + "ab" * 32}

    class TransientFailureRpc:
        def call(self, _method, _params):
            raise TimeoutError("temporary timestamp outage")

    assert evm.persist(event.payload, rpc=GoodRpc()) == "complete"
    assert evm.persist(event.payload, rpc=TransientFailureRpc()) == "complete"
    assert evm.persisted_evidence_state(event.payload) == "complete"
    c = evm._conn()
    try:
        assert c.execute(
            "SELECT evidence_state,block_at FROM raw_pools"
        ).fetchone() == ("complete", "1970-01-01T00:00:05+00:00")
    finally:
        c.close()


@pytest.mark.parametrize(
    ("ws_provider", "rpc", "expected_error"),
    [
        (None, _CoverageRpc(head=100), "missing_ws_provider"),
        (_pid("same.example"), _CoverageRpc(head=100, provider="same.example"),
         "ProviderIndependenceError"),
        (_pid("ws.example"), _CoverageRpc(head=100, chain_id=1), "RuntimeError"),
        (_pid("ws.example"), _CoverageRpc(head=100, failure=TimeoutError("down")),
         "TimeoutError"),
    ],
)
def test_missing_same_source_wrong_chain_or_rpc_failure_never_verify(
        evm, ws_provider, rpc, expected_error):
    from src.pipeline import stream_health

    spec = evm.bsc_pancake_v2_spec()
    state = evm.audit_finalized_coverage(
        spec, rpc, ws_provider_id=ws_provider, initial_lookback_blocks=1,
    )
    assert state["state"] == "blocked"
    assert state["provider_independent"] is False
    assert state["verified_through_block"] is None
    assert state["last_error_kind"] == expected_error
    worker = next(
        row for row in stream_health.snapshot()
        if row["stream"] == evm.coverage_stream(spec)
    )
    assert worker["status"] == "degraded"
    assert worker["details"]["state"] == "blocked"


def test_out_of_range_audit_log_fails_without_coverage_epoch(evm):
    spec = evm.bsc_pancake_v2_spec()
    state = evm.audit_finalized_coverage(
        spec, _CoverageRpc(head=100, logs=[_log(evm, block=97)]),
        ws_provider_id=_pid("ws.example"), initial_lookback_blocks=3,
    )
    assert state["state"] == "blocked"
    assert state["coverage_started_block"] == 98
    assert state["verified_through_block"] == 97
    c = evm._conn()
    try:
        assert c.execute("SELECT COUNT(*) FROM coverage_epochs").fetchone()[0] == 0
        assert c.execute("SELECT COUNT(*) FROM raw_pools").fetchone()[0] == 0
    finally:
        c.close()


def test_corrupt_same_provider_persisted_claim_fails_closed(evm):
    spec = evm.bsc_pancake_v2_spec()
    now = datetime.now(timezone.utc).isoformat()
    c = evm._conn()
    try:
        c.execute("""INSERT INTO coverage_state(
            chain,venue,factory,topic,coverage_started_block,
            verified_through_block,verified_through_hash,safe_head_block,
            safe_head_hash,safe_head_at,audit_duration_ms,verified_at,
            ws_provider_id,http_provider_id,provider_independent,status,
            consecutive_failures,next_retry_at,last_error_kind,updated_at
        ) VALUES (?,?,?,?,90,?,?,100,?,?,?,?,?,?,1,
                  'verified',0,NULL,NULL,?)""",
                  (*evm._coverage_key(spec), 100, "0x" + "ab" * 32,
                   "0x" + "ab" * 32, now, 1, now,
                   _pid("same.example"), _pid("same.example"), now))
        c.commit()
    finally:
        c.close()
    state = evm.coverage_snapshot(spec)
    assert state["state"] == "blocked"
    assert state["provider_independent"] is False
    assert state["last_error_kind"] == "invalid_coverage_proof"


def test_persisted_coverage_resumes_exactly_after_restart(evm):
    spec = evm.bsc_pancake_v2_spec()
    now = datetime.now(timezone.utc)
    first_rpc = _CoverageRpc(head=100)
    first = evm.audit_finalized_coverage(
        spec, first_rpc, ws_provider_id=_pid("ws.example"), now=now,
        initial_lookback_blocks=2,
    )
    assert first["coverage_started_block"] == 99
    assert first["verified_through_block"] == 100
    first_range = next(
        call[2][0] for call in first_rpc.calls if call[1] == "eth_getLogs"
    )
    assert first_range["fromBlock"] == "0x63" and first_range["toBlock"] == "0x64"

    # A new RPC object simulates a fresh worker process reading only durable state.
    second_rpc = _CoverageRpc(head=102)
    second = evm.audit_finalized_coverage(
        spec, second_rpc, ws_provider_id=_pid("ws.example"),
        now=now + timedelta(seconds=1), initial_lookback_blocks=50,
    )
    assert second["coverage_started_block"] == 99
    assert second["verified_through_block"] == 102
    second_range = next(
        call[2][0] for call in second_rpc.calls if call[1] == "eth_getLogs"
    )
    assert second_range["fromBlock"] == "0x65" and second_range["toBlock"] == "0x66"


def test_bounded_coverage_is_degraded_until_caught_up(evm):
    from src.pipeline import stream_health

    spec = evm.bsc_pancake_v2_spec()
    now = datetime.now(timezone.utc)
    first = evm.audit_finalized_coverage(
        spec, _CoverageRpc(head=5_000), ws_provider_id=_pid("ws.example"), now=now,
        initial_lookback_blocks=2_500, max_blocks=2_000,
    )
    assert first["state"] == "catching_up"
    assert first["verified_through_block"] == 3_999
    assert first["lag_blocks"] == 1_001
    worker = next(
        row for row in stream_health.snapshot()
        if row["stream"] == evm.coverage_stream(spec)
    )
    assert worker["status"] == "degraded"

    second = evm.audit_finalized_coverage(
        spec, _CoverageRpc(head=5_000), ws_provider_id=_pid("ws.example"),
        now=now + timedelta(seconds=1), max_blocks=2_000,
    )
    assert second["state"] == "verified"
    assert second["verified_through_block"] == 5_000


def test_fixed_epochs_aggregate_and_prune_to_hard_retention(evm, monkeypatch):
    monkeypatch.setattr(evm, "COVERAGE_EPOCH_BLOCKS", 2)
    monkeypatch.setattr(evm, "COVERAGE_EPOCH_RETENTION", 2)
    spec = evm.bsc_pancake_v2_spec()
    now = datetime.now(timezone.utc)
    for offset, head in enumerate((0, 2, 2, 4, 4)):
        evm.audit_finalized_coverage(
            spec, _CoverageRpc(head=head), ws_provider_id=_pid("ws.example"),
            now=now + timedelta(seconds=offset), initial_lookback_blocks=1,
        )
    c = evm._conn()
    try:
        rows = c.execute(
            "SELECT epoch_start_block,from_block,to_block,segment_count "
            "FROM coverage_epochs ORDER BY epoch_start_block"
        ).fetchall()
        persisted_start = c.execute(
            "SELECT coverage_started_block FROM coverage_state WHERE "
            "chain=? AND venue=? AND factory=? AND topic=?",
            evm._coverage_key(spec),
        ).fetchone()[0]
    finally:
        c.close()
    assert rows == [(2, 2, 3, 2), (4, 4, 4, 1)]
    assert persisted_start == 2
    snapshot = evm.coverage_snapshot(spec)
    assert snapshot["state"] == "verified"
    assert snapshot["coverage_started_block"] == 2


def test_legacy_retention_false_claim_blocks_immediately_and_repairs_on_audit(
        evm, monkeypatch):
    monkeypatch.setattr(evm, "COVERAGE_EPOCH_BLOCKS", 2)
    monkeypatch.setattr(evm, "COVERAGE_EPOCH_RETENTION", 2)
    spec = evm.bsc_pancake_v2_spec()
    now = datetime.now(timezone.utc)
    for offset, head in enumerate((0, 2, 2, 4, 4)):
        evm.audit_finalized_coverage(
            spec, _CoverageRpc(head=head), ws_provider_id=_pid("ws.example"),
            now=now + timedelta(seconds=offset), initial_lookback_blocks=1,
        )

    c = evm._conn()
    try:
        c.execute(
            "UPDATE coverage_state SET coverage_started_block=0 WHERE "
            "chain=? AND venue=? AND factory=? AND topic=?",
            evm._coverage_key(spec),
        )
        c.commit()
        before_epochs = c.execute(
            "SELECT epoch_start_block,from_block,to_block FROM coverage_epochs "
            "ORDER BY epoch_start_block"
        ).fetchall()
    finally:
        c.close()

    false_claim = evm.coverage_snapshot(spec)
    assert false_claim["state"] == "blocked"
    assert false_claim["last_error_kind"] == "retention_boundary_mismatch"
    assert false_claim["coverage_started_block"] == 0

    rpc = _CoverageRpc(head=4)
    repaired = evm.audit_finalized_coverage(
        spec, rpc, ws_provider_id=_pid("ws.example"),
        now=now + timedelta(seconds=5), initial_lookback_blocks=1,
    )
    assert repaired["state"] == "verified"
    assert repaired["coverage_started_block"] == 2
    assert not any(call[1] == "eth_getLogs" for call in rpc.calls)
    c = evm._conn()
    try:
        assert c.execute(
            "SELECT coverage_started_block FROM coverage_state WHERE "
            "chain=? AND venue=? AND factory=? AND topic=?",
            evm._coverage_key(spec),
        ).fetchone()[0] == 2
        assert c.execute(
            "SELECT epoch_start_block,from_block,to_block FROM coverage_epochs "
            "ORDER BY epoch_start_block"
        ).fetchall() == before_epochs
        c.execute(
            "DELETE FROM coverage_epochs WHERE chain=? AND venue=? "
            "AND factory=? AND topic=?",
            evm._coverage_key(spec),
        )
        c.commit()
    finally:
        c.close()

    missing_evidence = evm.coverage_snapshot(spec)
    assert missing_evidence["state"] == "blocked"
    assert missing_evidence["last_error_kind"] == "retention_boundary_mismatch"
    refused = evm.audit_finalized_coverage(
        spec, _CoverageRpc(head=4), ws_provider_id=_pid("ws.example"),
        now=now + timedelta(seconds=6), initial_lookback_blocks=1,
    )
    assert refused["state"] == "blocked"
    assert refused["verified_through_block"] == 4


def test_active_websocket_provider_exists_only_for_open_connection(evm):
    spec = evm.FactorySpec(
        chain="bsc", venue="pancakeswap_v2", address=evm.PANCAKE_V2_FACTORY,
        event_kind="pair_v2", topic=evm.PAIR_CREATED_TOPIC,
        ws_urls=("wss://ws.example/private/key",),
        rpc_urls=("https://audit.example",),
    )

    class RawSocket:
        def close(self): pass
        def shutdown(self): pass

    runner = evm.build_runner(
        spec=spec, rpc=object(), socket_factory=lambda _endpoint: RawSocket(),
    )
    socket = runner.connect()
    assert evm.active_ws_provider(spec) == _pid("ws.example")
    socket.close()
    assert evm.active_ws_provider(spec) is None


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


def test_backfill_wrapper_never_logs_rpc_exception_text(evm, monkeypatch):
    secret = "https://tenant:key@rpc.example/private/token"
    logged = []

    class CaptureLogger:
        def warning(self, event, **kwargs):
            logged.append((event, kwargs))

    def fail(*_args, **_kwargs):
        raise RuntimeError(f"provider failed {secret}")

    monkeypatch.setattr(evm, "logger", CaptureLogger())
    monkeypatch.setattr(evm, "_backfill_blocks", fail)
    assert evm.backfill_blocks(
        10, 10, spec=evm.bsc_pancake_v2_spec(), rpc=object(),
    ) is False
    assert secret not in repr(logged)
    assert logged == [("evm_factory_backfill_failed", {
        "chain": "bsc", "start": 10, "end": 10,
        "error_kind": "RuntimeError",
    })]


def test_oversized_gap_checkpoints_one_verified_prefix_per_retry(evm):
    from src.pipeline import stream_health

    spec = evm.bsc_pancake_v2_spec()
    start = 100
    stream_health.observe("bsc", spec.stream, cursor=start, expect_contiguous=True)
    stream_health.observe("bsc", spec.stream,
                          cursor=start + evm.MAX_BACKFILL_BLOCKS + 2,
                          expect_contiguous=True)
    requests = []

    class Rpc:
        def call(self, method, params):
            assert method == "eth_getLogs"
            request = params[0]
            requests.append((int(request["fromBlock"], 16),
                             int(request["toBlock"], 16)))
            return []

    assert evm.retry_open_gaps(spec, Rpc()) == {
        "attempted": 1, "advanced": 1, "recovered": 0, "failed": 0}
    remaining = stream_health.open_gaps("bsc", spec.stream)
    assert [(gap["from_cursor"], gap["to_cursor"]) for gap in remaining] == [
        (start + evm.MAX_BACKFILL_BLOCKS + 1,
         start + evm.MAX_BACKFILL_BLOCKS + 1)]
    assert stream_health.snapshot()[0]["status"] == "degraded"

    assert evm.retry_open_gaps(spec, Rpc()) == {
        "attempted": 1, "advanced": 0, "recovered": 1, "failed": 0}
    assert requests == [
        (start + 1, start + evm.MAX_BACKFILL_BLOCKS),
        (start + evm.MAX_BACKFILL_BLOCKS + 1,
         start + evm.MAX_BACKFILL_BLOCKS + 1),
    ]
    assert stream_health.open_gaps("bsc", spec.stream) == []
    assert stream_health.snapshot()[0]["status"] == "live"


def test_failed_gap_prefix_does_not_advance_or_claim_recovery(evm, monkeypatch):
    from src.pipeline import stream_health

    spec = evm.base_aerodrome_spec()
    secret = "wss://tenant:key@rpc.example/private/token"
    logged = []

    class CaptureLogger:
        def warning(self, event, **kwargs):
            logged.append((event, kwargs))

    monkeypatch.setattr(evm, "logger", CaptureLogger())
    stream_health.observe("base", spec.stream, cursor=10, expect_contiguous=True)
    stream_health.observe("base", spec.stream,
                          cursor=evm.MAX_BACKFILL_BLOCKS + 12,
                          expect_contiguous=True)

    class Rpc:
        def call(self, method, params):
            raise RuntimeError(f"temporary RPC failure {secret}")

    before = stream_health.open_gaps("base", spec.stream)
    assert evm.retry_open_gaps(spec, Rpc()) == {
        "attempted": 1, "advanced": 0, "recovered": 0, "failed": 1}
    after = stream_health.open_gaps("base", spec.stream)
    assert after == []  # persistent backoff keeps the failed prefix out of the due queue
    health = stream_health.snapshot()[0]
    assert health["status"] == "degraded"
    assert health["open_gaps"] == 1
    assert health["deferred_gaps"] == 1
    assert health["next_gap_retry_at"] is not None
    c = stream_health._conn()
    try:
        stored = c.execute(
            "SELECT from_cursor,to_cursor,retry_count,last_error FROM gaps"
        ).fetchone()
    finally:
        c.close()
    assert stored[:3] == (
        before[0]["from_cursor"], before[0]["to_cursor"], 1,
    )
    assert stored[3] == "EVM gap retry failed; error_kind=RuntimeError"
    assert secret not in repr((stored, logged))
    assert logged[0][1]["error_kind"] == "RuntimeError"


def _persist_complete(evm):
    class Rpc:
        def call(self, method, params):
            return {"number": "0x64", "timestamp": "0x5",
                    "hash": "0x" + "ab" * 32}

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


def test_provider_fingerprint_is_hostname_level_opaque_and_strict(evm):
    expected = evm.provider_id("https://same.example")
    assert expected == evm.provider_id("wss://same.example:443/private/key")
    assert expected == evm.provider_id("http://same.example:8123/other")
    assert expected == _pid("same.example")
    assert "same.example" not in expected and len(expected) == 73
    with pytest.raises(ValueError):
        evm._canonical_provider_identity("same.example")
    with pytest.raises(ValueError):
        evm._canonical_provider_identity("same.example:443")
    for endpoint in (
        " https://same.example", "https://same.example/path with space",
        "https://ténant.example/key",
    ):
        with pytest.raises(ValueError, match="endpoint"):
            evm.provider_id(endpoint)


class _RpcResponse(io.BytesIO):
    def __init__(self, payload, final_url):
        super().__init__(json.dumps(payload).encode())
        self.final_url = final_url

    def geturl(self):
        return self.final_url

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()


def test_http_jsonrpc_envelope_uses_unique_exact_id_and_xor(evm, monkeypatch):
    request_ids = []

    def valid(request, timeout):
        body = json.loads(request.data)
        request_ids.append(body["id"])
        return _RpcResponse(
            {"jsonrpc": "2.0", "id": body["id"], "result": False},
            request.full_url,
        )

    monkeypatch.setattr(evm.urllib.request, "urlopen", valid)
    rpc = evm.JsonRpc(("https://audit.example/private/token",))
    assert rpc.call("eth_chainId", []) is False
    assert rpc.call("eth_chainId", []) is False
    assert len(set(request_ids)) == 2
    assert all(isinstance(value, str) and len(value) == 32 for value in request_ids)

    def malformed(kind):
        def respond(request, timeout):
            body = json.loads(request.data)
            base = {"jsonrpc": "2.0", "id": body["id"], "result": None}
            if kind == "version":
                base["jsonrpc"] = "1.0"
            elif kind == "id":
                base["id"] = True
            elif kind == "both":
                base["error"] = {}
            elif kind == "neither":
                base.pop("result")
            return _RpcResponse(base, request.full_url)
        return respond

    for kind in ("version", "id", "both", "neither"):
        monkeypatch.setattr(evm.urllib.request, "urlopen", malformed(kind))
        with pytest.raises(RuntimeError):
            rpc._call_endpoint("https://audit.example/private/token", "eth_chainId", [])


def test_http_jsonrpc_remote_error_and_redirects_never_leak_or_cross_provider(
        evm, monkeypatch):
    secret = "/private/token"

    def remote_error(request, timeout):
        body = json.loads(request.data)
        return _RpcResponse({
            "jsonrpc": "2.0", "id": body["id"],
            "error": {"code": -32000, "message": f"failed {secret}"},
        }, request.full_url)

    monkeypatch.setattr(evm.urllib.request, "urlopen", remote_error)
    rpc = evm.JsonRpc(("https://audit.example/private/token",))
    with pytest.raises(RuntimeError) as rejected:
        rpc._call_endpoint("https://audit.example/private/token", "eth_chainId", [])
    assert secret not in str(rejected.value)

    def redirected(final_url):
        def respond(request, timeout):
            body = json.loads(request.data)
            return _RpcResponse(
                {"jsonrpc": "2.0", "id": body["id"], "result": "0x38"},
                final_url,
            )
        return respond

    for final_url in ("https://ws.example/rpc", "http://audit.example/rpc"):
        monkeypatch.setattr(evm.urllib.request, "urlopen", redirected(final_url))
        with pytest.raises(evm.ProviderIndependenceError):
            rpc._call_endpoint("https://audit.example/private/token", "eth_chainId", [])


def test_ws_jsonrpc_envelope_and_native_subscription_types_fail_closed(evm):
    parser = evm.EvmSubscriptionParser(evm.bsc_pancake_v2_spec())
    invalid_acks = (
        {"jsonrpc": "1.0", "id": 1, "result": "logs"},
        {"jsonrpc": "2.0", "id": True, "result": "logs"},
        {"jsonrpc": "2.0", "id": 1, "result": "logs", "error": {}},
        {"jsonrpc": "2.0", "id": 1,
         "error": {"message": "/private/token"}},
    )
    for message in invalid_acks:
        with pytest.raises((ValueError, PermissionError)) as rejected:
            parser(message)
        assert "/private/token" not in str(rejected.value)

    assert parser({"jsonrpc": "2.0", "id": 1, "result": "1"}) is None
    assert parser({"jsonrpc": "2.0", "id": 2, "result": "heads"}) is None
    with pytest.raises(PermissionError, match="subscription id"):
        parser({"jsonrpc": "2.0", "method": "eth_subscription", "params": {
            "subscription": 1,
            "result": {"number": "0x1", "timestamp": "0x1",
                       "hash": "0x" + "ab" * 32},
        }})


@pytest.mark.parametrize("quantity", ["-0x1", "0x00", "0x0400", 1, True, "1"])
def test_remote_quantities_are_canonical_nonnegative_hex(evm, quantity):
    with pytest.raises(ValueError, match="block number"):
        evm.parse_message(_notification({
            "number": quantity, "timestamp": "0x1",
            "hash": "0x" + "ab" * 32,
        }))


def test_rpc_data_prefixes_abi_padding_and_required_log_fields_are_strict(evm):
    item = _log(evm)
    item["topics"][1] = "0x" + "1" + item["topics"][1][3:]
    with pytest.raises(ValueError, match="token0"):
        evm.parse_message(_notification(item))

    for field in ("transactionHash", "blockHash"):
        item = _log(evm)
        item[field] = item[field][2:]
        with pytest.raises(ValueError):
            evm.parse_message(_notification(item))
    item = _log(evm)
    item["data"] = item["data"][2:]
    with pytest.raises(ValueError, match="0x-prefixed"):
        evm.parse_message(_notification(item))
    item = _log(evm)
    item.pop("removed")
    with pytest.raises(ValueError, match="removed flag"):
        evm.parse_message(_notification(item))
    with pytest.raises(ValueError, match="block hash"):
        evm.parse_message(_notification({"number": "0x1", "timestamp": "0x1"}))


@pytest.mark.parametrize("block_response", [
    {"number": "0x64", "timestamp": "0x5"},
    {"number": "0x64", "timestamp": "0x5", "hash": "0x" + "ef" * 32},
])
def test_missing_or_wrong_canonical_block_hash_never_completes_evidence(
        evm, block_response):
    event = evm.parse_message(_notification(_log(evm)))

    class Rpc:
        def call(self, _method, _params):
            return block_response

    assert evm.persist(event.payload, rpc=Rpc()) == "timestamp_unavailable"
    assert evm.persisted_evidence_state(event.payload) == "timestamp_unavailable"


def test_checkpoint_hash_flip_and_higher_fork_never_extend_proof(evm):
    spec = evm.bsc_pancake_v2_spec()
    now = datetime.now(timezone.utc)
    first = evm.audit_finalized_coverage(
        spec, _CoverageRpc(head=100, head_hash="0x" + "aa" * 32),
        ws_provider_id=_pid("ws.example"), now=now,
        initial_lookback_blocks=1,
    )
    assert first["state"] == "verified"

    same_height = evm.audit_finalized_coverage(
        spec, _CoverageRpc(head=100, head_hash="0x" + "bb" * 32,
                           head_at=now + timedelta(seconds=61)),
        ws_provider_id=_pid("ws.example"), now=now + timedelta(seconds=61),
    )
    assert same_height["state"] == "blocked"
    assert same_height["last_error_kind"] == "CoverageCheckpointMismatch"
    assert same_height["safe_head_hash"] == "0x" + "aa" * 32

    higher = evm.audit_finalized_coverage(
        spec, _CoverageRpc(
            head=101, head_hash="0x" + "cc" * 32,
            block_hashes={100: "0x" + "bb" * 32},
            head_at=now + timedelta(seconds=122),
        ),
        ws_provider_id=_pid("ws.example"), now=now + timedelta(seconds=122),
    )
    assert higher["state"] == "blocked"
    assert higher["verified_through_block"] == 100


def test_catching_up_safe_head_must_never_regress(evm):
    spec = evm.bsc_pancake_v2_spec()
    now = datetime.now(timezone.utc)
    first = evm.audit_finalized_coverage(
        spec, _CoverageRpc(head=5_000), ws_provider_id=_pid("ws.example"),
        now=now, initial_lookback_blocks=2_500, max_blocks=2_000,
    )
    assert first["state"] == "catching_up" and first["safe_head_block"] == 5_000
    regressed = evm.audit_finalized_coverage(
        spec, _CoverageRpc(head=4_500, head_at=now + timedelta(seconds=1)),
        ws_provider_id=_pid("ws.example"), now=now + timedelta(seconds=1),
    )
    assert regressed["state"] == "blocked"
    assert regressed["last_error_kind"] == "CoverageCheckpointMismatch"
    assert regressed["safe_head_block"] == 5_000


def test_stale_future_or_missing_finalized_timestamp_never_requests_logs(evm):
    spec = evm.bsc_pancake_v2_spec()
    now = datetime.now(timezone.utc).replace(microsecond=0)
    for audit_now, head_at, expected in (
        (now, now - timedelta(seconds=121), "FinalizedHeadStale"),
        (now + timedelta(seconds=61), now + timedelta(seconds=62),
         "FinalizedHeadFuture"),
    ):
        rpc = _CoverageRpc(head=100, head_at=head_at)
        state = evm.audit_finalized_coverage(
            spec, rpc, ws_provider_id=_pid("ws.example"), now=audit_now,
            initial_lookback_blocks=1,
        )
        assert state["state"] == "blocked" and state["last_error_kind"] == expected
        assert not any(call[1] == "eth_getLogs" for call in rpc.calls)

    class MissingTimestamp(_CoverageRpc):
        def call_with_provider(self, *args, **kwargs):
            header, provider = super().call_with_provider(*args, **kwargs)
            header.pop("timestamp")
            return header, provider

    state = evm.audit_finalized_coverage(
        spec, MissingTimestamp(head=100), ws_provider_id=_pid("ws.example"),
        now=now + timedelta(seconds=122), initial_lookback_blocks=1,
    )
    assert state["state"] == "blocked"
    assert state["verified_through_block"] is None


def test_http_omission_or_payload_conflict_blocks_exact_set_coverage(evm):
    spec = evm.bsc_pancake_v2_spec()
    ws_event = evm.parse_message(_notification(_log(evm)))
    assert evm.persist(ws_event.payload, rpc=_CanonicalBlockRpc()) == "complete"

    omitted = evm.audit_finalized_coverage(
        spec, _CoverageRpc(head=100, logs=[]),
        ws_provider_id=_pid("ws.example"), initial_lookback_blocks=1,
    )
    assert omitted["state"] == "blocked"
    assert omitted["last_error_kind"] == "CoverageEvidenceConflict"
    c = evm._conn()
    try:
        assert c.execute("SELECT COUNT(*) FROM coverage_epochs").fetchone()[0] == 0
    finally:
        c.close()

    conflict = _log(evm)
    conflict["data"] = conflict["data"][:-64] + f"{43:064x}"
    retried = evm.audit_finalized_coverage(
        spec, _CoverageRpc(head=100, logs=[conflict],
                           head_at=datetime.now(timezone.utc) + timedelta(seconds=61)),
        ws_provider_id=_pid("ws.example"),
        now=datetime.now(timezone.utc) + timedelta(seconds=61),
    )
    assert retried["state"] == "blocked"
    c = evm._conn()
    try:
        assert c.execute("SELECT pair_index FROM raw_pools").fetchone()[0] == 42
    finally:
        c.close()


def test_final_transaction_rereads_raw_set_and_catches_precommit_race(
        evm, monkeypatch):
    spec = evm.bsc_pancake_v2_spec()
    injected = evm.parse_message(_notification(_log(evm))).payload
    original = evm._reconcile_persisted_range
    inserted = False

    def inject_after_precheck(*args, **kwargs):
        nonlocal inserted
        result = original(*args, **kwargs)
        if kwargs.get("connection") is None and not inserted:
            inserted = True
            evm.persist(injected, rpc=_CanonicalBlockRpc())
        return result

    monkeypatch.setattr(evm, "_reconcile_persisted_range", inject_after_precheck)
    state = evm.audit_finalized_coverage(
        spec, _CoverageRpc(head=100, logs=[]),
        ws_provider_id=_pid("ws.example"), initial_lookback_blocks=1,
    )
    assert inserted and state["state"] == "blocked"
    assert state["verified_through_block"] == 99


def test_late_websocket_evidence_atomically_revokes_verified_empty_range(evm):
    spec = evm.bsc_pancake_v2_spec()
    state = evm.audit_finalized_coverage(
        spec, _CoverageRpc(head=100, logs=[]),
        ws_provider_id=_pid("ws.example"), initial_lookback_blocks=1,
    )
    assert state["state"] == "verified"
    event = evm.parse_message(_notification(_log(evm))).payload
    assert evm.persist(event, rpc=_CanonicalBlockRpc()) == "complete"
    revoked = evm.coverage_snapshot(spec)
    assert revoked["state"] == "blocked"
    assert revoked["verified_through_block"] == 99
    assert revoked["verified_through_hash"] is None
    assert revoked["last_error_kind"] == "late_evidence_after_coverage"
    c = evm._conn()
    try:
        assert c.execute("SELECT COUNT(*) FROM coverage_epochs").fetchone()[0] == 0
    finally:
        c.close()


def test_raw_identity_compare_and_set_is_safe_under_concurrent_conflict(evm):
    evm._conn().close()
    first = evm.parse_message(_notification(_log(evm))).payload
    second = dict(first, pair_index=43)

    def write(payload):
        try:
            return evm.persist(payload, rpc=_CanonicalBlockRpc())
        except evm.RawEvidenceConflict:
            return "conflict"

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(write, (first, second)))
    assert sorted(results) == ["complete", "conflict"]
    c = evm._conn()
    try:
        row = c.execute("SELECT pair_index,raw_payload_hash FROM raw_pools").fetchone()
    finally:
        c.close()
    assert row in ((42, evm._hash(first)), (43, evm._hash(second)))


def test_same_provider_reconnect_generation_prevents_old_socket_clear(evm):
    spec = evm.FactorySpec(
        chain="bsc", venue="pancakeswap_v2", address=evm.PANCAKE_V2_FACTORY,
        event_kind="pair_v2", topic=evm.PAIR_CREATED_TOPIC,
        ws_urls=("wss://tenant-key.ws.example/private/token",),
        rpc_urls=("https://audit.example",),
    )

    class RawSocket:
        def close(self): pass
        def shutdown(self): pass

    runner = evm.build_runner(
        spec=spec, rpc=object(), socket_factory=lambda _url: RawSocket(),
    )
    first = runner.connect()
    first_binding = evm.active_ws_connection(spec)
    second = runner.connect()
    second_binding = evm.active_ws_connection(spec)
    assert first_binding[0] == second_binding[0] == _pid("tenant-key.ws.example")
    assert first_binding[1] != second_binding[1]
    first.close()
    assert evm.active_ws_connection(spec) == second_binding
    second.close()
    assert evm.active_ws_connection(spec) is None
    assert "tenant-key.ws.example" not in repr(
        evm.stream_health.snapshot()
    )


@pytest.mark.parametrize(
    ("failure_path", "public_operation"),
    (("send", "send"), ("recv", "receive"), ("ping", "heartbeat")),
)
def test_websocket_io_errors_never_persist_or_log_endpoint_secrets(
        evm, monkeypatch, failure_path, public_operation):
    from src.pipeline import stream_runner
    from websocket import WebSocketTimeoutException

    secret = "tenant-key:password@ws.example/private/token"
    endpoint = f"wss://{secret}"
    stop = threading.Event()
    logged = []

    class CaptureLogger:
        def warning(self, event, **kwargs):
            logged.append((event, kwargs))

    class RawSocket:
        def send(self, _payload):
            if failure_path == "send":
                stop.set()
                raise RuntimeError(f"socket failed {endpoint}")

        def recv(self):
            if failure_path == "ping":
                raise WebSocketTimeoutException(f"timeout from {endpoint}")
            stop.set()
            raise RuntimeError(f"socket failed {endpoint}")

        def ping(self):
            stop.set()
            raise RuntimeError(f"heartbeat failed {endpoint}")

        def close(self): pass
        def shutdown(self): pass

    monkeypatch.setattr(stream_runner, "logger", CaptureLogger())
    spec = evm.FactorySpec(
        chain="bsc", venue="pancakeswap_v2", address=evm.PANCAKE_V2_FACTORY,
        event_kind="pair_v2", topic=evm.PAIR_CREATED_TOPIC,
        ws_urls=(endpoint,), rpc_urls=("https://audit.example",),
    )
    runner = evm.build_runner(
        spec=spec, rpc=object(), socket_factory=lambda _url: RawSocket(),
    )
    runner.heartbeat_seconds = 0
    runner.run_forever(stop)

    public = repr((evm.stream_health.snapshot(), logged))
    assert secret not in public and "/private/token" not in public
    assert f"EVM websocket {public_operation} failed; error_kind=RuntimeError" in public


def test_generation_health_write_failure_closes_socket_and_never_publishes_active(
        evm, monkeypatch):
    spec = evm.bsc_pancake_v2_spec()

    class RawSocket:
        closed = shutdown_called = False
        def close(self): self.closed = True
        def shutdown(self): self.shutdown_called = True

    raw = RawSocket()
    monkeypatch.setattr(
        evm.stream_health, "report_worker",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(sqlite3.OperationalError("locked")),
    )
    runner = evm.build_runner(
        spec=spec, rpc=object(), socket_factory=lambda _url: raw,
    )
    with pytest.raises(ConnectionError, match="coverage gate"):
        runner.connect()
    assert raw.closed and raw.shutdown_called
    assert evm.active_ws_connection(spec) is None


def test_old_audit_generation_cannot_overwrite_new_generation_health(evm):
    spec = evm.bsc_pancake_v2_spec()
    evm._mark_coverage_reaudit_required(
        spec, identity=_pid("ws.example"), generation="a" * 32,
    )
    evm._mark_coverage_reaudit_required(
        spec, identity=_pid("ws.example"), generation="b" * 32,
    )
    state = {
        "state": "verified", "provider_independent": True,
        "coverage_started_block": 100, "verified_through_block": 100,
        "verified_through_hash": "0x" + "ab" * 32,
        "safe_head_block": 100, "safe_head_hash": "0x" + "ab" * 32,
        "safe_head_at": datetime.now(timezone.utc).isoformat(),
        "audit_duration_ms": 1, "verified_at": datetime.now(timezone.utc).isoformat(),
        "lag_blocks": 0, "ws_provider_id": _pid("ws.example"),
        "http_provider_id": _pid("audit.example"),
    }
    assert evm._report_coverage(
        spec, state, connection_generation="a" * 32,
    ) is False
    row = next(item for item in evm.stream_health.snapshot()
               if item["stream"] == evm.coverage_stream(spec))
    assert row["status"] == "degraded"
    assert row["details"]["connection_generation"] == "b" * 32


def test_generation_cas_requires_outgoing_details_to_preserve_generation(evm):
    health = evm.stream_health
    source, stream = "bsc", "coverage:test"
    current = "a" * 32
    health.report_worker(
        source, stream, status="degraded",
        details={"connection_generation": current},
    )

    for details in ({}, {"connection_generation": "b" * 32}):
        with pytest.raises(ValueError, match="preserve the expected generation"):
            health.report_worker_if_connection_generation(
                source, stream, expected_generation=current, status="live",
                details=details,
            )
    assert health.report_worker_if_connection_generation(
        source, stream, expected_generation="b" * 32, status="live",
        details={"connection_generation": "b" * 32},
    ) is False
    unchanged = health.snapshot()[0]
    assert unchanged["status"] == "degraded"
    assert unchanged["details"]["connection_generation"] == current

    assert health.report_worker_if_connection_generation(
        source, stream, expected_generation=current, status="live",
        details={"connection_generation": current, "state": "verified"},
    ) is True
    updated = health.snapshot()[0]
    assert updated["status"] == "live"
    assert updated["details"]["connection_generation"] == current


def test_connection_gate_writes_coverage_once_and_rejects_redirect_handshake(
        evm, monkeypatch):
    spec = evm.bsc_pancake_v2_spec()
    writes = []
    original = evm.stream_health.report_worker

    def record(*args, **kwargs):
        writes.append((args, kwargs))
        return original(*args, **kwargs)

    monkeypatch.setattr(evm.stream_health, "report_worker", record)

    class RawSocket:
        closed = shutdown_called = False
        handshake_response = type("Handshake", (), {"status": 302})()
        def close(self): self.closed = True
        def shutdown(self): self.shutdown_called = True

    redirected = RawSocket()
    runner = evm.build_runner(
        spec=spec, rpc=object(), socket_factory=lambda _url: redirected,
    )
    with pytest.raises(ConnectionError):
        runner.connect()
    assert redirected.closed and redirected.shutdown_called
    assert evm.active_ws_connection(spec) is None
    assert writes == []

    class Upgraded(RawSocket):
        handshake_response = type("Handshake", (), {"status": 101})()

    runner = evm.build_runner(
        spec=spec, rpc=object(), socket_factory=lambda _url: Upgraded(),
    )
    socket = runner.connect()
    assert len([call for call in writes
                if call[0][1] == evm.coverage_stream(spec)]) == 1
    socket.close()


def test_combined_legacy_migration_preserves_quarantine_and_recovers(evm):
    _install_legacy_coverage_schema(
        evm, with_epoch=True, with_verified_state=True,
    )
    c = evm._conn()
    try:
        legacy = c.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name LIKE 'coverage_epochs_legacy_v%'"
        ).fetchone()[0]
        assert c.execute(f"SELECT COUNT(*) FROM {legacy}").fetchone()[0] == 1
        identities = c.execute(
            f"SELECT ws_provider_id,http_provider_id FROM {legacy}"
        ).fetchone()
        assert all(value.startswith("provider:") for value in identities)
    finally:
        c.close()
    quarantined = evm.coverage_snapshot(evm.bsc_pancake_v2_spec())
    assert quarantined["state"] == "blocked"
    assert quarantined["verified_through_block"] == 89
    assert quarantined["safe_head_block"] is None
    assert quarantined["next_retry_at"] is None

    recovered = evm.audit_finalized_coverage(
        evm.bsc_pancake_v2_spec(), _CoverageRpc(head=100),
        ws_provider_id=_pid("ws.example"), initial_lookback_blocks=11,
    )
    assert recovered["state"] == "verified"
    assert recovered["verified_through_hash"] == "0x" + "ab" * 32


def test_connection_initialization_failure_always_closes_handle(evm, monkeypatch):
    handles = []

    class Handle:
        closed = False
        def rollback(self): pass
        def close(self): self.closed = True

    def connect(*_args, **_kwargs):
        handle = Handle()
        handles.append(handle)
        return handle

    monkeypatch.setattr(evm.sqlite3, "connect", connect)
    monkeypatch.setattr(
        evm, "_initialize_connection",
        lambda _handle: (_ for _ in ()).throw(sqlite3.OperationalError("migration failed")),
    )
    for _index in range(50):
        with pytest.raises(sqlite3.OperationalError):
            evm._conn()
    assert len(handles) == 50 and all(handle.closed for handle in handles)


def test_disk_turning_critical_after_zero_log_rpc_never_commits_proof(evm, monkeypatch):
    from src.ops.stream_disk_guard import StreamDiskCritical

    class TransitionGuard:
        calls = 0

        def require_evidence_write(self, source):
            self.calls += 1
            if self.calls >= 4:
                raise StreamDiskCritical(source, {"state": "critical"})
            return {"state": "ok"}

    guard = TransitionGuard()
    monkeypatch.setattr(evm.stream_disk_guard, "GUARD", guard)
    spec = evm.bsc_pancake_v2_spec()
    state = evm.audit_finalized_coverage(
        spec, _CoverageRpc(head=100, logs=[]),
        ws_provider_id=_pid("ws.example"), initial_lookback_blocks=1,
    )
    assert guard.calls == 4
    assert state["state"] == "blocked" and state["last_error_kind"] == "disk_critical"
    c = evm._conn()
    try:
        assert c.execute("SELECT COUNT(*) FROM coverage_epochs").fetchone()[0] == 0
        assert c.execute(
            "SELECT verified_through_block FROM coverage_state"
        ).fetchone()[0] == 99
    finally:
        c.close()


def test_slow_audit_never_commits_finalized_proof(evm, monkeypatch):
    ticks = iter((0.0, 61.0, 61.0, 61.0))
    monkeypatch.setattr(evm, "_audit_monotonic", lambda: next(ticks))
    state = evm.audit_finalized_coverage(
        evm.bsc_pancake_v2_spec(), _CoverageRpc(head=100, logs=[]),
        ws_provider_id=_pid("ws.example"), initial_lookback_blocks=1,
    )
    assert state["state"] == "blocked"
    assert state["last_error_kind"] == "CoverageAuditTooSlow"
    assert state["verified_through_block"] == 99
    c = evm._conn()
    try:
        assert c.execute("SELECT COUNT(*) FROM coverage_epochs").fetchone()[0] == 0
    finally:
        c.close()


def test_removed_transition_is_one_way_and_controlled(evm):
    live = evm.parse_message(_notification(_log(evm))).payload
    removed = evm.parse_message(_notification(_log(evm, removed=True))).payload
    assert evm.persist(live, rpc=_CanonicalBlockRpc()) == "complete"
    assert evm.persist(removed, rpc=_CanonicalBlockRpc()) == "removed_reorg"
    with pytest.raises(evm.RawEvidenceConflict):
        evm.persist(live, rpc=_CanonicalBlockRpc())


def test_final_proof_transaction_rejects_concurrent_state_version_change(
        evm, monkeypatch):
    original = evm._reconcile_persisted_range
    changed = False

    def change_version(*args, **kwargs):
        nonlocal changed
        result = original(*args, **kwargs)
        if kwargs.get("connection") is None and not changed:
            changed = True
            c = evm._conn()
            try:
                c.execute(
                    "UPDATE coverage_state SET status='blocked',updated_at=?",
                    ((datetime.now(timezone.utc) + timedelta(seconds=1)).isoformat(),),
                )
                c.commit()
            finally:
                c.close()
        return result

    monkeypatch.setattr(evm, "_reconcile_persisted_range", change_version)
    state = evm.audit_finalized_coverage(
        evm.bsc_pancake_v2_spec(), _CoverageRpc(head=100, logs=[]),
        ws_provider_id=_pid("ws.example"), initial_lookback_blocks=1,
    )
    assert changed and state["state"] == "blocked"
    assert state["verified_through_block"] == 99


def test_provider_identity_triggers_and_orphan_quarantine_are_exact(evm):
    evm._conn().close()
    c = sqlite3.connect(str(evm.DB))
    try:
        with pytest.raises(sqlite3.IntegrityError, match="opaque"):
            c.execute("""INSERT INTO coverage_epochs(
                chain,venue,factory,topic,epoch_start_block,from_block,to_block,
                checked_at,ws_provider_id,http_provider_id,provider_independent,
                log_count,evidence_digest,segment_count,status
            ) VALUES ('bsc','orphan','0x0','0x0',0,0,0,?,
                      'provider:tenant-secret.example','provider:also-secret',
                      1,0,?,1,'open')""",
                      (datetime.now(timezone.utc).isoformat(), "a" * 64))
        c.rollback()
        c.execute("DROP TRIGGER trg_coverage_epochs_opaque_provider_insert")
        c.execute(
            "DELETE FROM bridge_meta WHERE key='provider_fingerprint_migration_v1'"
        )
        c.execute("""INSERT INTO coverage_epochs(
            chain,venue,factory,topic,epoch_start_block,from_block,to_block,
            checked_at,ws_provider_id,http_provider_id,provider_independent,
            log_count,evidence_digest,segment_count,status
        ) VALUES ('bsc','orphan','0x0','0x0',0,0,0,?,
                  'provider:tenant-secret.example','provider:also-secret',
                  1,0,?,1,'open')""",
                  (datetime.now(timezone.utc).isoformat(), "a" * 64))
        c.commit()
    finally:
        c.close()
    evm._conn().close()
    c = sqlite3.connect(str(evm.DB))
    try:
        assert c.execute(
            "SELECT COUNT(*) FROM coverage_epochs WHERE venue='orphan'"
        ).fetchone()[0] == 0
    finally:
        c.close()


def test_provider_switch_requires_persisted_reaudit_and_ack_before_live(evm):
    from src.pipeline.evm_launch_bridge import configured_stream_health

    now = datetime.now(timezone.utc).replace(microsecond=0)

    class RawSocket:
        def __init__(self): self.sent = []
        def send(self, payload): self.sent.append(payload)
        def close(self): pass
        def shutdown(self): pass

    old_spec = evm.FactorySpec(
        chain="bsc", venue="pancakeswap_v2", address=evm.PANCAKE_V2_FACTORY,
        event_kind="pair_v2", topic=evm.PAIR_CREATED_TOPIC,
        ws_urls=("wss://old-ws.example/private/token",),
        rpc_urls=("https://audit.example",),
    )
    old_runner = evm.build_runner(
        spec=old_spec, rpc=object(), socket_factory=lambda _url: RawSocket(),
    )
    old_socket = old_runner.connect()
    old_identity, old_generation = evm.active_ws_connection(old_spec)
    evm.audit_finalized_coverage(
        old_spec, _CoverageRpc(head=100, head_at=now),
        ws_provider_id=old_identity, connection_generation=old_generation,
        now=now, initial_lookback_blocks=1,
    )
    evm.stream_health.observe(
        old_spec.chain, old_spec.stream, cursor=100, event_at=now, received_at=now,
        expect_contiguous=True,
    )
    assert next(row for row in configured_stream_health(now=now + timedelta(seconds=1))
                if row["chain"] == "bsc" and row["venue"] == "pancakeswap_v2")[
                    "status"] == "live"

    new_spec = evm.FactorySpec(
        chain="bsc", venue="pancakeswap_v2", address=evm.PANCAKE_V2_FACTORY,
        event_kind="pair_v2", topic=evm.PAIR_CREATED_TOPIC,
        ws_urls=("wss://audit.example/private/new-token",),
        rpc_urls=("https://independent-two.example",),
    )
    raw = RawSocket()
    new_runner = evm.build_runner(
        spec=new_spec, rpc=object(), socket_factory=lambda _url: raw,
    )
    new_socket = new_runner.connect()
    new_identity, new_generation = evm.active_ws_connection(new_spec)
    assert new_identity == _pid("audit.example")
    evm._ACTIVE_WS_PROVIDERS.clear()  # board process has no in-memory provider map
    row = next(row for row in configured_stream_health(now=now + timedelta(seconds=1))
               if row["chain"] == "bsc" and row["venue"] == "pancakeswap_v2")
    assert row["status"] != "live" and row["coverage_verified"] is False

    evm.audit_finalized_coverage(
        new_spec, _CoverageRpc(
            head=100, provider="independent-two.example",
            head_at=now + timedelta(seconds=2),
        ),
        ws_provider_id=new_identity, connection_generation=new_generation,
        now=now + timedelta(seconds=2),
    )
    row = next(row for row in configured_stream_health(now=now + timedelta(seconds=3))
               if row["chain"] == "bsc" and row["venue"] == "pancakeswap_v2")
    assert row["transport_status"] == "disconnected" and row["status"] != "live"

    new_runner.subscribe(new_socket)
    assert new_runner.parse({
        "jsonrpc": "2.0", "id": 1, "result": "logs-new",
    }) is None
    assert new_runner.parse({
        "jsonrpc": "2.0", "id": 2, "result": "heads-new",
    }) is None
    head = new_runner.parse({
        "jsonrpc": "2.0", "method": "eth_subscription", "params": {
            "subscription": "heads-new", "result": {
                "number": "0x64",
                "timestamp": hex(int((now + timedelta(seconds=3)).timestamp())),
                "hash": "0x" + "ab" * 32,
            },
        },
    })
    new_runner.on_event(head.payload)
    evm.stream_health.observe(
        new_spec.chain, new_spec.stream, cursor=head.cursor,
        event_at=head.event_at, received_at=now + timedelta(seconds=3),
        expect_contiguous=True,
    )
    row = next(row for row in configured_stream_health(now=now + timedelta(seconds=3))
               if row["chain"] == "bsc" and row["venue"] == "pancakeswap_v2")
    assert row["status"] == "live" and row["coverage_verified"] is True
    new_socket.close()
    old_socket.close()


def test_maintenance_isolates_bindings_and_never_logs_exception_text(
        evm, monkeypatch):
    secret = "wss://tenant:key@rpc.example/private/token"
    specs = (evm.bsc_pancake_v2_spec(), evm.base_aerodrome_spec())
    identity, generation = _pid("ws.example"), "b" * 32
    evm._set_active_ws_provider(specs[0], identity, generation)
    evm.stream_health.report_worker(
        specs[0].chain, evm.coverage_stream(specs[0]), status="live",
        details={"connection_generation": generation},
    )
    attempted = []
    logged = []
    coverage_db_calls = []

    class OneRoundStop:
        waits = 0

        def wait(self, _seconds):
            self.waits += 1
            return self.waits > 1

    class UnknownGuard:
        calls = 0

        def snapshot(self):
            self.calls += 1
            return {"state": "unknown", "error_kind": "probe_failed"}

    class CaptureLogger:
        def info(self, event, **kwargs):
            logged.append((event, kwargs))

        def warning(self, event, **kwargs):
            logged.append((event, kwargs))

    guard = UnknownGuard()

    def retry(spec, _rpc):
        attempted.append(spec.venue)
        if spec == specs[0]:
            raise RuntimeError(f"provider failed {secret}")
        return {"attempted": 0, "advanced": 0, "recovered": 0, "failed": 0}

    def coverage_db_forbidden(*_args, **_kwargs):
        coverage_db_calls.append(True)
        raise AssertionError("coverage DB touched")

    monkeypatch.setattr(evm.stream_disk_guard, "GUARD", guard)
    monkeypatch.setattr(evm, "retry_open_gaps", retry)
    monkeypatch.setattr(evm, "_conn", coverage_db_forbidden)
    monkeypatch.setattr(evm, "logger", CaptureLogger())
    evm._maintenance(
        OneRoundStop(), ((specs[0], object()), (specs[1], object())),
    )

    assert guard.calls == 2
    assert attempted == [specs[0].venue, specs[1].venue]
    assert coverage_db_calls == []
    health = next(row for row in evm.stream_health.snapshot()
                  if row["stream"] == evm.coverage_stream(specs[0]))
    assert health["status"] == "degraded"
    assert health["last_error"] == "maintenance_failed"
    assert health["details"]["last_error_kind"] == "maintenance_failed"
    assert health["details"]["connection_generation"] == generation
    assert secret not in repr((logged, health))
    assert logged == [("evm_factory_maintenance_binding_failed", {
        "chain": specs[0].chain, "venue": specs[0].venue,
        "error_kind": "RuntimeError",
    })]


def test_maintenance_critical_skips_gap_coverage_and_rpc_before_generic_cas(
        evm, monkeypatch):
    spec = evm.bsc_pancake_v2_spec()
    identity, generation = _pid("ws.example"), "a" * 32
    secret = "https://tenant:key@rpc.example/private/token"
    evm._set_active_ws_provider(spec, identity, generation)
    evm.stream_health.report_worker(
        spec.chain, evm.coverage_stream(spec), status="live",
        details={"connection_generation": generation},
    )
    original_cas = evm.stream_health.report_worker_if_connection_generation
    touched = {"guard": 0, "gap": 0, "coverage_db": 0,
               "rpc": 0, "cas": 0, "direct_health": 0}

    class OneRoundStop:
        waits = 0

        def wait(self, _seconds):
            self.waits += 1
            return self.waits > 1

    class CriticalGuard:
        def snapshot(self):
            touched["guard"] += 1
            return {"state": "critical", "private": secret}

    class Rpc:
        def call(self, *_args, **_kwargs):
            touched["rpc"] += 1
            raise AssertionError(secret)

        call_with_provider = call
        call_from_provider = call

    def gap_forbidden(*_args, **_kwargs):
        touched["gap"] += 1
        raise AssertionError("gap DB touched")

    def coverage_db_forbidden(*_args, **_kwargs):
        touched["coverage_db"] += 1
        raise AssertionError("coverage DB touched")

    def direct_health_forbidden(*_args, **_kwargs):
        touched["direct_health"] += 1
        raise AssertionError("generation CAS was bypassed")

    def record_cas(*args, **kwargs):
        touched["cas"] += 1
        assert kwargs["expected_generation"] == generation
        return original_cas(*args, **kwargs)

    monkeypatch.setattr(evm.stream_disk_guard, "GUARD", CriticalGuard())
    monkeypatch.setattr(evm, "retry_open_gaps", gap_forbidden)
    monkeypatch.setattr(evm, "_conn", coverage_db_forbidden)
    monkeypatch.setattr(evm.stream_health, "report_worker", direct_health_forbidden)
    monkeypatch.setattr(
        evm.stream_health, "report_worker_if_connection_generation", record_cas,
    )
    evm._maintenance(OneRoundStop(), ((spec, Rpc()),))

    assert touched == {"guard": 1, "gap": 0, "coverage_db": 0,
                       "rpc": 0, "cas": 1, "direct_health": 0}
    row = next(item for item in evm.stream_health.snapshot()
               if item["stream"] == evm.coverage_stream(spec))
    assert row["status"] == "degraded"
    assert row["last_error"] == "disk_critical"
    assert row["details"]["last_error_kind"] == "disk_critical"
    assert row["details"]["connection_generation"] == generation
    assert secret not in repr(row)
