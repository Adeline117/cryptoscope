"""Read-only EVM factory log stream, starting with PancakeSwap V2 on BSC."""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import threading
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable

import structlog

from src.config import DATA_DIR
from src.pipeline import stream_health
from src.pipeline.stream_runner import StreamEvent, StreamRunner

logger = structlog.get_logger()

PAIR_CREATED_TOPIC = "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9"
PANCAKE_V2_FACTORY = "0xca143ce32fe78f1f7019d7d551a6402fc5350c73"
PANCAKE_V2_BASE_FACTORY = "0x02a84c1b3bbd7401a5f7fa98a384ebc70bb5749e"
PANCAKE_V2_ETH_FACTORY = "0x1097053fd2ea711dad45caccc45eff7548fcb362"
PUBLIC_BSC_WS = ("wss://bsc-rpc.publicnode.com", "wss://bsc.drpc.org")
PUBLIC_BSC_RPC = ("https://bsc.rpc.blxrbdn.com", "https://bsc.drpc.org")
PUBLIC_BASE_WS = ("wss://base-rpc.publicnode.com", "wss://base.drpc.org")
PUBLIC_BASE_RPC = ("https://mainnet.base.org", "https://base.drpc.org")
PUBLIC_ETH_WS = ("wss://ethereum-rpc.publicnode.com", "wss://eth.drpc.org")
PUBLIC_ETH_RPC = ("https://rpc.flashbots.net", "https://eth.drpc.org",
                  "https://ethereum-rpc.publicnode.com")
MAX_BACKFILL_BLOCKS = 2_000
DB = DATA_DIR / "evm_factory_events.db"
_BROWSER_UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
               "AppleWebKit/537.36 Chrome/120 Safari/537.36 CryptoScope/1.0")


@dataclass(frozen=True)
class FactorySpec:
    chain: str
    venue: str
    address: str
    ws_urls: tuple[str, ...]
    rpc_urls: tuple[str, ...]

    @property
    def stream(self) -> str:
        return f"{self.venue}_pairs"


def _urls(env_name: str, defaults: tuple[str, ...]) -> tuple[str, ...]:
    configured = tuple(value.strip() for value in os.getenv(env_name, "").split(",")
                       if value.strip())
    return configured or defaults


def bsc_pancake_v2_spec() -> FactorySpec:
    return FactorySpec(
        chain="bsc", venue="pancakeswap_v2", address=PANCAKE_V2_FACTORY,
        ws_urls=_urls("BSC_FACTORY_WS_URLS", PUBLIC_BSC_WS),
        rpc_urls=_urls("BSC_FACTORY_RPC_URLS", PUBLIC_BSC_RPC),
    )


def base_pancake_v2_spec() -> FactorySpec:
    return FactorySpec(
        chain="base", venue="pancakeswap_v2", address=PANCAKE_V2_BASE_FACTORY,
        ws_urls=_urls("BASE_FACTORY_WS_URLS", PUBLIC_BASE_WS),
        rpc_urls=_urls("BASE_FACTORY_RPC_URLS", PUBLIC_BASE_RPC),
    )


def ethereum_pancake_v2_spec() -> FactorySpec:
    return FactorySpec(
        chain="ethereum", venue="pancakeswap_v2", address=PANCAKE_V2_ETH_FACTORY,
        ws_urls=_urls("ETH_FACTORY_WS_URLS", PUBLIC_ETH_WS),
        rpc_urls=_urls("ETH_FACTORY_RPC_URLS", PUBLIC_ETH_RPC),
    )


def configured_specs() -> tuple[FactorySpec, ...]:
    return (bsc_pancake_v2_spec(), base_pancake_v2_spec(),
            ethereum_pancake_v2_spec())


def _conn() -> sqlite3.Connection:
    DB.parent.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(str(DB), timeout=10)
    c.execute("PRAGMA busy_timeout=8000")
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA synchronous=NORMAL")
    c.execute("""CREATE TABLE IF NOT EXISTS raw_pools(
        chain TEXT NOT NULL, venue TEXT NOT NULL, factory TEXT NOT NULL,
        transaction_hash TEXT NOT NULL, log_index INTEGER NOT NULL,
        block_number INTEGER NOT NULL, block_hash TEXT, transaction_index INTEGER,
        token0 TEXT NOT NULL, token1 TEXT NOT NULL, pool TEXT NOT NULL,
        pair_index INTEGER NOT NULL, block_at TEXT, detected_at TEXT NOT NULL,
        updated_at TEXT NOT NULL, raw_payload_hash TEXT NOT NULL,
        removed INTEGER NOT NULL DEFAULT 0, evidence_state TEXT NOT NULL,
        qualification_state TEXT NOT NULL DEFAULT 'raw_unqualified',
        PRIMARY KEY(chain,transaction_hash,log_index))""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_raw_pools_block "
              "ON raw_pools(chain,venue,block_number,log_index)")
    return c


def subscribe_requests(spec: FactorySpec) -> list[dict]:
    return [
        {"jsonrpc": "2.0", "id": 1, "method": "eth_subscribe", "params": [
            "logs", {"address": spec.address, "topics": [PAIR_CREATED_TOPIC]},
        ]},
        {"jsonrpc": "2.0", "id": 2, "method": "eth_subscribe",
         "params": ["newHeads"]},
    ]


def _hex_int(value: object, field: str) -> int:
    try:
        return int(str(value), 16)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"EVM factory log has invalid {field}") from exc


def _word_address(value: str, field: str) -> str:
    raw = str(value).lower().removeprefix("0x")
    if len(raw) != 64 or any(ch not in "0123456789abcdef" for ch in raw):
        raise ValueError(f"EVM factory log has invalid {field}")
    return "0x" + raw[-40:]


def _hash32(value: object, field: str) -> str:
    raw = str(value).lower().removeprefix("0x")
    if len(raw) != 64 or any(ch not in "0123456789abcdef" for ch in raw):
        raise ValueError(f"EVM factory log has invalid {field}")
    return "0x" + raw


def parse_message(raw: object, *, spec: FactorySpec | None = None) -> StreamEvent | None:
    spec = spec or bsc_pancake_v2_spec()
    msg = json.loads(raw) if isinstance(raw, (str, bytes, bytearray)) else raw
    if not isinstance(msg, dict):
        raise ValueError("EVM websocket message must be an object")
    if msg.get("error"):
        raise PermissionError(f"EVM subscription rejected: {msg['error']}")
    if msg.get("id") in {1, 2} and "result" in msg:
        return None
    if msg.get("method") != "eth_subscription":
        return None
    result = (msg.get("params") or {}).get("result") or {}
    if "number" in result and "topics" not in result:
        block = _hex_int(result.get("number"), "block number")
        timestamp = result.get("timestamp")
        event_at = (datetime.fromtimestamp(_hex_int(timestamp, "timestamp"), timezone.utc)
                    if timestamp is not None else None)
        return StreamEvent({"kind": "head", "block_number": block},
                           cursor=block, event_at=event_at)
    topics = result.get("topics") or []
    if (str(result.get("address", "")).lower() != spec.address
            or len(topics) != 3 or str(topics[0]).lower() != PAIR_CREATED_TOPIC):
        return None
    data = str(result.get("data", "")).lower().removeprefix("0x")
    if len(data) != 128:
        raise ValueError("PancakeSwap PairCreated data must contain two ABI words")
    removed = result.get("removed", False)
    if not isinstance(removed, bool):
        raise ValueError("EVM factory log removed flag must be boolean")
    token0 = _word_address(topics[1], "token0")
    token1 = _word_address(topics[2], "token1")
    pool = _word_address(data[:64], "pair")
    pair_index = int(data[64:], 16)
    zero = "0x" + "0" * 40
    if token0 == zero or token1 == zero or pool == zero:
        raise ValueError("PairCreated addresses must be non-zero")
    if int(token0, 16) >= int(token1, 16):
        raise ValueError("PairCreated tokens must be distinct and sorted")
    if pair_index <= 0:
        raise ValueError("PairCreated index must be positive")
    payload = {
        "kind": "pool", "chain": spec.chain, "venue": spec.venue,
        "factory": spec.address,
        "transaction_hash": _hash32(result.get("transactionHash"), "transaction hash"),
        "log_index": _hex_int(result.get("logIndex"), "log index"),
        "block_number": _hex_int(result.get("blockNumber"), "block number"),
        "block_hash": _hash32(result.get("blockHash"), "block hash"),
        "transaction_index": _hex_int(result.get("transactionIndex"), "transaction index"),
        "token0": token0, "token1": token1, "pool": pool,
        "pair_index": pair_index, "removed": removed,
    }
    return StreamEvent(payload)


class JsonRpc:
    def __init__(self, endpoints: tuple[str, ...], *, timeout: float = 15):
        if not endpoints:
            raise ValueError("at least one EVM RPC endpoint is required")
        self.endpoints = endpoints
        self.timeout = timeout
        self._index = 0

    def call(self, method: str, params: list) -> object:
        last_error: object = "no endpoint attempted"
        for offset in range(len(self.endpoints)):
            index = (self._index + offset) % len(self.endpoints)
            endpoint = self.endpoints[index]
            try:
                body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method,
                                   "params": params}).encode()
                request = urllib.request.Request(
                    endpoint, data=body,
                    headers={"Content-Type": "application/json",
                             "User-Agent": _BROWSER_UA})
                with urllib.request.urlopen(request, timeout=self.timeout) as response:
                    result = json.load(response)
                if result.get("error"):
                    raise RuntimeError(result["error"])
                self._index = index
                return result.get("result")
            except Exception as exc:
                last_error = exc
        raise RuntimeError(f"all EVM RPC endpoints failed for {method}: {last_error}")


def _block_at(rpc: JsonRpc, block_number: int) -> str:
    block = rpc.call("eth_getBlockByNumber", [hex(block_number), False])
    if not isinstance(block, dict) or block.get("timestamp") is None:
        raise RuntimeError("EVM block timestamp is unavailable")
    if block.get("number") is not None and int(block["number"], 16) != block_number:
        raise RuntimeError("EVM RPC returned the wrong block")
    return datetime.fromtimestamp(int(block["timestamp"], 16), timezone.utc).isoformat()


def _hash(payload: dict) -> str:
    return hashlib.sha256(json.dumps(
        payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def persist(payload: object, *, rpc: JsonRpc | None = None) -> None:
    if not isinstance(payload, dict) or payload.get("kind") != "pool":
        return
    now = datetime.now(timezone.utc).isoformat()
    block_at = None
    state = "removed_reorg" if payload["removed"] else "timestamp_unavailable"
    if not payload["removed"] and rpc is not None:
        try:
            block_at = _block_at(rpc, payload["block_number"])
            state = "complete"
        except Exception as exc:
            logger.warning("evm_pool_timestamp_failed", chain=payload["chain"],
                           block=payload["block_number"], error=str(exc)[:120])
    c = _conn()
    try:
        c.execute("""INSERT INTO raw_pools(
            chain,venue,factory,transaction_hash,log_index,block_number,block_hash,
            transaction_index,token0,token1,pool,pair_index,block_at,detected_at,
            updated_at,raw_payload_hash,removed,evidence_state,qualification_state
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'raw_unqualified')
        ON CONFLICT(chain,transaction_hash,log_index) DO UPDATE SET
          block_number=excluded.block_number,block_hash=excluded.block_hash,
          block_at=COALESCE(raw_pools.block_at,excluded.block_at),
          updated_at=excluded.updated_at,raw_payload_hash=excluded.raw_payload_hash,
          removed=excluded.removed,evidence_state=excluded.evidence_state""",
                  (payload["chain"], payload["venue"], payload["factory"],
                   payload["transaction_hash"].lower(), payload["log_index"],
                   payload["block_number"], payload.get("block_hash"),
                   payload["transaction_index"], payload["token0"], payload["token1"],
                   payload["pool"], payload["pair_index"], block_at, now, now,
                   _hash(payload), int(payload["removed"]), state))
        c.commit()
    finally:
        c.close()


def backfill_blocks(start: int, end: int, *, spec: FactorySpec, rpc: JsonRpc) -> bool:
    if end < start:
        return True
    if end - start + 1 > MAX_BACKFILL_BLOCKS:
        return False
    try:
        logs = rpc.call("eth_getLogs", [{
            "address": spec.address, "topics": [PAIR_CREATED_TOPIC],
            "fromBlock": hex(start), "toBlock": hex(end),
        }])
        if not isinstance(logs, list):
            raise RuntimeError("eth_getLogs returned a non-list result")
        for item in logs:
            event = parse_message({"method": "eth_subscription",
                                   "params": {"result": item}}, spec=spec)
            if event:
                if not start <= event.payload["block_number"] <= end:
                    raise RuntimeError("eth_getLogs returned an event outside requested range")
                persist(event.payload, rpc=rpc)
        return True
    except Exception as exc:
        logger.warning("evm_factory_backfill_failed", chain=spec.chain,
                       start=start, end=end, error=str(exc)[:120])
        return False


def retry_open_gaps(spec: FactorySpec, rpc: JsonRpc, *, limit: int = 10) -> dict:
    gaps = stream_health.open_gaps(spec.chain, spec.stream, limit=limit)
    recovered = failed = 0
    for gap in gaps:
        if backfill_blocks(gap["from_cursor"], gap["to_cursor"], spec=spec, rpc=rpc):
            if stream_health.resolve_gap(gap["id"], details={
                    "backfilled": True, "retry": True,
                    "from": gap["from_cursor"], "to": gap["to_cursor"]}):
                recovered += 1
        else:
            failed += 1
    return {"attempted": len(gaps), "recovered": recovered, "failed": failed}


class _EvmSocket:
    def __init__(self, socket):
        self.socket = socket

    def recv(self):
        return self.socket.recv()

    def ping(self):
        self.socket.ping()

    def send_json(self, payload: dict):
        self.socket.send(json.dumps(payload, separators=(",", ":")))

    def close(self):
        self.socket.close()


def build_runner(*, spec: FactorySpec | None = None, rpc: JsonRpc | None = None,
                 socket_factory: Callable[[str], object] | None = None) -> StreamRunner:
    spec = spec or bsc_pancake_v2_spec()
    rpc = rpc or JsonRpc(spec.rpc_urls)
    if socket_factory is None:
        from websocket import create_connection

        socket_factory = lambda endpoint: create_connection(endpoint, timeout=10)

    def connect():
        last_error: object = "no endpoint attempted"
        for endpoint in spec.ws_urls:
            try:
                return _EvmSocket(socket_factory(endpoint))
            except Exception as exc:
                last_error = exc
        raise ConnectionError(f"all {spec.chain} factory websockets failed: {last_error}")

    def subscribe(ws):
        for request in subscribe_requests(spec):
            ws.send_json(request)

    return StreamRunner(
        source=spec.chain, stream=spec.stream, connect=connect, subscribe=subscribe,
        parse=lambda raw: parse_message(raw, spec=spec),
        on_event=lambda payload: persist(payload, rpc=rpc),
        heartbeat_seconds=30, health_interval_seconds=1, expect_contiguous=True,
        backfill=lambda start, end: backfill_blocks(start, end, spec=spec, rpc=rpc),
    )


def _maintenance(stop: threading.Event,
                 bindings: tuple[tuple[FactorySpec, JsonRpc], ...]) -> None:
    while not stop.wait(60):
        for spec, rpc in bindings:
            result = retry_open_gaps(spec, rpc)
            if result["attempted"]:
                logger.info("evm_factory_gap_retry", chain=spec.chain, **result)


def main() -> None:
    from dotenv import load_dotenv
    from src.config import PROJECT_ROOT

    load_dotenv(PROJECT_ROOT / ".env")
    _conn().close()
    bindings = tuple((spec, JsonRpc(spec.rpc_urls)) for spec in configured_specs())
    for spec, rpc in bindings:
        initial = retry_open_gaps(spec, rpc)
        if initial["attempted"]:
            logger.info("evm_factory_initial_gap_retry", chain=spec.chain, **initial)
    stop = threading.Event()
    worker = threading.Thread(target=_maintenance, args=(stop, bindings), daemon=True)
    worker.start()
    runners = [build_runner(spec=spec, rpc=rpc) for spec, rpc in bindings]
    children = [threading.Thread(target=runner.run_forever, args=(stop,), daemon=True,
                                 name=f"factory-{runner.source}")
                for runner in runners[1:]]
    for child in children:
        child.start()
    try:
        runners[0].run_forever(stop)
    finally:
        stop.set()
        worker.join(timeout=2)
        for child in children:
            child.join(timeout=2)


if __name__ == "__main__":
    main()
