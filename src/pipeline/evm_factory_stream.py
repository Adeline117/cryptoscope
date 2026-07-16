"""Read-only EVM factory log stream, starting with PancakeSwap V2 on BSC."""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import threading
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable

import structlog

from src.config import DATA_DIR
from src.ops import stream_disk_guard
from src.pipeline import stream_health
from src.pipeline.stream_runner import StreamEvent, StreamRunner

logger = structlog.get_logger()

PAIR_CREATED_TOPIC = "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9"
POOL_CREATED_TOPIC = "0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118"
AERODROME_POOL_TOPIC = "0x2128d88d14c80cb081c1252a5acff7a264671bf199ce226b53788fb26065005e"
PANCAKE_V2_FACTORY = "0xca143ce32fe78f1f7019d7d551a6402fc5350c73"
PANCAKE_V2_BASE_FACTORY = "0x02a84c1b3bbd7401a5f7fa98a384ebc70bb5749e"
PANCAKE_V2_ETH_FACTORY = "0x1097053fd2ea711dad45caccc45eff7548fcb362"
PANCAKE_V3_FACTORY = "0x0bfbcf9fa4f9c56b0f40a671ad40e0805a091865"
AERODROME_FACTORY = "0x420dd381b31aef6683db6b902084cb0ffece40da"
PUBLIC_BSC_WS = ("wss://bsc-rpc.publicnode.com", "wss://bsc.drpc.org")
PUBLIC_BSC_RPC = ("https://bsc.rpc.blxrbdn.com", "https://bsc.drpc.org",
                  "https://56.rpc.thirdweb.com")
PUBLIC_BASE_WS = ("wss://base-rpc.publicnode.com", "wss://base.drpc.org")
PUBLIC_BASE_RPC = ("https://mainnet.base.org", "https://base.drpc.org")
PUBLIC_ETH_WS = ("wss://ethereum-rpc.publicnode.com", "wss://eth.drpc.org")
PUBLIC_ETH_RPC = ("https://rpc.flashbots.net", "https://eth.drpc.org",
                  "https://ethereum-rpc.publicnode.com")
MAX_BACKFILL_BLOCKS = 2_000
DB = DATA_DIR / "evm_factory_events.db"
_BROWSER_UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
               "AppleWebKit/537.36 Chrome/120 Safari/537.36 CryptoScope/1.0")
QUALIFICATION_STATES = {
    "raw_unqualified", "market_pending", "market_error", "below_threshold",
    "qualified_recorded", "ledger_orphan", "duplicate_token_existing",
    "unsupported_quote_pair", "ambiguous_target", "expired_unqualified",
    "reorg_removed", "historical_raw_only",
}
RETRYABLE_QUALIFICATION_STATES = {
    "raw_unqualified", "market_pending", "market_error", "below_threshold",
}


@dataclass(frozen=True)
class FactorySpec:
    chain: str
    venue: str
    address: str
    event_kind: str
    topic: str
    ws_urls: tuple[str, ...]
    rpc_urls: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.event_kind not in {"pair_v2", "pool_v3", "aerodrome_pool"}:
            raise ValueError("unsupported factory event kind")
        for value, size, name in ((self.address, 40, "address"),
                                  (self.topic, 64, "topic")):
            raw = value.removeprefix("0x")
            if (len(raw) != size
                    or any(ch not in "0123456789abcdef" for ch in raw)):
                raise ValueError(f"factory {name} must be lowercase canonical hex")

    @property
    def stream(self) -> str:
        suffix = "pairs" if self.event_kind == "pair_v2" else "pools"
        return f"{self.venue}_{suffix}"


def _urls(env_name: str, defaults: tuple[str, ...]) -> tuple[str, ...]:
    configured = tuple(value.strip() for value in os.getenv(env_name, "").split(",")
                       if value.strip())
    return configured or defaults


def bsc_pancake_v2_spec() -> FactorySpec:
    return FactorySpec(
        chain="bsc", venue="pancakeswap_v2", address=PANCAKE_V2_FACTORY,
        event_kind="pair_v2", topic=PAIR_CREATED_TOPIC,
        ws_urls=_urls("BSC_FACTORY_WS_URLS", PUBLIC_BSC_WS),
        rpc_urls=_urls("BSC_FACTORY_RPC_URLS", PUBLIC_BSC_RPC),
    )


def base_pancake_v2_spec() -> FactorySpec:
    return FactorySpec(
        chain="base", venue="pancakeswap_v2", address=PANCAKE_V2_BASE_FACTORY,
        event_kind="pair_v2", topic=PAIR_CREATED_TOPIC,
        ws_urls=_urls("BASE_FACTORY_WS_URLS", PUBLIC_BASE_WS),
        rpc_urls=_urls("BASE_FACTORY_RPC_URLS", PUBLIC_BASE_RPC),
    )


def ethereum_pancake_v2_spec() -> FactorySpec:
    return FactorySpec(
        chain="ethereum", venue="pancakeswap_v2", address=PANCAKE_V2_ETH_FACTORY,
        event_kind="pair_v2", topic=PAIR_CREATED_TOPIC,
        ws_urls=_urls("ETH_FACTORY_WS_URLS", PUBLIC_ETH_WS),
        rpc_urls=_urls("ETH_FACTORY_RPC_URLS", PUBLIC_ETH_RPC),
    )


def bsc_pancake_v3_spec() -> FactorySpec:
    return FactorySpec(
        chain="bsc", venue="pancakeswap_v3", address=PANCAKE_V3_FACTORY,
        event_kind="pool_v3", topic=POOL_CREATED_TOPIC,
        ws_urls=_urls("BSC_FACTORY_WS_URLS", PUBLIC_BSC_WS),
        rpc_urls=_urls("BSC_FACTORY_RPC_URLS", PUBLIC_BSC_RPC),
    )


def base_aerodrome_spec() -> FactorySpec:
    return FactorySpec(
        chain="base", venue="aerodrome", address=AERODROME_FACTORY,
        event_kind="aerodrome_pool", topic=AERODROME_POOL_TOPIC,
        ws_urls=_urls("BASE_FACTORY_WS_URLS", PUBLIC_BASE_WS),
        rpc_urls=_urls("BASE_FACTORY_RPC_URLS", PUBLIC_BASE_RPC),
    )


def configured_specs() -> tuple[FactorySpec, ...]:
    return (bsc_pancake_v2_spec(), bsc_pancake_v3_spec(), base_pancake_v2_spec(),
            base_aerodrome_spec(), ethereum_pancake_v2_spec())


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
        pair_index INTEGER NOT NULL, stable INTEGER, block_at TEXT,
        detected_at TEXT NOT NULL,
        updated_at TEXT NOT NULL, raw_payload_hash TEXT NOT NULL,
        removed INTEGER NOT NULL DEFAULT 0, evidence_state TEXT NOT NULL,
        qualification_state TEXT NOT NULL DEFAULT 'raw_unqualified',
        PRIMARY KEY(chain,transaction_hash,log_index))""")
    columns = {row[1] for row in c.execute("PRAGMA table_info(raw_pools)")}
    if "stable" not in columns:
        c.execute("ALTER TABLE raw_pools ADD COLUMN stable INTEGER")
    c.execute("CREATE INDEX IF NOT EXISTS idx_raw_pools_block "
              "ON raw_pools(chain,venue,block_number,log_index)")
    c.execute("""CREATE TABLE IF NOT EXISTS raw_v3_pools(
        chain TEXT NOT NULL, venue TEXT NOT NULL, factory TEXT NOT NULL,
        transaction_hash TEXT NOT NULL, log_index INTEGER NOT NULL,
        block_number INTEGER NOT NULL, block_hash TEXT, transaction_index INTEGER,
        token0 TEXT NOT NULL, token1 TEXT NOT NULL, pool TEXT NOT NULL,
        fee INTEGER NOT NULL, tick_spacing INTEGER NOT NULL, block_at TEXT,
        detected_at TEXT NOT NULL, updated_at TEXT NOT NULL,
        raw_payload_hash TEXT NOT NULL, removed INTEGER NOT NULL DEFAULT 0,
        evidence_state TEXT NOT NULL,
        qualification_state TEXT NOT NULL DEFAULT 'raw_unqualified',
        PRIMARY KEY(chain,transaction_hash,log_index))""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_raw_v3_pools_block "
              "ON raw_v3_pools(chain,venue,block_number,log_index)")
    for table in ("raw_pools", "raw_v3_pools"):
        table_columns = {row[1] for row in c.execute(f"PRAGMA table_info({table})")}
        for name, kind, default in (
            ("qualification_attempted_at", "TEXT", ""),
            ("qualification_retry_at", "TEXT", ""),
            ("qualification_reason", "TEXT", ""),
            ("qualification_attempts", "INTEGER", " NOT NULL DEFAULT 0"),
            ("qualified_at", "TEXT", ""),
            ("ledger_event_id", "TEXT", ""),
            ("target_token", "TEXT", ""),
        ):
            if name not in table_columns:
                c.execute(f"ALTER TABLE {table} ADD COLUMN {name} {kind}{default}")
        c.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_qualification "
                  f"ON {table}(evidence_state,qualification_state,detected_at)")
    c.execute("""CREATE TABLE IF NOT EXISTS bridge_meta(
        key TEXT PRIMARY KEY,value TEXT NOT NULL,updated_at TEXT NOT NULL)""")
    return c


def ensure_bridge_started_at(*, at: datetime | None = None) -> str:
    """Freeze the forward-test boundary and quarantine pre-deployment inventory."""
    now = (at or datetime.now(timezone.utc)).astimezone(timezone.utc).isoformat()
    c = _conn()
    try:
        c.execute("BEGIN IMMEDIATE")
        c.execute("INSERT OR IGNORE INTO bridge_meta(key,value,updated_at) "
                  "VALUES ('launch_bridge_started_at',?,?)", (now, now))
        started = c.execute(
            "SELECT value FROM bridge_meta WHERE key='launch_bridge_started_at'"
        ).fetchone()[0]
        for table in ("raw_pools", "raw_v3_pools"):
            c.execute(f"UPDATE {table} SET qualification_state='historical_raw_only', "
                      "qualification_reason='observed before forward bridge boundary' "
                      "WHERE detected_at<? AND qualification_state='raw_unqualified'",
                      (started,))
        c.commit()
        return started
    finally:
        c.close()


def qualification_batch(*, now: datetime | None = None, limit: int = 10,
                        max_age_hours: float = 24) -> list[dict]:
    """Return forward, non-reorg factory rows due for an exact-pool market check."""
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    started = ensure_bridge_started_at(at=now)
    cutoff = (now - timedelta(hours=max(0, float(max_age_hours)))).isoformat()
    c = _conn()
    try:
        for table in ("raw_pools", "raw_v3_pools"):
            c.execute(f"""UPDATE {table} SET qualification_state='expired_unqualified',
                              qualification_reason='older than 24h launch window',
                              qualification_attempted_at=?
                           WHERE detected_at>=? AND COALESCE(block_at,detected_at)<?
                             AND removed=0 AND evidence_state='complete'
                             AND qualification_state IN
                               ('raw_unqualified','market_pending','market_error','below_threshold')""",
                      (now.isoformat(), started, cutoff))
        c.commit()
        common = """chain,venue,factory,transaction_hash,log_index,block_number,
                    block_hash,transaction_index,token0,token1,pool,block_at,detected_at,
                    qualification_state,qualification_attempts,qualification_retry_at"""
        rows = c.execute(f"""SELECT 'v2' AS table_kind,{common} FROM raw_pools
            WHERE detected_at>=? AND removed=0 AND evidence_state='complete'
              AND qualification_state IN
                ('raw_unqualified','market_pending','market_error','below_threshold')
              AND (qualification_retry_at IS NULL OR qualification_retry_at<=?)
            UNION ALL SELECT 'v3' AS table_kind,{common} FROM raw_v3_pools
            WHERE detected_at>=? AND removed=0 AND evidence_state='complete'
              AND qualification_state IN
                ('raw_unqualified','market_pending','market_error','below_threshold')
              AND (qualification_retry_at IS NULL OR qualification_retry_at<=?)
            ORDER BY block_at,detected_at LIMIT ?""",
            (started, now.isoformat(), started, now.isoformat(), max(0, int(limit))),
        ).fetchall()
    finally:
        c.close()
    keys = ("table_kind", "chain", "venue", "factory", "transaction_hash",
            "log_index", "block_number", "block_hash", "transaction_index",
            "token0", "token1", "pool", "block_at", "detected_at",
            "qualification_state", "qualification_attempts", "qualification_retry_at")
    return [dict(zip(keys, row)) for row in rows]


def set_qualification(row: dict, state: str, *, reason: str | None = None,
                      target_token: str | None = None,
                      ledger_event_id: str | None = None,
                      retry_after_seconds: float | None = None,
                      at: datetime | None = None) -> bool:
    """Conditionally transition a raw row while preserving reorg terminal state."""
    if state not in QUALIFICATION_STATES:
        raise ValueError(f"unknown qualification state: {state}")
    table = {"v2": "raw_pools", "v3": "raw_v3_pools"}.get(row.get("table_kind"))
    if not table:
        raise ValueError("qualification row has unknown table_kind")
    now = (at or datetime.now(timezone.utc)).astimezone(timezone.utc)
    retry_at = ((now + timedelta(seconds=max(0, float(retry_after_seconds)))).isoformat()
                if retry_after_seconds is not None else None)
    qualified_at = now.isoformat() if state == "qualified_recorded" else None
    c = _conn()
    try:
        changed = c.execute(f"""UPDATE {table} SET qualification_state=?,
                    qualification_attempted_at=?,qualification_retry_at=?,
                    qualification_reason=?,qualification_attempts=qualification_attempts+1,
                    qualified_at=COALESCE(?,qualified_at),
                    ledger_event_id=COALESCE(?,ledger_event_id),
                    target_token=COALESCE(?,target_token)
                 WHERE chain=? AND transaction_hash=? AND log_index=? AND removed=0
                   AND qualification_state IN
                     ('raw_unqualified','market_pending','market_error','below_threshold')""",
            (state, now.isoformat(), retry_at, str(reason)[:240] if reason else None,
             qualified_at, ledger_event_id, target_token.lower() if target_token else None,
             row["chain"], row["transaction_hash"].lower(), int(row["log_index"])),
        ).rowcount
        c.commit()
        return bool(changed)
    finally:
        c.close()


def qualification_summary(
    *, ledger_readback: Callable[[str, str, str], bool] | None = None,
) -> dict:
    """Report coverage without trusting cross-database IDs by presence alone.

    The stream database cannot enforce a foreign key into the opportunity
    ledger.  Historical rows marked ``qualified_recorded`` therefore count as
    recorded only when an injected reader confirms the exact ledger ID, chain,
    and target token.  Failed read-backs are exposed as orphans; an unavailable
    reader is reported separately instead of silently accepting the claim.
    """
    c = _conn()
    try:
        rows = c.execute("""SELECT evidence_state,qualification_state,COUNT(*) FROM raw_pools
                            GROUP BY evidence_state,qualification_state
                            UNION ALL
                            SELECT evidence_state,qualification_state,COUNT(*) FROM raw_v3_pools
                            GROUP BY evidence_state,qualification_state""").fetchall()
        started = c.execute(
            "SELECT value FROM bridge_meta WHERE key='launch_bridge_started_at'"
        ).fetchone()
        marked_recorded = c.execute("""SELECT ledger_event_id,chain,target_token
            FROM raw_pools WHERE qualification_state='qualified_recorded'
            UNION ALL SELECT ledger_event_id,chain,target_token
            FROM raw_v3_pools WHERE qualification_state='qualified_recorded'""").fetchall()
        quarantined_ids = c.execute("""SELECT ledger_event_id FROM raw_pools
            WHERE qualification_state='ledger_orphan'
            UNION ALL SELECT ledger_event_id FROM raw_v3_pools
            WHERE qualification_state='ledger_orphan'""").fetchall()
    finally:
        c.close()
    evidence, raw_qualification = {}, {}
    for evidence_state, qualification_state, count in rows:
        evidence[evidence_state] = evidence.get(evidence_state, 0) + count
        raw_qualification[qualification_state] = (
            raw_qualification.get(qualification_state, 0) + count)

    traceable_ids: set[str] = set()
    orphan_ids = {str(row[0]) for row in quarantined_ids if row[0]}
    traceable_rows = 0
    orphan_rows = 0
    missing_id_rows = 0
    missing_identity_rows = 0
    readback_unavailable_rows = 0
    readback_error_rows = 0
    for ledger_id, chain, target_token in marked_recorded:
        if not ledger_id:
            missing_id_rows += 1
            orphan_rows += 1
            continue
        if not chain or not target_token:
            missing_identity_rows += 1
            orphan_rows += 1
            orphan_ids.add(str(ledger_id))
            continue
        if ledger_readback is None:
            readback_unavailable_rows += 1
            continue
        try:
            valid = bool(ledger_readback(
                str(ledger_id), str(chain), str(target_token)))
        except Exception:
            readback_error_rows += 1
            continue
        if valid:
            traceable_rows += 1
            traceable_ids.add(str(ledger_id))
        else:
            orphan_rows += 1
            orphan_ids.add(str(ledger_id))

    qualification = dict(raw_qualification)
    if marked_recorded or "qualified_recorded" in qualification:
        qualification["qualified_recorded"] = len(traceable_ids)
    quarantined_state_rows = int(raw_qualification.get("ledger_orphan") or 0)
    if orphan_rows or quarantined_state_rows:
        qualification["ledger_orphan"] = orphan_rows + quarantined_state_rows
    unavailable_rows = readback_unavailable_rows + readback_error_rows
    if unavailable_rows:
        qualification["ledger_readback_unavailable"] = unavailable_rows
    return {"raw_total": sum(evidence.values()), "evidence": evidence,
            "qualification": qualification,
            "raw_qualification_states": raw_qualification,
            "traceability": {
                "state": ("unavailable" if unavailable_rows else
                          "ok" if not orphan_rows and not quarantined_state_rows else
                          "partial"),
                "raw_marked_recorded_rows": len(marked_recorded),
                "traceable_rows": traceable_rows,
                "traceable_unique_ledger_events": len(traceable_ids),
                "orphan_rows": orphan_rows + quarantined_state_rows,
                "orphan_unique_ledger_ids": len(orphan_ids),
                "missing_ledger_id_rows": missing_id_rows,
                "missing_identity_rows": missing_identity_rows,
                "quarantined_state_rows": quarantined_state_rows,
                "readback_unavailable_rows": readback_unavailable_rows,
                "readback_error_rows": readback_error_rows,
            },
            "bridge_started_at": started[0] if started else None}


def subscribe_requests(spec: FactorySpec) -> list[dict]:
    return [
        {"jsonrpc": "2.0", "id": 1, "method": "eth_subscribe", "params": [
            "logs", {"address": spec.address, "topics": [spec.topic]},
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


def _uint_word(value: object, field: str) -> int:
    raw = str(value).lower().removeprefix("0x")
    if len(raw) != 64 or any(ch not in "0123456789abcdef" for ch in raw):
        raise ValueError(f"EVM factory log has invalid {field}")
    return int(raw, 16)


def _int24_word(value: str, field: str) -> int:
    number = _uint_word(value, field)
    if number >= 1 << 255:
        number -= 1 << 256
    if not -(1 << 23) <= number < (1 << 23):
        raise ValueError(f"EVM factory log has invalid {field}")
    return number


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
    expected_topics = 3 if spec.event_kind == "pair_v2" else 4
    if (str(result.get("address", "")).lower() != spec.address
            or len(topics) != expected_topics
            or str(topics[0]).lower() != spec.topic):
        return None
    data = str(result.get("data", "")).lower().removeprefix("0x")
    if len(data) != 128:
        raise ValueError("factory creation data must contain two ABI words")
    removed = result.get("removed", False)
    if not isinstance(removed, bool):
        raise ValueError("EVM factory log removed flag must be boolean")
    token0 = _word_address(topics[1], "token0")
    token1 = _word_address(topics[2], "token1")
    pool_word = data[64:] if spec.event_kind == "pool_v3" else data[:64]
    pool = _word_address(pool_word, "pool")
    zero = "0x" + "0" * 40
    if token0 == zero or token1 == zero or pool == zero:
        raise ValueError("PairCreated addresses must be non-zero")
    if int(token0, 16) >= int(token1, 16):
        raise ValueError("PairCreated tokens must be distinct and sorted")
    payload = {
        "kind": spec.event_kind, "chain": spec.chain, "venue": spec.venue,
        "factory": spec.address,
        "transaction_hash": _hash32(result.get("transactionHash"), "transaction hash"),
        "log_index": _hex_int(result.get("logIndex"), "log index"),
        "block_number": _hex_int(result.get("blockNumber"), "block number"),
        "block_hash": _hash32(result.get("blockHash"), "block hash"),
        "transaction_index": _hex_int(result.get("transactionIndex"), "transaction index"),
        "token0": token0, "token1": token1, "pool": pool,
        "removed": removed,
    }
    if spec.event_kind == "pair_v2":
        pair_index = _uint_word(data[64:], "pair index")
        if pair_index <= 0:
            raise ValueError("PairCreated index must be positive")
        payload["pair_index"] = pair_index
    elif spec.event_kind == "pool_v3":
        fee = _uint_word(topics[3], "fee")
        tick_spacing = _int24_word(data[:64], "tick spacing")
        if not 0 < fee <= 1_000_000 or tick_spacing <= 0:
            raise ValueError("PoolCreated fee and tick spacing must be positive")
        payload["fee"] = fee
        payload["tick_spacing"] = tick_spacing
    elif spec.event_kind == "aerodrome_pool":
        stable = _uint_word(topics[3], "stable")
        pool_index = _uint_word(data[64:], "pool index")
        if stable not in {0, 1} or pool_index <= 0:
            raise ValueError("Aerodrome stable flag or pool index is invalid")
        payload["stable"] = bool(stable)
        payload["pair_index"] = pool_index
    else:
        raise ValueError(f"unsupported factory event kind: {spec.event_kind}")
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
            except urllib.error.HTTPError as exc:
                # HTTPError owns the rejected response. Closing it before trying the
                # next RPC prevents a failed gap retry from leaking CLOSE_WAIT.
                exc.close()
                last_error = exc
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
    if (not isinstance(payload, dict)
            or payload.get("kind") not in {"pair_v2", "pool_v3", "aerodrome_pool"}):
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
    ledger_event_id = None
    try:
        common = (payload["chain"], payload["venue"], payload["factory"],
                  payload["transaction_hash"].lower(), payload["log_index"],
                  payload["block_number"], payload.get("block_hash"),
                  payload["transaction_index"], payload["token0"], payload["token1"],
                  payload["pool"])
        if payload["kind"] in {"pair_v2", "aerodrome_pool"}:
            c.execute("""INSERT INTO raw_pools(
                chain,venue,factory,transaction_hash,log_index,block_number,block_hash,
                transaction_index,token0,token1,pool,pair_index,stable,block_at,
                detected_at,updated_at,raw_payload_hash,removed,evidence_state,
                qualification_state
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'raw_unqualified')
            ON CONFLICT(chain,transaction_hash,log_index) DO UPDATE SET
              block_number=excluded.block_number,block_hash=excluded.block_hash,
              block_at=COALESCE(raw_pools.block_at,excluded.block_at),
              stable=excluded.stable,
              updated_at=excluded.updated_at,raw_payload_hash=excluded.raw_payload_hash,
              removed=excluded.removed,evidence_state=excluded.evidence_state,
              qualification_state=CASE WHEN excluded.removed=1 THEN 'reorg_removed'
                                       ELSE raw_pools.qualification_state END,
              qualification_reason=CASE WHEN excluded.removed=1 THEN 'factory log removed by reorg'
                                        ELSE raw_pools.qualification_reason END""",
                      (*common, payload["pair_index"],
                       int(payload["stable"]) if "stable" in payload else None,
                       block_at, now, now,
                       _hash(payload), int(payload["removed"]), state))
            if payload["removed"]:
                c.execute("""UPDATE raw_pools SET qualification_state='reorg_removed',
                              qualification_reason='factory log removed by reorg'
                           WHERE chain=? AND transaction_hash=? AND log_index=?""",
                          (payload["chain"], payload["transaction_hash"].lower(),
                           payload["log_index"]))
                row = c.execute(
                    "SELECT ledger_event_id FROM raw_pools WHERE chain=? "
                    "AND transaction_hash=? AND log_index=?",
                    (payload["chain"], payload["transaction_hash"].lower(),
                     payload["log_index"]),
                ).fetchone()
                ledger_event_id = row[0] if row else None
        else:
            c.execute("""INSERT INTO raw_v3_pools(
                chain,venue,factory,transaction_hash,log_index,block_number,block_hash,
                transaction_index,token0,token1,pool,fee,tick_spacing,block_at,
                detected_at,updated_at,raw_payload_hash,removed,evidence_state,
                qualification_state
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'raw_unqualified')
            ON CONFLICT(chain,transaction_hash,log_index) DO UPDATE SET
              block_number=excluded.block_number,block_hash=excluded.block_hash,
              block_at=COALESCE(raw_v3_pools.block_at,excluded.block_at),
              updated_at=excluded.updated_at,raw_payload_hash=excluded.raw_payload_hash,
              removed=excluded.removed,evidence_state=excluded.evidence_state,
              qualification_state=CASE WHEN excluded.removed=1 THEN 'reorg_removed'
                                       ELSE raw_v3_pools.qualification_state END,
              qualification_reason=CASE WHEN excluded.removed=1 THEN 'factory log removed by reorg'
                                        ELSE raw_v3_pools.qualification_reason END""",
                      (*common, payload["fee"], payload["tick_spacing"], block_at,
                       now, now, _hash(payload), int(payload["removed"]), state))
            if payload["removed"]:
                c.execute("""UPDATE raw_v3_pools SET qualification_state='reorg_removed',
                              qualification_reason='factory log removed by reorg'
                           WHERE chain=? AND transaction_hash=? AND log_index=?""",
                          (payload["chain"], payload["transaction_hash"].lower(),
                           payload["log_index"]))
                row = c.execute(
                    "SELECT ledger_event_id FROM raw_v3_pools WHERE chain=? "
                    "AND transaction_hash=? AND log_index=?",
                    (payload["chain"], payload["transaction_hash"].lower(),
                     payload["log_index"]),
                ).fetchone()
                ledger_event_id = row[0] if row else None
        c.commit()
    finally:
        c.close()
    if payload["removed"] and ledger_event_id:
        from src.pipeline.opportunity_ledger import invalidate
        invalidate(ledger_event_id, state="reorg_removed",
                   reason=(f"{payload['chain']} factory log removed by reorg: "
                           f"{payload['transaction_hash']}:{payload['log_index']}"))


def _persist_stream_event(payload: object, *, spec: FactorySpec, rpc: JsonRpc) -> None:
    """Block before the writer so StreamRunner cannot advance its health cursor."""
    stream_disk_guard.GUARD.require_evidence_write(spec.chain)
    persist(payload, rpc=rpc)


def _backfill_blocks(start: int, end: int, *, spec: FactorySpec, rpc: JsonRpc) -> None:
    if end < start:
        return
    if end - start + 1 > MAX_BACKFILL_BLOCKS:
        raise ValueError("EVM backfill range exceeds bounded block budget")
    # Gap repair is evidence persistence too.  Refuse before the first RPC so a
    # full volume cannot turn recovery into more in-memory data or DB writes.
    stream_disk_guard.GUARD.require_evidence_write(spec.chain)
    logs = rpc.call("eth_getLogs", [{
        "address": spec.address, "topics": [spec.topic],
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
            _persist_stream_event(event.payload, spec=spec, rpc=rpc)


def backfill_blocks(start: int, end: int, *, spec: FactorySpec, rpc: JsonRpc) -> bool:
    """Compatibility wrapper for immediate recovery; maintenance keeps the error."""
    try:
        _backfill_blocks(start, end, spec=spec, rpc=rpc)
        return True
    except Exception as exc:
        logger.warning("evm_factory_backfill_failed", chain=spec.chain,
                       start=start, end=end, error=str(exc)[:120])
        return False


def retry_open_gaps(spec: FactorySpec, rpc: JsonRpc, *, limit: int = 10) -> dict:
    gaps = stream_health.open_gaps(spec.chain, spec.stream, limit=limit)
    advanced = recovered = failed = 0
    for gap in gaps:
        start = int(gap["from_cursor"])
        end = min(int(gap["to_cursor"]), start + MAX_BACKFILL_BLOCKS - 1)
        try:
            _backfill_blocks(start, end, spec=spec, rpc=rpc)
            state = stream_health.advance_gap(gap["id"], end, details={
                "backfilled": True, "retry": True,
                "from": start, "to": end,
            })
            if state == "resolved":
                recovered += 1
            elif state == "advanced":
                advanced += 1
        except Exception as exc:
            failed += 1
            deferred = stream_health.defer_gap(gap["id"], str(exc))
            logger.warning(
                "evm_factory_gap_retry_deferred",
                chain=spec.chain, stream=spec.stream,
                start=start, end=end, error=str(exc)[:120],
                next_retry_at=(deferred or {}).get("next_retry_at"),
            )
    return {"attempted": len(gaps), "advanced": advanced,
            "recovered": recovered, "failed": failed}


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

    def shutdown(self):
        shutdown = getattr(self.socket, "shutdown", None)
        if callable(shutdown):
            shutdown()


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
        on_event=lambda payload: _persist_stream_event(payload, spec=spec, rpc=rpc),
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
