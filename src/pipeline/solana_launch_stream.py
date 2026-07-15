"""Standard Solana stream for raw Pump.fun launch evidence.

This intentionally uses the standard ``logsSubscribe`` API instead of Helius'
paid ``transactionSubscribe`` extension. A launch remains raw evidence until
the corresponding transaction proves both its mint and creator signer.
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import threading
import urllib.request
from datetime import datetime, timezone
from typing import Callable

import structlog

from src.config import DATA_DIR
from src.pipeline import stream_health
from src.pipeline.stream_runner import StreamEvent, StreamRunner

logger = structlog.get_logger()

PUMP_FUN_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
PUBLIC_SOLANA_RPC = "https://api.mainnet-beta.solana.com"
PUBLIC_SOLANA_WS = "wss://api.mainnet-beta.solana.com/"
MAX_BACKFILL_SLOTS = 16
DB = DATA_DIR / "solana_launch_events.db"


def _conn() -> sqlite3.Connection:
    DB.parent.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(str(DB), timeout=10)
    c.execute("PRAGMA busy_timeout=8000")
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA synchronous=NORMAL")
    c.execute("""CREATE TABLE IF NOT EXISTS raw_launches(
        signature TEXT PRIMARY KEY, slot INTEGER NOT NULL, transaction_index INTEGER,
        program TEXT NOT NULL, event_type TEXT NOT NULL, creator TEXT, mint TEXT,
        detected_at TEXT NOT NULL, hydrated_at TEXT, raw_payload_hash TEXT NOT NULL,
        hydration_payload_hash TEXT, logs TEXT NOT NULL,
        evidence_state TEXT NOT NULL DEFAULT 'raw_only', hydration_error TEXT,
        qualification_state TEXT NOT NULL DEFAULT 'raw_unqualified')""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_solana_launch_slot ON raw_launches(slot)")
    return c


def subscribe_requests() -> list[dict]:
    """Subscribe to Pump logs plus slots used to detect reconnect gaps."""
    return [
        {"jsonrpc": "2.0", "id": 1, "method": "logsSubscribe", "params": [
            {"mentions": [PUMP_FUN_PROGRAM]}, {"commitment": "confirmed"},
        ]},
        {"jsonrpc": "2.0", "id": 2, "method": "slotSubscribe"},
    ]


def _creation_type(logs: list[str]) -> str | None:
    for name in ("CreateV2", "Create"):
        if any(line.strip() == f"Program log: Instruction: {name}" for line in logs):
            return name
    return None


def parse_message(raw: object) -> StreamEvent | None:
    msg = json.loads(raw) if isinstance(raw, (str, bytes, bytearray)) else raw
    if not isinstance(msg, dict):
        raise ValueError("Solana websocket message must be an object")
    if msg.get("error"):
        raise PermissionError(f"Solana subscription rejected: {msg['error']}")
    if msg.get("id") in {1, 2} and "result" in msg:
        return None
    if msg.get("method") == "slotNotification":
        result = (msg.get("params") or {}).get("result") or {}
        if result.get("slot") is None:
            raise ValueError("Solana slot notification lacks slot")
        slot = int(result["slot"])
        return StreamEvent({"kind": "slot", "slot": slot}, cursor=slot)
    if msg.get("method") != "logsNotification":
        return None
    result = (msg.get("params") or {}).get("result") or {}
    value = result.get("value") or {}
    context = result.get("context") or {}
    signature, slot = value.get("signature"), context.get("slot")
    if not signature or slot is None:
        raise ValueError("Solana log notification lacks signature or slot")
    if value.get("err") is not None:
        return None
    logs = [str(line) for line in value.get("logs") or []]
    create_name = _creation_type(logs)
    if not create_name:
        return None
    payload = {
        "kind": "launch", "signature": str(signature), "slot": int(slot),
        "transaction_index": None, "program": PUMP_FUN_PROGRAM,
        "event_type": f"pump_fun_{create_name.lower()}", "logs": logs,
    }
    return StreamEvent(payload)


def _hash(value: object) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _insert_raw(payload: dict) -> None:
    detected_at = datetime.now(timezone.utc).isoformat()
    evidence = {key: payload.get(key) for key in (
        "signature", "slot", "transaction_index", "program", "event_type", "logs")}
    c = _conn()
    try:
        c.execute("""INSERT INTO raw_launches(
            signature,slot,transaction_index,program,event_type,detected_at,
            raw_payload_hash,logs,evidence_state,qualification_state
        ) VALUES (?,?,?,?,?,?,?,?,?,'raw_unqualified')
        ON CONFLICT(signature) DO UPDATE SET
          slot=excluded.slot,
          transaction_index=COALESCE(raw_launches.transaction_index,
                                     excluded.transaction_index)""",
                  (payload["signature"], payload["slot"],
                   payload.get("transaction_index"), payload["program"],
                   payload["event_type"], detected_at, _hash(evidence),
                   json.dumps(payload["logs"], separators=(",", ":")), "raw_only"))
        c.commit()
    finally:
        c.close()


def _message(tx: dict) -> dict:
    return ((tx.get("transaction") or {}).get("message") or {})


def _account_keys(tx: dict) -> tuple[list[str], set[str]]:
    keys, signers = [], set()
    for key in _message(tx).get("accountKeys") or []:
        value = key.get("pubkey") if isinstance(key, dict) else key
        if not value:
            continue
        value = str(value)
        keys.append(value)
        if isinstance(key, dict) and key.get("signer") is True:
            signers.add(value)
    return keys, signers


def _instructions(tx: dict):
    yield from _message(tx).get("instructions") or []
    for group in (tx.get("meta") or {}).get("innerInstructions") or []:
        yield from group.get("instructions") or []


def _extract_identity(tx: dict) -> tuple[str | None, str | None, str | None]:
    """Cross-check the creation instruction with transaction signer metadata."""
    _, signers = _account_keys(tx)
    for instruction in _instructions(tx):
        if instruction.get("programId") != PUMP_FUN_PROGRAM:
            continue
        accounts = [str(value) for value in instruction.get("accounts") or []]
        if not accounts:
            continue
        mint = accounts[0]
        creator_candidates = [value for value in accounts
                              if value != mint and value in signers]
        if mint in signers and len(creator_candidates) == 1:
            return creator_candidates[0], mint, None
    return None, None, "creation instruction did not prove one creator signer and mint signer"


def _set_hydration(signature: str, tx: dict | None, error: str | None) -> None:
    now = datetime.now(timezone.utc).isoformat()
    creator = mint = None
    if tx is not None and error is None:
        creator, mint, error = _extract_identity(tx)
    state = "complete" if creator and mint else ("rpc_unavailable" if tx is None
                                                   else "incomplete")
    c = _conn()
    try:
        c.execute("""UPDATE raw_launches SET creator=?,mint=?,hydrated_at=?,
                     hydration_payload_hash=?,evidence_state=?,hydration_error=?
                     WHERE signature=?""",
                  (creator, mint, now, _hash(tx) if tx is not None else None,
                   state, str(error)[:240] if error else None, signature))
        c.commit()
    finally:
        c.close()


class JsonRpc:
    def __init__(self, endpoint: str, *, timeout: float = 15):
        self.endpoint = endpoint
        self.timeout = timeout

    def call(self, method: str, params: list) -> object:
        body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method,
                           "params": params}).encode()
        request = urllib.request.Request(
            self.endpoint, data=body,
            headers={"Content-Type": "application/json", "User-Agent": "CryptoScope/1.0"})
        with urllib.request.urlopen(request, timeout=self.timeout) as response:
            result = json.load(response)
        if result.get("error"):
            raise RuntimeError(f"Solana RPC {method} failed: {result['error']}")
        return result.get("result")


def _transaction(rpc: JsonRpc, signature: str) -> dict:
    result = rpc.call("getTransaction", [signature, {
        "commitment": "confirmed", "encoding": "jsonParsed",
        "maxSupportedTransactionVersion": 0,
    }])
    if not isinstance(result, dict):
        raise RuntimeError("confirmed transaction is not available")
    return result


def persist(payload: object, *, rpc: JsonRpc | None = None,
            transaction: dict | None = None) -> None:
    if not isinstance(payload, dict) or payload.get("kind") != "launch":
        return
    _insert_raw(payload)
    try:
        tx = transaction if transaction is not None else (
            _transaction(rpc, payload["signature"]) if rpc is not None else None)
        if tx is not None:
            _set_hydration(payload["signature"], tx, None)
    except Exception as exc:
        _set_hydration(payload["signature"], None, str(exc))
        logger.warning("solana_launch_hydration_failed",
                       signature=payload["signature"][:12], error=str(exc)[:120])


def rehydrate_pending(rpc: JsonRpc, *, limit: int = 100,
                      include_incomplete: bool = False) -> dict:
    """Retry raw/RPC-failed evidence without repeatedly guessing ambiguous rows."""
    states = ["raw_only", "rpc_unavailable"]
    if include_incomplete:
        states.append("incomplete")
    placeholders = ",".join("?" for _ in states)
    c = _conn()
    try:
        rows = c.execute(
            f"SELECT signature FROM raw_launches WHERE evidence_state IN ({placeholders}) "
            "ORDER BY slot DESC LIMIT ?", (*states, max(0, int(limit))),
        ).fetchall()
    finally:
        c.close()
    completed = failed = 0
    for (signature,) in rows:
        try:
            _set_hydration(signature, _transaction(rpc, signature), None)
            completed += 1
        except Exception as exc:
            _set_hydration(signature, None, str(exc))
            failed += 1
    return {"attempted": len(rows), "completed": completed, "failed": failed}


def retry_open_gaps(rpc: JsonRpc, *, limit: int = 10) -> dict:
    gaps = stream_health.open_gaps("solana", "pump_fun_launches", limit=limit)
    recovered = failed = 0
    for gap in gaps:
        if backfill_slots(gap["from_cursor"], gap["to_cursor"], rpc=rpc):
            if stream_health.resolve_gap(
                    gap["id"], details={"backfilled": True, "retry": True,
                                        "from": gap["from_cursor"],
                                        "to": gap["to_cursor"]}):
                recovered += 1
        else:
            failed += 1
    return {"attempted": len(gaps), "recovered": recovered, "failed": failed}


def _rehydrate_loop(stop: threading.Event, rpc: JsonRpc,
                    *, interval_seconds: float = 60) -> None:
    while not stop.wait(max(1, interval_seconds)):
        result = rehydrate_pending(rpc, limit=25)
        if result["attempted"]:
            logger.info("solana_launch_rehydrated", **result)
        gaps = retry_open_gaps(rpc)
        if gaps["attempted"]:
            logger.info("solana_launch_gap_retry", **gaps)


def _launch_from_block_transaction(item: dict, slot: int, index: int) -> tuple[dict, dict] | None:
    meta = item.get("meta") or {}
    if meta.get("err") is not None:
        return None
    logs = [str(line) for line in meta.get("logMessages") or []]
    create_name = _creation_type(logs)
    tx = item.get("transaction") or {}
    if not create_name or not any(
            instruction.get("programId") == PUMP_FUN_PROGRAM
            for instruction in (tx.get("message") or {}).get("instructions") or []):
        return None
    signatures = tx.get("signatures") or []
    if not signatures:
        return None
    payload = {
        "kind": "launch", "signature": str(signatures[0]), "slot": int(slot),
        "transaction_index": int(index), "program": PUMP_FUN_PROGRAM,
        "event_type": f"pump_fun_{create_name.lower()}", "logs": logs,
    }
    normalized = {"transaction": tx, "meta": meta, "slot": int(slot)}
    return payload, normalized


def backfill_slots(from_slot: int, to_slot: int, *, rpc: JsonRpc) -> bool:
    """Recover short gaps; long gaps stay open instead of claiming completeness."""
    if to_slot < from_slot:
        return True
    if to_slot - from_slot + 1 > MAX_BACKFILL_SLOTS:
        return False
    try:
        for slot in range(int(from_slot), int(to_slot) + 1):
            block = rpc.call("getBlock", [slot, {
                "commitment": "confirmed", "encoding": "jsonParsed",
                "transactionDetails": "full", "rewards": False,
                "maxSupportedTransactionVersion": 0,
            }])
            if block is None:
                continue
            for index, item in enumerate(block.get("transactions") or []):
                launch = _launch_from_block_transaction(item, slot, index)
                if launch:
                    payload, tx = launch
                    persist(payload, transaction=tx)
        return True
    except Exception as exc:
        logger.warning("solana_launch_backfill_failed", from_slot=from_slot,
                       to_slot=to_slot, error=str(exc)[:120])
        return False


class _SolanaSocket:
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


def build_runner(*, rpc: JsonRpc | None = None,
                 socket_factory: Callable[[], object] | None = None) -> StreamRunner:
    if rpc is None:
        rpc = JsonRpc(os.getenv("SOLANA_STREAM_RPC_URL", PUBLIC_SOLANA_RPC))
    if socket_factory is None:
        from websocket import create_connection

        endpoint = os.getenv("SOLANA_STREAM_WS_URL", PUBLIC_SOLANA_WS)
        socket_factory = lambda: create_connection(endpoint, timeout=10)

    def connect():
        return _SolanaSocket(socket_factory())

    def subscribe(ws):
        for request in subscribe_requests():
            ws.send_json(request)

    return StreamRunner(
        source="solana", stream="pump_fun_launches", connect=connect,
        subscribe=subscribe, parse=parse_message,
        on_event=lambda payload: persist(payload, rpc=rpc),
        heartbeat_seconds=30, health_interval_seconds=1,
        expect_contiguous=True,
        backfill=lambda start, end: backfill_slots(start, end, rpc=rpc),
    )


def main() -> None:
    from dotenv import load_dotenv
    from src.config import PROJECT_ROOT

    load_dotenv(PROJECT_ROOT / ".env")
    _conn().close()
    rpc = JsonRpc(os.getenv("SOLANA_STREAM_RPC_URL", PUBLIC_SOLANA_RPC))
    initial = rehydrate_pending(rpc, include_incomplete=True)
    if initial["attempted"]:
        logger.info("solana_launch_initial_rehydration", **initial)
    recovered = retry_open_gaps(rpc)
    if recovered["attempted"]:
        logger.info("solana_launch_initial_gap_retry", **recovered)
    stop = threading.Event()
    worker = threading.Thread(target=_rehydrate_loop, args=(stop, rpc), daemon=True)
    worker.start()
    try:
        build_runner(rpc=rpc).run_forever(stop)
    finally:
        stop.set()
        worker.join(timeout=2)


if __name__ == "__main__":
    main()
