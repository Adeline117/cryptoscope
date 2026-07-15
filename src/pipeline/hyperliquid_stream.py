"""Read-only Hyperliquid market stream for BBO, L2, trades, and asset context."""
from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from datetime import datetime, timezone

from src.config import DATA_DIR
from src.pipeline.stream_runner import StreamEvent, StreamRunner

WSS_URL = "wss://api.hyperliquid.xyz/ws"
DB = DATA_DIR / "hyperliquid_realtime.db"
DEFAULT_COINS = ("BTC", "ETH", "SOL")
CHANNELS = ("bbo", "l2Book", "trades", "activeAssetCtx")


def _conn(db_path=None) -> sqlite3.Connection:
    path = db_path or DB
    path.parent.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(str(path), timeout=10)
    c.execute("PRAGMA busy_timeout=8000")
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA synchronous=NORMAL")
    c.execute("""CREATE TABLE IF NOT EXISTS bbo(
        coin TEXT PRIMARY KEY, event_ms INTEGER NOT NULL, bid_px REAL, bid_sz REAL,
        ask_px REAL, ask_sz REAL, received_at TEXT NOT NULL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS books(
        coin TEXT PRIMARY KEY, event_ms INTEGER NOT NULL, levels TEXT NOT NULL,
        received_at TEXT NOT NULL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS asset_ctx(
        coin TEXT PRIMARY KEY, mark_px REAL, mid_px REAL, oracle_px REAL,
        funding REAL, open_interest REAL, day_ntl_volume REAL,
        received_at TEXT NOT NULL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS trades(
        coin TEXT NOT NULL, event_ms INTEGER NOT NULL, tid INTEGER NOT NULL,
        side TEXT, px REAL NOT NULL, sz REAL NOT NULL, tx_hash TEXT,
        received_at TEXT NOT NULL, PRIMARY KEY(coin,event_ms,tid))""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_hl_trades_time ON trades(event_ms)")
    return c


def configured_coins() -> tuple[str, ...]:
    raw = os.getenv("HL_STREAM_COINS", ",".join(DEFAULT_COINS))
    coins = tuple(dict.fromkeys(x.strip().upper() for x in raw.split(",") if x.strip()))
    return coins or DEFAULT_COINS


def subscription_messages(coins: tuple[str, ...]) -> list[dict]:
    return [{"method": "subscribe", "subscription": {"type": channel, "coin": coin}}
            for coin in coins for channel in CHANNELS]


def parse_message(raw: object) -> StreamEvent | None:
    msg = json.loads(raw) if isinstance(raw, (str, bytes, bytearray)) else raw
    if not isinstance(msg, dict):
        raise ValueError("Hyperliquid websocket message must be an object")
    channel = msg.get("channel")
    if channel in {"subscriptionResponse", "pong"}:
        return None
    if channel not in CHANNELS:
        return None
    data = msg.get("data")
    if channel == "trades":
        if not isinstance(data, list) or not data:
            return None
        event_ms = max(int(trade["time"]) for trade in data)
    else:
        if not isinstance(data, dict) or not data.get("coin"):
            raise ValueError(f"invalid {channel} payload")
        event_ms = int(data["time"]) if data.get("time") is not None else None
    event_at = (datetime.fromtimestamp(event_ms / 1000, tz=timezone.utc)
                if event_ms is not None else None)
    return StreamEvent({"channel": channel, "data": data}, cursor=event_ms,
                       event_at=event_at)


def _float(value) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _persist(c: sqlite3.Connection, message: object, now: str) -> None:
    if not isinstance(message, dict):
        raise ValueError("normalized Hyperliquid message must be an object")
    channel, data = message.get("channel"), message.get("data")
    if channel == "bbo":
            bid, ask = (data.get("bbo") or [None, None])[:2]
            c.execute("""INSERT INTO bbo(coin,event_ms,bid_px,bid_sz,ask_px,ask_sz,received_at)
                         VALUES (?,?,?,?,?,?,?) ON CONFLICT(coin) DO UPDATE SET
                         event_ms=excluded.event_ms,bid_px=excluded.bid_px,bid_sz=excluded.bid_sz,
                         ask_px=excluded.ask_px,ask_sz=excluded.ask_sz,
                         received_at=excluded.received_at""",
                      (data["coin"], int(data["time"]), _float((bid or {}).get("px")),
                       _float((bid or {}).get("sz")), _float((ask or {}).get("px")),
                       _float((ask or {}).get("sz")), now))
    elif channel == "l2Book":
            c.execute("""INSERT INTO books(coin,event_ms,levels,received_at) VALUES (?,?,?,?)
                         ON CONFLICT(coin) DO UPDATE SET event_ms=excluded.event_ms,
                         levels=excluded.levels,received_at=excluded.received_at""",
                      (data["coin"], int(data["time"]),
                       json.dumps(data.get("levels") or [[], []], separators=(",", ":")), now))
    elif channel == "activeAssetCtx":
            ctx = data.get("ctx") or {}
            c.execute("""INSERT INTO asset_ctx(
                coin,mark_px,mid_px,oracle_px,funding,open_interest,day_ntl_volume,received_at
            ) VALUES (?,?,?,?,?,?,?,?) ON CONFLICT(coin) DO UPDATE SET
                mark_px=excluded.mark_px,mid_px=excluded.mid_px,oracle_px=excluded.oracle_px,
                funding=excluded.funding,open_interest=excluded.open_interest,
                day_ntl_volume=excluded.day_ntl_volume,received_at=excluded.received_at""",
                      (data["coin"], _float(ctx.get("markPx")), _float(ctx.get("midPx")),
                       _float(ctx.get("oraclePx")), _float(ctx.get("funding")),
                       _float(ctx.get("openInterest")), _float(ctx.get("dayNtlVlm")), now))
    elif channel == "trades":
        for trade in data:
            c.execute("""INSERT OR IGNORE INTO trades(
                    coin,event_ms,tid,side,px,sz,tx_hash,received_at
                ) VALUES (?,?,?,?,?,?,?,?)""",
                      (trade["coin"], int(trade["time"]), int(trade["tid"]),
                       trade.get("side"), float(trade["px"]), float(trade["sz"]),
                       trade.get("hash"), now))
    else:
        raise ValueError(f"unsupported normalized channel: {channel}")


def persist(message: object) -> None:
    """Single-message helper for scripts/tests; production uses batched store below."""
    c = _conn()
    try:
        _persist(c, message, datetime.now(timezone.utc).isoformat())
        c.commit()
    finally:
        c.close()


class HyperliquidStore:
    """One WAL connection with bounded-loss, sub-second batch commits."""
    def __init__(self, db_path=None, *, batch_size: int = 100,
                 flush_seconds: float = 0.5, monotonic=time.monotonic):
        self.connection = _conn(db_path)
        self.batch_size = max(1, batch_size)
        self.flush_seconds = max(0, flush_seconds)
        self.monotonic = monotonic
        self.pending = 0
        self.last_flush = monotonic()

    def persist(self, message: object) -> None:
        _persist(self.connection, message, datetime.now(timezone.utc).isoformat())
        self.pending += 1
        now = self.monotonic()
        if self.pending >= self.batch_size or now - self.last_flush >= self.flush_seconds:
            self.flush(now=now)

    def flush(self, *, now: float | None = None) -> None:
        if self.pending:
            self.connection.commit()
            self.pending = 0
        self.last_flush = self.monotonic() if now is None else now

    def close(self) -> None:
        try:
            self.flush()
        finally:
            self.connection.close()


class _HyperliquidSocket:
    """Translate the runner heartbeat into Hyperliquid's JSON ping protocol."""
    def __init__(self, socket):
        self.socket = socket

    def recv(self):
        return self.socket.recv()

    def ping(self):
        self.socket.send(json.dumps({"method": "ping"}, separators=(",", ":")))

    def send_json(self, payload: dict):
        self.socket.send(json.dumps(payload, separators=(",", ":")))

    def close(self):
        self.socket.close()

    def shutdown(self):
        shutdown = getattr(self.socket, "shutdown", None)
        if callable(shutdown):
            shutdown()


def build_runner(coins: tuple[str, ...] | None = None,
                 store: HyperliquidStore | None = None) -> StreamRunner:
    from websocket import create_connection

    coins = coins or configured_coins()

    def connect():
        return _HyperliquidSocket(create_connection(WSS_URL, timeout=10))

    def subscribe(ws):
        for message in subscription_messages(coins):
            ws.send_json(message)

    return StreamRunner(
        source="hyperliquid", stream="public_market:" + ",".join(coins),
        connect=connect, subscribe=subscribe, parse=parse_message,
        on_event=store.persist if store else persist,
        heartbeat_seconds=30, health_interval_seconds=1, expect_contiguous=False,
    )


def main() -> None:
    from dotenv import load_dotenv
    from src.config import PROJECT_ROOT

    load_dotenv(PROJECT_ROOT / ".env")
    store = HyperliquidStore()
    try:
        build_runner(store=store).run_forever(threading.Event())
    finally:
        store.close()


if __name__ == "__main__":
    main()
