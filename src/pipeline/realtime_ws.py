"""Real-time WebSocket transfer listener — the root-cure, event-driven layer.

Polling balanceOf was the disease (stale/oscillating → EVAA misses, SIREN spam).
The cure: subscribe to Transfer EVENTS over WebSocket. A token balance only changes
via Transfer, so events are ground truth, captured ~3s after the block — no polling,
no balanceOf, no lies.

Subscribes to `logs` for the tracked token contracts (Transfer topic), decodes
from/to/amount, and when a cluster wallet is the sender (SELL) or receiver (BUY),
buffers it; flushes every FLUSH_SEC into a net-flow alert with the Seattle order
time. Phase-dedup (shared sentinel state) avoids repeating the same behavior.
Reconnects on drop (free wss are flaky). Free: publicnode / drpc BSC wss.

    python -m src.pipeline.realtime_ws
"""

from __future__ import annotations

import json
import socket
import time
from collections import defaultdict
from contextlib import suppress

import structlog

logger = structlog.get_logger()

_WSS = ["wss://bsc-rpc.publicnode.com", "wss://bsc.drpc.org"]
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
FLUSH_SEC = 30           # batch transfers and emit a net-flow alert every N seconds
MIN_FLOW_TOKENS = 1.0    # ignore dust


def _close_websocket(ws) -> None:
    """Release the socket even after websocket-client marks a peer close disconnected.

    ``WebSocket.close()`` returns immediately when ``connected`` is already false.
    A received close frame sets that flag before our reconnect ``finally`` runs, so
    calling only ``close`` leaves the underlying TCP socket in CLOSE_WAIT. ``shutdown``
    is idempotent and closes the socket regardless of that flag.
    """
    with suppress(Exception):
        ws.close()
    with suppress(Exception):
        ws.shutdown()


def _open_subscription(url: str, tokens: list[str], socket_factory):
    """Open and subscribe, closing a partially-initialized socket on failure."""
    ws = socket_factory(url, timeout=10)
    try:
        ws.send(json.dumps({"jsonrpc": "2.0", "id": 1, "method": "eth_subscribe",
                            "params": ["logs", {"address": tokens,
                                                "topics": [TRANSFER_TOPIC]}]}))
        reply = json.loads(ws.recv())
        if reply.get("error") or not reply.get("result"):
            raise ConnectionError(f"subscription rejected: {reply.get('error') or reply}")
        return ws
    except BaseException:
        _close_websocket(ws)
        raise


def _receive_one(ws, buf: dict, targets: dict) -> None:
    """Receive one event; an empty frame means the peer closed the connection."""
    raw = ws.recv()
    if not raw:
        raise ConnectionError("websocket peer closed")
    try:
        msg = json.loads(raw)
        p = msg.get("params", {}).get("result")
        if p and len(p.get("topics", [])) >= 3:
            token = p["address"].lower()
            frm = "0x" + p["topics"][1][26:].lower()
            to = "0x" + p["topics"][2][26:].lower()
            amt = int(p["data"], 16) / 1e18
            wl = targets.get(token, {}).get("wallets", set())
            if to in wl and frm not in wl:
                buf[token]["buy"] += amt
            elif frm in wl and to not in wl:
                buf[token]["sell"] += amt
    except (AttributeError, KeyError, TypeError, ValueError, json.JSONDecodeError):
        logger.debug("ws_message_invalid")


def _load_targets() -> dict:
    """token(lower) -> {symbol, chain, wallets:set} for BSC clusters."""
    from src.pipeline.operator_sentinel import _load
    out = {}
    for t in _load().values():
        if t.get("chain") != "bsc":
            continue
        out[t["token"].lower()] = {"symbol": t["symbol"], "chain": "bsc",
                                   "wallets": {w.lower() for w in t["wallets"]}}
    return out


def _seattle_now() -> str:
    from datetime import datetime
    from zoneinfo import ZoneInfo
    return datetime.now(ZoneInfo("America/Los_Angeles")).strftime("%m-%d %H:%M:%S PDT")


def _flush(buf: dict, targets: dict) -> None:
    """Emit a net buy/sell alert per token from the buffered transfers."""
    from src.pipeline.anomaly_screener import _run_coro
    from src.distribution.telegram_sender import send_alert
    for token, flow in list(buf.items()):
        buy, sell = flow["buy"], flow["sell"]
        if max(buy, sell) < MIN_FLOW_TOKENS:
            continue
        meta = targets.get(token, {})
        sym = meta.get("symbol", token[:8])
        net = buy - sell
        # phase-dedup via the SHARED sentinel phase (same 'last_phase' key the 5-min
        # scheduler + watcher use) → ws is primary, scheduler is backup, no double-fire.
        from src.pipeline.operator_sentinel import _load, _save, _state_lock
        phase = "buy" if net > 0 else "sell"
        fire = True
        with _state_lock():
            d = _load()
            key = f"bsc:{token}"
            if key in d:
                if d[key].get("last_phase") == phase:
                    fire = False
                else:
                    d[key]["last_phase"] = phase
                    _save(d)
        if not fire:
            continue
        if net > 0:
            msg = (f"⚡ <b>{sym}</b> 实时(WebSocket)\n👉 🟢 庄在买 净 {net:,.0f}"
                   f"(买{buy:,.0f}/卖{sell:,.0f}) · {_seattle_now()}\n<code>{token}</code>")
        else:
            msg = (f"⚡ <b>{sym}</b> 实时(WebSocket)\n👉 🔴 庄在卖 净 {-net:,.0f}"
                   f"(卖{sell:,.0f}/买{buy:,.0f}) · {_seattle_now()}\n<code>{token}</code>")
        _run_coro(send_alert(msg))
        logger.warning("ws_alert", symbol=sym, net=net)
    buf.clear()


def run():
    from websocket import WebSocketTimeoutException, create_connection
    targets = _load_targets()
    if not targets:
        logger.info("ws_no_targets")
        return
    tokens = list(targets)
    logger.info("realtime_ws_start", tokens=len(tokens))
    while True:
        ws = None
        for url in _WSS:
            try:
                ws = _open_subscription(url, tokens, create_connection)
                logger.info("ws_connected", url=url)
                break
            except Exception as e:
                logger.debug("ws_connect_failed", url=url, error=str(e)[:60])
                ws = None
        if ws is None:
            time.sleep(15)
            continue
        buf = defaultdict(lambda: {"buy": 0.0, "sell": 0.0})
        last_flush = time.time()
        try:
            ws.settimeout(FLUSH_SEC)
            while True:
                try:
                    _receive_one(ws, buf, targets)
                except (TimeoutError, socket.timeout, WebSocketTimeoutException):
                    pass  # idle subscription: flush on schedule, keep connection
                if time.time() - last_flush >= FLUSH_SEC:
                    _flush(buf, targets)
                    last_flush = time.time()
                    targets = _load_targets()  # pick up newly registered clusters
                    tokens = list(targets)
        except Exception as e:
            logger.warning("ws_loop_error", error=str(e)[:80])
        finally:
            _close_websocket(ws)
        time.sleep(5)  # reconnect


def main():
    from dotenv import load_dotenv
    from src.config import PROJECT_ROOT
    load_dotenv(PROJECT_ROOT / ".env")
    run()


if __name__ == "__main__":
    main()
