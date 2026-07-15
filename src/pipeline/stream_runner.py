"""Transport-agnostic long-lived WebSocket runner with fail-visible recovery."""
from __future__ import annotations

import socket
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Protocol

import structlog

from src.pipeline import stream_health

logger = structlog.get_logger()


class SocketLike(Protocol):
    def recv(self): ...
    def ping(self): ...
    def close(self): ...


@dataclass(frozen=True)
class StreamEvent:
    payload: object
    cursor: int | None = None
    event_at: datetime | str | None = None


class StreamRunner:
    def __init__(
        self, *, source: str, stream: str,
        connect: Callable[[], SocketLike],
        subscribe: Callable[[SocketLike], None],
        parse: Callable[[object], StreamEvent | None],
        on_event: Callable[[object], None],
        expect_contiguous: bool = False,
        backfill: Callable[[int, int], bool] | None = None,
        heartbeat_seconds: float = 30,
        health_interval_seconds: float = 0,
        backoff_base_seconds: float = 1,
        backoff_max_seconds: float = 60,
        monotonic: Callable[[], float] = time.monotonic,
    ):
        self.source = source
        self.stream = stream
        self.connect = connect
        self.subscribe = subscribe
        self.parse = parse
        self.on_event = on_event
        self.expect_contiguous = expect_contiguous
        self.backfill = backfill
        self.heartbeat_seconds = max(0, heartbeat_seconds)
        self.health_interval_seconds = max(0, health_interval_seconds)
        self.backoff_base_seconds = max(0, backoff_base_seconds)
        self.backoff_max_seconds = max(self.backoff_base_seconds, backoff_max_seconds)
        self.monotonic = monotonic

    def _backoff(self, failures: int) -> float:
        return min(self.backoff_max_seconds,
                   self.backoff_base_seconds * (2 ** max(0, failures - 1)))

    def run_connection(self, stop: threading.Event) -> int:
        """Run one connected session; return processed events or raise on disconnect."""
        ws = self.connect()
        processed = 0
        last_ping = self.monotonic()
        last_health = None
        try:
            self.subscribe(ws)
            while not stop.is_set():
                try:
                    raw = ws.recv()
                    if raw in (None, "", b""):
                        raise ConnectionError("websocket closed")
                except (TimeoutError, socket.timeout):
                    now = self.monotonic()
                    if now - last_ping >= self.heartbeat_seconds:
                        ws.ping()
                        last_ping = now
                    continue
                event = self.parse(raw)
                if event is None:
                    continue
                health_now = self.monotonic()
                should_record_health = ((self.expect_contiguous and event.cursor is not None)
                                        or self.health_interval_seconds == 0
                                        or last_health is None
                                        or health_now - last_health >= self.health_interval_seconds)
                observed = {}
                if should_record_health:
                    observed = stream_health.observe(
                        self.source, self.stream, cursor=event.cursor,
                        event_at=event.event_at, expect_contiguous=self.expect_contiguous)
                    last_health = health_now
                gap = observed.get("gap")
                if gap and self.backfill:
                    try:
                        recovered = self.backfill(gap["from_cursor"], gap["to_cursor"])
                    except Exception as exc:
                        logger.warning("stream_backfill_failed", source=self.source,
                                       stream=self.stream, error=str(exc)[:120])
                        recovered = False
                    if recovered:
                        stream_health.resolve_gap(
                            gap["id"], details={"backfilled": True,
                                                "from": gap["from_cursor"],
                                                "to": gap["to_cursor"]})
                self.on_event(event.payload)
                processed += 1
            return processed
        finally:
            try:
                ws.close()
            except Exception:
                pass

    def run_forever(self, stop: threading.Event | None = None) -> None:
        stop = stop or threading.Event()
        failures = 0
        while not stop.is_set():
            try:
                processed = self.run_connection(stop)
                failures = 0 if processed else failures + 1
            except Exception as exc:
                failures += 1
                stream_health.mark_disconnected(self.source, self.stream, str(exc))
                logger.warning("stream_disconnected", source=self.source, stream=self.stream,
                               failures=failures, error=str(exc)[:120])
            if not stop.is_set():
                stop.wait(self._backoff(failures))
