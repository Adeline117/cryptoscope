"""Cached, fail-visible disk policy for independent realtime workers."""
from __future__ import annotations

import threading
import time
from collections.abc import Callable, Mapping

import structlog

from src.ops import health

logger = structlog.get_logger()

CACHE_SECONDS = 5.0
UNKNOWN_LOG_HEARTBEAT_SECONDS = 60.0
_STATES = {"ok", "warn", "critical", "unknown"}


class StreamDiskCritical(RuntimeError):
    """Stop a lossless stream before its evidence writer touches a full volume."""

    def __init__(self, source: str, snapshot: dict):
        super().__init__(f"{source} evidence write blocked: workspace disk critical")
        self.source = source
        self.snapshot = snapshot


class DiskStateGuard:
    """Measure at most once per cache window; unknown measurements fail open."""

    def __init__(
        self, *, probe: Callable[[], dict] | None = None,
        monotonic: Callable[[], float] = time.monotonic,
        cache_seconds: float = CACHE_SECONDS,
    ):
        self.probe = probe if probe is not None else health._disk_health
        self.monotonic = monotonic
        self.cache_seconds = max(0.0, float(cache_seconds))
        self._lock = threading.Lock()
        self._snapshot: dict | None = None
        self._expires_at = 0.0
        self._sample_id = 0
        self._last_probe_state = None
        self._last_unknown_log_kind = None
        self._last_unknown_log_at = None

    @staticmethod
    def _unknown(error_kind: str) -> dict:
        return {
            "state": "unknown", "free_gib": None, "free_percent": None,
            "thresholds": {}, "measurement_failed": True,
            "error_kind": error_kind[:64],
        }

    def snapshot(self) -> dict:
        now = self.monotonic()
        with self._lock:
            if self._snapshot is not None and now < self._expires_at:
                return dict(self._snapshot)
            try:
                raw = self.probe()
                if not isinstance(raw, Mapping):
                    raise TypeError("disk probe did not return an object")
                state = str(raw.get("state", "unknown")).lower()
                if state not in _STATES:
                    state = "unknown"
                    error_kind = "invalid_state"
                elif state == "unknown":
                    error_kind = "probe_reported_unknown"
                else:
                    error_kind = None
                snapshot = {
                    "state": state,
                    "free_gib": raw.get("free_gib"),
                    "free_percent": raw.get("free_percent"),
                    "thresholds": dict(raw.get("thresholds") or {}),
                    "measurement_failed": state == "unknown",
                    "error_kind": error_kind,
                }
            except Exception as exc:
                snapshot = self._unknown(type(exc).__name__)
            self._sample_id += 1
            snapshot["sample_id"] = self._sample_id
            self._snapshot = snapshot
            self._expires_at = now + self.cache_seconds
            error_kind = snapshot.get("error_kind")
            unknown_log_due = (
                self._last_unknown_log_at is None
                or now - self._last_unknown_log_at >= UNKNOWN_LOG_HEARTBEAT_SECONDS
            )
            if (snapshot["state"] == "unknown"
                    and (self._last_probe_state != "unknown"
                         or error_kind != self._last_unknown_log_kind
                         or unknown_log_due)):
                self._last_unknown_log_at = now
                self._last_unknown_log_kind = error_kind
                logger.warning(
                    "stream_disk_probe_unknown_fail_open",
                    measurement_failed=True,
                    error_kind=error_kind,
                )
            self._last_probe_state = snapshot["state"]
            return dict(snapshot)

    def require_evidence_write(self, source: str) -> dict:
        snapshot = self.snapshot()
        if snapshot["state"] == "critical":
            raise StreamDiskCritical(source, snapshot)
        return snapshot

    def retain_optional_raw(self) -> tuple[bool, dict]:
        snapshot = self.snapshot()
        return snapshot["state"] not in {"warn", "critical"}, snapshot


GUARD = DiskStateGuard()
