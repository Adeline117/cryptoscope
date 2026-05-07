"""Alert batching / anti-flood system for CryptoScope Telegram push.

Rules:
- Same-asset alerts within 5 minutes are merged into one message.
- P1 alerts are capped at 3 per hour; overflow is merged into a summary.
"""

from __future__ import annotations

import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

import structlog

logger = structlog.get_logger()

# Configuration
MERGE_WINDOW_SECONDS = 300  # 5 minutes
P1_MAX_PER_HOUR = 3
P1_WINDOW_SECONDS = 3600  # 1 hour


@dataclass
class PendingAlert:
    """A single pending alert waiting to be flushed."""

    asset: str  # e.g. "BTC", "ETH", or "MARKET"
    priority: str  # "P0", "P1", "P2"
    title: str
    body: str
    timestamp: float = field(default_factory=time.time)
    extra: dict[str, Any] = field(default_factory=dict)


class MessageBatcher:
    """Accumulate alerts and flush them in batches to avoid Telegram spam.

    Usage::

        batcher = MessageBatcher()
        batcher.add(PendingAlert(asset="BTC", priority="P1", title="...", body="..."))
        batcher.add(PendingAlert(asset="BTC", priority="P1", title="...", body="..."))

        for msg in batcher.flush():
            await send_message(msg)
    """

    def __init__(
        self,
        merge_window: int = MERGE_WINDOW_SECONDS,
        p1_max_per_hour: int = P1_MAX_PER_HOUR,
    ):
        self._merge_window = merge_window
        self._p1_max_per_hour = p1_max_per_hour

        # Pending alerts grouped by asset
        self._pending: list[PendingAlert] = []
        # P1 send timestamps (for hourly cap)
        self._p1_history: list[float] = []

    def add(self, alert: PendingAlert) -> None:
        """Add a new alert to the pending queue."""
        self._pending.append(alert)
        logger.debug(
            "batcher_alert_added",
            asset=alert.asset,
            priority=alert.priority,
            title=alert.title,
        )

    def flush(self) -> list[dict[str, Any]]:
        """Process pending alerts and return a list of messages to send.

        Returns a list of dicts with keys:
            - text (str): Formatted message text
            - priority (str): "P0", "P1", "P2"
            - assets (list[str]): Involved assets
            - merged_count (int): How many alerts were merged
        """
        if not self._pending:
            return []

        now = time.time()

        # Prune old P1 history
        self._p1_history = [
            ts for ts in self._p1_history if now - ts < P1_WINDOW_SECONDS
        ]

        # Group by (asset, priority)
        groups: dict[tuple[str, str], list[PendingAlert]] = defaultdict(list)
        for alert in self._pending:
            groups[(alert.asset, alert.priority)].append(alert)

        messages: list[dict[str, Any]] = []

        for (asset, priority), alerts in groups.items():
            # Merge same-asset alerts within the merge window
            merged = self._merge_by_time(alerts, now)

            for batch in merged:
                if priority == "P0":
                    # P0: always send immediately, never throttle
                    messages.append(self._format_batch(batch, asset, priority))
                elif priority == "P1":
                    # P1: cap at N per hour
                    if len(self._p1_history) < self._p1_max_per_hour:
                        messages.append(self._format_batch(batch, asset, priority))
                        self._p1_history.append(now)
                    else:
                        # Overflow: create summary instead
                        messages.append(self._format_overflow_summary(batch, asset))
                else:
                    # P2+: just send merged
                    messages.append(self._format_batch(batch, asset, priority))

        self._pending.clear()

        logger.info("batcher_flushed", message_count=len(messages))
        return messages

    def _merge_by_time(
        self, alerts: list[PendingAlert], now: float
    ) -> list[list[PendingAlert]]:
        """Group alerts that fall within the merge window."""
        if not alerts:
            return []

        alerts_sorted = sorted(alerts, key=lambda a: a.timestamp)
        batches: list[list[PendingAlert]] = []
        current_batch: list[PendingAlert] = [alerts_sorted[0]]

        for alert in alerts_sorted[1:]:
            if alert.timestamp - current_batch[0].timestamp <= self._merge_window:
                current_batch.append(alert)
            else:
                batches.append(current_batch)
                current_batch = [alert]
        batches.append(current_batch)
        return batches

    def _format_batch(
        self,
        batch: list[PendingAlert],
        asset: str,
        priority: str,
    ) -> dict[str, Any]:
        """Format a batch of alerts into a single message dict."""
        if len(batch) == 1:
            alert = batch[0]
            return {
                "text": f"<b>[{priority}] {alert.title}</b>\n\n{alert.body}",
                "priority": priority,
                "assets": [asset],
                "merged_count": 1,
            }

        # Merged message
        header = f"<b>[{priority}] {asset} — {len(batch)} alerts merged</b>\n"
        lines = []
        for i, alert in enumerate(batch, 1):
            lines.append(f"{i}. <b>{alert.title}</b>\n   {alert.body[:120]}")

        return {
            "text": header + "\n".join(lines),
            "priority": priority,
            "assets": [asset],
            "merged_count": len(batch),
        }

    def _format_overflow_summary(
        self,
        batch: list[PendingAlert],
        asset: str,
    ) -> dict[str, Any]:
        """Format a P1 overflow summary when hourly cap is exceeded."""
        titles = [a.title for a in batch]
        summary = ", ".join(titles[:5])
        if len(titles) > 5:
            summary += f" ... and {len(titles) - 5} more"

        return {
            "text": (
                f"<b>[P1 Summary] {asset}</b>\n\n"
                f"⚠️ Hourly alert cap reached ({self._p1_max_per_hour}/hr).\n"
                f"Merged {len(batch)} alerts:\n{summary}"
            ),
            "priority": "P1_SUMMARY",
            "assets": [asset],
            "merged_count": len(batch),
        }

    @property
    def pending_count(self) -> int:
        """Number of alerts waiting to be flushed."""
        return len(self._pending)
