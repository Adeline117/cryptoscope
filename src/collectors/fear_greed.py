"""Fear & Greed Index collector using the free Alternative.me API."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from src.collectors.base import BaseCollector, CollectedItem, CollectionResult


class FearGreedCollector(BaseCollector):
    """Collect the Crypto Fear & Greed Index from Alternative.me.

    The index ranges from 0 (Extreme Fear) to 100 (Extreme Greed) and is
    calculated from volatility, market momentum/volume, social media,
    dominance, and trends.

    No API key required.
    """

    source_id = "fear_greed"
    source_name = "Crypto Fear & Greed Index"
    source_type = "api"

    BASE_URL = "https://api.alternative.me/fng/"

    def __init__(self, limit: int = 30, **kwargs):
        """Initialize with configurable history depth.

        Args:
            limit: Number of days of history to fetch (default 30).
        """
        super().__init__(cache_ttl=1800, **kwargs)
        self.limit = limit

    async def _collect(self) -> CollectionResult:
        """Fetch Fear & Greed index data and detect anomalies."""
        data = await self._fetch_json(
            self.BASE_URL,
            params={"limit": str(self.limit), "format": "json"},
        )

        entries: list[dict[str, Any]] = data.get("data", [])
        if not entries:
            self.log.warning("no_fng_data")
            return CollectionResult(
                source_id=self.source_id,
                source_name=self.source_name,
                source_type=self.source_type,
            )

        items: list[CollectedItem] = []

        for i, entry in enumerate(entries):
            value = int(entry.get("value", 0))
            classification = entry.get("value_classification", "Unknown")
            timestamp = int(entry.get("timestamp", 0))
            dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
            date_str = dt.strftime("%Y-%m-%d")

            # Detect sudden change: compare with the next entry in the list
            # (entries are sorted newest-first, so index i+1 is the previous day)
            previous_value: int | None = None
            change: int | None = None
            is_anomaly = False

            if i + 1 < len(entries):
                previous_value = int(entries[i + 1].get("value", 0))
                change = value - previous_value
                if abs(change) > 15:
                    is_anomaly = True

            metadata: dict[str, Any] = {
                "data_type": "fear_greed_index",
                "value": value,
                "value_classification": classification,
                "timestamp": timestamp,
                "date": date_str,
                "previous_value": previous_value,
                "change": change,
                "is_anomaly": is_anomaly,
                "priority": "critical" if is_anomaly else "medium",
            }

            title = f"Fear & Greed Index: {value} ({classification}) [{date_str}]"
            content_parts = [f"Value: {value}/100 — {classification}"]
            if change is not None:
                content_parts.append(f"Change from previous day: {change:+d}")
            if is_anomaly:
                direction = "spike" if change and change > 0 else "drop"
                content_parts.append(
                    f"ANOMALY: Sudden {direction} of {abs(change)} points detected"
                )

            items.append(
                CollectedItem(
                    id=f"fng_{date_str}",
                    title=title,
                    content=" | ".join(content_parts),
                    url="https://alternative.me/crypto/fear-and-greed-index/",
                    published_at=dt,
                    metadata=metadata,
                    raw=entry,
                )
            )

        self.log.info(
            "fng_collected",
            total=len(items),
            anomalies=sum(1 for it in items if it.metadata.get("is_anomaly")),
            latest_value=items[0].metadata["value"] if items else None,
        )

        return CollectionResult(
            source_id=self.source_id,
            source_name=self.source_name,
            source_type=self.source_type,
            items=items,
        )
