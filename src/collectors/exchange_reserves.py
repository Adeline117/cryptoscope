"""Exchange reserve monitoring via DeFiLlama (no API key needed).

Tracks centralized exchange reserves and detects bank-run-like outflows.
"""

from __future__ import annotations

from datetime import datetime, timezone

import structlog

from src.collectors.base import BaseCollector, CollectedItem, CollectionResult

logger = structlog.get_logger()


class ExchangeReserveCollector(BaseCollector):
    """Monitor CEX reserves using DeFiLlama protocol data.

    Alerts:
        - 24h drop > 10%: high severity
        - 24h drop > 20%: critical ("bank run warning")
        - 3 consecutive days of decline totaling > 15%: critical
    """

    source_id = "exchange_reserves"
    source_name = "Exchange Reserves (DeFiLlama)"
    source_type = "api"

    BASE_URL = "https://api.llama.fi"

    EXCHANGE_SLUGS: dict[str, str] = {
        "binance": "binance-cex",
        "okx": "okx",
        "bybit": "bybit",
        "coinbase": "coinbase",
        "kraken": "kraken",
        "bitfinex": "bitfinex",
    }

    # Alert thresholds (percentage drops)
    THRESHOLD_HIGH_24H = 10.0
    THRESHOLD_CRITICAL_24H = 20.0
    THRESHOLD_CRITICAL_MULTI_DAY = 15.0
    MULTI_DAY_WINDOW = 3  # consecutive days

    def __init__(self, exchanges: list[str] | None = None, **kwargs):
        super().__init__(cache_ttl=1800, **kwargs)
        self.exchanges = exchanges or list(self.EXCHANGE_SLUGS.keys())

    async def _collect(self) -> CollectionResult:
        items: list[CollectedItem] = []

        for exchange_name in self.exchanges:
            slug = self.EXCHANGE_SLUGS.get(exchange_name)
            if not slug:
                self.log.warning("unknown_exchange", exchange=exchange_name)
                continue

            try:
                data = await self._fetch_json(f"{self.BASE_URL}/protocol/{slug}")
                items.extend(self._process_exchange(exchange_name, slug, data))
            except Exception as e:
                self.log.warning(
                    "exchange_reserve_fetch_failed",
                    exchange=exchange_name,
                    error=str(e),
                )

        return CollectionResult(
            source_id=self.source_id,
            source_name=self.source_name,
            source_type=self.source_type,
            items=items,
        )

    def _process_exchange(
        self, exchange_name: str, slug: str, data: dict
    ) -> list[CollectedItem]:
        """Process a single exchange's reserve data and generate alerts."""
        items: list[CollectedItem] = []

        current_tvl = data.get("tvl")
        if current_tvl is None or current_tvl == 0:
            self.log.warning("no_tvl_data", exchange=exchange_name)
            return items

        # Extract historical TVL from the tvl chart data
        tvl_history: list[dict] = data.get("tvl", []) if isinstance(data.get("tvl"), list) else []
        chain_tvls = data.get("chainTvls", {})

        # If tvl is a number (current), use currentChainTvls or the top-level value
        if isinstance(current_tvl, (int, float)):
            reserves_usd = current_tvl
        else:
            # tvl might be a list of historical entries
            reserves_usd = 0
            if tvl_history:
                reserves_usd = tvl_history[-1].get("totalLiquidityUSD", 0)

        # Calculate changes from historical data
        # DeFiLlama provides chainTvls with dated entries
        change_24h_pct = data.get("change_1d")
        change_7d_pct = data.get("change_7d")

        # Determine alert severity based on 24h change
        severity = None
        alert_description = ""

        if change_24h_pct is not None:
            try:
                change_val = float(change_24h_pct)
            except (ValueError, TypeError):
                change_val = 0.0

            if change_val <= -self.THRESHOLD_CRITICAL_24H:
                severity = "critical"
                alert_description = (
                    f"BANK RUN WARNING: {exchange_name.upper()} reserves dropped "
                    f"{abs(change_val):.1f}% in 24h (${reserves_usd:,.0f} remaining)"
                )
            elif change_val <= -self.THRESHOLD_HIGH_24H:
                severity = "high"
                alert_description = (
                    f"{exchange_name.upper()} reserves dropped "
                    f"{abs(change_val):.1f}% in 24h (${reserves_usd:,.0f} remaining)"
                )

        # Check for multi-day consecutive decline from historical TVL
        multi_day_severity = self._check_multi_day_decline(data, exchange_name)
        if multi_day_severity:
            if severity != "critical":
                severity = multi_day_severity["severity"]
                alert_description = multi_day_severity["description"]

        # Build the base item
        metadata = {
            "data_type": "exchange_reserve",
            "exchange": exchange_name,
            "slug": slug,
            "reserves_usd": reserves_usd,
            "change_24h_pct": change_24h_pct,
            "change_7d_pct": change_7d_pct,
            "alert_severity": severity,
            "alert_description": alert_description,
        }

        title = (
            f"{exchange_name.upper()} Reserves: ${reserves_usd:,.0f}"
        )
        if change_24h_pct is not None:
            title += f" (24h: {float(change_24h_pct):+.1f}%)"

        content = ""
        if alert_description:
            content = alert_description

        items.append(
            CollectedItem(
                id=f"exchange_reserve_{exchange_name}",
                title=title,
                content=content,
                url=f"https://defillama.com/protocol/{slug}",
                published_at=datetime.now(timezone.utc),
                metadata=metadata,
                raw=data,
            )
        )

        # If there's a severity alert, add a separate alert item
        if severity:
            items.append(
                CollectedItem(
                    id=f"exchange_reserve_alert_{exchange_name}",
                    title=f"ALERT: {alert_description}",
                    content=alert_description,
                    url=f"https://defillama.com/protocol/{slug}",
                    published_at=datetime.now(timezone.utc),
                    metadata={
                        **metadata,
                        "data_type": "exchange_reserve_alert",
                        "priority": severity,
                    },
                    raw=data,
                )
            )

        return items

    def _check_multi_day_decline(
        self, data: dict, exchange_name: str
    ) -> dict | None:
        """Check for 3 consecutive days of decline totaling > 15%.

        Uses the chainTvls historical data if available.
        """
        # Try to extract daily TVL history from chainTvls
        chain_tvls = data.get("chainTvls", {})
        tvl_entries: list[dict] = []

        # Aggregate across all chains
        for chain_data in chain_tvls.values():
            if isinstance(chain_data, dict) and "tvl" in chain_data:
                tvl_entries = chain_data["tvl"]
                break  # Use the first chain with TVL data

        # Fall back to top-level tvl if it's a list
        if not tvl_entries and isinstance(data.get("tvl"), list):
            tvl_entries = data["tvl"]

        if len(tvl_entries) < self.MULTI_DAY_WINDOW + 1:
            return None

        # Get the last N+1 days of data (newest last)
        recent = tvl_entries[-(self.MULTI_DAY_WINDOW + 1):]

        # Check if all days show decline
        all_declining = True
        for i in range(1, len(recent)):
            prev_val = recent[i - 1].get("totalLiquidityUSD", 0)
            curr_val = recent[i].get("totalLiquidityUSD", 0)
            if prev_val == 0 or curr_val >= prev_val:
                all_declining = False
                break

        if not all_declining:
            return None

        # Calculate total decline over the window
        start_val = recent[0].get("totalLiquidityUSD", 0)
        end_val = recent[-1].get("totalLiquidityUSD", 0)

        if start_val == 0:
            return None

        total_decline_pct = ((start_val - end_val) / start_val) * 100

        if total_decline_pct >= self.THRESHOLD_CRITICAL_MULTI_DAY:
            return {
                "severity": "critical",
                "description": (
                    f"{exchange_name.upper()} reserves declined for "
                    f"{self.MULTI_DAY_WINDOW} consecutive days, "
                    f"totaling {total_decline_pct:.1f}% loss "
                    f"(${end_val:,.0f} remaining)"
                ),
            }

        return None
