"""DeFi liquidation cascade monitoring via DeFiLlama.

Tracks global liquidation levels and alerts when significant liquidation
volume is clustered near current prices.
"""

from __future__ import annotations

from datetime import datetime, timezone

import structlog

from src.collectors.base import BaseCollector, CollectedItem, CollectionResult

logger = structlog.get_logger()


class LiquidationMonitor(BaseCollector):
    """Monitor DeFi liquidation levels and detect cascade risk.

    Uses DeFiLlama's liquidation data to track USD amounts at risk at
    various price points for major assets.

    Alert: liquidation volume within +/-10% of current price exceeds $500M.
    """

    source_id = "liquidation_monitor"
    source_name = "DeFi Liquidation Monitor"
    source_type = "api"

    BASE_URL = "https://api.llama.fi"
    COINGECKO_URL = "https://api.coingecko.com/api/v3"

    # Assets to monitor for liquidation cascades
    MONITORED_ASSETS = ["ETH", "BTC"]

    # Alert threshold: $500M within ±10% of current price
    LIQUIDATION_THRESHOLD_USD = 500_000_000
    PRICE_PROXIMITY_PCT = 10.0

    def __init__(self, **kwargs):
        super().__init__(cache_ttl=900, **kwargs)  # 15-minute cache

    async def _collect(self) -> CollectionResult:
        items: list[CollectedItem] = []

        # Fetch current prices for reference
        current_prices = await self._fetch_current_prices()

        # Fetch liquidation data from DeFiLlama
        try:
            liq_data = await self._fetch_json(f"{self.BASE_URL}/liquidations")
        except Exception as e:
            self.log.warning("liquidation_data_fetch_failed", error=str(e))
            liq_data = {}

        # Process liquidation data for each monitored asset
        for asset in self.MONITORED_ASSETS:
            current_price = current_prices.get(asset.lower())
            if current_price is None:
                self.log.warning("no_price_for_asset", asset=asset)
                continue

            asset_items = self._process_asset_liquidations(
                asset, liq_data, current_price
            )
            items.extend(asset_items)

        # Create a global summary item
        if liq_data:
            items.append(self._create_summary_item(liq_data, current_prices))

        return CollectionResult(
            source_id=self.source_id,
            source_name=self.source_name,
            source_type=self.source_type,
            items=items,
        )

    async def _fetch_current_prices(self) -> dict[str, float]:
        """Fetch current prices from CoinGecko for reference."""
        prices: dict[str, float] = {}
        try:
            data = await self._fetch_json(
                f"{self.COINGECKO_URL}/simple/price",
                params={
                    "ids": "bitcoin,ethereum",
                    "vs_currencies": "usd",
                },
            )
            if "bitcoin" in data:
                prices["btc"] = data["bitcoin"]["usd"]
            if "ethereum" in data:
                prices["eth"] = data["ethereum"]["usd"]
        except Exception as e:
            self.log.warning("coingecko_price_fetch_failed", error=str(e))
        return prices

    def _process_asset_liquidations(
        self, asset: str, liq_data: dict, current_price: float
    ) -> list[CollectedItem]:
        """Process liquidation data for a single asset and detect cascade risk."""
        items: list[CollectedItem] = []

        # DeFiLlama liquidations endpoint returns protocol-level data
        # with liquidatable positions at various price thresholds
        protocols = liq_data.get("protocols", [])

        # Calculate the price range we care about (±10% of current)
        price_lower = current_price * (1 - self.PRICE_PROXIMITY_PCT / 100)
        price_upper = current_price * (1 + self.PRICE_PROXIMITY_PCT / 100)

        total_at_risk = 0.0
        protocol_breakdown: list[dict] = []

        for protocol in protocols:
            symbol = protocol.get("symbol", "").upper()
            if symbol != asset:
                # Check if the protocol tracks this asset
                chains = protocol.get("chains", [])
                name = protocol.get("name", "").lower()
                # Broad match: skip protocols that clearly don't relate
                if asset.lower() not in name and asset.lower() not in symbol.lower():
                    continue

            # Extract liquidation amounts at various price levels
            liq_at_risk = protocol.get("liquidablePositions", 0)
            total_liq = protocol.get("totalLiquidable", 0)

            # Use per-price-level data if available
            price_levels = protocol.get("dangerousPositions", [])
            near_price_amount = 0.0

            for position in price_levels:
                liq_price = position.get("liquidationPrice", 0)
                collateral_usd = position.get("collateralValue", 0)

                if price_lower <= liq_price <= price_upper:
                    near_price_amount += collateral_usd

            # Fall back to total liquidable if no per-position data
            if near_price_amount == 0 and liq_at_risk > 0:
                near_price_amount = liq_at_risk

            if near_price_amount > 0:
                total_at_risk += near_price_amount
                protocol_breakdown.append({
                    "protocol": protocol.get("name", "Unknown"),
                    "amount_at_risk": near_price_amount,
                    "total_liquidable": total_liq,
                })

        # Determine alert severity
        severity = None
        alert_description = ""

        if total_at_risk >= self.LIQUIDATION_THRESHOLD_USD:
            severity = "critical"
            alert_description = (
                f"LIQUIDATION CASCADE RISK: ${total_at_risk:,.0f} in {asset} "
                f"liquidations within ±{self.PRICE_PROXIMITY_PCT:.0f}% of "
                f"current price (${current_price:,.0f})"
            )
        elif total_at_risk >= self.LIQUIDATION_THRESHOLD_USD * 0.5:
            severity = "high"
            alert_description = (
                f"Elevated liquidation risk: ${total_at_risk:,.0f} in {asset} "
                f"near current price (${current_price:,.0f})"
            )

        # Create the asset monitoring item
        metadata = {
            "data_type": "liquidation_levels",
            "asset": asset,
            "current_price": current_price,
            "price_range_lower": price_lower,
            "price_range_upper": price_upper,
            "total_at_risk_usd": total_at_risk,
            "protocol_breakdown": protocol_breakdown[:10],
            "alert_severity": severity,
            "alert_description": alert_description,
        }

        title = (
            f"{asset} Liquidation Monitor: ${total_at_risk:,.0f} at risk "
            f"near ${current_price:,.0f}"
        )

        items.append(
            CollectedItem(
                id=f"liquidation_{asset.lower()}_levels",
                title=title,
                content=alert_description or f"Monitoring {asset} liquidation levels",
                url="https://defillama.com/liquidations",
                published_at=datetime.now(timezone.utc),
                metadata=metadata,
                raw={"asset": asset, "protocols": protocol_breakdown},
            )
        )

        # Add separate alert item if threshold breached
        if severity:
            items.append(
                CollectedItem(
                    id=f"liquidation_alert_{asset.lower()}",
                    title=f"ALERT: {alert_description}",
                    content=alert_description,
                    url="https://defillama.com/liquidations",
                    published_at=datetime.now(timezone.utc),
                    metadata={
                        **metadata,
                        "data_type": "liquidation_alert",
                        "priority": severity,
                    },
                    raw={"asset": asset, "total_at_risk": total_at_risk},
                )
            )

        return items

    def _create_summary_item(
        self, liq_data: dict, current_prices: dict[str, float]
    ) -> CollectedItem:
        """Create a global liquidation summary item."""
        protocols = liq_data.get("protocols", [])
        total_liquidable = sum(
            p.get("totalLiquidable", 0) for p in protocols
        )
        protocol_count = len(protocols)

        return CollectedItem(
            id="liquidation_global_summary",
            title=f"Global DeFi Liquidation Summary: ${total_liquidable:,.0f} total liquidable",
            content=(
                f"{protocol_count} protocols tracked | "
                f"BTC: ${current_prices.get('btc', 0):,.0f} | "
                f"ETH: ${current_prices.get('eth', 0):,.0f}"
            ),
            url="https://defillama.com/liquidations",
            published_at=datetime.now(timezone.utc),
            metadata={
                "data_type": "liquidation_summary",
                "total_liquidable_usd": total_liquidable,
                "protocol_count": protocol_count,
                "btc_price": current_prices.get("btc"),
                "eth_price": current_prices.get("eth"),
            },
            raw=liq_data,
        )
