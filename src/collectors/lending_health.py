"""Lending protocol health monitoring: Aave V3, Pendle, Curve."""

from __future__ import annotations

from datetime import datetime, timezone

from src.collectors.base import BaseCollector, CollectedItem, CollectionResult


class LendingHealthCollector(BaseCollector):
    """Monitor lending protocol health across Aave V3, Pendle, and Curve.

    Alert thresholds:
        - Utilization > 85%: high severity
        - Utilization > 92%: critical severity
    """

    source_id = "lending_health"
    source_name = "Lending Protocol Health"
    source_type = "api"

    AAVE_V3_URL = "https://aave-api-v3.aave.com/data/markets"
    PENDLE_URL = "https://api-v2.pendle.finance/core/v1/sdk/1/markets"
    CURVE_URL = "https://api.curve.fi/v1/getPools/all/ethereum"

    # Utilization alert thresholds
    HIGH_UTILIZATION = 0.85
    CRITICAL_UTILIZATION = 0.92

    async def _collect(self) -> CollectionResult:
        items: list[CollectedItem] = []

        await self._collect_aave_v3(items)
        await self._collect_pendle(items)
        await self._collect_curve(items)

        return CollectionResult(
            source_id=self.source_id,
            source_name=self.source_name,
            source_type=self.source_type,
            items=items,
        )

    # ------------------------------------------------------------------
    # Aave V3
    # ------------------------------------------------------------------

    async def _collect_aave_v3(self, items: list[CollectedItem]) -> None:
        """Fetch Aave V3 market data and flag high utilization rates."""
        try:
            data = await self._fetch_json(self.AAVE_V3_URL)
            # The Aave API may return a list of reserves or a dict with a
            # "reserves" key depending on the version.
            reserves = data if isinstance(data, list) else data.get("reserves", data.get("data", []))
            if not isinstance(reserves, list):
                self.log.warning("aave_unexpected_format", data_type=type(reserves).__name__)
                return

            for reserve in reserves:
                symbol = reserve.get("symbol", "UNKNOWN")
                utilization = self._parse_float(reserve.get("utilizationRate") or reserve.get("borrowUsageRatio"))
                total_liquidity = self._parse_float(reserve.get("totalLiquidityUSD") or reserve.get("totalLiquidity"))
                supply_apy = self._parse_float(reserve.get("supplyAPY") or reserve.get("liquidityRate"))
                borrow_apy = self._parse_float(reserve.get("variableBorrowAPY") or reserve.get("variableBorrowRate"))

                severity = self._utilization_severity(utilization)
                alert_prefix = ""
                if severity == "critical":
                    alert_prefix = "[CRITICAL] "
                elif severity == "high":
                    alert_prefix = "[HIGH] "

                title = (
                    f"{alert_prefix}Aave V3 {symbol} — "
                    f"Utilization: {utilization * 100:.1f}%, "
                    f"Supply APY: {supply_apy * 100:.2f}%"
                )

                items.append(
                    CollectedItem(
                        id=f"lending_aave_{symbol}",
                        title=title,
                        content=f"Borrow APY: {borrow_apy * 100:.2f}%, Liquidity: ${total_liquidity:,.0f}",
                        url="https://app.aave.com/markets/",
                        published_at=datetime.now(timezone.utc),
                        metadata={
                            "data_type": "lending_health",
                            "protocol": "aave_v3",
                            "symbol": symbol,
                            "utilization_rate": utilization,
                            "supply_apy": supply_apy,
                            "borrow_apy": borrow_apy,
                            "total_liquidity_usd": total_liquidity,
                            "severity": severity,
                            "category": "defi_protocol",
                        },
                        raw=reserve,
                    )
                )

        except Exception as e:
            self.log.warning("aave_v3_fetch_failed", error=str(e))

    # ------------------------------------------------------------------
    # Pendle
    # ------------------------------------------------------------------

    async def _collect_pendle(self, items: list[CollectedItem]) -> None:
        """Fetch Pendle market data: implied APY, PT discount."""
        try:
            data = await self._fetch_json(
                self.PENDLE_URL,
                params={"limit": "20", "order_by": "name:1"},
            )
            markets = data if isinstance(data, list) else data.get("results", data.get("markets", []))
            if not isinstance(markets, list):
                self.log.warning("pendle_unexpected_format", data_type=type(markets).__name__)
                return

            for market in markets:
                name = market.get("name") or market.get("symbol") or "Unknown"
                implied_apy = self._parse_float(market.get("impliedApy") or market.get("implied_apy"))
                pt_discount = self._parse_float(market.get("ptDiscount") or market.get("pt_discount"))
                underlying_apy = self._parse_float(market.get("underlyingApy") or market.get("underlying_apy"))
                liquidity = self._parse_float(market.get("liquidity") or market.get("tvl") or market.get("totalLiquidity"))
                expiry = market.get("expiry") or market.get("maturity") or ""

                market_id = market.get("address") or market.get("id") or name

                title = (
                    f"Pendle {name} — Implied APY: {implied_apy * 100:.2f}%, "
                    f"PT Discount: {pt_discount * 100:.2f}%"
                )

                items.append(
                    CollectedItem(
                        id=f"lending_pendle_{market_id}",
                        title=title,
                        content=f"Underlying APY: {underlying_apy * 100:.2f}%, Expiry: {expiry}",
                        url="https://app.pendle.finance/trade/markets",
                        published_at=datetime.now(timezone.utc),
                        metadata={
                            "data_type": "lending_health",
                            "protocol": "pendle",
                            "name": name,
                            "implied_apy": implied_apy,
                            "pt_discount": pt_discount,
                            "underlying_apy": underlying_apy,
                            "liquidity": liquidity,
                            "expiry": expiry,
                            "category": "defi_protocol",
                        },
                        raw=market,
                    )
                )

        except Exception as e:
            self.log.warning("pendle_fetch_failed", error=str(e))

    # ------------------------------------------------------------------
    # Curve
    # ------------------------------------------------------------------

    async def _collect_curve(self, items: list[CollectedItem]) -> None:
        """Fetch Curve pool data and flag virtual price deviations."""
        try:
            data = await self._fetch_json(self.CURVE_URL)
            pool_data = data.get("data", data) if isinstance(data, dict) else data
            pools = pool_data.get("poolData", pool_data) if isinstance(pool_data, dict) else pool_data
            if not isinstance(pools, list):
                self.log.warning("curve_unexpected_format", data_type=type(pools).__name__)
                return

            for pool in pools:
                name = pool.get("name") or pool.get("id") or "Unknown"
                pool_id = pool.get("id") or pool.get("address") or name
                virtual_price = self._parse_float(pool.get("virtualPrice"))
                tvl = self._parse_float(pool.get("usdTotal") or pool.get("tvlUsd") or pool.get("tvl"))
                volume_24h = self._parse_float(pool.get("volume") or pool.get("tradingVolume24h"))
                base_apy = self._parse_float(pool.get("gaugeCrvApy") or pool.get("baseApy"))

                # Virtual price deviation: should be ~1.0 for stableswap,
                # significant deviation indicates imbalance
                vp_deviation = abs(virtual_price - 1.0) if virtual_price > 0 else 0
                vp_severity = "normal"
                if vp_deviation > 0.05:
                    vp_severity = "critical"
                elif vp_deviation > 0.02:
                    vp_severity = "high"
                elif vp_deviation > 0.01:
                    vp_severity = "elevated"

                alert_prefix = ""
                if vp_severity == "critical":
                    alert_prefix = "[VP-CRITICAL] "
                elif vp_severity == "high":
                    alert_prefix = "[VP-HIGH] "

                title = (
                    f"{alert_prefix}Curve {name} — "
                    f"VP: {virtual_price:.6f}, TVL: ${tvl:,.0f}"
                )

                items.append(
                    CollectedItem(
                        id=f"lending_curve_{pool_id}",
                        title=title,
                        content=f"24h Vol: ${volume_24h:,.0f}, Base APY: {base_apy:.2f}%",
                        url=f"https://curve.fi/#/ethereum/pools/{pool_id}",
                        published_at=datetime.now(timezone.utc),
                        metadata={
                            "data_type": "lending_health",
                            "protocol": "curve",
                            "pool_name": name,
                            "virtual_price": virtual_price,
                            "vp_deviation": vp_deviation,
                            "vp_severity": vp_severity,
                            "tvl_usd": tvl,
                            "volume_24h": volume_24h,
                            "base_apy": base_apy,
                            "category": "defi_protocol",
                        },
                        raw=pool,
                    )
                )

        except Exception as e:
            self.log.warning("curve_fetch_failed", error=str(e))

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_float(value) -> float:
        """Safely parse a numeric value to float."""
        if value is None:
            return 0.0
        try:
            return float(value)
        except (ValueError, TypeError):
            return 0.0

    def _utilization_severity(self, utilization: float) -> str:
        """Classify utilization into severity levels."""
        if utilization >= self.CRITICAL_UTILIZATION:
            return "critical"
        elif utilization >= self.HIGH_UTILIZATION:
            return "high"
        return "normal"
