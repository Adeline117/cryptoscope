"""Macro economic data collector: FRED API, economic calendar, market data.

Collects and processes macroeconomic indicators most relevant to crypto markets:
- Fed policy (rates, balance sheet, net liquidity)
- Inflation data (CPI, PCE, PPI)
- Employment (NFP, claims)
- Treasury yields and yield curve
- USD/DXY, VIX, equity indices
- Global liquidity (M2, central bank balance sheets)
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

from src.collectors.base import BaseCollector, CollectedItem, CollectionResult

# Key FRED series to monitor — mapped to human-readable names
FRED_SERIES: dict[str, dict[str, str]] = {
    # Inflation
    "CPIAUCSL": {"name": "CPI (All Urban Consumers)", "category": "inflation"},
    "CPILFESL": {"name": "Core CPI (ex Food & Energy)", "category": "inflation"},
    "PCEPI": {"name": "PCE Price Index", "category": "inflation"},
    "PCEPILFE": {"name": "Core PCE (Fed's preferred)", "category": "inflation"},
    "PPIACO": {"name": "PPI (All Commodities)", "category": "inflation"},
    "T5YIE": {"name": "5-Year Breakeven Inflation", "category": "inflation"},
    "T10YIE": {"name": "10-Year Breakeven Inflation", "category": "inflation"},
    # Employment
    "PAYEMS": {"name": "Non-Farm Payrolls", "category": "employment"},
    "UNRATE": {"name": "Unemployment Rate", "category": "employment"},
    "ICSA": {"name": "Initial Jobless Claims", "category": "employment"},
    "CCSA": {"name": "Continuing Claims", "category": "employment"},
    "JTSJOL": {"name": "JOLTS Job Openings", "category": "employment"},
    # GDP & Activity
    "GDP": {"name": "GDP (Nominal)", "category": "gdp"},
    "GDPC1": {"name": "Real GDP", "category": "gdp"},
    "RSXFS": {"name": "Retail Sales (ex Food Services)", "category": "activity"},
    "INDPRO": {"name": "Industrial Production", "category": "activity"},
    # Rates & Yields
    "FEDFUNDS": {"name": "Federal Funds Rate", "category": "rates"},
    "DGS2": {"name": "2-Year Treasury Yield", "category": "rates"},
    "DGS10": {"name": "10-Year Treasury Yield", "category": "rates"},
    "DGS30": {"name": "30-Year Treasury Yield", "category": "rates"},
    "T10Y2Y": {"name": "Yield Curve (10Y-2Y)", "category": "rates"},
    # Money & Liquidity
    "M2SL": {"name": "M2 Money Supply", "category": "liquidity"},
    "WALCL": {"name": "Fed Balance Sheet (Total Assets)", "category": "liquidity"},
    "WTREGEN": {"name": "Treasury General Account (TGA)", "category": "liquidity"},
    "RRPONTSYD": {"name": "Reverse Repo (ON RRP)", "category": "liquidity"},
    # Financial Conditions
    "DTWEXBGS": {"name": "Trade-Weighted USD Index (DXY proxy)", "category": "fx"},
    "VIXCLS": {"name": "VIX (CBOE Volatility)", "category": "volatility"},
    "NFCI": {"name": "Chicago Fed NFCI", "category": "financial_conditions"},
    "BAMLH0A0HYM2": {"name": "HY Credit Spread (OAS)", "category": "credit"},
    # Housing
    "MORTGAGE30US": {"name": "30-Year Mortgage Rate", "category": "housing"},
}

# Net Liquidity formula components
NET_LIQUIDITY_COMPONENTS = ["WALCL", "WTREGEN", "RRPONTSYD"]


class FREDCollector(BaseCollector):
    """Collect macroeconomic data from the FRED API."""

    source_id = "fred"
    source_name = "FRED Economic Data"
    source_type = "api"

    BASE_URL = "https://api.stlouisfed.org/fred"

    def __init__(self, series_ids: list[str] | None = None, **kwargs):
        super().__init__(cache_ttl=3600, **kwargs)
        self.api_key = os.environ.get("FRED_API_KEY", "")
        self.series_ids = series_ids or list(FRED_SERIES.keys())

    async def _fetch_series(self, series_id: str) -> CollectedItem | None:
        """Fetch the latest observation for a FRED series."""
        if not self.api_key:
            return None

        try:
            data = await self._fetch_json(
                f"{self.BASE_URL}/series/observations",
                params={
                    "series_id": series_id,
                    "api_key": self.api_key,
                    "file_type": "json",
                    "sort_order": "desc",
                    "limit": 5,
                },
            )

            observations = data.get("observations", [])
            if not observations:
                return None

            latest = observations[0]
            previous = observations[1] if len(observations) > 1 else None

            value = latest.get("value", ".")
            if value == ".":
                return None

            current_val = float(value)
            prev_val = float(previous["value"]) if previous and previous.get("value", ".") != "." else None

            meta = FRED_SERIES.get(series_id, {"name": series_id, "category": "other"})
            change = (current_val - prev_val) if prev_val is not None else None
            change_pct = ((current_val - prev_val) / abs(prev_val) * 100) if prev_val else None

            return CollectedItem(
                id=f"fred_{series_id}_{latest['date']}",
                title=f"{meta['name']}: {current_val:,.2f} ({latest['date']})",
                content=(
                    f"Current: {current_val:,.2f} | "
                    f"Previous: {prev_val:,.2f} | "
                    f"Change: {change:+,.2f} ({change_pct:+.2f}%)"
                    if prev_val is not None else f"Current: {current_val:,.2f}"
                ),
                url=f"https://fred.stlouisfed.org/series/{series_id}",
                published_at=datetime.fromisoformat(latest["date"]).replace(tzinfo=timezone.utc),
                metadata={
                    "data_type": "fred_series",
                    "series_id": series_id,
                    "series_name": meta["name"],
                    "category": meta["category"],
                    "value": current_val,
                    "previous_value": prev_val,
                    "change": change,
                    "change_pct": change_pct,
                    "date": latest["date"],
                    "priority": "critical" if meta["category"] in ("inflation", "rates", "liquidity") else "high",
                },
                raw=latest,
            )
        except Exception as e:
            self.log.debug("fred_series_error", series=series_id, error=str(e))
            return None

    async def _collect(self) -> CollectionResult:
        import asyncio

        if not self.api_key:
            self.log.warning("no_fred_api_key")
            return CollectionResult(
                source_id=self.source_id,
                source_name=self.source_name,
                source_type=self.source_type,
            )

        tasks = [self._fetch_series(sid) for sid in self.series_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        items = []
        for result in results:
            if isinstance(result, CollectedItem):
                items.append(result)

        # Calculate Net Liquidity if we have the components
        net_liq = self._calculate_net_liquidity(items)
        if net_liq:
            items.append(net_liq)

        return CollectionResult(
            source_id=self.source_id,
            source_name=self.source_name,
            source_type=self.source_type,
            items=items,
        )

    def _calculate_net_liquidity(self, items: list[CollectedItem]) -> CollectedItem | None:
        """Calculate Net Liquidity = Fed BS - TGA - Reverse Repo."""
        values = {}
        for item in items:
            sid = item.metadata.get("series_id")
            if sid in NET_LIQUIDITY_COMPONENTS:
                values[sid] = item.metadata.get("value")

        if len(values) != 3:
            return None

        # All values are in millions of dollars
        walcl = values["WALCL"]
        tga = values["WTREGEN"]
        rrp = values["RRPONTSYD"]
        net_liq = walcl - tga - rrp

        return CollectedItem(
            id=f"fred_net_liquidity_{datetime.now(timezone.utc).strftime('%Y%m%d')}",
            title=f"Net Liquidity: ${net_liq/1e6:,.2f}T",
            content=(
                f"Fed BS: ${walcl/1e6:,.2f}T | "
                f"TGA: ${tga/1e3:,.0f}B | "
                f"RRP: ${rrp/1e3:,.0f}B | "
                f"Net: ${net_liq/1e6:,.2f}T"
            ),
            url="https://fred.stlouisfed.org",
            published_at=datetime.now(timezone.utc),
            metadata={
                "data_type": "net_liquidity",
                "category": "liquidity",
                "value": net_liq,
                "components": {
                    "fed_bs": walcl,
                    "tga": tga,
                    "rrp": rrp,
                },
                "priority": "critical",
            },
            raw=values,
        )


class MarketDataCollector(BaseCollector):
    """Collect financial market data: equities, FX, commodities, crypto-TradFi."""

    source_id = "market_data"
    source_name = "Financial Market Data"
    source_type = "api"

    # Key market symbols to track
    SYMBOLS = {
        "equity": {
            "^GSPC": "S&P 500",
            "^IXIC": "Nasdaq Composite",
            "^DJI": "Dow Jones",
            "^RUT": "Russell 2000",
        },
        "volatility": {
            "^VIX": "VIX",
        },
        "crypto_stocks": {
            "COIN": "Coinbase",
            "MSTR": "MicroStrategy",
            "MARA": "Marathon Digital",
            "IBIT": "iShares Bitcoin Trust ETF",
        },
    }

    async def _collect(self) -> CollectionResult:
        """Collect market data. Uses Yahoo Finance as fallback data source."""
        items = []

        # Try to collect from a free API
        try:
            # Use DXY proxy from FRED if available, otherwise skip
            # Real implementation would use yfinance or Alpha Vantage
            self.log.info("market_data_collection", note="Requires yfinance or Alpha Vantage API")
        except Exception as e:
            self.log.warning("market_data_error", error=str(e))

        return CollectionResult(
            source_id=self.source_id,
            source_name=self.source_name,
            source_type=self.source_type,
            items=items,
        )
