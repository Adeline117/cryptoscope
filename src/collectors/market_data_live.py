"""Live market data collector using yfinance.

Tracks key TradFi instruments that correlate with crypto markets:
- DXY (Dollar Index) - inverse correlation with BTC
- VIX (Volatility Index) - risk sentiment
- Gold, Oil - macro commodities
- S&P 500, 10Y yield - risk-on/off indicators
- MSTR, COIN, IBIT - crypto-adjacent equities

Flags significant moves that may impact crypto.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from src.collectors.base import BaseCollector, CollectedItem, CollectionResult

# Symbols to track with human-readable names and categories
TRACKED_SYMBOLS: dict[str, dict[str, str]] = {
    "DX-Y.NYB": {"name": "US Dollar Index (DXY)", "category": "fx"},
    "^VIX": {"name": "VIX (CBOE Volatility)", "category": "volatility"},
    "GC=F": {"name": "Gold Futures", "category": "commodities"},
    "CL=F": {"name": "Crude Oil (WTI) Futures", "category": "commodities"},
    "^GSPC": {"name": "S&P 500", "category": "equity"},
    "^TNX": {"name": "10-Year Treasury Yield", "category": "rates"},
    "MSTR": {"name": "MicroStrategy", "category": "crypto_stocks"},
    "COIN": {"name": "Coinbase", "category": "crypto_stocks"},
    "IBIT": {"name": "iShares Bitcoin Trust ETF", "category": "crypto_etf"},
}

# Alert thresholds for significant moves
ALERT_THRESHOLDS: dict[str, dict[str, Any]] = {
    "DX-Y.NYB": {"pct_24h": 1.0, "description": "DXY move > 1% in 24h (BTC inverse correlation)"},
    "^VIX": {"absolute": 30.0, "description": "VIX above 30 (extreme fear)"},
    "CL=F": {"pct_24h": 10.0, "description": "Oil move > 10% in 24h (macro shock)"},
}


class MarketDataLiveCollector(BaseCollector):
    """Collect live market data via yfinance for crypto-relevant TradFi instruments."""

    source_id = "market_data_live"
    source_name = "Live Market Data (yfinance)"
    source_type = "api"

    def __init__(self, symbols: list[str] | None = None, **kwargs):
        super().__init__(cache_ttl=300, **kwargs)  # 5 min cache
        self.symbols = symbols or list(TRACKED_SYMBOLS.keys())

    def _fetch_symbol_data(self, symbol: str) -> CollectedItem | None:
        """Fetch 2 days of hourly data for a symbol and calculate 24h change.

        Uses yfinance synchronously (it wraps requests internally).
        Returns None on failure.
        """
        try:
            import yfinance as yf
        except ImportError:
            self.log.error("yfinance_not_installed", hint="pip install yfinance")
            return None

        try:
            ticker = yf.Ticker(symbol)
            # Fetch 2 days of hourly data
            hist = ticker.history(period="2d", interval="1h")

            if hist.empty or len(hist) < 2:
                self.log.debug("no_data", symbol=symbol)
                return None

            current_price = float(hist["Close"].iloc[-1])
            open_24h_ago = float(hist["Close"].iloc[0])

            change_24h = current_price - open_24h_ago
            change_24h_pct = (change_24h / abs(open_24h_ago) * 100) if open_24h_ago != 0 else 0.0

            high_24h = float(hist["High"].max())
            low_24h = float(hist["Low"].min())
            volume_24h = float(hist["Volume"].sum()) if "Volume" in hist.columns else 0.0

            meta_info = TRACKED_SYMBOLS.get(symbol, {"name": symbol, "category": "other"})
            now = datetime.now(timezone.utc)

            # Check for alerts
            alerts: list[str] = []
            threshold = ALERT_THRESHOLDS.get(symbol)
            if threshold:
                if "pct_24h" in threshold and abs(change_24h_pct) >= threshold["pct_24h"]:
                    alerts.append(threshold["description"])
                if "absolute" in threshold and current_price >= threshold["absolute"]:
                    alerts.append(threshold["description"])

            priority = "critical" if alerts else "high" if meta_info["category"] in ("fx", "volatility") else "medium"

            return CollectedItem(
                id=f"mktlive_{symbol}_{now.strftime('%Y%m%d_%H')}",
                title=f"{meta_info['name']}: {current_price:,.2f} ({change_24h_pct:+.2f}%)",
                content=(
                    f"Price: {current_price:,.2f} | "
                    f"24h Change: {change_24h:+,.2f} ({change_24h_pct:+.2f}%) | "
                    f"24h High: {high_24h:,.2f} | 24h Low: {low_24h:,.2f}"
                    + (f" | ALERTS: {'; '.join(alerts)}" if alerts else "")
                ),
                url=f"https://finance.yahoo.com/quote/{symbol}",
                published_at=now,
                metadata={
                    "data_type": "market_data_live",
                    "symbol": symbol,
                    "name": meta_info["name"],
                    "category": meta_info["category"],
                    "price": current_price,
                    "change_24h": change_24h,
                    "change_24h_pct": change_24h_pct,
                    "high_24h": high_24h,
                    "low_24h": low_24h,
                    "volume_24h": volume_24h,
                    "alerts": alerts,
                    "priority": priority,
                },
                raw={
                    "symbol": symbol,
                    "price": current_price,
                    "open_24h_ago": open_24h_ago,
                    "high_24h": high_24h,
                    "low_24h": low_24h,
                    "volume_24h": volume_24h,
                },
            )
        except Exception as e:
            self.log.warning("symbol_fetch_error", symbol=symbol, error=str(e))
            return None

    async def _collect(self) -> CollectionResult:
        import asyncio

        items: list[CollectedItem] = []

        # yfinance is synchronous; run each symbol fetch in a thread pool
        loop = asyncio.get_event_loop()
        tasks = [
            loop.run_in_executor(None, self._fetch_symbol_data, symbol)
            for symbol in self.symbols
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, CollectedItem):
                items.append(result)
            elif isinstance(result, Exception):
                self.log.warning("symbol_exception", error=str(result))

        # Generate summary item if we have data
        if items:
            alerts_summary = []
            for item in items:
                if item.metadata.get("alerts"):
                    alerts_summary.extend(item.metadata["alerts"])

            if alerts_summary:
                now = datetime.now(timezone.utc)
                items.append(CollectedItem(
                    id=f"mktlive_alerts_{now.strftime('%Y%m%d_%H')}",
                    title=f"Market Alerts: {len(alerts_summary)} triggered",
                    content=" | ".join(alerts_summary),
                    url="",
                    published_at=now,
                    metadata={
                        "data_type": "market_alerts",
                        "category": "alerts",
                        "alert_count": len(alerts_summary),
                        "alerts": alerts_summary,
                        "priority": "critical",
                    },
                    raw={"alerts": alerts_summary},
                ))

        return CollectionResult(
            source_id=self.source_id,
            source_name=self.source_name,
            source_type=self.source_type,
            items=items,
        )
