"""Order book depth and bid/ask imbalance collector using CCXT."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import ccxt.async_support as ccxt

from src.collectors.base import BaseCollector, CollectedItem, CollectionResult


class OrderBookDepthCollector(BaseCollector):
    """Collect order book depth data and calculate bid/ask imbalance.

    Uses CCXT to fetch order books from exchanges (default: Binance) for
    major trading pairs. Detects strong buy/sell pressure based on
    order book imbalance.

    No API key required for public order book data.
    Requires: pip install ccxt
    """

    source_id = "orderbook_depth"
    source_name = "Order Book Depth Analyzer"
    source_type = "api"

    # Trading pairs to monitor
    SYMBOLS = ["BTC/USDT", "ETH/USDT", "SOL/USDT"]

    # Order book depth (number of price levels)
    ORDER_BOOK_LIMIT = 50

    # Imbalance thresholds
    STRONG_BUY_THRESHOLD = 0.3
    STRONG_SELL_THRESHOLD = -0.3

    def __init__(
        self,
        symbols: list[str] | None = None,
        exchange_id: str = "binance",
        order_book_limit: int = 50,
        **kwargs,
    ):
        """Initialize with configurable parameters.

        Args:
            symbols: Trading pairs to monitor (default: SYMBOLS).
            exchange_id: CCXT exchange identifier (default: "binance").
            order_book_limit: Number of price levels to fetch (default: 50).
        """
        super().__init__(cache_ttl=60, **kwargs)  # 1 min cache (order books change fast)
        self.symbols = symbols or self.SYMBOLS
        self.exchange_id = exchange_id
        self.order_book_limit = order_book_limit

    def _calculate_imbalance(
        self, bids: list[list[float]], asks: list[list[float]]
    ) -> dict[str, Any]:
        """Calculate bid/ask imbalance and related metrics.

        Args:
            bids: List of [price, amount] pairs for bids.
            asks: List of [price, amount] pairs for asks.

        Returns:
            Dictionary with imbalance metrics.
        """
        total_bid_volume = sum(amount for _price, amount in bids)
        total_ask_volume = sum(amount for _price, amount in asks)
        total_bid_value = sum(price * amount for price, amount in bids)
        total_ask_value = sum(price * amount for price, amount in asks)

        total = total_bid_volume + total_ask_volume
        imbalance = (total_bid_volume - total_ask_volume) / total if total > 0 else 0.0

        # Best bid/ask (top of book)
        best_bid = bids[0][0] if bids else 0.0
        best_ask = asks[0][0] if asks else 0.0
        spread = best_ask - best_bid if best_bid > 0 and best_ask > 0 else 0.0
        spread_pct = (spread / best_bid * 100) if best_bid > 0 else 0.0
        mid_price = (best_bid + best_ask) / 2 if best_bid > 0 and best_ask > 0 else 0.0

        # Wall detection: find largest single order in top 10 levels
        top_bids = bids[:10]
        top_asks = asks[:10]
        max_bid_wall = max((amount for _p, amount in top_bids), default=0)
        max_ask_wall = max((amount for _p, amount in top_asks), default=0)

        return {
            "imbalance": round(imbalance, 4),
            "total_bid_volume": round(total_bid_volume, 4),
            "total_ask_volume": round(total_ask_volume, 4),
            "total_bid_value_usd": round(total_bid_value, 2),
            "total_ask_value_usd": round(total_ask_value, 2),
            "best_bid": best_bid,
            "best_ask": best_ask,
            "mid_price": round(mid_price, 2),
            "spread": round(spread, 6),
            "spread_pct": round(spread_pct, 4),
            "max_bid_wall": round(max_bid_wall, 4),
            "max_ask_wall": round(max_ask_wall, 4),
            "depth_levels": len(bids),
        }

    async def _collect(self) -> CollectionResult:
        """Fetch order books and calculate imbalance for each symbol."""
        exchange = getattr(ccxt, self.exchange_id)()

        try:
            items: list[CollectedItem] = []

            for symbol in self.symbols:
                try:
                    book = await exchange.fetch_order_book(
                        symbol, limit=self.order_book_limit
                    )

                    bids = book.get("bids", [])
                    asks = book.get("asks", [])

                    if not bids or not asks:
                        self.log.warning("empty_order_book", symbol=symbol)
                        continue

                    metrics = self._calculate_imbalance(bids, asks)
                    imbalance = metrics["imbalance"]

                    # Determine signal
                    if imbalance >= self.STRONG_BUY_THRESHOLD:
                        signal = "strong_buy_pressure"
                        signal_desc = f"Strong buy pressure (imbalance: {imbalance:+.2%})"
                        priority = "high"
                    elif imbalance <= self.STRONG_SELL_THRESHOLD:
                        signal = "strong_sell_pressure"
                        signal_desc = f"Strong sell pressure (imbalance: {imbalance:+.2%})"
                        priority = "high"
                    elif imbalance > 0.1:
                        signal = "moderate_buy_pressure"
                        signal_desc = f"Moderate buy pressure (imbalance: {imbalance:+.2%})"
                        priority = "medium"
                    elif imbalance < -0.1:
                        signal = "moderate_sell_pressure"
                        signal_desc = f"Moderate sell pressure (imbalance: {imbalance:+.2%})"
                        priority = "medium"
                    else:
                        signal = "balanced"
                        signal_desc = f"Balanced order book (imbalance: {imbalance:+.2%})"
                        priority = "low"

                    is_anomaly = abs(imbalance) >= self.STRONG_BUY_THRESHOLD

                    metadata: dict[str, Any] = {
                        "data_type": "orderbook_depth",
                        "symbol": symbol,
                        "exchange": self.exchange_id,
                        **metrics,
                        "signal": signal,
                        "signal_description": signal_desc,
                        "is_anomaly": is_anomaly,
                        "priority": priority,
                    }

                    # Build content
                    content_parts = [
                        f"{symbol} @ {self.exchange_id.capitalize()}",
                        f"Mid: ${metrics['mid_price']:,.2f} | Spread: {metrics['spread_pct']:.4f}%",
                        f"Bid Vol: {metrics['total_bid_volume']:,.4f} | Ask Vol: {metrics['total_ask_volume']:,.4f}",
                        f"Bid Value: ${metrics['total_bid_value_usd']:,.0f} | Ask Value: ${metrics['total_ask_value_usd']:,.0f}",
                        f"Imbalance: {imbalance:+.2%}",
                        signal_desc,
                    ]

                    if metrics["max_bid_wall"] > metrics["total_bid_volume"] * 0.2:
                        content_parts.append(
                            f"BID WALL: {metrics['max_bid_wall']:,.4f} units at top 10 levels"
                        )
                    if metrics["max_ask_wall"] > metrics["total_ask_volume"] * 0.2:
                        content_parts.append(
                            f"ASK WALL: {metrics['max_ask_wall']:,.4f} units at top 10 levels"
                        )

                    now = datetime.now(timezone.utc)
                    date_str = now.strftime("%Y%m%d_%H%M")

                    items.append(
                        CollectedItem(
                            id=f"ob_{symbol.replace('/', '_')}_{date_str}",
                            title=f"Order Book: {symbol} — {signal_desc}",
                            content=" | ".join(content_parts),
                            url=f"https://www.binance.com/en/trade/{symbol.replace('/', '_')}",
                            published_at=now,
                            metadata=metadata,
                            raw={
                                "symbol": symbol,
                                "bids_count": len(bids),
                                "asks_count": len(asks),
                                "top_5_bids": bids[:5],
                                "top_5_asks": asks[:5],
                            },
                        )
                    )

                except Exception as e:
                    self.log.warning(
                        "orderbook_fetch_failed",
                        symbol=symbol,
                        error=str(e),
                    )

            self.log.info(
                "orderbook_collected",
                total=len(items),
                anomalies=sum(1 for it in items if it.metadata.get("is_anomaly")),
                symbols=len(self.symbols),
            )

            return CollectionResult(
                source_id=self.source_id,
                source_name=self.source_name,
                source_type=self.source_type,
                items=items,
            )

        finally:
            await exchange.close()
