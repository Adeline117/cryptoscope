"""CoinGecko price tracker: market data, global stats, and trending coins."""

from __future__ import annotations

from datetime import datetime, timezone

from src.collectors.base import BaseCollector, CollectedItem, CollectionResult


class CoinGeckoCollector(BaseCollector):
    """Collect market prices, global data, and trending coins from the free CoinGecko API (no key needed)."""

    source_id = "coingecko"
    source_name = "CoinGecko"
    source_type = "api"

    BASE_URL = "https://api.coingecko.com/api/v3"

    # Threshold for flagging high-priority movers
    HIGH_CHANGE_THRESHOLD = 10.0  # percent

    async def _collect(self) -> CollectionResult:
        items: list[CollectedItem] = []

        # 1. Top 100 coins by market cap
        try:
            markets = await self._fetch_json(
                f"{self.BASE_URL}/coins/markets",
                params={
                    "vs_currency": "usd",
                    "order": "market_cap_desc",
                    "per_page": 100,
                    "page": 1,
                    "sparkline": "false",
                    "price_change_percentage": "1h,24h,7d",
                },
            )
            for coin in markets:
                change_24h = coin.get("price_change_percentage_24h") or 0
                change_1h = coin.get("price_change_percentage_1h_in_currency") or 0
                change_7d = coin.get("price_change_percentage_7d_in_currency") or 0

                is_significant_mover = abs(change_24h) >= self.HIGH_CHANGE_THRESHOLD
                priority = "high" if is_significant_mover else "medium"

                items.append(
                    CollectedItem(
                        id=f"coingecko_market_{coin.get('id', '')}",
                        title=f"{coin.get('name', 'Unknown')} ({coin.get('symbol', '').upper()}) — ${coin.get('current_price', 0):,.2f}",
                        content=f"24h change: {change_24h:+.2f}% | MCap: ${coin.get('market_cap', 0):,.0f}",
                        url=f"https://www.coingecko.com/en/coins/{coin.get('id', '')}",
                        published_at=datetime.now(timezone.utc),
                        metadata={
                            "data_type": "market_price",
                            "coin_id": coin.get("id"),
                            "symbol": coin.get("symbol"),
                            "name": coin.get("name"),
                            "price": coin.get("current_price"),
                            "market_cap": coin.get("market_cap"),
                            "market_cap_rank": coin.get("market_cap_rank"),
                            "volume_24h": coin.get("total_volume"),
                            "price_change_1h": change_1h,
                            "price_change_24h": change_24h,
                            "price_change_7d": change_7d,
                            "high_24h": coin.get("high_24h"),
                            "low_24h": coin.get("low_24h"),
                            "ath": coin.get("ath"),
                            "ath_change_percentage": coin.get("ath_change_percentage"),
                            "circulating_supply": coin.get("circulating_supply"),
                            "total_supply": coin.get("total_supply"),
                            "priority": priority,
                            "significant_mover": is_significant_mover,
                        },
                        raw=coin,
                    )
                )
        except Exception as e:
            self.log.warning("coingecko_markets_failed", error=str(e))

        # 2. Global market data
        try:
            global_data = await self._fetch_json(f"{self.BASE_URL}/global")
            data = global_data.get("data", {})
            items.append(
                CollectedItem(
                    id="coingecko_global_market",
                    title=f"Global Crypto Market — Cap: ${data.get('total_market_cap', {}).get('usd', 0):,.0f}",
                    content=f"BTC dominance: {data.get('market_cap_percentage', {}).get('btc', 0):.1f}%",
                    url="https://www.coingecko.com/en/global-charts",
                    published_at=datetime.now(timezone.utc),
                    metadata={
                        "data_type": "global_market",
                        "total_market_cap_usd": data.get("total_market_cap", {}).get("usd"),
                        "total_volume_24h_usd": data.get("total_volume", {}).get("usd"),
                        "btc_dominance": data.get("market_cap_percentage", {}).get("btc"),
                        "eth_dominance": data.get("market_cap_percentage", {}).get("eth"),
                        "active_cryptocurrencies": data.get("active_cryptocurrencies"),
                        "markets": data.get("markets"),
                        "market_cap_change_24h_pct": data.get("market_cap_change_percentage_24h_usd"),
                    },
                    raw=data,
                )
            )
        except Exception as e:
            self.log.warning("coingecko_global_failed", error=str(e))

        # 3. Trending coins
        try:
            trending = await self._fetch_json(f"{self.BASE_URL}/search/trending")
            for idx, entry in enumerate(trending.get("coins", [])):
                coin = entry.get("item", {})
                items.append(
                    CollectedItem(
                        id=f"coingecko_trending_{coin.get('id', idx)}",
                        title=f"Trending #{idx + 1}: {coin.get('name', 'Unknown')} ({coin.get('symbol', '').upper()})",
                        content=f"Market cap rank: {coin.get('market_cap_rank', 'N/A')}",
                        url=f"https://www.coingecko.com/en/coins/{coin.get('id', '')}",
                        published_at=datetime.now(timezone.utc),
                        metadata={
                            "data_type": "trending_coin",
                            "coin_id": coin.get("id"),
                            "symbol": coin.get("symbol"),
                            "name": coin.get("name"),
                            "market_cap_rank": coin.get("market_cap_rank"),
                            "score": coin.get("score"),
                            "price_btc": coin.get("price_btc"),
                            "trending_rank": idx + 1,
                        },
                        raw=coin,
                    )
                )
        except Exception as e:
            self.log.warning("coingecko_trending_failed", error=str(e))

        return CollectionResult(
            source_id=self.source_id,
            source_name=self.source_name,
            source_type=self.source_type,
            items=items,
        )
