"""On-chain data collector: DeFiLlama, Dune, Etherscan, CoinGecko."""

from __future__ import annotations

import hashlib
import os
from datetime import datetime, timezone

from src.collectors.base import BaseCollector, CollectedItem, CollectionResult


class DeFiLlamaCollector(BaseCollector):
    """Collect TVL, volume, yields, and stablecoin data from DeFiLlama (no API key needed)."""

    source_id = "defillama"
    source_name = "DeFiLlama"
    source_type = "api"

    BASE_URL = "https://api.llama.fi"
    STABLECOINS_URL = "https://stablecoins.llama.fi"
    YIELDS_URL = "https://yields.llama.fi"
    COINS_URL = "https://coins.llama.fi"

    async def _collect(self) -> CollectionResult:
        items = []

        # 1. Protocol TVL data
        protocols = await self._fetch_json(f"{self.BASE_URL}/protocols")
        top_protocols = sorted(protocols, key=lambda p: p.get("tvl") or 0, reverse=True)[:200]
        for p in top_protocols:
            items.append(
                CollectedItem(
                    id=f"defillama_protocol_{p.get('slug', p.get('name', ''))}",
                    title=f"{p.get('name', 'Unknown')} — TVL: ${p.get('tvl', 0):,.0f}",
                    content="",
                    url=f"https://defillama.com/protocol/{p.get('slug', '')}",
                    published_at=datetime.now(timezone.utc),
                    metadata={
                        "data_type": "protocol_tvl",
                        "name": p.get("name"),
                        "tvl": p.get("tvl", 0),
                        "tvl_change_1d": p.get("change_1d"),
                        "tvl_change_7d": p.get("change_7d"),
                        "chain": p.get("chain"),
                        "chains": p.get("chains", []),
                        "category": p.get("category"),
                        "mcap": p.get("mcap"),
                    },
                    raw=p,
                )
            )

        # 2. Chain TVL
        chains = await self._fetch_json(f"{self.BASE_URL}/v2/chains")
        for c in chains:
            items.append(
                CollectedItem(
                    id=f"defillama_chain_{c.get('name', '')}",
                    title=f"Chain {c.get('name', '')} — TVL: ${c.get('tvl', 0):,.0f}",
                    content="",
                    url=f"https://defillama.com/chain/{c.get('name', '')}",
                    published_at=datetime.now(timezone.utc),
                    metadata={
                        "data_type": "chain_tvl",
                        "name": c.get("name"),
                        "tvl": c.get("tvl", 0),
                    },
                    raw=c,
                )
            )

        # 3. Stablecoins overview
        try:
            stables = await self._fetch_json(f"{self.STABLECOINS_URL}/stablecoins?includePrices=true")
            for s in stables.get("peggedAssets", [])[:30]:
                items.append(
                    CollectedItem(
                        id=f"defillama_stable_{s.get('id', '')}",
                        title=f"Stablecoin: {s.get('name', '')} ({s.get('symbol', '')})",
                        content="",
                        url="https://defillama.com/stablecoins",
                        published_at=datetime.now(timezone.utc),
                        metadata={
                            "data_type": "stablecoin",
                            "name": s.get("name"),
                            "symbol": s.get("symbol"),
                            "peg_type": s.get("pegType"),
                            "circulating": s.get("circulating"),
                        },
                        raw=s,
                    )
                )
        except Exception as e:
            self.log.warning("stablecoins_fetch_failed", error=str(e))

        # 4. Top yield pools
        try:
            yields = await self._fetch_json(f"{self.YIELDS_URL}/pools")
            top_yields = sorted(
                yields.get("data", []),
                key=lambda y: y.get("tvlUsd", 0),
                reverse=True,
            )[:50]
            for y in top_yields:
                items.append(
                    CollectedItem(
                        id=f"defillama_yield_{y.get('pool', '')}",
                        title=f"Yield: {y.get('project', '')} {y.get('symbol', '')} — APY: {y.get('apy', 0):.2f}%",
                        content="",
                        url="https://defillama.com/yields",
                        published_at=datetime.now(timezone.utc),
                        metadata={
                            "data_type": "yield_pool",
                            "project": y.get("project"),
                            "chain": y.get("chain"),
                            "symbol": y.get("symbol"),
                            "tvl_usd": y.get("tvlUsd"),
                            "apy": y.get("apy"),
                            "apy_base": y.get("apyBase"),
                            "apy_reward": y.get("apyReward"),
                        },
                        raw=y,
                    )
                )
        except Exception as e:
            self.log.warning("yields_fetch_failed", error=str(e))

        # 5. DEX volume overview
        try:
            dex_data = await self._fetch_json(f"{self.BASE_URL}/overview/dexs")
            for d in dex_data.get("protocols", [])[:30]:
                items.append(
                    CollectedItem(
                        id=f"defillama_dex_{d.get('name', '')}",
                        title=f"DEX: {d.get('name', '')} — 24h Vol: ${d.get('total24h', 0):,.0f}",
                        content="",
                        url="https://defillama.com/dexs",
                        published_at=datetime.now(timezone.utc),
                        metadata={
                            "data_type": "dex_volume",
                            "name": d.get("name"),
                            "total_24h": d.get("total24h", 0),
                            "total_7d": d.get("total7d", 0),
                            "change_1d": d.get("change_1d"),
                            "chains": d.get("chains", []),
                        },
                        raw=d,
                    )
                )
        except Exception as e:
            self.log.warning("dex_volume_fetch_failed", error=str(e))

        # 6. Fees/revenue overview
        try:
            fees_data = await self._fetch_json(f"{self.BASE_URL}/overview/fees")
            for f_item in fees_data.get("protocols", [])[:30]:
                items.append(
                    CollectedItem(
                        id=f"defillama_fees_{f_item.get('name', '')}",
                        title=f"Fees: {f_item.get('name', '')} — 24h: ${f_item.get('total24h', 0):,.0f}",
                        content="",
                        url="https://defillama.com/fees",
                        published_at=datetime.now(timezone.utc),
                        metadata={
                            "data_type": "protocol_fees",
                            "name": f_item.get("name"),
                            "total_24h": f_item.get("total24h", 0),
                            "total_7d": f_item.get("total7d", 0),
                            "revenue_24h": f_item.get("dailyRevenue", 0),
                        },
                        raw=f_item,
                    )
                )
        except Exception as e:
            self.log.warning("fees_fetch_failed", error=str(e))

        # 7. Bridge volumes
        try:
            bridges = await self._fetch_json(f"{self.BASE_URL}/bridges")
            for b in (bridges.get("bridges", []) if isinstance(bridges, dict) else bridges)[:20]:
                items.append(
                    CollectedItem(
                        id=f"defillama_bridge_{b.get('id', '')}",
                        title=f"Bridge: {b.get('displayName', b.get('name', ''))}",
                        content="",
                        url="https://defillama.com/bridges",
                        published_at=datetime.now(timezone.utc),
                        metadata={
                            "data_type": "bridge",
                            "name": b.get("displayName", b.get("name")),
                            "chains": b.get("chains", []),
                        },
                        raw=b,
                    )
                )
        except Exception as e:
            self.log.warning("bridges_fetch_failed", error=str(e))

        # 8. Hacks database
        try:
            hacks = await self._fetch_json(f"{self.BASE_URL}/hacks")
            for h in (hacks if isinstance(hacks, list) else [])[:20]:
                items.append(
                    CollectedItem(
                        id=f"defillama_hack_{h.get('id', hashlib.md5(str(h).encode()).hexdigest()[:8])}",
                        title=f"Hack: {h.get('name', 'Unknown')} — ${h.get('amount', 0):,.0f}",
                        content=h.get("technique", ""),
                        url="https://defillama.com/hacks",
                        published_at=datetime.now(timezone.utc),
                        metadata={
                            "data_type": "hack",
                            "name": h.get("name"),
                            "amount": h.get("amount", 0),
                            "chain": h.get("chain"),
                            "technique": h.get("technique"),
                            "category": "security",
                        },
                        raw=h,
                    )
                )
        except Exception as e:
            self.log.warning("hacks_fetch_failed", error=str(e))

        # 9. Raises/fundraising
        try:
            raises = await self._fetch_json(f"{self.BASE_URL}/raises")
            for r in (raises.get("raises", []) if isinstance(raises, dict) else raises)[:30]:
                items.append(
                    CollectedItem(
                        id=f"defillama_raise_{r.get('name', '')}_{r.get('date', '')}",
                        title=f"Raise: {r.get('name', '')} — ${r.get('amount', 0):,.0f}M ({r.get('round', '')})",
                        content="",
                        url="https://defillama.com/raises",
                        published_at=datetime.now(timezone.utc),
                        metadata={
                            "data_type": "fundraise",
                            "name": r.get("name"),
                            "amount": r.get("amount", 0),
                            "round": r.get("round"),
                            "lead_investors": r.get("leadInvestors", []),
                            "chains": r.get("chains", []),
                            "sector": r.get("sector"),
                        },
                        raw=r,
                    )
                )
        except Exception as e:
            self.log.warning("raises_fetch_failed", error=str(e))

        return CollectionResult(
            source_id=self.source_id,
            source_name=self.source_name,
            source_type=self.source_type,
            items=items,
        )


class DuneCollector(BaseCollector):
    """Collect data from pre-saved Dune Analytics queries."""

    source_id = "dune"
    source_name = "Dune Analytics"
    source_type = "api"

    BASE_URL = "https://api.dune.com/api/v1"

    # Pre-configured query IDs for key dashboards
    DEFAULT_QUERIES: dict[str, int] = {
        # These are example query IDs — replace with your own saved queries
        # "eth_staking_overview": 2009862,
        # "l2_fees_comparison": 2444421,
        # "dex_volume_weekly": 1847009,
    }

    def __init__(self, query_ids: dict[str, int] | None = None, **kwargs):
        super().__init__(**kwargs)
        self.query_ids = query_ids or self.DEFAULT_QUERIES
        self.api_key = os.environ.get("DUNE_API_KEY", "")

    async def _collect(self) -> CollectionResult:
        if not self.api_key:
            self.log.warning("no_dune_api_key")
            return CollectionResult(
                source_id=self.source_id,
                source_name=self.source_name,
                source_type=self.source_type,
            )

        headers = {"X-Dune-API-Key": self.api_key}
        items = []

        for name, query_id in self.query_ids.items():
            try:
                data = await self._fetch_json(
                    f"{self.BASE_URL}/query/{query_id}/results",
                    headers=headers,
                )
                rows = data.get("result", {}).get("rows", [])
                items.append(
                    CollectedItem(
                        id=f"dune_{query_id}",
                        title=f"Dune Query: {name}",
                        content=f"{len(rows)} rows returned",
                        url=f"https://dune.com/queries/{query_id}",
                        published_at=datetime.now(timezone.utc),
                        metadata={
                            "data_type": "dune_query",
                            "query_name": name,
                            "query_id": query_id,
                            "row_count": len(rows),
                            "rows_sample": rows[:10],
                        },
                        raw=data,
                    )
                )
            except Exception as e:
                self.log.warning("dune_query_failed", query=name, error=str(e))

        return CollectionResult(
            source_id=self.source_id,
            source_name=self.source_name,
            source_type=self.source_type,
            items=items,
        )


class EtherscanCollector(BaseCollector):
    """Collect whale transfers and contract events from Etherscan-like APIs."""

    source_id = "etherscan"
    source_name = "Etherscan"
    source_type = "api"

    EXPLORERS = {
        "ethereum": "https://api.etherscan.io/api",
        "base": "https://api.basescan.org/api",
        "arbitrum": "https://api.arbiscan.io/api",
        "optimism": "https://api-optimistic.etherscan.io/api",
    }

    def __init__(self, chains: list[str] | None = None, **kwargs):
        super().__init__(**kwargs)
        self.chains = chains or ["ethereum"]
        self.api_key = os.environ.get("ETHERSCAN_API_KEY", "")

    async def _collect(self) -> CollectionResult:
        if not self.api_key:
            self.log.warning("no_etherscan_api_key")
            return CollectionResult(
                source_id=self.source_id,
                source_name=self.source_name,
                source_type=self.source_type,
            )

        items = []
        for chain in self.chains:
            base_url = self.EXPLORERS.get(chain)
            if not base_url:
                continue

            # 1. ETH price
            try:
                price_data = await self._fetch_json(
                    base_url,
                    params={
                        "module": "stats",
                        "action": "ethprice",
                        "apikey": self.api_key,
                    },
                    use_cache=False,
                )
                result = price_data.get("result", {})
                if isinstance(result, dict):
                    items.append(
                        CollectedItem(
                            id=f"etherscan_{chain}_ethprice",
                            title=f"[{chain}] ETH Price: ${result.get('ethusd', 'N/A')}",
                            content="",
                            url=f"https://etherscan.io/chart/etherprice",
                            published_at=datetime.now(timezone.utc),
                            metadata={
                                "data_type": "eth_price",
                                "chain": chain,
                                "eth_usd": result.get("ethusd"),
                                "eth_btc": result.get("ethbtc"),
                                "timestamp": result.get("ethusd_timestamp"),
                            },
                            raw=result,
                        )
                    )
            except Exception as e:
                self.log.warning("etherscan_price_failed", chain=chain, error=str(e))

            # 2. ETH supply
            try:
                supply_data = await self._fetch_json(
                    base_url,
                    params={
                        "module": "stats",
                        "action": "ethsupply2",
                        "apikey": self.api_key,
                    },
                    use_cache=False,
                )
                result = supply_data.get("result", {})
                if isinstance(result, dict):
                    items.append(
                        CollectedItem(
                            id=f"etherscan_{chain}_ethsupply",
                            title=f"[{chain}] ETH Supply Data",
                            content="",
                            url=f"https://etherscan.io/stat/supply",
                            published_at=datetime.now(timezone.utc),
                            metadata={
                                "data_type": "eth_supply",
                                "chain": chain,
                                "eth_supply": result.get("EthSupply"),
                                "eth2_staking": result.get("Eth2Staking"),
                                "burnt_fees": result.get("BurntFees"),
                            },
                            raw=result,
                        )
                    )
            except Exception as e:
                self.log.warning("etherscan_supply_failed", chain=chain, error=str(e))

            # 3. Gas price
            try:
                gas_data = await self._fetch_json(
                    base_url,
                    params={
                        "module": "proxy",
                        "action": "eth_gasPrice",
                        "apikey": self.api_key,
                    },
                    use_cache=False,
                )
                gas_hex = gas_data.get("result", "0x0")
                gas_wei = int(gas_hex, 16) if isinstance(gas_hex, str) else 0
                gas_gwei = gas_wei / 1e9
                items.append(
                    CollectedItem(
                        id=f"etherscan_{chain}_gasprice",
                        title=f"[{chain}] Gas Price: {gas_gwei:.2f} Gwei",
                        content="",
                        url=f"https://etherscan.io/gastracker",
                        published_at=datetime.now(timezone.utc),
                        metadata={
                            "data_type": "gas_price",
                            "chain": chain,
                            "gas_wei": gas_wei,
                            "gas_gwei": gas_gwei,
                        },
                        raw=gas_data,
                    )
                )
            except Exception as e:
                self.log.warning("etherscan_gas_failed", chain=chain, error=str(e))

        return CollectionResult(
            source_id=self.source_id,
            source_name=self.source_name,
            source_type=self.source_type,
            items=items,
        )
