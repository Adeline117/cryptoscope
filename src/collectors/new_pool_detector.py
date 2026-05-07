"""Detect new DeFi pools and protocols via DeFiLlama."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from src.collectors.base import BaseCollector, CollectedItem, CollectionResult


class NewPoolDetector(BaseCollector):
    """Detect newly launched DeFi protocols and yield pools.

    Detection rules:
        - New protocols: added to DeFiLlama within the last 48 hours
        - New yield pools: pools with no 30-day mean APY (null apyMean30d)
          but current APY > 20% — indicates a freshly launched pool
        - New pools receive auto_draft priority for immediate review
    """

    source_id = "new_pool_detector"
    source_name = "New Pool Detector"
    source_type = "api"

    PROTOCOLS_URL = "https://api.llama.fi/protocols"
    YIELDS_URL = "https://yields.llama.fi/pools"

    # How far back to look for new protocols
    NEW_PROTOCOL_WINDOW_HOURS = 48

    # Minimum APY for a new pool to be flagged
    MIN_APY_THRESHOLD = 20.0

    async def _collect(self) -> CollectionResult:
        items: list[CollectedItem] = []

        await self._detect_new_protocols(items)
        await self._detect_new_yield_pools(items)

        return CollectionResult(
            source_id=self.source_id,
            source_name=self.source_name,
            source_type=self.source_type,
            items=items,
        )

    # ------------------------------------------------------------------
    # New protocols
    # ------------------------------------------------------------------

    async def _detect_new_protocols(self, items: list[CollectedItem]) -> None:
        """Find protocols added to DeFiLlama within the last 48h."""
        try:
            protocols = await self._fetch_json(self.PROTOCOLS_URL)
            if not isinstance(protocols, list):
                self.log.warning("protocols_unexpected_format")
                return

            now = datetime.now(timezone.utc)
            cutoff = now - timedelta(hours=self.NEW_PROTOCOL_WINDOW_HOURS)

            # DeFiLlama protocol entries may have 'listedAt' (unix timestamp)
            # or we can sort by newest entries. We try both approaches.
            new_protocols = []
            for p in protocols:
                listed_at = p.get("listedAt")
                if listed_at is not None:
                    try:
                        listed_dt = datetime.fromtimestamp(int(listed_at), tz=timezone.utc)
                        if listed_dt >= cutoff:
                            new_protocols.append((p, listed_dt))
                    except (ValueError, TypeError, OSError):
                        continue

            # Sort newest first
            new_protocols.sort(key=lambda x: x[1], reverse=True)

            for protocol, listed_dt in new_protocols:
                name = protocol.get("name", "Unknown")
                slug = protocol.get("slug", name.lower().replace(" ", "-"))
                tvl = protocol.get("tvl") or 0
                category = protocol.get("category", "")
                chains = protocol.get("chains", [])
                hours_ago = (now - listed_dt).total_seconds() / 3600

                title = (
                    f"[NEW PROTOCOL] {name} — "
                    f"TVL: ${tvl:,.0f}, "
                    f"Category: {category}, "
                    f"Listed {hours_ago:.0f}h ago"
                )

                items.append(
                    CollectedItem(
                        id=f"new_protocol_{slug}",
                        title=title,
                        content=f"Chains: {', '.join(chains[:5])}. Category: {category}.",
                        url=f"https://defillama.com/protocol/{slug}",
                        published_at=listed_dt,
                        metadata={
                            "data_type": "new_protocol",
                            "name": name,
                            "slug": slug,
                            "tvl": tvl,
                            "category": category,
                            "chains": chains,
                            "listed_at": listed_dt.isoformat(),
                            "hours_since_listed": round(hours_ago, 1),
                            "priority": "high",
                            "auto_draft": True,
                        },
                        raw=protocol,
                    )
                )

            self.log.info("new_protocols_found", count=len(new_protocols))

        except Exception as e:
            self.log.warning("new_protocols_detection_failed", error=str(e))

    # ------------------------------------------------------------------
    # New yield pools
    # ------------------------------------------------------------------

    async def _detect_new_yield_pools(self, items: list[CollectedItem]) -> None:
        """Find new yield pools: no 30d mean APY but current APY > 20%."""
        try:
            data = await self._fetch_json(self.YIELDS_URL)
            pools = data.get("data", []) if isinstance(data, dict) else data
            if not isinstance(pools, list):
                self.log.warning("yields_unexpected_format")
                return

            new_pools = []
            for pool in pools:
                apy = pool.get("apy")
                apy_mean_30d = pool.get("apyMean30d")
                tvl = pool.get("tvlUsd") or 0

                # New pool criteria: no 30-day mean AND APY > threshold
                # AND minimum TVL to filter out dead/empty pools
                if (
                    apy_mean_30d is None
                    and apy is not None
                    and apy > self.MIN_APY_THRESHOLD
                    and tvl >= 10_000  # At least $10k TVL
                ):
                    new_pools.append(pool)

            # Sort by APY descending
            new_pools.sort(key=lambda p: p.get("apy", 0), reverse=True)

            for pool in new_pools[:50]:  # Cap at 50 to avoid flooding
                pool_id = pool.get("pool", "")
                project = pool.get("project", "Unknown")
                symbol = pool.get("symbol", "")
                chain = pool.get("chain", "")
                apy = pool.get("apy", 0)
                tvl = pool.get("tvlUsd", 0)
                apy_base = pool.get("apyBase") or 0
                apy_reward = pool.get("apyReward") or 0

                title = (
                    f"[NEW POOL] {project} {symbol} on {chain} — "
                    f"APY: {apy:.1f}%, TVL: ${tvl:,.0f}"
                )

                items.append(
                    CollectedItem(
                        id=f"new_pool_{pool_id}",
                        title=title,
                        content=(
                            f"Base APY: {apy_base:.1f}%, Reward APY: {apy_reward:.1f}%. "
                            f"No 30-day history — newly launched pool."
                        ),
                        url="https://defillama.com/yields",
                        published_at=datetime.now(timezone.utc),
                        metadata={
                            "data_type": "new_yield_pool",
                            "pool_id": pool_id,
                            "project": project,
                            "symbol": symbol,
                            "chain": chain,
                            "apy": apy,
                            "apy_base": apy_base,
                            "apy_reward": apy_reward,
                            "tvl_usd": tvl,
                            "priority": "high",
                            "auto_draft": True,
                            "category": "defi_protocol",
                        },
                        raw=pool,
                    )
                )

            self.log.info("new_yield_pools_found", count=len(new_pools))

        except Exception as e:
            self.log.warning("new_yield_pools_detection_failed", error=str(e))
