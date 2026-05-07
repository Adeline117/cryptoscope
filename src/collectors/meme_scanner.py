"""Meme coin hunting: Pump.fun new launches and DexScreener trending tokens.

Monitors:
- Pump.fun token launches via DexScreener search API (free, no key)
- Boosted / trending tokens across all chains
- Risk flagging: low liquidity, abnormal volume/liquidity ratios
- Auto-triggers GoPlus security checks for flagged tokens
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone
from typing import Any

import structlog

from src.collectors.base import BaseCollector, CollectedItem, CollectionResult

logger = structlog.get_logger()

# DexScreener public API endpoints (free, no API key required)
DEXSCREENER_BASE = "https://api.dexscreener.com"
DEXSCREENER_SEARCH = f"{DEXSCREENER_BASE}/latest/dex/search"
DEXSCREENER_BOOSTS_LATEST = f"{DEXSCREENER_BASE}/token-boosts/latest/v1"
DEXSCREENER_BOOSTS_TOP = f"{DEXSCREENER_BASE}/token-boosts/top/v1"
DEXSCREENER_TOKENS = f"{DEXSCREENER_BASE}/latest/dex/tokens"

# Risk thresholds
LOW_LIQUIDITY_THRESHOLD = 10_000  # USD
HIGH_VOLUME_LIQUIDITY_RATIO = 5.0  # volume_24h / liquidity


def _assess_risk(liquidity_usd: float, volume_24h: float) -> dict[str, Any]:
    """Assess token risk based on liquidity and volume metrics.

    Returns:
        dict with risk_level, flags list, and individual flag booleans.
    """
    flags: list[str] = []

    low_liquidity = liquidity_usd < LOW_LIQUIDITY_THRESHOLD
    if low_liquidity:
        flags.append("low_liquidity")

    vol_liq_ratio = volume_24h / liquidity_usd if liquidity_usd > 0 else 999.0
    potential_rug = vol_liq_ratio > HIGH_VOLUME_LIQUIDITY_RATIO
    if potential_rug:
        flags.append("potential_rug")

    if low_liquidity and potential_rug:
        risk_level = "critical"
    elif low_liquidity or potential_rug:
        risk_level = "high_risk"
    elif liquidity_usd < 50_000:
        risk_level = "medium_risk"
    else:
        risk_level = "low_risk"

    return {
        "risk_level": risk_level,
        "flags": flags,
        "low_liquidity": low_liquidity,
        "potential_rug": potential_rug,
        "volume_liquidity_ratio": round(vol_liq_ratio, 2),
    }


def _parse_pair(pair: dict) -> dict[str, Any]:
    """Extract normalised fields from a DexScreener pair object."""
    base_token = pair.get("baseToken", {})
    liquidity = pair.get("liquidity", {})
    liquidity_usd = liquidity.get("usd", 0) if isinstance(liquidity, dict) else 0
    volume_24h = pair.get("volume", {}).get("h24", 0) or 0
    price_change = pair.get("priceChange", {})

    return {
        "token_name": base_token.get("name", ""),
        "token_symbol": base_token.get("symbol", ""),
        "token_address": base_token.get("address", ""),
        "pair_address": pair.get("pairAddress", ""),
        "chain_id": pair.get("chainId", ""),
        "dex_id": pair.get("dexId", ""),
        "price_usd": pair.get("priceUsd"),
        "liquidity_usd": liquidity_usd,
        "volume_24h": volume_24h,
        "price_change_5m": price_change.get("m5"),
        "price_change_1h": price_change.get("h1"),
        "price_change_6h": price_change.get("h6"),
        "price_change_24h": price_change.get("h24"),
        "pair_created_at": pair.get("pairCreatedAt"),
        "url": pair.get("url", ""),
    }


class PumpFunCollector(BaseCollector):
    """Monitor Pump.fun new token launches via DexScreener public API.

    Searches for recently-created Pump.fun pairs, filters by creation time
    (last 6 hours), assesses risk, and optionally triggers GoPlus security checks.
    """

    source_id = "pumpfun_scanner"
    source_name = "Pump.fun Scanner"
    source_type = "api"

    # How far back to look for new tokens (hours)
    MAX_AGE_HOURS = 6

    def __init__(self, auto_security_check: bool = True, **kwargs):
        super().__init__(cache_ttl=120, **kwargs)  # 2-min cache for fresh data
        self.auto_security_check = auto_security_check
        self._security_checker = None

    async def _maybe_init_security_checker(self):
        """Lazy-import and initialise the GoPlus security checker."""
        if self._security_checker is not None:
            return
        if not self.auto_security_check:
            return
        try:
            from src.collectors.contract_security import ContractSecurityChecker
            self._security_checker = ContractSecurityChecker()
            await self._security_checker.setup()
        except Exception as e:
            self.log.warning("security_checker_init_failed", error=str(e))
            self._security_checker = None

    async def _run_security_check(self, chain_id: str, token_address: str) -> dict | None:
        """Run GoPlus security check if available."""
        await self._maybe_init_security_checker()
        if self._security_checker is None:
            return None
        try:
            result = await self._security_checker.check_token(chain_id, token_address)
            return {
                "risk_score": result.risk_score,
                "is_honeypot": result.is_honeypot,
                "risks": result.risks,
            } if result else None
        except Exception as e:
            self.log.debug("security_check_failed", token=token_address, error=str(e))
            return None

    async def _collect(self) -> CollectionResult:
        items: list[CollectedItem] = []
        cutoff = datetime.now(timezone.utc) - timedelta(hours=self.MAX_AGE_HOURS)
        cutoff_ms = int(cutoff.timestamp() * 1000)

        # 1. Search for Pump.fun tokens
        try:
            data = await self._fetch_json(
                DEXSCREENER_SEARCH,
                params={"q": "pump.fun"},
                use_cache=True,
            )
            pairs = data.get("pairs", []) or []
        except Exception as e:
            self.log.warning("pumpfun_search_failed", error=str(e))
            pairs = []

        # 2. Also fetch latest boosted tokens (may include Pump.fun tokens)
        boosted_pairs: list[dict] = []
        try:
            boosted_data = await self._fetch_json(
                DEXSCREENER_BOOSTS_LATEST,
                use_cache=True,
            )
            if isinstance(boosted_data, list):
                boosted_pairs = boosted_data
            elif isinstance(boosted_data, dict):
                boosted_pairs = boosted_data.get("tokens", boosted_data.get("pairs", []))
        except Exception as e:
            self.log.warning("boosted_tokens_fetch_failed", error=str(e))

        # Merge — deduplicate by pair address
        seen_pairs: set[str] = set()
        all_pairs: list[dict] = []
        for p in pairs + boosted_pairs:
            pa = p.get("pairAddress", p.get("tokenAddress", ""))
            if pa and pa not in seen_pairs:
                seen_pairs.add(pa)
                all_pairs.append(p)

        # 3. Process each pair
        for pair in all_pairs:
            parsed = _parse_pair(pair)
            created_at_ms = parsed["pair_created_at"]

            # Filter: only tokens created within MAX_AGE_HOURS
            if created_at_ms and isinstance(created_at_ms, (int, float)):
                if created_at_ms < cutoff_ms:
                    continue
                creation_time = datetime.fromtimestamp(
                    created_at_ms / 1000, tz=timezone.utc
                )
            else:
                creation_time = None

            risk = _assess_risk(parsed["liquidity_usd"], parsed["volume_24h"])

            # Optional GoPlus check for high-risk tokens
            security_info = None
            if risk["risk_level"] in ("critical", "high_risk") and self.auto_security_check:
                chain_map = {"solana": "solana", "ethereum": 1, "bsc": 56}
                chain_val = chain_map.get(parsed["chain_id"], parsed["chain_id"])
                security_info = await self._run_security_check(
                    str(chain_val), parsed["token_address"]
                )

            item_id = hashlib.sha256(
                f"pumpfun_{parsed['pair_address']}_{parsed['token_address']}".encode()
            ).hexdigest()[:16]

            title_parts = [
                f"[Pump.fun] {parsed['token_name']} (${parsed['token_symbol']})",
                f"Liq: ${parsed['liquidity_usd']:,.0f}",
                f"Vol24h: ${parsed['volume_24h']:,.0f}",
            ]
            if risk["flags"]:
                title_parts.append(f"RISK: {', '.join(risk['flags'])}")

            items.append(
                CollectedItem(
                    id=f"pumpfun_{item_id}",
                    title=" | ".join(title_parts),
                    content=f"Risk level: {risk['risk_level']}. "
                            f"V/L ratio: {risk['volume_liquidity_ratio']}. "
                            f"Chain: {parsed['chain_id']}.",
                    url=parsed["url"] or f"https://dexscreener.com/{parsed['chain_id']}/{parsed['pair_address']}",
                    published_at=creation_time or datetime.now(timezone.utc),
                    metadata={
                        "data_type": "meme_token_launch",
                        "source": "pumpfun",
                        **parsed,
                        "creation_time": creation_time.isoformat() if creation_time else None,
                        **risk,
                        "security_check": security_info,
                    },
                    raw=pair,
                )
            )

        self.log.info("pumpfun_scan_complete", tokens_found=len(items))
        return CollectionResult(
            source_id=self.source_id,
            source_name=self.source_name,
            source_type=self.source_type,
            items=items,
        )

    async def teardown(self) -> None:
        """Clean up security checker as well."""
        if self._security_checker:
            try:
                await self._security_checker.teardown()
            except Exception:
                pass
        await super().teardown()


class DexScreenerTrendingCollector(BaseCollector):
    """Track trending / most-boosted tokens across all chains via DexScreener.

    Fetches the top boosted tokens, enriches them with pair data, and flags
    risk based on liquidity and volume metrics.
    """

    source_id = "dexscreener_trending"
    source_name = "DexScreener Trending"
    source_type = "api"

    # Maximum addresses per batch lookup (DexScreener limit ~30)
    BATCH_SIZE = 30

    def __init__(self, auto_security_check: bool = True, **kwargs):
        super().__init__(cache_ttl=180, **kwargs)  # 3-min cache
        self.auto_security_check = auto_security_check
        self._security_checker = None

    async def _maybe_init_security_checker(self):
        if self._security_checker is not None:
            return
        if not self.auto_security_check:
            return
        try:
            from src.collectors.contract_security import ContractSecurityChecker
            self._security_checker = ContractSecurityChecker()
            await self._security_checker.setup()
        except Exception as e:
            self.log.warning("security_checker_init_failed", error=str(e))
            self._security_checker = None

    async def _run_security_check(self, chain_id: str, token_address: str) -> dict | None:
        await self._maybe_init_security_checker()
        if self._security_checker is None:
            return None
        try:
            result = await self._security_checker.check_token(chain_id, token_address)
            return {
                "risk_score": result.risk_score,
                "is_honeypot": result.is_honeypot,
                "risks": result.risks,
            } if result else None
        except Exception as e:
            self.log.debug("security_check_failed", token=token_address, error=str(e))
            return None

    async def _batch_lookup_tokens(self, addresses: list[str]) -> list[dict]:
        """Batch lookup token pair data from DexScreener."""
        all_pairs: list[dict] = []
        for i in range(0, len(addresses), self.BATCH_SIZE):
            batch = addresses[i : i + self.BATCH_SIZE]
            addr_str = ",".join(batch)
            try:
                data = await self._fetch_json(
                    f"{DEXSCREENER_TOKENS}/{addr_str}",
                    use_cache=True,
                )
                pairs = data.get("pairs", []) or []
                all_pairs.extend(pairs)
            except Exception as e:
                self.log.warning("batch_lookup_failed", batch_start=i, error=str(e))
        return all_pairs

    async def _collect(self) -> CollectionResult:
        items: list[CollectedItem] = []

        # 1. Fetch most boosted tokens
        boosted_tokens: list[dict] = []
        try:
            data = await self._fetch_json(DEXSCREENER_BOOSTS_TOP, use_cache=True)
            if isinstance(data, list):
                boosted_tokens = data
            elif isinstance(data, dict):
                boosted_tokens = data.get("tokens", data.get("pairs", []))
        except Exception as e:
            self.log.warning("top_boosts_fetch_failed", error=str(e))

        # 2. Collect unique addresses for batch lookup
        address_set: set[str] = set()
        token_meta: dict[str, dict] = {}  # address -> boost info
        for t in boosted_tokens:
            addr = t.get("tokenAddress", t.get("address", ""))
            if addr:
                address_set.add(addr)
                token_meta[addr] = {
                    "boost_amount": t.get("amount", 0),
                    "chain_id": t.get("chainId", ""),
                    "description": t.get("description", ""),
                    "icon": t.get("icon", ""),
                    "url": t.get("url", ""),
                }

        # 3. Batch lookup detailed pair data
        if address_set:
            pairs = await self._batch_lookup_tokens(list(address_set))
        else:
            pairs = []

        # Deduplicate pairs by pair address, preferring highest liquidity
        best_pairs: dict[str, dict] = {}
        for pair in pairs:
            token_addr = pair.get("baseToken", {}).get("address", "")
            existing = best_pairs.get(token_addr)
            pair_liq = (pair.get("liquidity", {}) or {}).get("usd", 0) or 0
            if existing is None:
                best_pairs[token_addr] = pair
            else:
                existing_liq = (existing.get("liquidity", {}) or {}).get("usd", 0) or 0
                if pair_liq > existing_liq:
                    best_pairs[token_addr] = pair

        # 4. Process each enriched token
        for token_addr, pair in best_pairs.items():
            parsed = _parse_pair(pair)
            risk = _assess_risk(parsed["liquidity_usd"], parsed["volume_24h"])
            boost_info = token_meta.get(token_addr, {})

            created_at_ms = parsed["pair_created_at"]
            if created_at_ms and isinstance(created_at_ms, (int, float)):
                creation_time = datetime.fromtimestamp(
                    created_at_ms / 1000, tz=timezone.utc
                )
            else:
                creation_time = None

            # Security check for risky tokens
            security_info = None
            if risk["risk_level"] in ("critical", "high_risk") and self.auto_security_check:
                chain_map = {"solana": "solana", "ethereum": 1, "bsc": 56}
                chain_val = chain_map.get(parsed["chain_id"], parsed["chain_id"])
                security_info = await self._run_security_check(
                    str(chain_val), parsed["token_address"]
                )

            item_id = hashlib.sha256(
                f"trending_{parsed['pair_address']}_{token_addr}".encode()
            ).hexdigest()[:16]

            title_parts = [
                f"[Trending] {parsed['token_name']} (${parsed['token_symbol']})",
                f"Liq: ${parsed['liquidity_usd']:,.0f}",
                f"Vol24h: ${parsed['volume_24h']:,.0f}",
            ]
            if boost_info.get("boost_amount"):
                title_parts.append(f"Boost: {boost_info['boost_amount']}")
            if risk["flags"]:
                title_parts.append(f"RISK: {', '.join(risk['flags'])}")

            items.append(
                CollectedItem(
                    id=f"trending_{item_id}",
                    title=" | ".join(title_parts),
                    content=f"Risk level: {risk['risk_level']}. "
                            f"V/L ratio: {risk['volume_liquidity_ratio']}. "
                            f"Chain: {parsed['chain_id']}.",
                    url=parsed["url"] or f"https://dexscreener.com/{parsed['chain_id']}/{parsed['pair_address']}",
                    published_at=creation_time or datetime.now(timezone.utc),
                    metadata={
                        "data_type": "trending_token",
                        "source": "dexscreener_trending",
                        **parsed,
                        "creation_time": creation_time.isoformat() if creation_time else None,
                        **risk,
                        "boost_amount": boost_info.get("boost_amount", 0),
                        "security_check": security_info,
                    },
                    raw=pair,
                )
            )

        self.log.info("trending_scan_complete", tokens_found=len(items))
        return CollectionResult(
            source_id=self.source_id,
            source_name=self.source_name,
            source_type=self.source_type,
            items=items,
        )

    async def teardown(self) -> None:
        if self._security_checker:
            try:
                await self._security_checker.teardown()
            except Exception:
                pass
        await super().teardown()
