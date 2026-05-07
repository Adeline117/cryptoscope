"""GoPlus contract security scanner — free, no API key required.

Scans EVM and Solana token contracts for security risks including:
- Honeypot detection
- Tax analysis (buy/sell)
- Ownership risks (mintable, proxy, owner balance manipulation)
- Holder concentration
- Liquidity pool analysis
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone

import structlog

from src.collectors.base import BaseCollector, CollectedItem, CollectionResult

logger = structlog.get_logger()

# Supported EVM chain IDs
CHAIN_IDS = {
    "ethereum": 1,
    "eth": 1,
    "bsc": 56,
    "polygon": 137,
    "arbitrum": 42161,
    "base": 8453,
}

GOPLUS_BASE = "https://api.gopluslabs.io/api/v1"


@dataclass
class SecurityResult:
    """Result from a contract security scan."""

    address: str
    chain_id: int | str
    risk_score: int  # 0-100 (0 = do not trade, 100 = safe)
    is_honeypot: bool
    risks: list[str]
    info: dict
    raw: dict


class ContractSecurityChecker(BaseCollector):
    """GoPlus-based token contract security checker.

    Usage:
        checker = ContractSecurityChecker()
        result = await checker.check_token(1, "0x...")  # ETH mainnet
        result = await checker.check_token("solana", "mint_address...")
    """

    source_id = "goplus_security"
    source_name = "GoPlus Security"
    source_type = "api"

    def __init__(self, **kwargs):
        super().__init__(cache_ttl=1800, **kwargs)  # 30 min cache

    async def _collect(self) -> CollectionResult:
        """Default collect — returns empty. Use check_token() directly."""
        return CollectionResult(
            source_id=self.source_id,
            source_name=self.source_name,
            source_type=self.source_type,
            items=[],
        )

    async def check_token(
        self, chain_id: int | str, address: str
    ) -> SecurityResult:
        """Scan a token contract and return a risk score (0-100).

        Args:
            chain_id: EVM chain ID (1, 56, 137, 42161, 8453) or "solana"
            address: Contract address (EVM) or mint address (Solana)

        Returns:
            SecurityResult with risk_score 0-100 where:
                0 = DO NOT TRADE (honeypot or critical risk)
                1-30 = Very high risk
                31-50 = High risk
                51-70 = Medium risk
                71-85 = Low risk
                86-100 = Safe
        """
        await self.setup()
        try:
            # Normalize chain_id
            if isinstance(chain_id, str):
                if chain_id.lower() == "solana":
                    return await self._check_solana_token(address)
                chain_id = CHAIN_IDS.get(chain_id.lower(), int(chain_id))

            return await self._check_evm_token(chain_id, address)
        finally:
            await self.teardown()

    async def _check_evm_token(
        self, chain_id: int, address: str
    ) -> SecurityResult:
        """Check an EVM token via GoPlus API."""
        address = address.lower()
        url = f"{GOPLUS_BASE}/token_security/{chain_id}"
        params = {"contract_addresses": address}

        try:
            data = await self._fetch_json(url, params=params, use_cache=True)
        except Exception as e:
            self.log.error("goplus_evm_fetch_failed", error=str(e), chain_id=chain_id, address=address)
            return SecurityResult(
                address=address,
                chain_id=chain_id,
                risk_score=50,  # Unknown — return neutral
                is_honeypot=False,
                risks=["API fetch failed — unable to verify"],
                info={},
                raw={},
            )

        result_data = data.get("result", {})
        token_data = result_data.get(address, {})

        if not token_data:
            return SecurityResult(
                address=address,
                chain_id=chain_id,
                risk_score=50,
                is_honeypot=False,
                risks=["Token not found in GoPlus database"],
                info={},
                raw=data,
            )

        return self._score_evm_token(address, chain_id, token_data, data)

    async def _check_solana_token(self, address: str) -> SecurityResult:
        """Check a Solana token via GoPlus API."""
        url = f"{GOPLUS_BASE}/solana/token_security"
        params = {"contract_addresses": address}

        try:
            data = await self._fetch_json(url, params=params, use_cache=True)
        except Exception as e:
            self.log.error("goplus_solana_fetch_failed", error=str(e), address=address)
            return SecurityResult(
                address=address,
                chain_id="solana",
                risk_score=50,
                is_honeypot=False,
                risks=["API fetch failed — unable to verify"],
                info={},
                raw={},
            )

        result_data = data.get("result", {})
        token_data = result_data.get(address, {})

        if not token_data:
            return SecurityResult(
                address=address,
                chain_id="solana",
                risk_score=50,
                is_honeypot=False,
                risks=["Token not found in GoPlus database"],
                info={},
                raw=data,
            )

        return self._score_solana_token(address, token_data, data)

    def _score_evm_token(
        self, address: str, chain_id: int, token: dict, raw: dict
    ) -> SecurityResult:
        """Apply risk scoring rules to EVM token data.

        Scoring starts at 100 (safe) and deducts points for each risk found.
        """
        score = 100
        risks: list[str] = []
        is_honeypot = False

        # --- Critical: Honeypot ---
        if _bool(token.get("is_honeypot")):
            is_honeypot = True
            score = 0
            risks.append("HONEYPOT DETECTED — DO NOT TRADE")
            # Return immediately, no need to check further
            return SecurityResult(
                address=address,
                chain_id=chain_id,
                risk_score=0,
                is_honeypot=True,
                risks=risks,
                info=self._extract_info(token),
                raw=raw,
            )

        # --- Sell tax > 10% → -30 ---
        sell_tax = _float(token.get("sell_tax"))
        if sell_tax is not None:
            sell_tax_pct = sell_tax * 100 if sell_tax <= 1 else sell_tax
            if sell_tax_pct > 10:
                score -= 30
                risks.append(f"High sell tax: {sell_tax_pct:.1f}%")
            elif sell_tax_pct > 5:
                score -= 15
                risks.append(f"Moderate sell tax: {sell_tax_pct:.1f}%")

        # --- Buy tax ---
        buy_tax = _float(token.get("buy_tax"))
        if buy_tax is not None:
            buy_tax_pct = buy_tax * 100 if buy_tax <= 1 else buy_tax
            if buy_tax_pct > 10:
                score -= 20
                risks.append(f"High buy tax: {buy_tax_pct:.1f}%")
            elif buy_tax_pct > 5:
                score -= 10
                risks.append(f"Moderate buy tax: {buy_tax_pct:.1f}%")

        # --- Top 10 holders > 80% → -20 ---
        holders = token.get("holders", [])
        if holders:
            top10_pct = sum(_float(h.get("percent", 0)) or 0 for h in holders[:10])
            top10_pct_display = top10_pct * 100 if top10_pct <= 1 else top10_pct
            if top10_pct_display > 80:
                score -= 20
                risks.append(f"Top 10 holders control {top10_pct_display:.1f}%")
            elif top10_pct_display > 60:
                score -= 10
                risks.append(f"Top 10 holders control {top10_pct_display:.1f}%")

        # --- is_mintable → -15 ---
        if _bool(token.get("is_mintable")):
            score -= 15
            risks.append("Token is mintable (supply can be inflated)")

        # --- is_proxy → -10 ---
        if _bool(token.get("is_proxy")):
            score -= 10
            risks.append("Contract uses proxy pattern (upgradeable)")

        # --- owner_change_balance → -15 ---
        if _bool(token.get("owner_change_balance")):
            score -= 15
            risks.append("Owner can modify balances")

        # --- can_take_back_ownership → -10 ---
        if _bool(token.get("can_take_back_ownership")):
            score -= 10
            risks.append("Ownership can be reclaimed")

        # --- hidden_owner → -10 ---
        if _bool(token.get("hidden_owner")):
            score -= 10
            risks.append("Hidden owner detected")

        # --- external_call → -5 ---
        if _bool(token.get("external_call")):
            score -= 5
            risks.append("Contract makes external calls")

        # --- cannot_sell_all → -20 ---
        if _bool(token.get("cannot_sell_all")):
            score -= 20
            risks.append("Cannot sell all tokens")

        # --- Low holder count → -5 ---
        holder_count = _int(token.get("holder_count"))
        if holder_count is not None and holder_count < 100:
            score -= 5
            risks.append(f"Very few holders: {holder_count}")

        # --- Low LP holder count → -5 ---
        lp_holder_count = _int(token.get("lp_holder_count"))
        if lp_holder_count is not None and lp_holder_count < 5:
            score -= 5
            risks.append(f"Very few LP holders: {lp_holder_count}")

        # Clamp score
        score = max(0, min(100, score))

        if not risks:
            risks.append("No significant risks detected")

        return SecurityResult(
            address=address,
            chain_id=chain_id,
            risk_score=score,
            is_honeypot=is_honeypot,
            risks=risks,
            info=self._extract_info(token),
            raw=raw,
        )

    def _score_solana_token(
        self, address: str, token: dict, raw: dict
    ) -> SecurityResult:
        """Apply risk scoring rules to Solana token data."""
        score = 100
        risks: list[str] = []

        # Solana-specific checks
        if _bool(token.get("is_token_2022")):
            # Token-2022 has transfer hooks, could be risky
            score -= 5
            risks.append("Token-2022 standard (may have transfer hooks)")

        if _bool(token.get("transferable")):
            pass  # Normal
        else:
            score -= 30
            risks.append("Token is not transferable")

        # Check for freeze authority
        if token.get("freeze_authority") not in (None, "", "null"):
            score -= 15
            risks.append("Freeze authority is set (tokens can be frozen)")

        # Check for mint authority
        if token.get("mint_authority") not in (None, "", "null"):
            score -= 15
            risks.append("Mint authority is set (supply can be inflated)")

        # Holder concentration
        holders = token.get("holders", [])
        if holders:
            top10_pct = sum(_float(h.get("percent", 0)) or 0 for h in holders[:10])
            top10_pct_display = top10_pct * 100 if top10_pct <= 1 else top10_pct
            if top10_pct_display > 80:
                score -= 20
                risks.append(f"Top 10 holders control {top10_pct_display:.1f}%")

        score = max(0, min(100, score))

        if not risks:
            risks.append("No significant risks detected")

        return SecurityResult(
            address=address,
            chain_id="solana",
            risk_score=score,
            is_honeypot=False,
            risks=risks,
            info={"token_standard": "Token-2022" if _bool(token.get("is_token_2022")) else "SPL"},
            raw=raw,
        )

    def _extract_info(self, token: dict) -> dict:
        """Extract key info fields from token data."""
        return {
            "token_name": token.get("token_name", ""),
            "token_symbol": token.get("token_symbol", ""),
            "holder_count": _int(token.get("holder_count")),
            "lp_holder_count": _int(token.get("lp_holder_count")),
            "total_supply": token.get("total_supply"),
            "creator_address": token.get("creator_address", ""),
            "owner_address": token.get("owner_address", ""),
            "is_open_source": _bool(token.get("is_open_source")),
        }


# --- Helpers ---


def _bool(value) -> bool:
    """Convert GoPlus string booleans ('1', '0') to Python bool."""
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip() in ("1", "true", "True", "yes")


def _float(value) -> float | None:
    """Safely convert to float."""
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def _int(value) -> int | None:
    """Safely convert to int."""
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None
