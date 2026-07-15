"""Fail-closed chain identity used before any provider request.

External APIs accept a numeric ID or chain slug.  A typo must never inherit a
provider's Ethereum default, because the same ``0x`` address can exist on many
EVM networks and return plausible-but-wrong evidence.
"""
from __future__ import annotations


_ALIASES = {
    "eth": "ethereum",
    "sol": "solana",
    "avax": "avalanche",
}

EVM_CHAIN_IDS = {
    "ethereum": 1,
    "bsc": 56,
    "polygon": 137,
    "arbitrum": 42161,
    "base": 8453,
    "optimism": 10,
    "avalanche": 43114,
}
SUPPORTED_EVM_CHAIN_IDS = frozenset(EVM_CHAIN_IDS.values())

# Moralis' wallet/token APIs use these exact query slugs.  Keep this separate
# from numeric identity so a caller cannot assume every provider has identical
# chain coverage.
MORALIS_CHAIN_SLUGS = {
    "ethereum": "eth",
    "bsc": "bsc",
    "polygon": "polygon",
    "arbitrum": "arbitrum",
    "base": "base",
    "optimism": "optimism",
    "avalanche": "avalanche",
}


def canonical_chain(chain: object) -> str | None:
    """Return one explicitly supported canonical chain, else ``None``."""
    value = str(chain or "").strip().lower()
    value = _ALIASES.get(value, value)
    if value == "solana" or value in EVM_CHAIN_IDS:
        return value
    return None


def storage_chain_aliases(chain: object) -> tuple[str, ...]:
    """Canonical + legacy labels that may already exist in local storage."""
    canonical = canonical_chain(chain)
    if canonical is None:
        return ()
    aliases = {canonical}
    aliases.update(alias for alias, target in _ALIASES.items() if target == canonical)
    return tuple(sorted(aliases))


def evm_chain_id(chain: object) -> int | None:
    """Resolve a known EVM chain without any Ethereum fallback."""
    canonical = canonical_chain(chain)
    return EVM_CHAIN_IDS.get(canonical) if canonical else None


def moralis_chain_slug(chain: object) -> str | None:
    """Resolve only chains supported by the Moralis paths used in this repo."""
    canonical = canonical_chain(chain)
    return MORALIS_CHAIN_SLUGS.get(canonical) if canonical else None


def security_chain_id(chain: object) -> int | str | None:
    """GoPlus identifier for the explicitly supported EVM/Solana security APIs."""
    canonical = canonical_chain(chain)
    if canonical == "solana":
        return "solana"
    return EVM_CHAIN_IDS.get(canonical) if canonical else None
