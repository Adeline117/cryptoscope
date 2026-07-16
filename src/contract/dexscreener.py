"""Strict, evidence-bearing contracts for DEX Screener launch reads."""
from __future__ import annotations

import hashlib
import json
import math
from typing import Any, Callable


TOKENS_URL = "https://api.dexscreener.com/tokens/v1/{chain}/{tokens}"
TOKEN_PAIRS_URL = "https://api.dexscreener.com/token-pairs/v1/{chain}/{token}"
BATCH_SIZE = 30
Fetch = Callable[[str], object]


class MarketDataSchemaError(RuntimeError):
    """A successful HTTP response did not satisfy the provider contract."""


def payload_hash(payload: object) -> str:
    return hashlib.sha256(json.dumps(
        payload, sort_keys=True, separators=(",", ":"),
    ).encode()).hexdigest()


def _identity(chain: str, value: Any) -> str:
    normalized = str(value or "")
    return normalized if chain == "solana" else normalized.lower()


def _pair_identity(chain: str, pair: object) -> tuple[str, str]:
    if not isinstance(pair, dict):
        raise MarketDataSchemaError("DEX Screener response contains a non-object")
    base, quote = pair.get("baseToken"), pair.get("quoteToken")
    if (not pair.get("chainId") or not pair.get("pairAddress")
            or not isinstance(base, dict) or not base.get("address")):
        raise MarketDataSchemaError("DEX Screener response has invalid pair identity")
    if str(pair["chainId"]).lower() != str(chain).lower():
        raise MarketDataSchemaError("DEX Screener response changed chain")
    return _identity(chain, base["address"]), _identity(
        chain, quote.get("address") if isinstance(quote, dict) else None,
    )


def batch_prefilter(chain: str, tokens: list[str], *, fetch: Fetch) -> dict:
    """Return base-side candidates; never treat this response as an entry price."""
    unique = list(dict.fromkeys(str(token) for token in tokens if token))
    if not unique:
        return {"base_tokens": set(), "response_hash": payload_hash([]), "url": None}
    if len(unique) > BATCH_SIZE:
        raise ValueError(f"DEX Screener batch exceeds {BATCH_SIZE} tokens")
    url = TOKENS_URL.format(chain=chain, tokens=",".join(unique))
    response = fetch(url)
    if not isinstance(response, list):
        raise MarketDataSchemaError("DEX Screener tokens response is not a list")
    requested = {_identity(chain, token) for token in unique}
    observed: set[str] = set()
    for pair in response:
        base, quote = _pair_identity(chain, pair)
        identities = {base} | ({quote} if quote else set())
        if not identities & requested:
            raise MarketDataSchemaError("DEX Screener tokens response is unrelated to request")
        if base in requested:
            observed.add(base)
    return {
        "base_tokens": observed,
        "response_hash": payload_hash(response),
        "url": url,
    }


def exact_base_pair(chain: str, token: str, *, fetch: Fetch) -> dict:
    """Select the deepest exact base-side pool and hash the complete response."""
    expected = _identity(chain, token)
    url = TOKEN_PAIRS_URL.format(chain=chain, token=token)
    response = fetch(url)
    if not isinstance(response, list):
        raise MarketDataSchemaError("DEX Screener token-pairs response is not a list")
    usable = []
    for pair in response:
        base, quote = _pair_identity(chain, pair)
        if expected not in {base, quote}:
            raise MarketDataSchemaError(
                "DEX Screener token-pairs response is unrelated to request"
            )
        if base != expected or not pair.get("priceUsd"):
            continue
        liquidity = (pair.get("liquidity") or {}).get("usd")
        try:
            depth = float(liquidity)
        except (TypeError, ValueError, OverflowError):
            continue
        if not math.isfinite(depth) or depth < 0:
            continue
        usable.append((depth, pair))
    selected = max(usable, key=lambda item: item[0], default=(None, None))[1]
    return {
        "pair": selected,
        "response_hash": payload_hash(response),
        "url": url,
    }
