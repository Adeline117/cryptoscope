"""Conservative identity rules for EVM factory events entering Launch Radar."""
from __future__ import annotations

from src.pipeline import evm_factory_stream, stream_health


QUOTE_ASSETS = {
    "bsc": {
        "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c",  # WBNB
        "0x55d398326f99059ff775485246999027b3197955",  # USDT
        "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d",  # USDC
        "0xe9e7cea3dedca5984780bafc599bd69add087d56",  # BUSD
        "0x2170ed0880ac9a755fd29b2688956bd959f933f8",  # ETH
        "0x7130d2a12b9bcbfae4f2634d864a1ee1ce3ead9c",  # BTCB
    },
    "base": {
        "0x4200000000000000000000000000000000000006",  # WETH
        "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",  # USDC
        "0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca",  # USDbC
    },
    "ethereum": {
        "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",  # WETH
        "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",  # USDC
        "0xdac17f958d2ee523a2206206994597c13d831ec7",  # USDT
        "0x6b175474e89094c44da98b954eedeac495271d0f",  # DAI
    },
}


def identify_target(row: dict) -> tuple[str | None, str | None]:
    """Return the non-quote token only when factory identity is unambiguous."""
    quotes = QUOTE_ASSETS.get(row.get("chain"), set())
    token0, token1 = str(row.get("token0", "")).lower(), str(row.get("token1", "")).lower()
    known = (token0 in quotes, token1 in quotes)
    if known == (True, True):
        return None, "unsupported_quote_pair"
    if known == (False, False):
        return None, "ambiguous_target"
    return (token1 if known[0] else token0), None


def exact_pair(row: dict, target: str, payload: object) -> dict | None:
    """Match the factory-emitted pool, never a deeper pre-existing token pool."""
    if not isinstance(payload, list):
        raise ValueError("DEX pool response is not a list")
    chain, pool = row["chain"], row["pool"].lower()
    quotes = QUOTE_ASSETS.get(chain, set())
    for pair in payload:
        if not isinstance(pair, dict):
            continue
        base = (pair.get("baseToken") or {}).get("address")
        quote = (pair.get("quoteToken") or {}).get("address")
        if (str(pair.get("chainId", "")).lower() == chain
                and str(pair.get("pairAddress", "")).lower() == pool
                and str(base or "").lower() == target.lower()
                and str(quote or "").lower() in quotes):
            return pair
    return None


def configured_stream_health() -> list[dict]:
    """Return every configured factory stream, including never-seen sources."""
    observed = {(item["source"], item["stream"]): item
                for item in stream_health.snapshot()}
    out = []
    for spec in evm_factory_stream.configured_specs():
        item = observed.get((spec.chain, spec.stream))
        if item is None:
            item = {"source": spec.chain, "stream": spec.stream, "status": "missing",
                    "stale": True, "open_gaps": 0, "last_received_at": None,
                    "last_error": "no stream observation"}
        out.append({**item, "chain": spec.chain, "venue": spec.venue,
                    "factory": spec.address})
    return out
