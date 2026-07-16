"""DEX Screener provider errors can never masquerade as an empty token market."""
import pytest


def _pair(*, base="Token", quote="SOL", liquidity=20_000, chain="solana",
          address="pool"):
    return {
        "chainId": chain, "pairAddress": address, "priceUsd": "0.001",
        "baseToken": {"address": base}, "quoteToken": {"address": quote},
        "liquidity": {"usd": liquidity},
    }


def test_batch_contract_accepts_valid_empty_and_hashes_it():
    from src.contract import dexscreener as dex

    got = dex.batch_prefilter("solana", ["Token"], fetch=lambda _url: [])

    assert got["base_tokens"] == set()
    assert got["response_hash"] == dex.payload_hash([])


@pytest.mark.parametrize("payload", [None, {}, {"error": "overloaded"}, [None]])
def test_batch_contract_rejects_wrong_schema(payload):
    from src.contract import dexscreener as dex

    with pytest.raises(dex.MarketDataSchemaError):
        dex.batch_prefilter("solana", ["Token"], fetch=lambda _url: payload)


def test_batch_contract_distinguishes_base_quote_and_unrelated_rows():
    from src.contract import dexscreener as dex

    mixed = dex.batch_prefilter(
        "solana", ["Base", "Quote"],
        fetch=lambda _url: [
            _pair(base="Base", quote="SOL", address="base-pool"),
            _pair(base="Other", quote="Quote", address="quote-pool"),
        ],
    )
    assert mixed["base_tokens"] == {"Base"}

    with pytest.raises(dex.MarketDataSchemaError, match="unrelated"):
        dex.batch_prefilter(
            "solana", ["Base"], fetch=lambda _url: [_pair(base="Other")],
        )


def test_batch_contract_enforces_official_30_address_limit():
    from src.contract import dexscreener as dex

    with pytest.raises(ValueError, match="30"):
        dex.batch_prefilter(
            "solana", [f"Token{index}" for index in range(31)],
            fetch=lambda _url: [],
        )


def test_exact_contract_selects_deepest_base_pool_and_hashes_full_payload():
    from src.contract import dexscreener as dex

    payload = [
        _pair(liquidity=10_000, address="shallow"),
        _pair(liquidity=50_000, address="deep"),
        _pair(base="Other", quote="Token", liquidity=1_000_000, address="quote"),
    ]
    got = dex.exact_base_pair("solana", "Token", fetch=lambda _url: payload)

    assert got["pair"]["pairAddress"] == "deep"
    assert got["response_hash"] == dex.payload_hash(payload)


def test_exact_contract_rejects_error_objects_and_unrelated_rows():
    from src.contract import dexscreener as dex

    with pytest.raises(dex.MarketDataSchemaError):
        dex.exact_base_pair(
            "solana", "Token", fetch=lambda _url: {"error": "rate limited"},
        )
    with pytest.raises(dex.MarketDataSchemaError, match="unrelated"):
        dex.exact_base_pair(
            "solana", "Token", fetch=lambda _url: [_pair(base="Other", quote="SOL")],
        )
