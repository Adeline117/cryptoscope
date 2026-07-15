"""EVM factory launches require exact pool and unambiguous quote-side identity."""
from datetime import datetime, timezone


WBNB = "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c"
TOKEN = "0x1111111111111111111111111111111111111111"
POOL = "0x3333333333333333333333333333333333333333"


def _raw(**overrides):
    item = {"chain": "bsc", "token0": TOKEN, "token1": WBNB, "pool": POOL}
    item.update(overrides)
    return item


def _pair(now, **overrides):
    item = {
        "chainId": "bsc", "pairAddress": POOL, "priceUsd": "0.001",
        "pairCreatedAt": int(now.timestamp() * 1000), "fdv": 100_000,
        "liquidity": {"usd": 20_000}, "volume": {"m5": 1_000},
        "txns": {"m5": {"buys": 10, "sells": 3}},
        "baseToken": {"address": TOKEN, "symbol": "NEW", "name": "New"},
        "quoteToken": {"address": WBNB, "symbol": "WBNB"},
    }
    item.update(overrides)
    return item


def test_quote_side_identity_works_in_both_token_orders():
    from src.pipeline.evm_launch_bridge import identify_target

    assert identify_target(_raw()) == (TOKEN, None)
    assert identify_target(_raw(token0=WBNB, token1=TOKEN)) == (TOKEN, None)


def test_ambiguous_or_double_quote_factory_pair_is_terminal():
    from src.pipeline.evm_launch_bridge import identify_target

    assert identify_target(_raw(token0=TOKEN, token1="0x2222222222222222222222222222222222222222")) \
        == (None, "ambiguous_target")
    assert identify_target(_raw(token0=WBNB,
                                token1="0x55d398326f99059ff775485246999027b3197955")) \
        == (None, "unsupported_quote_pair")


def test_exact_pool_never_substitutes_deeper_old_pool():
    from src.pipeline.evm_launch_bridge import exact_pair

    now = datetime(2026, 7, 15, 12, tzinfo=timezone.utc)
    old = _pair(now, pairAddress="0x4444444444444444444444444444444444444444",
                liquidity={"usd": 9_000_000})
    exact = _pair(now)
    assert exact_pair(_raw(), TOKEN, [old, exact]) is exact


def test_configured_health_includes_never_observed_streams(tmp_path, monkeypatch):
    from src.pipeline import stream_health
    from src.pipeline.evm_launch_bridge import configured_stream_health

    monkeypatch.setattr(stream_health, "DB", tmp_path / "health.db")
    rows = configured_stream_health()
    assert len(rows) == 5
    assert all(row["status"] == "missing" for row in rows)
    assert {(row["chain"], row["venue"]) for row in rows} == {
        ("bsc", "pancakeswap_v2"), ("bsc", "pancakeswap_v3"),
        ("base", "pancakeswap_v2"), ("base", "aerodrome"),
        ("ethereum", "pancakeswap_v2"),
    }
