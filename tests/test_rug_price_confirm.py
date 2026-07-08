"""RUG must require a PRICE crash alongside the liquidity drop — a thin pool's
normal swap swings liquidity ±30% with price flat, which fired phantom RUG (short)
alerts alternating with 浮筹收紧 (long) on BASED. A real LP pull craters both."""

from src.pipeline.operator_sentinel import RUG_DROP, RUG_PRICE_CONFIRM


def _rug_fires(pl, cl, ppr, cpr):
    price_drop = ((ppr - cpr) / ppr) if (ppr and cpr and ppr > 0) else 0
    return (cl is not None and pl and pl > 0 and cl < pl * (1 - RUG_DROP)
            and price_drop >= RUG_PRICE_CONFIRM)


def test_thin_pool_oscillation_does_not_fire():
    # liquidity -40% but price flat = the BASED false positive
    assert _rug_fires(250_000, 150_000, 0.082, 0.082) is False


def test_real_rug_fires():
    # liquidity -40% AND price -50% = genuine LP pull
    assert _rug_fires(250_000, 150_000, 0.082, 0.041) is True


def test_liquidity_drop_with_small_price_dip_does_not_fire():
    # -40% liquidity, only -8.5% price → below the 12% confirm → noise
    assert _rug_fires(250_000, 150_000, 0.082, 0.075) is False


def test_no_liquidity_drop_never_fires():
    assert _rug_fires(250_000, 240_000, 0.082, 0.02) is False
