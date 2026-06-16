"""Slippage / position-size calculator for thin pools.

These operator coins sit in $150k–$1.5M pools — a normal-size order moves the price
against you. This estimates price impact for a constant-product (Uniswap/Pancake/
Raydium-style) AMM so you size positions to the book, not blow through it.

Model: a constant-product pool with total value L (USD, both sides) has ~L/2 per
side. Buying/selling S USD moves price by impact ≈ S / (L/2 + S). Exit slippage is
symmetric. This is an estimate (ignores fees, routing, concentrated liquidity), but
it's the right order-of-magnitude for sizing on thin books.

    python -m src.pipeline.slippage <liquidity_usd> <trade_usd>
"""

from __future__ import annotations

import sys


def price_impact(liquidity_usd: float, trade_usd: float) -> float:
    """Estimated price impact (%) for a trade of `trade_usd` against the pool."""
    if liquidity_usd <= 0:
        return 100.0
    reserve = liquidity_usd / 2          # one side of the pool, in USD
    return round(trade_usd / (reserve + trade_usd) * 100, 2)


def max_size_for_impact(liquidity_usd: float, max_impact_pct: float = 2.0) -> float:
    """Largest trade (USD) that keeps impact under `max_impact_pct`."""
    if liquidity_usd <= 0:
        return 0.0
    reserve = liquidity_usd / 2
    x = max_impact_pct / 100
    return round(reserve * x / (1 - x), 0)


def tradability(liquidity_usd: float) -> str:
    """One-line tradability summary for an alert / report."""
    mx2 = max_size_for_impact(liquidity_usd, 2.0)
    return (f"流动性${liquidity_usd:,.0f} → 2%滑点内最大约${mx2:,.0f}; "
            f"$5k单冲击≈{price_impact(liquidity_usd, 5000):.1f}%, "
            f"$20k≈{price_impact(liquidity_usd, 20000):.1f}%")


def main():
    if len(sys.argv) >= 3:
        liq, trade = float(sys.argv[1]), float(sys.argv[2])
        print(f"流动性 ${liq:,.0f}, 交易 ${trade:,.0f}")
        print(f"  预估价格冲击: {price_impact(liq, trade)}%")
        print(f"  2%滑点内最大仓位: ${max_size_for_impact(liq, 2.0):,.0f}")
        print(f"  5%滑点内最大仓位: ${max_size_for_impact(liq, 5.0):,.0f}")
    else:
        for liq in (150_000, 300_000, 800_000, 1_500_000):
            print(tradability(liq))


if __name__ == "__main__":
    main()
