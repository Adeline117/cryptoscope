"""Stablecoin market share and dominance charts."""

from __future__ import annotations

import plotly.graph_objects as go

from src.visuals.chart_engine import _apply_brand, save_chart
from src.visuals.styles import arena_brand as brand

STABLECOIN_COLORS = {
    "USDT": "#26A17B",
    "USDC": "#2775CA",
    "DAI": "#F5AC37",
    "BUSD": "#F0B90B",
    "TUSD": "#002868",
    "FRAX": "#000000",
    "LUSD": "#745DDF",
    "USDD": "#216BFF",
    "GHO": "#7C3AED",
    "crvUSD": "#FF6B6B",
    "USDe": "#00D4AA",
    "FDUSD": "#FBBF24",
    "PYUSD": "#003087",
    "USDS": "#1AAB9B",
}


def stablecoin_market_share(
    dates: list[str],
    stablecoin_data: dict[str, list[float]],
    filename: str | None = None,
) -> go.Figure:
    """Stacked area chart of stablecoin market share over time.

    Args:
        dates: Date labels
        stablecoin_data: {coin_name: [market_cap_values]}
    """
    fig = go.Figure()

    for name, values in stablecoin_data.items():
        color = STABLECOIN_COLORS.get(name, brand.SERIES_COLORS[len(fig.data) % len(brand.SERIES_COLORS)])
        fig.add_trace(
            go.Scatter(
                x=dates,
                y=values,
                name=name,
                mode="lines",
                stackgroup="one",
                line=dict(width=0.5, color=color),
                fillcolor=color,
            )
        )

    fig = _apply_brand(fig, "Stablecoin Market Cap Distribution", "DeFiLlama Stablecoins")
    fig.update_layout(
        yaxis_title="Market Cap (USD)",
        legend=dict(orientation="h", yanchor="bottom", y=-0.2),
    )

    if filename:
        save_chart(fig, filename)
    return fig


def stablecoin_supply_change(
    stablecoins: list[str],
    changes: list[float],
    period: str = "7d",
    filename: str | None = None,
) -> go.Figure:
    """Bar chart of stablecoin supply changes."""
    colors = [brand.POSITIVE if v >= 0 else brand.NEGATIVE for v in changes]

    fig = go.Figure(
        go.Bar(
            x=stablecoins,
            y=changes,
            marker_color=colors,
            text=[f"{v:+,.0f}M" for v in changes],
            textposition="outside",
        )
    )

    fig = _apply_brand(fig, f"Stablecoin Supply Change ({period})", "DeFiLlama")
    fig.update_layout(yaxis_title="Supply Change (USD)")

    if filename:
        save_chart(fig, filename)
    return fig
