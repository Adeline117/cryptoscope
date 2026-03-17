"""Exchange fund flow charts (CryptoQuant style)."""

from __future__ import annotations

import plotly.graph_objects as go
from plotly.subplots import make_subplots

from src.visuals.chart_engine import _apply_brand, save_chart
from src.visuals.styles import arena_brand as brand


def exchange_netflow(
    dates: list[str],
    inflows: list[float],
    outflows: list[float],
    prices: list[float] | None = None,
    asset: str = "BTC",
    filename: str | None = None,
) -> go.Figure:
    """Double-sided bar chart of exchange inflows/outflows with optional price overlay.

    Args:
        dates: Date labels
        inflows: Positive values (deposits to exchanges)
        outflows: Negative values (withdrawals from exchanges)
        prices: Optional price series to overlay
        asset: Asset name (BTC, ETH, etc.)
    """
    fig = make_subplots(specs=[[{"secondary_y": prices is not None}]])

    # Inflows (positive, red = selling pressure)
    fig.add_trace(
        go.Bar(
            x=dates,
            y=inflows,
            name="Inflow (Deposit)",
            marker_color=brand.NEGATIVE,
            opacity=0.8,
        )
    )

    # Outflows (negative, green = accumulation)
    fig.add_trace(
        go.Bar(
            x=dates,
            y=[-abs(v) for v in outflows],
            name="Outflow (Withdrawal)",
            marker_color=brand.POSITIVE,
            opacity=0.8,
        )
    )

    # Price overlay
    if prices:
        fig.add_trace(
            go.Scatter(
                x=dates,
                y=prices,
                name=f"{asset} Price",
                line=dict(color="#FFEAA7", width=2),
                yaxis="y2",
            ),
            secondary_y=True,
        )
        fig.update_yaxes(
            title_text=f"{asset} Price (USD)",
            secondary_y=True,
            gridcolor="rgba(0,0,0,0)",
        )

    fig = _apply_brand(fig, f"{asset} Exchange Net Flow", "CryptoQuant / Glassnode")
    fig.update_layout(
        barmode="relative",
        yaxis_title="Flow (USD)",
    )

    if filename:
        save_chart(fig, filename)
    return fig


def exchange_reserve_change(
    exchanges: list[str],
    reserve_changes: list[float],
    asset: str = "BTC",
    period: str = "7d",
    filename: str | None = None,
) -> go.Figure:
    """Horizontal bar chart of exchange reserve changes."""
    colors = [brand.NEGATIVE if v > 0 else brand.POSITIVE for v in reserve_changes]

    fig = go.Figure(
        go.Bar(
            y=exchanges,
            x=reserve_changes,
            orientation="h",
            marker_color=colors,
            text=[f"{v:+,.0f}" for v in reserve_changes],
            textposition="outside",
            textfont=dict(size=10, color=brand.TEXT),
        )
    )

    fig = _apply_brand(
        fig,
        f"{asset} Exchange Reserve Change ({period})",
        "CryptoQuant / Glassnode",
    )
    fig.update_layout(xaxis_title=f"{asset} Reserve Change")

    if filename:
        save_chart(fig, filename)
    return fig
