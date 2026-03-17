"""Institutional fund flow charts (CoinShares, ETF, Grayscale)."""

from __future__ import annotations

import plotly.graph_objects as go
from plotly.subplots import make_subplots

from src.visuals.chart_engine import _apply_brand, save_chart
from src.visuals.styles import arena_brand as brand


def weekly_fund_flows(
    dates: list[str],
    flows: list[float],
    cumulative: list[float] | None = None,
    filename: str | None = None,
) -> go.Figure:
    """Weekly crypto fund flows bar chart with cumulative line.

    Args:
        dates: Week labels
        flows: Weekly net flow values (positive = inflow)
        cumulative: Optional cumulative flow line
    """
    has_cumulative = cumulative is not None
    fig = make_subplots(specs=[[{"secondary_y": has_cumulative}]])

    colors = [brand.POSITIVE if v >= 0 else brand.NEGATIVE for v in flows]

    fig.add_trace(
        go.Bar(
            x=dates,
            y=flows,
            name="Weekly Net Flow",
            marker_color=colors,
            text=[f"${v/1e6:+,.0f}M" for v in flows],
            textposition="outside",
            textfont=dict(size=9),
        )
    )

    if cumulative:
        fig.add_trace(
            go.Scatter(
                x=dates,
                y=cumulative,
                name="Cumulative YTD",
                line=dict(color="#FFEAA7", width=2, dash="dot"),
            ),
            secondary_y=True,
        )
        fig.update_yaxes(
            title_text="Cumulative (USD)",
            secondary_y=True,
            gridcolor="rgba(0,0,0,0)",
        )

    fig = _apply_brand(fig, "Weekly Crypto Fund Flows", "CoinShares")
    fig.update_layout(yaxis_title="Net Flow (USD)")

    if filename:
        save_chart(fig, filename)
    return fig


def fund_flows_by_asset(
    assets: list[str],
    flows: list[float],
    period: str = "Last Week",
    filename: str | None = None,
) -> go.Figure:
    """Horizontal bar chart of fund flows broken down by asset.

    Args:
        assets: Asset names (Bitcoin, Ethereum, Solana, etc.)
        flows: Net flow per asset
    """
    # Sort by absolute value
    paired = sorted(zip(assets, flows), key=lambda x: x[1])
    assets = [p[0] for p in paired]
    flows = [p[1] for p in paired]
    colors = [brand.POSITIVE if v >= 0 else brand.NEGATIVE for v in flows]

    fig = go.Figure(
        go.Bar(
            y=assets,
            x=flows,
            orientation="h",
            marker_color=colors,
            text=[f"${v/1e6:+,.0f}M" for v in flows],
            textposition="outside",
            textfont=dict(size=10, color=brand.TEXT),
        )
    )

    fig = _apply_brand(fig, f"Fund Flows by Asset — {period}", "CoinShares")
    fig.update_layout(xaxis_title="Net Flow (USD)")

    if filename:
        save_chart(fig, filename)
    return fig


def etf_daily_flows(
    dates: list[str],
    etf_data: dict[str, list[float]],
    filename: str | None = None,
) -> go.Figure:
    """Stacked bar chart of daily ETF flows by provider.

    Args:
        dates: Date labels
        etf_data: {provider_name: [daily_flow_values]}
    """
    etf_colors = {
        "BlackRock (IBIT)": "#000000",
        "Fidelity (FBTC)": "#4C8C2B",
        "ARK/21Shares (ARKB)": "#FF6B35",
        "Bitwise (BITB)": "#2962FF",
        "Grayscale (GBTC)": "#8B8B8B",
        "VanEck (HODL)": "#0061A8",
        "Invesco (BTCO)": "#003B71",
        "Franklin (EZBC)": "#002F5F",
    }

    fig = go.Figure()
    for name, values in etf_data.items():
        color = etf_colors.get(name, brand.SERIES_COLORS[len(fig.data) % len(brand.SERIES_COLORS)])
        fig.add_trace(
            go.Bar(
                x=dates,
                y=values,
                name=name,
                marker_color=color,
            )
        )

    fig = _apply_brand(fig, "Bitcoin ETF Daily Net Flows", "The Block / Farside")
    fig.update_layout(
        barmode="relative",
        yaxis_title="Daily Net Flow (USD)",
        legend=dict(orientation="h", yanchor="bottom", y=-0.25, font=dict(size=9)),
    )

    if filename:
        save_chart(fig, filename)
    return fig
