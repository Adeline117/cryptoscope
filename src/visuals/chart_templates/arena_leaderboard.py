"""Arena trader leaderboard chart."""

from __future__ import annotations

import plotly.graph_objects as go

from src.visuals.chart_engine import _apply_brand, save_chart
from src.visuals.styles import arena_brand as brand


def trader_leaderboard(
    traders: list[dict],
    period: str = "24h",
    filename: str | None = None,
) -> go.Figure:
    """Create horizontal bar chart of top traders by PnL.

    traders: list of dicts with keys: username, exchange, pnl, roi
    """
    # Sort by PnL descending
    traders = sorted(traders, key=lambda t: t.get("pnl", 0))  # reversed for horizontal

    labels = [f"{t['username']} ({t.get('exchange', '')})" for t in traders]
    values = [t.get("pnl", 0) for t in traders]
    colors = [brand.POSITIVE if v >= 0 else brand.NEGATIVE for v in values]

    fig = go.Figure(
        go.Bar(
            y=labels,
            x=values,
            orientation="h",
            marker_color=colors,
            text=[f"${v:,.0f}" for v in values],
            textposition="outside",
            textfont=dict(color=brand.TEXT, size=10),
        )
    )

    title = f"Arena Top Traders — PnL ({period})"
    fig = _apply_brand(fig, title, "Arena (arenafi.org)")
    fig.update_layout(
        xaxis_title="PnL (USD)",
        height=max(400, len(traders) * 35),
    )

    if filename:
        save_chart(fig, filename, height=max(400, len(traders) * 35))
    return fig


def exchange_volume_chart(
    exchanges: list[dict],
    filename: str | None = None,
) -> go.Figure:
    """Bar chart of volume by exchange.

    exchanges: list of dicts with keys: exchange, volume, trader_count
    """
    exchanges = sorted(exchanges, key=lambda e: e.get("volume", 0), reverse=True)

    fig = go.Figure(
        go.Bar(
            x=[e["exchange"] for e in exchanges],
            y=[e.get("volume", 0) for e in exchanges],
            marker_color=brand.SERIES_COLORS[: len(exchanges)],
            text=[f"${e.get('volume', 0)/1e9:.1f}B" for e in exchanges],
            textposition="outside",
        )
    )

    fig = _apply_brand(fig, "Exchange Volume Distribution", "Arena (arenafi.org)")
    fig.update_layout(yaxis_title="Volume (USD)")

    if filename:
        save_chart(fig, filename)
    return fig
