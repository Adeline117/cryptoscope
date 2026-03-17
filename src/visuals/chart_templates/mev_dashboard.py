"""MEV analytics charts — revenue, sandwich attacks, builder dominance, centralization."""

from __future__ import annotations

import math

import plotly.graph_objects as go
from plotly.subplots import make_subplots

from src.visuals.chart_engine import _apply_brand, save_chart
from src.visuals.styles import arena_brand as brand


def _fmt_usd(v: float) -> str:
    """Format dollar value: $1.2B, $340M, $52K, $800."""
    if v >= 1e9:
        return f"${v / 1e9:.1f}B"
    if v >= 1e6:
        return f"${v / 1e6:.1f}M"
    if v >= 1e3:
        return f"${v / 1e3:.0f}K"
    return f"${v:,.0f}"


def mev_profit_distribution(
    entities: list[str],
    profits: list[float],
    period: str = "7d",
    filename: str | None = None,
) -> go.Figure:
    """Horizontal log-scale bar chart for MEV profit distribution.

    Designed to make the magnitude gap between $5M and $3K visually obvious.
    Each bar shows the actual dollar amount as an annotation.

    entities: ["Top Searcher", "Sandwich Bot #2", ..., "Median Searcher"]
    profits:  [5_200_000, 1_800_000, ..., 3_400]
    """
    # Sort descending
    paired = sorted(zip(entities, profits), key=lambda x: x[1], reverse=True)
    entities = [p[0] for p in paired]
    profits = [p[1] for p in paired]

    # Color gradient: top = bright red, bottom = dim grey
    n = len(entities)
    colors = []
    for i in range(n):
        ratio = i / max(n - 1, 1)
        if ratio < 0.2:
            colors.append("#FF6B6B")   # Top tier — hot red
        elif ratio < 0.5:
            colors.append("#DDA15E")   # Mid tier — amber
        else:
            colors.append("#8B949E")   # Bottom — dim

    fig = go.Figure(
        go.Bar(
            y=entities,
            x=profits,
            orientation="h",
            marker_color=colors,
            text=[_fmt_usd(v) for v in profits],
            textposition="outside",
            textfont=dict(size=13, color=brand.TEXT, family="monospace"),
        )
    )

    fig = _apply_brand(fig, f"MEV Profit Distribution ({period})", "EigenPhi / Flashbots")
    fig.update_layout(
        xaxis=dict(
            type="log",
            title="Profit (USD, log scale)",
            dtick=1,  # 10, 100, 1K, 10K, 100K, 1M, 10M
            gridcolor=brand.GRID,
        ),
        yaxis=dict(autorange="reversed"),
        height=max(400, 50 * n + 100),
    )

    # Add a vertical annotation line at median
    if len(profits) >= 3:
        median_val = sorted(profits)[len(profits) // 2]
        fig.add_vline(
            x=median_val, line_dash="dot", line_color=brand.TEXT_DIM,
            annotation_text=f"Median: {_fmt_usd(median_val)}",
            annotation_font=dict(size=10, color=brand.TEXT_DIM),
        )

    if filename:
        save_chart(fig, filename)
    return fig


# Keep the old name as an alias
def mev_revenue_by_type(
    types: list[str],
    revenues: list[float],
    period: str = "7d",
    filename: str | None = None,
) -> go.Figure:
    """Alias → mev_profit_distribution (log-scale bar chart)."""
    return mev_profit_distribution(types, revenues, period, filename)


def centralization_showdown(
    pool_name: str,
    pool_share: float,
    builder_name: str,
    builder_share: float,
    filename: str | None = None,
) -> go.Figure:
    """Side-by-side "1 entity vs everyone else" chart.

    Instead of showing 63% vs 72% as two similar bars,
    shows each as a single entity dominating an entire pie —
    visually screaming "one player controls most of the network".

    pool_share / builder_share: percentage (0-100)
    """
    fig = make_subplots(
        rows=1, cols=2,
        specs=[[{"type": "pie"}, {"type": "pie"}]],
        subplot_titles=[
            f"⛏️ PoS Pool: {pool_name}",
            f"🏗️ Builder: {builder_name}",
        ],
    )

    # PoS pool: dominant vs rest
    fig.add_trace(
        go.Pie(
            labels=[pool_name, "All Others Combined"],
            values=[pool_share, 100 - pool_share],
            hole=0.55,
            marker=dict(colors=["#FF6B6B", "#21262D"]),
            textinfo="percent",
            textfont=dict(size=16, color=brand.TEXT),
            pull=[0.05, 0],
            direction="clockwise",
            sort=False,
        ),
        row=1, col=1,
    )

    # Builder: dominant vs rest
    fig.add_trace(
        go.Pie(
            labels=[builder_name, "All Others Combined"],
            values=[builder_share, 100 - builder_share],
            hole=0.55,
            marker=dict(colors=["#FF6B6B", "#21262D"]),
            textinfo="percent",
            textfont=dict(size=16, color=brand.TEXT),
            pull=[0.05, 0],
            direction="clockwise",
            sort=False,
        ),
        row=1, col=2,
    )

    # Big percentage in the center of each donut
    fig.add_annotation(
        text=f"<b>{pool_share:.0f}%</b>",
        x=0.19, y=0.5, xref="paper", yref="paper",
        showarrow=False, font=dict(size=36, color="#FF6B6B"),
    )
    fig.add_annotation(
        text=f"<b>{builder_share:.0f}%</b>",
        x=0.81, y=0.5, xref="paper", yref="paper",
        showarrow=False, font=dict(size=36, color="#FF6B6B"),
    )

    fig = _apply_brand(
        fig,
        "Ethereum Centralization: One Entity Dominates Each Layer",
        "Rated Network / Flashbots",
    )
    fig.update_layout(
        showlegend=False,
        height=500,
    )
    # Style subplot titles
    for ann in fig.layout.annotations:
        if "⛏️" in getattr(ann, "text", "") or "🏗️" in getattr(ann, "text", ""):
            ann.font = dict(size=14, color=brand.TEXT)

    if filename:
        save_chart(fig, filename)
    return fig


def builder_dominance(
    dates: list[str],
    builder_data: dict[str, list[float]],
    filename: str | None = None,
) -> go.Figure:
    """Stacked area chart of block builder market share over time.

    builder_data: {builder_name: [market_share_pct_per_date]}
    """
    fig = go.Figure()

    builder_colors = {
        "beaverbuild": "#FF6B6B",
        "Titan": "#4ECDC4",
        "rsync": "#45B7D1",
        "Flashbots": "#FFEAA7",
        "bloXroute": "#DDA15E",
        "BuildAI": "#A29BFE",
    }

    for name, shares in builder_data.items():
        color = builder_colors.get(name, brand.SERIES_COLORS[len(fig.data) % len(brand.SERIES_COLORS)])
        fig.add_trace(
            go.Scatter(
                x=dates,
                y=shares,
                name=name,
                stackgroup="one",
                line=dict(width=0.5),
                fillcolor=color,
            )
        )

    fig = _apply_brand(fig, "Block Builder Market Share", "Flashbots / MEV-Boost")
    fig.update_layout(
        yaxis_title="Market Share (%)",
        yaxis_range=[0, 100],
        legend=dict(orientation="h", yanchor="bottom", y=-0.2),
    )

    if filename:
        save_chart(fig, filename)
    return fig


def censorship_rate(
    dates: list[str],
    ofac_compliant_pct: list[float],
    filename: str | None = None,
) -> go.Figure:
    """Line chart tracking OFAC-compliant block percentage over time."""
    fig = go.Figure(
        go.Scatter(
            x=dates,
            y=ofac_compliant_pct,
            mode="lines+markers",
            line=dict(color=brand.NEGATIVE, width=2),
            marker=dict(size=4),
            fill="tozeroy",
            fillcolor="rgba(255,107,107,0.1)",
        )
    )

    fig = _apply_brand(fig, "OFAC-Compliant Block Rate", "Flashbots / MEV Watch")
    fig.update_layout(
        yaxis_title="OFAC-Compliant Blocks (%)",
        yaxis_range=[0, 100],
    )

    if filename:
        save_chart(fig, filename)
    return fig
