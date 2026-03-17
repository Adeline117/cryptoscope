"""L2 comparison charts — TVL, TPS, fees, active addresses."""

from __future__ import annotations

import math

import plotly.graph_objects as go
from plotly.subplots import make_subplots

from src.visuals.chart_engine import _apply_brand, save_chart
from src.visuals.styles import arena_brand as brand

L2_COLORS = {
    "Arbitrum": "#28A0F0",
    "Optimism": "#FF0420",
    "Base": "#0052FF",
    "zkSync Era": "#4E529A",
    "StarkNet": "#EC796B",
    "Scroll": "#FFEEDA",
    "Linea": "#61DFFF",
    "Mantle": "#000000",
    "Blast": "#FCFC03",
    "Mode": "#DFFE00",
    "Metis": "#00DACC",
    "Polygon zkEVM": "#8247E5",
    "Manta Pacific": "#15B2C0",
    "ZKFair": "#2B6CB0",
}


def l2_tvl_ranking(
    l2s: list[dict],
    filename: str | None = None,
) -> go.Figure:
    """Horizontal bar chart of L2 TVL rankings.

    l2s: list of {name, tvl, change_7d}
    """
    l2s = sorted(l2s, key=lambda x: x.get("tvl", 0))

    colors = [L2_COLORS.get(l["name"], brand.PRIMARY) for l in l2s]

    fig = go.Figure(
        go.Bar(
            y=[l["name"] for l in l2s],
            x=[l.get("tvl", 0) for l in l2s],
            orientation="h",
            marker_color=colors,
            text=[f"${l.get('tvl', 0)/1e9:.2f}B" for l in l2s],
            textposition="outside",
            textfont=dict(size=10, color=brand.TEXT),
        )
    )

    fig = _apply_brand(fig, "L2 TVL Rankings", "L2Beat / DeFiLlama")
    fig.update_layout(
        xaxis_title="TVL (USD)",
        height=max(400, len(l2s) * 35),
    )

    if filename:
        save_chart(fig, filename, height=max(400, len(l2s) * 35))
    return fig


def l2_radar_comparison(
    l2s: list[dict],
    metrics: list[str] | None = None,
    filename: str | None = None,
) -> go.Figure:
    """Radar chart comparing L2s across multiple dimensions.

    l2s: list of {name, tvl, tps, fees, active_addresses, protocols}
    Each metric normalized to 0-1 scale.
    """
    if metrics is None:
        metrics = ["TVL", "TPS", "Low Fees", "Active Addr", "Protocols"]

    fig = go.Figure()

    for l2 in l2s[:6]:  # Max 6 for readability
        values = [l2.get(m.lower().replace(" ", "_"), 0) for m in metrics]
        values.append(values[0])  # Close the polygon
        categories = metrics + [metrics[0]]

        color = L2_COLORS.get(l2["name"], brand.PRIMARY)
        fig.add_trace(
            go.Scatterpolar(
                r=values,
                theta=categories,
                name=l2["name"],
                line=dict(color=color, width=2),
                fill="toself",
                fillcolor=color,
                opacity=0.3,
            )
        )

    fig = _apply_brand(fig, "L2 Multi-Metric Comparison", "L2Beat / GrowThePie")
    fig.update_layout(
        polar=dict(
            bgcolor=brand.BACKGROUND,
            radialaxis=dict(visible=True, gridcolor=brand.GRID, color=brand.TEXT_DIM),
            angularaxis=dict(gridcolor=brand.GRID, color=brand.TEXT),
        ),
    )

    if filename:
        save_chart(fig, filename)
    return fig


def l2_fees_comparison(
    l2s: list[dict],
    filename: str | None = None,
) -> go.Figure:
    """Grouped bar chart comparing L2 transaction fees.

    l2s: list of {name, avg_fee, median_fee, swap_fee}
    """
    names = [l["name"] for l in l2s]
    colors_list = [L2_COLORS.get(n, brand.PRIMARY) for n in names]

    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=names,
        y=[l.get("avg_fee", 0) for l in l2s],
        name="Avg Transfer",
        marker_color=brand.PRIMARY,
    ))
    fig.add_trace(go.Bar(
        x=names,
        y=[l.get("swap_fee", 0) for l in l2s],
        name="Avg Swap",
        marker_color=brand.ACCENT,
    ))

    fig = _apply_brand(fig, "L2 Transaction Fee Comparison", "GrowThePie")
    fig.update_layout(
        barmode="group",
        yaxis_title="Fee (USD)",
    )

    if filename:
        save_chart(fig, filename)
    return fig
