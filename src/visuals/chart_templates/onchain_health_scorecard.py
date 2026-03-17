"""On-chain health scorecard — multi-indicator dashboard panel."""

from __future__ import annotations

import plotly.graph_objects as go
from plotly.subplots import make_subplots

from src.visuals.chart_engine import _apply_brand, save_chart
from src.visuals.styles import arena_brand as brand


def health_scorecard(
    metrics: list[dict],
    title: str = "On-Chain Health Scorecard",
    filename: str | None = None,
) -> go.Figure:
    """Multi-gauge dashboard showing key on-chain health indicators.

    metrics: list of {
        name: str,
        value: float,
        min_val: float,
        max_val: float,
        status: "bullish"|"neutral"|"bearish",
        description: str
    }
    """
    n = len(metrics)
    cols = min(n, 3)
    rows = (n + cols - 1) // cols

    fig = make_subplots(
        rows=rows,
        cols=cols,
        specs=[[{"type": "indicator"}] * cols for _ in range(rows)],
        horizontal_spacing=0.05,
        vertical_spacing=0.15,
    )

    status_colors = {
        "bullish": brand.POSITIVE,
        "neutral": brand.NEUTRAL,
        "bearish": brand.NEGATIVE,
    }

    for i, m in enumerate(metrics):
        row = i // cols + 1
        col = i % cols + 1
        color = status_colors.get(m.get("status", "neutral"), brand.NEUTRAL)

        fig.add_trace(
            go.Indicator(
                mode="gauge+number",
                value=m["value"],
                title=dict(text=m["name"], font=dict(size=12, color=brand.TEXT)),
                number=dict(font=dict(size=18, color=color)),
                gauge=dict(
                    axis=dict(
                        range=[m.get("min_val", 0), m.get("max_val", 100)],
                        tickcolor=brand.TEXT_DIM,
                    ),
                    bar=dict(color=color),
                    bgcolor=brand.SURFACE,
                    bordercolor=brand.GRID,
                    steps=[
                        dict(range=[m.get("min_val", 0), m.get("max_val", 100) * 0.33], color="#1a1e25"),
                        dict(range=[m.get("max_val", 100) * 0.33, m.get("max_val", 100) * 0.66], color="#1e2430"),
                        dict(range=[m.get("max_val", 100) * 0.66, m.get("max_val", 100)], color="#22293a"),
                    ],
                ),
            ),
            row=row,
            col=col,
        )

    fig = _apply_brand(fig, title, "Glassnode / CryptoQuant / Coin Metrics")
    fig.update_layout(
        height=280 * rows,
    )

    if filename:
        save_chart(fig, filename, height=280 * rows)
    return fig


def indicator_table(
    indicators: list[dict],
    filename: str | None = None,
) -> go.Figure:
    """Formatted table of on-chain indicators as an image.

    indicators: list of {name, value, change, signal, source}
    """
    signal_emojis = {"bullish": "🟢", "neutral": "🟡", "bearish": "🔴"}

    fig = go.Figure(
        go.Table(
            header=dict(
                values=["Indicator", "Value", "Change", "Signal", "Source"],
                fill_color=brand.SURFACE,
                font=dict(color=brand.TEXT, size=11),
                align="left",
                line_color=brand.GRID,
            ),
            cells=dict(
                values=[
                    [i["name"] for i in indicators],
                    [i["value"] for i in indicators],
                    [i.get("change", "—") for i in indicators],
                    [signal_emojis.get(i.get("signal", ""), "⚪") + " " + i.get("signal", "").title() for i in indicators],
                    [i.get("source", "") for i in indicators],
                ],
                fill_color=brand.BACKGROUND,
                font=dict(color=brand.TEXT, size=10),
                align="left",
                line_color=brand.GRID,
                height=28,
            ),
        )
    )

    fig = _apply_brand(fig, "On-Chain Indicator Summary", "Multiple Sources")
    fig.update_layout(
        height=max(300, 28 * len(indicators) + 100),
    )

    if filename:
        save_chart(fig, filename, height=max(300, 28 * len(indicators) + 100))
    return fig
