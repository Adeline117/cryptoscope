"""TVL comparison charts for protocols and chains."""

from __future__ import annotations

import plotly.graph_objects as go

from src.visuals.chart_engine import _apply_brand, save_chart
from src.visuals.styles import arena_brand as brand


def top_protocols_tvl(
    protocols: list[dict],
    top_n: int = 20,
    filename: str | None = None,
) -> go.Figure:
    """Horizontal bar chart of top protocols by TVL.

    protocols: list with keys: name, tvl, change_1d, category
    """
    protocols = sorted(protocols, key=lambda p: p.get("tvl", 0))[-top_n:]

    labels = [p["name"] for p in protocols]
    values = [p.get("tvl", 0) for p in protocols]

    fig = go.Figure(
        go.Bar(
            y=labels,
            x=values,
            orientation="h",
            marker_color=brand.PRIMARY,
            text=[f"${v/1e9:.2f}B" for v in values],
            textposition="outside",
            textfont=dict(size=9, color=brand.TEXT),
        )
    )

    fig = _apply_brand(fig, f"Top {top_n} Protocols by TVL", "DeFiLlama")
    fig.update_layout(
        xaxis_title="TVL (USD)",
        height=max(500, top_n * 30),
    )

    if filename:
        save_chart(fig, filename, height=max(500, top_n * 30))
    return fig


def chain_tvl_treemap(
    chains: list[dict],
    top_n: int = 30,
    filename: str | None = None,
) -> go.Figure:
    """Treemap of TVL distribution by chain.

    chains: list with keys: name, tvl
    """
    chains = sorted(chains, key=lambda c: c.get("tvl", 0), reverse=True)[:top_n]

    fig = go.Figure(
        go.Treemap(
            labels=[c["name"] for c in chains],
            parents=[""] * len(chains),
            values=[c.get("tvl", 0) for c in chains],
            textinfo="label+value+percent root",
            texttemplate="%{label}<br>$%{value:,.0f}<br>%{percentRoot:.1%}",
            marker=dict(
                colors=brand.SERIES_COLORS * (len(chains) // len(brand.SERIES_COLORS) + 1),
            ),
        )
    )

    fig = _apply_brand(fig, "TVL Distribution by Chain", "DeFiLlama")
    if filename:
        save_chart(fig, filename)
    return fig
