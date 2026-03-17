"""Macro dashboard and BTC correlation charts."""

from __future__ import annotations

import plotly.graph_objects as go
from plotly.subplots import make_subplots

from src.visuals.chart_engine import _apply_brand, save_chart
from src.visuals.styles import arena_brand as brand


def net_liquidity_vs_btc(
    dates: list[str],
    net_liquidity: list[float],
    btc_price: list[float],
    fomc_dates: list[str] | None = None,
    cpi_dates: list[str] | None = None,
    filename: str | None = None,
) -> go.Figure:
    """Dual-axis: Net Liquidity (left) vs BTC Price (right).

    The classic macro-crypto chart. Annotates FOMC and CPI dates.
    """
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Net Liquidity (area)
    fig.add_trace(
        go.Scatter(
            x=dates, y=net_liquidity,
            name="Net Liquidity (Fed BS - TGA - RRP)",
            line=dict(color=brand.ACCENT, width=2),
            fill="tozeroy",
            fillcolor="rgba(78,205,196,0.1)",
        ),
        secondary_y=False,
    )

    # BTC Price
    fig.add_trace(
        go.Scatter(
            x=dates, y=btc_price,
            name="BTC Price",
            line=dict(color="#F7931A", width=2),
        ),
        secondary_y=True,
    )

    # Annotate FOMC meetings
    if fomc_dates:
        for d in fomc_dates:
            fig.add_vline(
                x=d, line_dash="dot", line_color=brand.NEGATIVE,
                opacity=0.4, annotation_text="FOMC",
                annotation_font_size=8, annotation_font_color=brand.TEXT_DIM,
            )

    # Annotate CPI releases
    if cpi_dates:
        for d in cpi_dates:
            fig.add_vline(
                x=d, line_dash="dot", line_color=brand.NEUTRAL,
                opacity=0.3, annotation_text="CPI",
                annotation_font_size=8, annotation_font_color=brand.TEXT_DIM,
            )

    fig = _apply_brand(fig, "Net Liquidity vs BTC Price", "FRED / CoinGecko")
    fig.update_yaxes(title_text="Net Liquidity ($T)", secondary_y=False)
    fig.update_yaxes(title_text="BTC Price (USD)", secondary_y=True, gridcolor="rgba(0,0,0,0)")

    if filename:
        save_chart(fig, filename)
    return fig


def correlation_heatmap(
    assets: list[str],
    correlation_matrix: list[list[float]],
    window: str = "30d",
    filename: str | None = None,
) -> go.Figure:
    """Heatmap of BTC correlation with major asset classes.

    Red = positive correlation, Blue = negative correlation.
    """
    fig = go.Figure(
        go.Heatmap(
            x=assets, y=assets, z=correlation_matrix,
            colorscale="RdBu_r",
            zmid=0, zmin=-1, zmax=1,
            texttemplate="%{z:.2f}",
            textfont=dict(size=10),
            colorbar=dict(title="Correlation"),
        )
    )

    fig = _apply_brand(fig, f"Asset Correlation Matrix ({window} Rolling)", "Yahoo Finance / FRED")

    if filename:
        save_chart(fig, filename)
    return fig


def macro_six_panel(
    dates: list[str],
    btc: list[float],
    dxy: list[float],
    treasury_10y: list[float],
    net_liquidity: list[float],
    vix: list[float],
    spx: list[float],
    filename: str | None = None,
) -> go.Figure:
    """Six-panel macro dashboard: BTC, DXY, 10Y, Net Liq, VIX, S&P 500.

    All normalized to percentage change from start for comparison.
    """
    fig = make_subplots(
        rows=3, cols=2,
        subplot_titles=("BTC", "DXY (USD Index)", "10Y Treasury Yield",
                        "Net Liquidity", "VIX", "S&P 500"),
        vertical_spacing=0.08,
        horizontal_spacing=0.08,
    )

    panels = [
        (btc, "#F7931A", 1, 1),
        (dxy, "#4ECDC4", 1, 2),
        (treasury_10y, "#FF6B6B", 2, 1),
        (net_liquidity, "#96CEB4", 2, 2),
        (vix, "#FFEAA7", 3, 1),
        (spx, "#A29BFE", 3, 2),
    ]

    for data, color, row, col in panels:
        if data:
            fig.add_trace(
                go.Scatter(
                    x=dates, y=data,
                    line=dict(color=color, width=1.5),
                    showlegend=False,
                ),
                row=row, col=col,
            )

    fig = _apply_brand(fig, "Macro Dashboard (90 Days)", "FRED / Yahoo Finance")
    fig.update_layout(height=800)

    if filename:
        save_chart(fig, filename, height=800)
    return fig


def economic_surprise_chart(
    dates: list[str],
    surprise_index: list[float],
    btc_price: list[float],
    filename: str | None = None,
) -> go.Figure:
    """Economic surprise index (Citi-style) vs BTC price."""
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Surprise index as area
    colors = [brand.POSITIVE if s >= 0 else brand.NEGATIVE for s in surprise_index]
    fig.add_trace(
        go.Bar(
            x=dates, y=surprise_index,
            name="Economic Surprise",
            marker_color=colors,
            opacity=0.6,
        ),
        secondary_y=False,
    )

    # BTC Price
    fig.add_trace(
        go.Scatter(
            x=dates, y=btc_price,
            name="BTC Price",
            line=dict(color="#F7931A", width=2),
        ),
        secondary_y=True,
    )

    fig = _apply_brand(fig, "Economic Surprise Index vs BTC", "FRED / CoinGecko")
    fig.update_yaxes(title_text="Surprise (σ)", secondary_y=False)
    fig.update_yaxes(title_text="BTC Price (USD)", secondary_y=True, gridcolor="rgba(0,0,0,0)")

    if filename:
        save_chart(fig, filename)
    return fig


def fomc_impact_chart(
    meetings: list[dict],
    filename: str | None = None,
) -> go.Figure:
    """BTC price change around FOMC decisions (±24h).

    meetings: list of {date, decision, btc_change_24h_before, btc_change_24h_after}
    """
    fig = go.Figure()

    for i, m in enumerate(meetings):
        color = brand.POSITIVE if m.get("btc_change_24h_after", 0) >= 0 else brand.NEGATIVE
        fig.add_trace(
            go.Bar(
                x=[m["date"]],
                y=[m.get("btc_change_24h_after", 0)],
                name=f"{m['date']}: {m.get('decision', '')}",
                marker_color=color,
                text=[f"{m.get('btc_change_24h_after', 0):+.1f}%"],
                textposition="outside",
                showlegend=False,
            )
        )

    fig = _apply_brand(fig, "BTC Price Change After FOMC Decisions (24h)", "FRED / CoinGecko")
    fig.update_layout(
        yaxis_title="BTC Change (%)",
        xaxis_title="FOMC Meeting Date",
    )
    fig.add_hline(y=0, line_dash="dash", line_color=brand.GRID)

    if filename:
        save_chart(fig, filename)
    return fig


def regulation_timeline(
    events: list[dict],
    btc_dates: list[str] | None = None,
    btc_prices: list[float] | None = None,
    filename: str | None = None,
) -> go.Figure:
    """Regulatory event timeline with optional BTC price overlay.

    events: list of {date, title, impact: "positive"|"negative"|"neutral", magnitude: 1-10}
    """
    fig = go.Figure()

    # BTC price background
    if btc_dates and btc_prices:
        fig.add_trace(
            go.Scatter(
                x=btc_dates, y=btc_prices,
                name="BTC Price",
                line=dict(color="#F7931A", width=1),
                opacity=0.4,
                yaxis="y2",
            )
        )

    impact_colors = {
        "positive": brand.POSITIVE,
        "negative": brand.NEGATIVE,
        "neutral": brand.NEUTRAL,
    }

    for e in events:
        color = impact_colors.get(e.get("impact", "neutral"), brand.NEUTRAL)
        size = max(8, min(30, e.get("magnitude", 5) * 3))

        fig.add_trace(
            go.Scatter(
                x=[e["date"]],
                y=[e.get("magnitude", 5)],
                mode="markers+text",
                marker=dict(size=size, color=color, opacity=0.7),
                text=[e.get("title", "")[:30]],
                textposition="top center",
                textfont=dict(size=8, color=brand.TEXT_DIM),
                showlegend=False,
            )
        )

    fig = _apply_brand(fig, "Crypto Regulatory Event Timeline", "SEC / CFTC / Congress")
    fig.update_layout(
        yaxis_title="Impact Magnitude",
        yaxis2=dict(overlaying="y", side="right", title="BTC Price"),
    )

    if filename:
        save_chart(fig, filename)
    return fig
