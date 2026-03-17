"""Chart generation engine using Plotly with Arena branding."""

from __future__ import annotations

from pathlib import Path

import plotly.graph_objects as go
import plotly.io as pio

from src.config import OUTPUT_DIR
from src.visuals.styles import arena_brand as brand

CHARTS_DIR = OUTPUT_DIR / "charts"
CHARTS_DIR.mkdir(parents=True, exist_ok=True)


def _apply_brand(fig: go.Figure, title: str, data_source: str = "") -> go.Figure:
    """Apply Arena brand styling to any figure."""
    layout = dict(brand.PLOTLY_LAYOUT)
    layout["title"] = dict(text=title, font=dict(size=brand.FONT_SIZE_TITLE, color=brand.TEXT))

    # Add data source attribution
    annotations = list(layout.get("annotations", []))
    if data_source:
        annotations.append(
            dict(
                text=f"Data: {data_source}",
                xref="paper", yref="paper",
                x=0.02, y=-0.12,
                showarrow=False,
                font=dict(size=9, color=brand.TEXT_DIM),
                xanchor="left",
            )
        )
    layout["annotations"] = annotations

    fig.update_layout(**layout)
    return fig


def save_chart(fig: go.Figure, filename: str, width: int = 1200, height: int = 630) -> Path:
    """Save chart as PNG for social media (1200x630 = Twitter card)."""
    path = CHARTS_DIR / f"{filename}.png"
    pio.write_image(fig, str(path), width=width, height=height, scale=2)
    return path


def bar_chart(
    labels: list[str],
    values: list[float],
    title: str,
    data_source: str = "",
    horizontal: bool = True,
    color_positive: str = brand.POSITIVE,
    color_negative: str = brand.NEGATIVE,
    filename: str | None = None,
) -> go.Figure:
    """Create a branded bar chart (default horizontal for leaderboards)."""
    colors = [color_positive if v >= 0 else color_negative for v in values]

    if horizontal:
        fig = go.Figure(go.Bar(y=labels, x=values, orientation="h", marker_color=colors))
    else:
        fig = go.Figure(go.Bar(x=labels, y=values, marker_color=colors))

    fig = _apply_brand(fig, title, data_source)

    if filename:
        save_chart(fig, filename)
    return fig


def line_chart(
    x_data: list,
    y_series: dict[str, list[float]],
    title: str,
    data_source: str = "",
    x_title: str = "",
    y_title: str = "",
    filename: str | None = None,
) -> go.Figure:
    """Create a multi-series line chart."""
    fig = go.Figure()
    for i, (name, values) in enumerate(y_series.items()):
        color = brand.SERIES_COLORS[i % len(brand.SERIES_COLORS)]
        fig.add_trace(go.Scatter(x=x_data, y=values, name=name, line=dict(color=color, width=2)))

    fig = _apply_brand(fig, title, data_source)
    if x_title:
        fig.update_xaxes(title_text=x_title)
    if y_title:
        fig.update_yaxes(title_text=y_title)

    if filename:
        save_chart(fig, filename)
    return fig


def heatmap(
    x_labels: list[str],
    y_labels: list[str],
    z_data: list[list[float]],
    title: str,
    data_source: str = "",
    colorscale: str = "RdYlGn",
    filename: str | None = None,
) -> go.Figure:
    """Create a heatmap (good for funding rates, correlation matrices)."""
    fig = go.Figure(
        go.Heatmap(
            x=x_labels, y=y_labels, z=z_data,
            colorscale=colorscale,
            texttemplate="%{z:.3f}",
            textfont=dict(size=10),
        )
    )
    fig = _apply_brand(fig, title, data_source)

    if filename:
        save_chart(fig, filename)
    return fig


def treemap(
    labels: list[str],
    parents: list[str],
    values: list[float],
    title: str,
    data_source: str = "",
    filename: str | None = None,
) -> go.Figure:
    """Create a treemap (good for TVL distribution)."""
    fig = go.Figure(
        go.Treemap(
            labels=labels,
            parents=parents,
            values=values,
            marker=dict(colors=brand.SERIES_COLORS * (len(labels) // len(brand.SERIES_COLORS) + 1)),
            textinfo="label+value+percent parent",
        )
    )
    fig = _apply_brand(fig, title, data_source)

    if filename:
        save_chart(fig, filename)
    return fig
