"""Developer activity charts — commits, contributors, ecosystem growth."""

from __future__ import annotations

import plotly.graph_objects as go

from src.visuals.chart_engine import _apply_brand, save_chart
from src.visuals.styles import arena_brand as brand


def dev_activity_bubble(
    ecosystems: list[dict],
    filename: str | None = None,
) -> go.Figure:
    """Bubble chart: x=developer count, y=growth rate, size=TVL.

    ecosystems: list of {name, developers, growth_pct, tvl}
    """
    fig = go.Figure()

    for i, eco in enumerate(ecosystems):
        color = brand.SERIES_COLORS[i % len(brand.SERIES_COLORS)]
        tvl = eco.get("tvl", 1e8)
        size = max(10, min(80, (tvl / 1e9) * 15))  # Scale bubble size

        fig.add_trace(
            go.Scatter(
                x=[eco.get("developers", 0)],
                y=[eco.get("growth_pct", 0)],
                mode="markers+text",
                marker=dict(size=size, color=color, opacity=0.7, line=dict(width=1, color=brand.TEXT)),
                text=[eco["name"]],
                textposition="top center",
                textfont=dict(size=10, color=brand.TEXT),
                name=eco["name"],
                showlegend=False,
            )
        )

    fig = _apply_brand(fig, "Ecosystem Developer Activity", "Artemis / Electric Capital")
    fig.update_layout(
        xaxis_title="Monthly Active Developers",
        yaxis_title="Developer Growth (% YoY)",
    )

    # Add zero line
    fig.add_hline(y=0, line_dash="dash", line_color=brand.GRID, opacity=0.5)

    if filename:
        save_chart(fig, filename)
    return fig


def commit_activity_heatmap(
    repos: list[str],
    weeks: list[str],
    commits: list[list[int]],
    filename: str | None = None,
) -> go.Figure:
    """Heatmap of commit activity across repos over time.

    repos: Repository names (y-axis)
    weeks: Week labels (x-axis)
    commits: 2D array [repo_idx][week_idx] = commit_count
    """
    fig = go.Figure(
        go.Heatmap(
            x=weeks,
            y=repos,
            z=commits,
            colorscale=[
                [0, brand.BACKGROUND],
                [0.25, "#0E4429"],
                [0.5, "#006D32"],
                [0.75, "#26A641"],
                [1, "#39D353"],
            ],
            showscale=True,
            colorbar=dict(title="Commits", titleside="right"),
        )
    )

    fig = _apply_brand(fig, "Repository Commit Activity", "GitHub")
    fig.update_layout(
        height=max(400, len(repos) * 25),
    )

    if filename:
        save_chart(fig, filename, height=max(400, len(repos) * 25))
    return fig


def developer_count_ranking(
    ecosystems: list[dict],
    top_n: int = 15,
    filename: str | None = None,
) -> go.Figure:
    """Horizontal bar chart ranking ecosystems by developer count.

    ecosystems: list of {name, total_devs, full_time_devs}
    """
    ecosystems = sorted(ecosystems, key=lambda e: e.get("total_devs", 0))[-top_n:]

    fig = go.Figure()

    fig.add_trace(
        go.Bar(
            y=[e["name"] for e in ecosystems],
            x=[e.get("full_time_devs", 0) for e in ecosystems],
            name="Full-time",
            orientation="h",
            marker_color=brand.PRIMARY,
        )
    )
    fig.add_trace(
        go.Bar(
            y=[e["name"] for e in ecosystems],
            x=[e.get("total_devs", 0) - e.get("full_time_devs", 0) for e in ecosystems],
            name="Part-time",
            orientation="h",
            marker_color=brand.ACCENT,
        )
    )

    fig = _apply_brand(fig, f"Top {top_n} Ecosystems by Developer Count", "Electric Capital")
    fig.update_layout(
        barmode="stack",
        xaxis_title="Monthly Active Developers",
        height=max(400, top_n * 30),
    )

    if filename:
        save_chart(fig, filename, height=max(400, top_n * 30))
    return fig
