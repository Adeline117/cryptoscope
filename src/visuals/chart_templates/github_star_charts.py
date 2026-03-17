"""GitHub Star Velocity chart templates — Arena-branded Plotly visualizations."""

from __future__ import annotations

from typing import Any

import plotly.graph_objects as go

from src.visuals.chart_engine import _apply_brand, save_chart
from src.visuals.styles import arena_brand as brand

# Category color mapping for consistent visual identity
CATEGORY_COLORS: dict[str, str] = {
    "defi": "#45B7D1",           # Blue
    "ai_crypto": "#A29BFE",     # Purple
    "infrastructure": "#00D4AA", # Green (Arena teal)
    "zk": "#E17055",            # Orange
    "dev_tools": "#FFEAA7",     # Yellow
    "mev_infra": "#FD79A8",     # Pink
    "wallets_aa": "#6C5CE7",    # Deep purple
    "interop": "#96CEB4",       # Sage
    "data_oracles": "#DDA15E",  # Amber
    "unknown": "#8B949E",       # Gray
}


def star_velocity_chart(
    dates: list[str],
    repo_series: dict[str, list[float]],
    anomaly_points: list[dict[str, Any]] | None = None,
    title: str = "Daily Star Velocity",
    filename: str | None = None,
) -> go.Figure:
    """Time series of daily new stars for multiple repos with anomaly annotations.

    Args:
        dates: List of date strings for the x-axis (e.g., ["2026-03-01", ...]).
        repo_series: Mapping of repo name to list of daily star counts.
            Example: {"paradigmxyz/reth": [12, 15, 45, ...]}
        anomaly_points: Optional list of anomaly markers.
            Each dict: {"repo": str, "date": str, "value": float, "alert_level": str}
        title: Chart title.
        filename: If provided, save chart as PNG.

    Returns:
        Plotly Figure with Arena branding.
    """
    fig = go.Figure()

    for i, (repo_name, values) in enumerate(repo_series.items()):
        color = brand.SERIES_COLORS[i % len(brand.SERIES_COLORS)]
        # Use short name for legend
        short_name = repo_name.split("/")[-1] if "/" in repo_name else repo_name

        fig.add_trace(
            go.Scatter(
                x=dates,
                y=values,
                name=short_name,
                mode="lines",
                line=dict(color=color, width=2),
                hovertemplate=f"<b>{repo_name}</b><br>"
                              "Date: %{x}<br>"
                              "Stars: %{y:.0f}/day<extra></extra>",
            )
        )

    # Annotate anomaly points
    if anomaly_points:
        level_colors = {"HIGH": "#FF6B6B", "MEDIUM": "#FFEAA7", "NOTABLE": "#4ECDC4"}
        level_symbols = {"HIGH": "star", "MEDIUM": "diamond", "NOTABLE": "circle"}

        for anom in anomaly_points:
            alert = anom.get("alert_level", "NOTABLE")
            fig.add_trace(
                go.Scatter(
                    x=[anom["date"]],
                    y=[anom["value"]],
                    mode="markers+text",
                    marker=dict(
                        size=14 if alert == "HIGH" else 10,
                        color=level_colors.get(alert, brand.ACCENT),
                        symbol=level_symbols.get(alert, "circle"),
                        line=dict(width=1, color=brand.TEXT),
                    ),
                    text=[f"{anom['repo'].split('/')[-1]}"],
                    textposition="top center",
                    textfont=dict(size=9, color=brand.TEXT_DIM),
                    showlegend=False,
                    hovertemplate=(
                        f"<b>{anom['repo']}</b><br>"
                        f"Alert: {alert}<br>"
                        "Stars/day: %{y:.0f}<extra></extra>"
                    ),
                )
            )

    fig = _apply_brand(fig, title, "GitHub API")
    fig.update_layout(
        xaxis_title="Date",
        yaxis_title="Stars / Day",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
            font=dict(size=10, color=brand.TEXT_DIM),
        ),
        hovermode="x unified",
    )

    if filename:
        save_chart(fig, filename)
    return fig


def star_ranking_bar(
    repos: list[dict[str, Any]],
    top_n: int = 20,
    metric: str = "stars_7d",
    title: str = "Top Repos by Weekly Star Growth",
    filename: str | None = None,
) -> go.Figure:
    """Horizontal bar chart of top N repos by star growth, colored by category.

    Args:
        repos: List of dicts with keys: repo, stars_7d (or metric), category.
            Example: [{"repo": "paradigmxyz/reth", "stars_7d": 350, "category": "infrastructure"}]
        top_n: Number of repos to display.
        metric: Key in each dict to rank by (default "stars_7d").
        title: Chart title.
        filename: If provided, save chart as PNG.

    Returns:
        Plotly Figure with Arena branding.
    """
    # Sort and take top N
    sorted_repos = sorted(repos, key=lambda r: r.get(metric, 0))[-top_n:]

    labels = [r["repo"].split("/")[-1] if "/" in r["repo"] else r["repo"] for r in sorted_repos]
    values = [r.get(metric, 0) for r in sorted_repos]
    colors = [
        CATEGORY_COLORS.get(r.get("category", "unknown"), CATEGORY_COLORS["unknown"])
        for r in sorted_repos
    ]
    full_names = [r["repo"] for r in sorted_repos]

    fig = go.Figure(
        go.Bar(
            y=labels,
            x=values,
            orientation="h",
            marker_color=colors,
            customdata=full_names,
            hovertemplate="<b>%{customdata}</b><br>"
                          f"{metric}: %{{x:,.0f}}<extra></extra>",
        )
    )

    fig = _apply_brand(fig, title, "GitHub API")
    fig.update_layout(
        xaxis_title=f"Stars ({metric.replace('_', ' ').title()})",
        yaxis=dict(tickfont=dict(size=11)),
        height=max(450, top_n * 28),
    )

    # Add category legend manually (one invisible trace per category)
    seen_categories: set[str] = set()
    for r in sorted_repos:
        cat = r.get("category", "unknown")
        if cat not in seen_categories:
            seen_categories.add(cat)
            fig.add_trace(
                go.Scatter(
                    x=[None],
                    y=[None],
                    mode="markers",
                    marker=dict(size=10, color=CATEGORY_COLORS.get(cat, brand.NEUTRAL)),
                    name=cat.replace("_", " ").title(),
                    showlegend=True,
                )
            )

    if filename:
        save_chart(fig, filename, height=max(450, top_n * 28))
    return fig


def ecosystem_star_treemap(
    repos: list[dict[str, Any]],
    title: str = "Crypto Ecosystem Star Map",
    filename: str | None = None,
) -> go.Figure:
    """Treemap grouped by ecosystem/category, area=total stars, color=growth rate.

    Args:
        repos: List of dicts with keys: repo, category, total_stars, growth_rate_pct.
            growth_rate_pct is the % star growth over the period.
        title: Chart title.
        filename: If provided, save chart as PNG.

    Returns:
        Plotly Figure with Arena branding.
    """
    labels: list[str] = ["Crypto Ecosystem"]
    parents: list[str] = [""]
    values: list[float] = [0]
    colors: list[float] = [0]
    custom_text: list[str] = [""]

    # Add category parents
    categories: dict[str, list[dict[str, Any]]] = {}
    for r in repos:
        cat = r.get("category", "unknown")
        categories.setdefault(cat, []).append(r)

    for cat, cat_repos in categories.items():
        display_name = cat.replace("_", " ").title()
        labels.append(display_name)
        parents.append("Crypto Ecosystem")
        total = sum(r.get("total_stars", 0) for r in cat_repos)
        values.append(total)
        avg_growth = (
            sum(r.get("growth_rate_pct", 0) for r in cat_repos) / len(cat_repos)
            if cat_repos
            else 0
        )
        colors.append(avg_growth)
        custom_text.append(f"{len(cat_repos)} repos | {total:,} stars")

        # Add individual repos
        for r in cat_repos:
            short = r["repo"].split("/")[-1] if "/" in r["repo"] else r["repo"]
            labels.append(short)
            parents.append(display_name)
            values.append(r.get("total_stars", 0))
            colors.append(r.get("growth_rate_pct", 0))
            custom_text.append(
                f"{r['repo']}<br>"
                f"Stars: {r.get('total_stars', 0):,}<br>"
                f"Growth: {r.get('growth_rate_pct', 0):+.1f}%"
            )

    fig = go.Figure(
        go.Treemap(
            labels=labels,
            parents=parents,
            values=values,
            marker=dict(
                colors=colors,
                colorscale=[
                    [0.0, brand.NEGATIVE],     # negative growth = red
                    [0.3, "#FFEAA7"],           # low growth = yellow
                    [0.5, brand.ACCENT],        # moderate growth = teal
                    [1.0, brand.PRIMARY],       # high growth = green
                ],
                colorbar=dict(
                    title=dict(text="Growth %", font=dict(color=brand.TEXT_DIM)),
                    tickfont=dict(color=brand.TEXT_DIM),
                ),
                cmid=0,
            ),
            customdata=custom_text,
            hovertemplate="%{customdata}<extra></extra>",
            textinfo="label+value",
            texttemplate="<b>%{label}</b><br>%{value:,.0f}",
            textfont=dict(size=11),
            branchvalues="total",
        )
    )

    fig = _apply_brand(fig, title, "GitHub API")
    fig.update_layout(
        height=700,
        margin=dict(l=10, r=10, t=60, b=40),
    )

    if filename:
        save_chart(fig, filename, height=700)
    return fig


def new_repo_radar(
    repo: dict[str, Any],
    title: str | None = None,
    filename: str | None = None,
) -> go.Figure:
    """Radar chart evaluating a new repo across multiple quality dimensions.

    Args:
        repo: Dict with keys scored 0-100:
            - repo: str (full name)
            - stars: int (normalized to 0-100 score)
            - commits: int (normalized)
            - contributors: int (normalized)
            - issues: int (open issue activity, normalized)
            - forks: int (normalized)
            - age_days: int (repo age in days)
            Each dimension should have a raw value and an optional *_score (0-100).
        title: Chart title (auto-generated if None).
        filename: If provided, save chart as PNG.

    Returns:
        Plotly Figure with Arena branding.
    """
    repo_name = repo.get("repo", "Unknown")
    if title is None:
        short = repo_name.split("/")[-1] if "/" in repo_name else repo_name
        title = f"New Repo Assessment: {short}"

    # Dimensions and their scores
    dimensions = ["Stars", "Commits", "Contributors", "Issues", "Forks", "Maturity"]
    scores = [
        repo.get("stars_score", 0),
        repo.get("commits_score", 0),
        repo.get("contributors_score", 0),
        repo.get("issues_score", 0),
        repo.get("forks_score", 0),
        repo.get("maturity_score", 0),
    ]

    # Close the polygon
    dimensions_closed = dimensions + [dimensions[0]]
    scores_closed = scores + [scores[0]]

    fig = go.Figure()

    # Filled area
    fig.add_trace(
        go.Scatterpolar(
            r=scores_closed,
            theta=dimensions_closed,
            fill="toself",
            fillcolor=f"rgba(0, 212, 170, 0.2)",
            line=dict(color=brand.PRIMARY, width=2),
            name=repo_name,
            hovertemplate="<b>%{theta}</b>: %{r}/100<extra></extra>",
        )
    )

    # Score points
    fig.add_trace(
        go.Scatterpolar(
            r=scores,
            theta=dimensions,
            mode="markers+text",
            marker=dict(size=8, color=brand.PRIMARY),
            text=[str(s) for s in scores],
            textposition="top center",
            textfont=dict(size=10, color=brand.TEXT),
            showlegend=False,
        )
    )

    fig = _apply_brand(fig, title, "GitHub API")
    fig.update_layout(
        polar=dict(
            bgcolor=brand.BACKGROUND,
            radialaxis=dict(
                visible=True,
                range=[0, 100],
                gridcolor=brand.GRID,
                tickfont=dict(color=brand.TEXT_DIM, size=9),
            ),
            angularaxis=dict(
                gridcolor=brand.GRID,
                tickfont=dict(color=brand.TEXT, size=11),
            ),
        ),
        height=550,
    )

    # Add overall score annotation
    overall = round(sum(scores) / len(scores), 1)
    fig.add_annotation(
        text=f"Overall: {overall}/100",
        xref="paper",
        yref="paper",
        x=0.5,
        y=-0.05,
        showarrow=False,
        font=dict(size=14, color=brand.PRIMARY),
    )

    if filename:
        save_chart(fig, filename, height=550)
    return fig
