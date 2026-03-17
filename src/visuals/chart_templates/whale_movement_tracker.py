"""Whale movement tracking charts — large transfers, accumulation/distribution."""

from __future__ import annotations

import plotly.graph_objects as go

from src.visuals.chart_engine import _apply_brand, save_chart
from src.visuals.styles import arena_brand as brand


def whale_transactions_timeline(
    transactions: list[dict],
    filename: str | None = None,
) -> go.Figure:
    """Timeline chart of large whale transactions.

    transactions: list of {
        timestamp: str,
        amount_usd: float,
        direction: "in"|"out",  (in = to exchange, out = from exchange)
        entity: str,  (exchange name or wallet label)
        asset: str
    }
    """
    fig = go.Figure()

    inflows = [t for t in transactions if t.get("direction") == "in"]
    outflows = [t for t in transactions if t.get("direction") == "out"]

    if inflows:
        fig.add_trace(
            go.Scatter(
                x=[t["timestamp"] for t in inflows],
                y=[t["amount_usd"] for t in inflows],
                mode="markers+text",
                marker=dict(
                    size=[min(30, max(8, t["amount_usd"] / 1e7)) for t in inflows],
                    color=brand.NEGATIVE,
                    symbol="triangle-down",
                    opacity=0.7,
                ),
                text=[f"{t.get('entity', '')[:15]}" for t in inflows],
                textposition="top center",
                textfont=dict(size=8, color=brand.TEXT_DIM),
                name="To Exchange (Sell Signal)",
            )
        )

    if outflows:
        fig.add_trace(
            go.Scatter(
                x=[t["timestamp"] for t in outflows],
                y=[t["amount_usd"] for t in outflows],
                mode="markers+text",
                marker=dict(
                    size=[min(30, max(8, t["amount_usd"] / 1e7)) for t in outflows],
                    color=brand.POSITIVE,
                    symbol="triangle-up",
                    opacity=0.7,
                ),
                text=[f"{t.get('entity', '')[:15]}" for t in outflows],
                textposition="top center",
                textfont=dict(size=8, color=brand.TEXT_DIM),
                name="From Exchange (Accumulation)",
            )
        )

    fig = _apply_brand(fig, "Whale Transaction Activity", "Arkham / Nansen / Whale Alert")
    fig.update_layout(
        yaxis_title="Transaction Size (USD)",
        yaxis_type="log",
    )

    if filename:
        save_chart(fig, filename)
    return fig


def accumulation_distribution(
    dates: list[str],
    whale_balance_change: list[float],
    price: list[float] | None = None,
    asset: str = "BTC",
    filename: str | None = None,
) -> go.Figure:
    """Whale accumulation/distribution chart with optional price overlay.

    whale_balance_change: Daily net change in whale holdings (positive = accumulation)
    """
    from plotly.subplots import make_subplots

    has_price = price is not None
    fig = make_subplots(specs=[[{"secondary_y": has_price}]])

    colors = [brand.POSITIVE if v >= 0 else brand.NEGATIVE for v in whale_balance_change]

    fig.add_trace(
        go.Bar(
            x=dates,
            y=whale_balance_change,
            name="Whale Net Change",
            marker_color=colors,
            opacity=0.8,
        )
    )

    if price:
        fig.add_trace(
            go.Scatter(
                x=dates,
                y=price,
                name=f"{asset} Price",
                line=dict(color="#FFEAA7", width=2),
            ),
            secondary_y=True,
        )
        fig.update_yaxes(
            title_text=f"{asset} Price",
            secondary_y=True,
            gridcolor="rgba(0,0,0,0)",
        )

    fig = _apply_brand(
        fig,
        f"{asset} Whale Accumulation / Distribution",
        "Glassnode / CryptoQuant",
    )
    fig.update_layout(yaxis_title="Net Balance Change")

    if filename:
        save_chart(fig, filename)
    return fig


def top_whale_holders(
    wallets: list[dict],
    asset: str = "BTC",
    filename: str | None = None,
) -> go.Figure:
    """Horizontal bar chart of top whale wallets by holdings.

    wallets: list of {label, balance, change_7d, entity_type}
    """
    wallets = sorted(wallets, key=lambda w: w.get("balance", 0))[-15:]

    labels = [w.get("label", w.get("address", "")[:12]) for w in wallets]
    balances = [w.get("balance", 0) for w in wallets]
    changes = [w.get("change_7d", 0) for w in wallets]

    colors = [brand.POSITIVE if c >= 0 else brand.NEGATIVE for c in changes]

    fig = go.Figure(
        go.Bar(
            y=labels,
            x=balances,
            orientation="h",
            marker_color=colors,
            text=[f"{b:,.0f} {asset}" for b in balances],
            textposition="outside",
            textfont=dict(size=9, color=brand.TEXT),
        )
    )

    fig = _apply_brand(fig, f"Top {asset} Whale Wallets", "Arkham / DeBank")
    fig.update_layout(
        xaxis_title=f"Balance ({asset})",
        height=max(400, len(wallets) * 30),
    )

    if filename:
        save_chart(fig, filename, height=max(400, len(wallets) * 30))
    return fig
