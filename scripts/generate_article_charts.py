"""Generate improved charts for the $50.4M MEV article.

Fixes:
1. 利润分配 — use proportional squares instead of pie to show $29.9M vs $36K magnitude
2. PoW vs PoS — emphasize "fewer entities, more power" instead of similar-height bars
3. 5:2 cover image
"""

import plotly.graph_objects as go
import plotly.io as pio
from plotly.subplots import make_subplots
import math

# Brand
BG = "#0D1117"
SURFACE = "#161B22"
TEXT = "#E6EDF3"
DIM = "#8B949E"
GRID = "#21262D"
RED = "#FF6B6B"
TEAL = "#00D4AA"
AMBER = "#DDA15E"
BLUE = "#45B7D1"
PURPLE = "#A29BFE"
PINK = "#C44569"

FONT = "Inter, -apple-system, system-ui, sans-serif"
OUT = "/Users/adelinewen/cryptoscope-1/output/charts"


def _base_layout(**extra):
    layout = dict(
        paper_bgcolor=BG,
        plot_bgcolor=BG,
        font=dict(family=FONT, color=TEXT, size=13),
        margin=dict(l=60, r=60, t=70, b=70),
        xaxis=dict(gridcolor=GRID, zerolinecolor=GRID, tickfont=dict(color=DIM)),
        yaxis=dict(gridcolor=GRID, zerolinecolor=GRID, tickfont=dict(color=DIM)),
    )
    layout.update(extra)
    return layout


def chart2_profit_distribution():
    """配图2：利润分配 — 对数刻度水平柱状图，让 $29.9M vs $36K 的差距一目了然。"""
    fig = go.Figure()

    titan = 29_900_000
    mev_bot = 9_900_000
    user = 36_000
    gas = 2.5

    entities = ["Gas Fee", "User (You)", "MEV Bot", "Titan Builder"]
    values = [gas, user, mev_bot, titan]
    colors = [DIM, AMBER, PINK, RED]

    fig.add_trace(go.Bar(
        y=entities,
        x=values,
        orientation="h",
        marker_color=colors,
        text=["$2.50", "$36,000", "$9.9M", "$29.9M"],
        textposition="inside",
        insidetextanchor="end",
        textfont=dict(size=20, color="#FFFFFF", family="monospace"),
        width=0.55,
    ))

    fig.update_layout(
        paper_bgcolor=BG,
        plot_bgcolor=BG,
        font=dict(family=FONT, color=TEXT, size=13),
        title=dict(
            text="Who Got the Money?<br>"
                 "<span style='font-size:13px;color:#DDA15E'>"
                 "User put in $50.4M  →  Titan took 830× more than the user"
                 "</span>",
            font=dict(size=22, color=TEXT),
        ),
        xaxis=dict(
            type="log",
            title="USD (log scale)",
            dtick=1,
            gridcolor=GRID,
            zerolinecolor=GRID,
            tickfont=dict(color=DIM),
            range=[math.log10(0.3), math.log10(200_000_000)],
        ),
        yaxis=dict(
            categoryorder="array",
            categoryarray=entities,
            gridcolor=GRID,
            tickfont=dict(color=TEXT, size=14),
        ),
        height=450,
        width=1200,
        margin=dict(l=130, r=30, t=90, b=70),
        showlegend=False,
    )

    fig.add_annotation(
        text="Data: Coin Metrics ATLAS · Etherscan",
        xref="paper", yref="paper", x=0.01, y=-0.12,
        showarrow=False, font=dict(size=9, color=DIM),
    )

    pio.write_image(fig, f"{OUT}/swap_value_distribution.png", width=1200, height=450, scale=2)
    print("✅ 配图2: swap_value_distribution.png")


def chart6_pow_vs_pos():
    """配图6：PoW vs PoS 集中度对比。

    问题：65% vs 78% 两根柱子看起来差不多。
    解决：不比百分比，比 "几家实体控制了多少"。

    设计：左右对比面板
    - 左：PoW 时代，5-6个矿池图标，~65%，但"算力可自由切换"
    - 右：PoS 时代，2个builder，78%，且有"独家订单流锁定"
    强调的不是百分比差距，而是"权力结构"的变化。
    """
    fig = make_subplots(
        rows=1, cols=2,
        specs=[[{"type": "pie"}, {"type": "pie"}]],
        subplot_titles=["", ""],
        horizontal_spacing=0.08,
    )

    # PoW: 前几大矿池
    pow_labels = ["Ethermine", "F2Pool", "Sparkpool", "Nanopool", "Others (open entry)"]
    pow_values = [28, 15, 12, 10, 35]
    pow_colors = [BLUE, TEAL, "#96CEB4", "#4ECDC4", "#21262D"]

    fig.add_trace(go.Pie(
        labels=pow_labels,
        values=pow_values,
        hole=0.6,
        marker=dict(colors=pow_colors, line=dict(color=BG, width=2)),
        textinfo="none",
        hoverinfo="label+percent",
        direction="clockwise",
        sort=False,
    ), row=1, col=1)

    # PoS: 前两大 builder
    pos_labels = ["Titan Builder", "BuilderNet", "All Others"]
    pos_values = [50.15, 27.94, 21.91]
    pos_colors = [RED, PINK, "#21262D"]

    fig.add_trace(go.Pie(
        labels=pos_labels,
        values=pos_values,
        hole=0.6,
        marker=dict(colors=pos_colors, line=dict(color=BG, width=2)),
        textinfo="none",
        hoverinfo="label+percent",
        direction="clockwise",
        sort=False,
    ), row=1, col=2)

    # Center text in donuts
    fig.add_annotation(
        text="<b>~65%</b><br><span style='font-size:11px;color:#8B949E'>5+ pools</span>",
        x=0.195, y=0.5, xref="paper", yref="paper",
        showarrow=False, font=dict(size=28, color=BLUE),
    )
    fig.add_annotation(
        text="<b>78%</b><br><span style='font-size:11px;color:#8B949E'>2 builders</span>",
        x=0.805, y=0.5, xref="paper", yref="paper",
        showarrow=False, font=dict(size=28, color=RED),
    )

    # Era labels
    fig.add_annotation(
        text="<b>PoW Era (Pre-Merge)</b><br><span style='font-size:11px;color:#8B949E'>"
             "Miners compete · Hardware is open<br>"
             "Hashrate flows freely between pools</span>",
        x=0.195, y=-0.12, xref="paper", yref="paper",
        showarrow=False, font=dict(size=14, color=BLUE), align="center",
    )
    fig.add_annotation(
        text="<b>PoS Era (Post-Merge)</b><br><span style='font-size:11px;color:#8B949E'>"
             "Exclusive order flow deals · Winner-take-all<br>"
             "Validators don't choose what goes in blocks</span>",
        x=0.805, y=-0.12, xref="paper", yref="paper",
        showarrow=False, font=dict(size=14, color=RED), align="center",
    )

    # Arrow in the middle
    fig.add_annotation(
        text="<b>The Merge</b><br>→",
        x=0.5, y=0.5, xref="paper", yref="paper",
        showarrow=False, font=dict(size=16, color=DIM),
    )

    # Key insight at bottom
    fig.add_annotation(
        text="<b>Fewer entities. More power. No competition.</b>",
        x=0.5, y=-0.25, xref="paper", yref="paper",
        showarrow=False, font=dict(size=15, color=RED), align="center",
    )

    fig.update_layout(
        paper_bgcolor=BG,
        plot_bgcolor=BG,
        font=dict(family=FONT, color=TEXT),
        title=dict(
            text="Ethereum Got MORE Centralized After The Merge",
            font=dict(size=20, color=TEXT),
            x=0.5, xanchor="center",
        ),
        showlegend=False,
        height=520,
        width=1200,
        margin=dict(l=40, r=40, t=70, b=120),
    )

    # Data source
    fig.add_annotation(
        text="Data: Relayscan · Etherscan · arXiv:2412.18074",
        xref="paper", yref="paper", x=0.01, y=-0.3,
        showarrow=False, font=dict(size=9, color=DIM),
    )

    pio.write_image(fig, f"{OUT}/eth_pow_vs_pos.png", width=1200, height=520, scale=2)
    print("✅ 配图6: eth_pow_vs_pos.png")


def cover_image():
    """5:2 封面图。主题：以太坊完了。纯情绪，不列数据。"""
    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=[0], y=[0], mode="markers",
        marker=dict(size=0, color=BG), showlegend=False,
    ))

    # 背景：画一个巨大的、暗淡破碎感的以太坊菱形 (用 shapes)
    # Ethereum diamond — 用多个半透明三角形拼出，故意错位制造"裂开"感
    diamond_color = "rgba(255,107,107,0.06)"
    crack_color = BG  # 裂缝用背景色

    # 左上三角
    fig.add_shape(type="path",
        path="M 0.50 0.15 L 0.35 0.50 L 0.50 0.42 Z",
        fillcolor="rgba(255,107,107,0.07)",
        line=dict(color="rgba(255,107,107,0.12)", width=1),
        xref="paper", yref="paper",
    )
    # 右上三角
    fig.add_shape(type="path",
        path="M 0.50 0.15 L 0.65 0.50 L 0.50 0.42 Z",
        fillcolor="rgba(255,107,107,0.05)",
        line=dict(color="rgba(255,107,107,0.12)", width=1),
        xref="paper", yref="paper",
    )
    # 左下三角
    fig.add_shape(type="path",
        path="M 0.35 0.52 L 0.50 0.90 L 0.50 0.44 Z",
        fillcolor="rgba(255,107,107,0.04)",
        line=dict(color="rgba(255,107,107,0.10)", width=1),
        xref="paper", yref="paper",
    )
    # 右下三角 — 稍微偏移，制造裂缝
    fig.add_shape(type="path",
        path="M 0.655 0.52 L 0.505 0.90 L 0.505 0.44 Z",
        fillcolor="rgba(255,107,107,0.03)",
        line=dict(color="rgba(255,107,107,0.08)", width=1),
        xref="paper", yref="paper",
    )

    # 裂缝线 — 从中心往右下延伸
    fig.add_shape(type="line",
        x0=0.50, y0=0.42, x1=0.655, y1=0.52,
        line=dict(color="rgba(255,107,107,0.15)", width=2, dash="dot"),
        xref="paper", yref="paper",
    )

    # 主标题 — 大字，居中偏左
    fig.add_annotation(
        text="Every Line of Code",
        x=0.50, y=0.62, xref="paper", yref="paper",
        showarrow=False,
        font=dict(size=48, color=TEXT, family=FONT),
    )
    fig.add_annotation(
        text="Worked as Designed.",
        x=0.50, y=0.42, xref="paper", yref="paper",
        showarrow=False,
        font=dict(size=48, color=RED, family=FONT),
    )

    # 副标题
    fig.add_annotation(
        text="The system isn't broken. The system is the problem.",
        x=0.50, y=0.24, xref="paper", yref="paper",
        showarrow=False,
        font=dict(size=16, color=DIM, family=FONT),
    )

    # Branding
    fig.add_annotation(
        text="CryptoScope / Arena",
        x=0.97, y=0.05, xref="paper", yref="paper",
        showarrow=False, xanchor="right",
        font=dict(size=10, color=DIM), opacity=0.35,
    )

    fig.update_layout(
        paper_bgcolor=BG,
        plot_bgcolor=BG,
        xaxis=dict(visible=False, range=[0, 1]),
        yaxis=dict(visible=False, range=[0, 1]),
        margin=dict(l=0, r=0, t=0, b=0),
        height=600,
        width=1500,
    )

    pio.write_image(fig, f"{OUT}/article_cover.png", width=1500, height=600, scale=2)
    print("✅ 封面: article_cover.png (5:2)")


if __name__ == "__main__":
    chart2_profit_distribution()
    chart6_pow_vs_pos()
    cover_image()
    print("\n🎉 All done!")
