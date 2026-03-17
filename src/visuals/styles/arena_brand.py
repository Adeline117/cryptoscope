"""Arena brand styling for charts."""

# Colors
PRIMARY = "#00D4AA"      # Arena teal/green
SECONDARY = "#FF6B6B"    # Red for negative
ACCENT = "#4ECDC4"       # Light teal
BACKGROUND = "#0D1117"   # Dark background
SURFACE = "#161B22"      # Card/panel background
TEXT = "#E6EDF3"         # Primary text
TEXT_DIM = "#8B949E"     # Secondary text
GRID = "#21262D"         # Grid lines
POSITIVE = "#00D4AA"
NEGATIVE = "#FF6B6B"
NEUTRAL = "#8B949E"

# Extended palette for multi-series
SERIES_COLORS = [
    "#00D4AA", "#4ECDC4", "#45B7D1", "#96CEB4",
    "#FFEAA7", "#DDA15E", "#FF6B6B", "#C44569",
    "#A29BFE", "#6C5CE7", "#FD79A8", "#E17055",
]

# Font
FONT_FAMILY = "Inter, -apple-system, system-ui, sans-serif"
FONT_SIZE_TITLE = 18
FONT_SIZE_AXIS = 12
FONT_SIZE_ANNOTATION = 11

# Chart layout
WATERMARK_TEXT = "CryptoScope / Arena"
WATERMARK_OPACITY = 0.15

# Plotly layout template
PLOTLY_LAYOUT = dict(
    paper_bgcolor=BACKGROUND,
    plot_bgcolor=BACKGROUND,
    font=dict(family=FONT_FAMILY, color=TEXT, size=FONT_SIZE_AXIS),
    title=dict(font=dict(size=FONT_SIZE_TITLE, color=TEXT)),
    xaxis=dict(
        gridcolor=GRID, zerolinecolor=GRID,
        tickfont=dict(color=TEXT_DIM),
    ),
    yaxis=dict(
        gridcolor=GRID, zerolinecolor=GRID,
        tickfont=dict(color=TEXT_DIM),
    ),
    legend=dict(
        bgcolor="rgba(0,0,0,0)", font=dict(color=TEXT_DIM),
    ),
    margin=dict(l=60, r=40, t=60, b=80),
    # Watermark annotation
    annotations=[
        dict(
            text=WATERMARK_TEXT,
            xref="paper", yref="paper",
            x=0.98, y=0.02,
            showarrow=False,
            font=dict(size=10, color=TEXT_DIM),
            opacity=WATERMARK_OPACITY,
            xanchor="right", yanchor="bottom",
        )
    ],
)
