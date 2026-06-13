"""Kelly-half position sizing driven by historical signal precision.

Position size is set by the signal's *historical* win rate and payoff ratio
(not by its confidence), then halved for safety. With too few completed samples
we fall back to a conservative fixed fraction. A hard floor/cap bounds the
output. This feeds the paper-trade hint in accumulation alerts; it never places
real orders.
"""

from __future__ import annotations

import structlog

logger = structlog.get_logger()

MIN_SAMPLES = 10          # need this many completed signals before trusting stats
DEFAULT_PCT = 0.02        # conservative fallback (2%)
MIN_PCT = 0.005           # hard floor (0.5%)
MAX_PCT = 0.10            # hard cap (10%)
KELLY_FRACTION = 0.5      # half-Kelly


def _parse_pct(s: str | float | None) -> float:
    """Parse '65%' / '+12.50%' / 0.65 → float in the string's own units."""
    if s is None:
        return 0.0
    if isinstance(s, (int, float)):
        return float(s)
    return float(str(s).strip().rstrip("%").replace("+", "") or 0)


def kelly_fraction(win_rate: float, payoff_ratio: float) -> float:
    """Kelly fraction f* = p - (1-p)/b. Clamped to [0, 1].

    win_rate p in [0,1], payoff_ratio b = avg_win/avg_loss (> 0).
    """
    if payoff_ratio <= 0:
        return 0.0
    f = win_rate - (1 - win_rate) / payoff_ratio
    return max(0.0, min(1.0, f))


def calculate_kelly_position(
    signal_type: str,
    horizon: str = "24h",
    summary: dict | None = None,
) -> dict:
    """Return {"pct", "note"} — fraction of balance to allocate.

    Pulls historical win rate + avg PnL for `signal_type` from the signal
    scorecard, computes half-Kelly, and bounds the result. `summary` can be
    injected for testing; otherwise it is fetched live.
    """
    if summary is None:
        try:
            from src.trading.signal_scorecard import get_scorecard_summary

            summary = get_scorecard_summary()
        except Exception as e:
            logger.warning("scorecard_unavailable", error=str(e))
            summary = {}

    stats = (summary or {}).get(signal_type)
    if not stats:
        return {"pct": DEFAULT_PCT, "note": "无历史样本，保守默认 2%"}

    completed = int(stats.get("completed", 0) or 0)
    if completed < MIN_SAMPLES:
        return {
            "pct": DEFAULT_PCT,
            "note": f"样本不足({completed}/{MIN_SAMPLES})，保守默认 2%",
        }

    win_rate = _parse_pct(stats.get(f"win_rate_{horizon}", 0)) / 100.0
    avg_win = _parse_pct(stats.get(f"avg_pnl_{horizon}", 0))
    # Approximate payoff ratio: avg win vs an assumed stop. Use avg_win/|stop|.
    # Without per-trade loss data, assume a symmetric reference of the avg win
    # magnitude vs a conservative 1.0; clamp payoff into a sane band.
    payoff = max(0.5, min(5.0, abs(avg_win) / 5.0)) if avg_win else 1.0

    f = kelly_fraction(win_rate, payoff) * KELLY_FRACTION
    pct = max(MIN_PCT, min(MAX_PCT, f))
    return {
        "pct": round(pct, 4),
        "note": (
            f"基于 {completed} 笔历史(胜率{win_rate:.0%}, "
            f"均盈{avg_win:+.1f}%) 的半 Kelly"
        ),
    }
