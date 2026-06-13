"""Accumulation divergence signal — the core alpha of the 妖币 system.

This is deliberately NOT a concentration-*level* signal (that's the fake one).
It fires on the DIVERGENCE between effective and nominal concentration, plus the
accumulation-slope inflection, plus remaining float in weak hands:

  1. Divergence: effective concentration (post-clustering) is rising FASTER than
     nominal concentration — i.e. the effective-minus-nominal gap has a positive
     slope. On the surface holders look stable/dispersing while a single entity
     quietly concentrates.
  2. Slope inflection / saturation: the effective-concentration first differences
     are *decelerating* (accumulation slowing → whale nearly done) while the level
     is already meaningfully high.
  3. Float still active: weak hands / turnover remain high, so the launch is still
     ahead (don't chase a position the whale has already finished building).

All three must hold to emit a LONG. The signal is meant to be slow (state, not
event) — fast gas/mempool signals are confirmation only, handled elsewhere.

Input `market_data` keys (per token):
  - "gap_series":    list[float]  effective_top_n_pct - nominal_top_n_pct over time
  - "effective_series": list[float]  effective_top_n_pct over time
  - "float_active":  float 0-1  weak-hand / turnover indicator (1 = very active)
  - "security_passed": bool  Stage-0 contract gate result
  - "token_symbol", "token_address", "chain": labels (optional)
"""

from __future__ import annotations

import structlog

from src.signals.base import TradeSignal

logger = structlog.get_logger()


def _slope(series: list[float]) -> float:
    """Least-squares slope of a series against its index. 0 for < 2 points."""
    n = len(series)
    if n < 2:
        return 0.0
    xs = list(range(n))
    mean_x = sum(xs) / n
    mean_y = sum(series) / n
    num = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, series))
    den = sum((x - mean_x) ** 2 for x in xs)
    if den == 0:
        return 0.0
    return num / den


def _first_diffs(series: list[float]) -> list[float]:
    return [series[i] - series[i - 1] for i in range(1, len(series))]


def is_decelerating(series: list[float]) -> bool:
    """True if the series is still rising but its rate of increase is slowing.

    i.e. first differences are positive-ish overall but trending downward
    (negative slope of the first differences). This is the accumulation-slope
    inflection: the whale is nearly done collecting.
    """
    diffs = _first_diffs(series)
    if len(diffs) < 2:
        return False
    last = diffs[-1]
    # Still accumulating (most recent change non-negative) but decelerating.
    return last >= 0 and _slope(diffs) < 0


class AccumulationDivergenceSignal:
    """二级妖币吸筹背离信号。"""

    MIN_GAP_SLOPE = 0.3          # gap (eff-nominal) must rise this fast (pts/snapshot)
    MIN_EFFECTIVE_LEVEL = 25.0   # effective top-N must be at least this high (%)
    MIN_FLOAT_ACTIVE = 0.35      # weak hands still present (launch still ahead)
    MIN_POINTS = 4               # need enough history to trust slope/inflection

    signal_type = "accumulation_divergence"

    async def evaluate(self, market_data: dict) -> TradeSignal | None:
        gap_series = market_data.get("gap_series") or []
        eff_series = market_data.get("effective_series") or []
        float_active = float(market_data.get("float_active", 0) or 0)
        security_passed = market_data.get("security_passed", None)

        # Need enough history.
        if len(gap_series) < self.MIN_POINTS or len(eff_series) < self.MIN_POINTS:
            return None

        # Stage-0 safety gate: only commit on tokens that passed (or unknown→skip).
        if security_passed is False:
            return None

        gap_slope = _slope(gap_series)
        eff_level = eff_series[-1]
        decel = is_decelerating(eff_series)

        cond_divergence = gap_slope >= self.MIN_GAP_SLOPE
        cond_saturation = decel and eff_level >= self.MIN_EFFECTIVE_LEVEL
        cond_float = float_active >= self.MIN_FLOAT_ACTIVE

        if not (cond_divergence and cond_saturation and cond_float):
            return None

        # Confidence: divergence strength + saturation level + float room.
        divergence_boost = min(30, int(gap_slope / self.MIN_GAP_SLOPE * 15))
        saturation_boost = min(25, int((eff_level - self.MIN_EFFECTIVE_LEVEL) * 0.8) + 10)
        float_boost = min(15, int((float_active - self.MIN_FLOAT_ACTIVE) * 30))
        confidence = min(100, 30 + divergence_boost + saturation_boost + float_boost)

        symbol = market_data.get("token_symbol", "?")
        return TradeSignal(
            name="二级妖币吸筹背离",
            direction="LONG",
            confidence=confidence,
            signal_type=self.signal_type,
            components={
                "gap_slope": round(gap_slope, 3),
                "effective_level_pct": round(eff_level, 2),
                "decelerating": decel,
                "float_active": round(float_active, 3),
                "token_symbol": symbol,
                "token_address": market_data.get("token_address", ""),
                "chain": market_data.get("chain", ""),
            },
            reasoning=(
                f"{symbol}：有效集中度升至{eff_level:.0f}%且斜率拐头减速（庄快收完），"
                f"名义分散度未跟随（背离斜率{gap_slope:.1f}），浮筹仍活跃"
                f"（{float_active:.0%}），处于启动前吸筹尾段"
            ),
        )
