"""Walk-forward backtest for the accumulation signal — built to resist the two
ways this kind of system lies to you:

  1. Look-ahead / overfitting: the label ("did it launch?") is defined ONCE and
     never re-tuned, and the signal is evaluated only on an out-of-sample window
     AFTER a time cutoff. You fit thresholds on train, you report on test.
  2. Survivorship bias: $siren/$ward are survivors. The denominator MUST include
     the accumulation setups that fizzled (max_return below the launch bar).
     `evaluate` warns if the sample set looks survivor-heavy.

A "sample" is one token's decision-time snapshot plus its realized outcome:
    {
      "token": str, "timestamp": ISO8601 str,
      "features": dict,            # market_data passed to the signal predicate
      "max_return": float,         # realized max multiple after the decision (e.g. 3.0 = 3x)
    }

Samples are sourced offline (from the holder_snapshots DB once enough history
accrues, or from Dune/Solscan historical pulls). The core here is pure and
unit-tested; data loading is intentionally left as a thin, swappable step.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

import structlog

logger = structlog.get_logger()

# Fixed label definition — DO NOT re-tune after seeing results.
LAUNCH_MULTIPLE = 2.0  # a "real launch" = realized max return >= 2x
FAKE_MOVE_MULTIPLE = 1.3  # 1.3x–2x counts as a 30% fake move, NOT a launch


def is_launch(max_return: float, multiple: float = LAUNCH_MULTIPLE) -> bool:
    """Fixed launch label: realized max multiple reached the bar."""
    return max_return >= multiple


def default_signal_predicate(features: dict) -> bool:
    """Sync mirror of AccumulationDivergenceSignal's gating, for backtesting.

    Mirrors the three conditions (divergence slope, saturation/deceleration,
    float active) without the async TradeSignal wrapper.
    """
    from src.signals.accumulation_divergence import (
        AccumulationDivergenceSignal as A,
        _slope,
        is_decelerating,
    )

    gap = features.get("gap_series") or []
    eff = features.get("effective_series") or []
    if len(gap) < A.MIN_POINTS or len(eff) < A.MIN_POINTS:
        return False
    if features.get("security_passed") is False:
        return False
    cond_div = _slope(gap) >= A.MIN_GAP_SLOPE
    cond_sat = is_decelerating(eff) and eff[-1] >= A.MIN_EFFECTIVE_LEVEL
    cond_float = float(features.get("float_active", 0) or 0) >= A.MIN_FLOAT_ACTIVE
    return cond_div and cond_sat and cond_float


def walk_forward_split(
    samples: list[dict], cutoff_ts: str
) -> tuple[list[dict], list[dict]]:
    """Split into (train, test) at a time cutoff. Test = strictly after cutoff."""
    train = [s for s in samples if s.get("timestamp", "") <= cutoff_ts]
    test = [s for s in samples if s.get("timestamp", "") > cutoff_ts]
    return train, test


@dataclass
class BacktestMetrics:
    n: int
    launchers: int
    fired: int
    tp: int
    fp: int
    fn: int
    tn: int
    precision: float
    recall: float
    launch_base_rate: float
    survivorship_warning: bool

    def as_dict(self) -> dict[str, Any]:
        return self.__dict__.copy()


def evaluate(
    samples: list[dict],
    cutoff_ts: str,
    signal_predicate: Callable[[dict], bool] = default_signal_predicate,
    label_multiple: float = LAUNCH_MULTIPLE,
) -> BacktestMetrics:
    """Out-of-sample precision/recall for the signal on the post-cutoff window."""
    _, test = walk_forward_split(samples, cutoff_ts)
    tp = fp = fn = tn = launchers = 0
    for s in test:
        fired = bool(signal_predicate(s.get("features", {})))
        launched = is_launch(float(s.get("max_return", 0) or 0), label_multiple)
        launchers += int(launched)
        if fired and launched:
            tp += 1
        elif fired and not launched:
            fp += 1
        elif not fired and launched:
            fn += 1
        else:
            tn += 1

    n = len(test)
    precision = round(tp / (tp + fp), 4) if (tp + fp) else 0.0
    recall = round(tp / (tp + fn), 4) if (tp + fn) else 0.0
    base_rate = round(launchers / n, 4) if n else 0.0
    # If almost everything "launched", the sample set is survivor-heavy and the
    # precision number is meaningless — the dead setups were filtered out.
    survivorship_warning = base_rate > 0.5

    metrics = BacktestMetrics(
        n=n, launchers=launchers, fired=tp + fp, tp=tp, fp=fp, fn=fn, tn=tn,
        precision=precision, recall=recall, launch_base_rate=base_rate,
        survivorship_warning=survivorship_warning,
    )
    logger.info("walk_forward_evaluated", **metrics.as_dict())
    if survivorship_warning:
        logger.warning(
            "survivorship_bias_suspected",
            launch_base_rate=base_rate,
            hint="dataset looks survivor-heavy; include fizzled accumulation setups",
        )
    return metrics
