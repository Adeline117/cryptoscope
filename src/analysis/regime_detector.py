"""Detect market regime (bull/bear/sideways/high-volatility) using a Hidden Markov Model.

Uses ``hmmlearn`` (Gaussian HMM) fitted on daily log-returns and rolling
volatility.  Input: at least 30 daily close prices.  Output: current regime,
confidence, duration, and transition probabilities.

Install dependency:  ``pip install hmmlearn>=0.3``
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

import numpy as np
import structlog

logger = structlog.get_logger()


class MarketRegime(Enum):
    BULL = "bull"
    BEAR = "bear"
    SIDEWAYS = "sideways"
    HIGH_VOL = "high_volatility"


# Human-readable descriptions (Chinese) for each regime
_REGIME_DESCRIPTIONS_ZH: dict[MarketRegime, str] = {
    MarketRegime.BULL: "当前处于上涨趋势，市场情绪偏多",
    MarketRegime.BEAR: "当前处于下跌趋势，市场情绪偏空",
    MarketRegime.SIDEWAYS: "当前处于震荡整理阶段，方向不明确",
    MarketRegime.HIGH_VOL: "当前处于高波动状态，风险与机会并存",
}


@dataclass
class RegimeResult:
    """Output of :func:`detect_regime`."""

    current_regime: MarketRegime
    confidence: float  # 0-1, posterior probability of the decoded state
    regime_duration_days: int
    transition_probs: dict[str, float]  # probability of switching to each regime
    description_zh: str  # Chinese description of the current regime


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _build_features(
    prices: list[float],
    volumes: list[float] | None = None,
) -> np.ndarray:
    """Compute feature matrix from price (and optional volume) series.

    Features per day (after dropping the warm-up window):
      1. Daily log-return
      2. 10-day rolling annualised volatility
      3. Volume ratio vs 10-day mean (if volumes provided)
    """
    log_prices = np.log(np.array(prices, dtype=np.float64))
    returns = np.diff(log_prices)  # length = len(prices) - 1

    # Rolling 10-day volatility (annualised, sqrt(365) for crypto)
    window = 10
    vol = np.full_like(returns, np.nan)
    for i in range(window - 1, len(returns)):
        vol[i] = np.std(returns[i - window + 1 : i + 1], ddof=1) * np.sqrt(365)

    # Trim to where volatility is valid
    valid_start = window - 1
    ret_trimmed = returns[valid_start:]
    vol_trimmed = vol[valid_start:]

    feature_cols = [ret_trimmed.reshape(-1, 1), vol_trimmed.reshape(-1, 1)]

    if volumes is not None and len(volumes) == len(prices):
        vol_arr = np.array(volumes, dtype=np.float64)
        # Volume ratio: current / 10-day MA
        vol_ma = np.full_like(vol_arr, np.nan)
        for i in range(window - 1, len(vol_arr)):
            vol_ma[i] = np.mean(vol_arr[i - window + 1 : i + 1])
        # Align with returns (which start at index 1 of prices)
        vol_ratio = vol_arr[1:] / np.where(vol_ma[1:] > 0, vol_ma[1:], 1.0)
        vol_ratio_trimmed = vol_ratio[valid_start:]
        feature_cols.append(vol_ratio_trimmed.reshape(-1, 1))

    features = np.hstack(feature_cols)

    # Replace any remaining NaN / inf with 0
    features = np.nan_to_num(features, nan=0.0, posinf=0.0, neginf=0.0)
    return features


def _map_states_to_regimes(
    model,  # GaussianHMM
    n_states: int,
) -> dict[int, MarketRegime]:
    """Map HMM hidden states to human-readable regimes based on learned means.

    Strategy:
      - Sort states by mean return (feature 0).
      - Lowest mean return -> BEAR
      - Highest mean return -> BULL
      - Middle state(s) -> pick SIDEWAYS or HIGH_VOL based on mean volatility.
    """
    means = model.means_  # shape (n_states, n_features)
    # Sort state indices by mean return ascending
    sorted_indices = np.argsort(means[:, 0])

    mapping: dict[int, MarketRegime] = {}

    if n_states == 2:
        mapping[sorted_indices[0]] = MarketRegime.BEAR
        mapping[sorted_indices[1]] = MarketRegime.BULL
    elif n_states == 3:
        mapping[sorted_indices[0]] = MarketRegime.BEAR
        mapping[sorted_indices[2]] = MarketRegime.BULL

        mid_state = sorted_indices[1]
        mid_vol = means[mid_state, 1] if means.shape[1] > 1 else 0.0
        overall_avg_vol = means[:, 1].mean() if means.shape[1] > 1 else 0.0
        if mid_vol > overall_avg_vol * 1.2:
            mapping[mid_state] = MarketRegime.HIGH_VOL
        else:
            mapping[mid_state] = MarketRegime.SIDEWAYS
    else:  # 4 states
        mapping[sorted_indices[0]] = MarketRegime.BEAR
        mapping[sorted_indices[-1]] = MarketRegime.BULL
        # Among the two middle states, the one with higher vol -> HIGH_VOL
        mid_states = sorted_indices[1:-1]
        if means.shape[1] > 1:
            if means[mid_states[0], 1] > means[mid_states[1], 1]:
                mapping[mid_states[0]] = MarketRegime.HIGH_VOL
                mapping[mid_states[1]] = MarketRegime.SIDEWAYS
            else:
                mapping[mid_states[0]] = MarketRegime.SIDEWAYS
                mapping[mid_states[1]] = MarketRegime.HIGH_VOL
        else:
            mapping[mid_states[0]] = MarketRegime.SIDEWAYS
            mapping[mid_states[1]] = MarketRegime.HIGH_VOL

    return mapping


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def detect_regime(
    prices: list[float],
    volumes: list[float] | None = None,
    n_states: int = 3,
) -> RegimeResult | None:
    """Detect the current market regime using a Gaussian HMM.

    Args:
        prices: Daily close prices (chronological order, oldest first).
                Must contain at least 30 data points.
        volumes: Optional daily volumes aligned with *prices*.
        n_states: Number of hidden states (2-4). Default 3.

    Returns:
        A :class:`RegimeResult` or ``None`` if the model cannot be fitted
        (too few data points, missing dependency, etc.).
    """
    try:
        from hmmlearn.hmm import GaussianHMM  # type: ignore[import-untyped]
    except ImportError:
        logger.warning("hmmlearn_not_installed")
        return None

    if len(prices) < 30:
        logger.warning("regime_insufficient_data", n=len(prices))
        return None

    n_states = max(2, min(n_states, 4))

    features = _build_features(prices, volumes)
    if len(features) < 20:
        logger.warning("regime_insufficient_features", n=len(features))
        return None

    try:
        model = GaussianHMM(
            n_components=n_states,
            covariance_type="full",
            n_iter=200,
            random_state=42,
            tol=0.01,
        )
        model.fit(features)
    except Exception as e:
        logger.warning("hmm_fit_failed", error=str(e))
        return None

    # Decode the most likely state sequence
    try:
        _, state_sequence = model.decode(features)
    except Exception as e:
        logger.warning("hmm_decode_failed", error=str(e))
        return None

    state_mapping = _map_states_to_regimes(model, n_states)
    current_state = int(state_sequence[-1])
    current_regime = state_mapping.get(current_state, MarketRegime.SIDEWAYS)

    # Posterior probability of the current state (confidence)
    posteriors = model.predict_proba(features)
    confidence = float(posteriors[-1, current_state])

    # Duration: count consecutive days in the same state from the end
    duration = 1
    for i in range(len(state_sequence) - 2, -1, -1):
        if state_sequence[i] == current_state:
            duration += 1
        else:
            break

    # Transition probabilities from the current state
    trans_row = model.transmat_[current_state]
    transition_probs: dict[str, float] = {}
    for state_idx, regime in state_mapping.items():
        transition_probs[regime.value] = round(float(trans_row[state_idx]), 4)

    description_zh = _REGIME_DESCRIPTIONS_ZH.get(
        current_regime, "未知市场状态"
    )
    if duration >= 14:
        description_zh += f"，已持续 {duration} 天"
    elif duration >= 7:
        description_zh += f"，持续约 {duration} 天"

    result = RegimeResult(
        current_regime=current_regime,
        confidence=round(confidence, 4),
        regime_duration_days=duration,
        transition_probs=transition_probs,
        description_zh=description_zh,
    )

    logger.info(
        "regime_detected",
        regime=current_regime.value,
        confidence=result.confidence,
        duration_days=duration,
    )

    return result
