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
      "token": str, "chain": str, "timestamp": ISO8601 str,
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
    if features.get("dynamic_evidence_eligible") is not True:
        return False
    if len(gap) < A.MIN_POINTS or len(eff) < A.MIN_POINTS:
        return False
    if features.get("security_passed") is not True:
        return False
    cond_div = _slope(gap) >= A.MIN_GAP_SLOPE
    cond_sat = is_decelerating(eff) and eff[-1] >= A.MIN_EFFECTIVE_LEVEL
    cond_float = float(features.get("float_active", 0) or 0) >= A.MIN_FLOAT_ACTIVE
    return cond_div and cond_sat and cond_float


def build_samples_from_snapshots(
    outcomes: dict[Any, float],
    chain: str | None = None,
    db_path=None,
) -> list[dict]:
    """Reconstruct backtest samples from the holder_snapshots DB.

    For each snapshotted token, rebuilds the effective/nominal concentration
    series (the same features the live signal sees) and pairs it with a realized
    outcome from `outcomes` ((chain, token_address) -> max_return multiple).
    Legacy token-only keys are accepted only when `chain` explicitly limits the
    build to one chain; accepting them across all chains would silently apply one
    token outcome to the same address on another EVM network. Tokens absent from
    `outcomes` are skipped — the caller is responsible for supplying the FULL
    outcome set including fizzled setups (to avoid survivorship bias).

    `outcomes` must include the dead setups, not just the winners.
    """
    from src.onchain import holder_snapshot as hs
    from src.onchain.chain_identity import canonical_chain
    from src.onchain.entity_clustering import effective_concentration

    kwargs = {"db_path": db_path} if db_path is not None else {}
    requested_chain = canonical_chain(chain) if chain is not None else None
    if chain is not None and requested_chain is None:
        logger.warning("backtest_unsupported_chain", chain=chain)
        return []
    keyed_outcomes: dict[tuple[str, str], tuple[str, float]] = {}
    legacy_outcomes: dict[str, tuple[str, float]] = {}
    for raw_key, max_return in outcomes.items():
        if isinstance(raw_key, tuple) and len(raw_key) == 2:
            outcome_chain, outcome_token = map(str, raw_key)
            outcome_chain = canonical_chain(outcome_chain)
            if outcome_chain is None:
                logger.warning("outcome_unsupported_chain", chain=raw_key[0])
                continue
            keyed_outcomes[
                (outcome_chain, hs._canonical_token(outcome_token, outcome_chain))
            ] = (outcome_token, max_return)
        else:
            outcome_token = str(raw_key)
            legacy_outcomes[outcome_token] = (outcome_token, max_return)
    legacy_evm_outcomes = {
        token.lower(): value for token, value in legacy_outcomes.items()
    }
    if legacy_outcomes and chain is None:
        logger.warning(
            "ambiguous_token_only_outcomes_rejected",
            count=len(legacy_outcomes),
            hint="key outcomes by (chain, token), or pass an explicit single chain",
        )

    samples: list[dict] = []
    for token, ch in hs.list_tokens(**kwargs):
        if requested_chain and ch != requested_chain:
            continue
        matched = keyed_outcomes.get(
            (ch, hs._canonical_token(token, ch))
        )
        if matched is None and chain is not None:
            matched = (
                legacy_evm_outcomes.get(token.lower())
                if hs._is_evm_chain(ch)
                else legacy_outcomes.get(token)
            )
        if matched is None:
            continue
        outcome_token, max_return = matched
        raw_history = hs.get_holders_history(token, ch, **kwargs)
        if not raw_history:
            continue
        # A duplicated latest response has unknown currentness without provider
        # block/etag provenance. Do not turn it into either a decision-time sample
        # or an artificial zero final delta. Earlier cached copies are collapsed as
        # well, so they never count as independent walk-forward observations.
        if len(raw_history) < 2 or not hs.holder_state_changed(
            raw_history[-2][1], raw_history[-1][1], ch,
        ):
            logger.info(
                "backtest_sample_skipped_static_holder_state",
                token=token,
                chain=ch,
            )
            continue
        history = hs.deduplicate_holder_history(raw_history, ch)
        eff_series, gap_series = [], []
        for _ts, holders in history:
            m = effective_concentration(holders, top_n=10)
            eff_series.append(m["effective_top_n_pct"])
            gap_series.append(m["concentration_gap"])
        eff_top = eff_series[-1] if eff_series else 0
        samples.append({
            "token": outcome_token,
            "chain": ch,
            "timestamp": history[-1][0],  # latest snapshot time
            "features": {
                "gap_series": gap_series,
                "effective_series": eff_series,
                "dynamic_evidence_eligible": True,
                "float_active": max(0.0, min(1.0, 1 - eff_top / 100)),
                # The holder snapshot schema has no decision-time contract-security
                # attestation. Unknown must stay unknown; callers may enrich it
                # only from a timestamped evidence ledger before evaluation.
                "security_passed": None,
            },
            "max_return": float(max_return),
        })
    logger.info("backtest_samples_built", count=len(samples))
    return samples


def make_predicate(min_gap_slope: float, min_eff_level: float, min_float: float):
    """Build a signal predicate with explicit thresholds (for sweeping)."""
    from src.signals.accumulation_divergence import _slope, is_decelerating

    MIN_POINTS = 4

    def predicate(features: dict) -> bool:
        gap = features.get("gap_series") or []
        eff = features.get("effective_series") or []
        if features.get("dynamic_evidence_eligible") is not True:
            return False
        if len(gap) < MIN_POINTS or len(eff) < MIN_POINTS:
            return False
        if features.get("security_passed") is not True:
            return False
        return (
            _slope(gap) >= min_gap_slope
            and is_decelerating(eff) and eff[-1] >= min_eff_level
            and float(features.get("float_active", 0) or 0) >= min_float
        )

    return predicate


def sweep_thresholds(
    samples: list[dict],
    cutoff_ts: str,
    gap_grid: list[float] | None = None,
    eff_grid: list[float] | None = None,
    float_grid: list[float] | None = None,
    label_multiple: float = LAUNCH_MULTIPLE,
) -> list[dict]:
    """Grid-search signal thresholds; return per-config out-of-sample metrics.

    Replaces hand-picked thresholds with data-chosen ones. Sorted by precision
    then fired count (a config that fires zero times has meaningless precision).
    """
    gap_grid = gap_grid or [0.2, 0.3, 0.5, 0.8]
    eff_grid = eff_grid or [20.0, 25.0, 30.0, 40.0]
    float_grid = float_grid or [0.25, 0.35, 0.5]

    results = []
    for g in gap_grid:
        for e in eff_grid:
            for f in float_grid:
                pred = make_predicate(g, e, f)
                m = evaluate(samples, cutoff_ts, pred, label_multiple)
                results.append({
                    "min_gap_slope": g, "min_eff_level": e, "min_float": f,
                    "precision": m.precision, "recall": m.recall,
                    "fired": m.fired, "tp": m.tp, "fp": m.fp,
                })
    results.sort(key=lambda r: (r["precision"], r["fired"]), reverse=True)
    return results


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
