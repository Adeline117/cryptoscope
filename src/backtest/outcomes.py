"""Realized outcomes for backtesting — pairs detected tokens with what happened.

True walk-forward needs each sample's realized *max* return after the decision.
The free-tier proxy here is the entry→current return pulled from the signal
scorecard (which records entry prices) and DexScreener (current price). It is a
LOWER BOUND on max return and improves as more time passes; a precise max-return
backtest needs historical OHLC (Dune/GeckoTerminal) and is a later upgrade.

The honest part: `build_outcomes_from_scorecard` returns outcomes for ALL
recorded signals, winners AND losers — so the backtest denominator includes the
setups that fizzled, not just survivors.
"""

from __future__ import annotations

import json
import sqlite3
import urllib.request

import structlog

logger = structlog.get_logger()


def _dexscreener_price(token: str, chain: str, timeout: int = 12) -> float | None:
    url = f"https://api.dexscreener.com/token-pairs/v1/{chain}/{token}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "CryptoScope/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode())
        pairs = data if isinstance(data, list) else data.get("pairs", [])
        if not pairs:
            return None
        best = max(pairs, key=lambda p: (p.get("liquidity", {}) or {}).get("usd", 0) or 0)
        price = best.get("priceUsd")
        return float(price) if price else None
    except Exception as e:
        logger.debug("outcome_price_failed", token=token, error=str(e))
        return None


def build_outcomes_from_scorecard(
    signal_type: str = "accumulation_divergence",
    fetch_price=_dexscreener_price,
) -> dict[str, float]:
    """Return {token_address: return_multiple} from recorded signals.

    return_multiple = current_price / entry_price (1.0 = flat, 2.0 = 2x). Signals
    whose entry price is unknown (0) are skipped. Includes losers, by design.
    """
    from src.trading.signal_scorecard import DB_PATH

    if not DB_PATH.exists():
        return {}
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    conn.execute("PRAGMA busy_timeout=10000")
    try:
        rows = conn.execute(
            "SELECT asset, chain, entry_price, metadata FROM signals WHERE signal_type = ?",
            (signal_type,),
        ).fetchall()
    finally:
        conn.close()

    outcomes: dict[str, float] = {}
    for asset, chain, entry_price, meta_json in rows:
        try:
            meta = json.loads(meta_json) if meta_json else {}
        except (json.JSONDecodeError, TypeError):
            meta = {}
        token = meta.get("token_address", "")
        if not token or not entry_price or entry_price <= 0:
            continue
        current = fetch_price(token, chain)
        if current is None:
            continue
        outcomes[token] = round(current / entry_price, 4)
    logger.info("outcomes_built", signal_type=signal_type, count=len(outcomes))
    return outcomes


def run_backtest(
    cutoff_ts: str,
    signal_type: str = "accumulation_divergence",
    chain: str | None = None,
) -> dict:
    """End-to-end offline backtest: snapshots + scorecard outcomes → metrics."""
    from src.backtest.walk_forward import build_samples_from_snapshots, evaluate

    outcomes = build_outcomes_from_scorecard(signal_type)
    if not outcomes:
        return {"status": "no_outcomes", "note": "need recorded signals with entry prices"}

    samples = build_samples_from_snapshots(outcomes, chain=chain)
    if not samples:
        return {"status": "no_samples", "note": "need snapshot history for outcome tokens"}

    metrics = evaluate(samples, cutoff_ts=cutoff_ts)
    return {"status": "complete", **metrics.as_dict()}
