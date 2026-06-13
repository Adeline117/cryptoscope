"""Stage 0/1 accumulation detection pipeline (free batch layer).

Flow (MVP — Stage 2 real-time layer deferred):
  Stage 0  scan universe (new DEX pools/boosts/CTOs) → record birth
           → contract security gate (drop honeypots)
  Stage 1  snapshot holders → build effective/nominal concentration series
           → run AccumulationDivergenceSignal
           → on hit: record signal + Kelly hint + push Telegram alert

The divergence signal needs several snapshots of history before it can fire, so
early runs just accumulate snapshots — that is by design (state, not event). The
pipeline degrades gracefully: any per-token failure is logged and skipped.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import structlog

from src.onchain import holder_snapshot as hs
from src.onchain.entity_clustering import effective_concentration
from src.signals.accumulation_divergence import AccumulationDivergenceSignal

logger = structlog.get_logger()

# Bound per-run work so a cron tick stays cheap.
MAX_CANDIDATES = 15
MIN_SECURITY_SCORE = 50  # contract gate: drop anything riskier than this
# Only snapshots within this window feed the realtime accumulation slope —
# stale snapshots from a prior appearance must not pollute the signal (same
# class of bug as the 2h-highlight stale-item leak).
SERIES_WINDOW_DAYS = 7

# pool_watcher chain string → contract_security chain id / key
_CHAIN_ID = {
    "ethereum": 1, "eth": 1, "base": 8453, "bsc": 56,
    "arbitrum": 42161, "optimism": 10, "polygon": 137,
}


def _chain_for_security(chain: str) -> int | str:
    if chain in ("solana", "sol"):
        return "solana"
    return _CHAIN_ID.get(chain, 1)


async def _collect_universe() -> list[dict]:
    """Stage 0: gather new-token candidates (reuses pool_watcher)."""
    try:
        from src.sniper.pool_watcher import scan_new_tokens

        return scan_new_tokens()
    except Exception as e:
        logger.warning("universe_scan_failed", error=str(e))
        return []


async def _security_gate(candidates: list[dict]) -> list[dict]:
    """Stage 0 gate: drop honeypots / high-risk contracts."""
    try:
        from src.collectors.contract_security import ContractSecurityChecker
    except Exception as e:
        logger.warning("security_checker_unavailable", error=str(e))
        # Without the gate, pass through but mark unknown.
        for c in candidates:
            c["security_score"] = None
        return candidates

    checker = ContractSecurityChecker()
    passed: list[dict] = []
    for c in candidates:
        addr, chain = c.get("address"), c.get("chain", "")
        if not addr:
            continue
        try:
            result = await checker.check_token(_chain_for_security(chain), addr)
            c["security_score"] = result.risk_score
            c["security_passed"] = (not result.is_honeypot) and result.risk_score >= MIN_SECURITY_SCORE
            if c["security_passed"]:
                passed.append(c)
        except Exception as e:
            logger.debug("security_check_failed", address=addr, error=str(e))
    return passed


def _build_series(token: str, chain: str) -> dict | None:
    """Recompute effective/nominal concentration series from snapshot history.

    Resolves first-funders across the full holder set (once — funders are
    immutable and cached) and feeds them into the clustering so effective
    concentration can actually diverge from nominal.
    """
    since = (datetime.now(timezone.utc) - timedelta(days=SERIES_WINDOW_DAYS)).isoformat()
    history = hs.get_holders_history(token, chain, since=since)
    if not history:
        return None

    # Collect every address seen across snapshots, resolve funders once.
    from src.onchain.entity_clustering import _norm

    all_addrs = sorted({
        _norm(h.get("address", ""))
        for _ts, holders in history for h in holders if h.get("address")
    })
    try:
        from src.onchain.funder_graph import get_funders

        funders = get_funders(all_addrs, chain)
    except Exception as e:
        logger.debug("funder_resolve_failed", token=token, error=str(e))
        funders = {}

    eff_series: list[float] = []
    gap_series: list[float] = []
    latest_metrics: dict = {}
    for _ts, holders in history:
        m = effective_concentration(holders, funders=funders, top_n=10)
        eff_series.append(m["effective_top_n_pct"])
        gap_series.append(m["concentration_gap"])
        latest_metrics = m
    return {
        "effective_series": eff_series,
        "gap_series": gap_series,
        "latest": latest_metrics,
    }


async def run_accumulation_pipeline(send: bool = True) -> dict:
    """Run one Stage 0/1 tick. Returns a summary dict."""
    start = datetime.now(timezone.utc)
    logger.info("accumulation_pipeline_started")

    universe = await _collect_universe()
    if not universe:
        return {"status": "empty_universe", "elapsed": 0}

    candidates = (await _security_gate(universe))[:MAX_CANDIDATES]
    logger.info("stage0_gated", universe=len(universe), passed=len(candidates))

    signals_fired = 0
    snapshots_taken = 0
    sig_eval = AccumulationDivergenceSignal()

    for c in candidates:
        addr, chain = c["address"], c.get("chain", "")
        # Stage 1: take/refresh a snapshot (records birth on first sight).
        try:
            snap = hs.snapshot_token(addr, chain, source=c.get("source", ""))
            if snap:
                snapshots_taken += 1
        except Exception as e:
            logger.debug("snapshot_failed", address=addr, error=str(e))
            continue

        series = _build_series(addr, chain)
        if not series:
            continue

        latest = series.get("latest") or {}
        # Float-active proxy: supply NOT held by the top-10 entities is the float
        # still in (potentially weak) hands. Higher → launch more likely ahead.
        eff_top = latest.get("effective_top_n_pct", 0)
        float_active = max(0.0, min(1.0, 1 - eff_top / 100))
        c["nominal_top_n_pct"] = latest.get("nominal_top_n_pct")

        market_data = {
            "effective_series": series["effective_series"],
            "gap_series": series["gap_series"],
            "float_active": float_active,
            "security_passed": c.get("security_passed", None),
            "token_symbol": c.get("symbol", addr[:6]),
            "token_address": addr,
            "chain": chain,
        }
        try:
            sig = await sig_eval.evaluate(market_data)
        except Exception as e:
            logger.debug("signal_eval_failed", address=addr, error=str(e))
            continue
        if not sig:
            continue

        signals_fired += 1
        await _emit_signal(c, sig, send=send)

    elapsed = (datetime.now(timezone.utc) - start).total_seconds()
    summary = {
        "status": "complete",
        "universe": len(universe),
        "gated": len(candidates),
        "snapshots": snapshots_taken,
        "signals": signals_fired,
        "elapsed": round(elapsed, 1),
    }
    logger.info("accumulation_pipeline_complete", **summary)
    return summary


async def _emit_signal(token: dict, sig, send: bool = True) -> None:
    """Record the signal to the scorecard and push a Telegram alert."""
    addr, chain = token["address"], token.get("chain", "")

    # Record for precision tracking (best-effort).
    try:
        from src.trading.signal_scorecard import record_signal

        record_signal(
            signal_type=sig.signal_type,
            asset=sig.components.get("token_symbol", addr[:6]),
            chain=chain,
            direction=sig.direction or "LONG",
            confidence=sig.confidence,
            entry_price=0.0,  # batch layer has no live price; scorecard fills later
            metadata=sig.components,
        )
    except Exception as e:
        logger.debug("scorecard_record_failed", error=str(e))

    # Kelly-half paper position hint.
    try:
        from src.trading.position_sizer import calculate_kelly_position

        hint = calculate_kelly_position(sig.signal_type)
    except Exception:
        hint = None

    if not send:
        return

    try:
        from src.distribution.message_templates import format_accumulation_alert
        from src.distribution.telegram_sender import send_alert

        msg = format_accumulation_alert(
            token={
                "symbol": sig.components.get("token_symbol", addr[:6]),
                "address": addr, "chain": chain, "url": token.get("url", ""),
                "security_score": token.get("security_score"),
            },
            divergence={
                **sig.components,
                "confidence": sig.confidence,
                "nominal_top_n_pct": token.get("nominal_top_n_pct"),
            },
            position_hint=hint,
        )
        await send_alert(msg)
    except Exception as e:
        logger.warning("accumulation_alert_failed", error=str(e))
