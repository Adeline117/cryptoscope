"""Forward outcome measurement for the five-lane opportunity ledger.

An event card is not evidence of edge. This module settles the immutable first-seen
snapshot at 1h/24h/7d, subtracts the cost model frozen at discovery, and keeps the
result in the same ledger. It deliberately separates three claims:

* ``measured``: enough outcomes exist to describe the distribution;
* ``edge_verdict``: a comparable control exists and survives uncertainty;
* real execution: never claimed here -- Launch/Cascade costs are paper estimates.

Only lanes with a directional entry can be price-settled. Structure events remain
WATCH-only until they have a separately specified hypothesis; Airdrop and Carry use
their own reward/cost and delta-neutral accounting respectively.
"""
from __future__ import annotations

import json
import math
import statistics
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Callable
from urllib.parse import urlparse

import structlog

from src.pipeline import opportunity_ledger
from src.pipeline.edge_validation import (
    COHORT_VERSION,
    LAUNCH_COST_METHOD,
    LOOK_SIZES as EDGE_LOOK_SIZES,
    is_protocol_enrollment_candidate,
    is_protocol_event,
    launch_forward_validation,
    protocol_exclusion_reasons,
    protocol_point_state,
    protocol_snapshot,
)

logger = structlog.get_logger()

HORIZONS = (("1h", 1), ("24h", 24), ("7d", 7 * 24))
HORIZON_PRIORITY = {"24h": 0, "1h": 1, "7d": 2}
MIN_N = 20
MAX_PRICE_LOOKUPS = 20       # hard network/resource budget per hourly resolver run
UNRESOLVABLE_DAYS = 21       # 7d horizon plus a generous historical-data grace period
SUPPORTED_LANES = {"launch", "cascade"}
PriceResult = float | dict | None
PriceAt = Callable[[dict, datetime], PriceResult]


def _aware(value: str) -> datetime:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _direction(row: dict) -> str | None:
    if row.get("lane") == "launch":
        return "long"
    if row.get("lane") == "cascade":
        side = str((row.get("payload") or {}).get("side") or "").upper()
        return "short" if side == "SHORT" else "long" if side == "LONG" else None
    return None


def _is_current_launch_window(row: dict) -> bool:
    """Identify post-boundary v6 rows even when their snapshot is malformed.

    A malformed current row must not escape strict settlement merely because its
    entry observation cannot be normalized.  Pre-boundary v6 rows were emitted
    during deployment quarantine and remain explicitly descriptive.
    """
    return is_protocol_enrollment_candidate(row)


def _cost(row: dict) -> tuple[float, str]:
    """Return frozen cost evidence, failing closed for the current protocol."""
    if row.get("lane") == "launch":
        snapshot = protocol_snapshot(row)
        if snapshot is not None:
            return float(snapshot["all_in_cost_pct"]), LAUNCH_COST_METHOD
        if _is_current_launch_window(row):
            reasons = protocol_exclusion_reasons(row) or ["protocol_snapshot_invalid"]
            raise ValueError("launch_v6_cost_evidence_rejected:" + ",".join(reasons))

    # Only legacy/descriptive rows may use their historical frozen percentage or a
    # labelled reconstruction.  This fallback is forbidden after the v6 boundary.
    try:
        frozen = float(row.get("cost_pct_est"))
        if math.isfinite(frozen) and frozen >= 0:
            return frozen, str(row.get("cost_model") or "discovery_snapshot")
    except (TypeError, ValueError):
        pass
    payload = row.get("payload") or {}
    # Never trust a refreshable payload as the discovery-time cost. Legacy rows have
    # NULL frozen columns; after deployment a later scan may enrich their payload with
    # today's liquidity. Treat those rows as reconstructed even if that field appears.
    if row.get("lane") == "launch":
        # Old ledger rows predate the frozen field. Reconstruct from other immutable
        # discovery facts and label it as a reconstruction, never as a real fill.
        from src.pipeline.slippage import price_impact
        try:
            liq = float(payload.get("liquidity_usd") or 0)
            size = float(row.get("max_notional_usd") or 0)
            if liq > 0 and size > 0:
                return (round(2 * price_impact(liq, size) + 0.60, 3),
                        "legacy_reconstructed_constant_product_plus_0.60pct_buffer")
        except (TypeError, ValueError):
            pass
    return (0.20, "legacy_perp_roundtrip_0.20pct_buffer")


def _hyperliquid_price_at(row: dict, when: datetime) -> float | None:
    """Hourly Hyperliquid close nearest the requested horizon (public, keyless)."""
    coin = row.get("token") or row.get("symbol")
    if not coin:
        return None
    start = int((when - timedelta(hours=2)).timestamp() * 1000)
    end = int((when + timedelta(hours=2)).timestamp() * 1000)
    body = {"type": "candleSnapshot", "req": {"coin": coin, "interval": "1h",
                                                 "startTime": start, "endTime": end}}
    req = urllib.request.Request(
        "https://api.hyperliquid.xyz/info", data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json", "User-Agent": "CryptoScope/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=12) as response:
            candles = json.loads(response.read())
        usable = [c for c in candles if c.get("c") is not None and c.get("t") is not None]
        if not usable:
            return None
        best = min(usable, key=lambda c: abs(int(c["t"]) / 1000 - when.timestamp()))
        if abs(int(best["t"]) / 1000 - when.timestamp()) > 2 * 3600:
            return None
        return float(best["c"])
    except Exception:
        return None


def _outcome_anchor(row: dict) -> datetime:
    """Return the immutable price-observation clock used for every horizon.

    New events are anchored to the actual decision-time quote.  Rows created before
    that evidence contract remain descriptive and retain their historical detection
    clock; a malformed supplied observation never silently falls back to that clock.
    """
    if row.get("entry_observation") is not None:
        normalized = opportunity_ledger.validate_entry_observation(row)
        if not normalized:
            raise ValueError("entry observation is empty")
        return _aware(normalized["observed_at"])
    return _aware(row["detected_at"])


def _default_price_at(row: dict, when: datetime) -> PriceResult:
    if row.get("lane") == "launch":
        observation = row.get("entry_observation")
        if observation is not None:
            from src.pipeline.outcome_tracker import _price_observation_at
            return _price_observation_at(
                row.get("token"), row.get("chain"), observation.get("pair"), when,
            )
        from src.pipeline.outcome_tracker import _price_at
        return _price_at(row.get("token"), row.get("chain"), when,
                         raise_rate_limit=True)
    if row.get("lane") == "cascade":
        return _hyperliquid_price_at(row, when)
    return None


def _settled(entry: float, price: float, direction: str, cost_pct: float, *,
             anchor_at: datetime | None = None, target_at: datetime | None = None,
             observation_id: str | None = None) -> dict:
    raw = (price / entry - 1.0) * 100
    gross = -raw if direction == "short" else raw
    net = gross - cost_pct
    result = {"price": price, "gross_return_pct": round(gross, 4),
              "net_return_pct_est": round(net, 4), "positive_after_cost": net > 0}
    if anchor_at is not None:
        result["outcome_anchor_at"] = anchor_at.isoformat()
    if target_at is not None:
        result["target_at"] = target_at.isoformat()
    if observation_id is not None:
        result["price_observation_id"] = observation_id
    return result


def _next_due_task(row: dict, now: datetime) -> dict | None:
    """Choose one due horizon for an event, prioritizing the 24h evidence gate."""
    blocked = (row.get("outcome") or {}).get("settlement_blocked")
    if isinstance(blocked, dict) and blocked.get("permanent") is True:
        return None
    try:
        t0 = _outcome_anchor(row)
    except (TypeError, ValueError, KeyError):
        return None
    outcome = row.get("outcome") or {}
    horizons = outcome.get("horizons") or {}
    unavailable = set(outcome.get("unavailable_horizons") or [])
    attempted_at = outcome.get("attempted_at") or {}
    candidates = []
    for name, hours in HORIZONS:
        due_at = t0 + timedelta(hours=hours)
        if now < due_at or name in horizons or name in unavailable:
            continue
        candidates.append({
            "row": row, "name": name, "hours": hours, "due_at": due_at,
            "key": (HORIZON_PRIORITY[name], attempted_at.get(name) or "",
                    due_at.isoformat()),
        })
    return min(candidates, key=lambda task: task["key"], default=None)


def _select_due_tasks(rows: list[dict], now: datetime, budget: int) -> list[dict]:
    """Reserve one slot per live lane, then fill the remaining oldest priorities."""
    tasks = []
    for row in rows:
        if row.get("lane") not in SUPPORTED_LANES or not _direction(row):
            continue
        try:
            if float(row.get("entry_price") or 0) <= 0:
                continue
        except (TypeError, ValueError):
            continue
        task = _next_due_task(row, now)
        if task:
            tasks.append(task)
    budget = max(0, budget)
    if not tasks or budget == 0:
        return []
    lane_heads = []
    for lane in sorted({task["row"]["lane"] for task in tasks}):
        lane_heads.append(min((task for task in tasks if task["row"]["lane"] == lane),
                              key=lambda task: task["key"]))
    selected = sorted(lane_heads, key=lambda task: task["key"])[:budget]
    selected_ids = {task["row"]["id"] for task in selected}
    for task in sorted(tasks, key=lambda item: item["key"]):
        if len(selected) >= budget:
            break
        if task["row"]["id"] in selected_ids:
            continue
        selected.append(task)
        selected_ids.add(task["row"]["id"])
    return selected


def resolve(*, now: datetime | None = None, price_at: PriceAt | None = None,
            max_lookups: int = MAX_PRICE_LOOKUPS) -> dict:
    """Resolve at most one missing horizon per event within a hard lookup budget.

    One-horizon-per-event makes a backlog fair and bounds file descriptors/network
    work. Failed lookups are persisted as attempts and sorted to the back next run,
    preventing one dead pool from starving every newer event.
    """
    now = now or datetime.now(timezone.utc)
    price_at = price_at or _default_price_at
    rows = opportunity_ledger.outcome_rows(open_only=True)
    tasks = _select_due_tasks(rows, now, max_lookups)
    lookups = settled = retired = cost_rejected = 0
    source_backoff = None
    by_lane: dict[str, int] = {}
    by_horizon: dict[str, int] = {}
    for task in tasks:
        row, name, hours = task["row"], task["name"], task["hours"]
        try:
            entry = float(row.get("entry_price") or 0)
            t0 = _outcome_anchor(row)
        except (TypeError, ValueError, KeyError):
            continue
        if entry <= 0:
            continue
        outcome = dict(row.get("outcome") or {})
        horizons = dict(outcome.get("horizons") or {})
        unavailable = set(outcome.get("unavailable_horizons") or [])
        try:
            cost_pct, cost_model = _cost(row)
        except ValueError as exc:
            reasons = protocol_exclusion_reasons(row) or ["protocol_snapshot_invalid"]
            rejection = {
                "at": now.isoformat(),
                "horizon": name,
                "reason_code": "launch_v6_cost_evidence_rejected",
                "reasons": reasons,
                "detail": str(exc)[:240],
                "permanent": True,
            }
            outcome.update({
                "version": 1,
                "direction": _direction(row),
                "cost_is_real_fill": False,
                "cost_evidence_rejected": rejection,
                "settlement_blocked": rejection,
                "updated_at": now.isoformat(),
            })
            opportunity_ledger.save_outcome(row["id"], outcome, "open")
            cost_rejected += 1
            continue
        outcome.update({"version": 1, "direction": _direction(row),
                        "cost_pct_est": cost_pct, "cost_model": cost_model,
                        "cost_is_real_fill": False, "horizons": horizons})
        target_at = t0 + timedelta(hours=hours)
        observation = (row.get("price_observations") or {}).get(name)
        if observation is None:
            lookups += 1
            by_lane[row["lane"]] = by_lane.get(row["lane"], 0) + 1
            by_horizon[name] = by_horizon.get(name, 0) + 1
            outcome["last_attempt_at"] = now.isoformat()
            attempted_at = dict(outcome.get("attempted_at") or {})
            attempted_at[name] = now.isoformat()
            outcome["attempted_at"] = attempted_at
            attempts = dict(outcome.get("attempts") or {})
            attempts[name] = int(attempts.get(name) or 0) + 1
            outcome["attempts"] = attempts
        try:
            price_result = observation if observation is not None else price_at(row, target_at)
        except Exception as exc:
            from src.pipeline.evidence import OhlcvRateLimited
            from src.pipeline.outcome_tracker import PriceObservationInvalid
            if isinstance(exc, PriceObservationInvalid):
                rejected = dict(outcome.get("price_observation_rejected") or {})
                rejected[name] = {"at": now.isoformat(), "reason": str(exc)[:160]}
                outcome["price_observation_rejected"] = rejected
                price_result = None
            elif not isinstance(exc, OhlcvRateLimited):
                raise
            else:
                source_backoff = str(exc)[:120]
                outcome["price_source_backoff"] = {
                    "at": now.isoformat(), "horizon": name, "reason": source_backoff}
                outcome["updated_at"] = now.isoformat()
                opportunity_ledger.save_outcome(row["id"], outcome, "open")
                break
        state = "open"
        price = None
        observation_id = None
        if isinstance(price_result, dict):
            try:
                price = float(price_result.get("price"))
            except (TypeError, ValueError, OverflowError):
                price = None
            if row.get("entry_observation") is not None:
                try:
                    if observation is not None:
                        observation_id = observation.get("observation_id")
                    else:
                        observation_id, _inserted = opportunity_ledger.append_price_observation(
                            row["id"], name, price_result,
                        )
                except (TypeError, ValueError) as exc:
                    price = None
                    rejected = dict(outcome.get("price_observation_rejected") or {})
                    rejected[name] = {"at": now.isoformat(), "reason": str(exc)[:160]}
                    outcome["price_observation_rejected"] = rejected
        elif price_result is not None and row.get("entry_observation") is None:
            try:
                price = float(price_result)
            except (TypeError, ValueError, OverflowError):
                price = None
        if price is not None and math.isfinite(price) and price > 0:
            horizons[name] = _settled(
                entry, price, _direction(row), cost_pct, anchor_at=t0,
                target_at=target_at, observation_id=observation_id,
            )
            settled += 1
        elif (now - t0).total_seconds() >= UNRESOLVABLE_DAYS * 86400:
            # Retire this horizon, not the whole event: a missing 1h candle must not
            # prevent a valid 24h/7d outcome from entering the evidence sample.
            unavailable.add(name)
            outcome["unavailable_horizons"] = sorted(unavailable)
        complete = all(n in horizons or n in unavailable for n, _ in HORIZONS)
        if complete:
            state = "resolved" if "24h" in horizons else "unresolvable"
            if state == "unresolvable":
                outcome["unresolvable_reason"] = "24h historical price unavailable after grace"
                retired += 1
        outcome["updated_at"] = now.isoformat()
        opportunity_ledger.save_outcome(row["id"], outcome, state)
    result = {"lookups": lookups, "settled": settled, "retired": retired,
              "cost_evidence_rejected": cost_rejected,
              "lookups_by_lane": by_lane, "lookups_by_horizon": by_horizon,
              "source_backoff": source_backoff,
              "pending_events": sum(1 for r in rows if r.get("lane") in SUPPORTED_LANES)}
    logger.info("opportunity_outcomes_resolved", **result)
    return result


def _percentile(values: list[float], p: float) -> float | None:
    if not values:
        return None
    xs = sorted(values)
    pos = (len(xs) - 1) * p
    lo, hi = int(pos), min(int(pos) + 1, len(xs) - 1)
    weight = pos - lo
    value = xs[lo] * (1.0 - weight) + xs[hi] * weight
    return value if math.isfinite(value) else None


def _stable_median(values: list[float]) -> float | None:
    if not values:
        return None
    xs = sorted(values)
    middle = len(xs) // 2
    if len(xs) % 2:
        value = xs[middle]
    else:
        value = xs[middle - 1] / 2.0 + xs[middle] / 2.0
    return value if math.isfinite(value) else None


def _cohort(rows: list[dict], decision: str) -> dict:
    vals = []
    states = {"pending": 0, "unavailable": 0, "invalid": 0}
    eligible = 0
    for row in rows:
        if row.get("decision") != decision or not is_protocol_event(row):
            continue
        eligible += 1
        state, value = protocol_point_state(row)
        if state == "resolved" and value is not None:
            vals.append(float(value))
        elif state in states:
            states[state] += 1
    positives = sum(v > 0 for v in vals)
    return {"n": len(vals), "eligible_n": eligible, "resolved_n": len(vals),
            "pending_n": states["pending"],
            "unavailable_n": states["unavailable"],
            "invalid_n": states["invalid"],
            "positives": positives,
            "positive_rate": positives / len(vals) if vals else None,
            "median_net_24h": statistics.median(vals) if vals else None}


def _carry_stats(rows: list[dict]) -> dict:
    """Describe valid v3 quote proxies while keeping real-edge evidence at zero."""
    from src.pipeline.evidence import wilson
    from src.pipeline.carry_paper import proxy_exclusion_reasons

    closed = []
    total_closed = 0
    excluded: dict[str, int] = {}
    for row in rows:
        outcome = row.get("outcome") or {}
        if outcome.get("kind") != "delta_neutral_carry_paper":
            continue
        if row.get("outcome_state") != "resolved":
            continue
        total_closed += 1
        reasons = proxy_exclusion_reasons(outcome)
        if reasons:
            for reason in reasons:
                excluded[reason] = excluded.get(reason, 0) + 1
            continue
        value = outcome.get("net_proxy_after_book_quotes_and_modeled_fee_pct")
        closed.append(float(value))
    n = len(closed)
    positives = sum(value > 0 for value in closed)
    common = {
        "n": n, "n_proxy": n, "hits": positives, "real_edge_n": 0,
        "total_closed": total_closed,
        "excluded_closed": total_closed - n,
        "excluded_by_reason": excluded,
        "pending": sum(r.get("outcome_state") == "open" for r in rows),
        "metric": "quote_rate_integral_minus_book_quotes_and_modeled_fee_proxy",
        "cohort_kind": "descriptive_quote_proxy",
        "cost_completeness": "partial", "all_in_total_pct": None,
        "cost_is_real_fill": False,
        "execution_mode": "paper_orderbook_measurement",
        "real_edge_eligible": False,
    }
    if n < MIN_N:
        return {**common, "verdict": "不可判", "edge_verdict": "不可判",
                "note": (f"有效v3报价代理关闭样本 {n}/{MIN_N};另隔离 "
                         f"{total_closed - n} 个旧版/错误退出/盘口不全样本。"
                         "真实优势样本仍为0")}
    lo, hi = wilson(positives, n)
    return {
        **common, "verdict": "measured", "edge_verdict": "不可判",
        "positive_rate": round(positives / n, 3),
        "lo": round(lo, 3), "hi": round(hi, 3),
        "mean_net_proxy_pct": round(statistics.mean(closed), 4),
        "median_net_proxy_pct": round(statistics.median(closed), 4),
        "worst_net_proxy_pct": round(min(closed), 4),
        "note": ("已有描述性报价代理分布；仍缺实际资金费结算、basis、完整成本、"
                 "真实双腿成交与样本外验证，不能据此判定正EV"),
    }


def _airdrop_stats(rows: list[dict]) -> dict:
    """Separate transaction evidence from fully verified claim economics."""
    from src.pipeline.airdrop_radar import _transaction_url

    def finite_nonnegative(value: object) -> float | None:
        if isinstance(value, bool):
            return None
        try:
            number = float(value)
        except (TypeError, ValueError, OverflowError):
            return None
        return number if math.isfinite(number) and number >= 0 else None

    def canonical_timestamp(value: object) -> str | None:
        if not isinstance(value, str) or not value.strip():
            return None
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                return None
            return dt.astimezone(timezone.utc).isoformat()
        except (ValueError, OverflowError):
            return None

    claims = []
    transaction_verified = claim_semantics_verified = reward_valued = 0
    for row in rows:
        raw_outcome = row.get("outcome")
        outcome = raw_outcome if isinstance(raw_outcome, dict) else {}
        verification = outcome.get("transaction_verification")
        is_v3 = type(outcome.get("version")) is int and outcome.get("version") == 3
        has_transaction = (
            is_v3 and isinstance(verification, dict)
            and verification.get("onchain_success") is True
            and isinstance(verification.get("tx_id"), str)
            and bool(verification.get("tx_id"))
            and canonical_timestamp(verification.get("confirmed_at")) is not None
        )
        if has_transaction:
            transaction_verified += 1
        has_semantics = (
            has_transaction
            and verification.get("campaign_semantics_verified") is True
            and isinstance(verification.get("verified_campaign_id"), str)
            and bool(verification.get("verified_campaign_id"))
            and verification.get("verified_campaign_id") == row.get("token")
        )
        if has_semantics:
            claim_semantics_verified += 1
        has_reward_value = (
            has_semantics
            and verification.get("reward_amount_verified") is True
            and verification.get("reward_usd_verified") is True
            and finite_nonnegative(verification.get("verified_reward_amount")) is not None
            and finite_nonnegative(verification.get("verified_reward_usd")) is not None
            and isinstance(verification.get("verified_reward_asset"), str)
            and bool(verification.get("verified_reward_asset", "").strip())
        )
        if has_reward_value:
            reward_valued += 1

        payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
        source = (payload.get("source_verification")
                  if isinstance(payload.get("source_verification"), dict) else {})
        source_verified = (
            payload.get("source_state") == "source_verified"
            and source.get("official_page_verified") is True
            and source.get("evidence_page_verified") is True
            and payload.get("state") == "claimed"
            and payload.get("decision") == "CLAIMED"
        )
        claim_chain = (outcome.get("chain", "").strip().lower()
                       if isinstance(outcome.get("chain"), str) else None)
        row_chain = (row.get("chain", "").strip().lower()
                     if isinstance(row.get("chain"), str) else None)
        tx_url = outcome.get("tx_url")
        canonical_tx_url = (_transaction_url(tx_url, claim_chain)
                            if isinstance(claim_chain, str) else None)
        tx_id = verification.get("tx_id") if isinstance(verification, dict) else None
        expected_tx_id = (urlparse(tx_url).path.rstrip("/").rsplit("/", 1)[-1]
                          if isinstance(tx_url, str) else None)
        tx_identity_matches = (
            isinstance(tx_id, str) and isinstance(expected_tx_id, str)
            and (tx_id == expected_tx_id if claim_chain == "solana"
                 else tx_id.lower() == expected_tx_id.lower())
        )
        chain_matches = (
            isinstance(row_chain, str) and isinstance(claim_chain, str)
            and ((row_chain == "multi" and claim_chain != "multi")
                 or row_chain == claim_chain)
        )
        verified_amount = finite_nonnegative(
            verification.get("verified_reward_amount")
        ) if isinstance(verification, dict) else None
        outcome_amount = finite_nonnegative(outcome.get("verified_reward_amount"))
        raw_verified_asset = (verification.get("verified_reward_asset")
                              if isinstance(verification, dict) else None)
        verified_asset = (raw_verified_asset.strip()
                          if isinstance(raw_verified_asset, str) else None)
        verified_confirmed_at = (verification.get("confirmed_at")
                                 if isinstance(verification, dict) else None)
        canonical_shape = (
            outcome.get("campaign_id") == row.get("token")
            and chain_matches and canonical_tx_url == tx_url
            and tx_identity_matches
            and canonical_timestamp(outcome.get("claimed_at"))
            == canonical_timestamp(verified_confirmed_at)
            and canonical_timestamp(outcome.get("claimed_at")) is not None
            and canonical_timestamp(outcome.get("reported_claimed_at")) is not None
            and outcome_amount is not None and outcome_amount == verified_amount
            and isinstance(outcome.get("verified_reward_asset"), str)
            and outcome.get("verified_reward_asset") == verified_asset
        )
        fully_verified = (
            row.get("outcome_state") == "resolved"
            and outcome.get("kind") == "airdrop_claim"
            and outcome.get("claim_verification_state") == "fully_verified"
            and outcome.get("reward_is_claimed") is True
            and outcome.get("cost_is_actual") is True
            and has_reward_value
            and verification.get("beneficiary_verified") is True
            and verification.get("actual_cost_usd_verified") is True
            and isinstance(verification.get("verified_beneficiary"), str)
            and bool(verification.get("verified_beneficiary", "").strip())
            and outcome.get("campaign_id") == verification.get("verified_campaign_id")
            and source_verified
            and canonical_shape
            and all(outcome.get(flag) is True for flag in (
                "campaign_semantics_verified", "beneficiary_verified",
                "reward_amount_verified", "reward_usd_verified",
                "actual_cost_usd_verified",
            ))
        )
        if not fully_verified:
            continue
        try:
            gross = float(outcome["gross_reward_usd"])
            cost = float(outcome["actual_cost_usd"])
            net = float(outcome["net_reward_usd"])
            verified_gross = float(verification["verified_reward_usd"])
            verified_cost = float(verification["verified_actual_cost_usd"])
        except (KeyError, TypeError, ValueError, OverflowError):
            continue
        if (any(isinstance(value, bool) for value in (
                outcome.get("gross_reward_usd"), outcome.get("actual_cost_usd"),
                outcome.get("net_reward_usd"), verification.get("verified_reward_usd"),
                verification.get("verified_actual_cost_usd")))
                or not all(math.isfinite(value) for value in (
                    gross, cost, net, verified_gross, verified_cost))
                or min(gross, cost, verified_gross, verified_cost) < 0
                or gross != verified_gross or cost != verified_cost
                or not math.isclose(net, gross - cost, rel_tol=0, abs_tol=1e-9)):
            continue
        claims.append(outcome)
    common = {
        "n_events": len(rows), "n_claimed": len(claims),
        "n_transaction_verified": transaction_verified,
        "n_claim_semantics_verified": claim_semantics_verified,
        "n_reward_valued": reward_valued,
        "n_fully_verified_claims": len(claims),
        "pending": sum(r.get("outcome_state") == "open" for r in rows),
        "metric": "fully_verified_claim_net_usd", "edge_verdict": "不可判",
    }
    if not claims:
        return {**common, "verdict": "不可判",
                "note": (f"尚无完整领取与成本证据；交易执行已核验 {transaction_verified}；"
                         f"领取语义已核验 "
                         f"{claim_semantics_verified}；奖励已估值 {reward_valued}；"
                         "不完整记录不计入净回报")}
    nets = [float(c["net_reward_usd"]) for c in claims]
    try:
        gross_total = math.fsum(float(c["gross_reward_usd"]) for c in claims)
        cost_total = math.fsum(float(c["actual_cost_usd"]) for c in claims)
        net_total = math.fsum(nets)
        median_net = statistics.median(nets)
    except (OverflowError, statistics.StatisticsError):
        gross_total = cost_total = net_total = median_net = math.inf
    if not all(math.isfinite(value) for value in (
            gross_total, cost_total, net_total, median_net)):
        return {
            **common,
            "verdict": "不可判",
            "note": "完整证据记录的金额聚合非有限，已拒绝净回报汇总",
        }
    return {
        **common, "verdict": "realized_claims",
        "gross_reward_usd": round(gross_total, 2),
        "actual_cost_usd": round(cost_total, 2),
        "net_reward_usd": round(net_total, 2),
        "median_net_reward_usd": round(median_net, 2),
        "note": ("仅汇总领取语义、受益人、奖励数量/USD与实际成本全部核验的结果；"
                 "缺参与失败/资格未命中分母，不计算命中率或edge"),
    }


def _horizon_24h_progress(rows: list[dict], *, now: datetime | None = None) -> dict:
    now = now or datetime.now(timezone.utc)
    progress = {"resolved_24h": 0, "not_due_24h": 0, "due_24h": 0,
                "attempted_unpriced_24h": 0, "unavailable_24h": 0,
                "oldest_due_24h_hours": None}
    overdue = []
    for row in rows:
        try:
            due_at = _outcome_anchor(row) + timedelta(hours=24)
        except (TypeError, ValueError, KeyError):
            continue
        outcome = row.get("outcome") or {}
        if "24h" in (outcome.get("horizons") or {}):
            progress["resolved_24h"] += 1
        elif "24h" in set(outcome.get("unavailable_horizons") or []):
            progress["unavailable_24h"] += 1
        elif now < due_at:
            progress["not_due_24h"] += 1
        else:
            progress["due_24h"] += 1
            overdue.append((now - due_at).total_seconds() / 3600)
            if int((outcome.get("attempts") or {}).get("24h") or 0) > 0:
                progress["attempted_unpriced_24h"] += 1
    if overdue:
        progress["oldest_due_24h_hours"] = round(max(overdue), 1)
    return progress


def _strict_launch_24h(rows: list[dict], *, now: datetime | None = None) -> tuple[list[float], dict]:
    """Summarize v6 from the append-only point validator, never mutable outcomes."""
    now = now or datetime.now(timezone.utc)
    points: list[float] = []
    progress = {
        "resolved_24h": 0, "not_due_24h": 0, "due_24h": 0,
        "attempted_unpriced_24h": 0, "unavailable_24h": 0,
        "invalid_24h": 0, "oldest_due_24h_hours": None,
    }
    overdue: list[float] = []
    for row in rows:
        if not is_protocol_event(row):
            continue
        snapshot = protocol_snapshot(row)
        if snapshot is None:
            # Defensive only: is_protocol_event and protocol_snapshot share the same
            # validator, but a future refactor must still fail closed here.
            progress["invalid_24h"] += 1
            continue
        state, value = protocol_point_state(row)
        if state == "resolved" and value is not None:
            points.append(float(value))
            progress["resolved_24h"] += 1
            continue
        if state == "unavailable":
            progress["unavailable_24h"] += 1
            continue
        if state == "invalid":
            progress["invalid_24h"] += 1
            continue
        due_at = _aware(snapshot["anchor_at"]) + timedelta(hours=24)
        if now < due_at:
            progress["not_due_24h"] += 1
            continue
        progress["due_24h"] += 1
        overdue.append((now - due_at).total_seconds() / 3600)
        outcome = row.get("outcome") if isinstance(row.get("outcome"), dict) else {}
        if int((outcome.get("attempts") or {}).get("24h") or 0) > 0:
            progress["attempted_unpriced_24h"] += 1
    if overdue:
        progress["oldest_due_24h_hours"] = round(max(overdue), 1)
    return points, progress


def _legacy_launch_distribution(rows: list[dict]) -> dict:
    """Keep pre-v6 mutable outcomes visible but explicitly non-evidentiary."""
    legacy = [row for row in rows if not is_protocol_enrollment_candidate(row)]
    points: list[float] = []
    invalid = 0
    for row in legacy:
        point = ((row.get("outcome") or {}).get("horizons") or {}).get("24h")
        raw = point.get("net_return_pct_est") if isinstance(point, dict) else None
        if raw is None:
            continue
        if isinstance(raw, bool):
            invalid += 1
            continue
        try:
            value = float(raw)
        except (TypeError, ValueError, OverflowError):
            invalid += 1
            continue
        if not math.isfinite(value):
            invalid += 1
            continue
        points.append(value)
    positives = sum(value > 0 for value in points)
    result = {
        "sample_kind": "legacy_mutable_outcome_descriptive_only",
        "n_events": len(legacy), "n": len(points), "hits": positives,
        "invalid_n": invalid,
        "pending": sum(row.get("outcome_state") == "open" for row in legacy),
        "unresolvable": sum(
            row.get("outcome_state") == "unresolvable" for row in legacy
        ),
        "edge_eligible": False, "real_edge_n": 0,
        "real_edge_eligible": False, "execution_edge_eligible": False,
        "auto_execution_allowed": False,
        "note": "历史可变 outcome 仅作描述，不进入当前协议分母或优势判决",
        **_horizon_24h_progress(legacy),
    }
    if points:
        median = _stable_median(points)
        p90 = _percentile(points, 0.90)
        p99 = _percentile(points, 0.99)
        result.update({
            "rate": round(positives / len(points), 3),
            "median_net_24h": round(median, 3) if median is not None else None,
            "p90_net_24h": round(p90, 3) if p90 is not None else None,
            "p99_net_24h": round(p99, 3) if p99 is not None else None,
            "max_net_24h": round(max(points), 3),
        })
    return result


def _launch_stats(rows: list[dict]) -> dict:
    """Build the Launch headline exclusively from the strict append-only protocol."""
    from src.pipeline.evidence import wilson

    validation = launch_forward_validation(rows)
    points, progress = _strict_launch_24h(rows)
    probe = _cohort(rows, "SMALL_PROBE")
    control = _cohort(rows, "WATCH")
    n = len(points)
    positives = sum(value > 0 for value in points)
    version_counts: dict[str, int] = {}
    for row in rows:
        raw_version = row.get("cohort_version")
        if raw_version is None:
            label = "unversioned"
        elif isinstance(raw_version, bool) or not isinstance(raw_version, int):
            label = "invalid"
        else:
            label = f"v{raw_version}"
        version_counts[label] = version_counts.get(label, 0) + 1

    legacy = _legacy_launch_distribution(rows)
    common = {
        "n": n, "hits": positives,
        "pending": progress["not_due_24h"] + progress["due_24h"],
        "unresolvable": progress["unavailable_24h"],
        "metric": "append_only_exact_pool_24h_positive_after_frozen_full_paper_cost",
        "cost_is_real_fill": False,
        **progress,
        "legacy_distribution": legacy,
        "legacy_unfrozen_n": sum((row.get("cohort_version") or 0) < 2 for row in rows),
        "legacy_v2_descriptive_n": sum(row.get("cohort_version") == 2 for row in rows),
        "legacy_v3_descriptive_n": sum(row.get("cohort_version") == 3 for row in rows),
        "legacy_v4_descriptive_n": sum(row.get("cohort_version") == 4 for row in rows),
        "legacy_v5_descriptive_n": sum(row.get("cohort_version") == 5 for row in rows),
        "cohorts_by_version": dict(sorted(version_counts.items())),
        f"v{COHORT_VERSION}_cost_method": LAUNCH_COST_METHOD,
        "probe": probe, "control": control,
        "edge_validation": validation,
        "edge_verdict": validation["edge_verdict"],
        "edge_note": validation["reason"],
        "sample_kind": validation["sample_kind"],
        "selection_stage": validation["selection_stage"],
        "real_edge_n": 0, "real_edge_eligible": False,
        "execution_edge_eligible": False, "auto_execution_allowed": False,
        "current_protocol": {
            "protocol_id": validation["protocol_id"],
            "cohort_version": validation["cohort_version"],
            "protocol_start_at": validation["protocol_start_at"],
            "eligible_n": validation["eligible_n"],
            "excluded_n": validation["excluded_n"],
            "excluded_by_reason": validation["excluded_by_reason"],
            "integrity_candidate_n": sum(
                is_protocol_enrollment_candidate(row) for row in rows
            ),
            "integrity_invalid_n": validation.get("integrity_invalid_n", 0),
            "integrity_invalid_by_reason": validation.get(
                "integrity_invalid_by_reason", {}
            ),
        },
    }
    if n < MIN_N:
        return {
            **common, "verdict": "不可判",
            "note": (f"当前协议追加式24h样本 {n}/{MIN_N};到期待结算 "
                     f"{progress['due_24h']}；历史可变样本另列且不入分母"),
        }
    rate = positives / n
    lo, hi = wilson(positives, n)
    median = _stable_median(points)
    p90 = _percentile(points, 0.90)
    p99 = _percentile(points, 0.99)
    return {
        **common, "verdict": "measured", "rate": round(rate, 3),
        "lo": round(lo, 3), "hi": round(hi, 3),
        "median_net_24h": round(median, 3) if median is not None else None,
        "p90_net_24h": round(p90, 3) if p90 is not None else None,
        "p99_net_24h": round(p99, 3) if p99 is not None else None,
        "max_net_24h": round(max(points), 3),
    }


def actionability_gate(lane: str) -> dict:
    """Translate measured evidence into a fail-closed live action gate.

    The recorded cohort decision remains immutable so paper outcomes can accumulate.
    This gate only controls whether a current quote may be presented as actionable.
    A technical route is not an edge: promotion requires a cost-after 24h cohort and
    a contemporaneous WATCH control whose Wilson intervals no longer overlap.
    """
    if lane not in SUPPORTED_LANES:
        return {"state": "not_applicable", "lane": lane}
    try:
        stat = lane_stats().get(lane) or {}
    except Exception as exc:
        return {"state": "blocked", "lane": lane, "edge_verdict": "不可判",
                "reason": f"evidence read failed: {str(exc)[:80]}"}
    edge = stat.get("edge_verdict") or "不可判"
    validation = stat.get("edge_validation") or {}
    probe, control = stat.get("probe") or {}, stat.get("control") or {}
    strict_measured_n = (
        int(probe.get("resolved_n") or 0) + int(control.get("resolved_n") or 0)
        if lane == "launch" else int(stat.get("n") or 0)
    )
    common = {"lane": lane, "edge_verdict": edge,
              "minimum_n": EDGE_LOOK_SIZES[0] if lane == "launch" else MIN_N,
              "measured_n": strict_measured_n,
              "cost_is_real_fill": stat.get("cost_is_real_fill", False),
              "real_edge_n": 0, "real_edge_eligible": False,
              "execution_edge_eligible": False,
              "auto_execution_allowed": False}
    if lane == "launch":
        validation_state = validation.get("state")
        detail = {"protocol_id": validation.get("protocol_id"),
                  "protocol_state": validation_state,
                  "look_n_per_arm": validation.get("look_n_per_arm"),
                  "next_look_n_per_arm": validation.get("next_look_n_per_arm"),
                  "integrity_invalid_n": validation.get("integrity_invalid_n", 0),
                  "integrity_invalid_by_reason": validation.get(
                      "integrity_invalid_by_reason", {}),
                  "sample_kind": validation.get("sample_kind"),
                  "selection_stage": validation.get("selection_stage"),
                  "cost_evidence_policy": validation.get("cost_evidence_policy"),
                  "price_evidence_policy": validation.get("price_evidence_policy")}
        if validation_state == "pass":
            return {**common, **detail, "state": "pass",
                    "reason": validation.get("reason")}
        if validation_state == "no_edge_observed":
            return {**common, **detail, "state": "blocked",
                    "reason": validation.get("reason")}
        if validation_state in {
            "protocol_integrity_blocked", "coverage_blocked",
            "invalid_evidence", "regime_overlap_blocked",
            "validator_unavailable",
        }:
            return {**common, **detail, "state": "blocked",
                    "reason": validation.get("reason")}
        return {**common, **detail, "state": "collecting",
                "probe_n": int(probe.get("n") or 0),
                "control_n": int(control.get("n") or 0),
                "reason": (validation.get("reason")
                           or "前向 SPA/Reality Check 证据尚不可判")}
    return {**common, "state": "collecting",
            "reason": "Cascade 尚无同期可比 WATCH 对照；仅继续纸面测量"}


def lane_stats() -> dict:
    """Honest 24h distribution and, only where possible, a control comparison."""
    from src.pipeline.evidence import wilson

    rows = opportunity_ledger.outcome_rows()
    out: dict[str, dict] = {}
    # A configured-but-empty workbench is still a measurable state. Always expose
    # Airdrop's zero-event scorecard so the UI cannot hide missing discovery behind
    # an absent JSON key.
    for lane in sorted({r["lane"] for r in rows} | {"airdrop", "launch"}):
        lane_rows = [r for r in rows if r["lane"] == lane]
        if lane == "launch":
            out[lane] = _launch_stats(lane_rows)
            continue
        if lane == "carry":
            out[lane] = _carry_stats(lane_rows)
            continue
        if lane == "airdrop":
            out[lane] = _airdrop_stats(lane_rows)
            continue
        measurable = [r for r in lane_rows if _direction(r) and r.get("entry_price")]
        if not measurable:
            note = ("公开结构事件没有方向假设,不把事后涨跌伪装成命中率" if lane == "structure"
                    else "本线使用非价格结果账本,不套用方向命中率")
            out[lane] = {"verdict": "not_directional", "n_events": len(lane_rows),
                         "pending": 0, "note": note}
            continue
        points = []
        for row in measurable:
            point = ((row.get("outcome") or {}).get("horizons") or {}).get("24h")
            if point and point.get("net_return_pct_est") is not None:
                points.append(float(point["net_return_pct_est"]))
        n, positives = len(points), sum(v > 0 for v in points)
        pending = sum(r.get("outcome_state") == "open" for r in measurable)
        unresolvable = sum(r.get("outcome_state") == "unresolvable" for r in measurable)
        common = {"n": n, "hits": positives, "pending": pending,
                  "unresolvable": unresolvable, "metric": "positive_after_estimated_cost",
                  "cost_is_real_fill": False}
        common.update(_horizon_24h_progress(measurable))
        if n < MIN_N:
            out[lane] = {**common, "verdict": "不可判",
                         "edge_verdict": common.get("edge_verdict", "不可判"),
                         "note": (f"24h样本 {n}/{MIN_N};到期待结算"
                                  f" {common['due_24h']},继续积累")}
            continue
        rate = positives / n
        lo, hi = wilson(positives, n)
        stat = {**common, "verdict": "measured", "rate": round(rate, 3),
                "lo": round(lo, 3), "hi": round(hi, 3),
                "median_net_24h": round(statistics.median(points), 3),
                "p90_net_24h": round(_percentile(points, 0.90) or 0, 3),
                "p99_net_24h": round(_percentile(points, 0.99) or 0, 3),
                "max_net_24h": round(max(points), 3),
                "edge_verdict": common.get("edge_verdict", "不可判"),
                "edge_note": common.get("edge_note", "缺少同期可比对照")}
        out[lane] = stat
    return out


if __name__ == "__main__":
    from dotenv import load_dotenv
    from src.config import PROJECT_ROOT

    load_dotenv(PROJECT_ROOT / ".env")
    print(json.dumps({"resolve": resolve(), "lanes": lane_stats()}, ensure_ascii=False, indent=1))
