"""Launch Radar — the low-float / early repricing opportunity lane.

This is deliberately an event pipeline, not a generic meme score.  A launch is
captured once with its first observable pool price, liquidity and risk facts;
the UI can then show whether the user is early enough to *consider* a tiny probe
or should only watch.  No automatic execution and no claim that a new token will
rise.
"""
from __future__ import annotations

import json
from copy import deepcopy
import urllib.request
from datetime import datetime, timedelta, timezone

from src.pipeline.opportunity_ledger import (
    active, event_id_readback_matches, record, record_if_absent,
)

PROFILES_URL = "https://api.dexscreener.com/token-profiles/latest/v1"
PAIRS_URL = "https://api.dexscreener.com/token-pairs/v1/{chain}/{token}"
SUPPORTED_CHAINS = {"solana", "base", "bsc", "ethereum"}
MAX_CANDIDATES = 30
MAX_EXECUTION_ASSESSMENTS = 5


def _num(value, default=0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _json(url: str) -> object:
    req = urllib.request.Request(url, headers={"User-Agent": "CryptoScope/LaunchRadar/1.0"})
    with urllib.request.urlopen(req, timeout=12) as response:
        return json.loads(response.read().decode())


def _pair_for(profile: dict, fetch=_json) -> dict | None:
    chain, token = profile.get("chainId"), profile.get("tokenAddress")
    if chain not in SUPPORTED_CHAINS or not token:
        return None
    return _pair_for_token(chain, token, fetch=fetch)


def _pair_for_token(chain: str, token: str, *, fetch=_json) -> dict | None:
    """Select the deepest observable pool for one identity-proven token."""
    pairs = fetch(PAIRS_URL.format(chain=chain, token=token))
    pairs = pairs if isinstance(pairs, list) else []
    def same_token(observed: object) -> bool:
        observed, expected = str(observed or ""), str(token)
        return observed == expected if chain == "solana" else observed.lower() == expected.lower()

    # The token-pairs endpoint can return pools where the queried token is only the
    # quote side, or even unrelated rows.  Launch price/FDV semantics require the
    # identity-proven mint to be the base asset; never let a deeper unrelated pool
    # replace it.
    usable = [
        p for p in pairs
        if (p.get("pairAddress") and p.get("priceUsd")
            and str(p.get("chainId") or "").lower() == str(chain).lower()
            and same_token((p.get("baseToken") or {}).get("address")))
    ]
    return max(usable, key=lambda p: _num((p.get("liquidity") or {}).get("usd")), default=None)


def qualify(pair: dict, *, now: datetime | None = None, source: str = "dexscreener") -> dict | None:
    """Turn one observable DEX pool into a conservative executable event card.

    The thresholds only establish that a token is tradeable enough to observe;
    they do not predict a pump. A high sniper/boost/volume signal never overrides
    inadequate liquidity or a late launch.
    """
    now = now or datetime.now(timezone.utc)
    base = pair.get("baseToken") or {}
    chain, token = pair.get("chainId"), base.get("address")
    price = _num(pair.get("priceUsd"))
    liq = _num((pair.get("liquidity") or {}).get("usd"))
    fdv = _num(pair.get("fdv") or pair.get("marketCap"))
    created_ms = pair.get("pairCreatedAt")
    if chain not in SUPPORTED_CHAINS or not token or price <= 0 or not created_ms:
        return None
    try:
        event_at = datetime.fromtimestamp(float(created_ms) / 1000, tz=timezone.utc)
    except (TypeError, ValueError, OSError):
        return None
    age_min = max(0.0, (now - event_at).total_seconds() / 60)
    tx5 = (pair.get("txns") or {}).get("m5") or {}
    buys, sells = int(tx5.get("buys") or 0), int(tx5.get("sells") or 0)
    vol5 = _num((pair.get("volume") or {}).get("m5"))
    boost = _num((pair.get("boosts") or {}).get("active"))
    # A pool outside these bounds is either not executable, not the early-error
    # regime, or already too old for this specific lane.
    if not (5_000 <= liq <= 2_000_000 and 10_000 <= fdv <= 10_000_000 and age_min <= 24 * 60):
        return None
    flow_ratio = buys / max(sells, 1)
    # $25 is a hard cap for the first probe at $5k liquidity, rising only with depth.
    # This prevents a visual "opportunity" from silently implying an unfillable bet.
    max_notional = round(min(500.0, max(25.0, liq * 0.003)), 2)
    # Frozen at discovery so later validation cannot choose a friendlier cost after
    # seeing the return. This is a conservative model, not a claim of a real fill:
    # constant-product impact on entry+exit plus a 0.60% DEX fee/routing buffer.
    from src.pipeline.slippage import price_impact
    roundtrip_cost = round(2 * price_impact(liq, max_notional) + 0.60, 3)
    from src.pipeline.execution_cost import discovery_contract
    from src.pipeline.edge_validation import COHORT_VERSION
    cost_contract = discovery_contract(
        notional_usd=max_notional, modeled_roundtrip_pct=roundtrip_cost,
        method="constant_product_roundtrip_plus_0.60pct_buffer_v1")
    ready = age_min <= 180 and buys >= 3 and flow_ratio >= 1.15 and vol5 >= liq * 0.015
    decision = "SMALL_PROBE" if ready else "WATCH"
    reasons = [f"首池 {age_min:.0f}m", f"FDV ${fdv:,.0f}", f"流动性 ${liq:,.0f}"]
    if buys or sells:
        reasons.append(f"5m 买/卖 {buys}/{sells}")
    if boost:
        reasons.append(f"推广 {boost:.0f}(仅作注意力，不是买入理由)")
    return {
        "lane": "launch", "chain": chain, "token": token,
        "symbol": base.get("symbol") or "?", "name": base.get("name") or "",
        "source": source, "event_at": event_at.isoformat(), "state": "live",
        "decision": decision, "entry_price": price,
        "invalidation_price": round(price * 0.70, 12),
        "max_notional_usd": max_notional, "age_min": round(age_min, 1),
        "roundtrip_cost_pct_est": roundtrip_cost,
        "cost_model": "constant_product_roundtrip_plus_0.60pct_buffer",
        # v5 starts a fresh protocol after v4's shared-calendar attrition flaw was
        # found. Earlier versions remain descriptive and can never be relabeled.
        "cost_contract": cost_contract, "cohort_version": COHORT_VERSION,
        "fdv": fdv, "liquidity_usd": liq, "volume_m5": vol5,
        "buys_m5": buys, "sells_m5": sells, "flow_ratio": round(flow_ratio, 2),
        "boost_active": boost, "pair": pair.get("pairAddress"), "url": pair.get("url"),
        "reasons": reasons,
    }


def _assess_candidate(event: dict, assessor, *, assessed: int,
                      max_assessments: int) -> tuple[dict, int]:
    if event.get("decision") != "SMALL_PROBE":
        return event, assessed
    if assessed < max(0, max_assessments):
        assessed += 1
        try:
            return assessor(event), assessed
        except Exception as exc:
            event["decision"] = "WATCH"
            event["security_gate"] = {
                "state": "unknown", "reason": f"assessment failed: {str(exc)[:60]}",
            }
            event["execution_probe"] = {"state": "skipped", "reason": "assessment failed"}
            return event, assessed
    event["decision"] = "WATCH"
    event["security_gate"] = {
        "state": "unknown", "reason": "per-scan assessment budget exhausted",
    }
    event["execution_probe"] = {"state": "skipped", "reason": "assessment budget exhausted"}
    event["reasons"].append("执行门降级:本轮安全/路由检查预算已用完")
    return event, assessed


def _execution_assessment(event: dict, *, assessed_at: datetime) -> dict:
    """Convert a mutable gate result into one append-only current measurement."""
    security, route = event.get("security_gate") or {}, event.get("execution_probe") or {}
    notional = float(event.get("max_notional_usd") or 0)
    from src.pipeline.execution_cost import route_contract, unknown_route_contract
    try:
        route_loss = float(route["roundtrip_loss_pct"])
        cost = route_contract(
            notional_usd=notional, route_loss_pct=max(0, route_loss),
            method=str(route.get("api_mode") or route.get("source") or "read_only_route"))
    except (KeyError, TypeError, ValueError):
        cost = unknown_route_contract(
            notional_usd=notional, method=str(route.get("source") or "route_unavailable"))
    quote_at = route.get("checked_at") or event.get("quote_at")
    expires_at = event.get("expires_at")
    security_at = security.get("checked_at") or assessed_at.isoformat()
    security_expires_at = None
    if security.get("state") == "pass":
        try:
            security_expires_at = (datetime.fromisoformat(security_at).astimezone(timezone.utc)
                                   + timedelta(minutes=5)).isoformat()
        except (TypeError, ValueError):
            security_expires_at = None
    sec_state, route_state = security.get("state") or "unknown", route.get("state") or "unknown"
    reasons = []
    if sec_state != "pass":
        reasons.append(f"security_{sec_state}")
    if route_state != "quoted":
        reasons.append(f"route_{route_state}")
    if cost["completeness"] != "complete":
        reasons.append("cost_incomplete")
    if route.get("entry_reference_price") is None:
        reasons.append("entry_reference_price_unknown")
    reasons.append("delivery_sla_unverified")
    return {
        "kind": "read_only_quote", "assessed_at": assessed_at.isoformat(),
        "security_state": sec_state,
        "security_at": security_at, "security_expires_at": security_expires_at,
        "route_state": route_state, "quote_source": route.get("source"),
        "quote_mode": route.get("api_mode"), "quote_at": quote_at,
        "quote_expires_at": expires_at, "expires_at": expires_at,
        "notional_usd": notional,
        "entry_reference_price": route.get("entry_reference_price"),
        "invalidation_reference_price": route.get("invalidation_reference_price"),
        "roundtrip_back_usd": route.get("roundtrip_back_usd"),
        "cost_contract": cost, "is_real_fill": False,
        "reason_code": reasons[0] if reasons else None,
        "action_reason_codes": reasons, "delivery_sla_state": "unverified",
        "security_gate": security, "execution_probe": route,
        "auto_execution_allowed": False,
    }


def _append_assessment(ident: str, event: dict, assessor, *, assessed: int,
                       max_assessments: int, now: datetime) -> tuple[dict, int]:
    """Assess a copy so current quotes can never mutate the discovery snapshot."""
    assessed_event, assessed = _assess_candidate(
        deepcopy(event), assessor, assessed=assessed, max_assessments=max_assessments)
    if event.get("decision") == "SMALL_PROBE":
        from src.pipeline.opportunity_ledger import append_execution_assessment
        append_execution_assessment(ident, _execution_assessment(assessed_event, assessed_at=now))
    return assessed_event, assessed


def _scan_primary_solana(fetch, *, now: datetime, assessor, assessed: int,
                         max_assessments: int, max_candidates: int) -> tuple[dict, int]:
    """Bridge standard Pump.fun log evidence into the conservative launch ledger."""
    from src.pipeline import solana_launch_stream as stream

    rows = stream.qualification_batch(now=now, limit=max_candidates)
    result = {"available": True, "attempted": 0, "recorded": 0, "inserted": 0,
              "pending": 0, "errors": 0, "screened_out": 0, "orphaned": 0}
    for raw in rows:
        result["attempted"] += 1
        try:
            pair = _pair_for_token("solana", raw["mint"], fetch=fetch)
        except Exception as exc:
            # A shared market-data failure should not fan out into one failed request
            # per mint. Record the first miss and retry it after the persisted backoff.
            stream.set_qualification(raw["signature"], "market_error", error=str(exc), at=now)
            result["errors"] += 1
            break
        if not pair:
            stream.set_qualification(raw["signature"], "market_pending",
                                     error="DEX pool not indexed yet", at=now)
            result["pending"] += 1
            continue
        event = qualify(pair, now=now, source="Pump.fun standard logs + DEX Screener pool")
        if event is None:
            created_ms = pair.get("pairCreatedAt")
            try:
                age_hours = ((now - datetime.fromtimestamp(
                    float(created_ms) / 1000, tz=timezone.utc)).total_seconds() / 3600)
            except (TypeError, ValueError, OSError):
                age_hours = 0
            terminal = age_hours > 24
            state = "screened_out" if terminal else "market_pending"
            reason = ("pool is older than the 24h launch window" if terminal
                      else "pool not yet within liquidity/FDV launch bounds")
            stream.set_qualification(raw["signature"], state, error=reason, at=now)
            result["screened_out" if terminal else "pending"] += 1
            continue
        event["detected_at"] = raw["detected_at"]
        event["decision_at"] = now.isoformat()
        event["primary_evidence"] = {
            "source": "Solana logsSubscribe + confirmed transaction",
            "program": stream.PUMP_FUN_PROGRAM,
            "signature": raw["signature"],
            "creator": raw["creator"],
            "slot": raw["slot"],
            "event_type": raw["event_type"],
            "evidence_state": "complete",
            "explorer_url": f"https://solscan.io/tx/{raw['signature']}",
        }
        ident, new = record_if_absent(event)
        if not event_id_readback_matches(
                ident, lane="launch", chain="solana", token=raw["mint"]):
            stream.set_qualification(
                raw["signature"], "ledger_orphan",
                error="opportunity ledger ID failed exact read-back",
                ledger_event_id=ident, at=now,
            )
            result["orphaned"] += 1
            continue
        _, assessed = _append_assessment(
            ident, event, assessor, assessed=assessed,
            max_assessments=max_assessments, now=now)
        stream.set_qualification(raw["signature"], "qualified_recorded",
                                 ledger_event_id=ident, at=now)
        result["recorded"] += 1
        result["inserted"] += int(new)
    return result, assessed


def _scan_primary_evm(fetch, *, now: datetime, assessor, assessed: int,
                      max_assessments: int, max_candidates: int) -> tuple[dict, int]:
    """Bridge exact official factory pools without guessing token identity."""
    from src.pipeline import evm_factory_stream as stream
    from src.pipeline.evm_launch_bridge import exact_pair, identify_target

    rows = stream.qualification_batch(now=now, limit=max_candidates)
    result = {"available": True, "attempted": 0, "recorded": 0, "inserted": 0,
              "pending": 0, "errors": 0, "screened_out": 0, "duplicates": 0,
              "orphaned": 0}
    backoffs = (180, 360, 900, 1800)
    for raw in rows:
        result["attempted"] += 1
        target, terminal = identify_target(raw)
        if terminal:
            stream.set_qualification(raw, terminal, reason="quote-side identity is not unique",
                                     at=now)
            result["screened_out"] += 1
            continue
        retry_after = backoffs[min(int(raw.get("qualification_attempts") or 0),
                                   len(backoffs) - 1)]
        try:
            pairs = fetch(PAIRS_URL.format(chain=raw["chain"], token=target))
            pair = exact_pair(raw, target, pairs)
        except Exception as exc:
            stream.set_qualification(raw, "market_error", reason=str(exc),
                                     target_token=target,
                                     retry_after_seconds=retry_after, at=now)
            result["errors"] += 1
            break
        if pair is None:
            stream.set_qualification(raw, "market_pending",
                                     reason="exact factory pool not indexed yet",
                                     target_token=target,
                                     retry_after_seconds=retry_after, at=now)
            result["pending"] += 1
            continue
        try:
            chain_time = datetime.fromisoformat(raw["block_at"]).astimezone(timezone.utc)
            market_time = datetime.fromtimestamp(
                float(pair["pairCreatedAt"]) / 1000, tz=timezone.utc)
        except (KeyError, TypeError, ValueError, OSError) as exc:
            stream.set_qualification(raw, "market_error",
                                     reason=f"invalid pool creation clock: {exc}",
                                     target_token=target,
                                     retry_after_seconds=retry_after, at=now)
            result["errors"] += 1
            continue
        if abs((market_time - chain_time).total_seconds()) > 600:
            stream.set_qualification(raw, "market_error",
                                     reason="market pool clock differs from factory by >10m",
                                     target_token=target,
                                     retry_after_seconds=retry_after, at=now)
            result["errors"] += 1
            continue
        event = qualify(pair, now=now,
                        source=f"{raw['chain']} {raw['venue']} factory + DEX Screener exact pool")
        if event is None:
            stream.set_qualification(raw, "below_threshold",
                                     reason="exact pool below current liquidity/FDV bounds",
                                     target_token=target,
                                     retry_after_seconds=retry_after, at=now)
            result["pending"] += 1
            continue
        # The factory row is the identity authority. DEX APIs may return a
        # checksum-cased address; store the canonical raw chain/token so the
        # cross-database read-back is exact rather than merely case-insensitive.
        event["chain"] = raw["chain"]
        event["token"] = target
        # The factory clock is the primary event time. The market decision is made
        # now; never backdate it to the raw socket receipt.
        event["event_at"] = chain_time.isoformat()
        event["detected_at"] = now.isoformat()
        event["decision_at"] = now.isoformat()
        event["primary_evidence"] = {
            "source": "official EVM factory log",
            "factory": raw["factory"], "venue": raw["venue"],
            "transaction_hash": raw["transaction_hash"],
            "log_index": raw["log_index"], "block_number": raw["block_number"],
            "block_hash": raw["block_hash"], "pool": raw["pool"],
            "raw_detected_at": raw["detected_at"], "evidence_state": "complete",
            "explorer_url": (f"https://bscscan.com/tx/{raw['transaction_hash']}"
                             if raw["chain"] == "bsc" else
                             f"https://basescan.org/tx/{raw['transaction_hash']}"
                             if raw["chain"] == "base" else
                             f"https://etherscan.io/tx/{raw['transaction_hash']}"),
        }
        ident, new = record_if_absent(event)
        if not new:
            stream.set_qualification(
                raw, "duplicate_token_existing",
                reason="token already has a first launch event",
                target_token=target, at=now,
            )
            result["duplicates"] += 1
            continue
        try:
            ledger_verified = event_id_readback_matches(
                ident, lane="launch", chain=raw["chain"], token=target)
            orphan_reason = "opportunity ledger ID failed exact read-back"
        except Exception as exc:
            ledger_verified = False
            orphan_reason = f"opportunity ledger exact read-back unavailable: {exc}"
        if not ledger_verified:
            stream.set_qualification(
                raw, "ledger_orphan",
                reason=orphan_reason,
                target_token=target, ledger_event_id=ident, at=now,
            )
            result["orphaned"] += 1
            continue
        _, assessed = _append_assessment(
            ident, event, assessor, assessed=assessed,
            max_assessments=max_assessments, now=now)
        stream.set_qualification(raw, "qualified_recorded", target_token=target,
                                 ledger_event_id=ident, at=now)
        result["recorded"] += 1
        result["inserted"] += 1
    return result, assessed


def scan(fetch=_json, *, now: datetime | None = None, max_profiles: int = MAX_CANDIDATES,
         assessor=None, max_assessments: int = MAX_EXECUTION_ASSESSMENTS,
         max_primary: int = 20, max_evm: int = 10) -> dict:
    """Discover pools, then safety/round-trip gate only raw actionable candidates.

    The hard assessment budget bounds GoPlus/router calls. Anything beyond the budget
    is WATCH, never an unchecked SMALL_PROBE.
    """
    now = now or datetime.now(timezone.utc)
    if assessor is None:
        from src.pipeline.launch_execution import assess as assessor
    inserted = assessed = 0
    try:
        primary, assessed = _scan_primary_solana(
            fetch, now=now, assessor=assessor, assessed=assessed,
            max_assessments=max_assessments, max_candidates=max(0, max_primary))
        inserted += primary["inserted"]
    except Exception as exc:
        primary = {"available": False, "attempted": 0, "recorded": 0, "inserted": 0,
                   "pending": 0, "errors": 1, "screened_out": 0,
                   "reason": str(exc)[:120]}
    try:
        primary_evm, assessed = _scan_primary_evm(
            fetch, now=now, assessor=assessor, assessed=assessed,
            max_assessments=max_assessments, max_candidates=max(0, max_evm))
        inserted += primary_evm["inserted"]
    except Exception as exc:
        primary_evm = {"available": False, "attempted": 0, "recorded": 0,
                       "inserted": 0, "pending": 0, "errors": 1,
                       "screened_out": 0, "duplicates": 0,
                       "reason": str(exc)[:120]}
    profiles = fetch(PROFILES_URL)
    profiles = profiles if isinstance(profiles, list) else []
    for profile in profiles[:max_profiles]:
        try:
            pair = _pair_for(profile, fetch)
            event = qualify(pair, now=now) if pair else None
            if event:
                ident, new = record(event)
                _, assessed = _append_assessment(
                    ident, event, assessor, assessed=assessed,
                    max_assessments=max_assessments, now=now)
                inserted += int(new)
        except Exception:
            continue
    return {"scanned": (len(profiles[:max_profiles]) + primary["attempted"]
                        + primary_evm["attempted"]),
            "profile_scanned": len(profiles[:max_profiles]), "primary": primary,
            "primary_evm": primary_evm,
            "assessed": assessed, "inserted": inserted,
            "events": active("launch"),
            "source": "Primary chain launch evidence + DEX Screener pools/profiles"}


def refresh_quotes(*, now: datetime | None = None, assessor=None,
                   max_candidates: int = 1, refresh_before_seconds: int = 30,
                   retry_after_seconds: int = 60) -> dict:
    """Refresh a bounded set of protocol-eligible v5 quotes without rediscovery.

    Discovery remains a three-minute event job. This fast path only appends current
    read-only measurements and never changes the frozen cohort, price, or cost.
    """
    from src.pipeline import opportunity_ledger as ledger

    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    from src.pipeline.edge_validation import is_protocol_event

    rows = [row for row in ledger.outcome_rows(open_only=True)
            if row.get("decision") == "SMALL_PROBE" and is_protocol_event(row)]
    recent = []
    for row in rows:
        try:
            if now - datetime.fromisoformat(row["detected_at"]).astimezone(timezone.utc) \
                    <= timedelta(hours=3):
                recent.append(row)
        except (TypeError, ValueError, KeyError):
            continue
    recent.sort(key=lambda row: row["detected_at"], reverse=True)
    result = {"eligible": len(recent), "attempted": 0, "refreshed": 0,
              "skipped_fresh": 0, "skipped_backoff": 0, "errors": 0}
    for row in recent:
        if result["attempted"] >= max(0, int(max_candidates)):
            break
        latest = ledger.latest_execution_assessment(row["id"])
        if latest:
            try:
                assessed_age = (now - datetime.fromisoformat(latest["assessed_at"])
                                 .astimezone(timezone.utc)).total_seconds()
            except (TypeError, ValueError):
                assessed_age = retry_after_seconds + 1
            try:
                remaining = (datetime.fromisoformat(latest["expires_at"])
                             .astimezone(timezone.utc) - now).total_seconds()
            except (TypeError, ValueError):
                remaining = None
            if latest.get("route_state") == "quoted" and remaining is not None \
                    and remaining > refresh_before_seconds:
                result["skipped_fresh"] += 1
                continue
            if latest.get("route_state") != "quoted" and assessed_age < retry_after_seconds:
                result["skipped_backoff"] += 1
                continue
        event = dict(row.get("payload") or {})
        for key in ("lane", "chain", "token", "symbol", "entry_price",
                    "invalidation_price", "max_notional_usd", "decision",
                    "cost_pct_est", "cost_model", "cost_contract", "cohort_version"):
            if row.get(key) is not None:
                event[key] = row[key]
        event.setdefault("reasons", [])
        current_assessor = assessor
        if current_assessor is None:
            from src.pipeline.launch_execution import gate, route_probe, security_probe

            cached_security = ((latest or {}).get("payload") or {}).get("security_gate")
            try:
                security_age = (now - datetime.fromisoformat(latest["security_at"])
                                .astimezone(timezone.utc)).total_seconds()
            except (TypeError, ValueError, KeyError):
                security_age = 301
            security = (cached_security if isinstance(cached_security, dict)
                        and security_age <= 300 else security_probe(event))
            route = (route_probe(event) if security.get("state") == "pass"
                     else {"state": "skipped", "reason": "security gate did not pass",
                           "read_only": True})
            current_assessor = lambda candidate, s=security, r=route: gate(
                candidate, s, r, now=now)
        result["attempted"] += 1
        try:
            _append_assessment(row["id"], event, current_assessor, assessed=0,
                               max_assessments=1, now=now)
            result["refreshed"] += 1
        except Exception:
            result["errors"] += 1
    return result


def view() -> dict:
    """Read-only board payload; scanning belongs to a scheduled ingestion path."""
    try:
        from src.pipeline import solana_launch_stream, stream_health
        streams = [item for item in stream_health.snapshot()
                   if item["source"] == "solana" and item["stream"] == "pump_fun_launches"]
        primary = {"available": True,
                   "qualification": solana_launch_stream.qualification_summary(
                       ledger_readback=lambda ident, mint: event_id_readback_matches(
                           ident, lane="launch", chain="solana", token=mint)),
                   "streams": streams}
    except Exception as exc:
        primary = {"available": False, "reason": str(exc)[:120], "streams": []}
    try:
        from src.pipeline import evm_factory_stream
        from src.pipeline.evm_launch_bridge import configured_stream_health
        evm_primary = {"available": True,
                       "qualification": evm_factory_stream.qualification_summary(
                           ledger_readback=lambda ident, chain, token:
                           event_id_readback_matches(
                               ident, lane="launch", chain=chain, token=token)),
                       "streams": configured_stream_health()}
    except Exception as exc:
        evm_primary = {"available": False, "reason": str(exc)[:120], "streams": []}
    return {"events": active("launch"),
            "primary_sources": {"solana": primary, "evm": evm_primary},
            "source": "Launch event ledger + primary chain stream health"}


if __name__ == "__main__":
    print(json.dumps(scan(), ensure_ascii=False, indent=2))
