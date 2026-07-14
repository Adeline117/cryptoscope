"""Launch Radar — the low-float / early repricing opportunity lane.

This is deliberately an event pipeline, not a generic meme score.  A launch is
captured once with its first observable pool price, liquidity and risk facts;
the UI can then show whether the user is early enough to *consider* a tiny probe
or should only watch.  No automatic execution and no claim that a new token will
rise.
"""
from __future__ import annotations

import json
import urllib.request
from datetime import datetime, timezone

from src.pipeline.opportunity_ledger import active, record

PROFILES_URL = "https://api.dexscreener.com/token-profiles/latest/v1"
PAIRS_URL = "https://api.dexscreener.com/token-pairs/v1/{chain}/{token}"
SUPPORTED_CHAINS = {"solana", "base", "bsc", "ethereum"}
MAX_CANDIDATES = 30


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
    pairs = fetch(PAIRS_URL.format(chain=chain, token=token))
    pairs = pairs if isinstance(pairs, list) else []
    usable = [p for p in pairs if p.get("pairAddress") and p.get("priceUsd")]
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
        "fdv": fdv, "liquidity_usd": liq, "volume_m5": vol5,
        "buys_m5": buys, "sells_m5": sells, "flow_ratio": round(flow_ratio, 2),
        "boost_active": boost, "pair": pair.get("pairAddress"), "url": pair.get("url"),
        "reasons": reasons,
    }


def scan(fetch=_json, *, now: datetime | None = None, max_profiles: int = MAX_CANDIDATES) -> dict:
    """Discover profiles, enrich their deepest pool, and persist launch events."""
    now = now or datetime.now(timezone.utc)
    profiles = fetch(PROFILES_URL)
    profiles = profiles if isinstance(profiles, list) else []
    inserted = 0
    for profile in profiles[:max_profiles]:
        try:
            pair = _pair_for(profile, fetch)
            event = qualify(pair, now=now) if pair else None
            if event:
                _, new = record(event)
                inserted += int(new)
        except Exception:
            continue
    return {"scanned": len(profiles[:max_profiles]), "inserted": inserted,
            "events": active("launch"), "source": "DEX Screener profiles + pools"}


def view() -> dict:
    """Read-only board payload; scanning belongs to a scheduled ingestion path."""
    return {"events": active("launch"), "source": "Launch event ledger"}


if __name__ == "__main__":
    print(json.dumps(scan(), ensure_ascii=False, indent=2))
