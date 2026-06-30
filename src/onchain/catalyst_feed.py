"""Imminent dump/pump CATALYST feed — the layer the system was missing.

We caught ESPORTS (Yooldo Games) only *after* it dumped, because nothing in the
pipeline watched for the trigger: a KuCoin **listing** catalyst. On-chain holder /
clustering signals tell us a token is *primed* (concentrated, accumulated, operator
in control) but not *when* the operator will pull the trigger. A primed token sitting
on top of a near-term **token unlock** (supply shock → dump) or a fresh **CEX listing**
(liquidity / exit-pump → dump-into-strength) is the difference between "watch" and "act".

This module is that calendar layer. `catalyst_for()` answers one question for a token:
is there a known near-term event that meaningfully raises dump/pump odds, and when.

Data sources (free / keyed, all best-effort — never raise, degrade to empty):
- **Token unlocks**: DefiLlama emissions dataset, served KEYLESS from the public
  `defillama-datasets.llama.fi` host (the `api.llama.fi/emissions` route is now 402
  paywalled — we deliberately use the dataset host instead). Covers ~340 protocols
  with vesting/cliff schedules. Symbol/contract → protocol slug is resolved via
  CoinGecko (demo key) contract lookup + the DefiLlama protocol list.
- **CEX listing signal**: NO reliable keyless real-time listing-announcement feed
  exists (the ones that matter — Binance/KuCoin/Upbit announcement RSS — are either
  geo/JS-gated, rate-limited, or paid). Rather than fake it, `cex_listing_signal()`
  is an HONEST STUB: it returns "no signal" today and is the documented extension
  point. Wire a real feed (e.g. a paid listings API, an announcement scraper, or a
  CEX-deposit-wallet inflow detector) into `_listing_probe()` to light it up.

The unlock path is live and verified; the listing path is intentionally inert.
"""

from __future__ import annotations

import json
import os
import time
import urllib.request
from datetime import datetime, timezone
from typing import Any

import structlog

import src.config  # noqa: F401 — side effect: loads .env so COINGECKO_API_KEY is set

logger = structlog.get_logger()

# --------------------------------------------------------------------------
# Endpoints / constants
# --------------------------------------------------------------------------
_UA = "CryptoScope/1.0"
# DefiLlama unlocks, keyless dataset host (the api.llama.fi/emissions route is 402-paywalled).
_LLAMA_PROTO_LIST = "https://defillama-datasets.llama.fi/emissionsProtocolsList"
_LLAMA_EMISSIONS = "https://defillama-datasets.llama.fi/emissions/{slug}"
_CG_CONTRACT = "https://api.coingecko.com/api/v3/coins/{platform}/contract/{address}"
_CG_PRICE = "https://api.coingecko.com/api/v3/simple/price?ids={ids}&vs_currencies=usd"

# How near-term an unlock has to be to count as a catalyst.
_DEFAULT_WINDOW_DAYS = 14
# An unlock below this share of (max) supply is noise, not a dump catalyst.
_MIN_UNLOCK_PCT = 0.5

# Our chain ids → CoinGecko asset-platform ids (for contract→id resolution).
_CG_PLATFORM = {
    "bsc": "binance-smart-chain",
    "binance-smart-chain": "binance-smart-chain",
    "bnb": "binance-smart-chain",
    "eth": "ethereum",
    "ethereum": "ethereum",
    "solana": "solana",
    "sol": "solana",
    "base": "base",
    "arbitrum": "arbitrum-one",
    "arb": "arbitrum-one",
    "optimism": "optimistic-ethereum",
    "op": "optimistic-ethereum",
    "polygon": "polygon-pos",
    "matic": "polygon-pos",
    "avalanche": "avalanche",
    "avax": "avalanche",
}

# Small, cheap process caches (the protocol list + per-slug emissions rarely change
# within a run; this keeps a screener pass over many tokens to a few HTTP calls).
_proto_list_cache: list[str] | None = None
_emissions_cache: dict[str, dict | None] = {}


# --------------------------------------------------------------------------
# Low-level HTTP (defensive — returns None on any failure, never raises)
# --------------------------------------------------------------------------

def _get_json(url: str, headers: dict | None = None, timeout: int = 20) -> Any | None:
    try:
        h = {"User-Agent": _UA, "accept": "application/json"}
        if headers:
            h.update(headers)
        req = urllib.request.Request(url, headers=h)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode())
    except Exception as e:
        logger.debug("catalyst_http_failed", url=url[:90], error=str(e)[:90])
        return None


def _cg_headers() -> dict:
    key = os.environ.get("COINGECKO_API_KEY", "").strip()
    return {"x-cg-demo-api-key": key} if key else {}


# --------------------------------------------------------------------------
# Symbol / contract → DefiLlama protocol slug resolution
# --------------------------------------------------------------------------

def _protocol_list() -> list[str]:
    """Cached list of DefiLlama emission protocol slugs (~340). Empty on failure."""
    global _proto_list_cache
    if _proto_list_cache is None:
        data = _get_json(_LLAMA_PROTO_LIST)
        _proto_list_cache = [str(s) for s in data] if isinstance(data, list) else []
    return _proto_list_cache


def _emissions(slug: str) -> dict | None:
    """Cached per-protocol emissions blob. None on failure / unknown slug."""
    if slug not in _emissions_cache:
        data = _get_json(_LLAMA_EMISSIONS.format(slug=slug))
        _emissions_cache[slug] = data if isinstance(data, dict) else None
    return _emissions_cache[slug]


def _coingecko_id(token: str, chain: str) -> str | None:
    """Resolve a contract address → CoinGecko coin id (which is very often identical
    to the DefiLlama emission slug, e.g. arb→'arbitrum'). None if not a contract or
    not found."""
    platform = _CG_PLATFORM.get((chain or "").strip().lower())
    if not platform or not token or not str(token).startswith("0x"):
        return None
    data = _get_json(
        _CG_CONTRACT.format(platform=platform, address=token.strip().lower()),
        headers=_cg_headers(),
    )
    if isinstance(data, dict) and data.get("id"):
        return str(data["id"])
    return None


def _candidate_slugs(token: str, chain: str, symbol: str | None) -> list[str]:
    """Ordered, de-duplicated candidate DefiLlama slugs to probe for this token.

    Strategy (cheap → fuzzy): CoinGecko id (most reliable, slug≈gecko_id) → the
    symbol itself → substring matches against the protocol list. We confirm the
    eventual match in `_resolve_protocol` so a loose substring can't mislead us.
    """
    cands: list[str] = []
    cg_id = _coingecko_id(token, chain)
    if cg_id:
        cands.append(cg_id)
    sym = (symbol or "").strip().lower()
    if sym:
        cands.append(sym)
    # Substring search over the protocol list (e.g. 'op' → 'optimism-foundation').
    if sym and len(sym) >= 2:
        for slug in _protocol_list():
            if sym == slug or sym in slug.split("-"):
                cands.append(slug)
    # De-dup, preserve order.
    seen: set[str] = set()
    out: list[str] = []
    for c in cands:
        if c and c not in seen:
            seen.add(c)
            out.append(c)
    return out


def _confirm(emis: dict, token: str, chain: str, symbol: str | None, cg_id: str | None) -> bool:
    """Sanity-check that an emissions blob actually corresponds to our token, so a
    fuzzy slug match can't attach the wrong unlock schedule. Confirms on ANY of:
    contract-address match, gecko_id match, or protocol-name match."""
    # Contract address embedded in metadata.token == "chain:0xADDR".
    tok = ((emis.get("metadata") or {}).get("token") or "")
    if token and str(token).startswith("0x") and token.strip().lower() in str(tok).lower():
        return True
    if cg_id and str(emis.get("gecko_id") or "").lower() == cg_id.lower():
        return True
    sym = (symbol or "").strip().lower()
    name = str(emis.get("name") or "").lower()
    if sym and (sym == name or sym in name.split()):
        return True
    return False


def _resolve_protocol(token: str, chain: str, symbol: str | None) -> dict | None:
    """Find and confirm the DefiLlama emissions blob for this token. None if unknown
    (the common case — DefiLlama only tracks ~340 protocols, so most 妖币 won't match,
    which is fine: no unlock data simply means no unlock catalyst)."""
    cg_id = _coingecko_id(token, chain)
    for slug in _candidate_slugs(token, chain, symbol):
        emis = _emissions(slug)
        if emis and _confirm(emis, token, chain, symbol, cg_id):
            return emis
    return None


# --------------------------------------------------------------------------
# Unlock parsing
# --------------------------------------------------------------------------

def _max_supply(emis: dict) -> float:
    try:
        v = (emis.get("supplyMetrics") or {}).get("maxSupply")
        return float(v) if v else 0.0
    except (TypeError, ValueError):
        return 0.0


def _spot_price(emis: dict) -> float:
    """Best-effort USD spot price via CoinGecko (gecko_id). 0.0 if unavailable."""
    gid = str(emis.get("gecko_id") or "").strip()
    if not gid:
        return 0.0
    data = _get_json(_CG_PRICE.format(ids=gid), headers=_cg_headers())
    try:
        return float(((data or {}).get(gid) or {}).get("usd") or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _parse_unlock_events(emis: dict, within_days: int) -> list[dict]:
    """Extract FUTURE unlock events within `within_days`, newest-soonest first.

    DefiLlama event shape: {timestamp (unix s), noOfTokens: [float,...], category,
    unlockType, description}. We sum noOfTokens per event and (best-effort) price it.
    """
    events = (emis.get("metadata") or {}).get("events") or []
    if not isinstance(events, list):
        return []
    now = time.time()
    horizon = now + within_days * 86400
    max_supply = _max_supply(emis)
    price = None  # lazily fetched only if we actually have qualifying unlocks

    out: list[dict] = []
    for ev in events:
        if not isinstance(ev, dict):
            continue
        try:
            ts = float(ev.get("timestamp") or 0)
        except (TypeError, ValueError):
            continue
        if ts <= now or ts > horizon:
            continue
        tokens = ev.get("noOfTokens") or []
        try:
            amt = float(sum(float(t) for t in tokens)) if isinstance(tokens, list) else 0.0
        except (TypeError, ValueError):
            amt = 0.0
        pct = round(amt / max_supply * 100, 4) if max_supply > 0 else None
        if price is None:
            price = _spot_price(emis)
        usd = round(amt * price, 2) if price else None
        out.append({
            "date": datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d"),
            "days_until": int((ts - now) // 86400),
            "category": ev.get("category"),
            "unlock_type": ev.get("unlockType"),
            "tokens": amt,
            "pct_of_max_supply": pct,
            "usd": usd,
            "protocol": emis.get("name"),
        })
    out.sort(key=lambda e: e["days_until"])
    return out


# --------------------------------------------------------------------------
# Public: unlocks
# --------------------------------------------------------------------------

def upcoming_unlocks(
    symbol_or_token: str,
    chain: str = "",
    symbol: str | None = None,
    within_days: int = _DEFAULT_WINDOW_DAYS,
) -> list[dict]:
    """Near-term token-unlock events for a token, soonest first. Empty if the token
    has no DefiLlama unlock schedule (the common case) or on any failure.

    `symbol_or_token` may be a contract address (best — enables CoinGecko resolution)
    or a symbol. Pass `chain` + `symbol` when you have them for the strongest match.
    Each item: {date, days_until, category, unlock_type, tokens, pct_of_max_supply,
    usd, protocol}.
    """
    try:
        sym = symbol or (None if str(symbol_or_token).startswith("0x") else symbol_or_token)
        emis = _resolve_protocol(symbol_or_token, chain, sym)
        if not emis:
            return []
        return _parse_unlock_events(emis, within_days)
    except Exception as e:
        logger.debug("upcoming_unlocks_failed", token=str(symbol_or_token)[:42], error=str(e)[:90])
        return []


# --------------------------------------------------------------------------
# Public: CEX listing — HONEST STUB (documented extension point)
# --------------------------------------------------------------------------

def _listing_probe(token: str, chain: str, symbol: str | None) -> dict | None:
    """EXTENSION POINT for a real CEX-listing-announcement signal.

    Intentionally returns None (no signal). There is no reliable keyless real-time
    listing feed; wiring one in here (paid listings API, announcement scraper, or a
    CEX-hot-wallet inflow detector — a fresh listing is preceded by the project moving
    supply to the exchange's deposit wallet) lights up the listing catalyst without
    touching any caller. Return shape when implemented:
        {"exchange": "KuCoin", "detail": "...", "window": "~2d", "source": "..."}
    """
    return None


def cex_listing_signal(token: str, chain: str = "", symbol: str | None = None) -> dict:
    """Best-effort CEX-listing catalyst. Currently an honest stub: always reports no
    signal (see module docstring + `_listing_probe`). Never raises."""
    try:
        sig = _listing_probe(token, chain, symbol)
        if sig and sig.get("exchange"):
            return {"has_signal": True, **sig}
    except Exception as e:
        logger.debug("cex_listing_probe_failed", token=str(token)[:42], error=str(e)[:90])
    return {"has_signal": False, "detail": "no listing feed wired (stub)", "source": None}


# --------------------------------------------------------------------------
# Public: combined catalyst verdict
# --------------------------------------------------------------------------

def catalyst_for(token: str, chain: str, symbol: str | None = None) -> dict:
    """Is there a near-term dump/pump CATALYST for this token, and when?

    Combines the live token-unlock layer with the (stubbed) CEX-listing layer.
    Returns (never raises):
        {
          "has_catalyst": bool,
          "kinds": ["unlock", "listing", ...],   # which catalysts fired
          "detail": "human-readable summary",
          "window": "~Nd" | "" ,                 # soonest catalyst horizon
          "unlocks": [ ... ],                    # raw near-term unlock events
          "listing": { ... },                    # listing signal blob
        }
    """
    result: dict[str, Any] = {
        "has_catalyst": False,
        "kinds": [],
        "detail": "",
        "window": "",
        "unlocks": [],
        "listing": {"has_signal": False},
    }
    try:
        unlocks = upcoming_unlocks(token, chain=chain, symbol=symbol)
        # Keep only material unlocks (skip dust below threshold when we can size it).
        material = [
            u for u in unlocks
            if u.get("pct_of_max_supply") is None or u["pct_of_max_supply"] >= _MIN_UNLOCK_PCT
        ]
        result["unlocks"] = unlocks
        details: list[str] = []
        soonest_days: int | None = None

        if material:
            result["kinds"].append("unlock")
            nxt = material[0]
            soonest_days = nxt["days_until"]
            pct = nxt.get("pct_of_max_supply")
            pct_str = f" ({pct}% of max supply)" if pct is not None else ""
            usd = nxt.get("usd")
            usd_str = f" ~${usd:,.0f}" if usd else ""
            details.append(
                f"unlock {nxt['date']} (in {nxt['days_until']}d): "
                f"{nxt.get('category') or 'tokens'}{pct_str}{usd_str} — DUMP risk"
            )

        listing = cex_listing_signal(token, chain=chain, symbol=symbol)
        result["listing"] = listing
        if listing.get("has_signal"):
            result["kinds"].append("listing")
            details.append(
                f"{listing.get('exchange', 'CEX')} listing — {listing.get('detail', '')}".strip()
            )
            ld = listing.get("window")
            if ld:
                details.append(f"listing window {ld}")

        result["has_catalyst"] = bool(result["kinds"])
        result["detail"] = "; ".join(details) if details else "no near-term catalyst"
        if soonest_days is not None:
            result["window"] = f"~{soonest_days}d"
        return result
    except Exception as e:
        logger.debug("catalyst_for_failed", token=str(token)[:42], error=str(e)[:90])
        result["detail"] = "catalyst lookup failed"
        return result


__all__ = ["catalyst_for", "upcoming_unlocks", "cex_listing_signal"]
