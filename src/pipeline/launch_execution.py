"""Fail-closed security and round-trip quote gate for Launch candidates.

Liquidity is not sellability. Before a raw Launch candidate may remain
``SMALL_PROBE``, this module requires:

1. a current GoPlus contract/SPL safety read with no hard or unresolved risk; and
2. a read-only buy-then-sell router quote for the exact proposed notional.

No transaction is built, signed, or submitted. A quote is still not a real fill, so
the result records exclusions explicitly. Missing keys, unsupported chains, API
failures, and incomplete fields are UNKNOWN and downgrade the candidate to WATCH.
"""
from __future__ import annotations

import json
import math
import os
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Callable

JUPITER_ORDER = "https://api.jup.ag/swap/v2/order"
# Jupiter postponed the keyless Lite API retirement in February 2026. It remains a
# deliberately labelled fallback for quote-only validation when no portal key is
# configured; the keyed V2 order endpoint stays preferred.
JUPITER_LITE_QUOTE = "https://lite-api.jup.ag/swap/v1/quote"
JUPITER_PRICE = "https://api.jup.ag/price/v3"
JUPITER_LITE_PRICE = "https://lite-api.jup.ag/price/v3"
JUPITER_USDC = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
ZEROX_PRICE = "https://api.0x.org/swap/allowance-holder/price"
MAX_ROUNDTRIP_LOSS_PCT = 5.0
QUOTE_TTL_SECONDS = 60

# 0x's indicative route is retained as evidence but not promoted to executable:
# network gas is not yet converted to USD. BSC is deliberately omitted because its
# commonly used Binance-Peg USDC has 18 decimals and a different trust/cost profile.
_EVM_USDC = {
    "ethereum": (1, "0xA0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", 6),
    "base": (8453, "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913", 6),
}

Fetch = Callable[[str, dict, dict | None], dict]

_EVM_REQUIRED_BINARY_FLAGS = (
    "is_honeypot", "is_mintable", "transfer_pausable",
    "owner_change_balance", "hidden_owner", "can_take_back_ownership",
    "is_blacklisted", "trading_cooldown", "cannot_sell_all", "is_proxy",
)
_EVM_REQUIRED_TAX_FLAGS = ("buy_tax", "sell_tax")


def _get_json(url: str, params: dict, headers: dict | None = None) -> dict:
    query = urllib.parse.urlencode(params)
    req = urllib.request.Request(f"{url}?{query}", headers={
        "User-Agent": "CryptoScope/LaunchExecution/1.0", **(headers or {})})
    with urllib.request.urlopen(req, timeout=12) as response:
        return json.loads(response.read().decode())


def _flag(value) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _status(row: dict, key: str) -> int | None:
    value = row.get(key)
    return _flag(value.get("status")) if isinstance(value, dict) else _flag(value)


def _solana_security(token: str, fetch: Fetch) -> dict:
    try:
        data = fetch("https://api.gopluslabs.io/api/v1/solana/token_security",
                     {"contract_addresses": token}, None)
    except Exception as exc:
        return {"state": "unknown", "source": "GoPlus Solana",
                "reason": f"security fetch failed: {str(exc)[:60]}"}
    rows = data.get("result") or {}
    row = rows.get(token) or rows.get(token.lower())
    if data.get("code") != 1 or not isinstance(row, dict):
        return {"state": "unknown", "source": "GoPlus Solana",
                "reason": "token not indexed or malformed response"}

    required = ("mintable", "freezable", "balance_mutable_authority", "closable",
                "non_transferable")
    vals = {key: _status(row, key) for key in required}
    missing = [key for key, value in vals.items() if value is None]
    if missing:
        return {"state": "unknown", "source": "GoPlus Solana",
                "reason": "missing required fields", "unknown_fields": missing}

    hard = [key for key, value in vals.items() if value == 1]
    # Token-2022 transfer hooks/fees and mutable defaults can change what a holder
    # receives or whether it can transfer. They are not automatically scams, but an
    # early right-tail probe must not treat them as ordinary SPL behavior.
    extensions = []
    if row.get("transfer_hook"):
        extensions.append("transfer_hook")
    if row.get("transfer_fee"):
        extensions.append("transfer_fee")
    for key in ("transfer_hook_upgradable", "transfer_fee_upgradable",
                "default_account_state_upgradable"):
        if _status(row, key) == 1:
            extensions.append(key)
    state = "avoid" if hard else "caution" if extensions else "pass"
    return {"state": state, "source": "GoPlus Solana", "hard_flags": hard,
            "cautions": extensions, "checked_at": datetime.now(timezone.utc).isoformat()}


def _evm_security(token: str, chain: str) -> dict:
    from src.onchain.goplus_client import rug_risk

    rr = rug_risk(token, chain)
    if not rr.get("available"):
        return {"state": "unknown", "source": "GoPlus EVM",
                "reason": rr.get("reason") or "security unavailable"}
    if rr.get("is_open_source") != 1:
        return {"state": "unknown", "source": "GoPlus EVM",
                "reason": "contract source/flags not verifiable",
                "checked_at": rr.get("checked_at")}
    flags = rr.get("flags") or {}
    unknown = [key for key in _EVM_REQUIRED_BINARY_FLAGS
               if flags.get(key) not in (0, 1)]
    taxes = {}
    for key in _EVM_REQUIRED_TAX_FLAGS:
        try:
            value = float(flags[key])
            if not math.isfinite(value) or value < 0:
                raise ValueError
            taxes[key] = value
        except (KeyError, TypeError, ValueError):
            unknown.append(key)
    if unknown:
        return {"state": "unknown", "source": "GoPlus EVM",
                "reason": "missing or invalid required fields",
                "unknown_fields": unknown, "checked_at": rr.get("checked_at")}

    hard = []
    for key in ("is_honeypot", "owner_change_balance", "transfer_pausable",
                "hidden_owner", "can_take_back_ownership", "is_blacklisted",
                "trading_cooldown", "cannot_sell_all"):
        if flags.get(key) == 1:
            hard.append(key)
    if flags.get("is_mintable") == 1 and rr.get("owner_renounced") is not True:
        hard.append("mintable_with_live_or_unknown_owner")
    if taxes["sell_tax"] >= 0.05:
        hard.append("sell_tax_gte_5pct")
    cautions = []
    if flags.get("is_proxy") == 1:
        cautions.append("upgradeable_proxy")
    if rr.get("lp_all_locked") is not True:
        cautions.append("lp_lock_not_verified")
    state = "avoid" if hard else "caution" if cautions else "pass"
    return {"state": state, "source": "GoPlus EVM", "hard_flags": hard,
            "cautions": cautions, "facts": (rr.get("facts") or [])[:5],
            "checked_at": rr.get("checked_at")}


def security_probe(event: dict, fetch: Fetch = _get_json) -> dict:
    token, chain = event.get("token"), event.get("chain")
    if not token or not chain:
        return {"state": "unknown", "reason": "missing token/chain"}
    if chain == "solana":
        return _solana_security(token, fetch)
    if chain in {"ethereum", "base", "bsc"}:
        return _evm_security(token, chain)
    return {"state": "unknown", "reason": f"security chain unsupported: {chain}"}


def _route_labels(quote: dict) -> list[str]:
    labels = []
    for leg in quote.get("routePlan") or []:
        label = (leg.get("swapInfo") or {}).get("label")
        if label and label not in labels:
            labels.append(str(label))
    for fill in (quote.get("route") or {}).get("fills") or []:
        label = fill.get("source")
        if label and label not in labels:
            labels.append(str(label))
    return labels[:6]


def _roundtrip(notional: float, back_usd: float) -> float:
    return round((notional - back_usd) / notional * 100, 4)


def _jupiter_route(event: dict, key: str, fetch: Fetch, *, endpoint: str = JUPITER_ORDER) -> dict:
    notional = float(event.get("max_notional_usd") or 0)
    token = event.get("token")
    if notional <= 0 or not token:
        return {"state": "unknown", "reason": "missing quote notional/token"}
    headers = {"x-api-key": key} if key else None
    source = ("Jupiter Swap v2 order" if endpoint == JUPITER_ORDER
              else "Jupiter Swap v1 lite quote (keyless fallback)")
    common = {"slippageBps": 100}
    try:
        buy = fetch(endpoint, {**common, "inputMint": JUPITER_USDC,
                    "outputMint": token, "amount": str(round(notional * 1_000_000))}, headers)
    except Exception as exc:
        return {"state": "unknown", "source": source,
                "reason": f"buy quote unavailable: {str(exc)[:70]}", "read_only": True}
    # Use the minimum accepted output, not the optimistic headline outAmount. This
    # freezes the configured 1% slippage tolerance into the paper cost estimate.
    token_out = int(buy.get("otherAmountThreshold") or buy.get("outAmount") or 0)
    if token_out <= 0 or not buy.get("routePlan"):
        return {"state": "untradeable", "source": source,
                "reason": "no buy route", "read_only": True}
    decimals = None
    price_metadata = {}
    try:
        price_endpoint = JUPITER_PRICE if endpoint == JUPITER_ORDER else JUPITER_LITE_PRICE
        metadata = fetch(price_endpoint, {"ids": token}, headers)
        price_metadata = metadata.get(token) if isinstance(metadata, dict) else None
        if isinstance(price_metadata, dict):
            decimals = int(price_metadata.get("decimals"))
            if not 0 <= decimals <= 18:
                decimals = None
    except (KeyError, TypeError, ValueError, OSError):
        decimals = None
    try:
        sell = fetch(endpoint, {**common, "inputMint": token,
                     "outputMint": JUPITER_USDC, "amount": str(token_out)}, headers)
    except Exception as exc:
        return {"state": "unknown", "source": source,
                "reason": f"sell quote unavailable: {str(exc)[:70]}", "read_only": True}
    back_raw = int(sell.get("otherAmountThreshold") or sell.get("outAmount") or 0)
    if back_raw <= 0 or not sell.get("routePlan"):
        return {"state": "untradeable", "source": source,
                "reason": "no sell route", "read_only": True}
    back_usd = back_raw / 1_000_000
    loss = _roundtrip(notional, back_usd)
    token_units = token_out / (10 ** decimals) if decimals is not None else None
    entry_reference = (notional / token_units if token_units and token_units > 0 else None)
    return {"state": "quoted" if loss <= MAX_ROUNDTRIP_LOSS_PCT else "untradeable",
            "source": source, "read_only": True,
            "api_mode": "keyed_v2" if endpoint == JUPITER_ORDER else "keyless_lite_fallback",
            "roundtrip_loss_pct": loss, "notional_usd": notional,
            "roundtrip_back_usd": round(back_usd, 6),
            "token_decimals": decimals,
            "entry_reference_price": round(entry_reference, 12)
            if entry_reference is not None else None,
            "invalidation_reference_price": round(entry_reference * 0.70, 12)
            if entry_reference is not None else None,
            "market_reference_price_usd": (price_metadata or {}).get("usdPrice")
            if isinstance(price_metadata, dict) else None,
            "price_reference_block": (price_metadata or {}).get("blockId")
            if isinstance(price_metadata, dict) else None,
            "price_reference_source": "Jupiter Price v3 decimals + worst-threshold route",
            "price_reference_reason": None if entry_reference is not None
            else "token decimals unavailable; route price cannot be standardized",
            "buy_price_impact_pct": abs(float(buy.get("priceImpact") or 0)) * 100,
            "sell_price_impact_pct": abs(float(sell.get("priceImpact") or 0)) * 100,
            "buy_routes": _route_labels(buy), "sell_routes": _route_labels(sell),
            "network_fees_included": False, "is_real_fill": False,
            "checked_at": datetime.now(timezone.utc).isoformat()}


def _zerox_route(event: dict, key: str, fetch: Fetch) -> dict:
    chain = event.get("chain")
    config = _EVM_USDC.get(chain)
    if not config:
        return {"state": "unknown", "reason": f"0x replay unsupported on {chain}"}
    chain_id, usdc, decimals = config
    notional, token = float(event.get("max_notional_usd") or 0), event.get("token")
    headers = {"0x-api-key": key, "0x-version": "v2"}
    try:
        buy = fetch(ZEROX_PRICE, {"chainId": chain_id, "sellToken": usdc,
                    "buyToken": token, "sellAmount": str(round(notional * 10 ** decimals))}, headers)
        token_out = int(buy.get("buyAmount") or 0)
        if token_out <= 0 or buy.get("liquidityAvailable") is False:
            raise ValueError("no buy liquidity")
        sell = fetch(ZEROX_PRICE, {"chainId": chain_id, "sellToken": token,
                     "buyToken": usdc, "sellAmount": str(token_out)}, headers)
        back_raw = int(sell.get("buyAmount") or 0)
        if back_raw <= 0 or sell.get("liquidityAvailable") is False:
            raise ValueError("no sell liquidity")
    except Exception as exc:
        return {"state": "untradeable", "source": "0x indicative price v2",
                "reason": str(exc)[:80], "read_only": True}
    back_usd = back_raw / 10 ** decimals
    return {"state": "indicative", "source": "0x indicative price v2", "read_only": True,
            "roundtrip_loss_pct": _roundtrip(notional, back_usd),
            "notional_usd": notional, "roundtrip_back_usd": round(back_usd, 6),
            "buy_routes": _route_labels(buy), "sell_routes": _route_labels(sell),
            "network_fees_included": False, "is_real_fill": False,
            "reason": "route exists but gas cost is not converted to USD",
            "checked_at": datetime.now(timezone.utc).isoformat()}


def route_probe(event: dict, fetch: Fetch = _get_json) -> dict:
    chain = event.get("chain")
    if chain == "solana":
        key = os.getenv("JUPITER_API_KEY", "")
        if key:
            return _jupiter_route(event, key, fetch)
        return _jupiter_route(event, "", fetch, endpoint=JUPITER_LITE_QUOTE)
    if chain in _EVM_USDC:
        key = os.getenv("ZEROX_API_KEY", "")
        if not key:
            return {"state": "unknown", "source": "0x indicative price v2",
                    "reason": "ZEROX_API_KEY not configured", "read_only": True}
        return _zerox_route(event, key, fetch)
    return {"state": "unknown", "reason": f"round-trip replay unsupported on {chain}",
            "read_only": True}


def gate(event: dict, security: dict, execution: dict,
         *, now: datetime | None = None) -> dict:
    """Attach evidence and fail closed. Mutates and returns ``event``."""
    now = now or datetime.now(timezone.utc)
    event["security_gate"] = security
    event["execution_probe"] = execution
    event["decision_at"] = now.isoformat()
    event["executable_at"] = None
    checked_at = execution.get("checked_at")
    quote_at = None
    if checked_at:
        try:
            quote_at = datetime.fromisoformat(str(checked_at).replace("Z", "+00:00"))
            if quote_at.tzinfo is None:
                quote_at = None
            else:
                quote_at = quote_at.astimezone(timezone.utc)
        except (TypeError, ValueError):
            quote_at = None
    event["quote_at"] = quote_at.isoformat() if quote_at else None
    event["expires_at"] = None
    if event.get("decision") != "SMALL_PROBE":
        return event
    sec_state, route_state = security.get("state"), execution.get("state")
    if sec_state == "avoid" or route_state == "untradeable":
        event["decision"] = "AVOID"
    elif sec_state != "pass" or route_state != "quoted":
        event["decision"] = "WATCH"
    else:
        loss = float(execution.get("roundtrip_loss_pct") or 0)
        event["roundtrip_cost_pct_est"] = max(0.0, loss)
        event["cost_model"] = "live_read_only_roundtrip_quote_excluding_network_fees"
        # A route quote proves momentary route availability, not a fill. Keep the
        # executable clock empty and give the recommendation a deliberately short
        # lifetime so the board cannot display an old route as a current entry.
        if quote_at is None:
            event["decision"] = "WATCH"
        else:
            event["expires_at"] = (
                quote_at + timedelta(seconds=QUOTE_TTL_SECONDS)
            ).isoformat()
    if event["decision"] != "SMALL_PROBE":
        event.setdefault("reasons", []).append(
            f"执行门降级: security={sec_state or 'unknown'}, route={route_state or 'unknown'}")
    return event


def assess(event: dict, fetch: Fetch = _get_json) -> dict:
    security = security_probe(event, fetch=fetch)
    # Do not spend two router calls after a failed safety gate.
    execution = (route_probe(event, fetch=fetch) if security.get("state") == "pass"
                 else {"state": "skipped", "reason": "security gate did not pass",
                       "read_only": True})
    return gate(event, security, execution)
