"""Entity-label verification sweep — the institutionalized version of the audit
that unmasked Gate.io/Binance/MEXC wallets masquerading as operator entities.

Every address the system TRUSTS as an operator entity (sentinel cluster wallets,
watched funder roots, operator-funder allowlists) is checked against
(a) our local CEX list and (b) Dune labels.owner_addresses. Any hit = the entity
model is contaminated (a cluster wallet that is really an exchange hot wallet
poisons balances, net-flows, and every conclusion built on them).

Status-bearing: a failed Dune sweep returns complete=False — the caller must NOT
report "clean" off a failed check (failure ≠ no contamination).
"""

from __future__ import annotations

import json

import structlog

from src.config import DATA_DIR

logger = structlog.get_logger()

# Labels that mean "this is infrastructure, not an operator wallet".
_BAD_HINTS = ("gate", "binance", "mexc", "okx", "kucoin", "bybit", "htx", "huobi",
              "kraken", "coinbase", "bitget", "crypto_com", "bitfinex", "upbit",
              "bithumb", "exchange", "bridge", "router", "aggregator", "mixer",
              "tornado", "stargate")


def gather_trusted_addresses() -> dict[str, str]:
    """address_lower -> where we trust it (for the alert message). EVM only —
    Dune labels cover bnb; Solana addresses are skipped here."""
    out: dict[str, str] = {}
    reg = DATA_DIR / "operator_sentinels.json"
    if reg.exists():
        try:
            for s in json.loads(reg.read_text()).values():
                if s.get("chain") in ("solana", "sol"):
                    continue
                for w in s.get("wallets", []):
                    out[w.lower()] = f"{s.get('symbol')}哨兵簇"
        except Exception:
            pass
    try:
        from src.onchain.funder_watch import WATCHED_FUNDERS
        for a, label in WATCHED_FUNDERS.items():
            out[a.lower()] = f"funder_watch根({label})"
    except Exception:
        pass
    try:
        from src.pipeline.anomaly_screener import KNOWN_OPERATOR_FUNDERS
        for a in KNOWN_OPERATOR_FUNDERS:
            out[a.lower()] = "KNOWN_OPERATOR_FUNDERS白名单"
    except Exception:
        pass
    return out


def sweep(chain: str = "bnb") -> dict:
    """Check all trusted addresses for exchange/bridge/infra labels.
    Returns {complete, checked, hits: [{address, role, label, source}]}."""
    trusted = gather_trusted_addresses()
    if not trusted:
        return {"complete": True, "checked": 0, "hits": [],
                "dune_state": "not_needed", "dune_error_kind": None}
    hits: list[dict] = []

    # (a) local CEX list — instant, catches anything we already labeled.
    from src.onchain.cex_addresses import evm_exchanges
    local = evm_exchanges()
    for a, role in trusted.items():
        if a in local:
            hits.append({"address": a, "role": role, "label": local[a], "source": "local"})

    # (b) Dune labels — the authoritative check (this is what unmasked 0x0d0707).
    from src.onchain.dune_client import available, run_sql_result
    dune_ok = False
    dune_state = "failed"
    dune_error_kind = "not_configured"
    if available():
        addrs = ", ".join(trusted)
        dune_result = run_sql_result(
            f"select address, owner_key, custody_owner from labels.owner_addresses "
            f"where blockchain = '{chain}' and address in ({addrs})")
        rows = dune_result["rows"]
        dune_state = dune_result["state"]
        dune_error_kind = dune_result["error_kind"]
        # A status-bearing valid empty result is proof of a clean Dune lookup.
        # Never spend credits on a sentinel query after 402/auth/rate/network failure.
        dune_ok = dune_state == "ok"
        seen = {h["address"] for h in hits}
        for r in rows:
            a = str(r.get("address", "")).lower()
            key = (str(r.get("owner_key") or "") + " " + str(r.get("custody_owner") or "")).lower()
            if a in trusted and a not in seen and any(h in key for h in _BAD_HINTS):
                hits.append({"address": a, "role": trusted[a],
                             "label": r.get("custody_owner") or r.get("owner_key"),
                             "source": "dune"})
    complete = dune_ok
    if not complete:
        logger.warning(
            "label_verify_incomplete", dune_state=dune_state,
            dune_error_kind=dune_error_kind,
            note="Dune sweep failed — do NOT report clean")
    return {"complete": complete, "checked": len(trusted), "hits": hits,
            "dune_state": dune_state, "dune_error_kind": dune_error_kind}
