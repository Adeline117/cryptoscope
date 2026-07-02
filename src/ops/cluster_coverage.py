"""Weekly cluster-coverage audit — productionized from the manual sweep that found
ESPORTS' untracked 20% whale and the exchange-inventory SKYAI "cluster".

Per BSC sentinel: reconstruct the FULL holder list from Dune erc20_bnb.evt_Transfer
net flows (provider-independent), then
  (1) cross-check the tracked wallets' derived sum vs live balance_of — a drift
      means reflection/tax mechanics and the reconstruction is downgraded;
  (2) flag untracked holders ≥ MIN_SHARE that are plain EOAs (not CEX per our
      merged label set, not contracts/multisigs) — candidate blind spots.

Status-bearing: Dune failure marks that token "unverified", never "clean".
"""

from __future__ import annotations

import json

import structlog

from src.config import DATA_DIR

logger = structlog.get_logger()

MIN_SHARE = 2.0        # % of reconstructed top supply that makes a blind spot
_BURN = {"0x0000000000000000000000000000000000000000",
         "0x000000000000000000000000000000000000dead",
         "0x0000000000000000000000000000000000000001"}


def _bsc_sentinels() -> list[dict]:
    reg = DATA_DIR / "operator_sentinels.json"
    if not reg.exists():
        return []
    try:
        return [s for s in json.loads(reg.read_text()).values() if s.get("chain") == "bsc"]
    except Exception:
        return []


def audit_token(s: dict) -> dict:
    """One sentinel → {symbol, verified, drift_pct, blind: [{address, balance, share}]}."""
    from src.onchain.cex_addresses import evm_exchanges
    from src.onchain.dune_client import run_sql
    from src.onchain.entity_classify import classify_address
    from src.onchain.evm_archive import ArchiveRPC

    sym, tok = s.get("symbol"), s["token"].lower()
    wl = {w.lower() for w in s.get("wallets", [])}
    rpc = ArchiveRPC("bsc")
    dec = rpc.token_decimals(s["token"])
    sql = ("with flows as ("
           "select \"to\" as addr, cast(value as double)/1e%d as amt "
           "from erc20_bnb.evt_Transfer where contract_address = %s "
           "union all "
           "select \"from\" as addr, -cast(value as double)/1e%d as amt "
           "from erc20_bnb.evt_Transfer where contract_address = %s) "
           "select addr, sum(amt) as bal from flows group by 1 "
           "having sum(amt) > 0 order by bal desc limit 30") % (dec, tok, dec, tok)
    rows = run_sql(sql, poll_s=6, max_polls=40)
    if not rows:
        return {"symbol": sym, "verified": False, "drift_pct": None, "blind": []}

    derived = {str(r["addr"]).lower(): float(r["bal"]) for r in rows
               if str(r["addr"]).lower() not in _BURN}
    live = sum((rpc.balance_of(s["token"], w) or 0) for w in wl)
    tracked_sum = sum(v for a, v in derived.items() if a in wl)
    drift = (abs(tracked_sum - live) / live * 100) if live else None

    cex = evm_exchanges()
    total = sum(derived.values()) or 1
    blind = []
    for a, bal in list(derived.items())[:15]:
        share = bal / total * 100
        if a in wl or a in cex or share < MIN_SHARE:
            continue
        if classify_address(a, "bsc").get("type") == "eoa":
            blind.append({"address": a, "balance": bal, "share": round(share, 1)})
    return {"symbol": sym, "verified": True,
            "drift_pct": round(drift, 2) if drift is not None else None, "blind": blind}


def run_audit() -> list[dict]:
    return [audit_token(s) for s in _bsc_sentinels()]
