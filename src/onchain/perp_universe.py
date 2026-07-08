"""Perpetual-futures universe — the SHORTABLE (and leverage-longable) coin set,
mapped to on-chain contracts so the operator signals can run on them.

The strategy pivot (2026-07): stop pointing the detectors at thin BSC micro-caps
you can't trade, and point them at coins that HAVE a perp market (long or short
with leverage). Feasibility-verified: OKX has ~400 USDT perps; ~68% map to an
ERC20/SPL contract on a chain we can read (ETH/SOL/BSC/Base/L2s). The rest are
native L1s (APT, BERA…) and tokenized stocks (AAPL…) — no holder signal, excluded.

Source of truth for the perp list: OKX public API (reachable; Binance fapi is
geo-blocked 451 here, Bybit 403). Contract mapping: CoinGecko coins/list with
platforms. Cached to data/perp_universe.json; refresh is cheap and slow-changing.
"""

from __future__ import annotations

import json
import urllib.request
from collections import defaultdict

import structlog

from src.config import DATA_DIR

logger = structlog.get_logger()

_CACHE = DATA_DIR / "perp_universe.json"
# CoinGecko platform id -> our internal chain id (chains we can read holders/logs on).
_CHAIN_MAP = {
    "ethereum": "ethereum",
    "binance-smart-chain": "bsc",
    "solana": "solana",
    "base": "base",
    "arbitrum-one": "arbitrum",
    "optimistic-ethereum": "optimism",
    "polygon-pos": "polygon",
    "avalanche": "avalanche",
}


def _get(url: str, timeout: int = 20):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode())


def _okx_perp_bases() -> set[str]:
    """USDT-settled perpetual base assets on OKX."""
    try:
        data = _get("https://www.okx.com/api/v5/public/instruments?instType=SWAP")
    except Exception as e:
        logger.warning("okx_perps_failed", error=str(e)[:80])
        return set()
    out = set()
    for i in data.get("data", []):
        if i.get("settleCcy") != "USDT":
            continue
        base = i.get("ctValCcy") or i.get("instId", "").split("-")[0]
        if base:
            out.add(base.upper())
    return out


def _platform_to_hit(plats: dict) -> dict | None:
    for cg_chain, addr in (plats or {}).items():
        chain = _CHAIN_MAP.get(cg_chain)
        if chain and addr:
            return {"chain": chain, "address": addr.lower() if chain != "solana" else addr}
    return None


def refresh() -> dict:
    """Rebuild the perp→contract map and cache it. Returns {symbol: {chain, address}}.

    Ticker collisions are resolved by MARKET CAP: many symbols map to several
    CoinGecko ids (a real coin + obscure namesakes + tokenized stocks), and picking
    the wrong one = watching the wrong contract = garbage signals. We rank ids by
    mcap (coins/markets) and take the highest-cap id per ticker whose platforms
    resolve to a chain we read. Never wipes a good cache with an empty pull."""
    bases = _okx_perp_bases()
    if not bases:
        logger.warning("perp_refresh_no_bases", note="kept previous cache")
        return load()
    try:
        cg = _get("https://api.coingecko.com/api/v3/coins/list?include_platform=true")
        id_to_plats = {c["id"]: (c.get("platforms") or {}) for c in cg}
        # mcap-ranked id list per symbol (top ~1000 covers every perp-listed coin).
        ranked: list[dict] = []
        for pg in (1, 2, 3, 4):
            ranked += _get("https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd"
                           f"&order=market_cap_desc&per_page=250&page={pg}")
    except Exception as e:
        logger.warning("perp_refresh_cg_failed", error=str(e)[:80])
        return load()

    by_sym_ranked: dict[str, list[str]] = defaultdict(list)   # symbol -> [id...] mcap desc
    for c in ranked:
        by_sym_ranked[c["symbol"].upper()].append(c["id"])

    out: dict[str, dict] = {}
    for base in bases:
        for cid in by_sym_ranked.get(base, []):          # highest mcap first
            hit = _platform_to_hit(id_to_plats.get(cid, {}))
            if hit:
                out[base] = hit
                break
    if out:
        _CACHE.write_text(json.dumps(out, ensure_ascii=False))
        logger.info("perp_universe_refreshed", perps=len(bases), mapped=len(out))
    return out


def load() -> dict:
    """Cached perp→contract map ({symbol: {chain, address}}); {} if never built."""
    try:
        return json.loads(_CACHE.read_text())
    except Exception:
        return {}


def by_chain() -> dict[str, list[dict]]:
    """Grouped for batched scanning: {chain: [{symbol, address}, ...]}."""
    out: dict[str, list[dict]] = defaultdict(list)
    for sym, rec in load().items():
        out[rec["chain"]].append({"symbol": sym, "address": rec["address"]})
    return dict(out)
