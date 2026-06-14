"""Historical backfill — reconstruct past accumulation cycles from on-chain data.

Forward data collection is slow because accumulation→launch cycles run for weeks
to months. But on-chain transfer history is permanent: we can reconstruct a
token's holder distribution AS OF any past block and replay the whole
accumulation series in minutes. That turns "wait 2-3 months" into "run it now".

How it works (EVM, Alchemy-supported chains — ETH/Base/Arb/Opt/Polygon):
  1. Pull the FULL ERC-20 transfer history once (with block numbers).
  2. For N checkpoints across the token's life, sum only the transfers up to that
     block → holder balances as of that block.
  3. Compute effective vs nominal concentration at each checkpoint → the gap and
     effective series the live signal would have seen.
  4. Pair with the realized max return (price source) → a backtest sample.

BNB and Solana are NOT reconstructable on the free keys (Alchemy 403 / Etherscan
free-tier gated / Solana has no cheap historical holder API), so targets must be
on the supported EVM chains. This is an honest coverage limit, logged per token.
"""

from __future__ import annotations

import json
import urllib.request

import structlog

from src.onchain.entity_clustering import effective_concentration

logger = structlog.get_logger()

ALCHEMY_NET = {
    "ethereum": "eth-mainnet", "eth": "eth-mainnet", "base": "base-mainnet",
    "arbitrum": "arb-mainnet", "optimism": "opt-mainnet", "polygon": "polygon-mainnet",
}
RECONSTRUCTABLE = set(ALCHEMY_NET)


def _alchemy_all_transfers(token: str, net: str, key: str, max_pages: int = 40,
                           timeout: int = 30) -> list[dict]:
    """Fetch the full ERC-20 transfer history (asc) with block numbers."""
    url = f"https://{net}.g.alchemy.com/v2/{key}"
    out: list[dict] = []
    page_key = None
    for _ in range(max_pages):
        params = {
            "fromBlock": "0x0", "toBlock": "latest", "contractAddresses": [token],
            "category": ["erc20"], "withMetadata": False, "maxCount": "0x3e8",
            "order": "asc",
        }
        if page_key:
            params["pageKey"] = page_key
        payload = json.dumps({
            "jsonrpc": "2.0", "id": 1, "method": "alchemy_getAssetTransfers",
            "params": [params],
        })
        req = urllib.request.Request(url, data=payload.encode(),
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode())
        result = data.get("result", {})
        for t in result.get("transfers", []):
            try:
                blk = int(t.get("blockNum", "0x0"), 16)
                val = float(t.get("value") or 0)
            except (ValueError, TypeError):
                continue
            out.append({"block": blk, "from": (t.get("from") or "").lower(),
                        "to": (t.get("to") or "").lower(), "value": val})
        page_key = result.get("pageKey")
        if not page_key:
            break
    return out


def _balances_as_of(transfers: list[dict], block: int) -> list[dict]:
    """Net balances using only transfers up to `block`."""
    bal: dict[str, float] = {}
    zero = "0x0000000000000000000000000000000000000000"
    for t in transfers:
        if t["block"] > block:
            break  # transfers are ascending
        if t["from"]:
            bal[t["from"]] = bal.get(t["from"], 0.0) - t["value"]
        if t["to"]:
            bal[t["to"]] = bal.get(t["to"], 0.0) + t["value"]
    return [{"address": a, "balance": b} for a, b in bal.items()
            if b > 1e-9 and a != zero]


def reconstruct_series(token: str, chain: str, n_points: int = 8,
                       max_pages: int = 40, observe_fraction: float = 1.0) -> dict | None:
    """Reconstruct the effective/nominal concentration series over a token's life.

    `observe_fraction` < 1 reconstructs only over the first fraction of the
    token's block-life (the accumulation *observation window*), so the features
    precede most of the price outcome — reducing look-ahead in the backtest.

    Returns {gap_series, effective_series, n_transfers, blocks} or None if the
    chain is unsupported or there is too little history.
    """
    import os

    if chain not in RECONSTRUCTABLE:
        logger.info("historical_chain_unsupported", token=token, chain=chain)
        return None
    key = os.environ.get("ALCHEMY_API_KEY", "")
    if not key:
        return None

    transfers = _alchemy_all_transfers(token, ALCHEMY_NET[chain], key, max_pages)
    if len(transfers) < 20:
        logger.info("historical_too_few_transfers", token=token, n=len(transfers))
        return None

    transfers.sort(key=lambda t: t["block"])
    first_blk, full_last = transfers[0]["block"], transfers[-1]["block"]
    last_blk = int(first_blk + (full_last - first_blk) * max(0.1, min(1.0, observe_fraction)))
    if last_blk <= first_blk:
        return None

    # N checkpoints evenly spaced across the observation window.
    step = (last_blk - first_blk) / n_points
    checkpoints = [int(first_blk + step * i) for i in range(1, n_points + 1)]

    gap_series, eff_series = [], []
    for blk in checkpoints:
        holders = _balances_as_of(transfers, blk)
        # Clustering uses similar-balance + exclusion (funder lookups are skipped
        # here for cost; similar-balance still catches split positions).
        m = effective_concentration(holders, top_n=10)
        eff_series.append(m["effective_top_n_pct"])
        gap_series.append(m["concentration_gap"])

    return {
        "gap_series": gap_series, "effective_series": eff_series,
        "n_transfers": len(transfers), "blocks": checkpoints,
    }


def get_max_return(token: str, chain: str, timeout: int = 15) -> float | None:
    """Realized max return = ATH / earliest price, from GeckoTerminal OHLCV.

    Uses the most-liquid pool's daily candles (up to ~1000 days). Returns a
    multiple (5.0 = 5x) or None if unavailable.
    """
    gt_net = {"ethereum": "eth", "eth": "eth", "base": "base", "arbitrum": "arbitrum",
              "optimism": "optimism", "polygon": "polygon_pos"}.get(chain, chain)
    try:
        # Find the top pool for the token.
        url = f"https://api.geckoterminal.com/api/v2/networks/{gt_net}/tokens/{token}/pools"
        req = urllib.request.Request(url, headers={"User-Agent": "CryptoScope/1.0",
                                                   "Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            pools = json.loads(resp.read().decode()).get("data", [])
        if not pools:
            return None
        pool_addr = pools[0]["attributes"]["address"]

        ohlc_url = (f"https://api.geckoterminal.com/api/v2/networks/{gt_net}/pools/"
                    f"{pool_addr}/ohlcv/day?limit=1000")
        req = urllib.request.Request(ohlc_url, headers={"User-Agent": "CryptoScope/1.0",
                                                        "Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            ohlcv = json.loads(resp.read().decode())
        candles = ohlcv.get("data", {}).get("attributes", {}).get("ohlcv_list", [])
        if not candles:
            return None
        # candle = [ts, open, high, low, close, volume]; oldest last in GT.
        highs = [float(c[2]) for c in candles if c[2]]
        opens = [float(c[1]) for c in candles if c[1]]
        if not highs or not opens:
            return None
        ath = max(highs)
        early = opens[-1] or min(o for o in opens if o > 0)
        return round(ath / early, 4) if early > 0 else None
    except Exception as e:
        logger.debug("max_return_failed", token=token, error=str(e))
        return None


def build_historical_sample(token: str, chain: str, symbol: str = "",
                            n_points: int = 8, observe_fraction: float = 0.6) -> dict | None:
    """Assemble one backtest sample (features + outcome) for a token.

    Features come from the first `observe_fraction` of the token's life; the
    outcome (max return) spans the full price history.
    """
    # Cap pages: the first ~15k transfers cover a token's early/accumulation life,
    # which is what the signal needs, and keeps the offline run tractable.
    series = reconstruct_series(token, chain, n_points, max_pages=15,
                                observe_fraction=observe_fraction)
    if not series:
        return None
    mret = get_max_return(token, chain)
    if mret is None:
        logger.info("historical_no_price", token=token)
        return None
    return {
        "token": token, "symbol": symbol, "chain": chain,
        "timestamp": "2020-01-01",  # historical samples; cutoff handled by caller
        "features": {
            "gap_series": series["gap_series"],
            "effective_series": series["effective_series"],
            "float_active": max(0.0, min(1.0, 1 - (series["effective_series"][-1] / 100)
                                         if series["effective_series"] else 0)),
            "security_passed": True,
        },
        "max_return": mret,
        "n_transfers": series["n_transfers"],
    }
