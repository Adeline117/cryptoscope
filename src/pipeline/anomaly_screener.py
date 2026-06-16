"""Anomaly screener — surface coins that LOOK like quiet accumulation.

The funnel entry the user wants: I auto-find candidates worth an Arkham look;
they copy the operator cluster; I confirm with the free holding curve.

Screening here is intentionally CHEAP and scalable — one DexScreener call per
token, no per-token reconstruction. It scores the "quiet accumulation footprint"
from market microstructure (the observable side of a whale quietly building):

  - sustained BUY pressure (buys > sells across 1h/6h)
  - price flat-to-slightly-up, NOT already pumping (stealth, not lift-off)
  - real but not frenzied volume relative to liquidity
  - established enough (not a brand-new launch / curve token)

High score = "someone may be quietly accumulating; worth pulling the operator
cluster from Arkham." It does NOT confirm a whale — that's the second step
(run_operator_curve on the cluster you copy back).
"""

from __future__ import annotations

import json
import urllib.request

import structlog

logger = structlog.get_logger()

# Per-signal weights. Each fired signal (a "reason") contributes its weight to the
# candidate score. Defaults are hand-set; the calibrator (calibrate_weights.py)
# overwrites config/screener_weights.json with data-driven weights once enough
# labeled outcomes exist — so scoring becomes empirical, not guessed.
DEFAULT_WEIGHTS: dict[str, float] = {
    # L1 market footprint
    "absorption": 40, "buy_pressure": 22, "obv_absorption": 18,
    "vol_compression": 12, "consistency": 20, "established": 13,
    # L2 on-chain enrichment
    "cex_outflow": 20, "holders_rising": 25,
    "smart_money_t1": 35, "smart_money_t2": 25, "smart_money_t3": 15,
    "whale_accumulation": 18, "liquidity_rising": 12,
    # L2 effective concentration — the linchpin (one hidden entity controls float)
    "effective_concentration": 45, "hidden_cluster": 30,
    # cross-run / macro
    "persistent": 20, "cex_reserves_draining": 10,
}
_WEIGHTS_CACHE: dict | None = None

# Hard floors — a candidate must be a real, tradeable book, not a micro-cap.
# Below these you can neither accumulate nor exit, and "concentration" is just a
# dead allocation table (e.g. Solana PRISM: $27k liq / $598k mc, top holder $124k,
# the rest team multisig + DeFi vaults). Tuned to keep real small-caps
# (PRISM-eth $295k/$1.34M) while cutting micro-caps.
MIN_LIQUIDITY_USD = 150_000
MIN_MARKETCAP_USD = 1_000_000


def load_weights() -> dict[str, float]:
    """Signal weights: code defaults overlaid with calibrated config (if present)."""
    global _WEIGHTS_CACHE
    if _WEIGHTS_CACHE is not None:
        return _WEIGHTS_CACHE
    weights = dict(DEFAULT_WEIGHTS)
    try:
        from src.config import CONFIG_DIR

        p = CONFIG_DIR / "screener_weights.json"
        if p.exists():
            weights.update({k: float(v) for k, v in json.loads(p.read_text()).items()})
            logger.info("screener_weights_loaded", source="calibrated")
    except Exception as e:
        logger.debug("weights_load_failed", error=str(e))
    _WEIGHTS_CACHE = weights
    return weights


def score_reasons(reasons: list[str]) -> int:
    """Candidate score = sum of weights of the fired signals."""
    w = load_weights()
    return int(round(sum(w.get(r, 0) for r in reasons)))


def _http_json(url: str):
    req = urllib.request.Request(
        url, headers={"User-Agent": "CryptoScope/1.0", "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read().decode())


def _dexscreener_pairs(query: str) -> list[dict]:
    url = f"https://api.dexscreener.com/latest/dex/search?q={query}"
    try:
        return _http_json(url).get("pairs", [])
    except Exception as e:
        logger.debug("dexscreener_failed", query=query, error=str(e))
        return []


# GeckoTerminal network slug -> DexScreener chainId. Only the free-data chains we
# can actually confirm operators on downstream.
_GT_CHAINS = {"eth": "ethereum", "bsc": "bsc", "solana": "solana", "base": "base"}


def _dexscreener_tokens(chain: str, addresses: list[str]) -> list[dict]:
    """Batch-fetch DexScreener pair data for token addresses (<=30 per call)."""
    pairs: list[dict] = []
    for i in range(0, len(addresses), 30):
        csv = ",".join(addresses[i:i + 30])
        try:
            d = _http_json(f"https://api.dexscreener.com/tokens/v1/{chain}/{csv}")
            pairs.extend(d if isinstance(d, list) else d.get("pairs", []))
        except Exception as e:
            logger.debug("dexscreener_batch_failed", chain=chain, error=str(e))
    return pairs


def _gt_base_addresses(path: str) -> list[str]:
    """Extract base-token addresses from a GeckoTerminal pools response."""
    out = []
    try:
        for p in _http_json(f"https://api.geckoterminal.com/api/v2/{path}").get("data", []):
            tid = (p.get("relationships", {}).get("base_token", {})
                   .get("data", {}) or {}).get("id", "")
            if "_" in tid:
                out.append(tid.split("_", 1)[1])
    except Exception as e:
        logger.debug("geckoterminal_failed", path=path, error=str(e))
    return out


def _universe_pairs() -> list[dict]:
    """Build the screening universe from ACTIVELY-TRADED pools, not vanity name
    search. Per free-data chain: trending pools (attention) + top-volume pools
    (real flow). This is the fix for the keyword universe surfacing dead zombies
    like CAT just because the name matched 'cat'. Returns DexScreener pairs."""
    pairs: list[dict] = []
    for net, chain in _GT_CHAINS.items():
        addrs: list[str] = []
        addrs += _gt_base_addresses(f"networks/{net}/trending_pools?page=1")
        addrs += _gt_base_addresses(f"networks/{net}/pools?page=1&sort=h24_volume_usd_desc")
        # dedupe within chain, preserve order
        seen, uniq = set(), []
        for a in addrs:
            al = a.lower()
            if al not in seen:
                seen.add(al); uniq.append(a)
        if uniq:
            pairs.extend(_dexscreener_tokens(chain, uniq))
    return pairs


def _ratio(d: dict, w: str) -> float:
    x = d.get(w, {}) or {}
    return (x.get("buys", 0) or 0) / max(x.get("sells", 0) or 0, 1)


def wash_bot_flags(pair: dict) -> dict:
    """Detect wash-trading / market-maker-bot patterns that FAKE buy pressure.

    The current buy-pressure signal is gameable: bots can spam tiny buys, or
    quote both sides 1:1 at high frequency. These guards (computed from the
    already-fetched pair) flag such fakery so it doesn't score as accumulation.
    """
    txns = pair.get("txns", {}) or {}
    vol = pair.get("volume", {}) or {}
    liq = (pair.get("liquidity", {}) or {}).get("usd", 0) or 0
    h24 = txns.get("h24", {}) or {}
    n24 = (h24.get("buys", 0) or 0) + (h24.get("sells", 0) or 0)
    vol24 = float(vol.get("h24", 0) or 0)
    avg_trade = vol24 / max(n24, 1)
    vol_liq = vol24 / max(liq, 1)
    bsr = (h24.get("buys", 0) or 0) / max(h24.get("sells", 0) or 0, 1)

    flags = []
    if n24 >= 500 and avg_trade < 30:            # many trades, all dust
        flags.append("dust刷量")
    if n24 >= 2000 and 0.9 <= bsr <= 1.1:        # high-freq, near 1:1 both sides
        flags.append("做市bot(1:1高频)")
    if vol_liq > 6:                               # volume implausibly > liquidity
        flags.append(f"量/流动性畸高{vol_liq:.0f}x")
    return {"suspicious": bool(flags), "notes": flags, "avg_trade_usd": round(avg_trade, 1)}


def accumulation_footprint(pair: dict) -> dict | None:
    """Score a DexScreener pair for the quiet-accumulation footprint (0-100).

    L1 signals (all from the already-fetched pair, ZERO extra calls):
      - WASH/BOT FILTER: reject fake buy pressure (dust spam / MM 1:1 / wash vol).
      - ABSORPTION: buy pressure WHILE price suppressed (soaking up sell-side).
      - OBV-ABSORPTION: volume ACCELERATING while price stays flat (net buying).
      - VOLATILITY COMPRESSION: recent price range tightening (coiling).
      - CONSISTENCY across m5/h1/h6; volume vs liquidity; established age.
    """
    txns = pair.get("txns", {}) or {}
    pc = pair.get("priceChange", {}) or {}
    vol = pair.get("volume", {}) or {}
    liq = (pair.get("liquidity", {}) or {}).get("usd", 0) or 0
    mc = pair.get("marketCap") or pair.get("fdv") or 0
    age_ms = pair.get("pairCreatedAt", 0) or 0

    h1, h6 = txns.get("h1", {}) or {}, txns.get("h6", {}) or {}
    buys = (h1.get("buys", 0) or 0) + (h6.get("buys", 0) or 0)
    sells = (h1.get("sells", 0) or 0) + (h6.get("sells", 0) or 0)
    if buys + sells < 20 or liq < MIN_LIQUIDITY_USD or mc < MIN_MARKETCAP_USD:
        return None

    # WASH/BOT FILTER (highest-value de-noiser): reject faked buy pressure.
    wb = wash_bot_flags(pair)
    if wb["suspicious"]:
        return None

    buy_ratio = buys / max(sells, 1)
    ch24, ch6, ch1 = (float(pc.get(k, 0) or 0) for k in ("h24", "h6", "h1"))
    vol24, vol6, vol1 = (float(vol.get(k, 0) or 0) for k in ("h24", "h6", "h1"))
    vol_liq = vol24 / max(liq, 1)

    # LIVENESS GATE: a real accumulation target has actual turnover — a whale
    # buying leaves volume. Zombie books (deep liquidity, near-zero volume) show
    # flat prices that fake "compression" and stray short-window buy ratios that
    # fake "absorption". Reject dead books outright (this is what let CAT-BSC,
    # $3.8k/24h on $153k liq = 0.025 turnover, score 85). Need both real absolute
    # volume AND meaningful turnover vs liquidity.
    if vol24 < 25_000 or vol_liq < 0.05:
        return None
    consistency = sum(1 for w in ("m5", "h1", "h6") if _ratio(txns, w) >= 1.2)

    notes, reasons = [], []
    # ABSORPTION: buy pressure with suppressed price (the sharp signal)
    if buy_ratio >= 1.5 and -8 <= ch6 <= 5:
        notes.append(f"吸收(买{buy_ratio:.1f}x价压{ch6:+.0f}%)"); reasons.append("absorption")
    elif buy_ratio >= 1.3 and 0 <= ch24 <= 25:
        notes.append(f"买压{buy_ratio:.1f}x价未爆"); reasons.append("buy_pressure")
    if vol6 > 0 and vol1 * 6 >= vol6 * 1.3 and abs(ch1) <= 3:
        notes.append("量增价平(净吸)"); reasons.append("obv_absorption")
    if abs(ch1) <= 2 and abs(ch6) <= max(abs(ch24) * 0.5, 3):
        notes.append("波动压缩(蓄势)"); reasons.append("vol_compression")
    if consistency >= 2:
        notes.append(f"{consistency}/3窗口买压"); reasons.append("consistency")
    if 0.05 <= vol_liq <= 2.0:
        notes.append("量健康")  # hygiene only, not a weighted reason
    if age_ms and (__import__("time").time() * 1000 - age_ms) > 7 * 86400 * 1000:
        notes.append("已建立"); reasons.append("established")

    penalty = 30 if ch24 > 80 else 0
    if ch24 > 80:
        notes.append("已大涨")
    score = score_reasons(reasons) - penalty

    if score < 50:
        return None
    return {"score": score, "buy_ratio": round(buy_ratio, 2),
            "price_change_24h": ch24, "consistency": consistency,
            "avg_trade_usd": wb["avg_trade_usd"], "penalty": penalty,
            "mc": mc, "liquidity": liq, "reasons": reasons, "notes": " · ".join(notes)}


def cex_outflow_signal(token: str, chain: str, lookback_blocks: int = 600_000) -> dict | None:
    """Cheap CEX-outflow check: is token supply LEAVING known exchanges?

    Sums balance held by known CEX addresses now vs `lookback_blocks` ago via
    archive balanceOf. A decline = supply moving off exchanges = accumulation
    precursor. EVM only (uses evm_archive). A few calls per token, so run only on
    the top market-screened candidates.
    """
    if chain in ("solana", "sol"):
        return None
    try:
        from src.onchain.evm_archive import ArchiveRPC
        from src.onchain.cex_addresses import evm_exchanges
    except Exception:
        return None
    rpc = ArchiveRPC(chain)
    if not rpc.available():
        return None
    cex = list(evm_exchanges())[:12]  # bound calls
    latest = rpc.latest_block()
    now = sum((rpc.balance_of(token, a, latest) or 0) for a in cex)
    past = sum((rpc.balance_of(token, a, max(1, latest - lookback_blocks)) or 0) for a in cex)
    if past <= 0:
        return None
    change_pct = round((now - past) / past * 100, 1)
    return {"cex_change_pct": change_pct, "outflow": change_pct < -5}


_SMART_SET_CACHE: dict[str, dict[str, int]] = {}


def _smart_money_set(chain: str) -> dict[str, int]:
    """Known smart-money wallets for a chain → {address(lower): tier}. Cached."""
    if chain in _SMART_SET_CACHE:
        return _SMART_SET_CACHE[chain]
    out: dict[str, int] = {}
    try:
        import yaml
        from src.config import CONFIG_DIR

        data = yaml.safe_load((CONFIG_DIR / "smart_money_wallets.yaml").read_text()) or {}
        norm = "ethereum" if chain in ("ethereum", "base", "bsc", "arbitrum", "optimism") else chain
        for w in data.get("wallets", []):
            wc = w.get("chain", "")
            wc_norm = "ethereum" if wc in ("ethereum", "base", "bsc", "arbitrum", "optimism") else wc
            if wc_norm == norm and w.get("address"):
                addr = w["address"]
                out[addr if norm == "solana" else addr.lower()] = w.get("tier", 3)
    except Exception as e:
        logger.debug("smart_set_load_failed", error=str(e))
    _SMART_SET_CACHE[chain] = out
    return out


def _contract_addresses_evm(addresses: list[str], chain: str) -> set[str]:
    """Return the subset that are CONTRACTS (eth_getCode != 0x). EOAs have no code.
    Free on any EVM chain (basic RPC, no indexer). Lets us auto-drop protocol
    contracts (veOLAS/Timelock/vesting/bridges) that inflate raw concentration but
    aren't an operator. One call per address — bounded to the top holders."""
    out: set[str] = set()
    try:
        from src.onchain.evm_archive import ArchiveRPC
        rpc = ArchiveRPC(chain)
        if not rpc.available():
            return out
        for a in addresses:
            try:
                code = rpc._call("eth_getCode", [a, "latest"]).get("result", "0x")
                if code and code != "0x":
                    out.add(a.lower())
            except Exception:
                continue
    except Exception as e:
        logger.debug("getcode_failed", chain=chain, error=str(e))
    return out


# Chains where first-funder lookup is FREE: Etherscan free = ETH, Helius = Solana,
# and Moralis free tier = BSC + other EVM chains (when MORALIS_API_KEY is set).
# Without Moralis, BSC falls back to contract/CEX/similar-balance only and gets
# flagged "needs Arkham".
def _funder_available(chain: str) -> bool:
    if chain in ("ethereum", "solana", "sol"):
        return True
    # Non-ETH EVM chains need Moralis (multi-key rotated) for free funder lookups.
    from src.onchain import moralis_client
    return moralis_client.available() and chain in (
        "bsc", "base", "arbitrum", "optimism", "polygon")


def effective_concentration_signal(holders: list[dict], token: str, chain: str) -> dict | None:
    """The linchpin: does ONE private entity control a big share of the FLOAT,
    hidden across many wallets (Sybil)? This is what separates a real operator
    (SIREN: 131 wallets = 1 entity) from a genuinely distributed token (CREPE: 45
    wallets = 42 entities) or a protocol with locked tokenomics (OLAS: in
    contracts). Age is irrelevant — effective concentration is the discriminator.

    Steps (all free on ETH/SOL; BSC minus the funder edge):
      1. Top holders, minus known CEX and contracts (eth_getCode — drops
         veOLAS/bridges/LP/vesting that aren't an operator).
      2. Cluster remaining EOAs by shared first-funder + similar balance.
      3. Effective vs nominal concentration. A big gap = many addresses collapse
         to few entities = hidden operator.
    """
    # Burn / dead addresses: out of circulation, must not count as concentration
    # (this is what made WKC read as "52.8% one holder" — it was the burn address).
    _BURN = {"0x0000000000000000000000000000000000000000",
             "0x000000000000000000000000000000000000dead",
             "0x0000000000000000000000000000000000000001"}
    pos = [{"address": h["address"], "balance": float(h.get("balance", 0) or 0)}
           for h in holders
           if float(h.get("balance", 0) or 0) > 0 and str(h["address"]).lower() not in _BURN]
    if len(pos) < 8:
        return None
    pos.sort(key=lambda h: -h["balance"])
    top = pos[:70]   # deeper than 50 → catches larger Sybil nets (e.g. SIREN's 131)
    is_sol = chain in ("solana", "sol")

    # Exclude known CEX.
    try:
        from src.onchain.cex_addresses import evm_exchanges, solana_exchanges
        cex_raw = (solana_exchanges() if is_sol else evm_exchanges())
        cex = set(cex_raw) if is_sol else {a.lower() for a in cex_raw}
    except Exception:
        cex = set()
    # Exclude contracts (EVM only; free getCode).
    contracts = set() if is_sol else _contract_addresses_evm([h["address"] for h in top], chain)

    def is_excluded(a: str) -> bool:
        al = a if is_sol else a.lower()
        return al in cex or al in contracts

    eoa = [h for h in top if not is_excluded(h["address"])]
    if len(eoa) < 5:
        return None

    funder_free = _funder_available(chain)
    funders = {}
    if funder_free:
        try:
            from src.onchain.funder_graph import get_funders
            funders = get_funders([h["address"] for h in eoa], chain, max_lookups=65)
        except Exception as e:
            logger.debug("funders_failed", chain=chain, error=str(e))

    # Cluster the EOAs (shared funder + similar balance), then measure the largest
    # entity as a share of TOTAL supply — NOT of the filtered subset. (Subset-
    # relative inflates: CREPE's top holder is 27% of the top-38 EOAs but only
    # 2.5% of supply — not an operator.)
    try:
        from src.onchain.entity_clustering import cluster_addresses, _norm
        bal_map = {_norm(h["address"]): h["balance"] for h in eoa}
        mapping = cluster_addresses(
            [h["address"] for h in eoa], funders or None,
            balances=bal_map, min_batch_funder=2)
    except Exception as e:
        logger.debug("eff_conc_failed", token=token, error=str(e))
        return None

    total_supply = sum(h["balance"] for h in pos)
    if total_supply <= 0:
        return None
    entity_bal: dict[str, float] = {}
    for a, b in bal_map.items():
        ent = mapping.get(a, a)
        entity_bal[ent] = entity_bal.get(ent, 0.0) + b
    largest_entity = max(entity_bal.values()) if entity_bal else 0.0
    largest_addr = max(bal_map.values()) if bal_map else 0.0
    eff_pct = round(largest_entity / total_supply * 100, 2)   # share of supply
    nom_pct = round(largest_addr / total_supply * 100, 2)     # biggest single addr
    # Dominant funder = the funder whose funded wallets sum to the most balance —
    # the hunt verifies its fan-out to reject dispersers/launchpads (#3).
    dominant_funder = None
    if funders:
        by_funder: dict[str, float] = {}
        for a, b in bal_map.items():
            f = funders.get(a)
            if f:
                by_funder[f] = by_funder.get(f, 0.0) + b
        if by_funder:
            dominant_funder = max(by_funder, key=by_funder.get)
    return {
        "largest_entity_pct": eff_pct,           # biggest hidden entity / supply
        "largest_address_pct": nom_pct,          # biggest single address / supply
        "concentration_gap": round(eff_pct - nom_pct, 2),  # clustering uplift
        "entity_count": len(entity_bal),
        "eoa_analyzed": len(eoa),
        "funder_complete": bool(funders),
        "dominant_funder": dominant_funder,
    }


def onchain_enrich(token: str, chain: str) -> dict | None:
    """Fetch holders ONCE and derive two signals (zero double-fetch):

      1. Holder-count TREND vs snapshot history (rising = real accumulation).
      2. SMART-MONEY presence — intersect the holder set with known smart-money
         wallets. A proven tier-1/2 wallet holding the token is a strong tell,
         and it's FREE (we already have the holder list).
    """
    try:
        from src.onchain import holder_snapshot as hs
    except Exception:
        return None
    try:
        prior = hs.get_snapshots(token, chain, limit=20)
        if chain in ("solana", "sol"):
            holders = hs.fetch_holders_solana(token)
        else:
            cid = {"ethereum": 1, "base": 8453, "bsc": 56, "arbitrum": 42161, "optimism": 10}.get(chain, 1)
            holders = hs.fetch_holders_evm(token, chain_id=cid, max_pages=15)
        if not holders:
            return None
        hs.save_snapshot(token, chain, holders)
        count = len([h for h in holders if (h.get("balance") or 0) > 0])

        # Smart-money intersection (free — reuses fetched holders).
        smart = _smart_money_set(chain)
        hit_addrs = [h["address"] for h in holders
                     if (h["address"] if chain in ("solana", "sol") else str(h["address"]).lower()) in smart]
        top_tier = min((smart[a if chain in ("solana", "sol") else a.lower()] for a in hit_addrs), default=None)

        prior_counts = [s.get("holder_count", 0) for s in prior if s.get("holder_count")]
        base = prior_counts[0] if prior_counts else None
        rising = bool(base and base > 0 and count >= base * 1.05)
        # Effective concentration (the linchpin): cluster holders → is one hidden
        # entity controlling the float? Reuses the holders we just fetched.
        conc = effective_concentration_signal(holders, token, chain)
        return {
            "holder_count": count, "prior_count": base, "rising": rising,
            "change_pct": round((count - base) / max(base, 1) * 100, 1) if base else None,
            "smart_hits": len(hit_addrs), "smart_top_tier": top_tier,
            "concentration": conc,
        }
    except Exception as e:
        logger.debug("onchain_enrich_failed", token=token, error=str(e))
        return None


def _state_db():
    """Per-token state across runs: liquidity trend + appearance persistence."""
    import sqlite3
    from src.config import DATA_DIR

    path = DATA_DIR / "screener_state.db"
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), timeout=10)
    conn.execute("PRAGMA busy_timeout=10000")
    conn.execute("""CREATE TABLE IF NOT EXISTS screener_state (
        token TEXT, chain TEXT, first_seen TEXT, last_seen TEXT,
        appearances INTEGER DEFAULT 1, liquidity REAL,
        PRIMARY KEY (token, chain))""")
    # Migrate older tables (created before the persistence columns existed):
    # CREATE IF NOT EXISTS won't add columns, so ALTER any that are missing —
    # otherwise track_state silently fails and persistence never accrues.
    cols = {r[1] for r in conn.execute("PRAGMA table_info(screener_state)").fetchall()}
    for col, ddl in (("first_seen", "first_seen TEXT"), ("last_seen", "last_seen TEXT"),
                     ("appearances", "appearances INTEGER DEFAULT 1"),
                     ("liquidity", "liquidity REAL")):
        if col not in cols:
            conn.execute(f"ALTER TABLE screener_state ADD COLUMN {ddl}")
    # Emission log: which signals fired for each candidate each run. The
    # calibrator joins this with labeled outcomes to learn data-driven weights.
    conn.execute("""CREATE TABLE IF NOT EXISTS emissions (
        token TEXT, chain TEXT, ts TEXT, reasons TEXT, score REAL)""")
    conn.commit()
    return conn


def log_emission(token: str, chain: str, reasons: list[str], score: float) -> None:
    """Persist a candidate's fired signals for later weight calibration."""
    from datetime import datetime, timezone

    try:
        conn = _state_db()
        try:
            conn.execute(
                "INSERT INTO emissions (token, chain, ts, reasons, score) VALUES (?,?,?,?,?)",
                (token, chain, datetime.now(timezone.utc).isoformat(), json.dumps(reasons), score),
            )
            conn.commit()
        finally:
            conn.close()
    except Exception as e:
        logger.debug("log_emission_failed", token=token, error=str(e))


def track_state(token: str, chain: str, current_liq: float) -> dict:
    """Record this run's appearance and return persistence + liquidity trend.

    Persistence: a token that shows the accumulation footprint across MANY runs
    is real accumulation; a one-off is likely bot noise. Returns:
      {appearances, recurring, liq_rising, liq_change_pct}
    """
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc).isoformat()
    try:
        conn = _state_db()
        try:
            row = conn.execute(
                "SELECT appearances, liquidity, first_seen FROM screener_state "
                "WHERE token=? AND chain=?", (token, chain),
            ).fetchone()
            if row:
                appearances = (row[0] or 0) + 1
                conn.execute(
                    "UPDATE screener_state SET last_seen=?, appearances=?, liquidity=? "
                    "WHERE token=? AND chain=?",
                    (now, appearances, current_liq, token, chain),
                )
                prior_liq = row[1]
            else:
                appearances, prior_liq = 1, None
                conn.execute(
                    "INSERT INTO screener_state (token, chain, first_seen, last_seen, appearances, liquidity) "
                    "VALUES (?,?,?,?,1,?)", (token, chain, now, now, current_liq),
                )
            conn.commit()
        finally:
            conn.close()
        liq_change = ((current_liq - prior_liq) / max(prior_liq, 1) * 100) if prior_liq else None
        return {
            "appearances": appearances,
            "recurring": appearances >= 3,           # seen in 3+ runs = sustained
            "liq_rising": bool(liq_change is not None and liq_change >= 8),
            "liq_change_pct": round(liq_change, 1) if liq_change is not None else None,
        }
    except Exception as e:
        logger.debug("track_state_failed", token=token, error=str(e))
        return {"appearances": 1, "recurring": False, "liq_rising": False, "liq_change_pct": None}


def whale_accumulation_symbols() -> set[str]:
    """Run whale_tracker once → set of token SYMBOLS showing CEX→wallet
    accumulation (large $1M+ withdrawals into private wallets). Matched against
    candidates by symbol. One collector run per screen (not per-token)."""
    try:
        from src.collectors.whale_tracker import WhaleTrackerCollector

        async def _fetch():
            c = WhaleTrackerCollector()
            await c.setup()
            try:
                return await c._collect()
            finally:
                await c.teardown()

        res = _run_coro(_fetch())
        return {
            str(it.metadata.get("token", "")).upper()
            for it in res.items if it.metadata.get("signal") == "accumulation"
        }
    except Exception as e:
        logger.debug("whale_accum_failed", error=str(e))
        return set()


def screen_universe(queries: list[str] | None = None, max_out: int = 15) -> list[dict]:
    """Scan a universe of tokens and rank by accumulation footprint.

    `queries` seed the DexScreener search (defaults to broad meme/AI/low-cap terms).
    Returns ranked candidates: {symbol, chain, address, score, notes}.
    """
    queries = queries or ["ai", "agent", "meme", "pepe", "inu", "cat", "moon"]
    seen, cands = set(), []
    # Universe = actively-traded pools (GeckoTerminal trending + top-volume across
    # free-data chains) + keyword search. The first gives a real market universe;
    # the second catches narrative tokens by name. Both feed the same scorer.
    universe = _universe_pairs()
    for q in queries:
        universe.extend(_dexscreener_pairs(q))
    for p in universe:
        base = p.get("baseToken", {}) or {}
        addr, chain = base.get("address"), p.get("chainId")
        key = (chain, addr)
        if not addr or key in seen:
            continue
        seen.add(key)
        fp = accumulation_footprint(p)
        if fp:
            cands.append({
                "symbol": base.get("symbol", "?"), "chain": chain,
                "address": addr, "url": p.get("url", ""), **fp,
            })
    cands.sort(key=lambda c: -c["score"])
    cands = cands[:max_out]

    # --- Macro gate + whale accumulation set (each: 1 collector run per screen) ---
    macro_boost = _macro_reserve_gate()
    whale_accum = whale_accumulation_symbols()

    # --- L2 per-candidate enrichment (top-N only; reuses free collectors) ---
    import time

    for i, c in enumerate(cands):
        c["reasons"] = c.get("reasons", [])
        penalty = c.get("penalty", 0)
        # Whale accumulation (symbol match against $1M+ CEX→wallet flows)
        if c.get("symbol", "").upper() in whale_accum:
            c["notes"] += " · 鲸鱼累积流"; c["reasons"].append("whale_accumulation")
        # Persistence + liquidity trend across runs (one local DB op).
        st = track_state(c["address"], c["chain"], c.get("liquidity", 0))
        c["appearances"] = st["appearances"]
        if st.get("liq_rising"):
            c["notes"] += f" · 流动性+{st.get('liq_change_pct',0):.0f}%"; c["reasons"].append("liquidity_rising")
        if st.get("recurring"):
            c["notes"] += f" · 持续{st['appearances']}轮"; c["reasons"].append("persistent")
        # CEX outflow (EVM archive, ~24 calls)
        try:
            cx = cex_outflow_signal(c["address"], c["chain"])
        except Exception:
            cx = None
        if cx and cx.get("outflow"):
            c["notes"] += f" · CEX流出{cx['cex_change_pct']:+.0f}%"
            c["reasons"].append("cex_outflow"); c["cex_outflow"] = cx["cex_change_pct"]
        # On-chain enrich (holder trend + smart-money presence). Top 8 + pause.
        if i < 8:
            try:
                oc = onchain_enrich(c["address"], c["chain"])
            except Exception:
                oc = None
            if oc and oc.get("rising"):
                c["notes"] += f" · 持币人数+{oc.get('change_pct',0):.0f}%"; c["reasons"].append("holders_rising")
            if oc and oc.get("smart_hits"):
                tier = oc.get("smart_top_tier") or 3
                c["notes"] += f" · 🐳聪明钱{oc['smart_hits']}个(T{tier})"
                c["reasons"].append(f"smart_money_t{tier}")
            # Effective concentration — the linchpin. One hidden entity controlling
            # the float (SIREN-like) is the strongest operator signal we have.
            conc = (oc or {}).get("concentration")
            if conc:
                c["largest_entity_pct"] = conc.get("largest_entity_pct")
                c["largest_address_pct"] = conc.get("largest_address_pct")
                c["concentration_gap"] = conc.get("concentration_gap")
                c["funder_complete"] = conc.get("funder_complete")
                lg = conc.get("largest_entity_pct", 0) or 0
                gap = conc.get("concentration_gap", 0) or 0
                # Thresholds are share-of-SUPPLY: one entity holding >=15% of
                # supply is a controlling operator; a big clustering uplift (gap)
                # means many small-looking addresses collapse to one (Sybil).
                if conc.get("funder_complete") and lg >= 15:
                    c["notes"] += f" · ⭐有效集中{lg:.0f}%供应(单一实体控盘)"
                    c["reasons"].append("effective_concentration")
                if conc.get("funder_complete") and gap >= 8:
                    c["notes"] += f" · 隐藏簇(单址{conc.get('largest_address_pct',0):.0f}%→实体{lg:.0f}%)"
                    c["reasons"].append("hidden_cluster")
                if not conc.get("funder_complete") and c["chain"] not in ("ethereum", "solana", "sol"):
                    c["notes"] += " · ⚠️需Arkham(BSC funder未解析)"
            time.sleep(0.5)
        if macro_boost:
            c["reasons"].append("cex_reserves_draining")
        # Final score = weighted sum of ALL fired signals − penalty (data-driven).
        c["score"] = score_reasons(c["reasons"]) - penalty

    cands.sort(key=lambda c: -c["score"])

    # Log emissions (fired signals) for weight calibration once labels exist.
    for c in cands:
        log_emission(c["address"], c["chain"], c.get("reasons", []), c.get("score", 0))

    # Close the loop: high-confidence candidates → watchlist, so Stage 2 monitors
    # them for the launch event and forward holder history accrues.
    try:
        from src.onchain import watchlist

        for c in cands:
            if c["score"] >= 80:
                watchlist.add_to_watchlist(c["address"], c["chain"],
                                           c.get("score", 0), symbol=c.get("symbol", ""))
    except Exception as e:
        logger.debug("watchlist_add_failed", error=str(e))

    return cands


def _run_coro(coro):
    """Run a coroutine whether or not an event loop is already running
    (screen_universe is sync but called from the async scheduler)."""
    import asyncio

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    import concurrent.futures

    with concurrent.futures.ThreadPoolExecutor(1) as ex:
        return ex.submit(asyncio.run, coro).result()


def _macro_reserve_gate() -> int:
    """If major-CEX reserves are draining (accumulation regime), boost all
    candidates by a base amount. One cheap DeFiLlama check via exchange_reserves."""
    try:
        from src.collectors.exchange_reserves import ExchangeReserveCollector

        async def _fetch():
            c = ExchangeReserveCollector()
            await c.setup()
            try:
                return await c._collect()
            finally:
                await c.teardown()

        res = _run_coro(_fetch())
        draining = sum(
            1 for it in res.items
            if float(it.metadata.get("change_24h_pct", 0) or 0) <= -10
        )
        if draining >= 1:
            logger.info("macro_reserves_draining", count=draining)
            return 10
    except Exception as e:
        logger.debug("macro_gate_failed", error=str(e))
    return 0


def _esc(s) -> str:
    """HTML-escape for Telegram. Meme symbols often contain & < > / emojis that
    break parse_mode=HTML and render as garbled text."""
    return (str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))


def format_candidates(cands: list[dict]) -> str:
    """Telegram-friendly candidate list (all data-derived strings escaped)."""
    if not cands:
        return "本轮无吸筹足迹候选。"
    lines = ["🔎 <b>疑似吸筹候选</b>(待 Arkham 查庄簇 → 二次确认)", ""]
    for i, c in enumerate(cands[:12], 1):
        lines.append(
            f"{i}. <b>{_esc(c['symbol'])}</b> [{_esc(c['chain'])}] 评分{int(c['score'])}\n"
            f"   {_esc(c['notes'])}\n"
            f"   <code>{_esc(c['address'])}</code>"
        )
    lines.append("\n<i>仅市场足迹筛选,不代表确认有庄。下一步:Arkham 查控盘者→粘地址→曲线确认。</i>")
    return "\n".join(lines)


def main():
    cands = screen_universe()
    print("=" * 60)
    print("疑似吸筹候选(市场足迹)")
    print("=" * 60)
    for i, c in enumerate(cands, 1):
        print(f"{i:>2}. {c['symbol']:12} [{c['chain']:8}] score={c['score']:>3}  {c['notes']}")
        print(f"     {c['address']}")
    print(f"\n共 {len(cands)} 个候选 → 下一步: Arkham 查这些币的控盘者, 把庄簇粘回来确认")


if __name__ == "__main__":
    main()
