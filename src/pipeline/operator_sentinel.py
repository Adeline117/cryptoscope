"""Operator sentinel — watch confirmed operator clusters for the three events
that actually matter, and alert the moment one fires.

For a 妖币 where we've identified the operator's wallet cluster (e.g. BASED), the
trade-relevant signals are NOT price prediction — they're the operator's own
behavior:

  1. LAUNCH    — price breakout / volume spike vs baseline. The operator is
                 pumping. (Ride-with point.)
  2. DISTRIBUTE— the cluster's combined balance DROPS. The operator is selling
                 into strength. (Exit point.)
  3. RUG       — liquidity collapses. Pulled LP / honeypot turn. (Flee point.)

Each registered target stores a baseline + last-seen state; check_run() compares
current on-chain state to last-seen, fires Telegram on any trigger, and persists.
Free: cluster balance via BSC/EVM archive eth_call, market data via DexScreener.
Detection/alert only — never trades.

    python -m src.pipeline.operator_sentinel --register-based   # seed BASED
    python -m src.pipeline.operator_sentinel                    # one check pass
"""

from __future__ import annotations

import json
import sys
import urllib.request

import structlog

from src.config import DATA_DIR

logger = structlog.get_logger()

SENTINELS_FILE = DATA_DIR / "operator_sentinels.json"

# Trigger thresholds (vs last-seen for distribute/rug; vs baseline for launch).
# PRIMARY signal = the operator's own action (net position turn). balanceOf is
# exact (no noise), so even a small move is a real transaction — we trigger on the
# FIRST meaningful sell/buy to act BEFORE price has moved, capturing the full move.
OP_SELL = 0.015          # cluster balance fell >=1.5% vs last → operator SELLING (exit/short NOW)
OP_BUY = 0.02            # cluster balance rose >=2% vs last → operator BUYING (markup/launch → long)
BLEED_STEP = 5.0         # slow-bleed: alert each extra 5% below BASELINE (dribble evasion)
SLOW_BLEED = 0.04        # cluster down >=4% from running peak → chunked distribution
COOLDOWN_MIN = 45        # don't re-fire the same event kind within N minutes
STALL_FADE = 0.12        # after a buy/launch, price faded >=12% from its high...
MAX_MOMENTUM_H = 36      # ...within this window, with no fresh operator buying → 动能熄火
# 庄停手 is ADAPTIVE per token: silence > STOP_K × the operator's own recent buy
# cadence = stopped (clamped). A hot buyer (every few min) trips fast; a slow/
# dormant cluster never false-fires. Needs >=2 recent buys to know the rhythm.
STOP_K = 4               # stopped = silence > 4× the median recent buy interval
STOP_MIN_S = 900         # ...but never less than 15 min (avoid twitchy)
STOP_MAX_S = 21600       # ...never more than 6h (avoid waiting forever)
# Price/liquidity are only a BACKSTOP — catch a violent move if balance sampling lags.
RUG_DROP = 0.30          # liquidity fell >=30% → LP pull / rug
CRASH_DROP = 0.15        # price fell >=15% vs last check → 砸盘 backstop (violent)
PRICE_DD = 0.18          # price down >=18% from a recent high → 急跌 (GRADUAL bleed,
                         # even if no single cycle dropped 15% — what missed EVAA)
LAUNCH_PRICE = 0.25      # price up >=25% vs last check → launch backstop
LAUNCH_VOL = 3.0         # 24h volume >=3x baseline → volume backstop
# ===== 控浮筹型点火 (the MAME class) =====
# A high-control operator (e.g. MAME: 44.5% frozen, ~5% float) marks up by RESTRICTING
# the sellable float, NOT by buying — so cluster-balance watching misses the ignition.
# These two are operator-ATTRIBUTABLE on a controlled-float token (no independent buyer
# of size exists), so they fit the "only 庄 info" rule — both are GATED on the cluster
# not being the seller, so a retail-driven move never trips them.
FLOAT_TIGHTEN = 0.15     # LP token float fell >=15% vs last (cluster not selling) → 浮筹收紧 (pre-ignition)
BREAKOUT_UP = 0.12       # price broke >=12% above its running peak (cluster flat/up) → 控盘突破 (ignition)


import contextlib


@contextlib.contextmanager
def _state_lock():
    """Exclusive cross-process lock so the 20s watcher and 5-min scheduler never
    corrupt the shared state file with a concurrent read-modify-write (#7)."""
    import fcntl
    SENTINELS_FILE.parent.mkdir(parents=True, exist_ok=True)
    lockf = SENTINELS_FILE.with_suffix(".lock")
    f = open(lockf, "w")
    try:
        fcntl.flock(f, fcntl.LOCK_EX)
        yield
    finally:
        try:
            fcntl.flock(f, fcntl.LOCK_UN)
        finally:
            f.close()


def _load() -> dict:
    if SENTINELS_FILE.exists():
        try:
            return json.loads(SENTINELS_FILE.read_text())
        except Exception:
            return {}
    return {}


def _save(data: dict) -> None:
    SENTINELS_FILE.parent.mkdir(parents=True, exist_ok=True)
    # Atomic write (tmp + replace) so a reader never sees a half-written file.
    import os
    tmp = SENTINELS_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2))
    os.replace(tmp, SENTINELS_FILE)


def _dex(token: str, chain: str) -> dict:
    """Liquidity / price / 24h volume from DexScreener (deepest pair)."""
    try:
        url = f"https://api.dexscreener.com/token-pairs/v1/{chain}/{token}"
        req = urllib.request.Request(url, headers={"User-Agent": "CryptoScope/1.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            d = json.loads(r.read().decode())
        pairs = d if isinstance(d, list) else d.get("pairs", [])
        if not pairs:
            return {}
        p = max(pairs, key=lambda x: (x.get("liquidity", {}) or {}).get("usd", 0) or 0)
        return {
            "price": float(p.get("priceUsd") or 0),
            "liquidity": float((p.get("liquidity", {}) or {}).get("usd", 0) or 0),
            "vol24": float((p.get("volume", {}) or {}).get("h24", 0) or 0),
            "pair": p.get("pairAddress"),   # deepest LP — its token balance = the tradable float
        }
    except Exception as e:
        logger.debug("sentinel_dex_failed", token=token, error=str(e))
        return {}


_FUNDING_CACHE: dict[str, tuple[float, float | None]] = {}
_FUNDING_TTL = 300  # funding changes every 8h; 5-min cache is plenty (and lets the
                    # ~20s real-time loop reuse it instead of hammering Gate/MEXC).


def _funding_rate(symbol: str) -> float | None:
    """Perp funding rate (%/8h) for {SYMBOL}_USDT — Gate primary, MEXC fallback.
    Positive = longs crowded (short-favorable + paid); negative = shorts crowded.
    Cached 5 min."""
    if not symbol:
        return None
    import time as _t
    hit = _FUNDING_CACHE.get(symbol)
    if hit and _t.time() - hit[0] < _FUNDING_TTL:
        return hit[1]
    val = None
    try:
        url = f"https://api.gateio.ws/api/v4/futures/usdt/contracts/{symbol}_USDT"
        req = urllib.request.Request(url, headers={"User-Agent": "CryptoScope/1.0"})
        with urllib.request.urlopen(req, timeout=12) as r:
            d = json.loads(r.read().decode())
        if isinstance(d, dict) and d.get("funding_rate") is not None:
            val = round(float(d["funding_rate"]) * 100, 4)
    except Exception:
        pass
    if val is None:
        try:
            url = f"https://contract.mexc.com/api/v1/contract/funding_rate/{symbol}_USDT"
            req = urllib.request.Request(url, headers={"User-Agent": "CryptoScope/1.0"})
            with urllib.request.urlopen(req, timeout=12) as r:
                d = json.loads(r.read().decode())
            if isinstance(d, dict) and d.get("data"):
                val = round(float(d["data"].get("fundingRate", 0) or 0) * 100, 4)
        except Exception:
            pass
    _FUNDING_CACHE[symbol] = (_t.time(), val)
    return val


def _price_peak_now(token: str, chain: str) -> tuple[float, float] | None:
    """(30-day peak close, latest close) from GeckoTerminal daily OHLCV — free."""
    try:
        d = _dex(token, chain)  # warms nothing; need pool addr from DexScreener
        u = f"https://api.dexscreener.com/token-pairs/v1/{chain}/{token}"
        req = urllib.request.Request(u, headers={"User-Agent": "CryptoScope/1.0"})
        with urllib.request.urlopen(req, timeout=12) as r:
            pairs = json.loads(r.read().decode())
        pairs = pairs if isinstance(pairs, list) else pairs.get("pairs", [])
        if not pairs:
            return None
        pool = max(pairs, key=lambda x: (x.get("liquidity", {}) or {}).get("usd", 0) or 0).get("pairAddress")
        gt = "https://api.geckoterminal.com/api/v2/networks/" + \
             {"bsc": "bsc", "ethereum": "eth", "solana": "solana", "base": "base"}.get(chain, chain) + \
             f"/pools/{pool}/ohlcv/day?limit=30"
        req = urllib.request.Request(gt, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=12) as r:
            rows = json.loads(r.read().decode()).get("data", {}).get("attributes", {}).get("ohlcv_list", [])
        if not rows:
            return None
        closes = [float(x[4]) for x in rows]
        return max(closes), closes[0]  # ohlcv_list is newest-first
    except Exception as e:
        logger.debug("price_peak_failed", token=token, error=str(e))
        return None


def _distribution_history(token: str, chain: str, wallets: list[str]) -> dict:
    """Has this cluster ever DISTRIBUTED (sold down from a peak)? The n=2 backtest
    showed detected clusters are often 'accumulate-and-hold believers' who ride
    pumps AND crashes without selling — following them traps you too. A cluster
    with a real sell-down in its history is a profit-taking operator (worth
    following); one that only ever accumulates is a believer (caveat). EVM only."""
    if chain in ("solana", "sol"):
        return {"profile": "?", "max_drawdown_pct": None}
    try:
        from src.onchain.evm_archive import ArchiveRPC, operator_curve_evm
        rpc = ArchiveRPC(chain)
        if not rpc.available():
            return {"profile": "?", "max_drawdown_pct": None}
        latest = rpc.latest_block()
        spb = rpc.seconds_per_block()                 # live block time, not hardcoded 28800/day
        blocks_per_day = max(1, int(86400 / spb))
        c = operator_curve_evm(token, wallets, chain, latest - 90 * blocks_per_day, latest,
                               n_points=10, pause=0.05)
        bs = (c or {}).get("balance_series") or []
        blks = (c or {}).get("block_series") or []
        # ===== AGE GATE (the MAME false-positive root cause) =====
        # operator_curve samples balanceOf back 90d; for a token that didn't exist
        # yet, those samples read 0. Counting pre-existence 0s (and intra-launch LP
        # noise) as a "held-then-dropped" history fabricated MAME's phantom 71%
        # "distribution profile" on a 4-day-old token. Judge ONLY over samples where
        # the cluster actually held, and refuse to judge a too-young token/cluster.
        nz_idx = [i for i, v in enumerate(bs) if v and v > 0]
        if len(nz_idx) < 4:
            return {"profile": "?(数据不足,不可判派发履历)",
                    "max_drawdown_pct": None, "nonzero_samples": len(nz_idx)}
        if blks and nz_idx[0] < len(blks):
            age_days = (latest - blks[nz_idx[0]]) * spb / 86400.0
            if age_days < 14:
                return {"profile": f"?(币/簇仅~{age_days:.0f}天,太新不可判派发履历)",
                        "max_drawdown_pct": None, "age_days": round(age_days, 1)}
        nz = [bs[i] for i in nz_idx]                  # drawdown over the held period only
        peak = nz[0]
        max_dd = 0.0
        for v in nz:
            if v > peak:
                peak = v
            elif peak > 0:
                max_dd = max(max_dd, (peak - v) / peak)
        dd = round(max_dd * 100, 1)
        profile = "聪明庄(有派发履历)" if dd >= 25 else "信仰者(只吸不卖)"
        return {"profile": profile, "max_drawdown_pct": dd, "nonzero_samples": len(nz)}
    except Exception as e:
        logger.debug("dist_history_failed", token=token, error=str(e))
        return {"profile": "?", "max_drawdown_pct": None}


def assess_second_leg() -> dict:
    """Classify each tracked cluster as a SECOND-LEG candidate: pumped before
    (>=2x at some point), retraced into a buy zone (now <=60% of peak), AND the
    operator is still loaded (balance >= 90% of baseline = didn't distribute). Such
    a setup is coiled to re-pump off the same bag — its launch alert is top priority.
    Stores the verdict on each target. Returns {symbol: verdict}."""
    data = _load()
    out = {}
    for key, t in data.items():
        pn = _price_peak_now(t["token"], t["chain"])
        cb = _cluster_balance(t["token"], t["chain"], t["wallets"])
        base = (t.get("baseline", {}) or {}).get("cluster_balance") or 0
        loaded = bool(cb is not None and base and cb >= base * 0.9)
        verdict = "—"
        if pn:
            peak, now = pn
            pumped = peak >= now * 2          # at least doubled at some point
            pulled_back = now <= peak * 0.6   # retraced >=40% from peak
            if pumped and pulled_back and loaded:
                verdict = f"⭐二波候选 (峰值{peak/now:.1f}x→已回落, 庄满仓)"
            elif pumped and not pulled_back:
                verdict = "高位 (已拉未回落)"
            elif not pumped and loaded:
                verdict = "未拉过 (满仓待发,如BASED)"
            elif not loaded:
                verdict = "⚠️操作者已减仓"
        # balanceOf-based verdicts can't be trusted on reflection/fee tokens.
        if t.get("balanceof_reliable") is False:
            verdict += " ⚠️(balanceOf失真,二波/满仓判断仅供参考)"
        t["second_leg"] = verdict
        t["loaded"] = loaded
        # Operator type: profit-taker vs hold-forever believer (refines how much to
        # trust a launch signal from this cluster).
        dh = _distribution_history(t["token"], t["chain"], t["wallets"])
        t["operator_type"] = dh["profile"]
        t["max_drawdown_pct"] = dh["max_drawdown_pct"]
        out[t["symbol"]] = f"{verdict} | {dh['profile']}"
    _save(data)
    return out


def _solana_wallet_balance(owner: str, mint: str, rpc: str, timeout: int = 12) -> float:
    """One wallet's balance of a specific mint — light (no full holder fetch), so
    a cluster of N wallets is N cheap calls, usable in the ~20s real-time loop."""
    import json
    import urllib.request
    payload = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "getTokenAccountsByOwner",
                          "params": [owner, {"mint": mint}, {"encoding": "jsonParsed"}]})
    try:
        req = urllib.request.Request(rpc, data=payload.encode(),
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            res = json.loads(r.read().decode()).get("result", {}) or {}
        total = 0.0
        for acc in res.get("value", []):
            ta = acc.get("account", {}).get("data", {}).get("parsed", {}).get("info", {}).get("tokenAmount", {})
            total += float(ta.get("uiAmount") or 0)
        return total
    except Exception:
        return 0.0


def _cluster_balance(token: str, chain: str, wallets: list[str]) -> float | None:
    """Combined token balance of the operator cluster — light enough for real-time.
    Solana: per-wallet getTokenAccountsByOwner (N cheap calls, no full holder fetch).
    EVM: combined balanceOf via free archive eth_call."""
    if chain in ("solana", "sol"):
        import os
        rpc = os.environ.get("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")
        return sum(_solana_wallet_balance(w, token, rpc) for w in wallets)
    try:
        from src.onchain.evm_archive import ArchiveRPC, combined_balance_at
        rpc = ArchiveRPC(chain)
        if not rpc.available():
            return None
        # strict=True: a failed RPC read returns None (UNKNOWN), never a smaller
        # total — so a flaky node can't manufacture a phantom 庄在卖 / 已减仓.
        return combined_balance_at(token, wallets, chain, rpc.latest_block(),
                                   rpc=rpc, strict=True)
    except Exception as e:
        logger.debug("sentinel_balance_failed", token=token, error=str(e))
        return None


_MORALIS_EVM = {"bsc": "bsc", "ethereum": "eth", "base": "base",
                "arbitrum": "arbitrum", "optimism": "optimism", "polygon": "polygon"}


def _classify_outflow(token: str, chain: str, wallets: list[str]) -> str:
    """On a detected cluster DROP, where did the tokens go? (#2: tell a real SELL
    from an internal move.) Tokens flowing to the LP pair / router / a CEX =
    'sell'; to a plain EOA = 'internal' (possibly just reshuffling). Cheap: only
    called when a drop fires. Samples a few wallets' recent token transfers."""
    from src.onchain import moralis_client
    mchain = _MORALIS_EVM.get(chain)
    if not moralis_client.available() or not mchain:
        return "?"
    # Sell venues: the token's LP pairs (DexScreener) — selling sends token there.
    venues = set()
    try:
        import urllib.request
        u = f"https://api.dexscreener.com/token-pairs/v1/{chain}/{token}"
        req = urllib.request.Request(u, headers={"User-Agent": "CryptoScope/1.0"})
        with urllib.request.urlopen(req, timeout=10) as r:
            pairs = json.loads(r.read().decode())
        pairs = pairs if isinstance(pairs, list) else pairs.get("pairs", [])
        for p in pairs:
            if p.get("pairAddress"):
                venues.add(p["pairAddress"].lower())
    except Exception:
        pass
    sells = internals = 0
    for w in wallets[:5]:
        data = moralis_client.get(
            f"{w}/erc20/transfers?chain={mchain}&contract_addresses%5B0%5D={token}&order=DESC&limit=8")
        for tx in (data or {}).get("result", []):
            if (tx.get("from_address") or "").lower() != w.lower():
                continue
            to = (tx.get("to_address") or "").lower()
            lbl = (tx.get("to_address_label") or "").lower()
            if to in venues or any(x in lbl for x in ("pancake", "router", "swap",
                                                       "binance", "okx", "gate", "mexc", "dex")):
                sells += 1
            else:
                internals += 1
    if sells == 0 and internals == 0:
        return "?"
    return "sell" if sells >= internals else "internal"


def _seattle(iso: str) -> str:
    """UTC ISO timestamp → Seattle (Pacific) clock string for order confirmation."""
    try:
        from datetime import datetime
        from zoneinfo import ZoneInfo
        t = datetime.fromisoformat(iso.replace("Z", "+00:00")).astimezone(ZoneInfo("America/Los_Angeles"))
        return t.strftime("%m-%d %H:%M:%S PDT")
    except Exception:
        return iso[:19]


def _solana_cluster_net_flow(token: str, wallets: list[str], since_iso: str | None,
                             max_wallets: int = 8, timeout: int = 15) -> dict | None:
    """Solana operator buy/sell via TRANSFERS (ground truth, balanceOf-free) since
    `since_iso`. Method: find each cluster wallet's token account (ATA) for the mint,
    pull its recent signatures (the ATA is always an account key when its balance
    changes — the owner often isn't, so we key on the ATA to not miss inbound buys),
    then read each tx's pre/postTokenBalances and sum the cluster's net token delta
    PER TRANSACTION. Cluster↔cluster internal moves net to zero across the cluster, so
    a positive tx-net is a real external BUY and a negative one a real SELL. Helius/RPC
    only — no Moralis, no balanceOf. Returns net + latest order time (matches EVM)."""
    import os
    from datetime import datetime, timezone
    rpc = os.environ.get("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")

    def _call(method, params):
        req = urllib.request.Request(
            rpc, data=json.dumps({"jsonrpc": "2.0", "id": 1, "method": method,
                                  "params": params}).encode(),
            headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode()).get("result")

    since_ts = None
    if since_iso:
        try:
            since_ts = datetime.fromisoformat(since_iso.replace("Z", "+00:00")).timestamp()
        except Exception:
            since_ts = None
    wl = set(wallets)
    # 1. resolve each owner's ATA(s) for the mint, collect their signatures in window.
    sig_time: dict[str, int] = {}
    try:
        for w in list(wl)[:max_wallets]:
            accs = (_call("getTokenAccountsByOwner",
                          [w, {"mint": token}, {"encoding": "jsonParsed"}]) or {}).get("value", [])
            for acc in accs:
                ata = acc.get("pubkey")
                if not ata:
                    continue
                for s in (_call("getSignaturesForAddress", [ata, {"limit": 40}]) or []):
                    bt = s.get("blockTime")
                    if bt is None or (since_ts and bt <= since_ts):
                        continue
                    sig_time[s["signature"]] = bt
    except Exception as e:
        logger.debug("sol_netflow_sigs_failed", token=token, error=str(e))
        return None
    if not sig_time:
        return {"buy": 0.0, "sell": 0.0, "net": 0.0, "last_buy_ts": None,
                "last_sell_ts": None, "latest_ts": since_iso or ""}

    def _cluster_amt(bals):
        m: dict[str, float] = {}
        for b in bals or []:
            if b.get("mint") != token:
                continue
            owner = b.get("owner")
            if owner in wl:
                m[owner] = m.get(owner, 0.0) + float((b.get("uiTokenAmount") or {}).get("uiAmount") or 0)
        return m

    buy = sell = 0.0
    last_buy_bt = last_sell_bt = None
    latest_bt = 0
    # 2. per unique tx (newest 60), the cluster's net token delta = external flow.
    for sig, bt in sorted(sig_time.items(), key=lambda kv: kv[1])[-60:]:
        try:
            tx = _call("getTransaction",
                       [sig, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}])
        except Exception:
            continue
        if not tx:
            continue
        meta = tx.get("meta") or {}
        pm = _cluster_amt(meta.get("preTokenBalances"))
        qm = _cluster_amt(meta.get("postTokenBalances"))
        tx_net = sum(qm.get(o, 0.0) - pm.get(o, 0.0) for o in (set(pm) | set(qm)))
        if abs(tx_net) < 1e-9:
            continue
        latest_bt = max(latest_bt, bt)
        if tx_net > 0:
            buy += tx_net
            last_buy_bt = bt if last_buy_bt is None else max(last_buy_bt, bt)
        else:
            sell += -tx_net
            last_sell_bt = bt if last_sell_bt is None else max(last_sell_bt, bt)

    def _iso(bt):
        return datetime.fromtimestamp(bt, timezone.utc).isoformat() if bt else None
    return {"buy": buy, "sell": sell, "net": buy - sell,
            "last_buy_ts": _iso(last_buy_bt), "last_sell_ts": _iso(last_sell_bt),
            "latest_ts": _iso(latest_bt) or (since_iso or "")}


def _evm_cluster_net_flow_rpc(token: str, chain: str, wallets: list[str],
                              since_iso: str | None) -> dict | None:
    """EVM operator buy/sell via direct RPC eth_getLogs — the KEYLESS, Moralis-free
    path (Moralis free tier exhausts daily and blinds the BSC sentinels). Pull the
    token's Transfer logs since `since_iso`, filter cluster wallets (external→cluster=
    buy, cluster→external=sell), net them. Ground truth, same as Moralis but no quota."""
    from datetime import datetime, timezone
    try:
        from src.onchain.evm_archive import ArchiveRPC
        rpc = ArchiveRPC(chain)
        head = rpc.logs_head()
        # since_iso → fromBlock, using LIVE-measured block time (BSC dropped to
        # ~0.45s; a hardcoded 3s landed fromBlock far in the past and under-covered
        # the window → missed recent transfers / false-zero flow). Cap by TIME (~2d)
        # not a fixed block count, so the cap means the same window on any chain.
        secs = rpc.seconds_per_block()
        if since_iso:
            since_ts = datetime.fromisoformat(since_iso.replace("Z", "+00:00")).timestamp()
            now = datetime.now(timezone.utc).timestamp()
            blocks_ago = int((now - since_ts) / secs) + 50
        else:
            blocks_ago = int(1800 / secs)              # ~30min default
        cap = int(2 * 86400 / secs)                    # never scan more than ~2 days back
        blocks_ago = max(50, min(blocks_ago, cap))
        from_block = head - blocks_ago
        decimals = rpc.token_decimals(token)
        scale = float(10 ** decimals)
        wl = {w.lower() for w in wallets}
        logs = rpc.get_transfer_logs(token, from_block, head)
        if not rpc.logs_complete:
            # A chunk failed — the logs are partial. Returning buy=sell=0 here would
            # be a false "operator idle / no flow" (the exact mistake that read RPC
            # outages as "no transfers"). Return None = UNKNOWN; caller won't alert.
            logger.debug("netflow_incomplete_logs", token=token, chain=chain)
            return None
        buy = sell = 0.0
        last_buy_blk = last_sell_blk = None
        for lg in logs:
            t = lg.get("topics", [])
            if len(t) < 3:
                continue
            frm = "0x" + t[1][-40:].lower()
            to = "0x" + t[2][-40:].lower()
            try:
                amt = int(lg.get("data", "0x0"), 16) / scale
            except ValueError:
                continue
            blk = int(lg.get("blockNumber", "0x0"), 16)
            if to in wl and frm not in wl:
                buy += amt
                last_buy_blk = blk if last_buy_blk is None else max(last_buy_blk, blk)
            elif frm in wl and to not in wl:
                sell += amt
                last_sell_blk = blk if last_sell_blk is None else max(last_sell_blk, blk)

        def _iso(blk):
            if not blk:
                return None
            ts = rpc.block_time(blk)
            return datetime.fromtimestamp(ts, timezone.utc).isoformat() if ts else None
        last_buy = _iso(last_buy_blk)
        last_sell = _iso(last_sell_blk)
        latest = max([x for x in (last_buy, last_sell) if x], default=since_iso or "")
        return {"buy": buy, "sell": sell, "net": buy - sell,
                "last_buy_ts": last_buy, "last_sell_ts": last_sell, "latest_ts": latest}
    except Exception as e:
        logger.debug("evm_rpc_netflow_failed", token=token, chain=chain, error=str(e)[:80])
        return None


def cluster_net_flow(token: str, chain: str, wallets: list[str], since_iso: str | None,
                     max_wallets: int = 8) -> dict | None:
    """Operator buy/sell via TRANSFERS (ground truth) since `since_iso` — reliable
    even for reflection/wash tokens where balanceOf lies (EVAA). External→cluster =
    buy; cluster→external = sell. Returns net + the latest order's Seattle time.
    EVM via Moralis (bounded wallets), with a KEYLESS RPC eth_getLogs fallback when
    Moralis is unavailable (free tier exhausts daily — the fallback keeps the BSC
    sentinels seeing real flow). Solana via Helius/RPC token-balance deltas. All
    balanceOf-free — the root-cure principle."""
    if chain in ("solana", "sol"):
        return _solana_cluster_net_flow(token, wallets, since_iso, max_wallets)
    from src.onchain import moralis_client
    if not moralis_client.usable():
        # Moralis quota exhausted / all keys parked → keyless RPC keeps detection alive.
        return _evm_cluster_net_flow_rpc(token, chain, wallets, since_iso)
    from src.onchain import moralis_client
    mchain = _MORALIS_EVM.get(chain)
    if not moralis_client.available() or not mchain:
        return None
    wl = {w.lower() for w in wallets}
    buy = sell = 0.0
    last_buy = last_sell = None
    latest_ts = since_iso or ""
    for w in list(wl)[:max_wallets]:
        d = moralis_client.get(
            f"{w}/erc20/transfers?chain={mchain}&contract_addresses%5B0%5D={token}&order=DESC&limit=25")
        for r in (d or {}).get("result", []):
            ts = r.get("block_timestamp", "")
            if since_iso and ts <= since_iso:
                break  # DESC → older than last seen, stop this wallet
            if ts > latest_ts:
                latest_ts = ts
            to = (r.get("to_address") or "").lower()
            frm = (r.get("from_address") or "").lower()
            val = float(r.get("value_decimal") or 0)
            if to in wl and frm not in wl:        # external → cluster = buy
                buy += val
                if last_buy is None or ts > last_buy:
                    last_buy = ts
            elif frm in wl and to not in wl:       # cluster → external = sell
                sell += val
                if last_sell is None or ts > last_sell:
                    last_sell = ts
    return {"buy": buy, "sell": sell, "net": buy - sell,
            "last_buy_ts": last_buy, "last_sell_ts": last_sell, "latest_ts": latest_ts}


def _balanceof_reliable(token: str, chain: str, wallets: list[str]) -> bool:
    """Registration health check for the ACTUAL failure mode: balanceOf READS that
    disagree cycle-to-cycle (rotating free RPCs returning different values, or
    per-block reflection) — that's what spammed phantom SIREN sells. Read the
    cluster balance at the SAME block TWICE (rotates RPCs); if the two reads differ
    materially, balanceOf is unstable for this token → flag so balanceOf-based
    assessments are caveated. Detection already uses transfers regardless."""
    if chain in ("solana", "sol"):
        return True
    try:
        from src.onchain.evm_archive import ArchiveRPC, combined_balance_at
        rpc = ArchiveRPC(chain)
        if not rpc.available():
            return True
        blk = rpc.latest_block()
        subset = wallets[:6]
        r1 = combined_balance_at(token, subset, chain, blk, rpc=ArchiveRPC(chain))
        r2 = combined_balance_at(token, subset, chain, blk, rpc=ArchiveRPC(chain))
        if r1 <= 0 and r2 <= 0:
            return True
        ref = max(r1, r2, 1)
        return abs(r1 - r2) / ref <= 0.02   # same block, same value expected; drift = unstable
    except Exception:
        return True


def _lp_float(token: str, chain: str, pair: str | None) -> float | None:
    """The deepest LP's token balance = the tradable float. For a high-control operator
    that marks up by RESTRICTING float (MAME class), a shrinking float is the ignition
    tell that cluster-balance watching misses. EVM only (archive balanceOf); None if
    undeterminable."""
    if not pair or chain in ("solana", "sol"):
        return None
    try:
        from src.onchain.evm_archive import ArchiveRPC
        return ArchiveRPC(chain).balance_of(token, pair, "latest")
    except Exception:
        return None


def _measure(token: str, chain: str, wallets: list[str], symbol: str = "") -> dict:
    m = _dex(token, chain)
    m["cluster_balance"] = _cluster_balance(token, chain, wallets)
    m["float_lp"] = _lp_float(token, chain, m.get("pair"))
    m["funding"] = _funding_rate(symbol)
    return m


_SANITY_UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")


def _token_age_days(token: str, chain: str) -> float | None:
    """Days since the token's oldest DEX pair (DexScreener, keyless). Age-blindness was
    the MAME root cause; surfacing age at the registration boundary lets a too-young
    target be flagged before its (unmeasurable) operator-history signals fool us."""
    import json
    import time
    import urllib.request
    ds = {"bsc": "bsc", "ethereum": "ethereum", "eth": "ethereum", "base": "base",
          "solana": "solana", "sol": "solana", "arbitrum": "arbitrum",
          "optimism": "optimism", "polygon": "polygon"}.get(chain, chain)
    try:
        req = urllib.request.Request(
            f"https://api.dexscreener.com/latest/dex/tokens/{token}",
            headers={"User-Agent": _SANITY_UA})
        with urllib.request.urlopen(req, timeout=20) as r:
            pairs = (json.loads(r.read().decode()) or {}).get("pairs") or []
        created = [p.get("pairCreatedAt") for p in pairs
                   if p.get("pairCreatedAt") and p.get("chainId") == ds]
        created = created or [p.get("pairCreatedAt") for p in pairs if p.get("pairCreatedAt")]
        if not created:
            return None
        return max(0.0, (time.time() - min(created) / 1000.0) / 86400.0)
    except Exception:
        return None


def _registration_sanity(token: str, chain: str, wallets: list[str],
                         funder: str | None = None) -> dict:
    """Cheap pre-registration 'health panel' — the boundary guard MAME bypassed. hunt()
    already gates discovery on token age (>=14d) and funder fan-out (disperser), but
    register() ran NONE of it, so a hand-picked 4-day disperser-funded serial-degen play
    became the 'highest-conviction' sentinel. We CAVEAT, not block — a fresh token can
    still be a real play; it just must not borrow an operator-history edge it can't have.
    Returns the panel + human-readable caveats stored on the sentinel record."""
    caveats: list[str] = []
    age = _token_age_days(token, chain)
    if age is not None and age < 14:
        caveats.append(f"⚠️币龄仅~{age:.0f}天: 派发履历/隐藏簇等历史型信号在此不可测,"
                       "信心须来自实时流/集中度而非operator履历")
    fanout = None
    if funder:
        try:
            from src.pipeline.operator_hunt import _funder_fanout
            fanout = _funder_fanout(funder, chain)
        except Exception:
            fanout = None
        if fanout is not None and fanout > 40:
            caveats.append(f"⚠️funder喂了{fanout}+地址: 是分发器/launchpad,'专属funder=真庄'不成立")
    # Wallet behaviour: a disciplined single-bag operator trades few tokens; a wallet
    # born on launch day spraying 15+ micro-memes is the MAME serial-degen profile.
    degen = None
    try:
        from src.onchain import moralis_client
        mc = {"bsc": "bsc", "ethereum": "eth", "base": "base", "arbitrum": "arbitrum",
              "optimism": "optimism", "polygon": "polygon"}.get(chain)
        if mc and moralis_client.usable():
            spray = 0
            for w in wallets[:4]:
                d = moralis_client.get(f"{w}/erc20/transfers?chain={mc}&limit=100")
                toks = {x.get("token_symbol") for x in (d or {}).get("result", [])
                        if x.get("token_symbol")}
                if len(toks) >= 15:
                    spray += 1
            degen = spray
            if spray:
                caveats.append(f"⚠️{spray}个钱包近100笔刷>=15种代币: serial-degen特征,非纪律单仓operator")
    except Exception:
        degen = None
    # Entity composition: a cluster that is mostly multisig/treasury/contract is
    # TEAM/CUSTODY control (ESPORTS: owner Safe + treasury), not a trading operator
    # you can "follow"; flag it so its concentration isn't read as an operator setup.
    cluster_type = None
    try:
        from src.onchain.entity_classify import classify_cluster
        cc = classify_cluster(wallets, chain)
        cluster_type = cc["summary"]
        if cc["eoa_share_of_members"] < 0.34 and cc["counts"]:
            caveats.append(f"⚠️簇构成 {cc['summary']}: 多为多签/金库/合约=团队/托管控制,"
                           "非交易型operator,集中度≠坐庄,出货=团队解锁/内幕而非拉砸")
    except Exception:
        cluster_type = None
    # Solana: a shared feePayer / same Jito-bundle across the cluster wallets is a
    # high-confidence "one entity" CONFIRMATION (the cleanest Solana cluster edge).
    bundle_edge = None
    if chain in ("solana", "sol"):
        try:
            from src.onchain.solana_bundle import shared_fee_payer
            edges = shared_fee_payer(wallets)
            if edges:
                bundle_edge = edges[0].get("edge_type")
                caveats.append(f"✓Solana簇确认:{len(edges)}条同feePayer/bundle边 → 同一实体(高置信)")
        except Exception:
            bundle_edge = None
    # Off-chain identity: a real protocol (site/socials/age) behaves differently from
    # an anonymous meme — context the operator view lacked.
    identity = None
    try:
        from src.onchain.token_identity import token_identity
        idn = token_identity(token, chain)
        identity = idn.get("profile")
        if idn.get("profile") == "anon_meme":
            caveats.append("⚠️匿名meme(无官网/社交、极新): 纯操盘/赌狗盘,叙事支撑为0")
    except Exception:
        identity = None
    # Near-term catalyst: an imminent unlock is a DUMP catalyst (the ESPORTS/KuCoin
    # class) — surface it so a "loaded operator" isn't read bullishly into an unlock.
    catalyst = None
    try:
        from src.onchain.catalyst_feed import catalyst_for
        cat = catalyst_for(token, chain)   # contract-address path (symbol optional)
        if cat.get("has_catalyst"):
            catalyst = cat.get("detail")
            caveats.append(f"⚠️临近催化剂({cat.get('window')}): {cat.get('detail')} → 解锁/上市=砸盘窗口")
    except Exception:
        catalyst = None
    return {"token_age_days": round(age, 1) if age is not None else None,
            "funder_fanout": fanout, "degen_wallets": degen,
            "cluster_type": cluster_type, "identity": identity, "catalyst": catalyst,
            "bundle_edge": bundle_edge, "caveats": caveats}


def register(token: str, chain: str, symbol: str, wallets: list[str],
             funder: str | None = None) -> dict:
    """Snapshot the current state as the baseline and start watching.

    Runs a cheap sanity panel (token age / funder fan-out / wallet profile) at the
    registration boundary and stores its caveats on the record. Caveats DON'T block
    (a fresh token can be a real play) but ensure a young / disperser-funded / serial-
    degen target can never again masquerade as a proven operator — the MAME post-mortem."""
    data = _load()
    key = f"{chain}:{token.lower()}"
    state = _measure(token, chain, wallets, symbol)
    reliable = _balanceof_reliable(token, chain, wallets)
    sanity = _registration_sanity(token, chain, wallets, funder=funder)
    # EVM addresses are case-insensitive (lowercase for consistent matching); Solana
    # addresses are base58 and CASE-SENSITIVE — lowercasing them corrupts the cluster.
    is_sol = chain in ("solana", "sol")
    norm_wallets = list(wallets) if is_sol else [w.lower() for w in wallets]
    data[key] = {
        "token": token, "chain": chain, "symbol": symbol,
        "wallets": norm_wallets,
        "baseline": state, "last": state,
        "balanceof_reliable": reliable,
        "sanity": sanity,
    }
    _save(data)
    if not reliable:
        logger.warning("balanceof_unreliable", symbol=symbol,
                       note="reflection/fee token — balanceOf-based assessments caveated")
    for c in sanity["caveats"]:
        logger.warning("sentinel_registration_caveat", symbol=symbol, note=c)
    logger.info("sentinel_registered", symbol=symbol, chain=chain,
                cluster_balance=state.get("cluster_balance"), liquidity=state.get("liquidity"),
                caveats=len(sanity["caveats"]))
    return data[key]


def check_run(use_transfers: bool = False) -> list[dict]:
    """One monitoring pass over all registered targets. Returns fired alerts and
    persists updated last-seen state. Network I/O runs OUTSIDE the lock (#7).

    use_transfers=True (5-min scheduler): detect buy/sell via TRANSFERS — reliable
    even for reflection/wash tokens where balanceOf lies (EVAA). False (20s watcher):
    cheap balanceOf + price only (fast, free, no Moralis)."""
    targets = _load()
    measured = {k: _measure(t["token"], t["chain"], t["wallets"], t.get("symbol", ""))
                for k, t in targets.items()}
    flows = {}
    cex_flows: dict = {}
    if use_transfers:
        flows = {k: cluster_net_flow(t["token"], t["chain"], t["wallets"],
                                     (t.get("last", {}) or {}).get("flow_ts"))
                 for k, t in targets.items()}
        # CEX deposit-flow = the #1 LEADING dump signal: an operator cluster sending
        # tokens to an exchange deposit address precedes the sell by minutes-hours.
        # Only on the 5-min transfer pass (it's a getLogs scan). Pass the measured
        # cluster_balance so it doesn't re-fetch.
        try:
            from src.onchain.cex_flow import cex_outflow_signal
            cex_flows = {k: cex_outflow_signal(
                t["token"], t["chain"], t["wallets"],
                (t.get("last", {}) or {}).get("flow_ts"),
                cluster_balance=(measured.get(k) or {}).get("cluster_balance"))
                for k, t in targets.items()}
        except Exception as e:
            logger.debug("cex_flow_pass_failed", error=str(e)[:80])
    with _state_lock():
        data = _load()  # re-read under lock (another process may have updated)
        alerts = []
        # Per-target isolation: one token raising must NOT abort the whole pass and
        # blind every other sentinel. The body lives in a nested fn (same indent, so
        # it's unchanged — continue→return); the loop below runs it under try/except.
        def _check_one(key, t):
            cur = measured.get(key)
            if cur is None:
                return
            last, base = t.get("last", {}), t.get("baseline", {})
            fired = []

            # CEX deposit-flow — LEADING dump signal (operator → exchange deposit
            # precedes the sell). Only when the scan completed (incomplete = unknown).
            cxf = cex_flows.get(key)
            if cxf and cxf.get("has_signal") and cxf.get("complete"):
                fired.append(("CEX充值", f"操盘簇向交易所充值 {cxf.get('cex_outflow', 0):,.0f}"
                              f"(持仓{cxf.get('pct_of_cluster', 0):.0f}%) → 即将砸盘,逃命/做空"))

            cpr = cur.get("price")   # current price — used by the momentum/动能熄火 block
            cb, pb = cur.get("cluster_balance"), last.get("cluster_balance")
            flow = flows.get(key)
            if flow is not None:
                # ===== RELIABLE: buy/sell via TRANSFERS (ground truth) + Seattle time =====
                prev_flow_ts = last.get("flow_ts")
                t["flow_ts"] = flow["latest_ts"] or prev_flow_ts  # advance incremental cursor
                if prev_flow_ts is None:
                    flow = None  # first transfer check = establish baseline, don't alert
            if flow is not None:
                # MAGNITUDE GATE: only a net move that is MEANINGFUL vs the cluster's
                # holdings counts as 庄在买/庄在卖. Without this, a trivial net flow
                # (SIREN: 1,882 sold out of 90M held = 0.002%) flips the phase and fires
                # a phantom 庄在卖 — the exact spam we rooted out, reborn via transfers.
                # Threshold = OP_SELL/OP_BUY × current cluster balance (same fractions as
                # the legacy balance-delta path). cb unknown → can't size it → no alert.
                net_sell = flow["sell"] - flow["buy"]
                net_buy = flow["buy"] - flow["sell"]
                sell_min = OP_SELL * cb if cb else None
                buy_min = OP_BUY * cb if cb else None
                if sell_min and net_sell >= sell_min:
                    when = _seattle(flow["last_sell_ts"]) if flow["last_sell_ts"] else "?"
                    pct = net_sell / cb * 100
                    fired.append(("庄在卖", f"转账实测 净卖 {net_sell:,.0f} (持仓{pct:.1f}%) "
                                  f"(卖{flow['sell']:,.0f}/买{flow['buy']:,.0f}) · 最后卖单 {when}"))
                elif buy_min and net_buy >= buy_min:
                    when = _seattle(flow["last_buy_ts"]) if flow["last_buy_ts"] else "?"
                    pct = net_buy / cb * 100
                    fired.append(("庄在买", f"转账实测 净买 {net_buy:,.0f} (持仓{pct:.1f}%) "
                                  f"(买{flow['buy']:,.0f}/卖{flow['sell']:,.0f}) · 最后买单 {when}"))
            # NOTE: buy/sell is detected ONLY via transfers (use_transfers=True, the
            # 5-min scheduler). balanceOf is unreliable on reflection/wash tokens
            # (EVAA, SIREN) — it drifts/oscillates and spammed phantom 庄在卖 every
            # 20s cycle. The fast watcher therefore does PRICE moves only (below).

            # ===== SLOW-BLEED: cumulative distribution vs BASELINE. The per-window
            # magnitude gate above (1.5% per tick) is evadable by dribbling out below
            # it forever — SKYAI audit found -0.26%/12h in 35 small transfers, invisible
            # to every existing gate. This fires each time the cluster is another
            # BLEED_STEP% below baseline. First evaluation ARMS at the current drop
            # (no alert) so pre-existing, already-reported distribution (SIREN -24%)
            # doesn't rehash as news. Gated on balanceof_reliable — reflection drift
            # must not manufacture a phantom bleed. =====
            bb = base.get("cluster_balance")
            if (cb is not None and bb and bb > 0 and t.get("balanceof_reliable", True)
                    and "庄在卖" not in {k for k, _ in fired}):
                drop_pct = (bb - cb) / bb * 100
                armed = t.get("bleed_alerted_pct")
                if armed is None:
                    t["bleed_alerted_pct"] = max(0.0, drop_pct)   # arm, don't alert
                elif drop_pct >= armed + BLEED_STEP:
                    fired.append(("慢滴漏", f"簇持仓较基线 -{drop_pct:.1f}% "
                                  f"({bb:,.0f}→{cb:,.0f}) — 单窗口量级门下的缓慢派发,"
                                  "累计已显著 → 庄在阴跌出货"))
                    t["bleed_alerted_pct"] = drop_pct

            # ===== BACKSTOP: violent price/liquidity moves (if sampling lagged) =====
            cl, pl = cur.get("liquidity"), last.get("liquidity")
            if cl is not None and pl and pl > 0 and cl < pl * (1 - RUG_DROP):
                drop = (pl - cl) / pl * 100
                fired.append(("RUG", f"流动性 -{drop:.0f}% (${pl:,.0f}→${cl:,.0f}) 疑似抽池 → 逃命"))

            # ===== 控浮筹型点火 (the MAME class) — operator-attributable, so it fits
            # the "only 庄 info" rule. A high-control operator marks up by tightening the
            # float, NOT by buying, so 庄在买 never fires; these catch that. Both are
            # GATED on the cluster NOT being the seller (cb >= pb), so a retail-driven
            # move can't trip them. Not a rug (RUG already handled above). =====
            not_selling = (cb is not None and pb is not None and cb >= pb * 0.99)
            fl, pf = cur.get("float_lp"), last.get("float_lp")
            if not_selling and fl is not None and pf and pf > 0 \
                    and fl < pf * (1 - FLOAT_TIGHTEN) and not (cl and pl and cl < pl * (1 - RUG_DROP)):
                tg = (pf - fl) / pf * 100
                fired.append(("浮筹收紧", f"可交易浮筹 -{tg:.0f}% ({pf:,.0f}→{fl:,.0f}枚) 且庄未卖 "
                              "→ 操盘方在锁浮筹/吸走流通 = 拉升前兆,埋伏"))
            # Controlled breakout: price clears its running peak while the cluster holds.
            peak = last.get("price_peak") or base.get("price") or 0
            if not_selling and cpr and peak and cpr > peak * (1 + BREAKOUT_UP):
                fired.append(("控盘突破", f"价突破前高 +{(cpr/peak-1)*100:.0f}% (${peak:.5f}→${cpr:.5f}) "
                              "且庄满仓未卖 → 控盘式拉升点火,做多"))
            t["price_peak"] = max(peak, cpr or 0)   # advance running peak

            # PRICE/volume signals are otherwise OFF by user directive — "only 庄 info".
            # The system alerts on the operator's ACTIONS (庄在买/庄在卖/庄停手) + RUG +
            # the controlled-float ignition above (operator-attributable on a token whose
            # float the cluster controls). PURE retail price moves (砸盘/急跌/放量 with no
            # operator action) still do NOT alert — EVAA's -35% retail bleed stays silent.

            # ===== STALL: momentum fizzled (proactively report inaction) =====
            # After a buy/launch, if price faded from its post-buy high AND the
            # operator stopped adding, the move likely failed — say so (the 3rd
            # scenario the user wanted; silence shouldn't be the only signal).
            from datetime import datetime, timezone
            nowdt = datetime.now(timezone.utc)
            fk = {k for k, _ in fired}
            # ===== 庄停手: operator WAS buying, now idle — EARLY warning, fires
            # BEFORE price fades (the gap the user hit: stop preceded the drop). =====
            if "庄在买" in fk:
                bt = (t.get("buy_times", []) or [])
                bt.append(nowdt.isoformat())
                t["buy_times"] = bt[-6:]            # keep last 6 to gauge cadence
                t["last_buy_ts"] = nowdt.isoformat()
                t["stop_alerted"] = False
            elif t.get("last_buy_ts") and not t.get("stop_alerted"):
                bt = t.get("buy_times", []) or []
                if len(bt) >= 2:                    # need a rhythm to judge "stopped"
                    try:
                        tt = sorted(datetime.fromisoformat(x) for x in bt)
                        gaps = sorted((tt[i+1]-tt[i]).total_seconds() for i in range(len(tt)-1))
                        med = gaps[len(gaps)//2]                       # median buy interval
                        thresh = min(max(med * STOP_K, STOP_MIN_S), STOP_MAX_S)
                        since = (nowdt - datetime.fromisoformat(t["last_buy_ts"])).total_seconds()
                    except Exception:
                        since = thresh = 0
                    if since >= thresh > 0:
                        fired.append(("庄停手", f"停止加仓 {since/60:.0f}分钟(常态约每{med/60:.0f}"
                                      f"分钟买一次,超{thresh/60:.0f}分钟=停)→ 失去买盘支撑,减/观望"))
                        t["stop_alerted"] = True
            mom = t.get("momentum")
            if fk & {"庄在买", "拉升"}:
                t["momentum"] = {"ts": nowdt.isoformat(), "high": cpr or 0}
            elif mom and cpr:
                mom["high"] = max(mom.get("high", 0) or 0, cpr)
                try:
                    age_h = (nowdt - datetime.fromisoformat(mom["ts"])).total_seconds() / 3600
                except Exception:
                    age_h = 0
                bought = cb is not None and pb and cb > pb
                faded = mom["high"] > 0 and cpr <= mom["high"] * (1 - STALL_FADE)
                if faded and not bought and not (fk & {"庄在卖", "阴跌出货", "砸盘", "RUG"}):
                    fired.append(("动能熄火", f"反弹乏力:价较近高 {(cpr/mom['high']-1)*100:+.0f}%,"
                                  f"庄停手未续买 → 二波可能黄,多单减/观望"))
                    t.pop("momentum", None)
                elif age_h > MAX_MOMENTUM_H:
                    t.pop("momentum", None)  # expire silently

            # PHASE-CHANGE dedup (not a time cooldown): alert only when the
            # operator's BEHAVIOR changes — start buying / stop / flip to selling /
            # crash. A persisting same state does NOT re-send the identical message
            # (fixes 'keeps sending the same thing'). A real new action always fires
            # immediately (no time delay).
            if fired:
                kset = {k for k, _ in fired}
                phase = ("sell" if kset & {"庄在卖", "阴跌出货", "RUG", "CEX充值"} else
                         "buy" if kset & {"庄在买", "控盘突破", "浮筹收紧"} else
                         "stall" if kset & {"庄停手", "动能熄火"} else "other")
                if phase == t.get("last_phase"):
                    fired = []           # same phase as last alert → suppress repeat
                else:
                    t["last_phase"] = phase

            if fired:
                fund = cur.get("funding")
                kinds = {k for k, _ in fired}
                fstr = f"(费率 {fund:+.3f}%)" if fund is not None else ""
                if kinds & {"庄在卖", "阴跌出货", "砸盘", "RUG", "CEX充值"}:    # operator exiting / dump
                    action = "🔴 顶部跑 / 做空"
                    if fund is not None and fund > 0.03:
                        fstr = f"(费率 +{fund:.3f}% 多头拥挤,做空顺风)"
                    action += fstr
                elif kinds & {"庄在买", "拉升", "控盘突破", "浮筹收紧"}:   # operator marking up / launch
                    sl = t.get("second_leg", "")
                    action = "🟢🟢 二波启动!最高优先 做多" if "二波候选" in sl \
                        else "🟢 埋伏 / 做多(跟庄)"
                    if fund is not None and fund > 0.08:
                        fstr = f"(费率 +{fund:.3f}% 已过热,小心追高)"
                    action += fstr
                    # n=2 lesson: a believer cluster (never sells) is a weak follow.
                    ot = t.get("operator_type", "")
                    if "信仰者" in ot:
                        action += " ⚠️此簇只吸不卖(信仰者,跟了可能也套)"
                    elif "聪明庄" in ot:
                        action += " ✅此簇有派发履历(会套现)"
                elif "庄停手" in kinds:                    # operator went idle (early)
                    action = "⚪→🔴 庄停手,失去买盘支撑 → 减/观望(早期预警)"
                elif "动能熄火" in kinds:                  # rally fizzled, operator idle
                    action = "⚪→🔴 动能熄火,减多/观望(二波未成)"
                else:
                    action = f"⚪ 异动留意 {fstr}"
                alerts.append({"symbol": t["symbol"], "chain": t["chain"],
                               "token": t["token"], "events": fired,
                               "funding": fund, "action": action,
                               "liquidity": cur.get("liquidity")})
                try:
                    from src.pipeline.outcome_tracker import log_alert
                    direction = "short" if "做空" in action or "跑" in action else \
                                "long" if "做多" in action else "none"
                    log_alert(t["token"], t["chain"], t["symbol"],
                              ",".join(sorted(kinds)), direction,
                              cur.get("price") or 0, cur.get("liquidity") or 0)
                except Exception:
                    pass
            # Advance state, never overwriting a good value with None (#hardening).
            merged = dict(last)
            for k, v in cur.items():
                if v is not None:
                    merged[k] = v
            t["last"] = merged

        for key, t in data.items():
            try:
                _check_one(key, t)
            except Exception as e:
                logger.warning("sentinel_target_failed",
                               symbol=(t or {}).get("symbol", key), error=str(e)[:100])
        _save(data)
    return alerts


def _format(alerts: list[dict]) -> str:
    lines = ["🚨 <b>操作者哨兵告警</b>", "━━━━━━━━━━"]
    for a in alerts:
        lines.append(f"\n<b>{a['symbol']}</b> [{a['chain']}]")
        for kind, detail in a["events"]:
            lines.append(f"  ⚠️ <b>{kind}</b>: {detail}")
        if a.get("action"):
            lines.append(f"  👉 <b>{a['action']}</b>")
        if a.get("liquidity"):
            from src.pipeline.slippage import max_size_for_impact, price_impact
            lq = a["liquidity"]
            lines.append(f"  💧 2%滑点内≤${max_size_for_impact(lq,2.0):,.0f} · "
                         f"$5k单冲击≈{price_impact(lq,5000):.1f}%")
        lines.append(f"  <code>{a['token']}</code>")
    lines.append("\n<i>带止损,薄盘小仓。仅信号,非投资建议。</i>")
    return "\n".join(lines)


async def run_and_alert(use_transfers: bool = False) -> int:
    """Scheduler entry: check + push Telegram on any trigger. Returns alert count."""
    alerts = check_run(use_transfers=use_transfers)
    if alerts:
        from src.distribution.telegram_sender import send_alert
        await send_alert(_format(alerts))
    logger.info("operator_sentinel_done", targets=len(_load()), alerts=len(alerts))
    return len(alerts)


# Confirmed operator clusters — see data/research/*_analysis.md.
_KNOWN_CLUSTERS = [
    # BASED (9 wallets, hidden distributed Sybil, loaded-and-waiting).
    ("0x1d28D989F9e3CCb8B15D0cec601734514f958E4D", "bsc", "BASED",
     ["0xc3526ad0a5fa2d7bac0963904036d8604b13470e", "0x78a0cddf1e0c966d505181b0dfaf505e398d053a",
      "0x3e5dcdbded6ca3d2a78eb14c307c9ed7a9638c52", "0x40c0e5f38fecacd5d7dbea41cd1c34c8917a25b0",
      "0x42a99d7dc78415ea0995edd8d5e718495a07a7c1", "0x774922fbb5a9e6d52c14fa9dfa25c16219f91c90",
      "0x4bdaa8005233f251375212efab1d2ce938d62b78", "0xc4be9808281709d47489ce1e2a5422da29e5506a",
      "0x6908518e91b83a05a51b8961d1c5667b2e9bc4a3"]),
    # ESPORTS / Yooldo Games (3 wallets, 23.9%, held through a +382%/-79% pump-dump
    # on 06-12/13 without selling — watch for second leg or distribution).
    ("0xF39e4b21c84e737Df08e2C3b32541d856f508E48", "bsc", "ESPORTS",
     ["0x99d4b3f50b14bfc67892c472f4053ee3483d87b9", "0xd2dd7b597fd2435b6db61ddf48544fd931e6869f",
      "0x504ce9e51e508c85a161058c12e970a903d482fc"]),
    # EVAA / evaa.finance (15-wallet cluster, 9.9%, non-CEX funder). Accumulated
    # +54% over 10d INTO a +34% run, then stopped ~10h ago at the top — holding,
    # not yet distributing. Real project; watch for distribution (砸盘) or 2nd leg.
    ("0xaa036928c9c0Df07d525B55ea8EE690Bb5a628C1", "bsc", "EVAA",
     ["0xd5da17a84314194e348649c89a65143a061f7190", "0x024ee8dc380ad17d955b07149725d518b5cbba67",
      "0x8782163068c7cd74d2510768a61135c1e4eb07b3", "0xe92bd58a5c0d84d4af48d8b7d28068bcb7a92f74",
      "0xbd6f608b9747e564be011960d1e9bd35541e0dbf", "0xcd808e6bb368e06810ce20cccc0209c94f8a22da",
      "0xe6451016f095835a0d5ef98a5c0092e47ddf0a93", "0x8d17fbfb03a6b7e8fdcfd60f1f9e6c08578ba5d7",
      "0x24a0d9928a3b6cd13a6210d0ff6d450a080fc266", "0x39927a709eaba03d43c351ea0b1bf4228ce99ade",
      "0xb85b098448b2aac4af96f5bdd9c6c02373a08975", "0x035ae7d933dcbfe617ffba194a88af0c2867b90c",
      "0x604ef94e24a14052cc924a55f49f879757681d4d", "0x33a5e430b626d2b6f93fd5b94159d30a636a0c4b",
      "0x4306d7db991e3eb70d0a1f10e1c92f17a987f24f"]),
    # SIREN (bsc) — pumped ~23x to $1.31 then crashed -96%. Top holders are
    # RE-ACCUMULATING at the bottom (top-15 EOAs 29.7%→37.5% over 30d) = second-leg
    # build. Tracking the 14 largest non-contract holders (ex-burn) for the relaunch.
    ("0x997A58129890bBdA032231A52eD1ddC845fc18e1", "bsc", "SIREN",
     ["0x91dca37856240e5e1906222ec79278b16420dc92", "0x0d0707963952f2fba59dd06f2b425ace40b492fe",
      "0x4982085c9e2f89f2ecb8131eca71afad896e89cb", "0x7467a1ff2f66933057776ebf8a985613904ece0b",
      "0x55dd29e6a6d7c49f331493b318b4de57f7ef1b9b", "0x5ef135decb75fa43011ce6c058307bd437e71264",
      "0x97798387a5ec55988e840e1ded03f53c3c1aa7b8", "0x7e31d70f38b9f873ace22be146385805d9c5c2b2",
      "0x4df6b3022ee486cf60f4bea2ec1abbb2d23fbaf9", "0x522fd166904453443b9ed7fb43e622acc804839e",
      "0xe2aca79c6cad337499c2588972cd5dfd667ae2e6", "0x33279e8df05fd4dcf24eccf7efbe460c8e352ce6",
      "0x3627e15706126fe66dd7bcfeb96e391298da5763", "0xc58bff59c3480f6371e573634ef269612252e05f"]),
]


def main():
    if "--register-all" in sys.argv or "--register-based" in sys.argv:
        for cluster in _KNOWN_CLUSTERS:
            s = register(*cluster)
            print(f"✅ {cluster[2]} 已注册 — 簇余额 {s['baseline'].get('cluster_balance'):,.0f}, "
                  f"流动性 ${s['baseline'].get('liquidity'):,.0f}, 价格 ${s['baseline'].get('price')}")
        return
    alerts = check_run()
    if alerts:
        print(_format(alerts).replace("<b>", "").replace("</b>", "").replace("<code>", "").replace("</code>", ""))
    else:
        targets = _load()
        print(f"哨兵巡检完成 — {len(targets)} 个目标,无触发(派发/rug/砸盘/启动均未发生)")
        for k, t in targets.items():
            lc = t["last"]
            fund = lc.get("funding")
            ftag = ("🔴极端多拥挤(空顺风)" if fund and fund > 0.1 else
                    "🔴多拥挤" if fund and fund > 0.03 else
                    "🟢空拥挤(多顺风)" if fund and fund < -0.03 else "")
            fs = f" · 费率 {fund:+.3f}%/8h{ftag}" if fund is not None else ""
            sl = t.get("second_leg", "")
            fs += f" · {sl}" if sl and sl != "—" else ""
            print(f"  {t['symbol']}: 簇 {lc.get('cluster_balance'):,.0f} · "
                  f"流动性 ${lc.get('liquidity'):,.0f} · 价 ${lc.get('price')}{fs}")


if __name__ == "__main__":
    from dotenv import load_dotenv

    from src.config import PROJECT_ROOT
    load_dotenv(PROJECT_ROOT / ".env")
    main()
