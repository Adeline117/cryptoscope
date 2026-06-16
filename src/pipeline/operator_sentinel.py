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
SLOW_BLEED = 0.04        # cluster down >=4% from running peak → chunked distribution
COOLDOWN_MIN = 45        # don't re-fire the same event kind within N minutes
STALL_FADE = 0.12        # after a buy/launch, price faded >=12% from its high...
MAX_MOMENTUM_H = 36      # ...within this window, with no fresh operator buying → 动能熄火
STOP_HOURS = 2.0         # operator was buying, then no new buy for N hours → 庄停手
                         # (EARLY warning: fires BEFORE price fades, not after)
# Price/liquidity are only a BACKSTOP — catch a violent move if balance sampling lags.
RUG_DROP = 0.30          # liquidity fell >=30% → LP pull / rug
CRASH_DROP = 0.15        # price fell >=15% vs last check → 砸盘 backstop
LAUNCH_PRICE = 0.25      # price up >=25% vs last check → launch backstop
LAUNCH_VOL = 3.0         # 24h volume >=3x baseline → volume backstop


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
        # ~90d, ~9d spacing (BSC ~28800 blocks/day)
        c = operator_curve_evm(token, wallets, chain, latest - 90 * 28800, latest,
                               n_points=10, pause=0.05)
        bs = (c or {}).get("balance_series") or []
        if len(bs) < 4:
            return {"profile": "?", "max_drawdown_pct": None}
        peak = bs[0]
        max_dd = 0.0
        for v in bs:
            if v > peak:
                peak = v
            elif peak > 0:
                max_dd = max(max_dd, (peak - v) / peak)
        dd = round(max_dd * 100, 1)
        profile = "聪明庄(有派发履历)" if dd >= 25 else "信仰者(只吸不卖)"
        return {"profile": profile, "max_drawdown_pct": dd}
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
        return combined_balance_at(token, wallets, chain, rpc.latest_block(), rpc=rpc)
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


def cluster_net_flow(token: str, chain: str, wallets: list[str], since_iso: str | None,
                     max_wallets: int = 8) -> dict | None:
    """Operator buy/sell via TRANSFERS (ground truth) since `since_iso` — reliable
    even for reflection/wash tokens where balanceOf lies (EVAA). External→cluster =
    buy; cluster→external = sell. Returns net + the latest order's Seattle time.
    EVM via Moralis (bounded wallets, recent transfers only)."""
    if chain in ("solana", "sol"):
        return None
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


def _measure(token: str, chain: str, wallets: list[str], symbol: str = "") -> dict:
    m = _dex(token, chain)
    m["cluster_balance"] = _cluster_balance(token, chain, wallets)
    m["funding"] = _funding_rate(symbol)
    return m


def register(token: str, chain: str, symbol: str, wallets: list[str]) -> dict:
    """Snapshot the current state as the baseline and start watching."""
    data = _load()
    key = f"{chain}:{token.lower()}"
    state = _measure(token, chain, wallets, symbol)
    data[key] = {
        "token": token, "chain": chain, "symbol": symbol,
        "wallets": [w.lower() for w in wallets],
        "baseline": state, "last": state,
    }
    _save(data)
    logger.info("sentinel_registered", symbol=symbol, chain=chain,
                cluster_balance=state.get("cluster_balance"), liquidity=state.get("liquidity"))
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
    if use_transfers:
        flows = {k: cluster_net_flow(t["token"], t["chain"], t["wallets"],
                                     (t.get("last", {}) or {}).get("flow_ts"))
                 for k, t in targets.items()}
    with _state_lock():
        data = _load()  # re-read under lock (another process may have updated)
        alerts = []
        for key, t in data.items():
            cur = measured.get(key)
            if cur is None:
                continue
            last, base = t.get("last", {}), t.get("baseline", {})
            fired = []

            cb, pb = cur.get("cluster_balance"), last.get("cluster_balance")
            flow = flows.get(key)
            if flow is not None:
                # ===== RELIABLE: buy/sell via TRANSFERS (ground truth) + Seattle time =====
                prev_flow_ts = last.get("flow_ts")
                t["flow_ts"] = flow["latest_ts"] or prev_flow_ts  # advance incremental cursor
                if prev_flow_ts is None:
                    flow = None  # first transfer check = establish baseline, don't alert
            if flow is not None:
                if flow["sell"] > 0 and flow["sell"] >= flow["buy"]:
                    when = _seattle(flow["last_sell_ts"]) if flow["last_sell_ts"] else "?"
                    fired.append(("庄在卖", f"转账实测 净卖 {flow['sell']-flow['buy']:,.0f} "
                                  f"(卖{flow['sell']:,.0f}/买{flow['buy']:,.0f}) · 最后卖单 {when}"))
                elif flow["buy"] > 0 and flow["buy"] > flow["sell"]:
                    when = _seattle(flow["last_buy_ts"]) if flow["last_buy_ts"] else "?"
                    fired.append(("庄在买", f"转账实测 净买 {flow['buy']-flow['sell']:,.0f} "
                                  f"(买{flow['buy']:,.0f}/卖{flow['sell']:,.0f}) · 最后买单 {when}"))
            # NOTE: buy/sell is detected ONLY via transfers (use_transfers=True, the
            # 5-min scheduler). balanceOf is unreliable on reflection/wash tokens
            # (EVAA, SIREN) — it drifts/oscillates and spammed phantom 庄在卖 every
            # 20s cycle. The fast watcher therefore does PRICE moves only (below).

            # ===== BACKSTOP: violent price/liquidity moves (if sampling lagged) =====
            cl, pl = cur.get("liquidity"), last.get("liquidity")
            if cl is not None and pl and pl > 0 and cl < pl * (1 - RUG_DROP):
                drop = (pl - cl) / pl * 100
                fired.append(("RUG", f"流动性 -{drop:.0f}% (${pl:,.0f}→${cl:,.0f}) 疑似抽池 → 逃命"))

            cpr, ppr = cur.get("price"), last.get("price")
            if cpr is not None and ppr and ppr > 0 and cpr < ppr * (1 - CRASH_DROP):
                drop = (ppr - cpr) / ppr * 100
                fired.append(("砸盘", f"价格 -{drop:.0f}% (${ppr:.4g}→${cpr:.4g}) 急跌(兜底) → 注意"))
            elif cpr is not None and ppr and ppr > 0 and cpr >= ppr * (1 + LAUNCH_PRICE):
                fired.append(("拉升", f"价格 +{(cpr/ppr-1)*100:.0f}% 急涨(兜底) → 已在拉"))
            else:
                cv, bv = cur.get("vol24"), base.get("vol24")
                if cv and bv and bv > 0 and cv >= bv * LAUNCH_VOL:
                    fired.append(("放量", f"24h量 {cv/bv:.1f}x 基线 (${cv:,.0f}) → 异动"))

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
                t["last_buy_ts"] = nowdt.isoformat()
                t["stop_alerted"] = False
            elif t.get("last_buy_ts") and not t.get("stop_alerted"):
                try:
                    since_buy = (nowdt - datetime.fromisoformat(t["last_buy_ts"])).total_seconds() / 3600
                except Exception:
                    since_buy = 0
                if since_buy >= STOP_HOURS:
                    fired.append(("庄停手", f"操作者停止加仓 {since_buy:.0f}h(刚还在买)→ "
                                  f"失去买盘支撑,注意回落,减/观望"))
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
                phase = ("sell" if kset & {"庄在卖", "阴跌出货", "砸盘", "RUG"} else
                         "buy" if kset & {"庄在买", "拉升"} else
                         "stall" if kset & {"庄停手", "动能熄火"} else "other")
                if phase == t.get("last_phase"):
                    fired = []           # same phase as last alert → suppress repeat
                else:
                    t["last_phase"] = phase

            if fired:
                fund = cur.get("funding")
                kinds = {k for k, _ in fired}
                fstr = f"(费率 {fund:+.3f}%)" if fund is not None else ""
                if kinds & {"庄在卖", "阴跌出货", "砸盘", "RUG"}:    # operator exiting / dump
                    action = "🔴 顶部跑 / 做空"
                    if fund is not None and fund > 0.03:
                        fstr = f"(费率 +{fund:.3f}% 多头拥挤,做空顺风)"
                    action += fstr
                elif kinds & {"庄在买", "拉升"}:          # operator marking up / launch
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
