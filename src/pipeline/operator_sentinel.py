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
# Price/liquidity are only a BACKSTOP — catch a violent move if balance sampling lags.
RUG_DROP = 0.30          # liquidity fell >=30% → LP pull / rug
CRASH_DROP = 0.15        # price fell >=15% vs last check → 砸盘 backstop
LAUNCH_PRICE = 0.25      # price up >=25% vs last check → launch backstop
LAUNCH_VOL = 3.0         # 24h volume >=3x baseline → volume backstop


def _load() -> dict:
    if SENTINELS_FILE.exists():
        try:
            return json.loads(SENTINELS_FILE.read_text())
        except Exception:
            return {}
    return {}


def _save(data: dict) -> None:
    SENTINELS_FILE.parent.mkdir(parents=True, exist_ok=True)
    SENTINELS_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2))


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
        out[t["symbol"]] = verdict
    _save(data)
    return out


def _cluster_balance(token: str, chain: str, wallets: list[str]) -> float | None:
    """Combined token balance of the operator cluster (free archive eth_call)."""
    if chain in ("solana", "sol"):
        from src.onchain import holder_snapshot as hs
        holders = {h["address"]: h.get("balance", 0) for h in hs.fetch_holders_solana(token)}
        return sum(float(holders.get(w, 0) or 0) for w in wallets)
    try:
        from src.onchain.evm_archive import ArchiveRPC, combined_balance_at
        rpc = ArchiveRPC(chain)
        if not rpc.available():
            return None
        return combined_balance_at(token, wallets, chain, rpc.latest_block(), rpc=rpc)
    except Exception as e:
        logger.debug("sentinel_balance_failed", token=token, error=str(e))
        return None


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


def check_run() -> list[dict]:
    """One monitoring pass over all registered targets. Returns fired alerts and
    persists updated last-seen state."""
    data = _load()
    alerts = []
    for key, t in data.items():
        cur = _measure(t["token"], t["chain"], t["wallets"], t.get("symbol", ""))
        last, base = t.get("last", {}), t.get("baseline", {})
        fired = []

        # ===== PRIMARY: the operator's own action (net position turn) =====
        cb, pb = cur.get("cluster_balance"), last.get("cluster_balance")
        if cb is not None and pb and pb > 0:
            chg = (cb - pb) / pb
            if chg <= -OP_SELL:           # operator SELLING — the earliest exit/short
                fired.append(("庄在卖", f"簇余额 {chg*100:+.1f}% ({pb:,.0f}→{cb:,.0f}) "
                              f"操作者出货 → 顶部跑/做空,别等价格跌"))
            elif chg >= OP_BUY:           # operator BUYING — markup/launch begins
                fired.append(("庄在买", f"簇余额 {chg*100:+.1f}% ({pb:,.0f}→{cb:,.0f}) "
                              f"操作者加仓 → 拉升前埋伏/做多"))

        # ===== BACKSTOP: violent price/liquidity moves (in case sampling lagged) =====
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

        if fired:
            # Directional call: combine the event with funding (longs-crowded =
            # short-favorable). Operator pumping → LONG; operator selling / crash →
            # SHORT. Funding tilts/confirms the bias.
            fund = cur.get("funding")
            kinds = {k for k, _ in fired}
            fstr = f"(费率 {fund:+.3f}%)" if fund is not None else ""
            if kinds & {"庄在卖", "砸盘", "RUG"}:    # operator exiting / dump
                action = "🔴 顶部跑 / 做空"
                if fund is not None and fund > 0.03:
                    fstr = f"(费率 +{fund:.3f}% 多头拥挤,做空顺风)"
                action += fstr
            elif kinds & {"庄在买", "拉升"}:          # operator marking up / launch
                sl = t.get("second_leg", "")
                if "二波候选" in sl:
                    action = "🟢🟢 二波启动!最高优先 做多"
                else:
                    action = "🟢 埋伏 / 做多(跟庄)"
                if fund is not None and fund > 0.08:
                    fstr = f"(费率 +{fund:.3f}% 已过热,小心追高)"
                action += fstr
            else:                                     # volume-only anomaly
                action = f"⚪ 异动留意 {fstr}"
            alerts.append({"symbol": t["symbol"], "chain": t["chain"],
                           "token": t["token"], "events": fired,
                           "funding": fund, "action": action})
            # Log for outcome scoring (does the call actually work?).
            try:
                from src.pipeline.outcome_tracker import log_alert
                direction = "short" if "做空" in action or "跑" in action else \
                            "long" if "做多" in action else "none"
                log_alert(t["token"], t["chain"], t["symbol"],
                          ",".join(sorted(kinds)), direction, cur.get("price") or 0)
            except Exception:
                pass
        # Advance state, but NEVER overwrite a good last value with None — a
        # transient fetch failure must not blind the next comparison (else a drop
        # that happens during the outage is missed). Keep the last known good.
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
        lines.append(f"  <code>{a['token']}</code>")
    lines.append("\n<i>带止损,薄盘小仓。仅信号,非投资建议。</i>")
    return "\n".join(lines)


async def run_and_alert() -> int:
    """Scheduler entry: check + push Telegram on any trigger. Returns alert count."""
    alerts = check_run()
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
            fs = (f" · 费率 {fund:+.3f}%/8h{'🔴多拥挤' if fund and fund > 0.03 else ''}"
                  if fund is not None else "")
            print(f"  {t['symbol']}: 簇 {lc.get('cluster_balance'):,.0f} · "
                  f"流动性 ${lc.get('liquidity'):,.0f} · 价 ${lc.get('price')}{fs}")


if __name__ == "__main__":
    from dotenv import load_dotenv

    from src.config import PROJECT_ROOT
    load_dotenv(PROJECT_ROOT / ".env")
    main()
