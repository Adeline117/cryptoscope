"""Majors monitor — BTC/ETH/SOL via flow + positioning, not operator clustering.

The 妖币 method (find one operator who controls the float) does NOT apply to
majors — no entity corners BTC. But the PRINCIPLE (follow what actually moves the
market) does, with different signals: derivatives positioning (funding, open
interest, retail long/short) and — where free — exchange flows. Majors are also
far more tradeable (deep perps, real shorts, no rug/squeeze-by-operator).

Signals (OKX public API, free, no key):
  - FUNDING extreme: very + = longs crowded/paying → squeeze-DOWN risk (short lean);
    negative = shorts crowded → squeeze-UP fuel (long lean).
  - RETAIL long/short (contrarian): retail very long = top-ish (short lean); very
    short = bottom-ish (long lean).
  - OI surge + flat price = leverage building → volatility/forced move coming.

State-tracked like the operator sentinel; alerts on strong setups. Detection only.

    python -m src.pipeline.majors_monitor            # snapshot + signals
"""

from __future__ import annotations

import json
import urllib.request

import structlog

from src.config import DATA_DIR

logger = structlog.get_logger()

STATE_FILE = DATA_DIR / "majors_state.json"
MAJORS = ["BTC", "ETH", "SOL"]

FUNDING_HOT = 0.03      # funding %/8h >= this → longs crowded (short lean)
FUNDING_COLD = -0.01    # <= this → shorts crowded (long lean)
RETAIL_LONG = 2.3       # retail L/S >= this → over-long (contrarian short)
RETAIL_SHORT = 0.8      # <= this → over-short (contrarian long)
OI_SURGE = 0.08         # OI +8% vs last check → leverage building


_CEX_SLUGS = ["binance-cex", "okx", "bybit", "bitfinex"]


def _llama(slug: str):
    try:
        req = urllib.request.Request(f"https://api.llama.fi/protocol/{slug}",
                                     headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            return json.loads(r.read().decode())
    except Exception:
        return None


def exchange_flow_24h(price_proxy_ch24: float = 0.0) -> dict | None:
    """Net 24h exchange-reserve flow across major CEXs (DeFiLlama), PRICE-ADJUSTED:
    reserves are USD so they move with price; subtracting BTC's 24h change isolates
    the FLOW. Net outflow (reserves fell more than price) = coins leaving exchanges
    = accumulation/bullish; net inflow = distribution/bearish. Market-wide overlay."""
    import time as _t
    now_sum = past_sum = 0.0
    for slug in _CEX_SLUGS:
        d = _llama(slug)
        tvl = (d or {}).get("tvl", [])
        if not tvl:
            continue
        now = tvl[-1]
        now_t = now.get("date", 0)
        past = min(tvl, key=lambda x: abs(x.get("date", 0) - (now_t - 86400)))
        now_sum += now.get("totalLiquidityUSD", 0) or 0
        past_sum += past.get("totalLiquidityUSD", 0) or 0
    if past_sum <= 0:
        return None
    reserve_ch = (now_sum / past_sum - 1) * 100
    net_flow = round(reserve_ch - price_proxy_ch24, 2)  # remove the price effect
    return {"reserve_change_24h": round(reserve_ch, 2), "net_flow_pct": net_flow,
            "reserves_usd": round(now_sum)}


def _okx(path: str):
    try:
        req = urllib.request.Request(f"https://www.okx.com{path}", headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=12) as r:
            return json.loads(r.read().decode()).get("data", [])
    except Exception as e:
        logger.debug("okx_failed", path=path[:50], error=str(e))
        return []


def snapshot(ccy: str) -> dict | None:
    inst = f"{ccy}-USDT-SWAP"
    t = _okx(f"/api/v5/market/ticker?instId={inst}")
    if not t:
        return None
    t = t[0]
    fr = _okx(f"/api/v5/public/funding-rate?instId={inst}")
    oi = _okx(f"/api/v5/public/open-interest?instId={inst}")
    ls = _okx(f"/api/v5/rubik/stat/contracts/long-short-account-ratio?ccy={ccy}&period=1H")
    last = float(t.get("last", 0) or 0)
    o24 = float(t.get("open24h", 0) or last)
    return {
        "ccy": ccy, "price": last,
        "ch24": round((last / o24 - 1) * 100, 2) if o24 else 0,
        "funding": round(float(fr[0].get("fundingRate", 0)) * 100, 4) if fr else None,
        "oi_usd": round(float(oi[0].get("oiCcy", 0)) * last) if oi else None,
        "retail_ls": round(float(ls[0][1]), 2) if ls else None,
    }


def assess(s: dict, prev: dict | None) -> dict:
    """Directional read from positioning. Returns {signals, lean, note}."""
    sig = []
    f, r = s.get("funding"), s.get("retail_ls")
    if f is not None and f >= FUNDING_HOT:
        sig.append(("多头拥挤", f"费率 +{f:.3f}%/8h → 多头付费拥挤,挤压向下风险(空向)", "short"))
    elif f is not None and f <= FUNDING_COLD:
        sig.append(("空头拥挤", f"费率 {f:.3f}%/8h → 空头拥挤,逼空燃料(多向)", "long"))
    if r is not None and r >= RETAIL_LONG:
        sig.append(("散户过度看多", f"散户多空比 {r:.1f} → 反指,顶部风险(空向)", "short"))
    elif r is not None and r <= RETAIL_SHORT:
        sig.append(("散户过度看空", f"散户多空比 {r:.1f} → 反指,底部信号(多向)", "long"))
    if prev and prev.get("oi_usd") and s.get("oi_usd"):
        chg = (s["oi_usd"] - prev["oi_usd"]) / prev["oi_usd"]
        if chg >= OI_SURGE and abs(s.get("ch24", 0)) < 3:
            sig.append(("杠杆堆积", f"持仓 +{chg*100:.0f}% 价格未动 → 杠杆在堆,波动/强平将至", "vol"))
        elif chg <= -OI_SURGE:
            sig.append(("去杠杆", f"持仓 {chg*100:.0f}% → 仓位在平,可能已强平洗盘", "vol"))
    longs = sum(1 for _, _, d in sig if d == "long")
    shorts = sum(1 for _, _, d in sig if d == "short")
    lean = "🟢 偏多" if longs > shorts else "🔴 偏空" if shorts > longs else "⚪ 中性"
    return {"signals": sig, "lean": lean}


def _load() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception:
            return {}
    return {}


def _save(d: dict) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(d, ensure_ascii=False, indent=2))


def check_run() -> list[dict]:
    """Snapshot all majors, assess vs last, return strong-signal alerts. A
    market-wide exchange-flow overlay (accumulation/distribution) is added to each."""
    state = _load()
    snaps = {ccy: snapshot(ccy) for ccy in MAJORS}
    btc_ch = (snaps.get("BTC") or {}).get("ch24", 0) or 0
    flow = exchange_flow_24h(btc_ch)
    flow_sig = None
    if flow:
        nf = flow["net_flow_pct"]
        if nf <= -2:
            flow_sig = ("交易所净流出", f"CEX储备净流出 {nf:+.1f}%(剔价) → 币进冷钱包,机构吸筹(多向)", "long")
        elif nf >= 2:
            flow_sig = ("交易所净流入", f"CEX储备净流入 {nf:+.1f}%(剔价) → 币进交易所,派发(空向)", "short")
    alerts = []
    for ccy in MAJORS:
        s = snaps[ccy]
        if not s:
            continue
        a = assess(s, state.get(ccy))
        if flow_sig:
            a["signals"].append(flow_sig)
        longs = sum(1 for _, _, d in a["signals"] if d == "long")
        shorts = sum(1 for _, _, d in a["signals"] if d == "short")
        a["lean"] = "🟢 偏多" if longs > shorts else "🔴 偏空" if shorts > longs else "⚪ 中性"
        # CONFLUENCE ONLY: alert when >=2 signals align in one direction with a net
        # edge >=2 (no offsetting opposite). Single weak signals are noise on
        # efficient majors — don't cry wolf. (OI 'vol' is context, never a trigger.)
        net = longs - shorts
        direction = "long" if net >= 2 else "short" if net <= -2 else None
        if direction:
            alerts.append({"ccy": ccy, "price": s["price"], "lean": a["lean"],
                           "signals": a["signals"], "direction": direction})
            try:
                from src.pipeline.outcome_tracker import log_alert
                kinds = ",".join(n for n, _det, d in a["signals"] if d != "vol")
                log_alert(ccy, "majors", ccy, kinds or "majors", direction, s["price"], 0)
            except Exception:
                pass
        state[ccy] = s
    if flow:
        state["_flow"] = flow
    _save(state)
    return alerts


def _format(alerts: list[dict]) -> str:
    lines = ["📊 <b>大币持仓信号 (BTC/ETH/SOL)</b>", "━━━━━━━━━━"]
    for a in alerts:
        lines.append(f"\n<b>{a['ccy']}</b> ${a['price']:,.2f} — {a['lean']}")
        for name, detail, _ in a["signals"]:
            lines.append(f"  • {detail}")
    lines.append("\n<i>持仓/资金费率信号,非投资建议。majors 可双向交易、深流动性。</i>")
    return "\n".join(lines)


async def run_and_alert() -> int:
    alerts = check_run()
    if alerts:
        from src.distribution.telegram_sender import send_alert
        await send_alert(_format(alerts))
    logger.info("majors_monitor_done", alerts=len(alerts))
    return len(alerts)


def main():
    state = _load()
    print("=" * 60)
    print("大币持仓快照 (BTC/ETH/SOL)")
    print("=" * 60)
    snaps = {ccy: snapshot(ccy) for ccy in MAJORS}
    flow = exchange_flow_24h((snaps.get("BTC") or {}).get("ch24", 0) or 0)
    if flow:
        nf = flow["net_flow_pct"]
        tag = ("🟢机构吸筹" if nf <= -2 else "🔴派发" if nf >= 2 else "⚪中性")
        print(f"\n💰 交易所净流(剔价): {nf:+.1f}% {tag} (总储备 ${flow['reserves_usd']:,.0f})")
    for ccy in MAJORS:
        s = snaps.get(ccy)
        if not s:
            print(f"  {ccy}: 数据获取失败")
            continue
        a = assess(s, state.get(ccy))
        print(f"\n{ccy}: ${s['price']:,.2f} 24h{s['ch24']:+.1f}% | 费率{s['funding']:+.4f}% | "
              f"OI ${s['oi_usd']:,.0f} | 散户多空比 {s['retail_ls']}")
        print(f"  → {a['lean']}")
        for name, detail, _ in a["signals"]:
            print(f"     • {detail}")
        state[ccy] = s
    _save(state)


if __name__ == "__main__":
    from dotenv import load_dotenv

    from src.config import PROJECT_ROOT
    load_dotenv(PROJECT_ROOT / ".env")
    main()
