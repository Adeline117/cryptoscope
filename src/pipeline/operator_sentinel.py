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
DISTRIBUTE_DROP = 0.05   # cluster balance fell >=5% → operator selling
RUG_DROP = 0.30          # liquidity fell >=30% → LP pull / rug
LAUNCH_PRICE = 0.30      # price up >=30% vs baseline → launch underway
LAUNCH_VOL = 3.0         # 24h volume >=3x baseline → launch underway


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


def _measure(token: str, chain: str, wallets: list[str]) -> dict:
    m = _dex(token, chain)
    m["cluster_balance"] = _cluster_balance(token, chain, wallets)
    return m


def register(token: str, chain: str, symbol: str, wallets: list[str]) -> dict:
    """Snapshot the current state as the baseline and start watching."""
    data = _load()
    key = f"{chain}:{token.lower()}"
    state = _measure(token, chain, wallets)
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
        cur = _measure(t["token"], t["chain"], t["wallets"])
        last, base = t.get("last", {}), t.get("baseline", {})
        fired = []

        # DISTRIBUTE — cluster balance dropped vs last seen.
        cb, pb = cur.get("cluster_balance"), last.get("cluster_balance")
        if cb is not None and pb and pb > 0 and cb < pb * (1 - DISTRIBUTE_DROP):
            drop = (pb - cb) / pb * 100
            fired.append(("派发", f"操作者簇余额 -{drop:.0f}% ({pb:,.0f}→{cb:,.0f}) 庄在卖 → 离场"))

        # RUG — liquidity collapsed vs last seen.
        cl, pl = cur.get("liquidity"), last.get("liquidity")
        if cl is not None and pl and pl > 0 and cl < pl * (1 - RUG_DROP):
            drop = (pl - cl) / pl * 100
            fired.append(("RUG", f"流动性 -{drop:.0f}% (${pl:,.0f}→${cl:,.0f}) 疑似抽池 → 逃命"))

        # LAUNCH — price breakout or volume spike vs baseline.
        cp, bp = cur.get("price"), base.get("price")
        cv, bv = cur.get("vol24"), base.get("vol24")
        if cp and bp and bp > 0 and cp >= bp * (1 + LAUNCH_PRICE):
            fired.append(("启动", f"价格 +{(cp/bp-1)*100:.0f}% vs 基线 → 庄在拉"))
        elif cv and bv and bv > 0 and cv >= bv * LAUNCH_VOL:
            fired.append(("启动", f"24h量 {cv/bv:.1f}x 基线 (${cv:,.0f}) → 放量,可能启动"))

        if fired:
            alerts.append({"symbol": t["symbol"], "chain": t["chain"],
                           "token": t["token"], "events": fired})
        t["last"] = cur  # advance state regardless
    _save(data)
    return alerts


def _format(alerts: list[dict]) -> str:
    lines = ["🚨 <b>操作者哨兵告警</b>", "━━━━━━━━━━"]
    for a in alerts:
        lines.append(f"\n<b>{a['symbol']}</b> [{a['chain']}]")
        for kind, detail in a["events"]:
            lines.append(f"  ⚠️ <b>{kind}</b>: {detail}")
        lines.append(f"  <code>{a['token']}</code>")
    return "\n".join(lines)


async def run_and_alert() -> int:
    """Scheduler entry: check + push Telegram on any trigger. Returns alert count."""
    alerts = check_run()
    if alerts:
        from src.distribution.telegram_sender import send_alert
        await send_alert(_format(alerts))
    logger.info("operator_sentinel_done", targets=len(_load()), alerts=len(alerts))
    return len(alerts)


# BASED operator cluster (9 wallets) — confirmed in data/research/BASED_analysis.md.
_BASED = (
    "0x1d28D989F9e3CCb8B15D0cec601734514f958E4D", "bsc", "BASED",
    ["0xc3526ad0a5fa2d7bac0963904036d8604b13470e", "0x78a0cddf1e0c966d505181b0dfaf505e398d053a",
     "0x3e5dcdbded6ca3d2a78eb14c307c9ed7a9638c52", "0x40c0e5f38fecacd5d7dbea41cd1c34c8917a25b0",
     "0x42a99d7dc78415ea0995edd8d5e718495a07a7c1", "0x774922fbb5a9e6d52c14fa9dfa25c16219f91c90",
     "0x4bdaa8005233f251375212efab1d2ce938d62b78", "0xc4be9808281709d47489ce1e2a5422da29e5506a",
     "0x6908518e91b83a05a51b8961d1c5667b2e9bc4a3"],
)


def main():
    if "--register-based" in sys.argv:
        s = register(*_BASED)
        print(f"✅ BASED 已注册哨兵 — 基线: 簇余额 {s['baseline'].get('cluster_balance'):,.0f}, "
              f"流动性 ${s['baseline'].get('liquidity'):,.0f}, 价格 ${s['baseline'].get('price')}")
        return
    alerts = check_run()
    if alerts:
        print(_format(alerts).replace("<b>", "").replace("</b>", "").replace("<code>", "").replace("</code>", ""))
    else:
        targets = _load()
        print(f"哨兵巡检完成 — {len(targets)} 个目标,无触发(派发/rug/启动均未发生)")
        for k, t in targets.items():
            lc = t["last"]
            print(f"  {t['symbol']}: 簇余额 {lc.get('cluster_balance'):,.0f} · 流动性 ${lc.get('liquidity'):,.0f} · 价格 ${lc.get('price')}")


if __name__ == "__main__":
    from dotenv import load_dotenv

    from src.config import PROJECT_ROOT
    load_dotenv(PROJECT_ROOT / ".env")
    main()
