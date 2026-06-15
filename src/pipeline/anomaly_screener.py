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


def _dexscreener_pairs(query: str) -> list[dict]:
    url = f"https://api.dexscreener.com/latest/dex/search?q={query}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "CryptoScope/1.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            return json.loads(r.read().decode()).get("pairs", [])
    except Exception as e:
        logger.debug("dexscreener_failed", query=query, error=str(e))
        return []


def _ratio(d: dict, w: str) -> float:
    x = d.get(w, {}) or {}
    return (x.get("buys", 0) or 0) / max(x.get("sells", 0) or 0, 1)


def accumulation_footprint(pair: dict) -> dict | None:
    """Score a DexScreener pair for the quiet-accumulation footprint (0-100).

    Sharper signals than buy-pressure alone:
      - ABSORPTION: strong buy pressure WHILE price is suppressed (flat/down) —
        someone is quietly soaking up sell-side without letting price run. This
        is the cleanest stealth-accumulation tell.
      - CONSISTENCY: buy dominance sustained across m5/h1/h6 (not a one-off blip).
      - volume present vs liquidity; established (not a fresh curve launch).
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
    if buys + sells < 20 or liq < 20_000:
        return None

    buy_ratio = buys / max(sells, 1)
    ch24 = float(pc.get("h24", 0) or 0)
    ch6 = float(pc.get("h6", 0) or 0)
    vol_liq = float(vol.get("h24", 0) or 0) / max(liq, 1)
    # buy dominance across windows = sustained, not a single spike
    consistency = sum(1 for w in ("m5", "h1", "h6") if _ratio(txns, w) >= 1.2)

    score, notes = 0, []
    # ABSORPTION: buy pressure with suppressed price (the sharp signal)
    if buy_ratio >= 1.5 and -8 <= ch6 <= 5:
        score += 40; notes.append(f"吸收(买{buy_ratio:.1f}x价压{ch6:+.0f}%)")
    elif buy_ratio >= 1.3 and 0 <= ch24 <= 25:
        score += 22; notes.append(f"买压{buy_ratio:.1f}x价未爆")
    if consistency >= 2:                       # sustained across windows
        score += 20; notes.append(f"{consistency}/3窗口买压")
    if 0.05 <= vol_liq <= 2.0:
        score += 15; notes.append("量健康")
    if age_ms and (__import__("time").time() * 1000 - age_ms) > 7 * 86400 * 1000:
        score += 15; notes.append("已建立")
    if ch24 > 80:                              # already mooned → penalize
        score -= 30; notes.append("已大涨")

    if score < 50:
        return None
    return {"score": score, "buy_ratio": round(buy_ratio, 2),
            "price_change_24h": ch24, "consistency": consistency,
            "mc": mc, "liquidity": liq, "notes": " · ".join(notes)}


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


def screen_universe(queries: list[str] | None = None, max_out: int = 15) -> list[dict]:
    """Scan a universe of tokens and rank by accumulation footprint.

    `queries` seed the DexScreener search (defaults to broad meme/AI/low-cap terms).
    Returns ranked candidates: {symbol, chain, address, score, notes}.
    """
    queries = queries or ["ai", "agent", "meme", "pepe", "inu", "cat", "moon"]
    seen, cands = set(), []
    for q in queries:
        for p in _dexscreener_pairs(q):
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

    # Enrich top candidates with the cheap on-chain CEX-outflow check (EVM only).
    for c in cands:
        try:
            cx = cex_outflow_signal(c["address"], c["chain"])
        except Exception:
            cx = None
        if cx and cx.get("outflow"):
            c["score"] += 20
            c["notes"] += f" · CEX流出{cx['cex_change_pct']:+.0f}%"
            c["cex_outflow"] = cx["cex_change_pct"]
    cands.sort(key=lambda c: -c["score"])
    return cands


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
