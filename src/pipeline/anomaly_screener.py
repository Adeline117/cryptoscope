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


def accumulation_footprint(pair: dict) -> dict | None:
    """Score a DexScreener pair for the quiet-accumulation footprint (0-100)."""
    txns = pair.get("txns", {}) or {}
    pc = pair.get("priceChange", {}) or {}
    vol = pair.get("volume", {}) or {}
    liq = (pair.get("liquidity", {}) or {}).get("usd", 0) or 0
    mc = pair.get("marketCap") or pair.get("fdv") or 0
    age_ms = pair.get("pairCreatedAt", 0) or 0

    h1 = txns.get("h1", {}) or {}
    h6 = txns.get("h6", {}) or {}
    buys = (h1.get("buys", 0) or 0) + (h6.get("buys", 0) or 0)
    sells = (h1.get("sells", 0) or 0) + (h6.get("sells", 0) or 0)
    if buys + sells < 20 or liq < 20_000:
        return None  # too illiquid/inactive to read

    buy_ratio = buys / max(sells, 1)
    ch24 = float(pc.get("h24", 0) or 0)
    vol24 = float(vol.get("h24", 0) or 0)
    vol_liq = vol24 / max(liq, 1)

    # Footprint conditions (the quiet-accumulation signature):
    score = 0
    notes = []
    if buy_ratio >= 1.3:                       # sustained buy pressure
        score += min(35, int((buy_ratio - 1) * 30)); notes.append(f"买压{buy_ratio:.1f}x")
    if 0 <= ch24 <= 25:                        # rising slowly, not mooning
        score += 25; notes.append(f"价{ch24:+.0f}%(未爆)")
    elif 25 < ch24 <= 60:
        score += 10
    if 0.05 <= vol_liq <= 2.0:                 # active but not frenzied
        score += 20; notes.append("量健康")
    if age_ms and (__import__("time").time() * 1000 - age_ms) > 7 * 86400 * 1000:
        score += 20; notes.append("已建立")  # >1 week old = not a fresh curve launch

    if score < 50:
        return None
    return {
        "score": score,
        "buy_ratio": round(buy_ratio, 2),
        "price_change_24h": ch24,
        "mc": mc, "liquidity": liq,
        "notes": " · ".join(notes),
    }


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
    return cands[:max_out]


def format_candidates(cands: list[dict]) -> str:
    """Telegram-friendly candidate list."""
    if not cands:
        return "本轮无吸筹足迹候选。"
    lines = ["🔎 <b>疑似吸筹候选</b>(待 Arkham 查庄簇 → 二次确认)", ""]
    for i, c in enumerate(cands[:12], 1):
        lines.append(
            f"{i}. <b>{c['symbol']}</b> [{c['chain']}] 评分{c['score']}\n"
            f"   {c['notes']}\n"
            f"   <code>{c['address']}</code>"
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
