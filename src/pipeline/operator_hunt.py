"""Operator hunt — find tokens with a HIDDEN operator, not just market footprint.

The screener pre-filters on market microstructure, which surfaces established/legit
tokens. A real 妖币 operator is defined on-chain: ONE private entity controls a big
share of supply, split across many wallets (Sybil) to look dispersed. That's the
effective-concentration signature — and it's the actual discriminator (SIREN: 131
wallets = 1 entity; CREPE: 45 wallets = 42 entities).

So this scanner inverts the funnel: cast a wide net over the 妖币 hunting grounds
(BSC + Solana trending / new / top-volume pools, in the operator sweet-spot
liquidity band), then run effective_concentration_signal on EACH and rank by the
operator signature — large effective concentration and/or a big nominal->effective
clustering gap (hidden Sybil).

Free end to end: BSC holders+funders via Moralis, Solana via Helius.

    python -m src.pipeline.operator_hunt          # scan + rank, print suspects
    python -m src.pipeline.operator_hunt --push    # also Telegram the suspects
"""

from __future__ import annotations

import sys
import time

import structlog

from src.pipeline.anomaly_screener import (
    _dexscreener_tokens, _gt_base_addresses, effective_concentration_signal,
)

logger = structlog.get_logger()

# 妖币 hunting grounds: where operators accumulate (BSC strongest, Solana next).
_HUNT_CHAINS = {"bsc": "bsc", "solana": "solana"}
# Operator sweet spot: established enough to trade, small enough for one entity to
# control. Too big (>$8M liq) = real project; too small (<$120k) = dead micro-cap.
MIN_LIQ, MAX_LIQ = 120_000, 8_000_000


def _gather_universe(per_chain: int = 80, pages: int = 2) -> list[dict]:
    """Token pairs from GeckoTerminal trending + new + top-volume (multiple pages),
    in the hunt band. Returns DexScreener pairs (with txns/liquidity), deduped."""
    pairs: list[dict] = []
    for net, chain in _HUNT_CHAINS.items():
        addrs: list[str] = []
        for pg in range(1, pages + 1):
            for path in (f"networks/{net}/trending_pools?page={pg}",
                         f"networks/{net}/new_pools?page={pg}",
                         f"networks/{net}/pools?page={pg}&sort=h24_volume_usd_desc"):
                addrs += _gt_base_addresses(path)
        seen, uniq = set(), []
        for a in addrs:
            al = a.lower()
            if al not in seen:
                seen.add(al); uniq.append(a)
        pairs.extend(_dexscreener_tokens(chain, uniq[:per_chain]))
    return pairs


def _in_band(pair: dict) -> bool:
    liq = (pair.get("liquidity", {}) or {}).get("usd", 0) or 0
    if not (MIN_LIQ <= liq <= MAX_LIQ):
        return False
    # Skip brand-new (<2d, no accumulation history) and already-mooned.
    age_ms = pair.get("pairCreatedAt", 0) or 0
    if age_ms and (time.time() * 1000 - age_ms) < 2 * 86400 * 1000:
        return False
    ch24 = float((pair.get("priceChange", {}) or {}).get("h24", 0) or 0)
    return ch24 <= 60


def hunt(per_chain: int = 40, max_scan: int = 50) -> list[dict]:
    """Scan the universe, run effective concentration on each, rank by operator
    signature. Returns suspects sorted by a concentration score."""
    universe = _gather_universe(per_chain)
    # Dedup by (chain, token); keep the deepest-liquidity pair per token.
    best: dict[tuple, dict] = {}
    for p in universe:
        base = p.get("baseToken", {}) or {}
        addr, chain = base.get("address"), p.get("chainId")
        if not addr or not chain or not _in_band(p):
            continue
        key = (chain, addr.lower())
        liq = (p.get("liquidity", {}) or {}).get("usd", 0) or 0
        if key not in best or liq > (best[key].get("liquidity", {}) or {}).get("usd", 0):
            best[key] = p
    targets = list(best.values())[:max_scan]
    logger.info("operator_hunt_scan", targets=len(targets))

    from src.onchain import holder_snapshot as hs
    suspects = []
    for p in targets:
        base = p.get("baseToken", {}) or {}
        addr, chain = base.get("address"), p.get("chainId")
        sym = base.get("symbol", "?")
        try:
            if chain in ("solana", "sol"):
                holders = hs.fetch_holders_solana(addr)
            else:
                cid = {"bsc": 56, "ethereum": 1, "base": 8453}.get(chain, 56)
                holders = hs.fetch_holders_evm(addr, chain_id=cid, max_pages=5)
            if not holders:
                continue
            conc = effective_concentration_signal(holders, addr, chain)
        except Exception as e:
            logger.debug("hunt_token_failed", token=addr, error=str(e))
            continue
        if not conc:
            continue
        lg = conc.get("largest_entity_pct", 0) or 0
        gap = conc.get("concentration_gap", 0) or 0
        fc = conc.get("funder_complete")
        # Operator score: effective concentration + the hidden-cluster uplift.
        op_score = (lg if fc else 0) + gap * 2
        suspects.append({
            "symbol": sym, "chain": chain, "address": addr,
            "liquidity": (p.get("liquidity", {}) or {}).get("usd", 0),
            "mc": p.get("marketCap") or p.get("fdv") or 0,
            "largest_entity_pct": lg, "largest_address_pct": conc.get("largest_address_pct", 0),
            "concentration_gap": gap, "entity_count": conc.get("entity_count"),
            "eoa_analyzed": conc.get("eoa_analyzed"), "funder_complete": fc,
            "op_score": round(op_score, 1),
            "url": p.get("url", ""),
        })
        time.sleep(0.3)

    suspects.sort(key=lambda s: -s["op_score"])
    return suspects


def format_suspects(suspects: list[dict], top: int = 12) -> str:
    lines = ["=" * 66, "操作者猎手 — 按控盘签名排序", "=" * 66]
    if not suspects:
        lines.append("本轮无候选(宇宙为空或持币列表都没取到)")
        return "\n".join(lines)
    for i, s in enumerate(suspects[:top], 1):
        verdict = ("⭐⭐控盘嫌疑" if s["funder_complete"] and s["largest_entity_pct"] >= 15
                   else "⭐隐藏簇" if s["concentration_gap"] >= 8
                   else "分散")
        fc = "✅" if s["funder_complete"] else "⚠️"
        lines.append(
            f"\n{i:2}. {s['symbol']:12}[{s['chain']:7}] op={s['op_score']:>5}  {verdict}")
        lines.append(
            f"    实体{s['largest_entity_pct']:.1f}%供应 单址{s['largest_address_pct']:.1f}% "
            f"缺口{s['concentration_gap']:+.1f} | {s['eoa_analyzed']}EOA→{s['entity_count']}实体 funder{fc}")
        lines.append(
            f"    流动性${s['liquidity']:,.0f} 市值${s['mc']:,.0f}")
        lines.append(f"    {s['address']}")
    return "\n".join(lines)


def main():
    res = hunt()
    print(format_suspects(res))
    if "--push" in sys.argv and res:
        strong = [s for s in res if s["funder_complete"]
                  and (s["largest_entity_pct"] >= 15 or s["concentration_gap"] >= 8)]
        if strong:
            from src.distribution.telegram_sender import send_alert
            from src.pipeline.anomaly_screener import _run_coro, _esc
            msg = "🎯 <b>操作者猎手 — 控盘嫌疑</b>\n━━━━━━━━━━\n"
            for s in strong[:8]:
                msg += (f"<b>{_esc(s['symbol'])}</b> [{s['chain']}] "
                        f"实体{s['largest_entity_pct']:.0f}%供应 缺口{s['concentration_gap']:+.0f}\n"
                        f"<code>{s['address']}</code>\n")
            _run_coro(send_alert(msg))
            print(f"\n→ 已推送 {len(strong)} 个控盘嫌疑到 Telegram")


if __name__ == "__main__":
    import os

    from dotenv import load_dotenv

    from src.config import PROJECT_ROOT
    load_dotenv(PROJECT_ROOT / ".env")
    os.environ.setdefault("PYTHONUNBUFFERED", "1")
    main()
