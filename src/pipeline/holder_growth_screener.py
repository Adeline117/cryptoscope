"""Holder-growth / concentration-trend screener — a universe source that does NOT
depend on GeckoTerminal trending feeds.

The trending/new feeds structurally surface HYPE (new launches, pumps), the opposite
of the quiet accumulation this system hunts. This screener instead reads the holder
snapshots we already collect over time (data/holder_snapshots.db) and surfaces tokens
whose FLOAT IS CONCENTRATING — top-10 share / gini rising while the holder base stays
stable = a few wallets absorbing supply from the broad base. That is the on-chain
accumulation footprint, independent of any market feed.

Critical de-noising: raw top10_pct trend is dominated by FETCH-DEPTH artifacts — when
a later snapshot fetched fewer holders, top10 trivially approaches 100% (holders -90%
→ top10 99%). We only compare snapshots whose fetch depth (holder_count) is STABLE, so
the concentration delta is apples-to-apples, not an artifact.

Flagged candidates are then confirmed with effective_concentration_signal (which carries
the CEX/disperser-funder guard) to tell a real hidden operator from retail noise.

    python -m src.pipeline.holder_growth_screener
"""

from __future__ import annotations

import json
import sqlite3

import structlog

logger = structlog.get_logger()

MIN_SNAPSHOTS = 3        # need a trend, not two points
MIN_HOLDERS = 50         # enough fetch depth for top10/gini to mean something
MAX_DEPTH_DRIFT = 0.20   # |holder_count change| must be ≤20% — else top10 delta is a fetch artifact
MIN_TOP10_RISE = 2.0     # percentage points: float concentrating
MIN_GINI_RISE = 0.01


def _series(db_path) -> dict:
    """token,chain -> list of (snapshot_at, holder_count, top10_pct, gini) oldest-first."""
    conn = sqlite3.connect(str(db_path))
    try:
        rows = conn.execute(
            """SELECT token, chain, snapshot_at, holder_count, top10_pct, gini
               FROM holder_snapshots ORDER BY snapshot_at ASC""").fetchall()
    finally:
        conn.close()
    out: dict = {}
    for token, chain, ts, hc, t10, gini in rows:
        out.setdefault((token, chain), []).append((ts, hc, t10, gini))
    return out


def screen_holder_growth(db_path=None, min_snapshots: int = MIN_SNAPSHOTS) -> list[dict]:
    """Rank tokens by a rising-concentration (accumulation) footprint over the snapshot
    history. Fetch-depth-stable only, so the concentration delta is real."""
    if db_path is None:
        from src.onchain.holder_snapshot import DB_PATH
        db_path = DB_PATH
    cands: list[dict] = []
    for (token, chain), s in _series(db_path).items():
        if len(s) < min_snapshots:
            continue
        (_, hc0, t10_0, g0) = s[0]
        (_, hc1, t10_1, g1) = s[-1]
        if None in (hc0, hc1, t10_0, t10_1) or hc0 <= 0:
            continue
        if hc1 < MIN_HOLDERS:
            continue
        depth_drift = abs(hc1 - hc0) / hc0
        if depth_drift > MAX_DEPTH_DRIFT:        # fetch depth changed → delta is an artifact
            continue
        d_top10 = t10_1 - t10_0
        d_gini = (g1 - g0) if (g0 is not None and g1 is not None) else 0.0
        if d_top10 < MIN_TOP10_RISE and d_gini < MIN_GINI_RISE:
            continue
        d_holders = (hc1 - hc0) / hc0 * 100
        # Score: concentration rise is the signal; a stable/growing holder base while it
        # rises is the cleanest accumulation (few absorbing from many, not a holder exodus).
        score = d_top10 * 1.0 + d_gini * 100 + max(0.0, d_holders) * 0.1
        cands.append({
            "token": token, "chain": chain, "snapshots": len(s),
            "top10_now": round(t10_1, 1), "top10_delta": round(d_top10, 1),
            "gini_delta": round(d_gini, 3), "holders": hc1,
            "holders_delta_pct": round(d_holders, 1), "score": round(score, 1),
        })
    cands.sort(key=lambda c: -c["score"])
    return cands


def confirm_operators(cands: list[dict], top: int = 8) -> list[dict]:
    """Confirm the top concentration-rising candidates with effective_concentration_signal
    (CEX/disperser guard included). Returns those with a real hidden-entity signal."""
    from src.onchain.holder_snapshot import (fetch_holders_evm, fetch_holders_solana)
    from src.pipeline.anomaly_screener import effective_concentration_signal
    confirmed = []
    for c in cands[:top]:
        token, chain = c["token"], c["chain"]
        try:
            if chain in ("solana", "sol"):
                holders = fetch_holders_solana(token, max_pages=6)
            else:
                cid = {"bsc": 56, "ethereum": 1, "base": 8453}.get(chain, 56)
                holders = fetch_holders_evm(token, chain_id=cid, max_pages=5)
            if not holders:
                continue
            sig = effective_concentration_signal(holders, token, chain)
        except Exception as e:
            logger.debug("confirm_failed", token=token, error=str(e))
            continue
        if sig and sig.get("largest_entity_pct", 0) >= 8 and (sig.get("concentration_gap", 0) or 0) >= 3:
            confirmed.append({**c, **sig})
    confirmed.sort(key=lambda x: -(x.get("concentration_gap", 0) or 0))
    return confirmed


def format_candidates(cands: list[dict], top: int = 15) -> str:
    lines = ["=" * 60, "持币集中度趋势筛选 — 浮筹被吸走(不靠trending)", "=" * 60]
    if not cands:
        lines.append("无候选(快照历史不足或无上升集中度)")
        return "\n".join(lines)
    for i, c in enumerate(cands[:top], 1):
        lines.append(
            f"{i:2}. {c['token'][:42]} [{c['chain']}]  分{c['score']}\n"
            f"    top10 {c['top10_now']}% (+{c['top10_delta']}pp) gini{c['gini_delta']:+.3f} "
            f"持币{c['holders']}({c['holders_delta_pct']:+.0f}%) 快照{c['snapshots']}")
    return "\n".join(lines)


def main():
    from dotenv import load_dotenv
    from src.config import PROJECT_ROOT
    load_dotenv(PROJECT_ROOT / ".env")
    cands = screen_holder_growth()
    print(format_candidates(cands))
    logger.info("holder_growth_screen", candidates=len(cands))


if __name__ == "__main__":
    main()
