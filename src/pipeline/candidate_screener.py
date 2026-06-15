"""Automatic candidate screener — find tokens worth a deep operator look.

This is the funnel's entry: a cheap, free, automatable scan that surfaces tokens
showing the *coordination precursor* (a batch-funder operator cluster holding a
meaningful share), ranked, so the expensive per-token operator-curve analysis
only runs on the promising few.

It does NOT predict pumps (proven not viable on raw signals). It answers a cheaper
question: "which tokens have a coordinated operator cluster accumulating a real
share right now?" — using the validated batch-funder clustering (bw_flag, >=5).

Per token: reconstruct top holders → resolve funders → cluster (min_batch_funder
=5) → measure (a) largest operator entity's share, (b) share held by all
batch-funded wallets. Rank by these. EVM only (free funder resolution = ETH).

Incremental: writes data/research/candidates.json as it goes (resumable).
"""

from __future__ import annotations

import json
from pathlib import Path

import structlog

logger = structlog.get_logger()

OUT = Path("data/research/candidates.json")


def screen_token(token: str, chain: str, symbol: str = "", top_n: int = 60) -> dict:
    """Compute the coordination metrics for one token."""
    from src.onchain.holder_snapshot import fetch_holders_evm
    from src.onchain.entity_clustering import (
        cluster_addresses, batch_funder_flags, _exchange_addresses, _norm,
    )
    from src.onchain.funder_graph import get_funders

    rec = {"token": token, "chain": chain, "symbol": symbol}
    if chain not in ("ethereum", "eth"):
        return rec | {"status": "unsupported_chain"}

    holders = fetch_holders_evm(token, chain_id=1, max_pages=20)
    if len(holders) < 10:
        return rec | {"status": "too_few_holders"}
    holders.sort(key=lambda h: h["balance"], reverse=True)
    top = holders[:top_n]
    addrs = [_norm(h["address"]) for h in top]
    bal = {_norm(h["address"]): h["balance"] for h in top}
    total = sum(h["balance"] for h in holders) or 1.0

    funders = get_funders(addrs, "ethereum", max_lookups=top_n)
    exclude = _exchange_addresses()

    # Largest operator entity share (validated >=5 batch-funder clustering).
    mapping = cluster_addresses(addrs, funders=funders, exclude=exclude,
                                balances=bal, min_batch_funder=5)
    by_ent: dict[str, list[str]] = {}
    for a, e in mapping.items():
        by_ent.setdefault(e, []).append(a)
    best = max(by_ent.values(), key=lambda g: sum(bal.get(a, 0) for a in g)) if by_ent else []
    op_share = round(sum(bal.get(a, 0) for a in best) / total * 100, 2)

    # Share held by ALL batch-funded (coordinated) wallets.
    flags = batch_funder_flags(addrs, funders, min_batch=5, exclude=exclude)
    coord = [a for a in addrs if flags.get(a, {}).get("has_batch_funder")]
    coord_share = round(sum(bal.get(a, 0) for a in coord) / total * 100, 2)

    return rec | {
        "status": "ok",
        "operator_cluster_size": len(best),
        "operator_share_pct": op_share,
        "coordinated_wallets": len(coord),
        "coordinated_share_pct": coord_share,
        # candidate score: how much supply sits in coordinated/operator hands
        "score": round(max(op_share, coord_share), 2),
    }


def run(targets: list[dict], limit: int = 40) -> list[dict]:
    done = {}
    if OUT.exists():
        try:
            done = {r["token"]: r for r in json.loads(OUT.read_text())}
        except Exception:
            done = {}
    for t in targets[:limit]:
        if t["address"] in done:
            continue
        try:
            r = screen_token(t["address"], t["chain"], t.get("symbol", ""))
        except Exception as e:
            logger.warning("screen_failed", symbol=t.get("symbol"), error=str(e))
            r = {"token": t["address"], "symbol": t.get("symbol", ""), "status": "error"}
        done[t["address"]] = r
        OUT.parent.mkdir(parents=True, exist_ok=True)
        OUT.write_text(json.dumps(list(done.values()), indent=2))
        if r.get("status") == "ok":
            print(f"  {r['symbol']:12} 操作者簇 {r['operator_cluster_size']:>2}个/{r['operator_share_pct']:>5}%  "
                  f"协同钱包 {r['coordinated_wallets']:>2}个/{r['coordinated_share_pct']:>5}%  score={r['score']}")
        else:
            print(f"  {r.get('symbol',''):12} — {r.get('status')}")
    return list(done.values())


def main():
    try:
        from dotenv import load_dotenv
        from src.config import PROJECT_ROOT
        load_dotenv(PROJECT_ROOT / ".env")
    except ImportError:
        pass
    tf = Path("data/backtest_targets.json")
    targets = json.loads(tf.read_text()) if tf.exists() else []
    if not targets:
        print("无目标; 先生成 data/backtest_targets.json")
        return
    print("=" * 64)
    print("候选筛选 — 按协同/操作者簇占比排序(验证过的 batch-funder)")
    print("=" * 64)
    results = run(targets)
    ranked = sorted([r for r in results if r.get("status") == "ok"],
                    key=lambda r: -r.get("score", 0))
    print("\n" + "=" * 64)
    print("候选排名(score = 协同/操作者占供应%, 越高越值得深查):")
    for i, r in enumerate(ranked[:10], 1):
        print(f"  {i:>2}. {r['symbol']:12} score={r['score']:>5}  "
              f"(操作者簇{r['operator_cluster_size']}个持{r['operator_share_pct']}%)")
    print("\n→ 对 top 候选跑深度: run_operator_curve 看是否吸筹式建仓")
    print("=" * 64)


if __name__ == "__main__":
    main()
