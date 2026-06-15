"""Definitive falsification: does operator accumulation separate winners from duds?

For each token (with a known realized max_return), autonomously find the operator
cluster, reconstruct its combined holding curve, and judge accumulation. Then test:
do high-return tokens show operator accumulation more than low-return ones?

Incremental: each token's result is appended to data/research/falsification.json,
so a timeout/rate-limit mid-run never loses completed work (re-run resumes).
"""

from __future__ import annotations

import json
from pathlib import Path

import structlog

logger = structlog.get_logger()

SAMPLES = Path("data/backtest_samples.json")
OUT = Path("data/research/falsification.json")


def _load_done() -> dict:
    if OUT.exists():
        try:
            return {r["token"]: r for r in json.loads(OUT.read_text())}
        except Exception:
            return {}
    return {}


def _save(done: dict) -> None:
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(list(done.values()), indent=2))


def run() -> list[dict]:
    from src.onchain.operator_finder import find_operator_cluster
    from src.onchain.evm_archive import ArchiveRPC, operator_curve_evm
    from src.backtest.operator_curve import analyze_curve

    samples = json.loads(SAMPLES.read_text())
    done = _load_done()

    for s in samples:
        token, chain, sym = s["token"], s["chain"], s.get("symbol", "")
        mret = s.get("max_return", 0)
        if token in done:
            continue
        rec = {"token": token, "symbol": sym, "chain": chain, "max_return": mret}
        try:
            found = find_operator_cluster(token, chain)
            if not found or not found.get("operator"):
                rec["verdict"] = "no_operator"
            else:
                ops = found["operator"]
                rpc = ArchiveRPC(chain)
                latest = rpc.latest_block()
                # Reconstruct over the token's life (broad window).
                curve = operator_curve_evm(token, ops, chain,
                                           from_block=max(1, latest - 30_000_000),
                                           to_block=latest, n_points=10)
                if curve:
                    v = analyze_curve({"share_series": curve["balance_series"],
                                       "n_operator_seen": len(ops),
                                       "n_operator_addresses": len(ops)})
                    rec["verdict"] = v["verdict"]
                    rec["operator_size"] = len(ops)
                    rec["operator_share_pct"] = found.get("share_pct")
                    rec["peak"] = v["share_peak_pct"]
                else:
                    rec["verdict"] = "no_curve"
        except Exception as e:
            logger.warning("falsification_token_failed", symbol=sym, error=str(e))
            rec["verdict"] = "error"
        done[token] = rec
        _save(done)
        print(f"  {sym:12} ret={mret:>7.1f}x  verdict={rec['verdict']}")

    return list(done.values())


def report(results: list[dict]) -> None:
    accumulated = {"accumulation", "accumulation_then_distribution"}
    print("\n" + "=" * 60)
    print("证伪结果: 庄吸筹 vs 真实回报")
    print("=" * 60)
    rows = sorted(results, key=lambda r: -r.get("max_return", 0))
    for r in rows:
        flag = "✅吸筹" if r.get("verdict") in accumulated else "—"
        print(f"  {r['symbol']:12} {r.get('max_return',0):>7.1f}x  {r.get('verdict','?'):30} {flag}")

    winners = [r for r in results if r.get("max_return", 0) >= 2.0]
    duds = [r for r in results if r.get("max_return", 0) < 2.0]
    w_acc = sum(1 for r in winners if r.get("verdict") in accumulated)
    d_acc = sum(1 for r in duds if r.get("verdict") in accumulated)
    print("\n  赢家(≥2x):", f"{w_acc}/{len(winners)} 显示吸筹")
    print("  横死(<2x):", f"{d_acc}/{len(duds)} 显示吸筹")
    if winners and duds:
        w_rate = w_acc / len(winners)
        d_rate = d_acc / len(duds)
        print(f"\n  → 赢家吸筹率 {w_rate:.0%} vs 横死吸筹率 {d_rate:.0%}")
        if w_rate > d_rate + 0.2:
            print("  ✅ 有区分力: 赢家显著更常出现庄吸筹")
        elif w_rate > d_rate:
            print("  ⚠️ 弱区分: 方向对但差距小, 样本不足以下定论")
        else:
            print("  ❌ 无区分力: 此样本上吸筹不能区分赢家/横死")
    print("\n  ⚠️ caveats: 样本小且偏幸存者; 自动认庄弱于 Arkham; 方向性参考")
    print("=" * 60)


def main():
    try:
        from dotenv import load_dotenv
        from src.config import PROJECT_ROOT
        load_dotenv(PROJECT_ROOT / ".env")
    except ImportError:
        pass
    results = run()
    report(results)


if __name__ == "__main__":
    main()
