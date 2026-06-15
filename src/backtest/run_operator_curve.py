"""One-click operator accumulation curve.

Drop in any operator address cluster (e.g. from Arkham's free web export) + a
token + chain, and this reconstructs the cluster's combined holding over time and
judges whether it shows accumulation that preceded a launch — all on free data
(EVM archive RPCs / Helius for Solana).

Usage:
    python -m src.backtest.run_operator_curve data/research/<target>.json

Target JSON:
    {"token": "0x...", "chain": "bsc", "symbol": "SIREN",
     "operators": ["0x...", "0x...", ...]}

The EVM path AUTO-LOCATES the accumulation window (coarse scan for where the
cluster balance rises from ~0), so you don't have to know block numbers.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import structlog

logger = structlog.get_logger()


def _find_window_evm(rpc, token, ops, latest, probes=10):
    """Coarse scan to find [from_block, to_block] spanning the accumulation.

    Walks back in big steps until the cluster balance hits ~0 (pre-accumulation),
    returning a window from just before that to the latest block.
    """
    span = latest  # search the whole chain history coarsely
    step = span // probes
    last_zero = 0
    # Probe the FULL operator set back through history; first block with ~0 total
    # is pre-accumulation. Start the window one step earlier than that for margin.
    for i in range(1, probes + 1):
        blk = max(1, latest - step * i)
        total = sum((rpc.balance_of(token, a, blk) or 0) for a in ops)
        logger.info("window_probe", block=blk, total=round(total))
        if total <= 0:
            last_zero = blk
            break
    from_block = max(1, last_zero - step) if last_zero else max(1, latest - span // 2)
    return from_block, latest


def run(target_path: str) -> dict:
    tgt = json.loads(Path(target_path).read_text())
    token, chain = tgt["token"], tgt["chain"]
    symbol = tgt.get("symbol", token[:6])
    ops = tgt.get("operators")
    from src.backtest.operator_curve import analyze_curve

    # Auto-discover the operator cluster when none is provided (ETH only, free).
    if not ops:
        from src.onchain.operator_finder import find_operator_cluster

        found = find_operator_cluster(token, chain)
        if not found or not found.get("operator"):
            return {"status": "no_operator_found", "symbol": symbol}
        ops = found["operator"]
        logger.info("auto_operator", symbol=symbol, count=len(ops), share=found.get("share_pct"))

    if chain in ("solana", "sol"):
        from src.backtest.operator_curve import operator_curve_solana

        curve = operator_curve_solana(token, ops)
        series = curve["share_series"] if curve else []
    else:
        from src.onchain.evm_archive import ArchiveRPC, operator_curve_evm

        rpc = ArchiveRPC(chain)
        if not rpc.available():
            return {"status": "no_rpc", "chain": chain}
        latest = rpc.latest_block()
        from_block, to_block = _find_window_evm(rpc, token, ops, latest)
        logger.info("evm_window", from_block=from_block, to_block=to_block)
        curve = operator_curve_evm(token, ops, chain, from_block=from_block,
                                   to_block=to_block, n_points=12)
        series = curve["balance_series"] if curve else []

    if not curve:
        return {"status": "no_data", "symbol": symbol}

    verdict = analyze_curve({
        "share_series": series,
        "n_operator_seen": len(ops), "n_operator_addresses": len(ops),
    })
    out = {"status": "complete", "symbol": symbol, "chain": chain,
           "series": series, "verdict": verdict}
    Path(target_path).with_suffix(".curve.json").write_text(json.dumps(out, indent=2))
    return out


def main():
    if len(sys.argv) < 2:
        print("usage: python -m src.backtest.run_operator_curve <target.json>")
        return
    try:
        from dotenv import load_dotenv
        from src.config import PROJECT_ROOT
        load_dotenv(PROJECT_ROOT / ".env")
    except ImportError:
        pass

    res = run(sys.argv[1])
    print("=" * 56)
    print(f"庄持仓曲线 · {res.get('symbol')} ({res.get('chain')})")
    print("=" * 56)
    if res["status"] != "complete":
        print("结果:", res)
        return
    s = res["series"]
    print("持仓序列:", [round(x / 1e6, 1) for x in s] if max(s) > 1e6 else [round(x, 2) for x in s])
    v = res["verdict"]
    print(f"判定: {v['verdict']} | 涨到峰值:{v.get('rose_to_peak')} | 峰后派发:{v.get('distributed_after_peak')}")
    print(f"持仓: 起{v['share_start_pct']:,.0f} → 峰{v['share_peak_pct']:,.0f} → 末{v['share_end_pct']:,.0f}")


if __name__ == "__main__":
    main()
