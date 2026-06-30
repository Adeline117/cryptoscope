"""Assemble historical samples from a target list and run the threshold sweep.

    python -m src.backtest.run_historical

Reads data/backtest_targets.json (chain/address/symbol), reconstructs each
token's accumulation-window concentration series + realized max return, then
runs walk_forward.sweep over the assembled samples and prints the result.

Honest caveats printed with the output: chain coverage (EVM-only on free keys),
sampling bias (targets come from currently-liquid pools, so survivor-leaning),
and the look-ahead reduction via the observation window.
"""

from __future__ import annotations

import json
import time
from pathlib import Path

import structlog

from src.backtest.historical import build_historical_sample
from src.backtest.walk_forward import evaluate, sweep_thresholds

logger = structlog.get_logger()

TARGETS_FILE = Path("data/backtest_targets.json")
SAMPLES_FILE = Path("data/backtest_samples.json")

# Infrastructure / stablecoins / wrapped assets — never accumulation targets.
# Single source of truth shared with the screener + operator hunt.
from src.onchain.token_registry import NON_OPERATOR_SYMBOLS as EXCLUDE_SYMBOLS


def load_targets() -> list[dict]:
    if not TARGETS_FILE.exists():
        return []
    targets = json.loads(TARGETS_FILE.read_text())
    out = []
    seen = set()
    for t in targets:
        sym = (t.get("symbol") or "").upper()
        key = (t.get("chain"), t.get("address"))
        if sym in EXCLUDE_SYMBOLS or key in seen or not t.get("address"):
            continue
        seen.add(key)
        out.append(t)
    return out


def build_samples(limit: int = 25) -> list[dict]:
    targets = load_targets()[:limit]
    samples = []
    for i, t in enumerate(targets):
        sym = t.get("symbol", "")
        try:
            s = build_historical_sample(t["address"], t["chain"], symbol=sym)
        except Exception as e:
            logger.warning("sample_build_failed", symbol=sym, error=str(e))
            s = None
        if s:
            samples.append(s)
            print(f"  [{i+1}/{len(targets)}] {sym:12} {t['chain']:8} "
                  f"max_return={s['max_return']:.2f}x  "
                  f"eff_series={[round(x) for x in s['features']['effective_series']]}")
        else:
            print(f"  [{i+1}/{len(targets)}] {sym:12} {t['chain']:8} — skipped (no data)")
        time.sleep(2.5)  # GeckoTerminal free-tier rate limit
    SAMPLES_FILE.write_text(json.dumps(samples, indent=2))
    return samples


def main():
    try:
        from dotenv import load_dotenv

        from src.config import PROJECT_ROOT

        load_dotenv(PROJECT_ROOT / ".env")
    except ImportError:
        pass

    print("=" * 60)
    print("历史回测 — 重建吸筹序列 + 真实 max_return")
    print("=" * 60)
    samples = build_samples()
    n = len(samples)
    if n < 5:
        print(f"\n样本太少 ({n})，无法得出有意义的数字。")
        return

    launchers = sum(1 for s in samples if s["max_return"] >= 2.0)
    print(f"\n样本: {n}  |  其中 ≥2x(算启动): {launchers}  "
          f"|  基础启动率: {launchers/n:.0%}")

    # All samples are independent tokens → cross-sectional eval (cutoff before all).
    base = evaluate(samples, cutoff_ts="2000-01-01")
    print(f"\n默认阈值: precision={base.precision:.0%}  recall={base.recall:.0%}  "
          f"触发={base.fired}  TP={base.tp} FP={base.fp}")
    if base.survivorship_warning:
        print("  ⚠️ 幸存者偏差警告：样本里启动率过高（目标来自当前活跃池，偏幸存者）")

    print("\n阈值扫描 (前 5 组，按 precision 排序):")
    sweep = sweep_thresholds(samples, cutoff_ts="2000-01-01")
    for r in sweep[:5]:
        print(f"  gap≥{r['min_gap_slope']} eff≥{r['min_eff_level']} float≥{r['min_float']}"
              f"  → precision={r['precision']:.0%} 触发={r['fired']} TP={r['tp']} FP={r['fp']}")

    print("\n" + "=" * 60)
    print("诚实caveats:")
    print("- 链覆盖：免费 key 只能重建 EVM(ETH/Base/...)，BNB/Solana 妖币重建不了")
    print("- 采样偏差：目标取自当前活跃池 → 偏幸存者，启动率虚高")
    print("- 已用前60%生命周期做特征以减前视，但价格结果仍含完整历史")
    print("- 这是方向性 sanity check，不是干净的 walk-forward")
    print("=" * 60)


if __name__ == "__main__":
    main()
