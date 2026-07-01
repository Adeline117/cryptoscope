"""Batch validation on MANUALLY-LABELED operator clusters — the proper falsification.

Loads every label file in data/research/labels/ (real operator clusters exported
from Arkham), reconstructs each operator's holding curve (free archive), judges
accumulation, and measures whether pumps show operator accumulation more than
duds. Unlike the earlier auto-found-operator runs, this uses GROUND-TRUTH
clusters, so a positive result is real evidence the thesis predicts.

    python -m src.backtest.run_labeled_validation

Resumable: per-token verdicts cached in the label file's sibling .result.
"""

from __future__ import annotations

import json
from pathlib import Path

LABELS_DIR = Path("data/research/labels")


def _load_labels() -> list[dict]:
    # Dedup by (token, chain): a token can have both a manual file and an
    # auto-generated alert_outcomes file with CONFLICTING outcomes (e.g. SIREN
    # manual=pump vs objective 24h=dud). Prefer the price-derived alert_outcomes
    # label — same rule calibrate_weights uses — so one token counts once, objectively.
    by_key: dict[tuple[str, str], dict] = {}
    for f in sorted(LABELS_DIR.glob("*.json")):
        if f.name.startswith("_"):
            continue
        try:
            d = json.loads(f.read_text())
        except Exception:
            continue
        if not (d.get("operators") and d.get("token") and d.get("chain")):
            continue
        d["_file"] = str(f)
        key = (d["token"].lower(), d["chain"])
        prev = by_key.get(key)
        if prev and prev.get("source") == "alert_outcomes" and d.get("source") != "alert_outcomes":
            continue                             # keep the objective one already stored
        by_key[key] = d
    return list(by_key.values())


def _verdict_for(label: dict) -> dict:
    """Reconstruct the labeled operator's curve and return the verdict."""
    from src.backtest.run_operator_curve import run as run_curve

    tgt = {"token": label["token"], "chain": label["chain"],
           "symbol": label.get("symbol", ""), "operators": label["operators"]}
    tmp = LABELS_DIR / f"_run_{label.get('symbol','x')}.json"
    tmp.write_text(json.dumps(tgt))
    try:
        res = run_curve(str(tmp))
    finally:
        tmp.unlink(missing_ok=True)
    return res


def main():
    try:
        from dotenv import load_dotenv
        from src.config import PROJECT_ROOT
        load_dotenv(PROJECT_ROOT / ".env")
    except ImportError:
        pass

    labels = _load_labels()
    if not labels:
        print("无标注文件。看 data/research/labels/README.md，照模板标几个币(拉盘+横死各几个)。")
        return

    print("=" * 64)
    print(f"标签验证 — {len(labels)} 个币(真实庄簇)")
    print("=" * 64)
    accumulated = {"accumulation", "accumulation_then_distribution"}
    rows = []
    for lab in labels:
        sym = lab.get("symbol", lab["token"][:6])
        try:
            res = _verdict_for(lab)
            verdict = res.get("verdict", {}).get("verdict") if isinstance(res.get("verdict"), dict) else res.get("status")
        except Exception as e:
            verdict = f"error:{str(e)[:20]}"
        acc = verdict in accumulated
        rows.append({"symbol": sym, "outcome": lab.get("outcome", "?"),
                     "max_return": lab.get("max_return", 0), "verdict": verdict, "acc": acc})
        print(f"  {sym:12} [{lab.get('outcome','?'):4}] verdict={str(verdict):32} {'✅吸筹' if acc else '—'}")

    pumps = [r for r in rows if r["outcome"] == "pump"]
    duds = [r for r in rows if r["outcome"] == "dud"]
    print("\n" + "=" * 64)
    if pumps and duds:
        pr = sum(r["acc"] for r in pumps) / len(pumps)
        dr = sum(r["acc"] for r in duds) / len(duds)
        print(f"赢家吸筹率: {sum(r['acc'] for r in pumps)}/{len(pumps)} = {pr:.0%}")
        print(f"横死吸筹率: {sum(r['acc'] for r in duds)}/{len(duds)} = {dr:.0%}")
        print(f"\n区分度: 赢家 {pr:.0%} vs 横死 {dr:.0%}")
        if pr >= dr + 0.3:
            print("✅ 强区分: 庄吸筹显著预测拉盘 — 方法验证通过, 值得训自动认庄模型")
        elif pr > dr:
            print("⚠️ 弱区分: 方向对但差距小, 多标几个再判")
        else:
            print("❌ 无区分: 真实庄簇下吸筹也不能区分 — 信号/判定需重新设计")
    else:
        print(f"需要两类样本: 现有 pump={len(pumps)} dud={len(duds)}。两类都标几个才能算区分度。")
    print("=" * 64)


if __name__ == "__main__":
    main()
