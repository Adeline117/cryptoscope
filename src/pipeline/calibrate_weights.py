"""Auto-calibrate screener signal weights from labeled outcomes.

Ready to run the moment enough labels exist. It learns which signals actually
discriminate pumps from duds and rewrites config/screener_weights.json, so the
screener score stops being hand-guessed and becomes empirical.

Method (per signal s):
    pump_rate = P(s fired | token pumped)
    dud_rate  = P(s fired | token was a dud)
    weight ∝ how much more often s fires for pumps than duds (discriminative power)
A signal common in pumps but rare in duds → high weight; equally common in both
→ ~0 (non-discriminative); more common in duds → 0 (floored).

Inputs:
  - labels: data/research/labels/*.json  (operator outcome: pump/dud)
  - emissions: screener_state.db  (which signals fired for each token, logged by
    the screener each run)

    python -m src.pipeline.calibrate_weights        # report + write if ready
    python -m src.pipeline.calibrate_weights --dry  # report only

Guard: requires >=5 pump and >=5 dud labeled tokens (else refuses — too few to
calibrate). Until then it prints what's missing.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import structlog

logger = structlog.get_logger()

LABELS_DIR = Path("data/research/labels")
MIN_PER_CLASS = 5
W_MAX, W_MIN = 50, 0


def _load_labels() -> dict[tuple[str, str], str]:
    """(token_lower, chain) -> outcome ('pump'/'dud').

    On conflict, a price-derived label (source='alert_outcomes') WINS over a
    legacy/manual file — a loose file can carry a stale outcome (e.g. siren.json
    tagged SIREN 'pump' before it dumped); the resolved-price label is ground truth."""
    out: dict[tuple[str, str], str] = {}
    src: dict[tuple[str, str], str] = {}
    if not LABELS_DIR.exists():
        return out
    for f in LABELS_DIR.glob("*.json"):
        if f.name.startswith("_"):
            continue
        try:
            d = json.loads(f.read_text())
        except Exception:
            continue
        tok, chain, outcome = d.get("token"), d.get("chain"), d.get("outcome")
        if tok and chain and outcome in ("pump", "dud"):
            key = (tok.lower(), chain)
            this_src = d.get("source")
            if key in out and src.get(key) == "alert_outcomes" and this_src != "alert_outcomes":
                continue          # don't let a legacy file override ground truth
            out[key] = outcome
            src[key] = this_src
    return out


def _emission_reasons() -> dict[tuple[str, str], set[str]]:
    """(token_lower, chain) -> union of signals ever fired for it."""
    from src.pipeline.anomaly_screener import _state_db

    fired: dict[tuple[str, str], set[str]] = {}
    try:
        conn = _state_db()
        try:
            rows = conn.execute("SELECT token, chain, reasons FROM emissions").fetchall()
        finally:
            conn.close()
    except Exception as e:
        logger.warning("emissions_read_failed", error=str(e))
        return fired
    for tok, chain, reasons in rows:
        try:
            rs = set(json.loads(reasons) if reasons else [])
        except Exception:
            rs = set()
        fired.setdefault((str(tok).lower(), chain), set()).update(rs)
    return fired


def _sentinel_clusters() -> dict[tuple[str, str], list[str]]:
    """(token_lower, chain) -> operator wallets, from the sentinel registry. Lets
    auto-generated alert_outcomes labels carry a cluster so run_labeled_validation
    can reconstruct the operator curve (else they're outcome-only, unusable there)."""
    from src.config import DATA_DIR
    reg = DATA_DIR / "operator_sentinels.json"
    if not reg.exists():
        return {}
    try:
        d = json.loads(reg.read_text())
    except Exception:
        return {}
    out: dict[tuple[str, str], list[str]] = {}
    for s in (d.values() if isinstance(d, dict) else d):
        tok, chain, w = s.get("token"), s.get("chain"), s.get("wallets")
        if tok and chain and w:
            out[(tok.lower(), chain)] = w
    return out


def generate_labels(pump_thr: float = 1.15, dud_thr: float = 0.95) -> dict:
    """Derive pump/dud labels from RESOLVED outcomes so calibration has data to learn
    from — the missing plumbing (0 labels existed, so calibrate_weights never ran).
    Source: alert_outcomes.db (token+chain+price0+price_24h). 24h return >= +15% =
    pump, <= -5% = dud, in-between skipped (ambiguous). Writes one idempotent label
    file per token+chain. Labels accrue as the system runs; calibration auto-activates
    once >=5 pump + >=5 dud exist. (signal_scorecard is symbol-keyed, not address-
    keyed, so it can't be matched to address-keyed emissions — alert_outcomes is the
    usable source.)"""
    import sqlite3

    from src.config import DATA_DIR
    db = DATA_DIR / "alert_outcomes.db"
    if not db.exists():
        return {"written": 0, "reason": "no alert_outcomes.db"}
    try:
        conn = sqlite3.connect(str(db))
        try:
            rows = conn.execute(
                "SELECT token, chain, symbol, price0, price_24h FROM alerts "
                "WHERE resolved=1 AND chain != 'majors' AND price0 > 0 AND price_24h > 0"
            ).fetchall()
        finally:
            conn.close()
    except Exception as e:
        logger.warning("alert_outcomes_read_failed", error=str(e))
        return {"written": 0, "reason": str(e)[:60]}
    by_tok: dict[tuple[str, str], list[float]] = {}
    sym_of: dict[tuple[str, str], str] = {}
    for tok, chain, sym, p0, p24 in rows:
        if not tok or not chain or not p0:
            continue
        by_tok.setdefault((tok, chain), []).append(p24 / p0)
        sym_of[(tok, chain)] = sym or ""
    LABELS_DIR.mkdir(parents=True, exist_ok=True)
    clusters = _sentinel_clusters()            # attach operator wallets where we have them
    written = 0
    for (tok, chain), rets in by_tok.items():
        rets.sort()
        med = rets[len(rets) // 2]                 # median 24h return across its alerts
        outcome = "pump" if med >= pump_thr else "dud" if med <= dud_thr else None
        if not outcome:
            continue
        rec = {"token": tok, "chain": chain, "symbol": sym_of.get((tok, chain), ""),
               "outcome": outcome, "source": "alert_outcomes", "ret_24h": round(med, 3)}
        ops = clusters.get((tok.lower(), chain))
        if ops:                                    # makes the label usable by run_labeled_validation
            rec["operators"] = ops
        (LABELS_DIR / f"{chain}_{tok.lower()}.json").write_text(
            json.dumps(rec, ensure_ascii=False))
        written += 1
    logger.info("labels_generated", written=written)
    return {"written": written}


def calibrate(dry: bool = False) -> dict:
    from src.pipeline.anomaly_screener import DEFAULT_WEIGHTS

    generate_labels()        # refresh labels from resolved outcomes first
    labels = _load_labels()
    pumps = [k for k, v in labels.items() if v == "pump"]
    duds = [k for k, v in labels.items() if v == "dud"]
    if len(pumps) < MIN_PER_CLASS or len(duds) < MIN_PER_CLASS:
        return {"status": "not_ready", "pump": len(pumps), "dud": len(duds),
                "need": MIN_PER_CLASS}

    fired = _emission_reasons()
    # Tokens that have BOTH a label and screener emissions.
    pump_set = [k for k in pumps if k in fired]
    dud_set = [k for k in duds if k in fired]
    if len(pump_set) < MIN_PER_CLASS or len(dud_set) < MIN_PER_CLASS:
        return {"status": "no_emissions", "labeled_pump": len(pumps),
                "labeled_dud": len(duds), "with_emissions_pump": len(pump_set),
                "with_emissions_dud": len(dud_set),
                "note": "labels exist but screener hasn't logged emissions for them yet"}

    signals = set(DEFAULT_WEIGHTS)
    for s in fired.values():
        signals |= s

    weights, report = {}, []
    for sig in sorted(signals):
        pr = sum(1 for k in pump_set if sig in fired[k]) / len(pump_set)
        dr = sum(1 for k in dud_set if sig in fired[k]) / len(dud_set)
        # discriminative power + baseline credit for firing in pumps
        raw = 45 * (pr - dr) + 15 * pr
        w = int(round(max(W_MIN, min(W_MAX, raw))))
        weights[sig] = w
        report.append({"signal": sig, "pump_rate": round(pr, 2),
                       "dud_rate": round(dr, 2), "weight": w})

    report.sort(key=lambda r: -r["weight"])
    result = {"status": "calibrated", "pump_n": len(pump_set), "dud_n": len(dud_set),
              "weights": weights, "report": report}

    if not dry:
        from src.config import CONFIG_DIR
        (CONFIG_DIR / "screener_weights.json").write_text(json.dumps(weights, indent=2))
        result["written"] = str(CONFIG_DIR / "screener_weights.json")
    return result


def main():
    dry = "--dry" in sys.argv
    res = calibrate(dry=dry)
    print("=" * 60)
    print("筛选器权重自动校准")
    print("=" * 60)
    if res["status"] == "not_ready":
        print(f"标签不足: 拉盘 {res['pump']}/{res['need']} · 横死 {res['dud']}/{res['need']}")
        print("→ 继续标注(尤其横死组),够 5+5 再跑。")
        return
    if res["status"] == "no_emissions":
        print(f"有标签但筛选器还没记录它们的触发:")
        print(f"  拉盘 {res['labeled_pump']}(有记录{res['with_emissions_pump']}) · "
              f"横死 {res['labeled_dud']}(有记录{res['with_emissions_dud']})")
        print("→ 这些币要先被筛选器扫到并记录(emissions),或确保地址/链与标签一致。")
        return
    print(f"样本: 拉盘 {res['pump_n']} · 横死 {res['dud_n']}\n")
    print(f"{'信号':22} {'拉盘率':>6} {'横死率':>6} {'权重':>5}")
    for r in res["report"]:
        print(f"  {r['signal']:20} {r['pump_rate']:>6.0%} {r['dud_rate']:>6.0%} {r['weight']:>5}")
    if res.get("written"):
        print(f"\n✅ 已写入 {res['written']} — 筛选器下次跑自动用校准后权重")
    else:
        print("\n(--dry: 未写入)")


if __name__ == "__main__":
    main()
