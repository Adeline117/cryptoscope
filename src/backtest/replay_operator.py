"""Walk-forward replay of identify_operator — the only route to a quotable n.

Live alerts accrue ~16 independent episodes in 24 days. To distinguish a true 55%
from a ~30% base rate at 80% power needs ~120-150. Replay is how we get there:
ask the verdict engine what it would have said at block B, then look up what the
price actually did from B to B+horizon.

Three rules this file exists to enforce:

1. NO LEAK. The verdict is produced with `as_of_block=B`; every read past B is cut
   off (see operator_id._before and tests/test_replay_no_leak.py). The OUTCOME is
   read from price data strictly after B.

2. NO SURVIVORSHIP. Sampling tokens that exist today and replaying their past means
   every sample survived. `walk_forward.evaluate` already emits a survivorship
   warning when the base rate is implausible; we pass its output through and refuse
   to headline a precision that carries it.

3. NO BASE-RATE-FREE CLAIM. A verdict's precision is compared against the same
   token's own chance base rate over the same horizon (evidence.base_rate), using
   the same hit rule. A precision of 60% on a token that drops 5% by chance 55% of
   the time is not skill.

Replay is honestly WEAKER than live: the current holder graph and the market/terminal
gate cannot time-travel and are disabled, so only historical-ledger verdicts
(distributing / exited_by_selling / present_rotating_confirmed / indeterminate) are
reachable. That is the price of not lying.

    python -m src.backtest.replay_operator --token 0x... --chain bsc --points 6
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone

import structlog

from src.config import DATA_DIR

logger = structlog.get_logger()

OUT_PATH = DATA_DIR / "research" / "replay_samples.json"
# Which verdicts assert a direction, and which way.
SHORT_VERDICTS = {"distributing", "exited_by_selling", "present_rotating_confirmed"}
LONG_VERDICTS = {"loaded_accumulating"}


# Measured archive depth per chain (probe date 2026-07-10, keyless/Alchemy pools).
# An earlier note claimed "BSC ~30d, Base none". Both were wrong: the BSC probe used a
# wallet that genuinely held nothing 90d ago, and the Base probe predated the RPC fix.
# Never infer "no archive" from a zero balance — probe with a block that must have state.
ARCHIVE_DAYS = {"bsc": 240, "ethereum": 90, "base": 30}


def _blocks_for_dates(chain: str, days_ago: list[int]) -> dict[int, int]:
    """Map each `days_ago` to a block number, via measured seconds-per-block.

    Returns {} when the chain has no archive at all. Individual blocks are probed
    separately by `_archive_ok`: a chain can serve state at 30d and not at 90d, and
    replaying past its depth would read every balance as 0 and call every wallet
    exited — a fabricated `exited_by_selling` on every token."""
    from src.onchain.evm_archive import ArchiveRPC
    rpc = ArchiveRPC(chain)
    if not rpc.available():
        return {}
    head = rpc.latest_block()
    spb = rpc.seconds_per_block()
    if not spb or spb <= 0:
        return {}
    out = {}
    for d in days_ago:
        b = int(head - (d * 86400) / spb)
        if b > 0:
            out[d] = b
    return out


def _archive_ok(chain: str, token: str, block: int) -> bool:
    """Probe: does this chain actually serve historical state at `block`? A None here
    means 'no archive', and a replay on top of it would read every balance as 0 and
    call every wallet exited."""
    try:
        from src.onchain.evm_archive import ArchiveRPC
        rpc = ArchiveRPC(chain)
        probe = rpc.balance_of(token, "0x000000000000000000000000000000000000dEaD", block)
        return probe is not None
    except Exception:
        return False


def _price_at(chain: str, pool: str, ts: datetime) -> float | None:
    """Hourly close nearest to `ts` (GeckoTerminal, keyless)."""
    from src.pipeline.evidence import _ohlcv
    try:
        candles = _ohlcv(chain, pool)
    except Exception:
        return None
    target = int(ts.timestamp())
    best = None
    for c in candles:
        if best is None or abs(c[0] - target) < abs(best[0] - target):
            best = c
    if best is None or abs(best[0] - target) > 6 * 3600:
        return None                      # nearest candle >6h away → don't guess
    return best[4]


def replay(token: str, chain: str, days_ago: list[int], horizon_h: int = 24) -> list[dict]:
    """Replay the verdict at each historical point and attach the realized outcome."""
    from src.onchain.evm_archive import ArchiveRPC
    from src.onchain.operator_id import identify_operator
    from src.pipeline.evidence import _deepest_pool

    blocks = _blocks_for_dates(chain, days_ago)
    if not blocks:
        logger.warning("replay_no_archive", chain=chain, note="无archive,拒绝回放")
        return []
    pool = _deepest_pool(token, chain)
    if not pool:
        logger.warning("replay_no_pool", token=token)
        return []

    rpc = ArchiveRPC(chain)
    samples = []
    for d, blk in sorted(blocks.items(), reverse=True):
        if not _archive_ok(chain, token, blk):
            logger.warning("replay_archive_gap", chain=chain, block=blk, days_ago=d,
                           note="该区块无历史状态 → 跳过(不以0代余额)")
            continue
        bt = rpc.block_time(blk)
        if not bt:
            continue
        t0 = datetime.fromtimestamp(bt, timezone.utc)
        v = identify_operator(token, chain, as_of_block=blk)

        p0 = _price_at(chain, pool, t0)
        p1 = _price_at(chain, pool, t0 + timedelta(hours=horizon_h))
        sample = {"token": token, "chain": chain, "days_ago": d, "block": blk,
                  "ts": t0.isoformat(), "verdict": v["verdict"],
                  "confidence": v["confidence"],
                  "caveats": v["caveats"], "price0": p0, "price1": p1}
        if p0 and p1:
            sample["ret"] = (p1 - p0) / p0
        else:
            sample["ret"] = None          # unpriced → excluded from scoring, not zeroed
        samples.append(sample)
        logger.info("replay_point", token=token[:10], days_ago=d,
                    verdict=v["verdict"], ret=sample["ret"])
    return samples


def score(samples: list[dict], horizon_h: int = 24) -> dict:
    """Precision of the directional verdicts vs. the token's own chance base rate."""
    from src.pipeline.evidence import base_rate, wilson

    scored = [s for s in samples if s.get("ret") is not None]
    directional = [s for s in scored
                   if s["verdict"] in SHORT_VERDICTS | LONG_VERDICTS]
    if not directional:
        return {"n": 0, "note": "无方向性判决(回放下当前持仓图不可用,只能出历史台账判决)"}

    hits = 0
    expected = 0.0
    per_token_br: dict = {}
    for s in directional:
        want_short = s["verdict"] in SHORT_VERDICTS
        direction = "short" if want_short else "long"
        hit = (s["ret"] <= -0.05) if want_short else (s["ret"] >= 0.05)
        hits += int(hit)
        key = (s["token"], s["chain"], direction)
        if key not in per_token_br:
            # tradeable=False: we are measuring the DETECTOR, not the pool's depth.
            per_token_br[key] = base_rate(s["token"], s["chain"], direction, 0,
                                          horizon_h=horizon_h, tradeable=False)
        br = per_token_br[key]
        if br["available"]:
            expected += br["p"]
    n = len(directional)
    lo, hi = wilson(hits, n)
    out = {"n": n, "hits": hits, "precision": hits / n,
           "wilson": [round(lo, 3), round(hi, 3)],
           "expected_by_chance": round(expected, 2)}
    if expected > 0.5:
        out["lift"] = round(hits / expected, 2)
    if n < 30:
        out["warning"] = f"n={n} 远不足以断言胜率(需 ~120-150);仅方向性参考"

    # THE CATEGORY ERROR GUARD. identify_operator is a STATE classifier ("is there an
    # operator and what did they do"), not a TIMING signal ("will price fall in 24h").
    # SIREN replayed at 5 blocks over 25 days returned `distributing conf75` at every
    # single one. A constant output cannot time anything: its precision EQUALS the
    # base rate by construction, and the resulting lift ~= 1.0 looks like "slight
    # edge" when it is literally zero information. Scoring a standing state as if it
    # were an entry signal is how a state label becomes a fake trading number.
    per_token_verdicts: dict = {}
    for s in scored:
        per_token_verdicts.setdefault(s["token"], set()).add(s["verdict"])
    constant = [t for t, vs in per_token_verdicts.items() if len(vs) == 1 and len(
        [s for s in scored if s["token"] == t]) > 1]
    if constant:
        out["constant_verdict"] = True
        out["invalid_as_timing_signal"] = (
            "判决在所有回放点恒定不变 → 无时间信息。恒定输出的精度必然≈基准率,"
            "lift≈1 不是'弱edge'而是'零信息'。identify_operator 是状态分类器,"
            "不是择时信号;要测择时必须回测状态【跃迁】(首次翻转为 distributing),"
            "而非常驻状态。")
        out.pop("lift", None)      # refuse to publish a meaningless ratio
    return out


def transitions(samples: list[dict]) -> list[dict]:
    """The moments the verdict CHANGED — the only thing in a state classifier that
    carries timing information.

    A standing `distributing` says the operator has been selling for weeks; it cannot
    tell you today is the day. The FLIP into `distributing` is an event with a
    timestamp, and an event is the only thing you can be early to.

    The first sample per token is never a transition: we don't know the state before
    the replay window opened, and calling an unknown->X change an event would count
    every token's first observation as a signal.
    """
    by_token: dict = {}
    for s in samples:
        by_token.setdefault((s["token"], s["chain"]), []).append(s)
    out = []
    for key, group in by_token.items():
        group = sorted(group, key=lambda s: s["block"])       # chronological
        for prev, cur in zip(group, group[1:]):
            if cur["verdict"] != prev["verdict"]:
                out.append({**cur, "from_verdict": prev["verdict"],
                            "to_verdict": cur["verdict"],
                            "from_block": prev["block"]})
    return out


def score_transitions(samples: list[dict], horizon_h: int = 24) -> dict:
    """Precision of verdict FLIPS against the token's own chance base rate.

    This is the honest counterpart to `score()`: it asks whether the moment the
    detector changed its mind carried information, rather than whether a standing
    label correlates with a direction (it cannot — see score()'s constant-verdict
    guard).
    """
    from src.pipeline.evidence import base_rate, wilson

    trans = [t for t in transitions(samples) if t.get("ret") is not None]
    directional = [t for t in trans
                   if t["to_verdict"] in SHORT_VERDICTS | LONG_VERDICTS]
    if not directional:
        return {"n_transitions": len(trans), "n_directional": 0,
                "note": ("回放窗口内没有方向性跃迁。要么状态一直没变(需更长窗口/更密网格),"
                         "要么跃迁的目标状态不带方向。无跃迁 = 无择时信号可测,不是'无 edge'。")}

    hits, expected, br_cache = 0, 0.0, {}
    for t in directional:
        short = t["to_verdict"] in SHORT_VERDICTS
        direction = "short" if short else "long"
        hits += int((t["ret"] <= -0.05) if short else (t["ret"] >= 0.05))
        key = (t["token"], t["chain"], direction)
        if key not in br_cache:
            br_cache[key] = base_rate(t["token"], t["chain"], direction, 0,
                                      horizon_h=horizon_h, tradeable=False)
        if br_cache[key]["available"]:
            expected += br_cache[key]["p"]

    n = len(directional)
    lo, hi = wilson(hits, n)
    out = {"n_transitions": len(trans), "n_directional": n, "hits": hits,
           "precision": round(hits / n, 3), "wilson": [round(lo, 3), round(hi, 3)],
           "expected_by_chance": round(expected, 2),
           "flips": [f"{t['from_verdict']}→{t['to_verdict']} @{t['days_ago']}d "
                     f"ret={t['ret']:+.1%}" for t in directional]}
    if expected > 0.5:
        out["lift"] = round(hits / expected, 2)
    if expected < 2.0:
        out["fragile"] = (f"期望仅 {expected:.1f},单个结果翻转即可大幅改变 lift → 不可引用")
    if n < 30:
        out["warning"] = f"n={n} 个跃迁,远不足以断言胜率(需 ~120-150)"
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--token", required=True)
    ap.add_argument("--chain", default="bsc")
    ap.add_argument("--points", type=int, default=8)
    ap.add_argument("--max-days", type=int, default=0,
                    help="0 = use the measured archive depth for the chain")
    ap.add_argument("--horizon", type=int, default=24)
    args = ap.parse_args()

    max_days = args.max_days or ARCHIVE_DAYS.get(args.chain, 30)
    step = max(max_days // args.points, 1)
    days = [d for d in range(step, max_days + 1, step)]
    samples = replay(args.token, args.chain, days, args.horizon)

    print("=== 常驻状态(不应作为择时信号)===")
    print(json.dumps(score(samples, args.horizon), ensure_ascii=False, indent=2))
    print("\n=== 状态跃迁(唯一带时间信息的东西)===")
    print(json.dumps(score_transitions(samples, args.horizon), ensure_ascii=False, indent=2))

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    prev = []
    if OUT_PATH.exists():
        try:
            prev = json.loads(OUT_PATH.read_text())
        except Exception:
            prev = []
    # replace any prior samples for this (token, chain) — a re-run supersedes
    prev = [s for s in prev if not (s["token"].lower() == args.token.lower()
                                    and s["chain"] == args.chain)]
    OUT_PATH.write_text(json.dumps(prev + samples, ensure_ascii=False, indent=1))
    print(f"\n{len(samples)} samples → {OUT_PATH}")


if __name__ == "__main__":
    main()
