"""Evidence layer — the ONLY sanctioned way to state how well the alerts work.

Three failures made the old scoreboard lie, and each has a counterpart here:

1. ROWS WERE COUNTED AS TRIALS. `alert_outcomes.db` stored 40 SIREN rows fired
   between 09:06:18 and 09:23:37 on 2026-06-16 — one 17-minute episode — and the
   report read "40 hits / 90 = 44%". `episodes()` sessionizes rows into independent
   episodes before anything is counted.

2. NO BASE RATE. A hit is a >=5% move (slippage-adjusted) within 4h/24h. These
   tokens make that move by CHANCE 24-37% of the time. A hit rate without
   P(hit | no alert) is uninterpretable. `base_rate()` draws matched controls from
   the same token's own history and scores them with the SAME `_hit()`, including
   the same liquidity — a thin pool's real bar is 42%, not 5%, and comparing it to
   a flat 5% control would invert the conclusion.

3. NO UNCERTAINTY. At n=16 a point estimate is theatre. Everything is reported with
   a Wilson 95% interval, and `report()` refuses to print a bare percentage.

Non-operator rows (`chain='majors'` sentiment alerts) are excluded everywhere.

    python -m src.pipeline.evidence          # the honest scoreboard
"""

from __future__ import annotations

import math
import sqlite3
from datetime import datetime, timedelta

import structlog

from src.config import DATA_DIR

logger = structlog.get_logger()

DB_PATH = DATA_DIR / "alert_outcomes.db"

# A new episode starts after this much silence on the same (token, direction, phase),
# or immediately on a phase change. Reuses the value that sat dead in outcome_tracker.
EPISODE_GAP_MIN = 45
# Controls drawn per episode when estimating the token's own base rate.
CONTROLS_PER_EPISODE = 60


def _rows(conn: sqlite3.Connection) -> list[dict]:
    """Operator alerts only, oldest first. `chain='majors'` is BTC/ETH/SOL sentiment,
    not an operator call — counting it in an operator scoreboard is category error."""
    cols = {r[1] for r in conn.execute("PRAGMA table_info(alerts)")}
    phase = "phase" if "phase" in cols else "NULL AS phase"
    q = (f"SELECT id, ts, token, chain, symbol, kind, direction, price0, liquidity, "
         f"hit_4h, hit_24h, resolved, {phase} FROM alerts "
         f"WHERE chain != 'majors' ORDER BY token, chain, direction, ts")
    return [dict(zip([c[0] for c in conn.execute(q).description], r))
            for r in conn.execute(q)]


def _ts(s: str) -> datetime:
    return datetime.fromisoformat(str(s).replace("Z", "+00:00"))


def episodes(db_path=None) -> list[dict]:
    """Collapse raw alert rows into INDEPENDENT episodes.

    An episode is a contiguous run of alerts on the same (token, chain, direction)
    where consecutive fires are < EPISODE_GAP_MIN apart AND the phase hasn't changed.
    The FIRST fire supplies the entry (`price0`, `liquidity`) — that is the price you
    could actually have traded on. The episode is `resolved` once its first fire is,
    and its `hit` is that first fire's outcome. Later fires are confirmations of an
    already-known event; scoring them again is double-counting.
    """
    conn = sqlite3.connect(str(db_path or DB_PATH))
    try:
        rows = _rows(conn)
    finally:
        conn.close()

    out: list[dict] = []
    cur: dict | None = None
    for r in rows:
        key = (r["token"], r["chain"], r["direction"])
        new_ep = (
            cur is None
            or cur["key"] != key
            or (r["phase"] or "") != (cur["phase"] or "")
            or _ts(r["ts"]) - _ts(cur["last_ts"]) >= timedelta(minutes=EPISODE_GAP_MIN)
        )
        if new_ep:
            cur = {"key": key, "token": r["token"], "chain": r["chain"],
                   "symbol": r["symbol"], "direction": r["direction"],
                   "phase": r["phase"], "start_ts": r["ts"], "last_ts": r["ts"],
                   "kind": r["kind"], "price0": r["price0"],
                   "liquidity": r["liquidity"] or 0, "n_fires": 1,
                   "resolved": r["resolved"], "hit_4h": r["hit_4h"],
                   "hit_24h": r["hit_24h"]}
            out.append(cur)
        else:
            cur["last_ts"] = r["ts"]
            cur["n_fires"] += 1
    return out


def wilson(k: int, n: int, z: float = 1.96) -> tuple[float, float]:
    """Wilson score interval. At n=16 this spans ~[1%, 28%] for k=1 — that WIDTH is
    the honest headline, not the 6.2% point estimate."""
    if n == 0:
        return (0.0, 0.0)
    p = k / n
    d = 1 + z * z / n
    centre = (p + z * z / (2 * n)) / d
    half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return (max(0.0, centre - half), min(1.0, centre + half))


# ---------------------------------------------------------------- base rate

_CANDLE_CACHE: dict = {}
_POOL_CACHE: dict = {}


def _ohlcv(chain: str, pool: str) -> list[list]:
    """Hourly candles, cached per (chain, pool) and paced. A 429 must NOT silently
    return [] and quietly drop that token from the comparison set — that would shrink
    the denominator and flatter the lift. Raises on failure; the caller reports it."""
    import json
    import time
    import urllib.request
    key = (chain, pool.lower())
    if key in _CANDLE_CACHE:
        return _CANDLE_CACHE[key]
    net = {"bsc": "bsc", "ethereum": "eth", "base": "base", "solana": "solana"}.get(chain)
    if not net:
        raise ValueError(f"unsupported chain {chain}")
    u = (f"https://api.geckoterminal.com/api/v2/networks/{net}/pools/{pool}"
         f"/ohlcv/hour?aggregate=1&limit=1000")
    last = None
    for attempt in range(4):                     # free tier ~30/min → pace + retry
        try:
            r = urllib.request.urlopen(
                urllib.request.Request(u, headers={"User-Agent": "Mozilla/5.0"}), timeout=25)
            d = json.load(r)
            out = (d.get("data") or {}).get("attributes", {}).get("ohlcv_list") or []
            _CANDLE_CACHE[key] = out
            time.sleep(2.2)
            return out
        except Exception as e:
            last = e
            time.sleep(6 * (attempt + 1))
    raise RuntimeError(f"ohlcv unavailable after retries: {str(last)[:60]}")


def _deepest_pool(token: str, chain: str) -> str | None:
    import json
    import time
    import urllib.request
    key = (chain, token.lower())
    if key in _POOL_CACHE:
        return _POOL_CACHE[key]
    try:
        r = urllib.request.urlopen(urllib.request.Request(
            f"https://api.dexscreener.com/latest/dex/tokens/{token}",
            headers={"User-Agent": "Mozilla/5.0"}), timeout=20)
        pairs = [p for p in (json.load(r).get("pairs") or []) if p.get("pairAddress")]
        time.sleep(0.4)
    except Exception:
        return None
    if not pairs:
        return None
    best = max(pairs, key=lambda p: float((p.get("liquidity") or {}).get("usd") or 0))
    _POOL_CACHE[key] = best["pairAddress"]
    return _POOL_CACHE[key]


def hit_threshold_pct(liquidity: float) -> float:
    """The move a call must make to count as a hit, given the pool it was called on."""
    from src.pipeline.slippage import price_impact
    if not liquidity or liquidity <= 0:
        return 5.0
    return max(5.0, 2 * price_impact(liquidity, 5000))


def base_rate(token: str, chain: str, direction: str, liquidity: float,
              horizon_h: int = 24, tradeable: bool = True) -> dict:
    """P(hit | random entry) for THIS token, scored with the SAME rule as a real alert.

    Two different questions, so two modes:
      tradeable=True  — reuse `_hit` with the episode's own liquidity. Answers "could
                        this have been traded profitably". On a $33k pool the bar is
                        21.5%, so almost nothing hits — for BASED that drives both the
                        alert hit rate AND the base rate to ~0, and a lift computed on
                        that degenerate pair is meaningless.
      tradeable=False — flat 5%. Answers "was the call directionally right", which is
                        what actually measures the DETECTOR rather than the market.
    Never returns a fabricated number: unavailable candles → available=False.
    """
    from src.pipeline.outcome_tracker import _hit
    pool = _deepest_pool(token, chain)
    if not pool:
        return {"available": False, "reason": "no pool"}
    try:
        candles = _ohlcv(chain, pool)
    except Exception as e:
        return {"available": False, "reason": str(e)[:50]}
    if len(candles) < horizon_h + 30:
        return {"available": False, "reason": f"only {len(candles)} candles"}
    liq = liquidity if tradeable else 0
    closes = [c[4] for c in sorted(candles, key=lambda c: c[0])]
    hits = n = 0
    for i in range(len(closes) - horizon_h):
        p0, p1 = closes[i], closes[i + horizon_h]
        if not p0 or not p1:
            continue
        n += 1
        hits += _hit(direction, p0, p1, liq)
    if n == 0:
        return {"available": False, "reason": "no usable windows"}
    return {"available": True, "p": hits / n, "n_windows": n, "hits": hits,
            "threshold_pct": hit_threshold_pct(liq)}


# ---------------------------------------------------------------- report

def _lift_block(res: list[dict], direction: str, tradeable: bool) -> list[str]:
    """Lift on a MATCHED subset: an episode contributes to BOTH the observed hits and
    the expected hits, or to neither. Mixing n (all resolved) with base_n (those whose
    base rate resolved) silently rescales the ratio — the first bug this file's own
    output revealed."""
    lines: list[str] = []
    matched: list[tuple[dict, float]] = []
    unavailable: list[str] = []
    for e in res:
        liq = e["liquidity"] if tradeable else 0
        br = base_rate(e["token"], e["chain"], direction, liq, tradeable=tradeable)
        if br["available"]:
            matched.append((e, br["p"]))
        else:
            unavailable.append(f"{e['symbol']}({br['reason']})")
    if unavailable:
        lines.append(f"      基准率不可得,已从对比中剔除: {', '.join(sorted(set(unavailable)))}")
    if not matched:
        lines.append("      → 无可比事件,不给结论")
        return lines

    k_m = sum(1 for e, _ in matched if e["hit_24h"])
    n_m = len(matched)
    expected = sum(p for _, p in matched)          # Poisson-binomial expectation
    lo, hi = wilson(k_m, n_m)

    by_sym: dict = {}
    for e, p in matched:
        by_sym.setdefault(e["symbol"], []).append(p)
    for sym, ps in sorted(by_sym.items()):
        thr = hit_threshold_pct(next(e["liquidity"] for e, _ in matched
                                     if e["symbol"] == sym) if tradeable else 0)
        lines.append(f"      {sym:9} 随机基准 {sum(ps)/len(ps)*100:5.1f}%  "
                     f"(命中门槛 {thr:.1f}%, {len(ps)} 个事件)")
    lines.append(f"      → 匹配子集: 实际 {k_m}/{n_m} "
                 f"(Wilson [{lo*100:.1f}%, {hi*100:.1f}%]),掷骰子期望 {expected:.1f}")
    if expected <= 0.5:
        lines.append("      → 期望命中≈0(门槛过高,几乎不可能命中)→ lift 无意义,不报")
        return lines
    lift = (k_m / expected)
    lines.append(f"      → LIFT = {lift:.2f}  "
                 f"({'无 edge' if lift < 1.15 else '有待验证的正向迹象'})")
    if expected < 2.0:
        # 1 hit against 0.7 expected reads as lift 1.5, but a single flipped outcome
        # swings it to 0. A ratio built on <2 expected events is noise, not signal —
        # exactly the shape of the fake 44%.
        lines.append(f"      → ⚠️ 期望仅 {expected:.1f},单个结果翻转就能让 lift 从 "
                     f"{lift:.2f} 掉到 {(max(k_m-1,0))/expected:.2f} → 该数字不稳定,不可引用")
    if lift < 1.0:
        lines.append("      → lift < 1:表现不如随机入场。不要据此交易。")
    return lines


def report(db_path=None, with_base_rate: bool = True) -> str:
    eps = episodes(db_path)
    lines = ["妖币告警 — 诚实记分牌(事件级,非行级)", "=" * 66]
    conn = sqlite3.connect(str(db_path or DB_PATH))
    try:
        raw = conn.execute("SELECT COUNT(*) FROM alerts WHERE chain != 'majors'").fetchone()[0]
    finally:
        conn.close()
    lines.append(f"原始告警行 {raw} → 独立事件 {len(eps)} "
                 f"(去重比 {raw / max(len(eps), 1):.1f}x)")
    lines.append("")

    for direction in ("short", "long"):
        de = [e for e in eps if e["direction"] == direction]
        res = [e for e in de if e["resolved"]]
        label = "空头" if direction == "short" else "多头"
        if not res:
            lines.append(f"{label}: {len(de)} 个事件, 0 个已结算 → 无可报告\n")
            continue
        k, n = sum(1 for e in res if e["hit_24h"]), len(res)
        lo, hi = wilson(k, n)
        lines.append(f"{label}: {k}/{n} 个已结算事件命中 "
                     f"(点估计 {k/n*100:.1f}%, Wilson 95% CI [{lo*100:.1f}%, {hi*100:.1f}%])")
        if n < 30:
            lines.append(f"      ⚠️ n={n} 远不足以支撑任何胜率断言(需 ~120-150 个独立事件)")
        if with_base_rate:
            lines.append("")
            lines.append("      [A] 可交易口径(含滑点门槛 — 你真能不能赚到):")
            lines += _lift_block(res, direction, tradeable=True)
            lines.append("")
            lines.append("      [B] 方向口径(固定5% — 探测器判断对不对):")
            lines += _lift_block(res, direction, tradeable=False)
        lines.append("")

    lines.append("口径:每个事件按首次触发的价格入场;后续重复触发是同一事件的确认,不重复计分。")
    lines.append("为什么分两个口径:薄池子(BASED告警时仅$33k)的滑点门槛高达21.5%,")
    lines.append("        此时'没命中'说明的是池子太浅无法交易,而非探测器判断错误。")
    lines.append("混淆项:告警并非随机时点触发,它们聚集在高波动区间(该区间基准率本就更高),")
    lines.append("        故 lift<1 是'无 edge 证据',不等于'已证明劣于随机'。")
    return "\n".join(lines)


if __name__ == "__main__":
    print(report())
