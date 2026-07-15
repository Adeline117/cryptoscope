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
# or immediately on a phase change. Set to the HOLDING HORIZON, not a 45-min burst
# window: you can hold only ONE position per token at a time, so two same-direction
# alerts within the outcome horizon are the SAME position, not independent bets. The
# 45-min window let one sustained decline (BASED, 2026-07-01→02) count as 4 independent
# "hits" and inflated the directional lift from ~1.7 to ~1.9 — the SIREN-40-fire double
# count at a coarser time scale. 24h merges the position; genuinely separate events
# (>24h apart, or a phase flip) still split.
EPISODE_GAP_MIN = 24 * 60
# Controls drawn per episode when estimating the token's own base rate.
CONTROLS_PER_EPISODE = 60


def _rows(conn: sqlite3.Connection) -> list[dict]:
    """Operator alerts only, oldest first. `chain='majors'` is BTC/ETH/SOL sentiment,
    not an operator call — counting it in an operator scoreboard is category error."""
    cols = {r[1] for r in conn.execute("PRAGMA table_info(alerts)")}
    phase = "phase" if "phase" in cols else "NULL AS phase"
    q = (f"SELECT id, ts, token, chain, symbol, kind, direction, price0, liquidity, "
         f"price_4h, price_24h, hit_4h, hit_24h, resolved, {phase} FROM alerts "
         f"WHERE chain != 'majors' ORDER BY token, chain, direction, ts")
    return [dict(zip([c[0] for c in conn.execute(q).description], r))
            for r in conn.execute(q)]


def _ts(s: str) -> datetime:
    return datetime.fromisoformat(str(s).replace("Z", "+00:00"))


def episodes(db_path=None) -> list[dict]:
    """Collapse raw alert rows into INDEPENDENT episodes.

    An episode is a contiguous run of alerts on the same (token, chain, direction)
    where consecutive fires are < EPISODE_GAP_MIN apart AND the phase hasn't changed.
    The ANCHOR fire — the first fire with a real entry price (`price0 > 0`) — supplies
    entry, outcome and liquidity; that is the price you could actually have traded on.
    Pinning to the literal first fire was a bug: a first fire logged with `price0=0`
    (market data unavailable at log time) is retired unscoreable, discarding a later
    sibling fire that holds the only real hit. Later fires are otherwise confirmations
    of an already-known event; scoring them again is double-counting.
    """
    conn = sqlite3.connect(str(db_path or DB_PATH))
    try:
        rows = _rows(conn)
    finally:
        conn.close()

    # group into episodes (a list of fires each)
    groups: list[list[dict]] = []
    key = last_ts = phase = None
    for r in rows:
        k = (r["token"], r["chain"], r["direction"])
        new_ep = (not groups or k != key or (r["phase"] or "") != (phase or "")
                  or _ts(r["ts"]) - _ts(last_ts) >= timedelta(minutes=EPISODE_GAP_MIN))
        if new_ep:
            groups.append([r])
            key, phase = k, r["phase"]
        else:
            groups[-1].append(r)
        last_ts = r["ts"]

    out: list[dict] = []
    for fires in groups:
        f0 = fires[0]
        anchor = next((f for f in fires if f["price0"] and f["price0"] > 0), f0)
        out.append({
            "key": (f0["token"], f0["chain"], f0["direction"]),
            "token": f0["token"], "chain": f0["chain"], "symbol": f0["symbol"],
            "direction": f0["direction"], "phase": f0["phase"],
            "start_ts": f0["ts"], "kind": f0["kind"], "n_fires": len(fires),
            "price0": anchor["price0"], "price_24h": anchor.get("price_24h"),
            "liquidity": anchor["liquidity"] or 0,
            # resolved: 1 = scored, 2 = RETIRED (never priceable). Only 1 counts — a
            # retired alert scored as a miss would silently drag the hit rate down.
            "resolved": anchor["resolved"] == 1,
            "hit_4h": anchor["hit_4h"], "hit_24h": anchor["hit_24h"]})
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


class OhlcvRateLimited(RuntimeError):
    """The shared historical-price source requested a scheduler-level backoff."""


def _ohlcv(chain: str, pool: str, before: int | None = None,
           timeframe: str = "hour") -> list[list]:
    """Candles, cached per (chain, pool, before, tf) and paced. A 429 must NOT silently
    return [] and quietly drop that token from the comparison set — that would shrink
    the denominator and flatter the lift. Raises on failure; the caller reports it.

    One page is 1000 candles = ~41 days hourly. A replay point older than that gets
    NO price unless we page back with `before_timestamp` — otherwise deep samples
    silently score as `ret=None` and the surviving handful produce a precision like
    1.0 out of n=1.
    """
    import json
    import time
    import urllib.error
    import urllib.request
    key = (chain, pool.lower(), before, timeframe)
    if key in _CANDLE_CACHE:
        return _CANDLE_CACHE[key]
    net = {"bsc": "bsc", "ethereum": "eth", "base": "base", "solana": "solana"}.get(chain)
    if not net:
        raise ValueError(f"unsupported chain {chain}")
    u = (f"https://api.geckoterminal.com/api/v2/networks/{net}/pools/{pool}"
         f"/ohlcv/{timeframe}?aggregate=1&limit=1000")
    if before:
        u += f"&before_timestamp={int(before)}"
    last = None
    for attempt in range(4):                     # free tier ~30/min → pace + retry
        try:
            req = urllib.request.Request(u, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=25) as response:
                d = json.load(response)
            out = (d.get("data") or {}).get("attributes", {}).get("ohlcv_list") or []
            _CANDLE_CACHE[key] = out
            time.sleep(2.2)
            return out
        except urllib.error.HTTPError as e:
            try:
                e.close()
            except Exception:
                pass
            if e.code == 429:
                # Retrying four times per token turns one shared quota response into
                # minutes of sleeps and more 429s. Abort this resolver cycle; the
                # hourly scheduler is the retry policy.
                raise OhlcvRateLimited("GeckoTerminal OHLCV rate limited") from e
            last = e
            time.sleep(6 * (attempt + 1))
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
        req = urllib.request.Request(
            f"https://api.dexscreener.com/latest/dex/tokens/{token}",
            headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=20) as response:
            pairs = [p for p in (json.load(response).get("pairs") or []) if p.get("pairAddress")]
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

    # F1 fix: the OBSERVED numerator must use the SAME threshold as the expected.
    # In mode B (directional) `expected` is computed at a flat 5% bar, but the stored
    # `hit_24h` was computed at the alert's REAL pool liquidity (slippage-adjusted, e.g.
    # 47% on a $33k pool). Reusing hit_24h there scored a −28% short as a MISS while its
    # expectation was summed at 5% → a fabricated lift ≈ 0, and the report told the user
    # the DIRECTIONAL detector was worse than random when a −28% call was directionally
    # right. In mode B, recompute the hit from the raw move at 5%.
    from src.pipeline.outcome_tracker import _hit
    def observed_hit(e: dict) -> int:
        if tradeable:
            return 1 if e["hit_24h"] else 0
        p0, p1 = e.get("price0"), e.get("price_24h")
        if not p0 or not p1:
            return 1 if e["hit_24h"] else 0        # no raw price → fall back
        return _hit(direction, p0, p1, 0)          # flat 5%, matches the expectation
    k_m = sum(observed_hit(e) for e, _ in matched)
    n_m = len(matched)
    expected = sum(p for _, p in matched)          # Poisson-binomial expectation
    lo, hi = wilson(k_m, n_m)

    by_sym: dict = {}
    for e, p in matched:
        by_sym.setdefault(e["symbol"], []).append(p)
    for sym, ps in sorted(by_sym.items()):
        # median liquidity of the symbol's episodes, not an arbitrary first one — a
        # symbol whose episodes span $32k–$38k has different thresholds per episode.
        liqs = sorted(e["liquidity"] for e, _ in matched if e["symbol"] == sym)
        med_liq = liqs[len(liqs) // 2] if (tradeable and liqs) else 0
        thr = hit_threshold_pct(med_liq)
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
        res = [e for e in de if e["resolved"]]   # resolved==1 only (see episodes)
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


# ---------------------------------------------------------------- kill line

# The bet: "event-precursor" alarms (things that happen at a MOMENT) carry timing
# information that the standing verdict does not. Standing-state verdicts are excluded
# on principle — SIREN replayed `distributing` at every block for 240 days, so a state
# can never be early to anything.
EVENT_KINDS = {"CEX充值前兆", "CEX充值", "授权路由", "注入gas", "庄在卖", "RUG", "阴跌出货"}
TARGET_EVENTS = 120        # to separate a true 55% from a ~30% base rate at ~80% power
KILL_LIFT_CI_UPPER = 1.2   # if the 95% upper bound is below this, there is no edge


def _shortable_tokens() -> set[str]:
    """Tokens that actually have a perpetual future — the ONLY ones a short thesis can
    be tested on. Empty set on failure (and the caller must say so, not assume all)."""
    try:
        from src.onchain.perp_universe import load as perp_load
        return {r["address"].lower() for r in perp_load().values() if r.get("address")}
    except Exception:
        return set()


def kill_line(db_path=None) -> str:
    """Progress toward a VERDICT ON THE SYSTEM ITSELF, with a pre-committed stop rule.

    Declared in advance so it cannot be moved once the number is inconvenient:
      - accrue TARGET_EVENTS independent event-episodes ON SHORTABLE COINS;
      - if the 95% upper bound on lift is below KILL_LIFT_CI_UPPER, the thesis is dead.

    The shortable filter is the whole point. Events on BSC micro-caps cannot be traded
    short (no perp), so measuring an edge there proves nothing you can act on. The
    system's detection strength and its monetisation venue are DIFFERENT UNIVERSES,
    and counting the wrong one would burn months confirming an untradeable edge.
    """
    all_eps = [e for e in episodes(db_path)
               if any(k in EVENT_KINDS for k in str(e["kind"]).split(","))]
    perp = _shortable_tokens()
    eps = [e for e in all_eps if e["token"].lower() in perp]
    untradeable = len(all_eps) - len(eps)

    lines = ["事件抢先 — 死线进度(停机规则已预先声明,不得事后移动)", "=" * 66]
    lines.append(f"目标: {TARGET_EVENTS} 个【可开空标的上的】独立事件;"
                 f"lift 95% 置信上界 < {KILL_LIFT_CI_UPPER} → 判定无 edge,停止作为交易工具")
    if not perp:
        lines.append("⚠️ perp 宇宙加载失败 → 无法判断哪些事件可交易,不做任何统计。")
        return "\n".join(lines)

    lines.append(f"可开空事件: {len(eps)}   |   不可开空事件(不计入): {untradeable}")
    # No shortable event-kind episodes at all. Without this guard the min()/max() over
    # an empty `eps` below CRASHED the whole report — 'not measured' must never be a
    # stack trace. Two sub-cases: events exist but none are shortable (structural
    # mismatch), or no event-precursor alerts have fired yet (未测量).
    if not eps:
        if untradeable:
            lines.append("")
            lines.append("🔴 全部事件都发生在【不能开空的币】上(BSC 小盘妖币无永续合约)。")
            lines.append("   在这些币上measure出的任何 edge 都无法变现 —— 这是能力与变现场所的")
            lines.append("   结构性错配,不是数据不足。可交易宇宙的积累速率 = 0/天,判定期 = ∞。")
            lines.append("")
            lines.append("   必须先扩大【perp 币上的】事件面,否则这个赌注永远无法判定:")
            lines.append("   · perp_cex_scan 每日跑满 190 币(当前 0 命中 → 需实测信号密度)")
            lines.append("   · 把 mobilization(授权/注gas)与 LP 解锁接到 perp 大户上")
            lines.append("   · 或承认:我们最擅长探测的币,恰恰是不能做空的币")
        else:
            lines.append("尚无事件抢先类告警 → 未测量(不是'无 edge')。")
        return "\n".join(lines)

    resolved = [e for e in eps if e["resolved"]]
    k = sum(1 for e in resolved if e["hit_24h"])
    n = len(resolved)
    lines.append(f"其中已结算 {n} 个,命中 {k}")

    # How long is this bet? A stop rule you cannot reach is not a stop rule.
    first, last = min(_ts(e["start_ts"]) for e in eps), max(_ts(e["start_ts"]) for e in eps)
    span_d = max((last - first).total_seconds() / 86400.0, 1.0)
    rate = len(eps) / span_d
    lines.append(f"可交易事件积累速率: {len(eps)} / {span_d:.0f} 天 = {rate:.2f} 个/天")
    if rate > 0:
        days = max(TARGET_EVENTS - n, 0) / rate
        lines.append(f"按此速率,还需 ~{days:.0f} 天(≈{days/30:.1f} 个月)才能判定")
        if days > 365:
            lines.append("⚠️ 判定期 >1 年:实践中不可判。扩大事件面,或现在承认不可验证。")

    if n == 0:
        lines.append("\n尚无已结算的可开空事件 → 无可判定。这不是'无 edge',是'未测量'。")
        return "\n".join(lines)

    lo, hi = wilson(k, n)
    lines.append(f"命中率 Wilson 95% CI = [{lo*100:.1f}%, {hi*100:.1f}%]")
    lines.append(f"进度 {n}/{TARGET_EVENTS} ({100.0 * n / TARGET_EVENTS:.0f}%)")
    if n < TARGET_EVENTS:
        lines.append(f"⏳ 样本不足,任何 lift 都不可作为停机依据。继续积累 {TARGET_EVENTS - n} 个。")
        lines.append("注意:静音推送不影响积累 —— log_alert 照常写库。")
    else:
        lines.append("样本达标 → 执行停机规则(对照基准率算 lift,见 report())。")
    return "\n".join(lines)


if __name__ == "__main__":
    print(report())
    print("\n")
    print(kill_line())
