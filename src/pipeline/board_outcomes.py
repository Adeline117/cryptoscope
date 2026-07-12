"""Board measurement layer — the honest fix for 'detection without measurement'.

The board surfaces buy signals but had ZERO evidence any of them make money (the same
unmeasured-signal trap this repo was built to kill — the 44%-fake lesson). This logs
each lane's picks with an entry price, resolves them N hours later against historical
candles, and reports the REAL hit rate with a Wilson interval — refusing to quote a
number until the sample can support one ('不可判' until then, by design).

Reuses the proven honesty machinery verbatim: outcome_tracker._price_at / _hit
(slippage-aware, reads the horizon price from candles so a late resolve is still
correct) and evidence.wilson. It does NOT invent a fused score — un-backtested weights
are exactly how the fake 44% was born.

    python -m src.pipeline.board_outcomes            # resolve due + print report
    python -m src.pipeline.board_outcomes --report   # report only
"""

from __future__ import annotations

import sqlite3
import sys
from datetime import datetime, timedelta, timezone

import structlog

from src.config import DATA_DIR

logger = structlog.get_logger()

DB = DATA_DIR / "board_picks.db"
HORIZONS_H = [4, 24]
MIN_N = 20                 # below this, no hit rate is quoted — '不可判'
DEDUP_HOURS = 12           # don't re-log the same (lane, token) within this window
# a lane counts as a 'buy' bet; hit = price up past the slippage-aware threshold
DIRECTION = "long"


def _conn() -> sqlite3.Connection:
    DB.parent.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(str(DB), timeout=10)
    c.execute("""CREATE TABLE IF NOT EXISTS picks(
        id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT, lane TEXT, symbol TEXT,
        chain TEXT, token TEXT, price0 REAL, liquidity REAL, metric REAL,
        price_4h REAL, price_24h REAL, hit_4h INTEGER, hit_24h INTEGER,
        resolved INTEGER DEFAULT 0)""")
    return c


def log_picks(lane: str, picks: list[dict]) -> int:
    """Record a lane's current picks with entry price. `picks`: [{symbol, chain, token,
    price0, liquidity?, metric?}]. Dedups (lane, token) within DEDUP_HOURS so the same
    token surfacing every 15 min is ONE bet, not many (the episode discipline)."""
    if not picks:
        return 0
    now = datetime.now(timezone.utc)
    cutoff = (now - timedelta(hours=DEDUP_HOURS)).isoformat()
    c = _conn()
    n = 0
    try:
        for p in picks:
            tok, ch = p.get("token"), p.get("chain")
            p0 = p.get("price0")
            if not tok or not ch or not p0 or p0 <= 0:
                continue
            dup = c.execute("SELECT 1 FROM picks WHERE lane=? AND token=? AND ts>? LIMIT 1",
                            (lane, tok.lower(), cutoff)).fetchone()
            if dup:
                continue
            c.execute("INSERT INTO picks(ts,lane,symbol,chain,token,price0,liquidity,metric) "
                      "VALUES (?,?,?,?,?,?,?,?)",
                      (now.isoformat(), lane, p.get("symbol", "?"), ch, tok.lower(),
                       float(p0), float(p.get("liquidity") or 0), float(p.get("metric") or 0)))
            n += 1
        c.commit()
    finally:
        c.close()
    if n:
        logger.info("board_picks_logged", lane=lane, n=n)
    return n


def resolve() -> int:
    """Fill 4h/24h prices + hit flags for picks whose horizon elapsed. Reads the horizon
    price from candles (reuse outcome_tracker._price_at) so a late run resolves
    correctly. Unpriceable picks retire instead of inflating the backlog."""
    from src.pipeline.outcome_tracker import _hit, _price_at
    now = datetime.now(timezone.utc)
    c = _conn()
    resolved = 0
    try:
        rows = c.execute("SELECT id,ts,chain,token,price0,liquidity,price_4h,price_24h "
                         "FROM picks WHERE resolved=0").fetchall()
        for rid, ts, ch, tok, p0, liq, p4, p24 in rows:
            t0 = datetime.fromisoformat(ts)
            age_h = (now - t0).total_seconds() / 3600
            if age_h > 14 * 24 and (p4 is None or p24 is None):
                c.execute("UPDATE picks SET resolved=2 WHERE id=?", (rid,))
                continue
            upd = {}
            if p4 is None and age_h >= 4:
                px = _price_at(tok, ch, t0 + timedelta(hours=4))
                if px:
                    upd["price_4h"] = px
                    upd["hit_4h"] = _hit(DIRECTION, p0, px, liq or 0)
            if p24 is None and age_h >= 24:
                px = _price_at(tok, ch, t0 + timedelta(hours=24))
                if px:
                    upd["price_24h"] = px
                    upd["hit_24h"] = _hit(DIRECTION, p0, px, liq or 0)
            done = (p24 is not None or "price_24h" in upd)
            if upd:
                if done:
                    upd["resolved"] = 1
                    resolved += 1
                sets = ", ".join(f"{k}=?" for k in upd)
                c.execute(f"UPDATE picks SET {sets} WHERE id=?", (*upd.values(), rid))
        c.commit()
    finally:
        c.close()
    logger.info("board_picks_resolved", count=resolved)
    return resolved


def lane_stats() -> dict:
    """Per-lane honest hit rate: {lane: {n, hits, rate, lo, hi, verdict, pending}}.
    verdict is '不可判' until n>=MIN_N — never quote a rate the sample can't support."""
    from src.pipeline.evidence import wilson
    c = _conn()
    out = {}
    try:
        lanes = [r[0] for r in c.execute("SELECT DISTINCT lane FROM picks").fetchall()]
        for lane in lanes:
            row = c.execute("SELECT COUNT(*), SUM(hit_24h) FROM picks "
                            "WHERE lane=? AND hit_24h IS NOT NULL", (lane,)).fetchone()
            n, hits = row[0] or 0, row[1] or 0
            pending = c.execute("SELECT COUNT(*) FROM picks WHERE lane=? AND resolved=0",
                                (lane,)).fetchone()[0]
            if n >= MIN_N:
                lo, hi = wilson(hits, n)
                out[lane] = {"n": n, "hits": hits, "rate": round(hits / n, 3),
                             "lo": round(lo, 3), "hi": round(hi, 3),
                             "verdict": "measured", "pending": pending}
            else:
                out[lane] = {"n": n, "hits": hits, "verdict": "不可判", "pending": pending,
                             "note": f"样本 {n}/{MIN_N},还在积累"}
    finally:
        c.close()
    return out


if __name__ == "__main__":
    from dotenv import load_dotenv

    from src.config import PROJECT_ROOT
    load_dotenv(PROJECT_ROOT / ".env")
    if "--report" not in sys.argv:
        resolve()
    import json
    print(json.dumps(lane_stats(), ensure_ascii=False, indent=1))
