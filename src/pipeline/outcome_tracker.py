"""Outcome tracker — does the system actually work? Log every alert, then score it.

Each sentinel/hunt alert is logged with the price at fire time + its direction
(long/short). Later, resolve_outcomes() looks up the price N hours after and marks
hit/miss: a 🟢 long alert is a HIT if price rose, a 🔴 short alert is a HIT if price
fell. hit_rate_report() aggregates by signal type — turning the system from
"plausible" into "measured", and feeding calibrate_weights with real outcomes.

Free: prices via DexScreener. SQLite store.

    python -m src.pipeline.outcome_tracker            # resolve due + print report
    python -m src.pipeline.outcome_tracker --report   # report only
"""

from __future__ import annotations

import sqlite3
import sys
from datetime import datetime, timedelta, timezone

import structlog

from src.config import DATA_DIR

logger = structlog.get_logger()

DB_PATH = DATA_DIR / "alert_outcomes.db"
HORIZONS_H = [4, 24]            # score price move 4h and 24h after the alert
HIT_MOVE = 0.05                # >=5% in the predicted direction = a hit


def _conn() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(str(DB_PATH), timeout=10)
    c.execute("PRAGMA busy_timeout=8000")
    c.execute("""CREATE TABLE IF NOT EXISTS alerts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts TEXT, token TEXT, chain TEXT, symbol TEXT,
        kind TEXT, direction TEXT, price0 REAL, liquidity REAL,
        price_4h REAL, price_24h REAL, hit_4h INTEGER, hit_24h INTEGER,
        resolved INTEGER DEFAULT 0)""")
    # add columns to older DBs
    cols = {r[1] for r in c.execute("PRAGMA table_info(alerts)").fetchall()}
    if "liquidity" not in cols:
        c.execute("ALTER TABLE alerts ADD COLUMN liquidity REAL")
    if "phase" not in cols:
        # The operator's behavioral phase (sell/arm/buy/stall) at fire time. It was
        # already computed in operator_sentinel and then discarded; episode grouping
        # needs it, because a phase change starts a NEW episode even inside cooldown.
        c.execute("ALTER TABLE alerts ADD COLUMN phase TEXT")
    return c


def log_alert(token: str, chain: str, symbol: str, kind: str, direction: str,
              price0: float, liquidity: float = 0, phase: str | None = None) -> None:
    """Record a fired alert with entry price, direction ('long'/'short') + the pool
    liquidity (so the hit threshold can require beating slippage — #5) + the operator
    `phase` at fire time (episode grouping needs it; see src/pipeline/evidence.py).

    NOTE: every fire is stored, including repeats of an ongoing episode. Rows are an
    AUDIT TRAIL, not independent trials — 40 rows once came from one 17-minute SIREN
    episode and were read as a 44% hit rate. Never compute a hit rate off this table
    directly; go through evidence.episodes()."""
    try:
        c = _conn()
        try:
            c.execute("INSERT INTO alerts (ts, token, chain, symbol, kind, direction, "
                      "price0, liquidity, phase) VALUES (?,?,?,?,?,?,?,?,?)",
                      (datetime.now(timezone.utc).isoformat(), token, chain, symbol,
                       kind, direction, price0, liquidity, phase))
            c.commit()
        finally:
            c.close()
    except Exception as e:
        logger.debug("log_alert_failed", error=str(e))


def _price(token: str, chain: str) -> float | None:
    import json
    import urllib.request
    if chain == "majors":   # BTC/ETH/SOL — price via OKX, not DexScreener
        try:
            u = f"https://www.okx.com/api/v5/market/ticker?instId={token}-USDT-SWAP"
            req = urllib.request.Request(u, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=12) as r:
                d = json.loads(r.read().decode()).get("data", [])
            return float(d[0]["last"]) if d else None
        except Exception:
            return None
    try:
        u = f"https://api.dexscreener.com/token-pairs/v1/{chain}/{token}"
        req = urllib.request.Request(u, headers={"User-Agent": "CryptoScope/1.0"})
        with urllib.request.urlopen(req, timeout=12) as r:
            pairs = json.loads(r.read().decode())
        pairs = pairs if isinstance(pairs, list) else pairs.get("pairs", [])
        if not pairs:
            return None
        p = max(pairs, key=lambda x: (x.get("liquidity", {}) or {}).get("usd", 0) or 0)
        return float(p.get("priceUsd") or 0) or None
    except Exception:
        return None


def _hit(direction: str, price0: float, price1: float, liquidity: float = 0) -> int:
    """A HIT requires the move to clear BOTH the base 5% AND ~2x the entry slippage
    on a $5k order — a 'right' call that slippage would have eaten isn't a hit (#5)."""
    if not price0 or not price1:
        return 0
    move = (price1 - price0) / price0
    threshold = HIT_MOVE
    if liquidity and liquidity > 0:
        from src.pipeline.slippage import price_impact
        threshold = max(HIT_MOVE, 2 * price_impact(liquidity, 5000) / 100)
    if direction == "long":
        return 1 if move >= threshold else 0
    if direction == "short":
        return 1 if move <= -threshold else 0
    return 0


UNRESOLVABLE_DAYS = 14      # past this, an unpriced alert will never resolve


def _price_at(token: str, chain: str, when: datetime) -> float | None:
    """The price AT `when` — not the price whenever we happen to run.

    `_price()` returns the CURRENT price. The resolver used it for `price_24h`, which
    is only correct if the resolver fires punctually at ts+24h. It does not: the
    scheduler was starving it (11,240 misfires; it once went two days without
    running), so `price_24h` silently became 'price whenever we got around to it'.
    A late resolution then measures days of return and calls it a 24-hour move.

    Reading the horizon price from historical candles makes resolution CORRECT
    regardless of when it runs — and therefore idempotent and back-fillable.
    """
    if chain == "majors":
        # No free historical OHLCV wired for OKX majors; only resolve punctually.
        age_h = (datetime.now(timezone.utc) - when).total_seconds() / 3600
        return _price(token, chain) if abs(age_h) <= 2 else None
    try:
        from src.pipeline.evidence import _deepest_pool, _ohlcv
        pool = _deepest_pool(token, chain)
        if not pool:
            return None
        target = int(when.timestamp())
        for before in (None, target + 36 * 3600):
            try:
                candles = _ohlcv(chain, pool, before=before)
            except Exception:
                continue
            best = None
            for cd in candles:
                if best is None or abs(cd[0] - target) < abs(best[0] - target):
                    best = cd
            if best is not None and abs(best[0] - target) <= 2 * 3600:
                return best[4]          # hourly close within 2h of the horizon
    except Exception as e:
        logger.debug("price_at_failed", token=token, error=str(e)[:60])
    return None


def resolve_outcomes() -> int:
    """Fill in 4h/24h prices + hit flags for alerts whose horizon has elapsed.

    Prices are read AT the horizon, so a late run resolves correctly rather than
    measuring its own lateness. Alerts that can never be priced are retired instead
    of accumulating forever as a fake backlog."""
    now = datetime.now(timezone.utc)
    c = _conn()
    resolved = 0
    try:
        rows = c.execute("SELECT id, ts, token, chain, direction, price0, liquidity, price_4h, price_24h "
                         "FROM alerts WHERE resolved = 0").fetchall()
        for rid, ts, token, chain, direction, p0, liq, p4, p24 in rows:
            t0 = datetime.fromisoformat(ts)
            age_h = (now - t0).total_seconds() / 3600
            # An alert with no entry price can never be scored — retire it rather
            # than letting it inflate the backlog or count as a miss.
            if not p0 or p0 <= 0:
                if age_h > 24:
                    c.execute("UPDATE alerts SET resolved=2 WHERE id=?", (rid,))
                continue
            if age_h > UNRESOLVABLE_DAYS * 24 and (p4 is None or p24 is None):
                c.execute("UPDATE alerts SET resolved=2 WHERE id=?", (rid,))
                logger.info("alert_unresolvable", id=rid, token=token[:10],
                            age_days=round(age_h / 24, 1), note="无价源,退休,不计入分母")
                continue
            updates = {}
            if p4 is None and age_h >= 4:
                px = _price_at(token, chain, t0 + timedelta(hours=4))
                if px:
                    updates["price_4h"] = px
                    updates["hit_4h"] = _hit(direction, p0, px, liq or 0)
            if p24 is None and age_h >= 24:
                px = _price_at(token, chain, t0 + timedelta(hours=24))
                if px:
                    updates["price_24h"] = px
                    updates["hit_24h"] = _hit(direction, p0, px, liq or 0)
            # RESOLVED is gated on the 24h horizon ONLY — that is the horizon the
            # scoreboard reads. Requiring the 4h price too meant a valid 24h hit on a
            # thin pool (whose 4h candle GeckoTerminal omits for no-trade hours) stayed
            # resolved=0, went invisible for 14 days, then was retired — silently
            # deleting real outcomes from the denominator. 4h is a bonus, not a gate.
            done = (p24 is not None or "price_24h" in updates)
            if updates:
                if done:
                    updates["resolved"] = 1
                    resolved += 1
                sets = ", ".join(f"{k}=?" for k in updates)
                c.execute(f"UPDATE alerts SET {sets} WHERE id=?",
                          (*updates.values(), rid))
        c.commit()
    finally:
        c.close()
    logger.info("outcomes_resolved", count=resolved)
    return resolved


def hit_rate_report() -> str:
    c = _conn()
    try:
        total = c.execute("SELECT COUNT(*) FROM alerts").fetchone()[0]
        lines = ["=" * 56, f"告警命中率(共 {total} 条告警)", "=" * 56]
        for direction in ("long", "short"):
            for h in ("4h", "24h"):
                row = c.execute(
                    f"SELECT COUNT(*), SUM(hit_{h}) FROM alerts "
                    f"WHERE direction=? AND hit_{h} IS NOT NULL", (direction,)).fetchone()
                n, hits = row[0], (row[1] or 0)
                if n:
                    lines.append(f"  {direction:5} {h}: {hits}/{n} 命中 ({hits/n*100:.0f}%)")
        # by kind
        lines.append("  ── 按信号类型(24h)──")
        for kind, n, hits in c.execute(
            "SELECT kind, COUNT(*), SUM(hit_24h) FROM alerts "
            "WHERE hit_24h IS NOT NULL GROUP BY kind"):
            lines.append(f"  {kind:10}: {(hits or 0)}/{n} ({(hits or 0)/n*100:.0f}%)")
        pending = c.execute("SELECT COUNT(*) FROM alerts WHERE resolved=0").fetchone()[0]
        lines.append(f"  待结算: {pending}")
        if total == 0:
            lines.append("  (还没有告警——等哨兵触发后开始累积)")
        return "\n".join(lines)
    finally:
        c.close()


def main():
    if "--report" not in sys.argv:
        resolve_outcomes()
    print(hit_rate_report())


if __name__ == "__main__":
    from dotenv import load_dotenv

    from src.config import PROJECT_ROOT
    load_dotenv(PROJECT_ROOT / ".env")
    main()
