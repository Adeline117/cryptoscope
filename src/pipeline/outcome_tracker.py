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
        kind TEXT, direction TEXT, price0 REAL,
        price_4h REAL, price_24h REAL, hit_4h INTEGER, hit_24h INTEGER,
        resolved INTEGER DEFAULT 0)""")
    return c


def log_alert(token: str, chain: str, symbol: str, kind: str, direction: str,
              price0: float) -> None:
    """Record a fired alert with its entry price + direction ('long'/'short')."""
    try:
        c = _conn()
        try:
            c.execute("INSERT INTO alerts (ts, token, chain, symbol, kind, direction, price0) "
                      "VALUES (?,?,?,?,?,?,?)",
                      (datetime.now(timezone.utc).isoformat(), token, chain, symbol,
                       kind, direction, price0))
            c.commit()
        finally:
            c.close()
    except Exception as e:
        logger.debug("log_alert_failed", error=str(e))


def _price(token: str, chain: str) -> float | None:
    import json
    import urllib.request
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


def _hit(direction: str, price0: float, price1: float) -> int:
    if not price0 or not price1:
        return 0
    move = (price1 - price0) / price0
    if direction == "long":
        return 1 if move >= HIT_MOVE else 0
    if direction == "short":
        return 1 if move <= -HIT_MOVE else 0
    return 0


def resolve_outcomes() -> int:
    """Fill in 4h/24h prices + hit flags for alerts whose horizon has elapsed."""
    now = datetime.now(timezone.utc)
    c = _conn()
    resolved = 0
    try:
        rows = c.execute("SELECT id, ts, token, chain, direction, price0, price_4h, price_24h "
                         "FROM alerts WHERE resolved = 0").fetchall()
        for rid, ts, token, chain, direction, p0, p4, p24 in rows:
            age_h = (now - datetime.fromisoformat(ts)).total_seconds() / 3600
            updates = {}
            if p4 is None and age_h >= 4:
                px = _price(token, chain)
                if px:
                    updates["price_4h"] = px
                    updates["hit_4h"] = _hit(direction, p0, px)
            if p24 is None and age_h >= 24:
                px = _price(token, chain)
                if px:
                    updates["price_24h"] = px
                    updates["hit_24h"] = _hit(direction, p0, px)
            done = (p4 is not None or "price_4h" in updates) and \
                   (p24 is not None or "price_24h" in updates)
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
