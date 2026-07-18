"""Falsify — or prove — the smart-money convergence offense, for free.

`smart_wallets.fresh_smart_buys` already fires "K proven-profitable wallets
bought token X in the last N min" convergence signals, and the board publishes
them as its earliest lane. But nothing ever recorded whether those signals
actually PREDICTED A PUMP, so the one accessible offense line is an unproven
vibe — exactly the thing this repo exists to distrust.

This ledger freezes each convergence event with a keyless DexScreener entry
price and resolves its forward return at 1h / 24h / 7d, so after a few weeks the
hit rate and expectancy are measured, not assumed. Two honesty rules:

  1. Entry is our DETECTION price. The proven wallets bought first; we are late by
     construction. The measured edge is therefore net of that lateness — the real
     number a follower would get, not the wallets' own entry.
  2. Nothing is hard-filtered up front (that biases the sample). Each event carries
     a farm-likeness co-occurrence score, so the outcomes can later split diverse
     independent convergence from the same wallet-farm churning together — the
     failure mode that fakes convergence.

No paid data (GMGN via FlareSolverr + keyless DexScreener), no real orders.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timezone

from src.config import DATA_DIR

DB = DATA_DIR / "convergence_ledger.db"
MIN_BUYERS = 3               # ≥3 distinct proven wallets — not ≥2 (a pair is cheap to farm)
HORIZONS = {"1h": 3600, "24h": 86_400, "7d": 604_800}
FARM_LIKE_THRESHOLD = 0.6    # buyer-set overlap with a prior event at/above this = farm-like

# fresh_smart_buys reports the display chain name; map to a DexScreener slug.
_DEX_CHAIN = {
    "solana": "solana", "sol": "solana",
    "bsc": "bsc", "bnb": "bsc",
    "base": "base",
    "eth": "ethereum", "ethereum": "ethereum",
}


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).isoformat()


def dex_chain(display: object) -> str | None:
    return _DEX_CHAIN.get(str(display or "").strip().lower())


def _conn() -> sqlite3.Connection:
    DB.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB), timeout=10)
    conn.execute("""CREATE TABLE IF NOT EXISTS events(
        id TEXT PRIMARY KEY, token TEXT NOT NULL, chain TEXT NOT NULL,
        symbol TEXT, first_seen_at TEXT NOT NULL, first_seen_ts REAL NOT NULL,
        n_buyers INTEGER NOT NULL, buyers TEXT NOT NULL, usd_bought REAL,
        co_occurrence REAL NOT NULL, entry_price_usd REAL, entry_fdv_usd REAL,
        entry_liquidity_usd REAL, price_source TEXT)""")
    conn.execute("""CREATE TABLE IF NOT EXISTS outcomes(
        event_id TEXT NOT NULL, horizon TEXT NOT NULL, due_ts REAL NOT NULL,
        resolved_at TEXT, price_usd REAL, return_pct REAL,
        PRIMARY KEY(event_id, horizon))""")
    return conn


def _event_id(chain: str, token: str, first_seen_ts: float) -> str:
    # One event per token per UTC day: a re-convergence a week later is new, but the
    # same wave across adjacent 15-min polls collapses to one frozen event.
    day = datetime.fromtimestamp(first_seen_ts, timezone.utc).strftime("%Y%m%d")
    raw = f"{chain}:{token.lower()}:{day}"
    return hashlib.sha256(raw.encode()).hexdigest()[:20]


def co_occurrence_score(buyers: list[str], conn: sqlite3.Connection) -> float:
    """Max buyer-set overlap (Jaccard) against every prior event.

    A set of wallets that keeps showing up together on many tokens is a farm
    manufacturing fake convergence, not independent conviction. High score =
    farm-like. Self-contained: computed from this ledger's own history, no paid
    data. Returns 0 when there is no prior event to compare against.
    """
    current = {w.lower() for w in buyers if isinstance(w, str) and w.strip()}
    if not current:
        return 0.0
    best = 0.0
    for (raw,) in conn.execute("SELECT buyers FROM events"):
        try:
            prior = {str(w).lower() for w in json.loads(raw)}
        except (TypeError, ValueError):
            continue
        union = current | prior
        if union:
            best = max(best, len(current & prior) / len(union))
    return round(best, 3)


def record(buys: list[dict], *, price_fn, now: float, min_buyers: int = MIN_BUYERS,
           conn: sqlite3.Connection | None = None) -> dict:
    """Freeze new convergence events (≥min_buyers distinct wallets) with an entry price."""
    owned = conn is None
    conn = conn or _conn()
    inserted = skipped_existing = skipped_small = skipped_chain = 0
    try:
        for row in buys or []:
            if not isinstance(row, dict):
                continue
            if int(row.get("n_buyers") or 0) < min_buyers:
                skipped_small += 1
                continue
            token = str(row.get("token") or "").strip()
            chain = dex_chain(row.get("chain"))
            if not token or chain is None:
                skipped_chain += 1
                continue
            event_id = _event_id(chain, token, now)
            if conn.execute("SELECT 1 FROM events WHERE id=?", (event_id,)).fetchone():
                skipped_existing += 1
                continue
            buyers = [str(w) for w in (row.get("buyers") or []) if isinstance(w, str)]
            score = co_occurrence_score(buyers, conn)
            price = None
            try:
                price = price_fn(token, chain)
            except Exception:
                price = None
            entry = price or {}
            conn.execute(
                "INSERT INTO events VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (event_id, token, chain, row.get("symbol"), _iso(now), now,
                 int(row["n_buyers"]), json.dumps(buyers),
                 _num(row.get("usd_bought")), score,
                 _num(entry.get("price_usd")), _num(entry.get("fdv_usd")),
                 _num(entry.get("liquidity_usd")),
                 "dexscreener" if price else "unavailable"),
            )
            conn.executemany(
                "INSERT OR IGNORE INTO outcomes(event_id,horizon,due_ts) VALUES (?,?,?)",
                [(event_id, h, now + secs) for h, secs in HORIZONS.items()],
            )
            inserted += 1
        conn.commit()
    finally:
        if owned:
            conn.close()
    return {"inserted": inserted, "skipped_existing": skipped_existing,
            "skipped_small": skipped_small, "skipped_chain": skipped_chain}


def resolve_due(*, price_fn, now: float,
                conn: sqlite3.Connection | None = None) -> dict:
    """Fill every due, unresolved horizon with its forward return."""
    owned = conn is None
    conn = conn or _conn()
    resolved = priced = 0
    try:
        due = conn.execute(
            """SELECT o.event_id, o.horizon, e.token, e.chain, e.entry_price_usd
               FROM outcomes o JOIN events e ON e.id=o.event_id
               WHERE o.resolved_at IS NULL AND o.due_ts<=?""",
            (now,),
        ).fetchall()
        for event_id, horizon, token, chain, entry in due:
            price = None
            try:
                price = price_fn(token, chain)
            except Exception:
                price = None
            current = _num((price or {}).get("price_usd"))
            ret = None
            if (current is not None and entry is not None and entry > 0):
                ret = round((current / entry - 1) * 100, 3)
                priced += 1
            conn.execute(
                "UPDATE outcomes SET resolved_at=?,price_usd=?,return_pct=? "
                "WHERE event_id=? AND horizon=?",
                (_iso(now), current, ret, event_id, horizon),
            )
            resolved += 1
        conn.commit()
    finally:
        if owned:
            conn.close()
    return {"resolved": resolved, "priced": priced}


def summary(*, conn: sqlite3.Connection | None = None) -> dict:
    """Measured hit rate + mean forward return per horizon, split diverse vs farm-like."""
    owned = conn is None
    conn = conn or _conn()
    try:
        n_events = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        priced_entries = conn.execute(
            "SELECT COUNT(*) FROM events WHERE entry_price_usd IS NOT NULL"
        ).fetchone()[0]
        out: dict = {"n_events": n_events, "n_priced_entries": priced_entries,
                     "min_buyers": MIN_BUYERS, "horizons": {}}
        for horizon in HORIZONS:
            buckets: dict = {}
            rows = conn.execute(
                """SELECT o.return_pct, e.co_occurrence
                   FROM outcomes o JOIN events e ON e.id=o.event_id
                   WHERE o.horizon=? AND o.return_pct IS NOT NULL""",
                (horizon,),
            ).fetchall()
            for label in ("all", "diverse", "farm_like"):
                rets = [
                    r for r, co in rows
                    if label == "all"
                    or (label == "diverse" and co < FARM_LIKE_THRESHOLD)
                    or (label == "farm_like" and co >= FARM_LIKE_THRESHOLD)
                ]
                buckets[label] = _stats(rets)
            out["horizons"][horizon] = buckets
        return out
    finally:
        if owned:
            conn.close()


def _stats(rets: list[float]) -> dict:
    n = len(rets)
    if not n:
        return {"n": 0, "hit_rate": None, "mean_return_pct": None,
                "median_return_pct": None}
    s = sorted(rets)
    return {
        "n": n,
        "hit_rate": round(sum(r > 0 for r in rets) / n, 3),
        "mean_return_pct": round(sum(rets) / n, 3),
        "median_return_pct": round(s[n // 2], 3),
    }


def _num(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if out == out and out not in (float("inf"), float("-inf")) else None


def _dexscreener_price(token: str, chain: str) -> dict | None:
    """Keyless DexScreener price for the token's highest-liquidity pair."""
    from src.onchain import token_identity

    pairs = token_identity._pairs_for(token, chain)
    best = token_identity._best_pair(pairs)
    if not best:
        return None
    price = _num(best.get("priceUsd"))
    if price is None:
        return None
    return {"price_usd": price, "fdv_usd": _num(best.get("fdv")),
            "liquidity_usd": _num((best.get("liquidity") or {}).get("usd"))}


def run(*, now: float | None = None) -> dict:
    """Fetch fresh convergence, freeze new events, resolve due outcomes. Free."""
    from src.onchain import smart_wallets

    current = now if now is not None else datetime.now(timezone.utc).timestamp()
    result = smart_wallets.fresh_smart_buys_result(now_ts=current)
    conn = _conn()
    try:
        recorded = record(result.get("buys") or [], price_fn=_dexscreener_price,
                          now=current, conn=conn)
        resolved = resolve_due(price_fn=_dexscreener_price, now=current, conn=conn)
    finally:
        conn.close()
    return {"recorded": recorded, "resolved": resolved,
            "source_state": (result.get("source_health") or {}).get("state")}


if __name__ == "__main__":
    print(json.dumps(run(), ensure_ascii=False, indent=2))
