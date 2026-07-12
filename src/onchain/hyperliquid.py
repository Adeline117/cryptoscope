"""Hyperliquid perp signals — keyless, one POST gives OI + funding + mark + vol for
every coin. Powers two get-rich structures on the board:

  · CASCADE / crowding (#3): extreme funding = one side is crowded and paying to stay
    in — that side is the fuel. High + funding → longs crowded → down-cascade risk
    (position short-side / avoid longs). High − funding → shorts crowded → squeeze-up
    risk. This is computable from a SINGLE snapshot (no history needed).
  · IGNITION (#2): open interest rising WITH price + volume = new leveraged money
    entering the move → the trend has fuel (vs OI falling on a move = short-covering,
    weaker). Needs OI change over time, so we persist a snapshot each run and diff.

Honest limits shipped with the signal: funding/OI raise cascade/ignition PROBABILITY,
they do NOT time it (312/LUNA/FTX are survivorship-flavored); thresholds are regime-
dependent — we score |funding| annualized and OI change, and label strength, never
'certain'. Nothing here is a trade instruction.
"""

from __future__ import annotations

import json
import sqlite3
import urllib.request
from datetime import datetime, timezone

import structlog

from src.config import DATA_DIR

logger = structlog.get_logger()

INFO_URL = "https://api.hyperliquid.xyz/info"
SNAP_DB = DATA_DIR / "perp_snapshots.db"

# thresholds (heuristics, labeled as such — annualized funding %)
FUNDING_CROWDED_ANN = 50.0     # |ann funding| above this = a crowded, paying side
MIN_OI_USD = 500_000           # ignore illiquid coins (noise)
IGNITION_OI_JUMP = 0.05        # +5% OI since last snapshot = leverage piling in
SNAPSHOT_MIN_GAP_MIN = 20      # diff against a snapshot at least this old


def _fetch_ctxs() -> list[dict]:
    """[{name, markPx, openInterest, funding, dayNtlVlm, prevDayPx, oi_usd, funding_ann,
    price_chg_24h, vol24}] for every perp. Never raises → [] on failure."""
    try:
        req = urllib.request.Request(
            INFO_URL, data=json.dumps({"type": "metaAndAssetCtxs"}).encode(),
            headers={"Content-Type": "application/json"})
        d = json.loads(urllib.request.urlopen(req, timeout=20).read())
    except Exception as e:
        logger.warning("hyperliquid_fetch_failed", error=str(e)[:100])
        return []
    universe, ctxs = d[0].get("universe", []), d[1]
    out = []
    for u, c in zip(universe, ctxs):
        try:
            px = float(c.get("markPx") or 0)
            oi = float(c.get("openInterest") or 0) * px
            fund = float(c.get("funding") or 0)          # hourly
            vol = float(c.get("dayNtlVlm") or 0)
            prev = float(c.get("prevDayPx") or 0)
            out.append({
                "name": u.get("name"), "markPx": px, "oi_usd": oi,
                "funding_ann": fund * 24 * 365 * 100,     # annualized %
                "vol24": vol,
                "price_chg_24h": ((px / prev - 1) * 100) if prev else None,
            })
        except Exception:
            continue
    return out


def _conn() -> sqlite3.Connection:
    SNAP_DB.parent.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(str(SNAP_DB), timeout=10)
    c.execute("""CREATE TABLE IF NOT EXISTS snaps(
        coin TEXT, ts TEXT, oi_usd REAL, mark REAL, vol24 REAL)""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_coin_ts ON snaps(coin, ts)")
    return c


def _store_and_diff(rows: list[dict]) -> None:
    """Persist this snapshot and annotate each row with oi_chg/price_chg since the last
    snapshot ≥ SNAPSHOT_MIN_GAP_MIN old (None on first run — honestly absent, not 0)."""
    now = datetime.now(timezone.utc)
    c = _conn()
    try:
        # read most-recent prior snapshot per coin within a sane window
        prev = {}
        for coin, ts, oi, mark in c.execute(
                "SELECT coin, ts, oi_usd, mark FROM snaps ORDER BY ts DESC"):
            if coin in prev:
                continue
            try:
                age = (now - datetime.fromisoformat(ts)).total_seconds() / 60
            except Exception:
                continue
            if age >= SNAPSHOT_MIN_GAP_MIN:
                prev[coin] = (oi, mark)
        for r in rows:
            p = prev.get(r["name"])
            if p and p[0] and p[1]:
                r["oi_chg_pct"] = (r["oi_usd"] / p[0] - 1) * 100
                r["price_chg_since"] = (r["markPx"] / p[1] - 1) * 100
            else:
                r["oi_chg_pct"] = None
                r["price_chg_since"] = None
        # write current snapshot
        c.executemany("INSERT INTO snaps(coin, ts, oi_usd, mark, vol24) VALUES (?,?,?,?,?)",
                      [(r["name"], now.isoformat(), r["oi_usd"], r["markPx"], r["vol24"]) for r in rows])
        # prune snapshots older than ~2 days
        c.execute("DELETE FROM snaps WHERE ts < ?",
                  ((now.replace(microsecond=0)).isoformat()[:10] + "T00:00:00+00:00",))
        c.commit()
    finally:
        c.close()


def _signal(r: dict) -> dict | None:
    """Classify a coin into a cascade or ignition signal, or None. Returns
    {kind, direction, strength, why}."""
    if r["oi_usd"] < MIN_OI_USD:
        return None
    fa = r["funding_ann"]
    # CASCADE / crowding — from funding alone
    if abs(fa) >= FUNDING_CROWDED_ANN:
        if fa > 0:
            return {"kind": "cascade", "direction": "longs_crowded",
                    "strength": "强" if fa >= 150 else "中",
                    "why": f"多头在付 {fa:.0f}%/年资金费 = 多头拥挤,一旦下跌他们是燃料 → 防向下清算,别接多"}
        else:
            return {"kind": "cascade", "direction": "shorts_crowded",
                    "strength": "强" if fa <= -150 else "中",
                    "why": f"空头在付 {abs(fa):.0f}%/年资金费 = 空头拥挤 → 防轧空向上"}
    # IGNITION — OI rising with price (needs history)
    oc, pc = r.get("oi_chg_pct"), r.get("price_chg_since")
    if oc is not None and pc is not None and oc >= IGNITION_OI_JUMP * 100:
        if pc > 0.5:
            return {"kind": "ignition", "direction": "up",
                    "strength": "强" if oc >= 15 else "中",
                    "why": f"未平仓 +{oc:.0f}%、价 +{pc:.1f}% 同向放量 = 新杠杆进场推涨,趋势有燃料"}
        if pc < -0.5:
            return {"kind": "ignition", "direction": "down",
                    "strength": "强" if oc >= 15 else "中",
                    "why": f"未平仓 +{oc:.0f}%、价 {pc:.1f}% 同向 = 新空头进场推跌"}
    return None


def perp_signals() -> list[dict]:
    """Ranked perp signals: cascade-crowding + ignition. Persists a snapshot each call
    so ignition (OI-delta) becomes available from the 2nd call on."""
    rows = _fetch_ctxs()
    if not rows:
        return []
    _store_and_diff(rows)
    out = []
    for r in rows:
        sig = _signal(r)
        if not sig:
            continue
        out.append({
            "symbol": r["name"], "signal": sig["kind"], "direction": sig["direction"],
            "strength": sig["strength"], "why": sig["why"],
            "funding_ann": round(r["funding_ann"], 1), "oi_usd": round(r["oi_usd"]),
            "vol24": round(r["vol24"]), "price_chg_24h": r["price_chg_24h"],
            "oi_chg_pct": r.get("oi_chg_pct"),
        })
    # rank: ignition first (actionable now), then cascade by |funding|, weighted by OI
    order = {"ignition": 0, "cascade": 1}
    out.sort(key=lambda x: (order.get(x["signal"], 2),
                            -abs(x["funding_ann"]) if x["signal"] == "cascade" else -(x["oi_chg_pct"] or 0)))
    return out


if __name__ == "__main__":
    from dotenv import load_dotenv

    from src.config import PROJECT_ROOT
    load_dotenv(PROJECT_ROOT / ".env")
    sigs = perp_signals()
    print(f"{len(sigs)} perp signals")
    for s in sigs[:15]:
        print(f"  {s['symbol']:9} {s['signal']:8} {s['direction']:14} [{s['strength']}] "
              f"fund {s['funding_ann']:+.0f}% OI ${s['oi_usd']/1e6:.1f}M — {s['why'][:60]}")
