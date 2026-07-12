"""Real-time smart-wallet watch — the ONE way to be earlier than an aggregate rank.

A rank ('N smart wallets bought X') only lights up AFTER the crowd piles in. Watching
the wallets themselves catches the buy the moment it lands — before it aggregates. So:

  1. harvest() a standing watchlist of PROVEN-PROFITABLE, DISCRETIONARY wallets from
     GMGN's wallet PnL rank (filtered: real winrate + real realized profit + a human
     trade count, not a bot doing thousands of swaps).
  2. fresh_smart_buys() polls each watched wallet's recent activity and aggregates by
     token: 'in the last N min, these K watched wallets bought TOKEN.' K>=2 different
     proven wallets on the same fresh token in the same window = convergence, the
     earliest actionable signal a dashboard can honestly give.

All via GMGN through FlareSolverr; returns []/None when it's down (caller falls back).
Honest ceiling unchanged: still behind the creation-block insider snipers.
"""

from __future__ import annotations

import json
import sqlite3
import urllib.request
from datetime import datetime, timezone

import structlog

from src.config import DATA_DIR
from src.onchain.gmgn import CHAINS, _fs_get, usable

logger = structlog.get_logger()

DB = DATA_DIR / "smart_wallets.db"

# discretionary-skilled filter (drop bots / one-lucky-week)
MIN_WINRATE = 0.55
MIN_REALIZED_7D = 5_000       # USD actually made this week
MIN_BUYS_7D = 15
MAX_BUYS_7D = 800             # above this = HFT bot, not copyable
WATCH_PER_CHAIN = 14          # bounded — each poll is a serial FlareSolverr call


def _conn() -> sqlite3.Connection:
    DB.parent.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(str(DB), timeout=10)
    c.execute("""CREATE TABLE IF NOT EXISTS watchlist(
        wallet TEXT, chain TEXT, winrate REAL, realized_7d REAL, buys_7d INTEGER,
        harvested_at TEXT, PRIMARY KEY(wallet, chain))""")
    return c


def _f(v, d=0.0):
    try:
        return float(v)
    except (TypeError, ValueError):
        return d


def harvest(chain_code: str) -> int:
    """Refresh the watchlist for one GMGN chain code (sol/bsc/base/eth). Returns count
    kept. Run infrequently (daily) — the skilled set is stable day-to-day."""
    d = _fs_get(f"https://gmgn.ai/defi/quotation/v1/rank/{chain_code}/wallets/7d"
                f"?orderby=pnl_7d&direction=desc")
    rank = ((d or {}).get("data") or {}).get("rank") or []
    kept = []
    for w in rank:
        wr, real, buys = _f(w.get("winrate_7d")), _f(w.get("realized_profit_7d")), int(w.get("buy_7d") or 0)
        if wr >= MIN_WINRATE and real >= MIN_REALIZED_7D and MIN_BUYS_7D <= buys <= MAX_BUYS_7D:
            kept.append((w.get("address"), wr, real, buys))
    kept.sort(key=lambda x: -x[2])           # by realized profit
    kept = kept[:WATCH_PER_CHAIN]
    now = datetime.now(timezone.utc).isoformat()
    c = _conn()
    try:
        c.execute("DELETE FROM watchlist WHERE chain=?", (chain_code,))
        c.executemany("INSERT OR REPLACE INTO watchlist VALUES (?,?,?,?,?,?)",
                      [(a, chain_code, wr, real, b, now) for a, wr, real, b in kept])
        c.commit()
    finally:
        c.close()
    logger.info("smart_wallets_harvested", chain=chain_code, kept=len(kept), scanned=len(rank))
    return len(kept)


def watchlist(chain_code: str | None = None) -> list[dict]:
    c = _conn()
    try:
        q = "SELECT wallet, chain, winrate, realized_7d, buys_7d FROM watchlist"
        rows = c.execute(q + (" WHERE chain=?" if chain_code else ""),
                         (chain_code,) if chain_code else ()).fetchall()
    finally:
        c.close()
    return [{"wallet": r[0], "chain": r[1], "winrate": r[2], "realized_7d": r[3], "buys_7d": r[4]}
            for r in rows]


def recent_buys(wallet: str, chain_code: str, window_min: int = 40) -> list[dict]:
    """A watched wallet's BUY events in the last `window_min` minutes (GMGN activity)."""
    now = datetime.now(timezone.utc).timestamp()
    d = _fs_get(f"https://gmgn.ai/api/v1/wallet_activity/{chain_code}?wallet={wallet}&limit=20")
    acts = ((d or {}).get("data") or {}).get("activities") or []
    out = []
    for a in acts:
        if a.get("event_type") != "buy":
            continue
        ts = a.get("timestamp") or 0
        if now - ts > window_min * 60:
            continue
        tk = a.get("token") or {}
        if not tk.get("address"):
            continue
        out.append({"token": tk["address"], "symbol": tk.get("symbol", "?"),
                    "cost_usd": _f(a.get("cost_usd")), "ts": ts})
    return out


MIN_BUY_USD = 20             # drop dust / fee-sized buys ($1 noise)


def fresh_smart_buys(chain_codes=("sol", "bsc", "base", "eth"),
                     window_min: int = 40) -> list[dict] | None:
    """Tokens that WATCHED proven wallets just bought, aggregated across wallets.
    Ranked by number of DISTINCT watched buyers (convergence) then recency. None if
    FlareSolverr is down."""
    if not usable():
        return None
    agg: dict = {}
    for ch in chain_codes:
        for w in watchlist(ch):
            for b in recent_buys(w["wallet"], ch, window_min):
                if b["cost_usd"] < MIN_BUY_USD:
                    continue
                key = (ch, b["token"])
                e = agg.setdefault(key, {"symbol": b["symbol"], "chain": CHAINS.get(ch, ch),
                                         "token": b["token"], "buyers": set(),
                                         "usd": 0.0, "latest_ts": 0})
                e["buyers"].add(w["wallet"])
                e["usd"] += b["cost_usd"]
                e["latest_ts"] = max(e["latest_ts"], b["ts"])
    now = datetime.now(timezone.utc).timestamp()
    out = []
    for e in agg.values():
        n = len(e["buyers"])
        out.append({
            "symbol": e["symbol"], "chain": e["chain"], "token": e["token"],
            "n_buyers": n, "buyers": list(e["buyers"])[:5],
            "usd_bought": round(e["usd"]),
            "mins_ago": round((now - e["latest_ts"]) / 60, 1) if e["latest_ts"] else None,
            "strength": "收敛" if n >= 2 else "单个",
        })
    out.sort(key=lambda x: (-x["n_buyers"], x["mins_ago"] if x["mins_ago"] is not None else 999))
    return out


def harvest_all(chain_codes=("sol", "bsc", "base", "eth")) -> dict:
    if not usable():
        return {"harvested": 0, "note": "flaresolverr down"}
    total = sum(harvest(ch) for ch in chain_codes)
    return {"harvested": total, "chains": list(chain_codes)}


if __name__ == "__main__":
    import sys

    from dotenv import load_dotenv

    from src.config import PROJECT_ROOT
    load_dotenv(PROJECT_ROOT / ".env")
    if "--harvest" in sys.argv:
        print(harvest_all())
    wl = watchlist()
    print(f"watchlist: {len(wl)} wallets")
    buys = fresh_smart_buys()
    print(f"{len(buys or [])} tokens bought by watched wallets in the window")
    for b in (buys or [])[:15]:
        print(f"  {b['symbol'][:12]:12} [{b['chain']:8}] {b['n_buyers']}个聪明钱买 "
              f"[{b['strength']}] {b['mins_ago']}分钟前 ${b['usd_bought']}")
