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

All via GMGN through FlareSolverr. Every fetch carries source health so an upstream
failure or schema drift can never masquerade as an empty market. Honest ceiling
unchanged: still behind the creation-block insider snipers.
"""

from __future__ import annotations

import json
import math
import sqlite3
from datetime import datetime, timezone

import structlog

from src.config import DATA_DIR
from src.onchain.gmgn import CHAINS, _fs_get_result, usable

logger = structlog.get_logger()

DB = DATA_DIR / "smart_wallets.db"

# discretionary-skilled filter (drop bots / one-lucky-week). Chain-aware: sol/bsc have
# a fast degen scene (high winrate + many trades), base/eth trade slower (fewer buys,
# lower winrate is normal), so EVM uses looser activity + a higher profit bar so a
# lucky-few-wins wallet still can't slip in.
FILTERS = {
    "sol": {"winrate": 0.55, "realized": 5_000, "min_buys": 15, "max_buys": 800},
    "bsc": {"winrate": 0.55, "realized": 5_000, "min_buys": 15, "max_buys": 800},
    "base": {"winrate": 0.45, "realized": 10_000, "min_buys": 4, "max_buys": 800},
    "eth": {"winrate": 0.45, "realized": 10_000, "min_buys": 4, "max_buys": 800},
}
_DEFAULT_FILTER = {"winrate": 0.5, "realized": 8_000, "min_buys": 6, "max_buys": 800}
MAX_BUYS_7D = 800             # above this = HFT bot, not copyable
WATCH_PER_CHAIN = 14          # bounded source list
SMART_WALLET_HTTP_TIMEOUT_S = 20
SMART_WALLET_ROTATION_SLOTS = 3  # cover the set across the 45-minute activity window
ACTIVITY_CACHE_SCHEMA_VERSION = 1


def _conn() -> sqlite3.Connection:
    DB.parent.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(str(DB), timeout=10)
    c.execute("""CREATE TABLE IF NOT EXISTS watchlist(
        wallet TEXT, chain TEXT, winrate REAL, realized_7d REAL, buys_7d INTEGER,
        harvested_at TEXT, PRIMARY KEY(wallet, chain))""")
    c.execute("""CREATE TABLE IF NOT EXISTS activity_cache(
        wallet TEXT, chain TEXT, payload TEXT, checked_at REAL,
        PRIMARY KEY(wallet, chain))""")
    return c


def _finite_number(value, *, minimum: float | None = None,
                   maximum: float | None = None) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    if minimum is not None and number < minimum:
        return None
    if maximum is not None and number > maximum:
        return None
    return number


def _finite_count(value) -> int | None:
    number = _finite_number(value, minimum=0)
    if number is None or not number.is_integer():
        return None
    return int(number)


def _harvest_failure(chain_code: str, error_kind: str) -> dict:
    preserved = len(watchlist(chain_code))
    return {
        "state": "failed", "error_kind": error_kind, "chain": chain_code,
        "received": 0, "validated": 0, "kept": preserved, "preserved": True,
    }


def harvest_result(chain_code: str) -> dict:
    """Refresh one chain only after validating a non-empty wallet rank in full.

    Empty, malformed, blocked, or schema-drifted responses preserve the last-known
    watchlist. A verified non-empty rank may legitimately filter down to zero.
    """
    fetched = _fs_get_result(
        f"https://gmgn.ai/defi/quotation/v1/rank/{chain_code}/wallets/7d"
        f"?orderby=pnl_7d&direction=desc")
    if fetched["state"] != "ok":
        return _harvest_failure(
            chain_code, fetched.get("error_kind") or "wallet_rank_fetch_failed")
    payload = fetched.get("payload")
    data = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data, dict):
        return _harvest_failure(chain_code, "missing_rank_data")
    if "rank" not in data:
        return _harvest_failure(chain_code, "missing_rank")
    rank = data.get("rank")
    if not isinstance(rank, list):
        return _harvest_failure(chain_code, "invalid_rank_schema")
    if not rank:
        return _harvest_failure(chain_code, "suspicious_empty_rank")

    validated = []
    for row in rank:
        if not isinstance(row, dict):
            return _harvest_failure(chain_code, "invalid_rank_row")
        address = row.get("address")
        winrate = _finite_number(row.get("winrate_7d"), minimum=0, maximum=1)
        realized = _finite_number(row.get("realized_profit_7d"))
        buys = _finite_count(row.get("buy_7d"))
        if (not isinstance(address, str) or not address.strip()
                or winrate is None or realized is None or buys is None):
            return _harvest_failure(chain_code, "invalid_rank_row")
        validated.append((address.strip(), winrate, realized, buys))

    flt = FILTERS.get(chain_code, _DEFAULT_FILTER)
    kept = []
    seen = set()
    for address, winrate, realized, buys in validated:
        if address in seen:
            continue
        seen.add(address)
        if (winrate >= flt["winrate"] and realized >= flt["realized"]
                and flt["min_buys"] <= buys <= flt["max_buys"]):
            kept.append((address, winrate, realized, buys))
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
    logger.info("smart_wallets_harvested", chain=chain_code, kept=len(kept),
                scanned=len(rank), state="ok")
    return {
        "state": "ok", "error_kind": None, "chain": chain_code,
        "received": len(rank), "validated": len(validated),
        "kept": len(kept), "preserved": False,
    }


def harvest(chain_code: str) -> int:
    """Compatibility view returning the current kept/preserved wallet count."""
    return harvest_result(chain_code)["kept"]


def watchlist(chain_code: str | None = None) -> list[dict]:
    c = _conn()
    try:
        q = "SELECT wallet, chain, winrate, realized_7d, buys_7d FROM watchlist"
        rows = c.execute(q + (" WHERE chain=?" if chain_code else "") + " ORDER BY wallet",
                         (chain_code,) if chain_code else ()).fetchall()
    finally:
        c.close()
    return [{"wallet": r[0], "chain": r[1], "winrate": r[2], "realized_7d": r[3], "buys_7d": r[4]}
            for r in rows]


def recent_buys_result(wallet: str, chain_code: str, window_min: int = 40,
                       request_timeout_s: int = SMART_WALLET_HTTP_TIMEOUT_S,
                       *, now_ts: float | None = None) -> dict:
    """Fetch a wallet's recent BUY events without collapsing failures into []."""
    now_ts = now_ts if now_ts is not None else datetime.now(timezone.utc).timestamp()
    fetched = _fs_get_result(
        f"https://gmgn.ai/api/v1/wallet_activity/{chain_code}?wallet={wallet}&limit=20",
        timeout=request_timeout_s,
    )
    if fetched["state"] != "ok":
        return {"state": "failed", "buys": [],
                "error_kind": fetched.get("error_kind") or "activity_fetch_failed",
                "received": 0, "accepted": 0}
    payload = fetched.get("payload")
    data = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data, dict):
        return {"state": "failed", "buys": [], "error_kind": "missing_activity_data",
                "received": 0, "accepted": 0}
    if "activities" not in data:
        return {"state": "failed", "buys": [], "error_kind": "missing_activities",
                "received": 0, "accepted": 0}
    acts = data.get("activities")
    if not isinstance(acts, list):
        return {"state": "failed", "buys": [],
                "error_kind": "invalid_activities_schema",
                "received": 0, "accepted": 0}
    out = []
    for a in acts:
        if not isinstance(a, dict) or not isinstance(a.get("event_type"), str):
            return {"state": "failed", "buys": [],
                    "error_kind": "invalid_activity_row",
                    "received": len(acts), "accepted": 0}
        if a["event_type"] != "buy":
            continue
        ts = _finite_number(a.get("timestamp"), minimum=0)
        cost_usd = _finite_number(a.get("cost_usd"), minimum=0)
        token = a.get("token")
        address = token.get("address") if isinstance(token, dict) else None
        if (ts is None or cost_usd is None or not isinstance(address, str)
                or not address.strip() or ts > now_ts + 5 * 60):
            return {"state": "failed", "buys": [],
                    "error_kind": "invalid_activity_row",
                    "received": len(acts), "accepted": 0}
        # Allow small upstream/client clock skew, but never publish negative mins_ago.
        ts = min(ts, now_ts)
        if now_ts - ts > window_min * 60:
            continue
        symbol = token.get("symbol")
        out.append({"token": address.strip(),
                    "symbol": symbol if isinstance(symbol, str) and symbol else "?",
                    "cost_usd": cost_usd, "ts": ts})
    return {"state": "ok", "buys": out, "error_kind": None,
            "received": len(acts), "accepted": len(out)}


def recent_buys(wallet: str, chain_code: str, window_min: int = 40,
                request_timeout_s: int = SMART_WALLET_HTTP_TIMEOUT_S) -> list[dict] | None:
    """Compatibility view; None means source/schema failure, [] is verified empty."""
    result = recent_buys_result(
        wallet, chain_code, window_min, request_timeout_s=request_timeout_s)
    return result["buys"] if result["state"] == "ok" else None


MIN_BUY_USD = 20             # drop dust / fee-sized buys ($1 noise)


def _rotation_batch(jobs: list[tuple[str, str]], now_ts: float) -> list[tuple[str, str]]:
    """Select one deterministic third of the watchlist for this 15-minute slot."""
    if not jobs:
        return []
    batch_size = math.ceil(len(jobs) / SMART_WALLET_ROTATION_SLOTS)
    slot = int(now_ts // (15 * 60))
    start = (slot * batch_size) % len(jobs)
    return [jobs[(start + offset) % len(jobs)] for offset in range(batch_size)]


def _update_activity_cache(observations: list[tuple[str, str, list[dict] | None]],
                           checked_at: float) -> None:
    rows = [
        (wallet, chain, json.dumps({
            "schema_version": ACTIVITY_CACHE_SCHEMA_VERSION,
            "activities": buys,
        }, separators=(",", ":")), checked_at)
        for chain, wallet, buys in observations
        if buys is not None
    ]
    if not rows:
        return
    c = _conn()
    try:
        c.executemany(
            "INSERT OR REPLACE INTO activity_cache(wallet,chain,payload,checked_at) "
            "VALUES(?,?,?,?)",
            rows,
        )
        c.commit()
    finally:
        c.close()


def _fresh_cached_activity(wallet_jobs: list[tuple[str, str]], window_min: int,
                           now_ts: float) -> list[tuple[str, str, list[dict]]]:
    allowed = set(wallet_jobs)
    c = _conn()
    try:
        rows = c.execute(
            "SELECT chain,wallet,payload FROM activity_cache WHERE checked_at>=?",
            (now_ts - window_min * 60,),
        ).fetchall()
    finally:
        c.close()
    out = []
    for chain, wallet, payload in rows:
        if (chain, wallet) not in allowed:
            continue
        try:
            cached = json.loads(payload)
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        # Legacy bare lists include poisoned false-empty rows from the old schema.
        # Ignore them immediately; only a fresh verified fetch can repopulate cache.
        cache_version = (
            cached.get("schema_version") if isinstance(cached, dict) else None)
        if (not isinstance(cached, dict)
                or type(cache_version) is not int
                or cache_version != ACTIVITY_CACHE_SCHEMA_VERSION
                or not isinstance(cached.get("activities"), list)):
            continue
        buys = []
        valid = True
        for row in cached["activities"]:
            if not isinstance(row, dict):
                valid = False
                break
            token = row.get("token")
            symbol = row.get("symbol")
            cost_usd = _finite_number(row.get("cost_usd"), minimum=0)
            ts = _finite_number(row.get("ts"), minimum=0)
            if (not isinstance(token, str) or not token.strip()
                    or not isinstance(symbol, str)
                    or cost_usd is None or ts is None):
                valid = False
                break
            buys.append({"token": token.strip(), "symbol": symbol or "?",
                         "cost_usd": cost_usd, "ts": ts})
        if valid:
            out.append((chain, wallet, buys))
    return out


def fresh_smart_buys_result(chain_codes=("sol", "bsc", "base", "eth"),
                            window_min: int = 40, *, now_ts: float | None = None) -> dict:
    """Poll a rotating bounded batch and aggregate fresh cached wallet activity.

    FlareSolverr serializes these Cloudflare browser requests in practice. Polling all
    wallets every run made the 15-minute publication job take almost five minutes;
    parallel requests timed out together. A deterministic three-slot rotation keeps
    each run bounded while the cache retains every still-valid observation.
    """
    chain_codes = tuple(chain_codes)
    now_ts = now_ts if now_ts is not None else datetime.now(timezone.utc).timestamp()
    wallet_jobs = [
        (ch, w["wallet"])
        for ch in chain_codes
        for w in watchlist(ch)
    ]
    selected = _rotation_batch(wallet_jobs, now_ts)
    observations: list[tuple[str, str, list[dict] | None]] = []
    errors: list[tuple[str, str]] = []
    source_available = usable()
    if source_available:
        for ch, wallet in selected:
            try:
                activity = recent_buys_result(
                    wallet,
                    ch,
                    window_min,
                    request_timeout_s=SMART_WALLET_HTTP_TIMEOUT_S,
                    now_ts=now_ts,
                )
            except Exception as exc:
                logger.debug("smart_wallet_activity_failed", chain=ch,
                             wallet=wallet, error=str(exc)[:80])
                activity = {"state": "failed", "buys": [],
                            "error_kind": "activity_exception"}
            if activity.get("state") == "ok":
                observations.append((ch, wallet, activity.get("buys", [])))
            else:
                errors.append((ch, activity.get("error_kind") or "activity_fetch_failed"))
        _update_activity_cache(observations, now_ts)
    else:
        errors.extend((ch, "source_unavailable") for ch, _wallet in selected)

    cached = _fresh_cached_activity(wallet_jobs, window_min, now_ts)
    agg: dict = {}
    for ch, wallet, buys in cached:
        for b in buys:
            if now_ts - (b.get("ts") or 0) > window_min * 60:
                continue
            if b["cost_usd"] < MIN_BUY_USD:
                continue
            key = (ch, b["token"])
            e = agg.setdefault(key, {"symbol": b["symbol"], "chain": CHAINS.get(ch, ch),
                                     "token": b["token"], "buyers": set(),
                                     "usd": 0.0, "latest_ts": 0})
            e["buyers"].add(wallet)
            e["usd"] += b["cost_usd"]
            e["latest_ts"] = max(e["latest_ts"], b["ts"])
    out = []
    for e in agg.values():
        n = len(e["buyers"])
        out.append({
            "symbol": e["symbol"], "chain": e["chain"], "token": e["token"],
            "n_buyers": n, "buyers": list(e["buyers"])[:5],
            "usd_bought": round(e["usd"]),
            "mins_ago": round((now_ts - e["latest_ts"]) / 60, 1) if e["latest_ts"] else None,
            "strength": "收敛" if n >= 2 else "单个",
        })
    out.sort(key=lambda x: (-x["n_buyers"], x["mins_ago"] if x["mins_ago"] is not None else 999))
    observed = len(observations)
    failed = len(selected) - observed if source_available else len(selected)
    fresh_cached = len(cached)
    configured_by_chain = {
        ch: sum(job_chain == ch for job_chain, _wallet in wallet_jobs)
        for ch in chain_codes
    }
    missing_chain_coverage = any(count == 0 for count in configured_by_chain.values())
    error_counts: dict[str, int] = {}
    for _chain, error_kind in errors:
        error_counts[error_kind] = error_counts.get(error_kind, 0) + 1
    if not wallet_jobs:
        state, error_kind = "failed", "no_configured_wallets"
    elif not source_available:
        state, error_kind = "failed", "source_unavailable"
    elif selected and observed == 0:
        state, error_kind = "failed", "all_requests_failed"
    elif failed or fresh_cached < len(wallet_jobs) or missing_chain_coverage:
        state = "partial"
        error_kind = (
            "chain_coverage_gap"
            if missing_chain_coverage and not failed and fresh_cached == len(wallet_jobs)
            else "request_or_rotation_gap")
    else:
        state, error_kind = "ok", None

    cached_by_chain = {
        ch: sum(cached_chain == ch for cached_chain, _wallet, _buys in cached)
        for ch in chain_codes
    }
    chain_health = []
    for ch in chain_codes:
        configured = configured_by_chain[ch]
        requested = sum(job_chain == ch for job_chain, _wallet in selected)
        chain_observed = sum(job_chain == ch for job_chain, _wallet, _buys in observations)
        chain_failed = requested - chain_observed
        chain_errors: dict[str, int] = {}
        for error_chain, chain_error in errors:
            if error_chain == ch:
                chain_errors[chain_error] = chain_errors.get(chain_error, 0) + 1
        if configured == 0:
            chain_state, chain_error = "failed", "no_configured_wallets"
        elif requested and chain_observed == 0:
            chain_state, chain_error = "failed", (
                "source_unavailable" if not source_available else "all_requests_failed")
        elif chain_failed or cached_by_chain[ch] < configured:
            chain_state, chain_error = "partial", "request_or_rotation_gap"
        else:
            chain_state, chain_error = "ok", None
        chain_health.append({
            "chain": ch, "state": chain_state, "error_kind": chain_error,
            "configured_wallets": configured, "requested": requested,
            "observed": chain_observed, "request_failed": chain_failed,
            "fresh_cached_wallets": cached_by_chain[ch],
            "error_counts": chain_errors,
        })
    return {
        "buys": out,
        "source_health": {
            "state": state,
            "error_kind": error_kind,
            "configured_wallets": len(wallet_jobs),
            "requested": len(selected),
            "observed": observed,
            "request_failed": failed,
            "fresh_cached_wallets": fresh_cached,
            "error_counts": error_counts,
            "chains": chain_health,
            "rotation_slots": SMART_WALLET_ROTATION_SLOTS,
            "window_min": window_min,
            "request_timeout_s": SMART_WALLET_HTTP_TIMEOUT_S,
            "checked_at": datetime.fromtimestamp(now_ts, timezone.utc).isoformat(),
        },
    }


def fresh_smart_buys(chain_codes=("sol", "bsc", "base", "eth"),
                     window_min: int = 40) -> list[dict] | None:
    """Compatibility view; None means the current source sweep failed completely."""
    result = fresh_smart_buys_result(chain_codes=chain_codes, window_min=window_min)
    if result["source_health"]["state"] == "failed":
        return None
    return result["buys"]


def harvest_all(chain_codes=("sol", "bsc", "base", "eth")) -> dict:
    chain_codes = tuple(chain_codes)
    if usable():
        chain_health = [harvest_result(ch) for ch in chain_codes]
    else:
        chain_health = [
            _harvest_failure(ch, "flaresolverr_unavailable") for ch in chain_codes
        ]
    succeeded = sum(row["state"] == "ok" for row in chain_health)
    failed = len(chain_health) - succeeded
    state = "failed" if succeeded == 0 else ("partial" if failed else "ok")
    error_kind = (
        "all_chains_failed" if state == "failed"
        else "chain_gap" if state == "partial" else None)
    return {
        "harvested": sum(row["kept"] for row in chain_health if row["state"] == "ok"),
        "watchlisted": sum(row["kept"] for row in chain_health),
        "chains": list(chain_codes),
        "source_health": {
            "state": state, "error_kind": error_kind,
            "requested_chains": len(chain_health), "successful_chains": succeeded,
            "failed_chains": failed, "chains": chain_health,
            "checked_at": datetime.now(timezone.utc).isoformat(),
        },
    }


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
