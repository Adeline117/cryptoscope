"""Holder distribution snapshots — the foundation of accumulation detection.

A token must be snapshotted *from birth*; with no snapshot history the past is
permanently lost. This module:

1. Records token birth (first time we see it).
2. Fetches the current holder distribution (top holders) per chain.
3. Persists timestamped snapshots to SQLite.
4. Computes *nominal* concentration metrics (top-N %, holder count, gini).

The nominal metrics here are deliberately the "naive" view. The *effective*
concentration (after Sybil clustering) lives in `entity_clustering.py`; the
accumulation signal is the divergence between the two (see
`src/signals/accumulation_divergence.py`).

Fetchers are best-effort and free-tier:
- Solana: Helius / Solana RPC `getTokenLargestAccounts` (top accounts) + owner
  resolution. (Solscan free keys cannot access holders — HTTP 401.)
- EVM: reconstruct balances from Etherscan V2 `tokentx` over a recent window.

Full holder enumeration is a later upgrade; top-holder coverage is sufficient
to compute concentration and feed clustering on the significant holders.
"""

from __future__ import annotations

import json
import math
import os
import sqlite3
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import structlog

from src.config import DATA_DIR
from src.onchain.chain_identity import (
    EVM_CHAIN_IDS,
    MORALIS_CHAIN_SLUGS,
    canonical_chain,
    evm_chain_id,
    storage_chain_aliases,
)

logger = structlog.get_logger()

DB_PATH = DATA_DIR / "holder_snapshots.db"

def _canonical_chain(chain: str) -> str:
    """Compatibility wrapper that rejects unsupported storage identities."""
    canonical = canonical_chain(chain)
    if canonical is None:
        raise ValueError(f"unsupported chain identity: {chain!r}")
    return canonical


def _is_evm_chain(chain: str) -> bool:
    return evm_chain_id(chain) is not None


def _canonical_token(token: str, chain: str) -> str:
    """Canonical storage key without corrupting case-sensitive chain ids.

    EVM addresses are case-insensitive (checksum casing is presentation only), while
    Solana mints are base58 identifiers whose casing is part of the identity.
    """
    value = str(token or "")
    return value.lower() if _is_evm_chain(chain) else value


def _token_match_sql(chain: str) -> str:
    """SQL predicate used to merge legacy checksum/lowercase EVM rows."""
    return "lower(token) = ?" if _is_evm_chain(chain) else "token = ?"


def _chain_match_sql(chain: str) -> tuple[str, tuple[str, ...]]:
    aliases = storage_chain_aliases(chain)
    if not aliases:
        raise ValueError(f"unsupported chain identity: {chain!r}")
    placeholders = ",".join("?" for _ in aliases)
    return f"lower(chain) IN ({placeholders})", aliases


# --------------------------------------------------------------------------
# Pure logic — concentration metrics (unit-tested, no I/O)
# --------------------------------------------------------------------------

def top_n_pct(balances: list[float], n: int = 10) -> float:
    """Share of total supply held by the largest `n` holders (0-100)."""
    total = sum(balances)
    if total <= 0:
        return 0.0
    top = sorted(balances, reverse=True)[:n]
    return round(sum(top) / total * 100, 4)


def gini(balances: list[float]) -> float:
    """Gini coefficient of the balance distribution (0 = equal, 1 = concentrated).

    Returns 0.0 for empty/degenerate input.
    """
    vals = sorted(b for b in balances if b > 0)
    n = len(vals)
    if n == 0:
        return 0.0
    total = sum(vals)
    if total <= 0:
        return 0.0
    # Gini via the ordered-cumulative formula.
    cum = 0.0
    for i, v in enumerate(vals, start=1):
        cum += i * v
    return round((2 * cum) / (n * total) - (n + 1) / n, 4)


def concentration_metrics(holders: list[dict[str, Any]]) -> dict[str, Any]:
    """Compute nominal concentration metrics from a holder list.

    Each holder dict must have an "address" and a numeric "balance".
    """
    balances = [float(h.get("balance", 0) or 0) for h in holders]
    return {
        "holder_count": len([b for b in balances if b > 0]),
        "top10_pct": top_n_pct(balances, 10),
        "top25_pct": top_n_pct(balances, 25),
        "gini": gini(balances),
        "total_supply_observed": round(sum(balances), 6),
    }


def _valid_holder_payload(holders: object) -> bool:
    """Semantic gate shared by persistence, freshness, and history reads."""
    if not isinstance(holders, list) or not holders:
        return False
    saw_positive = False
    for holder in holders:
        if not isinstance(holder, dict):
            return False
        address = holder.get("address")
        if not isinstance(address, str) or not address.strip():
            return False
        raw_balance = holder.get("balance")
        if isinstance(raw_balance, bool):
            return False
        try:
            balance = float(raw_balance)
        except (TypeError, ValueError):
            return False
        if not math.isfinite(balance) or balance < 0:
            return False
        saw_positive = saw_positive or balance > 0
    return saw_positive


# --------------------------------------------------------------------------
# Storage
# --------------------------------------------------------------------------

def _connect(db_path: Path = DB_PATH) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=10000")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS token_birth (
            token TEXT NOT NULL,
            chain TEXT NOT NULL,
            first_seen_at TEXT NOT NULL,
            source TEXT,
            PRIMARY KEY (token, chain)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS holder_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            token TEXT NOT NULL,
            chain TEXT NOT NULL,
            snapshot_at TEXT NOT NULL,
            holder_count INTEGER,
            top10_pct REAL,
            top25_pct REAL,
            gini REAL,
            total_supply_observed REAL,
            holders_json TEXT
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_snap_token "
        "ON holder_snapshots(token, chain, snapshot_at)"
    )
    return conn


def record_token_birth(
    token: str, chain: str, source: str = "", db_path: Path = DB_PATH
) -> None:
    """Record the first time a token enters the universe (idempotent)."""
    chain = _canonical_chain(chain)
    token = _canonical_token(token, chain)
    conn = _connect(db_path)
    try:
        conn.execute(
            "INSERT OR IGNORE INTO token_birth (token, chain, first_seen_at, source) "
            "VALUES (?, ?, ?, ?)",
            (token, chain, datetime.now(timezone.utc).isoformat(), source),
        )
        conn.commit()
    finally:
        conn.close()


def save_snapshot(
    token: str,
    chain: str,
    holders: list[dict[str, Any]],
    snapshot_at: str | None = None,
    db_path: Path = DB_PATH,
) -> dict[str, Any]:
    """Persist a holder snapshot and return the computed metrics."""
    if not _valid_holder_payload(holders):
        raise ValueError("invalid holder snapshot payload")
    chain = _canonical_chain(chain)
    token = _canonical_token(token, chain)
    metrics = concentration_metrics(holders)
    ts = snapshot_at or datetime.now(timezone.utc).isoformat()
    snapshot_dt = _parse_iso(ts)
    if snapshot_dt is None:
        raise ValueError("invalid holder snapshot timestamp")
    if snapshot_dt > datetime.now(timezone.utc) + MAX_FUTURE_CLOCK_SKEW:
        raise ValueError("future holder snapshot timestamp")
    conn = _connect(db_path)
    try:
        conn.execute(
            """INSERT INTO holder_snapshots
               (token, chain, snapshot_at, holder_count, top10_pct, top25_pct,
                gini, total_supply_observed, holders_json)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                token, chain, ts, metrics["holder_count"], metrics["top10_pct"],
                metrics["top25_pct"], metrics["gini"],
                metrics["total_supply_observed"], json.dumps(holders, allow_nan=False),
            ),
        )
        conn.commit()
    finally:
        conn.close()
    logger.info("holder_snapshot_saved", token=token, chain=chain, **metrics)
    return {**metrics, "snapshot_at": ts}


def get_snapshots(
    token: str, chain: str, limit: int = 100, db_path: Path = DB_PATH
) -> list[dict[str, Any]]:
    """Return snapshots for a token, oldest first (time series)."""
    chain = canonical_chain(chain)
    if chain is None:
        return []
    token = _canonical_token(token, chain)
    chain_match, chain_aliases = _chain_match_sql(chain)
    conn = _connect(db_path)
    try:
        rows = conn.execute(
            """SELECT snapshot_at, holder_count, top10_pct, top25_pct, gini,
                      total_supply_observed, holders_json FROM (
                   SELECT snapshot_at, holder_count, top10_pct, top25_pct, gini,
                          total_supply_observed, holders_json
                   FROM holder_snapshots WHERE """ + _token_match_sql(chain)
                   + """ AND """ + chain_match + """
                   ORDER BY snapshot_at DESC LIMIT ?
               ) ORDER BY snapshot_at ASC""",
            (token, *chain_aliases, limit),
        ).fetchall()
    finally:
        conn.close()
    cols = ["snapshot_at", "holder_count", "top10_pct", "top25_pct", "gini",
            "total_supply_observed"]
    now = datetime.now(timezone.utc)
    for row in rows:
        observed_at = _parse_iso(row[0])
        valid_state, _signature = _decode_holder_state(row[6], chain)
        if (
            observed_at is None
            or observed_at > now + MAX_FUTURE_CLOCK_SKEW
            or not valid_state
        ):
            logger.warning(
                "holder_metrics_history_invalid",
                token=token,
                chain=chain,
                snapshot_at=row[0],
            )
            return []
    return [dict(zip(cols, row[:6])) for row in rows]


# --------------------------------------------------------------------------
# Freshness guard — catch a holder-data source that has silently STALLED (stops
# producing new rows). A successful fetch can legitimately return an unchanged
# holder set for an inactive token, so source health and dynamic-signal eligibility
# are deliberately separate verdicts:
#
# * a recent identical row proves the fetch path is alive (`source_healthy=True`);
# * without block/etag provenance it does NOT prove a new on-chain observation, so
#   it cannot be another point in an accumulation slope
#   (`dynamic_evidence_eligible=False`).
#
# Pure reads + clock injection keep the distinction unit-testable.
# --------------------------------------------------------------------------

# Opportunistic accumulation candidates normally refresh many times per day; 18h
# without any observation is stale for that path. The tracked health universe has
# a separate daily cadence at 06:30 plus six hours of scheduler/provider grace.
STALE_AFTER_H = 18.0
DAILY_SNAPSHOT_CADENCE_H = 24.0
DAILY_STALE_GRACE_H = 6.0
DAILY_STALE_AFTER_H = DAILY_SNAPSHOT_CADENCE_H + DAILY_STALE_GRACE_H
# Allow only minor host/provider clock skew. A materially future observation can
# manufacture recency and must never become dynamic evidence.
MAX_FUTURE_CLOCK_SKEW = timedelta(minutes=5)
# N consecutive semantically identical holder rows = an unchanged on-chain state. The
# legacy name remains part of the function signature for compatibility.
FROZEN_RUNS = 3


def _parse_iso(ts: str) -> datetime | None:
    try:
        d = datetime.fromisoformat(ts)
        return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _holder_state_signature(
    holders: list[dict[str, Any]], chain: str,
) -> tuple[tuple[str, float], ...]:
    """Order-independent identity for one observed holder state.

    Providers may reorder an otherwise identical ranked response, and EVM
    checksum casing is presentation-only.  Neither difference may manufacture a
    new time-series point.  Solana addresses remain exact-case.
    """
    state: dict[str, float] = {}
    for holder in holders or []:
        address = str(holder.get("address") or "").strip()
        if _is_evm_chain(chain):
            address = address.lower()
        try:
            balance = float(holder.get("balance", 0) or 0)
        except (TypeError, ValueError):
            balance = 0.0
        state[address] = state.get(address, 0.0) + balance
    return tuple(sorted(state.items()))


def _decode_holder_state(
    raw: str | None, chain: str,
) -> tuple[bool, tuple[tuple[str, float], ...]]:
    """Decode a stored state without turning corruption into an empty snapshot."""
    try:
        holders = json.loads(raw) if raw is not None else None
    except (json.JSONDecodeError, TypeError):
        return False, ()
    if not _valid_holder_payload(holders):
        return False, ()
    return True, _holder_state_signature(holders, chain)


def holder_state_changed(
    previous: list[dict[str, Any]], current: list[dict[str, Any]], chain: str,
) -> bool:
    """Whether two holder responses contain independently changed state."""
    if not _valid_holder_payload(previous) or not _valid_holder_payload(current):
        return False
    return (
        _holder_state_signature(previous, chain)
        != _holder_state_signature(current, chain)
    )


def deduplicate_holder_history(
    history: list[tuple[str, list[dict[str, Any]]]], chain: str,
) -> list[tuple[str, list[dict[str, Any]]]]:
    """Drop consecutive cached/static copies from a dynamic time series."""
    if any(not _valid_holder_payload(holders) for _ts, holders in history):
        return []
    out: list[tuple[str, list[dict[str, Any]]]] = []
    previous: tuple[tuple[str, float], ...] | None = None
    for timestamp, holders in history:
        signature = _holder_state_signature(holders, chain)
        if previous is None or signature != previous:
            out.append((timestamp, holders))
        previous = signature
    return out


def snapshot_freshness(
    token: str,
    chain: str,
    *,
    stale_after_h: float = STALE_AFTER_H,
    frozen_runs: int = FROZEN_RUNS,
    now: datetime | None = None,
    db_path: Path = DB_PATH,
) -> dict[str, Any]:
    """Verdict on whether a token's holder snapshots can be trusted as CURRENT.

    `stale` / `source_healthy` describe the fetch path.  `currentness` /
    `dynamic_evidence_eligible` answer the stricter question needed by a slope
    signal: did the newest response contain changed state?  A recent static row is
    healthy source evidence but dynamically ineligible because this schema has no
    provider block/etag provenance. Never raises.
    """
    now = now or datetime.now(timezone.utc)
    chain = canonical_chain(chain)
    if chain is None:
        return {
            "stale": True, "source_healthy": False,
            "reason": "unsupported_chain", "currentness": "unknown_unsupported_chain",
            "dynamic_evidence_eligible": False, "age_hours": None,
            "latest": None, "identical_run": 0, "n": 0,
        }
    token = _canonical_token(token, chain)
    chain_match, chain_aliases = _chain_match_sql(chain)
    conn = _connect(db_path)
    try:
        rows = conn.execute(
            """SELECT snapshot_at, holder_count, top10_pct, top25_pct, gini,
                      total_supply_observed, holders_json
               FROM holder_snapshots WHERE """ + _token_match_sql(chain) + """ AND """
               + chain_match + """
               ORDER BY snapshot_at DESC LIMIT ?""",
            (token, *chain_aliases, max(frozen_runs, 2)),
        ).fetchall()
    finally:
        conn.close()

    if not rows:
        return {
            "stale": True, "source_healthy": False,
            "reason": "no_snapshots", "currentness": "unknown_no_snapshot",
            "dynamic_evidence_eligible": False, "age_hours": None,
            "latest": None, "identical_run": 0, "n": 0,
        }

    latest_ts = rows[0][0]
    latest_dt = _parse_iso(latest_ts)
    if latest_dt is None:
        return {
            "stale": True, "source_healthy": False,
            "reason": "invalid_snapshot_timestamp",
            "currentness": "unknown_invalid_timestamp",
            "dynamic_evidence_eligible": False, "age_hours": None,
            "latest": latest_ts, "identical_run": 0, "n": len(rows),
        }
    if latest_dt > now + MAX_FUTURE_CLOCK_SKEW:
        return {
            "stale": True, "source_healthy": False,
            "reason": "future_snapshot_timestamp",
            "currentness": "unknown_future_timestamp",
            "dynamic_evidence_eligible": False, "age_hours": None,
            "latest": latest_ts, "identical_run": 0, "n": len(rows),
        }
    age_h = (now - latest_dt).total_seconds() / 3600.0

    # Count the newest run of semantically identical holder states. Exact cached
    # responses stay identical even if a provider changes array ordering/casing.
    decoded_states = [_decode_holder_state(row[6], chain) for row in rows]
    latest_valid, sig0 = decoded_states[0]
    if not latest_valid:
        return {
            "stale": True, "source_healthy": False,
            "reason": "invalid_snapshot", "currentness": "unknown_invalid_snapshot",
            "dynamic_evidence_eligible": False,
            "age_hours": round(age_h, 1) if age_h is not None else None,
            "latest": latest_ts, "identical_run": 0, "n": len(rows),
        }

    identical_run = 1
    for valid, signature in decoded_states[1:]:
        if not valid:
            break
        if signature == sig0:
            identical_run += 1
        else:
            break

    if age_h is not None and age_h > stale_after_h:
        return {
            "stale": True, "source_healthy": False,
            "reason": "stalled", "currentness": "unknown_stalled",
            "dynamic_evidence_eligible": False, "age_hours": round(age_h, 1),
            "latest": latest_ts, "identical_run": identical_run, "n": len(rows),
        }
    previous_valid = len(decoded_states) >= 2 and decoded_states[1][0]
    dynamic_eligible = previous_valid and identical_run == 1
    currentness = (
        "observed_change" if dynamic_eligible
        else "unknown_static" if identical_run >= 2
        else "unknown_previous_snapshot" if len(rows) >= 2
        else "unknown_single_snapshot"
    )
    if identical_run >= frozen_runs:
        return {
            "stale": False, "source_healthy": True,
            "reason": "static", "currentness": currentness,
            "dynamic_evidence_eligible": dynamic_eligible,
            "age_hours": round(age_h, 1) if age_h is not None else None,
            "latest": latest_ts, "identical_run": identical_run, "n": len(rows),
        }
    return {
        "stale": False, "source_healthy": True,
        "reason": "fresh", "currentness": currentness,
        "dynamic_evidence_eligible": dynamic_eligible,
        "age_hours": round(age_h, 1) if age_h is not None else None,
        "latest": latest_ts, "identical_run": identical_run, "n": len(rows),
    }


# A token snapshotted only a handful of times is a one-shot screener candidate
# that is SUPPOSED to go dormant — flagging it as "stale" is noise. Only a token
# that was on a real cadence (>= this many snapshots) and then stopped is a
# genuine feed regression worth surfacing.
TRACKED_MIN_SNAPSHOTS = 4


def find_stale_snapshots(
    *, tokens: "list[str] | None" = None,
    stale_after_h: float = DAILY_STALE_AFTER_H, frozen_runs: int = FROZEN_RUNS,
    min_snapshots: int = TRACKED_MIN_SNAPSHOTS,
    now: datetime | None = None, db_path: Path = DB_PATH,
) -> list[dict[str, Any]]:
    """Return stalled/frozen feeds worth surfacing (for health).

    If `tokens` is given (the set we are ACTIVELY watching — operator sentinels +
    live watchlist), only those are checked: a stalled feed for a token we still
    care about (SIREN) is a real regression, while a screener candidate that went
    dormant 12 days ago is not. Without an allowlist, falls back to tokens with
    >= min_snapshots total rows (had a cadence, then stopped).
    """
    conn = _connect(db_path)
    try:
        all_pairs = conn.execute(
            "SELECT DISTINCT token, chain FROM holder_snapshots"
        ).fetchall() if tokens is not None else conn.execute(
            "SELECT token, chain FROM holder_snapshots "
            "GROUP BY token, chain HAVING COUNT(*) >= ?", (min_snapshots,),
        ).fetchall()
    finally:
        conn.close()
    # Collapse legacy checksum/lowercase aliases for EVM only. Solana mints remain
    # exact-case identifiers and must never be lowercased or case-folded.
    pairs: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for token, chain in all_pairs:
        chain = canonical_chain(chain)
        if chain is None:
            logger.warning(
                "holder_snapshot_unsupported_stored_chain",
                token=token,
            )
            continue
        pair = (_canonical_token(token, chain), chain)
        if pair not in seen:
            seen.add(pair)
            pairs.append(pair)
    if tokens is not None:
        want_exact = {str(t) for t in tokens}
        want_evm = {t.lower() for t in want_exact}
        rows = [
            (t, c) for t, c in pairs
            if (t.lower() in want_evm if _is_evm_chain(c) else t in want_exact)
        ]
    else:
        rows = pairs
    out = []
    for token, chain in rows:
        v = snapshot_freshness(token, chain, stale_after_h=stale_after_h,
                               frozen_runs=frozen_runs, now=now, db_path=db_path)
        if v["stale"]:
            out.append({"token": token, "chain": chain, **v})
    return out


def list_tokens(db_path: Path = DB_PATH) -> list[tuple[str, str]]:
    """Return [(token, chain)] for every token with at least one snapshot."""
    conn = _connect(db_path)
    try:
        rows = conn.execute(
            "SELECT DISTINCT token, chain FROM holder_snapshots"
        ).fetchall()
    finally:
        conn.close()
    # Preserve first-seen order while merging aliases. Unsupported legacy rows
    # cannot participate in cross-chain evidence and are omitted fail-closed.
    out: list[tuple[str, str]] = []
    for token, chain in rows:
        canonical = canonical_chain(chain)
        if canonical is None:
            continue
        pair = (_canonical_token(token, canonical), canonical)
        if pair not in out:
            out.append(pair)
    return out


def get_holders_history(
    token: str, chain: str, limit: int = 100, since: str | None = None,
    db_path: Path = DB_PATH,
) -> list[tuple[str, list[dict[str, Any]]]]:
    """Return [(snapshot_at, holders)] oldest-first, for recomputing series.

    `since` (ISO timestamp) bounds the window so a *realtime* signal is not
    polluted by stale snapshots from months ago — the same class of bug that
    let a 3-month-old item leak into the 2h highlight. Pass it to keep the
    accumulation slope reflecting recent activity only.
    """
    requested_chain = chain
    chain = canonical_chain(chain)
    if chain is None:
        logger.warning("holder_history_unsupported_chain", chain=requested_chain)
        return []
    token = _canonical_token(token, chain)
    match = _token_match_sql(chain)
    chain_match, chain_aliases = _chain_match_sql(chain)
    conn = _connect(db_path)
    try:
        if since is not None:
            rows = conn.execute(
                """SELECT snapshot_at, holders_json FROM (
                       SELECT snapshot_at, holders_json FROM holder_snapshots
                       WHERE """ + match + """ AND """ + chain_match
                       + """ AND snapshot_at >= ?
                       ORDER BY snapshot_at DESC LIMIT ?
                   ) ORDER BY snapshot_at ASC""",
                (token, *chain_aliases, since, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                """SELECT snapshot_at, holders_json FROM (
                       SELECT snapshot_at, holders_json FROM holder_snapshots
                       WHERE """ + match + """ AND """ + chain_match + """
                       ORDER BY snapshot_at DESC LIMIT ?
                   ) ORDER BY snapshot_at ASC""",
                (token, *chain_aliases, limit),
            ).fetchall()
    finally:
        conn.close()
    out: list[tuple[str, list[dict[str, Any]]]] = []
    now = datetime.now(timezone.utc)
    for ts, hj in rows:
        observed_at = _parse_iso(ts)
        if observed_at is None or observed_at > now + MAX_FUTURE_CLOCK_SKEW:
            logger.warning(
                "holder_history_invalid_timestamp",
                token=token,
                chain=chain,
                snapshot_at=ts,
            )
            return []
        try:
            holders = json.loads(hj) if hj is not None else None
        except (json.JSONDecodeError, TypeError):
            holders = None
        if not _valid_holder_payload(holders):
            logger.warning(
                "holder_history_invalid_json",
                token=token,
                chain=chain,
                snapshot_at=ts,
            )
            return []
        out.append((ts, holders))
    return out


# --------------------------------------------------------------------------
# I/O — chain fetchers (best-effort, free-tier)
# --------------------------------------------------------------------------

def _rpc(url: str, method: str, params: list, timeout: int = 15) -> dict:
    payload = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
    req = urllib.request.Request(
        url, data=payload.encode(), headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


def _rpc_das(url: str, method: str, params: dict, timeout: int = 20) -> dict:
    """Helius DAS RPC (params is an object, not a list)."""
    payload = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
    req = urllib.request.Request(
        url, data=payload.encode(), headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


def fetch_holders_solana(mint: str, max_pages: int = 10, timeout: int = 20) -> list[dict[str, Any]]:
    """Fetch the FULL Solana holder set via Helius DAS `getTokenAccounts`.

    Paginates all token accounts (1000/page) and aggregates by OWNER (one owner
    may hold many token accounts). Raw amounts are fine for concentration since
    every account shares the mint's decimals. Caps at `max_pages` (~10k accounts)
    — beyond that is dust that doesn't move concentration.

    Falls back to the top-20 `getTokenLargestAccounts` only if Helius is absent.
    Best-effort: returns [] on total failure.
    """
    rpc = os.environ.get("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")
    has_helius = "helius" in rpc

    if has_helius:
        owner_bal: dict[str, float] = {}
        try:
            for p in range(1, max_pages + 1):
                data = _rpc_das(
                    rpc, "getTokenAccounts",
                    {"mint": mint, "page": p, "limit": 1000}, timeout,
                )
                accts = data.get("result", {}).get("token_accounts", [])
                if not accts:
                    break
                for a in accts:
                    owner = a.get("owner")
                    amt = float(a.get("amount") or 0)
                    if owner and amt > 0:
                        owner_bal[owner] = owner_bal.get(owner, 0.0) + amt
                if len(accts) < 1000:
                    break
            if owner_bal:
                return [{"address": o, "balance": b} for o, b in owner_bal.items()]
        except Exception as e:
            logger.warning("solana_das_fetch_failed", mint=mint, error=str(e))
            # fall through to the largest-accounts fallback

    # Fallback: top-20 token accounts (coarse, but better than nothing).
    try:
        data = _rpc(rpc, "getTokenLargestAccounts", [mint], timeout)
        accounts = data.get("result", {}).get("value", [])
    except Exception as e:
        logger.warning("solana_holders_fetch_failed", mint=mint, error=str(e))
        return []
    holders: list[dict[str, Any]] = []
    for acc in accounts:
        amount = float(acc.get("uiAmount") or 0)
        if amount > 0:
            holders.append({"address": acc.get("address", ""), "balance": amount})
    return holders


# chain → Alchemy network subdomain
_ALCHEMY_NET = {
    1: "eth-mainnet", 8453: "base-mainnet", 42161: "arb-mainnet",
    10: "opt-mainnet", 137: "polygon-mainnet", 56: "bnb-mainnet",
}


_MORALIS_CHAINS = {
    EVM_CHAIN_IDS[name]: slug for name, slug in MORALIS_CHAIN_SLUGS.items()
}
_BROWSER_UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
               "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")


def fetch_holders_moralis(
    token: str, chain_id: int, max_pages: int = 5, timeout: int = 25
) -> list[dict[str, Any]]:
    """Token holders via Moralis (free tier) — the ranked owner list with balances.
    This is the path for BSC and other EVM chains where Alchemy/Etherscan free
    don't work. Cloudflare blocks the default urllib UA (1010), so a browser UA is
    required. Paginates by cursor; max_pages*100 holders."""
    from src.onchain import moralis_client
    mchain = _MORALIS_CHAINS.get(chain_id)
    if not moralis_client.available() or not mchain:
        return []
    holders: list[dict[str, Any]] = []
    cursor = None
    zero = "0x0000000000000000000000000000000000000000"
    for _ in range(max_pages):
        path = f"erc20/{token}/owners?chain={mchain}&order=DESC&limit=100"
        if cursor:
            path += f"&cursor={cursor}"
        data = moralis_client.get(path, timeout)
        if not data:
            break
        for r in data.get("result", []):
            addr = (r.get("owner_address") or "").lower()
            try:
                bal = float(r.get("balance_formatted") or 0)
            except (ValueError, TypeError):
                bal = 0.0
            if addr and addr != zero and bal > 0:
                holders.append({"address": addr, "balance": round(bal, 8)})
        cursor = data.get("cursor")
        if not cursor:
            break
    return holders


def _verify_onchain(token: str, chain_id: int, holders: list[dict],
                    top_n: int = 30) -> list[dict]:
    """Replace reconstructed balances with the on-chain truth for the head of the list.

    A transfer-netted balance is only as current as the last transfer you paged. When
    the walk is truncated, the head of the list is exactly where the error is largest
    and where every concentration verdict is decided. `balance_of` returns None on an
    RPC failure and 0.0 for a genuinely empty wallet — those are different answers, and
    a wallet we could not read is DROPPED (unknown), never kept at its stale value.

    Returns [] when nothing could be verified: no data beats wrong data.
    """
    chain = {1: "ethereum", 56: "bsc", 8453: "base", 42161: "arbitrum"}.get(chain_id)
    if not chain:
        return holders
    try:
        from src.onchain.evm_archive import ArchiveRPC
        rpc = ArchiveRPC(chain)
    except Exception:
        return []
    out, checked, failed = [], 0, 0
    for h in holders[:top_n]:
        b = rpc.balance_of(token, h["address"])
        if b is None:
            failed += 1
            continue                       # unreadable → unknown → excluded
        checked += 1
        if b > 1e-9:
            out.append({"address": h["address"], "balance": round(b, 8)})
    if not checked:
        return []
    logger.info("holders_onchain_verified", token=token, checked=checked,
                failed=failed, kept=len(out), dropped=checked - len(out))
    out.sort(key=lambda h: -h["balance"])
    return out


def fetch_holders_evm(
    token: str, chain_id: int = 1, max_pages: int = 25, timeout: int = 25
) -> list[dict[str, Any]]:
    """Reconstruct EVM holder balances from FULL transfer history via Alchemy.

    Paginates `alchemy_getAssetTransfers` (all ERC-20 transfers of the token,
    1000/page, value already decimal-adjusted) and nets per address — the real
    holder set, not just recent transactors. Caps at `max_pages` for very large
    tokens (logged when truncated). Falls back to Etherscan recent-window if no
    Alchemy key. Best-effort: returns [] on total failure.

    Non-ETH chains (BSC etc.) try Moralis first — Alchemy/Etherscan free don't
    cover them.
    """
    # Moralis returns OWNER BALANCES directly — the truth, no reconstruction.
    #
    # ETH used to be excluded here (`chain_id != 1`), which forced ethereum onto the
    # Alchemy transfer-netting path below. That path returns the token's EARLIEST
    # balances when it truncates, and it truncates on any busy token: WOO's top
    # "holder" showed 50% of supply while balanceOf said 0, and a cluster_confidence
    # of 56 was built on the ghost. ETH was the ONLY chain exposed, because it was
    # the only chain denied the direct source.
    #
    # Verified 2026-07-10 on WOO: Moralis ETH returns 299 holders, top-5 matching
    # balanceOf exactly, and agreeing with GoPlus's independent list.
    from src.onchain import moralis_client
    if moralis_client.usable():
        m = fetch_holders_moralis(token, chain_id, max_pages=min(max_pages, 5), timeout=timeout)
        if m:
            return m

    # Covalent/GoldRush is the keyed FREE fallback when Moralis is parked/unavailable.
    from src.onchain import covalent_client
    if covalent_client.available():
        c = covalent_client.fetch_holders(token, chain_id, max_pages=min(max_pages, 3), timeout=timeout)
        if c:
            return c

    key = os.environ.get("ALCHEMY_API_KEY", "")
    net = _ALCHEMY_NET.get(chain_id)
    if key and net:
        balances: dict[str, float] = {}
        page_key = None
        truncated = False
        try:
            for p in range(max_pages):
                params = {
                    "fromBlock": "0x0", "toBlock": "latest",
                    "contractAddresses": [token], "category": ["erc20"],
                    "withMetadata": False, "maxCount": "0x3e8", "order": "asc",
                }
                if page_key:
                    params["pageKey"] = page_key
                data = _rpc_das(
                    f"https://{net}.g.alchemy.com/v2/{key}",
                    "alchemy_getAssetTransfers", [params], timeout,
                )
                result = data.get("result", {})
                for t in result.get("transfers", []):
                    try:
                        amount = float(t.get("value") or 0)
                    except (ValueError, TypeError):
                        continue
                    frm = (t.get("from") or "").lower()
                    to = (t.get("to") or "").lower()
                    if frm:
                        balances[frm] = balances.get(frm, 0.0) - amount
                    if to:
                        balances[to] = balances.get(to, 0.0) + amount
                page_key = result.get("pageKey")
                if not page_key:
                    break
            else:
                truncated = True
            if truncated:
                logger.warning("evm_holders_truncated", token=token, max_pages=max_pages,
                               note="只累加了最早的转账 → 余额是历史值,必须链上核实")
            zero = "0x0000000000000000000000000000000000000000"
            holders = [
                {"address": a, "balance": round(b, 8)}
                for a, b in balances.items() if b > 1e-9 and a != zero
            ]
            holders.sort(key=lambda h: -h["balance"])
            if truncated and holders:
                # A TRUNCATED reconstruction nets only the token's EARLIEST transfers,
                # so it reports balances from years ago and returns them as "current
                # holders". WOO's top wallet showed 1.49B tokens (50% of supply) while
                # balanceOf said 0 — and effective_concentration_signal built a
                # `cluster_confidence 56` on that. Every ETH concentration verdict was
                # exposed to this. Verify the head of the list on-chain, or refuse.
                holders = _verify_onchain(token, chain_id, holders, top_n=30)
                if not holders:
                    logger.warning("evm_holders_unverifiable", token=token,
                                   note="截断且链上核实失败 → 返回空(不可判),绝不返回历史余额")
                    return []
            if holders:
                return holders
        except Exception as e:
            logger.warning("evm_alchemy_fetch_failed", token=token, error=str(e))
            # fall through to Etherscan

    es = _fetch_holders_evm_etherscan(token, chain_id, timeout)
    if es:
        return es

    # LAST fallback: Dune full-history net-flow reconstruction. During the 2026-07
    # audit Alchemy 403'd and Moralis+Covalent went empty at the same time — this
    # was the only path standing (and cross-checked exact vs balance_of). Slow
    # (~1-3 min) but it means holder data can no longer go completely blind.
    try:
        from src.onchain.dune_client import reconstruct_holders
        from src.onchain.evm_archive import ArchiveRPC
        chain_name = {56: "bsc", 8453: "base", 1: "ethereum"}.get(chain_id)
        dec = ArchiveRPC(chain_name).token_decimals(token) if chain_name else 18
        d = reconstruct_holders(token, chain_id, decimals=dec)
        if d:
            logger.info("holders_via_dune_reconstruction", token=token, count=len(d))
        return d
    except Exception as e:
        logger.debug("dune_holder_fallback_failed", token=token, error=str(e)[:80])
        return []


def _fetch_holders_evm_etherscan(token: str, chain_id: int, timeout: int = 20) -> list[dict[str, Any]]:
    """Fallback: approximate balances from a recent Etherscan tokentx window."""
    key = os.environ.get("ETHERSCAN_API_KEY", "")
    if not key:
        return []
    url = (
        f"https://api.etherscan.io/v2/api?chainid={chain_id}&module=account"
        f"&action=tokentx&contractaddress={token}&page=1&offset=10000"
        f"&sort=desc&apikey={key}"
    )
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "CryptoScope/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode())
        if data.get("status") != "1":
            return []
        txs = data.get("result", [])
    except Exception as e:
        logger.warning("evm_holders_fetch_failed", token=token, error=str(e))
        return []

    balances: dict[str, float] = {}
    decimals: int | None = None
    for tx in txs:
        if decimals is None:
            try:
                decimals = int(tx.get("tokenDecimal", 18))
            except (ValueError, TypeError):
                decimals = 18
        try:
            raw = float(tx.get("value", 0))
        except (ValueError, TypeError):
            continue
        amount = raw / (10 ** (decimals or 18))
        frm = (tx.get("from") or "").lower()
        to = (tx.get("to") or "").lower()
        if frm:
            balances[frm] = balances.get(frm, 0.0) - amount
        if to:
            balances[to] = balances.get(to, 0.0) + amount

    zero = "0x0000000000000000000000000000000000000000"
    return [
        {"address": a, "balance": round(b, 8)}
        for a, b in balances.items()
        if b > 0 and a != zero
    ]


def snapshot_token(
    token: str, chain: str, source: str = "", db_path: Path = DB_PATH
) -> dict[str, Any] | None:
    """End-to-end: record birth, fetch holders for the chain, save snapshot.

    Returns the snapshot metrics, or None if no holders could be fetched.
    """
    requested_chain = chain
    chain = canonical_chain(chain)
    if chain is None:
        logger.warning(
            "holder_snapshot_unsupported_chain",
            token=token,
            chain=requested_chain,
            note="unknown chain is never routed to Ethereum",
        )
        return None
    if chain == "solana":
        holders = fetch_holders_solana(token)
    else:
        chain_id = evm_chain_id(chain)
        if chain_id is None:
            logger.warning(
                "holder_snapshot_unsupported_chain",
                token=token,
                chain=chain,
                note="unknown chain is never routed to Ethereum",
            )
            return None
        holders = fetch_holders_evm(token, chain_id)

    # Unsupported/typo chains return above and never create even a birth row.
    record_token_birth(token, chain, source, db_path)

    if not holders:
        # Fetch produced nothing → no new row. That silently AGES the latest
        # snapshot; if it is already past the stall threshold, shout (this is the
        # exact SIREN failure: feed died, latest froze, stale data read as truth).
        fresh = snapshot_freshness(token, chain, db_path=db_path)
        if fresh["stale"] and fresh["reason"] in ("stalled", "no_snapshots"):
            logger.warning("holder_snapshot_stalled", token=token, chain=chain,
                           age_hours=fresh["age_hours"], latest=fresh["latest"],
                           note="holder feed returned no data and the latest snapshot is stale — do NOT trust it")
        else:
            logger.info("snapshot_skipped_no_holders", token=token, chain=chain)
        return None
    result = save_snapshot(token, chain, holders, db_path=db_path)
    # An inactive token may return byte-identical live state for days. Surface that
    # fact without claiming provider failure; only a missing/aged observation is a
    # freshness failure. Proving a cache freeze requires independent block/provenance
    # evidence, not equality of holder metrics alone.
    fresh = snapshot_freshness(token, chain, db_path=db_path)
    if fresh["reason"] == "static":
        logger.info("holder_snapshot_static", token=token, chain=chain,
                    identical_run=fresh["identical_run"],
                    note="fresh fetch succeeded; unchanged holder metrics are not a source failure")
    return result
