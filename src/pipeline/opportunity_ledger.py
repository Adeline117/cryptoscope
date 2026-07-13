"""Immutable-ish ledger for the five asymmetric-opportunity lanes.

A candidate is not a trade.  The ledger records what the system knew *when it
first saw it*, the executable plan it produced, and its later outcome.  This is
the shared spine for Launch, Cascade, Structure, Airdrop, and Carry: without it
the board is only a stream of hindsight-friendly rows.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timezone

from src.config import DATA_DIR

DB = DATA_DIR / "opportunity_ledger.db"
LANES = {"launch", "cascade", "structure", "airdrop", "carry"}


def _conn() -> sqlite3.Connection:
    DB.parent.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(str(DB), timeout=10)
    c.execute("""CREATE TABLE IF NOT EXISTS opportunities(
        id TEXT PRIMARY KEY, lane TEXT NOT NULL, chain TEXT, token TEXT,
        symbol TEXT, detected_at TEXT NOT NULL, event_at TEXT, source TEXT,
        state TEXT NOT NULL, decision TEXT NOT NULL, entry_price REAL,
        invalidation_price REAL, max_notional_usd REAL, payload TEXT NOT NULL,
        outcome_state TEXT NOT NULL DEFAULT 'open', outcome TEXT,
        updated_at TEXT NOT NULL
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_opportunities_lane_open "
              "ON opportunities(lane, outcome_state, detected_at DESC)")
    return c


def event_id(lane: str, chain: str | None, token: str | None) -> str:
    """Stable identity: one initial thesis per token per lane, never a poll row."""
    if lane not in LANES:
        raise ValueError(f"unknown lane: {lane}")
    raw = f"{lane}:{chain or '?'}:{(token or '?').lower()}"
    return hashlib.sha256(raw.encode()).hexdigest()[:24]


def record(candidate: dict) -> tuple[str, bool]:
    """Persist the first observation. Returns ``(id, inserted)``.

    Existing rows retain their original entry snapshot; only liveness/state and
    the latest enriched payload are refreshed.  This prevents repeated scans from
    quietly moving an entry forward after a token already ran.
    """
    lane = candidate["lane"]
    chain, token = candidate.get("chain"), candidate.get("token")
    # Most lanes use one initial thesis per token. Repeating-event lanes (Cascade,
    # Structure) supply an explicit event_key so a later, genuinely new episode does
    # not overwrite an old one.
    ident = candidate.get("id") or event_id(lane, chain, candidate.get("event_key") or token)
    now = datetime.now(timezone.utc).isoformat()
    payload = json.dumps(candidate, ensure_ascii=False, separators=(",", ":"))
    values = (ident, lane, chain, token, candidate.get("symbol", "?"),
              candidate.get("detected_at") or now, candidate.get("event_at"),
              candidate.get("source", "unknown"), candidate.get("state", "new"),
              candidate.get("decision", "WATCH"), candidate.get("entry_price"),
              candidate.get("invalidation_price"), candidate.get("max_notional_usd"),
              payload, now)
    c = _conn()
    try:
        inserted = c.execute("SELECT 1 FROM opportunities WHERE id=?", (ident,)).fetchone() is None
        c.execute("""INSERT INTO opportunities(
              id,lane,chain,token,symbol,detected_at,event_at,source,state,decision,
              entry_price,invalidation_price,max_notional_usd,payload,updated_at)
              VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
              ON CONFLICT(id) DO UPDATE SET
                state=excluded.state, decision=excluded.decision, payload=excluded.payload,
                updated_at=excluded.updated_at
        """, values)
        c.commit()
        return ident, inserted
    finally:
        c.close()


def active(lane: str, limit: int = 50) -> list[dict]:
    """Return live event cards with their *first-seen* price/plan intact."""
    if lane not in LANES:
        raise ValueError(f"unknown lane: {lane}")
    c = _conn()
    try:
        rows = c.execute("""SELECT id, lane, chain, token, symbol, detected_at, event_at,
                                  source, state, decision, entry_price, invalidation_price,
                                  max_notional_usd, payload
                           FROM opportunities WHERE lane=? AND outcome_state='open'
                           ORDER BY detected_at DESC LIMIT ?""", (lane, limit)).fetchall()
    finally:
        c.close()
    out = []
    for row in rows:
        keys = ("id", "lane", "chain", "token", "symbol", "detected_at", "event_at",
                "source", "state", "decision", "entry_price", "invalidation_price",
                "max_notional_usd", "payload")
        item = dict(zip(keys, row))
        try:
            payload = json.loads(item.pop("payload"))
            # The stored columns are the immutable first-observed execution plan.
            # Enrichment payload may contain newer market values, but it must never
            # rewrite the entry, invalidation, size cap, or discovery timestamp.
            for key, value in payload.items():
                if key not in {"entry_price", "invalidation_price", "max_notional_usd", "detected_at"}:
                    item[key] = value
        except (TypeError, json.JSONDecodeError):
            item.pop("payload", None)
        out.append(item)
    return out
