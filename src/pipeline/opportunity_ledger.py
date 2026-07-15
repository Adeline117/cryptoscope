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
CLOCK_FIELDS = ("event_at", "detected_at", "decision_at", "quote_at",
                "executable_at", "expires_at")


def _utc_iso(value: object, *, field: str) -> str | None:
    """Return one canonical UTC timestamp, rejecting ambiguous clock values."""
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        try:
            dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except (TypeError, ValueError) as exc:
            raise ValueError(f"invalid {field}: {value!r}") from exc
    if dt.tzinfo is None:
        raise ValueError(f"{field} must include a timezone")
    return dt.astimezone(timezone.utc).isoformat()


def _conn() -> sqlite3.Connection:
    DB.parent.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(str(DB), timeout=10)
    c.execute("PRAGMA busy_timeout=8000")
    c.execute("""CREATE TABLE IF NOT EXISTS opportunities(
        id TEXT PRIMARY KEY, lane TEXT NOT NULL, chain TEXT, token TEXT,
        symbol TEXT, detected_at TEXT NOT NULL, event_at TEXT,
        decision_at TEXT NOT NULL, quote_at TEXT, executable_at TEXT, expires_at TEXT,
        source TEXT,
        state TEXT NOT NULL, decision TEXT NOT NULL, entry_price REAL,
        invalidation_price REAL, max_notional_usd REAL, cost_pct_est REAL,
        cost_model TEXT, cohort_version INTEGER, payload TEXT NOT NULL,
        outcome_state TEXT NOT NULL DEFAULT 'open', outcome TEXT,
        updated_at TEXT NOT NULL
    )""")
    # Additive migration for ledgers created before discovery-time costs were frozen.
    cols = {r[1] for r in c.execute("PRAGMA table_info(opportunities)").fetchall()}
    if "cost_pct_est" not in cols:
        c.execute("ALTER TABLE opportunities ADD COLUMN cost_pct_est REAL")
    if "cost_model" not in cols:
        c.execute("ALTER TABLE opportunities ADD COLUMN cost_model TEXT")
    if "cohort_version" not in cols:
        # Deliberately NULL for legacy rows: their decision was mutable before v2,
        # so they must never be smuggled into the frozen WATCH-vs-PROBE comparison.
        c.execute("ALTER TABLE opportunities ADD COLUMN cohort_version INTEGER")
    # Canonical event clocks. Only decision_at can be truthfully reconstructed for
    # legacy rows: the old first-seen row proves the decision existed by detected_at.
    # Quote/executable/expiry clocks stay NULL rather than inventing precision.
    for name in ("decision_at", "quote_at", "executable_at", "expires_at"):
        if name not in cols:
            c.execute(f"ALTER TABLE opportunities ADD COLUMN {name} TEXT")
    c.execute("UPDATE opportunities SET decision_at=detected_at WHERE decision_at IS NULL")
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
    detected_at = _utc_iso(candidate.get("detected_at") or now, field="detected_at")
    clocks = {
        "event_at": _utc_iso(candidate.get("event_at"), field="event_at"),
        "decision_at": _utc_iso(candidate.get("decision_at") or detected_at,
                                field="decision_at"),
        "quote_at": _utc_iso(candidate.get("quote_at"), field="quote_at"),
        "executable_at": _utc_iso(candidate.get("executable_at"), field="executable_at"),
        "expires_at": _utc_iso(candidate.get("expires_at"), field="expires_at"),
    }
    payload = json.dumps(candidate, ensure_ascii=False, separators=(",", ":"))
    values = (ident, lane, chain, token, candidate.get("symbol", "?"),
              detected_at, clocks["event_at"], clocks["decision_at"], clocks["quote_at"],
              clocks["executable_at"], clocks["expires_at"],
              candidate.get("source", "unknown"), candidate.get("state", "new"),
              candidate.get("decision", "WATCH"), candidate.get("entry_price"),
              candidate.get("invalidation_price"), candidate.get("max_notional_usd"),
              candidate.get("roundtrip_cost_pct_est"), candidate.get("cost_model"), 2,
              payload, now)
    c = _conn()
    try:
        inserted = c.execute("SELECT 1 FROM opportunities WHERE id=?", (ident,)).fetchone() is None
        c.execute("""INSERT INTO opportunities(
              id,lane,chain,token,symbol,detected_at,event_at,decision_at,quote_at,
              executable_at,expires_at,source,state,decision,
              entry_price,invalidation_price,max_notional_usd,cost_pct_est,cost_model,
              cohort_version,payload,updated_at)
              VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
              ON CONFLICT(id) DO UPDATE SET
                state=excluded.state, payload=excluded.payload, updated_at=excluded.updated_at
        """, values)
        c.commit()
        return ident, inserted
    finally:
        c.close()


def active(lane: str, limit: int = 50, *, now: datetime | None = None) -> list[dict]:
    """Return event cards plus a fail-closed current-actionability verdict."""
    if lane not in LANES:
        raise ValueError(f"unknown lane: {lane}")
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    c = _conn()
    try:
        rows = c.execute("""SELECT id, lane, chain, token, symbol, detected_at, event_at,
                                  decision_at, quote_at, executable_at, expires_at,
                                  source, state, decision, entry_price, invalidation_price,
                                  max_notional_usd, cost_pct_est, cost_model, cohort_version,
                                  payload, outcome_state, outcome
                           FROM opportunities WHERE lane=? AND outcome_state='open'
                           ORDER BY detected_at DESC LIMIT ?""", (lane, limit)).fetchall()
    finally:
        c.close()
    out = []
    for row in rows:
        keys = ("id", "lane", "chain", "token", "symbol", "detected_at", "event_at",
                "decision_at", "quote_at", "executable_at", "expires_at",
                "source", "state", "decision", "entry_price", "invalidation_price",
                "max_notional_usd", "cost_pct_est", "cost_model", "cohort_version", "payload",
                "outcome_state", "outcome")
        item = dict(zip(keys, row))
        try:
            payload = json.loads(item.pop("payload"))
            # The stored columns are the immutable first-observed execution plan.
            # Enrichment payload may contain newer market values, but it must never
            # rewrite the entry, invalidation, size cap, or discovery timestamp.
            for key, value in payload.items():
                if key not in {"entry_price", "invalidation_price", "max_notional_usd",
                               "roundtrip_cost_pct_est", "cost_model", *CLOCK_FIELDS}:
                    item[key] = value
        except (TypeError, json.JSONDecodeError):
            item.pop("payload", None)
        if item.get("outcome"):
            try:
                item["outcome"] = json.loads(item["outcome"])
            except (TypeError, json.JSONDecodeError):
                item["outcome"] = None
        recorded_decision = item.get("decision") or "WATCH"
        effective_decision = recorded_decision
        expires_at = item.get("expires_at")
        seconds_to_expiry = None
        expired = False
        if expires_at:
            try:
                expiry = datetime.fromisoformat(expires_at).astimezone(timezone.utc)
                seconds_to_expiry = round((expiry - now).total_seconds())
                expired = seconds_to_expiry <= 0
            except (TypeError, ValueError):
                expired = True
        # A SMALL_PROBE is a quote-bounded state, never a durable label. Legacy rows
        # without both clocks are preserved for outcomes but fail closed for display.
        if recorded_decision == "SMALL_PROBE":
            if not item.get("quote_at") or not expires_at:
                effective_decision = "WATCH"
                item["actionability_reason"] = "missing quote or expiry clock"
            elif expired:
                effective_decision = "EXPIRED"
                item["actionability_reason"] = "read-only quote expired"
        elif expired and recorded_decision == "CLAIM_CHECK":
            effective_decision = "EXPIRED"
            item["actionability_reason"] = "claim window expired"
        item["recorded_decision"] = recorded_decision
        item["effective_decision"] = effective_decision
        item["actionable_now"] = effective_decision in {"SMALL_PROBE", "CLAIM_CHECK"}
        item["is_expired"] = expired
        item["seconds_to_expiry"] = seconds_to_expiry
        out.append(item)
    return out


def outcome_rows(*, open_only: bool = False) -> list[dict]:
    """Return immutable event facts plus their mutable measurement record.

    This is intentionally a separate read surface from :func:`active`: the board's
    live-event list can age out, while the validation sample must retain every trial.
    The first-seen price/plan comes from columns, never from the refreshable payload.
    """
    c = _conn()
    try:
        where = "WHERE outcome_state='open'" if open_only else ""
        rows = c.execute(f"""SELECT id,lane,chain,token,symbol,detected_at,event_at,
                                    decision_at,quote_at,executable_at,expires_at,
                                    source,state,decision,entry_price,invalidation_price,
                                    max_notional_usd,cost_pct_est,cost_model,cohort_version,payload,
                                    outcome_state,outcome,updated_at
                             FROM opportunities {where}
                             ORDER BY detected_at ASC""").fetchall()
    finally:
        c.close()
    keys = ("id", "lane", "chain", "token", "symbol", "detected_at", "event_at",
            "decision_at", "quote_at", "executable_at", "expires_at",
            "source", "state", "decision", "entry_price", "invalidation_price",
            "max_notional_usd", "cost_pct_est", "cost_model", "cohort_version", "payload",
            "outcome_state", "outcome", "updated_at")
    out = []
    for row in rows:
        item = dict(zip(keys, row))
        for key in ("payload", "outcome"):
            try:
                item[key] = json.loads(item[key]) if item.get(key) else {}
            except (TypeError, json.JSONDecodeError):
                item[key] = {}
        out.append(item)
    return out


def save_outcome(ident: str, outcome: dict, state: str = "open") -> None:
    """Update only the measurement side of an event; never its entry snapshot."""
    if state not in {"open", "resolved", "unresolvable"}:
        raise ValueError(f"unknown outcome state: {state}")
    now = datetime.now(timezone.utc).isoformat()
    c = _conn()
    try:
        c.execute("UPDATE opportunities SET outcome_state=?,outcome=?,updated_at=? WHERE id=?",
                  (state, json.dumps(outcome, ensure_ascii=False, separators=(",", ":")),
                   now, ident))
        c.commit()
    finally:
        c.close()
