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
    c.execute("PRAGMA foreign_keys=ON")
    c.execute("""CREATE TABLE IF NOT EXISTS opportunities(
        id TEXT PRIMARY KEY, lane TEXT NOT NULL, chain TEXT, token TEXT,
        symbol TEXT, detected_at TEXT NOT NULL, event_at TEXT,
        decision_at TEXT NOT NULL, quote_at TEXT, executable_at TEXT, expires_at TEXT,
        source TEXT,
        state TEXT NOT NULL, decision TEXT NOT NULL, entry_price REAL,
        invalidation_price REAL, max_notional_usd REAL, cost_pct_est REAL,
        cost_model TEXT, cost_contract_version INTEGER, cost_contract TEXT,
        cohort_version INTEGER, payload TEXT NOT NULL,
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
    if "cost_contract_version" not in cols:
        c.execute("ALTER TABLE opportunities ADD COLUMN cost_contract_version INTEGER")
    if "cost_contract" not in cols:
        c.execute("ALTER TABLE opportunities ADD COLUMN cost_contract TEXT")
    # Canonical event clocks. Only decision_at can be truthfully reconstructed for
    # legacy rows: the old first-seen row proves the decision existed by detected_at.
    # Quote/executable/expiry clocks stay NULL rather than inventing precision.
    for name in ("decision_at", "quote_at", "executable_at", "expires_at"):
        if name not in cols:
            c.execute(f"ALTER TABLE opportunities ADD COLUMN {name} TEXT")
    c.execute("UPDATE opportunities SET decision_at=detected_at WHERE decision_at IS NULL")
    c.execute("CREATE INDEX IF NOT EXISTS idx_opportunities_lane_open "
              "ON opportunities(lane, outcome_state, detected_at DESC)")
    c.execute("""CREATE TABLE IF NOT EXISTS execution_assessments(
        assessment_id TEXT PRIMARY KEY,
        opportunity_id TEXT NOT NULL,
        kind TEXT NOT NULL CHECK(kind IN ('read_only_quote','paper_fill','real_fill')),
        assessed_at TEXT NOT NULL,
        security_state TEXT NOT NULL,
        security_at TEXT,
        security_expires_at TEXT,
        route_state TEXT NOT NULL,
        quote_source TEXT,
        quote_mode TEXT,
        quote_at TEXT,
        quote_expires_at TEXT,
        expires_at TEXT,
        notional_usd REAL,
        entry_reference_price REAL,
        invalidation_reference_price REAL,
        roundtrip_back_usd REAL,
        cost_contract_version INTEGER NOT NULL,
        cost_contract TEXT NOT NULL,
        is_real_fill INTEGER NOT NULL DEFAULT 0,
        reason_code TEXT,
        payload TEXT NOT NULL,
        created_at TEXT NOT NULL,
        FOREIGN KEY(opportunity_id) REFERENCES opportunities(id)
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_execution_assessments_latest "
              "ON execution_assessments(opportunity_id,assessed_at DESC,assessment_id DESC)")
    c.execute("""CREATE TRIGGER IF NOT EXISTS execution_assessments_no_update
                 BEFORE UPDATE ON execution_assessments BEGIN
                   SELECT RAISE(ABORT,'execution assessments are append-only');
                 END""")
    c.execute("""CREATE TRIGGER IF NOT EXISTS execution_assessments_no_delete
                 BEFORE DELETE ON execution_assessments BEGIN
                   SELECT RAISE(ABORT,'execution assessments are append-only');
                 END""")
    return c


def event_id(lane: str, chain: str | None, token: str | None) -> str:
    """Stable identity: one initial thesis per token per lane, never a poll row."""
    if lane not in LANES:
        raise ValueError(f"unknown lane: {lane}")
    raw = f"{lane}:{chain or '?'}:{(token or '?').lower()}"
    return hashlib.sha256(raw.encode()).hexdigest()[:24]


def record(candidate: dict, *, refresh_existing: bool = True) -> tuple[str, bool]:
    """Persist the first observation. Returns ``(id, inserted)``.

    Existing rows retain their original entry snapshot. By default only liveness
    and the latest enrichment payload are refreshed. Primary event bridges can set
    ``refresh_existing=False`` so a later pool cannot alter first-event provenance.
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
    cost_contract = candidate.get("cost_contract")
    if cost_contract is not None:
        from src.pipeline.execution_cost import validate
        cost_contract = validate(cost_contract)
    cost_contract_json = (json.dumps(cost_contract, ensure_ascii=False,
                                     separators=(",", ":"))
                          if cost_contract is not None else None)
    payload = json.dumps(candidate, ensure_ascii=False, separators=(",", ":"))
    values = (ident, lane, chain, token, candidate.get("symbol", "?"),
              detected_at, clocks["event_at"], clocks["decision_at"], clocks["quote_at"],
              clocks["executable_at"], clocks["expires_at"],
              candidate.get("source", "unknown"), candidate.get("state", "new"),
              candidate.get("decision", "WATCH"), candidate.get("entry_price"),
              candidate.get("invalidation_price"), candidate.get("max_notional_usd"),
              candidate.get("roundtrip_cost_pct_est"), candidate.get("cost_model"),
              cost_contract.get("version") if cost_contract else None,
              cost_contract_json, candidate.get("cohort_version", 2),
              payload, now)
    c = _conn()
    try:
        inserted = c.execute("SELECT 1 FROM opportunities WHERE id=?", (ident,)).fetchone() is None
        if refresh_existing:
            c.execute("""INSERT INTO opportunities(
                  id,lane,chain,token,symbol,detected_at,event_at,decision_at,quote_at,
                  executable_at,expires_at,source,state,decision,
                  entry_price,invalidation_price,max_notional_usd,cost_pct_est,cost_model,
                  cost_contract_version,cost_contract,cohort_version,payload,updated_at)
                  VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                  ON CONFLICT(id) DO UPDATE SET
                    state=excluded.state,
                    decision=CASE WHEN opportunities.lane='carry'
                                  THEN excluded.decision ELSE opportunities.decision END,
                    max_notional_usd=CASE WHEN opportunities.lane='carry'
                                          THEN excluded.max_notional_usd
                                          ELSE opportunities.max_notional_usd END,
                    cost_pct_est=CASE WHEN opportunities.lane='carry'
                                           AND opportunities.cost_contract IS NULL
                                      THEN excluded.cost_pct_est
                                      ELSE opportunities.cost_pct_est END,
                    cost_model=CASE WHEN opportunities.lane='carry'
                                         AND opportunities.cost_contract IS NULL
                                    THEN excluded.cost_model
                                    ELSE opportunities.cost_model END,
                    cost_contract_version=CASE
                        WHEN opportunities.lane='carry'
                             AND opportunities.cost_contract_version IS NULL
                        THEN excluded.cost_contract_version
                        ELSE opportunities.cost_contract_version END,
                    cost_contract=CASE
                        WHEN opportunities.lane='carry' AND opportunities.cost_contract IS NULL
                        THEN excluded.cost_contract ELSE opportunities.cost_contract END,
                    payload=excluded.payload, updated_at=excluded.updated_at
            """, values)
        else:
            inserted = bool(c.execute("""INSERT INTO opportunities(
                  id,lane,chain,token,symbol,detected_at,event_at,decision_at,quote_at,
                  executable_at,expires_at,source,state,decision,
                  entry_price,invalidation_price,max_notional_usd,cost_pct_est,cost_model,
                  cost_contract_version,cost_contract,cohort_version,payload,updated_at)
                  VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                  ON CONFLICT(id) DO NOTHING
            """, values).rowcount)
        c.commit()
        return ident, inserted
    finally:
        c.close()


def record_if_absent(candidate: dict) -> tuple[str, bool]:
    """Atomically preserve the first event and reject provenance refreshes."""
    return record(candidate, refresh_existing=False)


def event_id_readback_matches(ident: str, *, lane: str, chain: str,
                              token: str) -> bool:
    """Require one exact ledger row before another store may claim traceability.

    Stream databases cannot enforce a foreign key into this SQLite file.  This
    narrow read-back contract prevents a stale or fabricated ``ledger_event_id``
    from being counted as a recorded opportunity merely because it is non-empty.
    """
    c = _conn()
    try:
        rows = c.execute(
            "SELECT lane,chain,token FROM opportunities WHERE id=? LIMIT 2", (ident,)
        ).fetchall()
    finally:
        c.close()
    return (len(rows) == 1
            and rows[0] == (str(lane), str(chain), str(token)))


def append_execution_assessment(ident: str, assessment: dict) -> tuple[str, bool]:
    """Append one immutable security/route measurement for an existing event."""
    kind = assessment.get("kind", "read_only_quote")
    if kind not in {"read_only_quote", "paper_fill", "real_fill"}:
        raise ValueError(f"unknown execution assessment kind: {kind}")
    configured_auto_execution = assessment.get("auto_execution_allowed")
    if configured_auto_execution is not None and configured_auto_execution is not False:
        raise ValueError("execution assessments cannot allow automatic execution")
    is_real_fill = bool(assessment.get("is_real_fill", False))
    if (kind == "real_fill") != is_real_fill:
        raise ValueError("real_fill kind and is_real_fill must agree")
    assessed_at = _utc_iso(assessment.get("assessed_at"), field="assessed_at")
    if assessed_at is None:
        raise ValueError("assessed_at is required")
    clock_names = ("security_at", "security_expires_at", "quote_at",
                   "quote_expires_at", "expires_at")
    clocks = {name: _utc_iso(assessment.get(name), field=name) for name in clock_names}
    if clocks["quote_expires_at"] is None and clocks["expires_at"] is not None:
        clocks["quote_expires_at"] = clocks["expires_at"]
    if clocks["quote_at"] and clocks["expires_at"]:
        if datetime.fromisoformat(clocks["expires_at"]) <= datetime.fromisoformat(clocks["quote_at"]):
            raise ValueError("execution assessment expiry must be after quote_at")
    from src.pipeline.execution_cost import validate
    contract = validate(assessment.get("cost_contract"))
    supplied_notional = assessment.get("notional_usd")
    if supplied_notional is not None:
        try:
            supplied_notional = float(supplied_notional)
        except (TypeError, ValueError) as exc:
            raise ValueError("assessment notional_usd must be numeric") from exc
        if abs(supplied_notional - contract["notional_usd"]) > 1e-6:
            raise ValueError("assessment notional disagrees with cost contract")
    normalized = {
        **assessment, "kind": kind, "assessed_at": assessed_at, **clocks,
        "security_state": str(assessment.get("security_state") or "unknown"),
        "route_state": str(assessment.get("route_state") or "unknown"),
        "cost_contract": contract, "is_real_fill": is_real_fill,
        "auto_execution_allowed": False,
    }
    canonical = json.dumps(normalized, ensure_ascii=False, sort_keys=True,
                           separators=(",", ":"))
    assessment_id = assessment.get("assessment_id") or hashlib.sha256(
        f"{ident}:{canonical}".encode()).hexdigest()[:32]
    created_at = datetime.now(timezone.utc).isoformat()
    values = (
        assessment_id, ident, kind, assessed_at, normalized["security_state"],
        clocks["security_at"], clocks["security_expires_at"], normalized["route_state"],
        assessment.get("quote_source"), assessment.get("quote_mode"), clocks["quote_at"],
        clocks["quote_expires_at"], clocks["expires_at"], contract["notional_usd"],
        assessment.get("entry_reference_price"),
        assessment.get("invalidation_reference_price"),
        assessment.get("roundtrip_back_usd"), contract["version"],
        json.dumps(contract, ensure_ascii=False, separators=(",", ":")),
        int(is_real_fill), assessment.get("reason_code"), canonical, created_at,
    )
    c = _conn()
    try:
        if c.execute("SELECT 1 FROM opportunities WHERE id=?", (ident,)).fetchone() is None:
            raise ValueError(f"unknown opportunity: {ident}")
        inserted = bool(c.execute("""INSERT INTO execution_assessments(
            assessment_id,opportunity_id,kind,assessed_at,security_state,security_at,
            security_expires_at,route_state,quote_source,quote_mode,quote_at,
            quote_expires_at,expires_at,notional_usd,entry_reference_price,
            invalidation_reference_price,roundtrip_back_usd,cost_contract_version,
            cost_contract,is_real_fill,reason_code,payload,created_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(assessment_id) DO NOTHING""", values).rowcount)
        c.commit()
        return assessment_id, inserted
    finally:
        c.close()


def _assessment_from_row(row: tuple | None) -> dict | None:
    if row is None:
        return None
    keys = ("assessment_id", "opportunity_id", "kind", "assessed_at",
            "security_state", "security_at", "security_expires_at", "route_state",
            "quote_source", "quote_mode", "quote_at", "quote_expires_at", "expires_at",
            "notional_usd", "entry_reference_price", "invalidation_reference_price",
            "roundtrip_back_usd", "cost_contract_version", "cost_contract",
            "is_real_fill", "reason_code", "payload", "created_at")
    item = dict(zip(keys, row))
    for name in ("cost_contract", "payload"):
        try:
            item[name] = json.loads(item[name])
        except (TypeError, json.JSONDecodeError):
            item[name] = {}
    item["is_real_fill"] = bool(item["is_real_fill"])
    return item


def latest_execution_assessment(ident: str) -> dict | None:
    """Return the newest append-only measurement; legacy rows return ``None``."""
    c = _conn()
    try:
        row = c.execute("""SELECT assessment_id,opportunity_id,kind,assessed_at,
            security_state,security_at,security_expires_at,route_state,quote_source,
            quote_mode,quote_at,quote_expires_at,expires_at,notional_usd,
            entry_reference_price,invalidation_reference_price,roundtrip_back_usd,
            cost_contract_version,cost_contract,is_real_fill,reason_code,payload,created_at
          FROM execution_assessments WHERE opportunity_id=?
          ORDER BY assessed_at DESC,assessment_id DESC LIMIT 1""", (ident,)).fetchone()
    finally:
        c.close()
    return _assessment_from_row(row)


def _latest_assessment_map(c: sqlite3.Connection, identities: list[str]) -> dict[str, dict]:
    if not identities:
        return {}
    placeholders = ",".join("?" for _ in identities)
    rows = c.execute(f"""SELECT assessment_id,opportunity_id,kind,assessed_at,
        security_state,security_at,security_expires_at,route_state,quote_source,
        quote_mode,quote_at,quote_expires_at,expires_at,notional_usd,
        entry_reference_price,invalidation_reference_price,roundtrip_back_usd,
        cost_contract_version,cost_contract,is_real_fill,reason_code,payload,created_at
      FROM execution_assessments WHERE opportunity_id IN ({placeholders})
      ORDER BY opportunity_id,assessed_at DESC,assessment_id DESC""", identities).fetchall()
    latest = {}
    for row in rows:
        item = _assessment_from_row(row)
        latest.setdefault(item["opportunity_id"], item)
    return latest


def _current_assessment(item: dict) -> dict:
    payload = item.get("payload") if isinstance(item.get("payload"), dict) else {}
    current = {**payload, **{key: value for key, value in item.items() if key != "payload"}}
    # Old immutable assessments predate the explicit flag. Missing meant disabled;
    # expose that fact explicitly while preserving any contradictory stored value.
    current.setdefault("auto_execution_allowed", False)
    if current.get("quote_expires_at") is None and current.get("expires_at") is not None:
        current["quote_expires_at"] = current["expires_at"]
    security = (dict(current["security_gate"])
                if isinstance(current.get("security_gate"), dict) else {})
    security.update({
        "state": current.get("security_state") or "unknown",
        "checked_at": current.get("security_at"),
        "expires_at": current.get("security_expires_at"),
    })
    for field in ("hard_flags", "cautions", "unknown_fields"):
        security.setdefault(field, [])
    current["security_gate"] = security
    execution = (dict(current["execution_probe"])
                 if isinstance(current.get("execution_probe"), dict) else {})
    contract = current.get("cost_contract") if isinstance(current.get("cost_contract"), dict) else {}
    route_loss = next((component.get("pct") for component in contract.get("components", [])
                       if isinstance(component, dict) and component.get("name") == "route_loss"
                       and component.get("status") == "included"), None)
    network_fee = next((component for component in contract.get("components", [])
                        if isinstance(component, dict)
                        and component.get("name") == "network_fee"), None)
    execution.update({
        "state": current.get("route_state") or "unknown",
        "source": current.get("quote_source") or execution.get("source"),
        "api_mode": current.get("quote_mode") or execution.get("api_mode"),
        "checked_at": current.get("quote_at"),
        "notional_usd": current.get("notional_usd"),
        "entry_reference_price": current.get("entry_reference_price"),
        "invalidation_reference_price": current.get("invalidation_reference_price"),
        "roundtrip_back_usd": current.get("roundtrip_back_usd"),
        "roundtrip_loss_pct": route_loss,
        "network_fees_included": (
            network_fee.get("status") == "included" if network_fee else None
        ),
        "is_real_fill": current.get("is_real_fill") is True,
        "read_only": current.get("kind") == "read_only_quote",
    })
    current["execution_probe"] = execution
    return current


def _launch_action(item: dict, assessment: dict | None, evidence_gate: dict | None,
                   now: datetime) -> dict:
    """Derive A0-A4 at read time; active read-only candidates can never be A4."""
    common = {"auto_execution_allowed": False, "actionable_now": False,
              "current_assessment": _current_assessment(assessment) if assessment else None}
    if item.get("decision") == "AVOID":
        return {**common, "action_level": "A0_BLOCKED",
                "action_reason_codes": ["discovery_hard_block"]}
    from src.pipeline.edge_validation import is_protocol_event

    if not is_protocol_event(item):
        return {**common, "action_level": "A1_WATCH",
                "action_reason_codes": ["outside_frozen_edge_protocol"]}
    if assessment is None:
        return {**common, "action_level": "A1_WATCH",
                "action_reason_codes": ["assessment_missing"]}
    current = common["current_assessment"]
    if assessment.get("kind") == "real_fill" and assessment.get("is_real_fill"):
        if current.get("roundtrip_validated") is True:
            return {**common, "action_level": "A4_REAL_FILL_VALIDATED",
                    "action_reason_codes": ["historical_real_roundtrip_validated"]}
        return {**common, "action_level": "A1_WATCH",
                "action_reason_codes": ["real_fill_roundtrip_not_validated"]}
    if assessment.get("security_state") == "avoid" or assessment.get("route_state") == "untradeable":
        return {**common, "action_level": "A0_BLOCKED",
                "action_reason_codes": ["security_or_reverse_route_block"]}
    from src.contract.launch_probe import launch_manual_probe_failures

    candidate = {**item, "recorded_decision": item.get("decision"),
                 "auto_execution_allowed": False, "is_expired": False}
    reasons = launch_manual_probe_failures(
        candidate, current, evidence_gate, now=now
    )
    if reasons:
        watch_reasons = {
            "assessment_not_read_only_quote", "security_not_pass", "route_not_quoted",
            "quote_source_missing", "quote_clock_invalid", "security_clock_invalid",
        }
        level = "A1_WATCH" if any(reason in watch_reasons for reason in reasons) \
            else "A2_PAPER_READY"
        return {**common, "action_level": level,
                "action_reason_codes": reasons}
    return {**common, "action_level": "A3_MANUAL_PROBE", "actionable_now": True,
            "action_reason_codes": ["all_manual_probe_gates_pass"]}


def _normalize_carry_read(item: dict) -> dict:
    """Keep legacy paper-book measurement size out of the position-limit field."""
    if item.get("lane") != "carry":
        return item
    payload = item.get("payload") if isinstance(item.get("payload"), dict) else {}
    recorded_limit = item.get("max_notional_usd")
    measurement = (item.get("measurement_notional_usd_per_leg")
                   or payload.get("measurement_notional_usd_per_leg")
                   or recorded_limit)
    if recorded_limit is not None:
        item["legacy_recorded_max_notional_usd"] = recorded_limit
    item.update({
        "max_notional_usd": None,
        "measurement_notional_usd_per_leg": measurement,
        "measurement_gross_notional_usd": (
            measurement * 2 if isinstance(measurement, (int, float)) else None
        ),
        "position_limit_status": "unknown",
        "action_level": "A1_WATCH",
        "actionable_now": False,
        "auto_execution_allowed": False,
        "action_reason_codes": ["paper_measurement_not_position_limit"],
    })
    return item


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
                                  cost_contract_version,cost_contract,
                                  payload, outcome_state, outcome
                           FROM opportunities WHERE lane=? AND outcome_state='open'
                           ORDER BY detected_at DESC LIMIT ?""", (lane, limit)).fetchall()
        latest_assessments = _latest_assessment_map(c, [row[0] for row in rows])
    finally:
        c.close()
    evidence_gate = None
    if lane in {"launch", "cascade"}:
        try:
            from src.pipeline.opportunity_outcomes import actionability_gate
            evidence_gate = actionability_gate(lane)
        except Exception as exc:
            evidence_gate = {"state": "blocked", "lane": lane,
                             "edge_verdict": "不可判",
                             "reason": f"evidence gate failed: {str(exc)[:80]}"}
    out = []
    for row in rows:
        keys = ("id", "lane", "chain", "token", "symbol", "detected_at", "event_at",
                "decision_at", "quote_at", "executable_at", "expires_at",
                "source", "state", "decision", "entry_price", "invalidation_price",
                "max_notional_usd", "cost_pct_est", "cost_model", "cohort_version",
                "cost_contract_version", "cost_contract", "payload",
                "outcome_state", "outcome")
        item = dict(zip(keys, row))
        initial_recorded_decision = item.get("decision") or "WATCH"
        try:
            payload = json.loads(item.pop("payload"))
            # The stored columns are the immutable first-observed execution plan.
            # Enrichment payload may contain newer market values, but it must never
            # rewrite the entry, invalidation, size cap, or discovery timestamp.
            immutable = {"entry_price", "invalidation_price", "max_notional_usd",
                         "roundtrip_cost_pct_est", "cost_model", "cost_contract",
                         "cost_contract_version", "cohort_version", "decision",
                         *CLOCK_FIELDS}
            if lane == "launch":
                immutable.update({"security_gate", "execution_probe", "action_level",
                                  "current_assessment"})
            elif lane == "airdrop":
                # Campaign verification, claim windows and owned-wallet evidence are
                # current facts, not a price thesis frozen at first discovery. The SQL
                # columns retain the initial audit record while the latest validated
                # payload is allowed to fail closed (or become claim-checkable) here.
                immutable.difference_update({"decision", "decision_at", "expires_at"})
            for key, value in payload.items():
                if key not in immutable:
                    item[key] = value
        except (TypeError, json.JSONDecodeError):
            item.pop("payload", None)
        try:
            item["cost_contract"] = (json.loads(item["cost_contract"])
                                     if item.get("cost_contract") else None)
        except (TypeError, json.JSONDecodeError):
            item["cost_contract"] = None
        if item.get("outcome"):
            try:
                item["outcome"] = json.loads(item["outcome"])
            except (TypeError, json.JSONDecodeError):
                item["outcome"] = None
        _normalize_carry_read(item)
        item["initial_recorded_decision"] = initial_recorded_decision
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
            elif not evidence_gate or evidence_gate.get("state") != "pass":
                effective_decision = "WATCH"
                item["actionability_reason"] = ((evidence_gate or {}).get("reason")
                                                or "evidence gate unavailable")
        elif expired and recorded_decision == "CLAIM_CHECK":
            effective_decision = "EXPIRED"
            item["actionability_reason"] = "claim window expired"
        item["recorded_decision"] = recorded_decision
        if evidence_gate is not None:
            item["evidence_gate"] = evidence_gate
        if lane == "launch":
            action = _launch_action(item, latest_assessments.get(item["id"]),
                                    evidence_gate, now)
            item.update(action)
            effective_decision = ("AVOID" if action["action_level"] == "A0_BLOCKED"
                                  else "SMALL_PROBE" if action["action_level"] == "A3_MANUAL_PROBE"
                                  else "WATCH")
            current_expiry = (action.get("current_assessment") or {}).get("expires_at")
            if current_expiry:
                try:
                    seconds_to_expiry = round((datetime.fromisoformat(current_expiry)
                                               .astimezone(timezone.utc) - now).total_seconds())
                except (TypeError, ValueError):
                    seconds_to_expiry = None
                expired = seconds_to_expiry is not None and seconds_to_expiry <= 0
        elif lane == "carry":
            effective_decision = "WATCH"
            item.update({"action_level": "A1_WATCH", "actionable_now": False,
                         "auto_execution_allowed": False,
                         "action_reason_codes": ["paper_measurement_not_position_limit"]})
        else:
            item["actionable_now"] = effective_decision in {"SMALL_PROBE", "CLAIM_CHECK"}
        item["effective_decision"] = effective_decision
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
                                    max_notional_usd,cost_pct_est,cost_model,cohort_version,
                                    cost_contract_version,cost_contract,payload,
                                    outcome_state,outcome,updated_at
                             FROM opportunities {where}
                             ORDER BY detected_at ASC""").fetchall()
    finally:
        c.close()
    keys = ("id", "lane", "chain", "token", "symbol", "detected_at", "event_at",
            "decision_at", "quote_at", "executable_at", "expires_at",
            "source", "state", "decision", "entry_price", "invalidation_price",
            "max_notional_usd", "cost_pct_est", "cost_model", "cohort_version",
            "cost_contract_version", "cost_contract", "payload",
            "outcome_state", "outcome", "updated_at")
    out = []
    for row in rows:
        item = dict(zip(keys, row))
        for key in ("cost_contract", "payload", "outcome"):
            try:
                item[key] = json.loads(item[key]) if item.get(key) else (
                    None if key == "cost_contract" else {})
            except (TypeError, json.JSONDecodeError):
                item[key] = None if key == "cost_contract" else {}
        _normalize_carry_read(item)
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


def invalidate(ident: str, *, reason: str, state: str = "invalidated") -> bool:
    """Retire disproven source evidence without deleting its audit trail."""
    if state not in {"invalidated", "reorg_removed"}:
        raise ValueError(f"unknown invalidation state: {state}")
    now = datetime.now(timezone.utc).isoformat()
    c = _conn()
    try:
        row = c.execute("SELECT outcome FROM opportunities WHERE id=?", (ident,)).fetchone()
        if row is None:
            return False
        try:
            outcome = json.loads(row[0]) if row[0] else {}
        except (TypeError, json.JSONDecodeError):
            outcome = {}
        outcome["invalidation"] = {"state": state, "reason": str(reason)[:240], "at": now}
        c.execute("""UPDATE opportunities SET state=?,outcome_state='unresolvable',
                     outcome=?,updated_at=? WHERE id=?""",
                  (state, json.dumps(outcome, ensure_ascii=False, separators=(",", ":")),
                   now, ident))
        c.commit()
        return True
    finally:
        c.close()
