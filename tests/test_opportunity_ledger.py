"""The opportunity ledger keeps evidence clocks explicit and immutable."""
from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone

import pytest


def _candidate(**overrides):
    item = {
        "lane": "launch", "chain": "solana", "token": "token", "symbol": "T",
        "event_at": "2026-07-14T11:59:00Z",
        "detected_at": "2026-07-14T12:00:00+00:00",
        "decision_at": "2026-07-14T12:00:02+00:00",
        "quote_at": "2026-07-14T12:00:03+00:00",
        "executable_at": "2026-07-14T12:00:04+00:00",
        "expires_at": "2026-07-14T12:03:00+00:00",
        "state": "live", "decision": "SMALL_PROBE", "entry_price": 1.0,
    }
    item.update(overrides)
    return item


def test_clocks_are_canonical_and_first_observation_is_immutable(tmp_path, monkeypatch):
    from src.pipeline import opportunity_ledger as ledger

    monkeypatch.setattr(ledger, "DB", tmp_path / "ledger.db")
    ledger.record(_candidate())
    ledger.record(_candidate(
        detected_at="2026-07-14T13:00:00+00:00",
        quote_at="2026-07-14T13:00:01+00:00",
        executable_at="2026-07-14T13:00:02+00:00",
        expires_at="2026-07-14T13:03:00+00:00",
    ))

    row = ledger.outcome_rows()[0]
    assert row["event_at"] == "2026-07-14T11:59:00+00:00"
    assert row["detected_at"] == "2026-07-14T12:00:00+00:00"
    assert row["decision_at"] == "2026-07-14T12:00:02+00:00"
    assert row["quote_at"] == "2026-07-14T12:00:03+00:00"
    assert row["executable_at"] == "2026-07-14T12:00:04+00:00"
    assert row["expires_at"] == "2026-07-14T12:03:00+00:00"


def test_record_if_absent_never_refreshes_first_event_provenance(tmp_path, monkeypatch):
    from src.pipeline import opportunity_ledger as ledger

    monkeypatch.setattr(ledger, "DB", tmp_path / "ledger.db")
    ident, inserted = ledger.record_if_absent(_candidate(
        source="factory-a", primary_evidence={"pool": "first"}))
    assert inserted is True
    duplicate, inserted = ledger.record_if_absent(_candidate(
        source="factory-b", primary_evidence={"pool": "later"}, decision="WATCH"))
    assert duplicate == ident and inserted is False

    row = ledger.outcome_rows()[0]
    assert row["source"] == "factory-a"
    assert row["decision"] == "SMALL_PROBE"
    assert row["payload"]["primary_evidence"]["pool"] == "first"


def test_carry_refresh_corrects_legacy_measurement_size_without_weakening_other_lanes(
        tmp_path, monkeypatch):
    from src.pipeline import opportunity_ledger as ledger

    monkeypatch.setattr(ledger, "DB", tmp_path / "ledger.db")
    legacy = _candidate(
        lane="carry", chain="hyperliquid+okx", token="BTC", event_key="paper:1",
        decision="PAPER_OPEN", max_notional_usd=10_000,
    )
    ident, _ = ledger.record(legacy)
    ledger.record({
        **legacy, "decision": "WATCH", "max_notional_usd": None,
        "measurement_notional_usd_per_leg": 10_000,
        "measurement_gross_notional_usd": 20_000,
        "position_limit_status": "unknown",
    })
    launch_id, _ = ledger.record(_candidate(token="launch", max_notional_usd=50))
    ledger.record(_candidate(token="launch", decision="WATCH", max_notional_usd=None))

    c = sqlite3.connect(tmp_path / "ledger.db")
    stored = c.execute(
        "SELECT decision,max_notional_usd FROM opportunities WHERE id=?", (ident,)
    ).fetchone()
    launch_stored = c.execute(
        "SELECT decision,max_notional_usd FROM opportunities WHERE id=?", (launch_id,)
    ).fetchone()
    c.close()
    assert stored == ("WATCH", None)
    assert launch_stored == ("SMALL_PROBE", 50)
    row = next(item for item in ledger.outcome_rows() if item["lane"] == "carry")
    assert row["max_notional_usd"] is None
    assert row["measurement_notional_usd_per_leg"] == 10_000
    assert row["position_limit_status"] == "unknown"
    assert row["action_level"] == "A1_WATCH"
    assert row["actionable_now"] is False
    assert row["auto_execution_allowed"] is False


def test_invalidated_source_event_leaves_audit_row_but_not_active_list(tmp_path, monkeypatch):
    from src.pipeline import opportunity_ledger as ledger

    monkeypatch.setattr(ledger, "DB", tmp_path / "ledger.db")
    ident, _ = ledger.record(_candidate())
    assert ledger.invalidate(ident, state="reorg_removed", reason="factory reorg")
    assert ledger.active("launch") == []
    row = ledger.outcome_rows()[0]
    assert row["state"] == "reorg_removed"
    assert row["outcome_state"] == "unresolvable"
    assert row["outcome"]["invalidation"]["reason"] == "factory reorg"


def test_clock_requires_timezone_and_preserves_late_discovery(tmp_path, monkeypatch):
    from src.pipeline import opportunity_ledger as ledger

    monkeypatch.setattr(ledger, "DB", tmp_path / "ledger.db")
    with pytest.raises(ValueError, match="timezone"):
        ledger.record(_candidate(detected_at="2026-07-14T12:00:00"))
    # A source can be discovered only after its deadline. Preserve that miss instead
    # of rejecting the event or pretending the system saw it while actionable.
    ledger.record(_candidate(expires_at="2026-07-14T11:00:00Z"))
    row = ledger.outcome_rows()[0]
    assert row["expires_at"] < row["detected_at"]


def test_legacy_schema_backfills_only_provable_decision_clock(tmp_path, monkeypatch):
    from src.pipeline import opportunity_ledger as ledger

    path = tmp_path / "legacy.db"
    conn = sqlite3.connect(path)
    conn.execute("""CREATE TABLE opportunities(
        id TEXT PRIMARY KEY, lane TEXT NOT NULL, chain TEXT, token TEXT,
        symbol TEXT, detected_at TEXT NOT NULL, event_at TEXT, source TEXT,
        state TEXT NOT NULL, decision TEXT NOT NULL, entry_price REAL,
        invalidation_price REAL, max_notional_usd REAL, payload TEXT NOT NULL,
        outcome_state TEXT NOT NULL DEFAULT 'open', outcome TEXT, updated_at TEXT NOT NULL
    )""")
    conn.execute("""INSERT INTO opportunities(
        id,lane,detected_at,state,decision,payload,updated_at
    ) VALUES ('old','launch','2026-07-14T12:00:00+00:00','live','WATCH','{}',
              '2026-07-14T12:00:00+00:00')""")
    conn.commit()
    conn.close()
    monkeypatch.setattr(ledger, "DB", path)

    row = ledger.outcome_rows()[0]
    assert row["decision_at"] == row["detected_at"]
    assert row["quote_at"] is None
    assert row["executable_at"] is None
    assert row["expires_at"] is None


def test_active_actionability_fails_closed_on_missing_or_expired_quote(tmp_path, monkeypatch):
    from src.pipeline import opportunity_ledger as ledger

    monkeypatch.setattr(ledger, "DB", tmp_path / "ledger.db")
    now = datetime(2026, 7, 14, 12, tzinfo=timezone.utc)
    ledger.record(_candidate(token="fresh", detected_at=now.isoformat(),
                             quote_at=now.isoformat(),
                             expires_at=(now + timedelta(seconds=60)).isoformat()))
    ledger.record(_candidate(token="expired", detected_at=now.isoformat(),
                             quote_at=now.isoformat(),
                             expires_at=(now - timedelta(seconds=1)).isoformat()))
    ledger.record(_candidate(token="legacy", detected_at=now.isoformat(),
                             quote_at=None, expires_at=None))

    rows = {row["token"]: row for row in ledger.active("launch", now=now)}
    assert rows["fresh"]["effective_decision"] == "WATCH"
    assert rows["fresh"]["actionable_now"] is False
    assert rows["fresh"]["evidence_gate"]["state"] == "collecting"
    assert "SMALL_PROBE 0/20" in rows["fresh"]["actionability_reason"]
    assert rows["fresh"]["seconds_to_expiry"] == 60
    assert rows["expired"]["effective_decision"] == "WATCH"
    assert rows["expired"]["actionable_now"] is False
    assert rows["legacy"]["effective_decision"] == "WATCH"
    assert rows["legacy"]["actionability_reason"] == "missing quote or expiry clock"
    # Historical cohort label remains intact for outcome measurement.
    assert all(row["decision"] == "SMALL_PROBE" for row in rows.values())
    assert all(row["action_level"] == "A1_WATCH" for row in rows.values())


def test_active_probe_requires_proven_cost_after_control_edge(tmp_path, monkeypatch):
    from src.pipeline import opportunity_ledger as ledger

    monkeypatch.setattr(ledger, "DB", tmp_path / "ledger.db")
    now = datetime(2026, 7, 14, 12, tzinfo=timezone.utc)
    for i in range(20):
        ident, _ = ledger.record(_candidate(token=f"probe-{i}"))
        ledger.save_outcome(ident, {"horizons": {"24h": {"net_return_pct_est": 8.0}}},
                            "resolved")
    for i in range(20):
        ident, _ = ledger.record(_candidate(token=f"watch-{i}", decision="WATCH"))
        ledger.save_outcome(ident, {"horizons": {"24h": {"net_return_pct_est": -2.0}}},
                            "resolved")
    ledger.record(_candidate(token="fresh", detected_at=now.isoformat(),
                             quote_at=now.isoformat(),
                             expires_at=(now + timedelta(seconds=60)).isoformat()))

    fresh = {row["token"]: row for row in ledger.active("launch", now=now)}["fresh"]
    assert fresh["evidence_gate"]["state"] == "collecting"
    assert fresh["evidence_gate"]["edge_verdict"] == "不可判"
    assert fresh["effective_decision"] == "WATCH"
    assert fresh["action_level"] == "A1_WATCH"
    assert fresh["action_reason_codes"] == ["legacy_without_v3_contract"]
    assert fresh["actionable_now"] is False
