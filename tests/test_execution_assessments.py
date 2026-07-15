"""Execution measurements are append-only and separate from discovery snapshots."""
from datetime import datetime, timedelta, timezone

import pytest


def _setup(tmp_path, monkeypatch):
    from src.pipeline import opportunity_ledger as ledger

    monkeypatch.setattr(ledger, "DB", tmp_path / "ledger.db")
    ident, _ = ledger.record({
        "lane": "launch", "chain": "solana", "token": "token", "symbol": "T",
        "decision": "WATCH", "state": "live", "entry_price": 1.0,
        "max_notional_usd": 25,
    })
    return ledger, ident


def _assessment(at, **overrides):
    from src.pipeline.execution_cost import route_contract

    item = {
        "kind": "read_only_quote", "assessed_at": at.isoformat(),
        "security_state": "pass", "security_at": at.isoformat(),
        "security_expires_at": (at + timedelta(minutes=5)).isoformat(),
        "route_state": "quoted", "quote_source": "Jupiter", "quote_mode": "keyless",
        "quote_at": at.isoformat(), "quote_expires_at": (at + timedelta(seconds=60)).isoformat(),
        "expires_at": (at + timedelta(seconds=60)).isoformat(), "notional_usd": 25,
        "entry_reference_price": 1.1, "invalidation_reference_price": 0.77,
        "roundtrip_back_usd": 24.5,
        "cost_contract": route_contract(notional_usd=25, route_loss_pct=2.0,
                                        method="jupiter_worst_threshold_roundtrip"),
        "is_real_fill": False,
    }
    item.update(overrides)
    return item


def test_two_quotes_are_retained_and_latest_is_selected(tmp_path, monkeypatch):
    ledger, ident = _setup(tmp_path, monkeypatch)
    first = datetime(2026, 7, 15, 12, tzinfo=timezone.utc)
    second = first + timedelta(minutes=1)

    first_id, inserted = ledger.append_execution_assessment(ident, _assessment(first))
    assert inserted is True
    second_id, inserted = ledger.append_execution_assessment(
        ident, _assessment(second, entry_reference_price=1.2))
    assert inserted is True and second_id != first_id
    latest = ledger.latest_execution_assessment(ident)
    assert latest["assessment_id"] == second_id
    assert latest["entry_reference_price"] == 1.2
    assert latest["cost_contract"]["all_in_total_pct"] is None

    c = ledger._conn()
    try:
        assert c.execute("SELECT COUNT(*) FROM execution_assessments").fetchone()[0] == 2
    finally:
        c.close()


def test_duplicate_assessment_is_idempotent(tmp_path, monkeypatch):
    ledger, ident = _setup(tmp_path, monkeypatch)
    at = datetime(2026, 7, 15, 12, tzinfo=timezone.utc)
    assessment = _assessment(at)
    first = ledger.append_execution_assessment(ident, assessment)
    second = ledger.append_execution_assessment(ident, assessment)
    assert first[0] == second[0]
    assert first[1] is True and second[1] is False


def test_assessment_table_rejects_mutation(tmp_path, monkeypatch):
    import sqlite3

    ledger, ident = _setup(tmp_path, monkeypatch)
    at = datetime(2026, 7, 15, 12, tzinfo=timezone.utc)
    assessment_id, _ = ledger.append_execution_assessment(ident, _assessment(at))
    c = ledger._conn()
    try:
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            c.execute("UPDATE execution_assessments SET route_state='fake' "
                      "WHERE assessment_id=?", (assessment_id,))
    finally:
        c.close()


def test_assessment_rejects_bad_clocks_notional_and_fill_claims(tmp_path, monkeypatch):
    ledger, ident = _setup(tmp_path, monkeypatch)
    at = datetime(2026, 7, 15, 12, tzinfo=timezone.utc)
    with pytest.raises(ValueError, match="timezone"):
        ledger.append_execution_assessment(
            ident, _assessment(at, assessed_at="2026-07-15T12:00:00"))
    with pytest.raises(ValueError, match="expiry"):
        ledger.append_execution_assessment(
            ident, _assessment(at, expires_at=(at - timedelta(seconds=1)).isoformat()))
    with pytest.raises(ValueError, match="notional disagrees"):
        ledger.append_execution_assessment(ident, _assessment(at, notional_usd=30))
    with pytest.raises(ValueError, match="real_fill"):
        ledger.append_execution_assessment(
            ident, _assessment(at, kind="real_fill", is_real_fill=False))


def test_legacy_event_has_no_invented_assessment(tmp_path, monkeypatch):
    ledger, ident = _setup(tmp_path, monkeypatch)
    assert ledger.latest_execution_assessment(ident) is None


def test_discovery_cost_contract_is_stored_on_immutable_row(tmp_path, monkeypatch):
    from src.pipeline import opportunity_ledger as ledger
    from src.pipeline.execution_cost import discovery_contract

    monkeypatch.setattr(ledger, "DB", tmp_path / "ledger.db")
    contract = discovery_contract(notional_usd=25, modeled_roundtrip_pct=1.2,
                                  method="constant_product_v1")
    ledger.record({"lane": "launch", "chain": "solana", "token": "token",
                   "symbol": "T", "decision": "WATCH", "state": "live",
                   "entry_price": 1.0, "max_notional_usd": 25,
                   "roundtrip_cost_pct_est": 1.2, "cost_contract": contract,
                   "cohort_version": 3})
    row = ledger.outcome_rows()[0]
    assert row["cohort_version"] == 3
    assert row["cost_contract_version"] == 1
    assert row["cost_contract"]["purpose"] == "discovery_outcome"
    assert ledger.active("launch")[0]["cost_contract"]["known_total_pct"] == 1.2
