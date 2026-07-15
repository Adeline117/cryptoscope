"""The opportunity ledger keeps evidence clocks explicit and immutable."""
from __future__ import annotations

import sqlite3

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
