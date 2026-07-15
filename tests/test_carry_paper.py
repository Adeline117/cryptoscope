"""Carry paper-tracker math — hermetic (no network). Locks the accrual + realized-net
formula so the numbers it reports in a few days are trustworthy, not a fluent-looking lie.
The whole point of the tracker is to REPLACE assumptions with measurement; if its own
math is wrong, it launders a bug into 'measured truth'. So we pin it."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest


@pytest.fixture
def cp(tmp_path, monkeypatch):
    import src.pipeline.carry_paper as cp
    from src.pipeline import opportunity_ledger
    monkeypatch.setattr(cp, "DB", tmp_path / "paper.db")
    monkeypatch.setattr(opportunity_ledger, "DB", tmp_path / "opportunities.db")
    monkeypatch.setattr(
        cp, "_roundtrip_slip",
        lambda sym, notional=cp.NOTIONAL, phase="entry": 0.05,
    )
    return cp


def test_opens_only_fat_net_cross(cp):
    cp.run([
        {"symbol": "FAT", "cross": True, "net_ann": 40, "edge_ann": 40},
        {"symbol": "THIN", "cross": True, "net_ann": 3, "edge_ann": 3},   # < OPEN_MIN_NET
        {"symbol": "SINGLE", "cross": False, "net_ann": 90, "edge_ann": 90},  # not cross
    ])
    import sqlite3
    rows = sqlite3.connect(str(cp.DB)).execute("SELECT symbol FROM paper").fetchall()
    assert {r[0] for r in rows} == {"FAT"}          # only the fat-net cross carry opens


def test_accrues_and_closes_with_correct_realized_net(cp):
    # open a 40%/yr differential position
    cp.run([{"symbol": "X", "cross": True, "net_ann": 40, "edge_ann": 40}])
    import sqlite3
    c = sqlite3.connect(str(cp.DB))
    # backdate entry + last update to 5 days ago, diff held at 40 the whole time
    past = (datetime.now(timezone.utc) - timedelta(days=5)).isoformat()
    c.execute("UPDATE paper SET entry_ts=?, last_ts=?, last_diff=40, accrued_pct=0 WHERE symbol='X'",
              (past, past))
    c.commit(); c.close()
    # differential decays below the floor → natural close
    stats = cp.run([{"symbol": "X", "cross": True, "net_ann": 1, "edge_ann": 1}])
    assert stats["n_closed"] == 1 and stats["n_open"] == 0
    # Internal raw annualization remains available for long-lived cohorts, but the
    # public summary uses the honest absolute return for this five-day episode.
    #   accrued ≈ 40 * (5/365) = 0.5479% ; hold_yr = 5/365
    #   annualized funding = accrued / hold_yr = 40
    #   cost = entry_slip .05 + exit_slip .05 + fees(2*0.095=.19) = 0.29 ; /hold_yr = 21.17
    #   absolute net ≈ 0.5479 − 0.29 = 0.2579%
    assert stats["avg_hold_days"] == pytest.approx(5.0, abs=0.1)
    assert stats["avg_funding_accrued_pct"] == pytest.approx(0.548, abs=0.01)
    assert stats["avg_cost_pct"] == pytest.approx(0.29, abs=0.001)
    assert stats["avg_net_return_pct"] == pytest.approx(0.258, abs=0.01)
    assert stats["avg_predicted_ann_pct"] == 40
    assert stats["annualized_n"] == 0
    assert "avg_annualized_net_pct" not in stats


def test_stats_honest_when_nothing_closed(cp):
    cp.run([{"symbol": "X", "cross": True, "net_ann": 40, "edge_ann": 40}])
    s = cp.paper_stats()
    assert s["n_open"] == 1 and s["n_closed"] == 0
    assert "avg_net_return_pct" not in s         # never a number before a real close
    position = s["open_positions"][0]
    assert position["symbol"] == "X"
    assert position["entry_at"] and position["last_measured_at"]
    assert position["entry_diff_ann_pct"] == 40
    assert position["predicted_net_ann_pct"] == 40
    assert position["exit_diff_floor_ann_pct"] == cp.CLOSE_DIFF_FLOOR
    assert position["execution_mode"] == "paper_orderbook_measurement"


def test_missing_observation_pauses_without_closing_or_accruing(cp):
    import sqlite3

    cp.run([{"symbol": "X", "cross": True, "net_ann": 40, "edge_ann": 40}])
    c = sqlite3.connect(str(cp.DB))
    past = (datetime.now(timezone.utc) - timedelta(days=2)).isoformat()
    c.execute(
        "UPDATE paper SET entry_ts=?,last_ts=?,last_valid_ts=?,last_diff=40,accrued_pct=0 "
        "WHERE symbol='X'",
        (past, past, past),
    )
    c.commit()
    c.close()

    stats = cp.run([])
    assert stats["n_open"] == 1 and stats["n_closed"] == 0
    position = stats["open_positions"][0]
    assert position["measurement_state"] == "source_gap"
    assert position["unmeasured_h"] == pytest.approx(48, abs=0.1)
    assert position["last_valid_at"] == past
    assert position["last_measured_at"] == past
    assert position["last_attempt_at"] != past

    c = sqlite3.connect(str(cp.DB))
    row = c.execute(
        "SELECT accrued_pct,last_ts FROM paper WHERE symbol='X'"
    ).fetchone()
    assert row[0] == 0
    recovery_start = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    c.execute("UPDATE paper SET last_ts=? WHERE symbol='X'", (recovery_start,))
    c.commit()
    c.close()

    recovered = cp.run([{"symbol": "X", "cross": True, "net_ann": 40, "edge_ann": 40}])
    position = recovered["open_positions"][0]
    assert position["measurement_state"] == "observed"
    assert position["unmeasured_h"] == pytest.approx(49, abs=0.1)
    c = sqlite3.connect(str(cp.DB))
    assert c.execute("SELECT accrued_pct FROM paper WHERE symbol='X'").fetchone()[0] == 0
    c.close()

    c = sqlite3.connect(str(cp.DB))
    observed_start = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    c.execute("UPDATE paper SET last_ts=? WHERE symbol='X'", (observed_start,))
    c.commit()
    c.close()
    cp.run([{"symbol": "X", "cross": True, "net_ann": 40, "edge_ann": 40}])
    c = sqlite3.connect(str(cp.DB))
    accrued = c.execute("SELECT accrued_pct FROM paper WHERE symbol='X'").fetchone()[0]
    c.close()
    assert accrued == pytest.approx(40 / 8760, rel=0.02)


def test_legacy_open_episode_migrates_without_backfilling_unknown_time(cp):
    import sqlite3

    past = (datetime.now(timezone.utc) - timedelta(days=2)).isoformat()
    c = sqlite3.connect(str(cp.DB))
    c.execute("""CREATE TABLE paper(
        id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT, entry_ts TEXT,
        entry_diff REAL, pred_net REAL, entry_slip REAL, notional REAL,
        accrued_pct REAL DEFAULT 0, last_ts TEXT, last_diff REAL,
        status TEXT DEFAULT 'open', exit_ts TEXT, exit_slip REAL,
        hold_h REAL, realized_net REAL, close_reason TEXT)""")
    c.execute(
        "INSERT INTO paper(symbol,entry_ts,entry_diff,pred_net,entry_slip,notional,"
        "accrued_pct,last_ts,last_diff,status) VALUES ('X',?,?,?,?,?,0,?,40,'open')",
        (past, 40, 40, 0.05, 10_000, past),
    )
    c.commit()
    c.close()

    migrated = cp._conn()
    state = migrated.execute(
        "SELECT measurement_state,last_valid_ts,last_ts,unmeasured_h FROM paper"
    ).fetchone()
    migrated.close()
    assert state[0] == "migration_gap"
    assert state[1] == past and state[2] != past
    assert state[3] == pytest.approx(48, abs=0.1)

    cp.run([{"symbol": "X", "cross": True, "net_ann": 40, "edge_ann": 40}])
    c = sqlite3.connect(str(cp.DB))
    accrued, measurement_state = c.execute(
        "SELECT accrued_pct,measurement_state FROM paper WHERE symbol='X'"
    ).fetchone()
    c.close()
    assert accrued == 0
    assert measurement_state == "observed"


def test_open_and_close_share_one_carry_ledger_lifecycle(cp):
    from src.pipeline import opportunity_ledger

    opened = cp.run([{"symbol": "X", "cross": True, "net_ann": 40, "edge_ann": 40}])
    assert opened["ledger_sync"] == {"status": "ok", "synced": 1, "resolved": 0}
    event = opportunity_ledger.outcome_rows()[0]
    assert event["lane"] == "carry" and event["decision"] == "PAPER_OPEN"
    assert event["quote_at"] == event["detected_at"]
    assert event["executable_at"] is None       # measured book is not a real fill
    assert event["outcome_state"] == "open"

    import sqlite3
    c = sqlite3.connect(str(cp.DB))
    past = (datetime.now(timezone.utc) - timedelta(days=5)).isoformat()
    c.execute("UPDATE paper SET entry_ts=?,last_ts=?,last_diff=40,accrued_pct=0 WHERE symbol='X'",
              (past, past))
    c.commit()
    c.close()
    closed = cp.run([{"symbol": "X", "cross": True, "net_ann": 1, "edge_ann": 1}])
    assert closed["ledger_sync"] == {"status": "ok", "synced": 1, "resolved": 1}
    event = opportunity_ledger.outcome_rows()[0]
    outcome = event["outcome"]
    assert event["outcome_state"] == "resolved"
    assert outcome["close_reason"] == "diff_below_floor"
    assert outcome["net_return_pct"] == pytest.approx(
        outcome["funding_accrued_pct"] - outcome["realized_cost_pct"])
    assert outcome["cost_is_real_fill"] is False
    stats = cp.paper_stats()
    assert stats["open_positions"] == []
    assert stats["recent"][0]["entry_at"] == past
    assert stats["recent"][0]["closed_at"]
    assert stats["recent"][0]["close_reason"] == "diff_below_floor"


def test_annualized_summary_requires_long_enough_cohort(cp):
    import sqlite3

    c = cp._conn()
    for i in range(cp.MIN_ANNUALIZED_SAMPLES):
        c.execute("""INSERT INTO paper(
            symbol,entry_ts,entry_diff,pred_net,entry_slip,notional,accrued_pct,
            last_ts,last_diff,status,exit_ts,exit_slip,hold_h,realized_net
        ) VALUES (?,?,?,?,?,?,?,?,?,'closed',?,?,?,?)""",
                  (f"L{i}", "2026-01-01T00:00:00+00:00", 40, 30, 0.05, 10_000,
                   3.0, "2026-02-01T00:00:00+00:00", 1,
                   "2026-02-01T00:00:00+00:00", 0.05,
                   cp.MIN_ANNUALIZED_HOLD_H, 30))
    c.commit()
    c.close()
    stats = cp.paper_stats()
    assert stats["annualized_n"] == cp.MIN_ANNUALIZED_SAMPLES
    assert stats["avg_annualized_net_pct"] == 30


def test_carry_slippage_uses_real_leg_direction_for_entry_and_exit(monkeypatch):
    import src.pipeline.carry_paper as carry

    monkeypatch.setattr(carry, "_post", lambda *_args, **_kwargs: {"levels": [
        [{"px": "100", "sz": "0.5"}, {"px": "99", "sz": "1"}],
        [{"px": "101", "sz": "0.5"}, {"px": "103", "sz": "1"}],
    ]})
    monkeypatch.setattr(carry, "_get", lambda *_args, **_kwargs: {"data": [{
        "bids": [["200", "0.25"], ["198", "1"]],
        "asks": [["201", "0.25"], ["204", "1"]],
    }]})
    monkeypatch.setattr(carry, "_okx_ctval", lambda _coin: 1.0)

    hl_sell = carry._hl_slip("X", 100, "sell")
    hl_buy = carry._hl_slip("X", 100, "buy")
    okx_buy = carry._okx_slip("X", 100, "buy")
    okx_sell = carry._okx_slip("X", 100, "sell")

    assert carry._roundtrip_slip("X", 100, phase="entry") == pytest.approx(
        hl_sell + okx_buy
    )
    assert carry._roundtrip_slip("X", 100, phase="exit") == pytest.approx(
        hl_buy + okx_sell
    )
    assert carry._roundtrip_slip("X", 100, phase="bad") is None
