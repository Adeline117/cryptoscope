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
    monkeypatch.setattr(cp, "_roundtrip_slip", lambda sym, notional=cp.NOTIONAL: 0.05)
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
