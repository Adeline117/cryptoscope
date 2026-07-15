"""Carry quote-proxy tracker math — hermetic (no network).

The tests pin what is observed, modeled and unknown so paper quotes cannot be laundered
into realized profit or an all-in edge claim.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest


def _run(cp, carries):
    observations = [{
        "symbol": item["symbol"], "status": "observed", "cross": True,
        "observation_version": 1, "observed_edge_ann": item["edge_ann"],
    } for item in carries if item.get("cross")]
    return cp.run(carries, observations=observations)


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
    _run(cp, [
        {"symbol": "FAT", "cross": True, "partial_model_proxy_ann_pct": 40,
         "edge_ann": 40},
        {"symbol": "THIN", "cross": True, "partial_model_proxy_ann_pct": 3,
         "edge_ann": 3},
        {"symbol": "SINGLE", "cross": False, "partial_model_proxy_ann_pct": 90,
         "edge_ann": 90},
    ])
    import sqlite3
    rows = sqlite3.connect(str(cp.DB)).execute("SELECT symbol FROM paper").fetchall()
    assert {r[0] for r in rows} == {"FAT"}          # only the fat-net cross carry opens


def test_accrues_and_closes_with_correct_quote_proxy(cp):
    # open a 40%/yr differential position
    _run(cp, [{"symbol": "X", "cross": True,
              "partial_model_proxy_ann_pct": 40, "edge_ann": 40}])
    import sqlite3
    c = sqlite3.connect(str(cp.DB))
    # backdate entry + last update to 5 days ago, diff held at 40 the whole time
    past = (datetime.now(timezone.utc) - timedelta(days=5)).isoformat()
    c.execute("UPDATE paper SET entry_ts=?, last_ts=?, last_diff=40, accrued_pct=0 WHERE symbol='X'",
              (past, past))
    c.commit(); c.close()
    # differential decays below the floor → natural close
    stats = cp.run([], observations=[{
        "symbol": "X", "status": "observed", "cross": True,
        "observation_version": 1, "observed_edge_ann": 1,
    }])
    assert stats["n_closed"] == 1 and stats["n_open"] == 0
    # Internal raw annualization remains available for long-lived cohorts, but the
    # public summary uses the honest absolute return for this five-day episode.
    #   accrued ≈ 40 * (5/365) = 0.5479% ; hold_yr = 5/365
    #   annualized funding = accrued / hold_yr = 40
    #   cost = entry_slip .05 + exit_slip .05 + fees(2*0.095=.19) = 0.29 ; /hold_yr = 21.17
    #   absolute net ≈ 0.5479 − 0.29 = 0.2579%
    assert stats["avg_hold_days"] == pytest.approx(5.0, abs=0.1)
    assert stats["avg_quoted_rate_integral_pct"] == pytest.approx(0.548, abs=0.01)
    assert stats["avg_book_and_modeled_fee_proxy_pct"] == pytest.approx(0.29, abs=0.001)
    assert stats["avg_net_proxy_pct"] == pytest.approx(0.258, abs=0.01)
    assert stats["avg_predicted_partial_model_ann_pct"] == 40
    assert stats["annualized_proxy_n"] == 0
    assert "avg_annualized_net_proxy_pct" not in stats
    assert stats["all_in_total_pct"] is None
    assert stats["real_edge_n"] == 0
    assert stats["real_edge_eligible"] is False


def test_stats_honest_when_nothing_closed(cp):
    _run(cp, [{"symbol": "X", "cross": True,
              "partial_model_proxy_ann_pct": 40, "edge_ann": 40}])
    s = cp.paper_stats()
    assert s["n_open"] == 1 and s["n_closed"] == 0
    assert "avg_net_proxy_pct" not in s
    position = s["open_positions"][0]
    assert position["symbol"] == "X"
    assert position["entry_at"] and position["last_measured_at"]
    assert position["entry_diff_ann_pct"] == 40
    assert position["predicted_partial_model_net_ann_pct"] == 40
    assert position["exit_diff_floor_ann_pct"] == cp.CLOSE_DIFF_FLOOR
    assert position["execution_mode"] == "paper_orderbook_measurement"
    assert position["cost_contract"]["completeness"] == "partial"
    assert position["cost_contract"]["all_in_total_pct"] is None
    assert position["settled_funding_pct"] is None
    assert position["basis_pnl_pct"] is None
    assert position["realized_net_return_pct"] is None


def test_missing_observation_pauses_without_closing_or_accruing(cp):
    import sqlite3

    _run(cp, [{"symbol": "X", "cross": True,
              "partial_model_proxy_ann_pct": 40, "edge_ann": 40}])
    c = sqlite3.connect(str(cp.DB))
    past = (datetime.now(timezone.utc) - timedelta(days=2)).isoformat()
    c.execute(
        "UPDATE paper SET entry_ts=?,last_ts=?,last_valid_ts=?,last_diff=40,accrued_pct=0 "
        "WHERE symbol='X'",
        (past, past, past),
    )
    c.commit()
    c.close()

    stats = _run(cp, [])
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

    recovered = _run(cp, [{"symbol": "X", "cross": True,
                           "partial_model_proxy_ann_pct": 40, "edge_ann": 40}])
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
    _run(cp, [{"symbol": "X", "cross": True,
              "partial_model_proxy_ann_pct": 40, "edge_ann": 40}])
    c = sqlite3.connect(str(cp.DB))
    accrued = c.execute("SELECT accrued_pct FROM paper WHERE symbol='X'").fetchone()[0]
    c.close()
    assert accrued == pytest.approx(40 / 8760, rel=0.02)


def test_legacy_observation_protocol_cannot_accrue_close_or_open_v3_episode(cp):
    import sqlite3

    _run(cp, [{"symbol": "X", "cross": True,
              "partial_model_proxy_ann_pct": 40, "edge_ann": 40}])
    past = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
    c = sqlite3.connect(str(cp.DB))
    c.execute(
        "UPDATE paper SET entry_ts=?,last_ts=?,last_valid_ts=?,last_diff=40,accrued_pct=0 "
        "WHERE symbol='X'", (past, past, past),
    )
    c.commit()
    c.close()

    stats = cp.run([], observations=[{
        "symbol": "X", "status": "observed", "cross": True,
        "observation_version": 0, "observed_edge_ann": 1,
    }])
    assert stats["n_open"] == 1 and stats["n_closed_total"] == 0
    assert stats["open_positions"][0]["measurement_state"] == "source_gap"
    c = sqlite3.connect(str(cp.DB))
    assert c.execute(
        "SELECT accrued_pct,status FROM paper WHERE symbol='X'"
    ).fetchone() == (0.0, "open")
    c.close()

    no_legacy_open = cp.run(
        [{"symbol": "Y", "cross": True,
          "partial_model_proxy_ann_pct": 40, "edge_ann": 40}],
        observations=None,
    )
    assert {row["symbol"] for row in no_legacy_open["open_positions"]} == {"X"}


def test_exit_quote_gap_freezes_episode_until_real_book_cost_arrives(cp, monkeypatch):
    import sqlite3

    exit_quote = {"value": None}

    def slip(_sym, notional=cp.NOTIONAL, phase="entry"):
        return 0.05 if phase == "entry" else exit_quote["value"]

    monkeypatch.setattr(cp, "_roundtrip_slip", slip)
    _run(cp, [{"symbol": "X", "cross": True,
              "partial_model_proxy_ann_pct": 40, "edge_ann": 40}])
    c = sqlite3.connect(str(cp.DB))
    past = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
    c.execute("UPDATE paper SET entry_ts=?,last_ts=?,last_diff=40 WHERE symbol='X'",
              (past, past))
    c.commit()
    c.close()

    pending = cp.run([], observations=[{
        "symbol": "X", "status": "observed", "cross": True,
        "observation_version": 1, "observed_edge_ann": 1,
    }])
    assert pending["n_open"] == pending["n_exit_pending"] == 1
    assert pending["n_closed_total"] == 0
    position = pending["open_positions"][0]
    assert position["status"] == "exit_pending"
    assert position["measurement_state"] == "exit_quote_gap"
    assert position["exit_signal_diff_ann_pct"] == 1
    assert cp.open_symbols() == ["X"]
    from src.pipeline import opportunity_ledger
    event = opportunity_ledger.outcome_rows()[0]
    assert event["outcome_state"] == "open"
    assert event["outcome"]["status"] == "exit_pending"
    assert "net_return_pct" not in event["outcome"]

    c = sqlite3.connect(str(cp.DB))
    frozen = c.execute(
        "SELECT accrued_pct,exit_signal_ts FROM paper WHERE symbol='X'"
    ).fetchone()
    c.execute(
        "UPDATE paper SET last_ts=? WHERE symbol='X'",
        ((datetime.now(timezone.utc) - timedelta(days=2)).isoformat(),),
    )
    c.commit()
    c.close()
    still_pending = cp.run([], observations=[])
    assert still_pending["n_exit_pending"] == 1
    c = sqlite3.connect(str(cp.DB))
    assert c.execute(
        "SELECT accrued_pct,exit_signal_ts FROM paper WHERE symbol='X'"
    ).fetchone() == frozen
    c.close()

    exit_quote["value"] = 0.08
    closed = cp.run([], observations=[])
    assert closed["n_open"] == 0 and closed["n_closed"] == 1
    recent = closed["recent"][0]
    assert recent["exit_signal_at"] == frozen[1]
    assert recent["exit_quote_at"]
    assert recent["exit_quote_delay_s"] >= 0
    c = sqlite3.connect(str(cp.DB))
    status, exit_slip, complete = c.execute(
        "SELECT status,exit_slip,cost_complete FROM paper WHERE symbol='X'"
    ).fetchone()
    c.close()
    assert (status, exit_slip, complete) == ("closed", 0.08, 1)


def test_delayed_exit_quote_is_retained_but_excluded_from_edge_sample(cp):
    from src.pipeline.execution_cost import carry_paper_contract

    sample = {
        "episode_version": cp.CURRENT_EPISODE_VERSION, "observation_version": 1,
        "close_reason": "diff_below_floor", "book_quote_cost_complete": True,
        "entry_book_impact_pct": 0.05, "exit_book_impact_pct": 0.05,
        "cost_contract": carry_paper_contract(
            notional_usd_per_leg=10_000, entry_book_impact_pct=0.05,
            exit_book_impact_pct=0.05,
            modeled_fee_pct=cp.MODELED_ROUNDTRIP_FEE_PCT),
        "exit_quote_delay_s": cp.CARRY_EXIT_QUOTE_SLA_S + 1,
        "unmeasured_h": 0, "hold_h": 24, "quoted_rate_integral_pct": 0.1,
        "net_proxy_after_book_quotes_and_modeled_fee_pct": -0.19,
    }
    assert cp.proxy_exclusion_reasons(sample) == ["exit_quote_outside_sla"]


def test_proxy_cohort_rejects_cost_contract_or_proxy_math_mismatch(cp):
    from src.pipeline.execution_cost import carry_paper_contract

    contract = carry_paper_contract(
        notional_usd_per_leg=10_000, entry_book_impact_pct=0.05,
        exit_book_impact_pct=0.05,
        modeled_fee_pct=cp.MODELED_ROUNDTRIP_FEE_PCT,
    )
    sample = {
        "episode_version": cp.CURRENT_EPISODE_VERSION, "observation_version": 1,
        "close_reason": "diff_below_floor", "book_quote_cost_complete": True,
        "entry_book_impact_pct": 0.05, "exit_book_impact_pct": 0.05,
        "cost_contract": contract, "exit_quote_delay_s": 0, "unmeasured_h": 0,
        "hold_h": 24, "quoted_rate_integral_pct": 0.54,
        "net_proxy_after_book_quotes_and_modeled_fee_pct": 9,
    }
    assert cp.proxy_exclusion_reasons(sample) == ["inconsistent_proxy_math"]

    malformed = {**sample, "net_proxy_after_book_quotes_and_modeled_fee_pct": 0.25,
                 "cost_contract": {**contract, "known_total_pct": 99}}
    assert cp.proxy_exclusion_reasons(malformed) == [
        "invalid_partial_cost_contract",
    ]

    non_finite_delay = {**sample,
                        "net_proxy_after_book_quotes_and_modeled_fee_pct": 0.25,
                        "exit_quote_delay_s": float("nan")}
    assert cp.proxy_exclusion_reasons(non_finite_delay) == ["exit_quote_outside_sla"]
    non_finite_gap = {**sample,
                      "net_proxy_after_book_quotes_and_modeled_fee_pct": 0.25,
                      "unmeasured_h": float("nan")}
    assert cp.proxy_exclusion_reasons(non_finite_gap) == ["incomplete_quote_rate_path"]
    non_finite_hold = {**sample,
                       "net_proxy_after_book_quotes_and_modeled_fee_pct": 0.25,
                       "hold_h": float("nan")}
    assert cp.proxy_exclusion_reasons(non_finite_hold) == ["invalid_hold_period"]


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

    stats = _run(cp, [{"symbol": "X", "cross": True,
                      "partial_model_proxy_ann_pct": 40, "edge_ann": 40}])
    c = sqlite3.connect(str(cp.DB))
    rows = c.execute(
        "SELECT accrued_pct,measurement_state,status,episode_version,observation_version "
        "FROM paper WHERE symbol='X' ORDER BY id"
    ).fetchall()
    c.close()
    assert rows == [
        (0.0, "legacy_quarantined", "quarantined", None, None),
        (0.0, "observed", "open", cp.CURRENT_EPISODE_VERSION, 1),
    ]
    assert stats["n_open"] == 1
    assert stats["n_quarantined_total"] == 1
    assert cp.open_symbols() == ["X"]

    from src.pipeline import opportunity_ledger
    ledger_rows = opportunity_ledger.outcome_rows()
    assert [row["outcome_state"] for row in ledger_rows] == ["unresolvable", "open"]
    assert ledger_rows[0]["outcome"]["quarantine_reason"] == \
        "legacy_observation_protocol"


def test_legacy_open_episode_never_accrues_or_closes_from_valid_v1_quote(cp):
    import sqlite3

    _run(cp, [{"symbol": "X", "cross": True,
              "partial_model_proxy_ann_pct": 40, "edge_ann": 40}])
    past = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
    c = sqlite3.connect(str(cp.DB))
    c.execute(
        "UPDATE paper SET entry_ts=?,last_ts=?,last_valid_ts=?,last_diff=40,"
        "accrued_pct=0,episode_version=NULL,observation_version=NULL WHERE symbol='X'",
        (past, past, past),
    )
    c.commit()
    c.close()

    assert cp.open_symbols() == []
    before_run = cp.paper_stats()
    assert before_run["n_open"] == 0
    assert before_run["n_quarantined_total"] == 1
    assert cp._sync_opportunity_ledger() == {
        "status": "ok", "synced": 1, "resolved": 0,
    }
    from src.pipeline import opportunity_ledger
    assert opportunity_ledger.outcome_rows()[0]["outcome_state"] == "unresolvable"

    stats = cp.run([], observations=[{
        "symbol": "X", "status": "observed", "cross": True,
        "observation_version": 1, "observed_edge_ann": 1,
    }])
    c = sqlite3.connect(str(cp.DB))
    row = c.execute(
        "SELECT accrued_pct,last_ts,last_valid_ts,status,measurement_state FROM paper "
        "WHERE symbol='X'"
    ).fetchone()
    c.close()
    assert row == (0.0, past, past, "quarantined", "legacy_quarantined")
    assert stats["n_open"] == 0
    assert stats["n_closed_total"] == 0
    assert stats["n_quarantined_total"] == 1


def test_open_and_close_share_one_carry_ledger_lifecycle(cp):
    from src.pipeline import opportunity_ledger

    opened = _run(cp, [{"symbol": "X", "cross": True,
                        "partial_model_proxy_ann_pct": 40, "edge_ann": 40}])
    assert opened["ledger_sync"] == {"status": "ok", "synced": 1, "resolved": 0}
    event = opportunity_ledger.outcome_rows()[0]
    assert event["lane"] == "carry" and event["decision"] == "WATCH"
    assert event["quote_at"] == event["detected_at"]
    assert event["executable_at"] is None       # measured book is not a real fill
    assert event["outcome_state"] == "open"
    assert event["max_notional_usd"] is None
    assert event["measurement_notional_usd_per_leg"] == cp.NOTIONAL
    assert event["measurement_gross_notional_usd"] == cp.NOTIONAL * 2
    assert event["position_limit_status"] == "unknown"
    assert event["action_level"] == "A1_WATCH"
    assert event["actionable_now"] is False
    assert event["auto_execution_allowed"] is False
    entry_contract = event["cost_contract"]
    assert entry_contract["purpose"] == "paper_measurement"
    assert entry_contract["completeness"] == "partial"
    assert entry_contract["all_in_total_pct"] is None
    assert entry_contract["book_quote_cost_complete"] is False
    assert entry_contract["is_real_fill"] is False
    active = opportunity_ledger.active("carry")[0]
    assert active["max_notional_usd"] is None
    assert active["measurement_notional_usd_per_leg"] == cp.NOTIONAL
    assert active["effective_decision"] == "WATCH"

    import sqlite3
    c = sqlite3.connect(str(cp.DB))
    past = (datetime.now(timezone.utc) - timedelta(days=5)).isoformat()
    c.execute("UPDATE paper SET entry_ts=?,last_ts=?,last_diff=40,accrued_pct=0 WHERE symbol='X'",
              (past, past))
    c.commit()
    c.close()
    closed = _run(cp, [{"symbol": "X", "cross": True,
                        "partial_model_proxy_ann_pct": 1, "edge_ann": 1}])
    assert closed["ledger_sync"] == {"status": "ok", "synced": 1, "resolved": 1}
    event = opportunity_ledger.outcome_rows()[0]
    outcome = event["outcome"]
    assert event["outcome_state"] == "resolved"
    assert outcome["close_reason"] == "diff_below_floor"
    assert outcome["net_proxy_after_book_quotes_and_modeled_fee_pct"] == pytest.approx(
        outcome["quoted_rate_integral_pct"]
        - outcome["book_and_modeled_fee_proxy_pct"])
    assert outcome["settled_funding_pct"] is None
    assert outcome["basis_pnl_pct"] is None
    assert outcome["realized_net_return_pct"] is None
    assert outcome["cost_contract"]["all_in_total_pct"] is None
    assert outcome["cost_contract"]["completeness"] == "partial"
    assert outcome["cost_contract"]["book_quote_cost_complete"] is True
    # The top-level event contract is the entry-time snapshot.  The mutable
    # outcome contract may add the later exit quote without rewriting history.
    assert event["cost_contract"] == entry_contract
    assert event["cost_contract"]["book_quote_cost_complete"] is False
    assert outcome["proxy_sample_eligible"] is True
    assert outcome["edge_sample_eligible"] is False
    assert outcome["real_edge_eligible"] is False
    assert outcome["annualized_proxy_eligible"] is False
    assert outcome["annualized_net_proxy_pct"] is None
    assert outcome["cost_is_real_fill"] is False
    stats = cp.paper_stats()
    assert stats["open_positions"] == []
    assert stats["recent"][0]["entry_at"] == past
    assert stats["recent"][0]["closed_at"]
    assert stats["recent"][0]["close_reason"] == "diff_below_floor"
    assert stats["recent"][0]["realized_net_return_pct"] is None

    # Rebuilding an empty opportunity ledger from a closed paper DB must not
    # backfill the later exit quote into the immutable entry-time contract.
    opportunity_ledger.DB = cp.DB.parent / "rebuilt_opportunities.db"
    assert cp._sync_opportunity_ledger() == {"status": "ok", "synced": 1, "resolved": 1}
    rebuilt = opportunity_ledger.outcome_rows()[0]
    assert rebuilt["cost_contract"]["book_quote_cost_complete"] is False
    assert rebuilt["outcome"]["cost_contract"]["book_quote_cost_complete"] is True


def test_annualized_summary_requires_long_enough_cohort(cp):
    import sqlite3

    c = cp._conn()
    for i in range(cp.MIN_ANNUALIZED_SAMPLES):
        c.execute("""INSERT INTO paper(
            symbol,entry_ts,entry_diff,pred_net,entry_slip,notional,accrued_pct,
            last_ts,last_diff,status,exit_ts,exit_slip,hold_h,realized_net,close_reason,
            cost_complete,observation_version,exit_quote_delay_s
        ) VALUES (?,?,?,?,?,?,?,?,?,'closed',?,?,?,?,'diff_below_floor',1,1,0)""",
                  (f"L{i}", "2026-01-01T00:00:00+00:00", 40, 30, 0.05, 10_000,
                   3.0, "2026-02-01T00:00:00+00:00", 1,
                   "2026-02-01T00:00:00+00:00", 0.05,
                   cp.MIN_ANNUALIZED_HOLD_H, 30))
    c.commit()
    c.close()
    stats = cp.paper_stats()
    assert stats["annualized_proxy_n"] == cp.MIN_ANNUALIZED_SAMPLES
    assert stats["avg_annualized_net_proxy_pct"] == 30


def test_stats_quarantine_legacy_bad_exit_cost_and_funding_gaps(cp):
    c = cp._conn()
    base = ("2026-01-01T00:00:00+00:00", 40, 30, 0.05, 10_000, 0.5,
            "2026-01-03T00:00:00+00:00", 1,
            "2026-01-03T00:00:00+00:00", 0.05, 48, 30)
    rows = [
        ("VALID", *base, "diff_below_floor", cp.CURRENT_EPISODE_VERSION, 1, 0, 1, 0),
        ("LEGACY", *base, "diff_below_floor", None, 1, 0, 1, 0),
        ("MISSING", *base, "market_missing", cp.CURRENT_EPISODE_VERSION, 1, 0, 1, 0),
        ("NOEXIT", *base[:-3], None, 48, None, "diff_below_floor",
         cp.CURRENT_EPISODE_VERSION, 0, 0, 1, 0),
        ("GAP", *base, "diff_below_floor", cp.CURRENT_EPISODE_VERSION, 1, 5, 1, 0),
    ]
    c.executemany("""INSERT INTO paper(
        symbol,entry_ts,entry_diff,pred_net,entry_slip,notional,accrued_pct,last_ts,
        last_diff,status,exit_ts,exit_slip,hold_h,realized_net,close_reason,
        episode_version,cost_complete,unmeasured_h,observation_version,exit_quote_delay_s
    ) VALUES (?,?,?,?,?,?,?,?,?,'closed',?,?,?,?,?,?,?,?,?,?)""", rows)
    c.commit()
    c.close()

    stats = cp.paper_stats()
    assert stats["n_closed_total"] == 5
    assert stats["n_closed"] == 1 and stats["n_closed_excluded"] == 4
    assert stats["recent"][0]["symbol"] == "VALID"
    assert stats["excluded_by_reason"]["legacy_episode"] == 1
    assert stats["excluded_by_reason"]["market_missing_close"] == 1
    assert stats["excluded_by_reason"]["incomplete_book_quote_cost"] == 1
    assert stats["excluded_by_reason"]["incomplete_quote_rate_path"] == 1
    assert stats["real_edge_n"] == 0


def test_carry_slippage_uses_real_leg_direction_for_entry_and_exit(monkeypatch):
    import src.pipeline.carry_paper as carry

    monkeypatch.setattr(carry, "_post", lambda *_args, **_kwargs: {"levels": [
        [{"px": "100", "sz": "0.5"}, {"px": "99", "sz": "1"}],
        [{"px": "101", "sz": "0.5"}, {"px": "103", "sz": "1"}],
    ]})
    monkeypatch.setattr(carry, "_get", lambda *_args, **_kwargs: {"code": "0", "data": [{
        "bids": [["200", "0.25"], ["198", "1"]],
        "asks": [["201", "0.25"], ["204", "1"]],
    }]})
    monkeypatch.setattr(carry, "_okx_contract", lambda _coin: ("X", 1.0))

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


def test_okx_contract_metadata_maps_multiplier_symbols_and_never_defaults(monkeypatch):
    import src.pipeline.carry_paper as carry

    carry._OKX_CTVAL.clear()
    monkeypatch.setattr(carry, "_OKX_META_LOADED", False)
    monkeypatch.setattr(carry, "_get", lambda *_args, **_kwargs: {"code": "0", "data": [
        {"instId": "BTC-USDT-SWAP", "state": "live", "ctType": "linear",
         "settleCcy": "USDT", "ctValCcy": "BTC", "ctVal": "0.01"},
        {"instId": "PEPE-USDT-SWAP", "state": "live", "ctType": "linear",
         "settleCcy": "USDT", "ctValCcy": "PEPE", "ctVal": "10000000"},
    ]})

    assert carry._okx_contract("BTC") == ("BTC", 0.01)
    assert carry._okx_contract("kPEPE") == ("PEPE", 10_000_000)
    assert carry._okx_contract("1000PEPE") == ("PEPE", 10_000_000)
    assert carry._okx_ctval("UNKNOWN") is None


@pytest.mark.parametrize("ctval", [None, "", "0", "-1", "nan", "inf", "bad"])
def test_okx_contract_metadata_rejects_invalid_ctval(monkeypatch, ctval):
    import src.pipeline.carry_paper as carry

    carry._OKX_CTVAL.clear()
    monkeypatch.setattr(carry, "_OKX_META_LOADED", False)
    monkeypatch.setattr(carry, "_get", lambda *_args, **_kwargs: {"code": "0", "data": [{
        "instId": "X-USDT-SWAP", "state": "live", "ctType": "linear",
        "settleCcy": "USDT", "ctValCcy": "X", "ctVal": ctval,
    }]})
    assert carry._okx_contract("X") is None


@pytest.mark.parametrize("override", [
    {"state": "suspend"}, {"ctType": "inverse"}, {"settleCcy": "USD"},
    {"ctValCcy": "OTHER"}, {"instId": "X-USDC-SWAP"},
])
def test_okx_contract_metadata_rejects_nontradable_or_mismatched_contract(monkeypatch,
                                                                         override):
    import src.pipeline.carry_paper as carry

    carry._OKX_CTVAL.clear()
    monkeypatch.setattr(carry, "_OKX_META_LOADED", False)
    item = {"instId": "X-USDT-SWAP", "state": "live", "ctType": "linear",
            "settleCcy": "USDT", "ctValCcy": "X", "ctVal": "1"}
    item.update(override)
    monkeypatch.setattr(
        carry, "_get", lambda *_args, **_kwargs: {"code": "0", "data": [item]}
    )
    assert carry._okx_contract("X") is None


def test_okx_contract_metadata_network_failure_remains_retryable(monkeypatch):
    import src.pipeline.carry_paper as carry

    carry._OKX_CTVAL.clear()
    monkeypatch.setattr(carry, "_OKX_META_LOADED", False)
    calls = []

    def fail(*_args, **_kwargs):
        calls.append(1)
        raise OSError("offline")

    monkeypatch.setattr(carry, "_get", fail)
    assert carry._okx_contract("X") is None
    assert carry._okx_contract("X") is None
    assert len(calls) == 2


@pytest.mark.parametrize("bad", ["0", "-1", "nan", "inf"])
def test_okx_slippage_rejects_malformed_book_numbers(monkeypatch, bad):
    import src.pipeline.carry_paper as carry

    monkeypatch.setattr(carry, "_okx_contract", lambda _coin: ("X", 1.0))
    monkeypatch.setattr(carry, "_get", lambda *_args, **_kwargs: {"code": "0", "data": [{
        "bids": [["100", bad]], "asks": [["101", bad]],
    }]})
    assert carry._okx_slip("X", 100, "buy") is None
    assert carry._okx_slip("X", 100, "sell") is None


def test_okx_slippage_uses_resolved_multiplier_symbol(monkeypatch):
    import src.pipeline.carry_paper as carry

    urls = []
    monkeypatch.setattr(carry, "_okx_contract", lambda _coin: ("PEPE", 10_000_000.0))
    monkeypatch.setattr(carry, "_get", lambda url, **_kwargs: urls.append(url) or {
        "code": "0", "data": [{"bids": [["0.00001", "100"]],
                                  "asks": [["0.000011", "100"]]}],
    })
    assert carry._okx_slip("kPEPE", 100, "buy") == pytest.approx(0)
    assert "instId=PEPE-USDT-SWAP" in urls[0]


def test_roundtrip_slippage_rejects_nonfinite_leg(monkeypatch):
    import src.pipeline.carry_paper as carry

    monkeypatch.setattr(carry, "_hl_slip", lambda *_args, **_kwargs: float("nan"))
    monkeypatch.setattr(carry, "_okx_slip", lambda *_args, **_kwargs: 0.1)
    assert carry._roundtrip_slip("X") is None
