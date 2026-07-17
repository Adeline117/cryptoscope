"""HLP lane: honest historical metrics, fail-closed on any malformed input."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from src.pipeline import hlp_tracker


NOW = datetime(2026, 7, 17, 6, 0, tzinfo=timezone.utc)
DAY = 86_400_000


def _window(start_ms, tvl_points, pnl_points):
    return {
        "accountValueHistory": [[start_ms + i * DAY, str(v)] for i, v in enumerate(tvl_points)],
        "pnlHistory": [[start_ms + i * DAY, str(v)] for i, v in enumerate(pnl_points)],
    }


def _details(**overrides):
    # 100 avg TVL, +10 PnL over 10 days => 10% period => 365% annualized; a dip
    # to +2 after a +6 peak makes the drawdown a clean -4.
    ten_day_tvl = [100] * 11
    ten_day_pnl = [0, 2, 4, 6, 2, 3, 5, 7, 8, 9, 10]
    payload = {
        "name": "Hyperliquidity Provider (HLP)",
        "vaultAddress": hlp_tracker.HLP_VAULT_ADDRESS,
        "apr": -3.0e-05,
        "allowDeposits": True,
        "portfolio": [
            ["day", _window(0, [100, 100], [0, 1])],
            ["week", _window(0, ten_day_tvl, ten_day_pnl)],
            ["month", _window(0, ten_day_tvl, ten_day_pnl)],
            ["allTime", _window(0, ten_day_tvl, ten_day_pnl)],
        ],
    }
    payload.update(overrides)
    return payload


def test_valid_vault_projects_annualized_return_and_drawdown():
    state = hlp_tracker.compute_hlp_state(_details(), now=NOW)

    assert state["available"] is True
    assert state["generated_at"] == NOW.isoformat()
    assert state["vault_address"] == hlp_tracker.HLP_VAULT_ADDRESS
    assert state["current_tvl_usd"] == 100.0
    assert state["allow_deposits"] is True
    # The noisy rolling apr is surfaced as a small percentage, never as the return.
    assert state["instant_apr_pct"] == pytest.approx(-0.003, abs=1e-6)
    assert state["drawdown_basis"] == "return_compounded_lower_bound_at_series_resolution"
    assert "非投资建议" in state["disclaimer"]

    window = state["windows"]["allTime"]
    assert window["span_days"] == 10.0
    assert window["pnl_usd"] == 10.0
    assert window["avg_tvl_usd"] == 100.0
    # +10 on 100 avg TVL over 10 days => 10% * (365/10) = 365%.
    assert window["annualized_pct"] == pytest.approx(365.0)
    # cumulative pnl peaked at +6 then fell to +2 => -4 dollar peak-to-trough.
    assert window["max_drawdown_usd"] == -4.0
    # Return-based: the -0.04 step falls straight off the compounded peak => -4%.
    assert window["max_drawdown_pct"] == pytest.approx(-4.0)
    # The fixture points are 1 day apart.
    assert window["resolution_hours"] == 24.0
    assert set(state["windows"]) == {"week", "month", "allTime"}


@pytest.mark.parametrize("mutate, reason_contains", [
    (lambda d: d.update(vaultAddress="0xdeadbeef"), "canonical HLP"),
    (lambda d: d.update(portfolio="not-a-list"), "portfolio is missing"),
    (lambda d: d.update(portfolio=[["week", _window(0, [100, 100], [0, 1])]]), "month is missing"),
    (lambda d: d.update(allowDeposits="yes"), "allowDeposits"),
    (lambda d: d.update(apr="not-a-number"), "apr"),
    (lambda d: d["portfolio"].__setitem__(3, ["allTime", {"accountValueHistory": [[0, "100"]], "pnlHistory": [[0, "0"]]}]), "two points"),
    (lambda d: d["portfolio"].__setitem__(3, ["allTime", _window(0, [0, 0], [0, 5])]), "TVL is not positive"),
])
def test_malformed_vault_fails_closed_without_fabricating(mutate, reason_contains):
    payload = _details()
    mutate(payload)
    state = hlp_tracker.compute_hlp_state(payload, now=NOW)

    assert state["available"] is False
    assert reason_contains in state["reason"]
    # A fail-closed state must never leak a fabricated metric.
    assert "windows" not in state and "annualized_pct" not in state


def test_non_dict_payload_fails_closed():
    for bad in (None, [], "x", 42):
        state = hlp_tracker.compute_hlp_state(bad, now=NOW)
        assert state["available"] is False


def test_run_fails_closed_when_fetch_raises(monkeypatch, tmp_path):
    monkeypatch.setattr(hlp_tracker, "STATE_FILE", tmp_path / "hlp_state.json")

    def boom(**_kwargs):
        raise TimeoutError("slow")

    monkeypatch.setattr(hlp_tracker, "fetch_details", boom)
    state = hlp_tracker.run(now=NOW)

    assert state["available"] is False
    assert "fetch failed: TimeoutError" in state["reason"]
    # State is still persisted so the board can render the outage honestly.
    assert (tmp_path / "hlp_state.json").exists()


def _details_with_day(day_window, **overrides):
    payload = _details(**overrides)
    payload["portfolio"][0] = ["day", day_window]
    return payload


def test_fine_history_dedups_rebased_windows_and_accumulates(monkeypatch, tmp_path):
    monkeypatch.setattr(hlp_tracker, "HISTORY_DB", tmp_path / "hlp_history.db")

    # First fetch: window covering t0..t2 with absolute pnl [0, 3, 5].
    first = _details_with_day(_window(0, [100, 100, 100], [0, 3, 5]))
    got = hlp_tracker.record_fine_history(first, now=NOW)
    assert got == {"recorded": True, "inserted": 2, "total": 2}

    # Second fetch: window slid by one day and REBASED (same intervals now
    # reported as [0, 2, 4] from t1). Steps are base-invariant: the t1..t2
    # interval dedups; only the genuinely new t2..t3 step lands.
    second = _details_with_day(_window(DAY, [100, 100, 100], [0, 2, 4]))
    got = hlp_tracker.record_fine_history(second, now=NOW)
    assert got == {"recorded": True, "inserted": 1, "total": 3}

    summary = hlp_tracker.fine_history_summary()
    assert summary["available"] is True
    assert summary["n_steps"] == 3
    assert summary["segments"] == 1
    assert summary["coverage_pct"] == 100.0
    # steps +3, +2, +2 on base 100 — monotonic climb, no drawdown.
    assert summary["max_drawdown_pct"] == 0.0
    assert summary["basis"] == "forward_accumulated_fine_steps_gap_segmented"


def test_fine_history_never_compounds_across_gaps(monkeypatch, tmp_path):
    monkeypatch.setattr(hlp_tracker, "HISTORY_DB", tmp_path / "hlp_history.db")

    # Segment 1 ends at t2; segment 2 starts at t5 — a 3-day recording gap.
    hlp_tracker.record_fine_history(
        _details_with_day(_window(0, [100, 100, 100], [0, -4, -4])), now=NOW)
    hlp_tracker.record_fine_history(
        _details_with_day(_window(5 * DAY, [100, 100], [0, -2])), now=NOW)

    summary = hlp_tracker.fine_history_summary()
    assert summary["segments"] == 2
    # Worst per-segment drawdown is segment 1's -4%; the gap does not let the
    # -2% of segment 2 stack onto it (-6%) via silently-assumed zero returns.
    assert summary["max_drawdown_pct"] == pytest.approx(-4.0)
    assert summary["span_days"] == 6.0
    assert summary["covered_days"] == 3.0
    assert summary["coverage_pct"] == 50.0


def test_fine_history_fails_closed_on_malformed_details(monkeypatch, tmp_path):
    monkeypatch.setattr(hlp_tracker, "HISTORY_DB", tmp_path / "hlp_history.db")

    for bad in (None, {}, _details(vaultAddress="0xdead"),
                _details(portfolio=[["week", _window(0, [100, 100], [0, 1])]])):
        got = hlp_tracker.record_fine_history(bad, now=NOW)
        assert got["recorded"] is False and got["reason"]
    assert hlp_tracker.fine_history_summary() == {
        "available": False, "reason": "insufficient_history", "n_steps": 0}


def test_run_records_fine_history_from_the_same_fetch(monkeypatch, tmp_path):
    monkeypatch.setattr(hlp_tracker, "STATE_FILE", tmp_path / "hlp_state.json")
    monkeypatch.setattr(hlp_tracker, "HISTORY_DB", tmp_path / "hlp_history.db")
    monkeypatch.setattr(hlp_tracker, "fetch_details", lambda **_k: _details())

    state = hlp_tracker.run(now=NOW)
    assert state["available"] is True
    # The day-window fixture has 2 points => 1 step recorded.
    assert hlp_tracker.fine_history_summary()["n_steps"] >= 1


def test_run_persists_projected_state(monkeypatch, tmp_path):
    monkeypatch.setattr(hlp_tracker, "STATE_FILE", tmp_path / "hlp_state.json")
    monkeypatch.setattr(hlp_tracker, "HISTORY_DB", tmp_path / "hlp_history.db")
    monkeypatch.setattr(hlp_tracker, "fetch_details", lambda **_k: _details())

    state = hlp_tracker.run(now=NOW)
    assert state["available"] is True

    import json
    written = json.loads((tmp_path / "hlp_state.json").read_text())
    assert written["windows"]["allTime"]["annualized_pct"] == pytest.approx(365.0)
