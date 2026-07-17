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
    assert state["drawdown_basis"] == "coarse_pnl_history_understates_intraday"
    assert "非投资建议" in state["disclaimer"]

    window = state["windows"]["allTime"]
    assert window["span_days"] == 10.0
    assert window["pnl_usd"] == 10.0
    assert window["avg_tvl_usd"] == 100.0
    # +10 on 100 avg TVL over 10 days => 10% * (365/10) = 365%.
    assert window["annualized_pct"] == pytest.approx(365.0)
    # cumulative pnl peaked at +6 then fell to +2 => -4 peak-to-trough.
    assert window["max_drawdown_usd"] == -4.0
    assert window["max_drawdown_pct_of_avg_tvl"] == -4.0
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


def test_run_persists_projected_state(monkeypatch, tmp_path):
    monkeypatch.setattr(hlp_tracker, "STATE_FILE", tmp_path / "hlp_state.json")
    monkeypatch.setattr(hlp_tracker, "fetch_details", lambda **_k: _details())

    state = hlp_tracker.run(now=NOW)
    assert state["available"] is True

    import json
    written = json.loads((tmp_path / "hlp_state.json").read_text())
    assert written["windows"]["allTime"]["annualized_pct"] == pytest.approx(365.0)
