"""meta.hlp: fresh-only projection from the state file, fail-closed contract."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from src.contract.board_view import BoardViewContractError, _validate_hlp
from src.pipeline import board_export, hlp_tracker


NOW = datetime(2026, 7, 17, 6, 0, tzinfo=timezone.utc)


def _valid_state(generated_at=NOW):
    return {
        "available": True,
        "generated_at": generated_at.isoformat(),
        "vault": "Hyperliquidity Provider (HLP)",
        "vault_address": hlp_tracker.HLP_VAULT_ADDRESS,
        "current_tvl_usd": 251_000_000.0,
        "allow_deposits": True,
        "instant_apr_pct": -0.003,
        "windows": {
            name: {
                "span_days": 10.0, "pnl_usd": 10.0, "avg_tvl_usd": 100.0,
                "annualized_pct": 365.0, "max_drawdown_usd": -4.0,
                "max_drawdown_pct": -4.0, "resolution_hours": 24.0,
            } for name in ("week", "month", "allTime")
        },
        "drawdown_basis": "return_compounded_lower_bound_at_series_resolution",
        "disclaimer": "非投资建议。",
    }


def test_hlp_reads_fresh_state_and_fails_closed_when_stale_or_missing(
        monkeypatch, tmp_path):
    state_file = tmp_path / "hlp_state.json"
    monkeypatch.setattr(hlp_tracker, "STATE_FILE", state_file)

    # Missing file → fail closed.
    assert board_export._hlp(now=NOW) == {
        "available": False, "reason": "hlp_state_unavailable"}

    # Fresh file → passed through and contract-valid.
    state_file.write_text(json.dumps(_valid_state()))
    fresh = board_export._hlp(now=NOW + timedelta(minutes=10))
    assert fresh["available"] is True
    _validate_hlp(fresh, path="meta.hlp")

    # Stale beyond the max age → fail closed, no metrics leak.
    stale = board_export._hlp(now=NOW + timedelta(hours=3))
    assert stale["available"] is False and stale["reason"] == "hlp_state_stale"
    assert "windows" not in stale
    _validate_hlp(stale, path="meta.hlp")

    # A fresh file written by an OLDER tracker schema (e.g. the pre-return-based
    # drawdown fields) must fail closed rather than pass a shape the meta
    # validator would hard-reject and break the whole export.
    legacy = _valid_state()
    legacy["drawdown_basis"] = "coarse_pnl_history_understates_intraday"
    for window in legacy["windows"].values():
        window["max_drawdown_pct_of_avg_tvl"] = window.pop("max_drawdown_pct")
        window.pop("resolution_hours")
    state_file.write_text(json.dumps(legacy))
    drifted = board_export._hlp(now=NOW + timedelta(minutes=10))
    assert drifted["available"] is False
    assert drifted["reason"] == "hlp_state_schema_mismatch"
    _validate_hlp(drifted, path="meta.hlp")


def test_validate_hlp_accepts_fail_closed_and_rejects_tampering():
    # Fail-closed shape is always acceptable.
    _validate_hlp({"available": False, "reason": "x"}, path="meta.hlp")

    valid = _valid_state()
    _validate_hlp(valid, path="meta.hlp")

    tampering = [
        {**valid, "vault_address": "0xdeadbeef"},
        {**valid, "allow_deposits": "yes"},
        {**valid, "drawdown_basis": "clean"},
        {**valid, "current_tvl_usd": "lots"},
        {**valid, "disclaimer": ""},
        {**valid, "windows": {"week": valid["windows"]["week"]}},
        # An unavailable state must never smuggle a fabricated metric.
        {"available": False, "reason": "x", "windows": valid["windows"]},
        {"available": False, "reason": "x", "current_tvl_usd": 1.0},
    ]
    for forged in tampering:
        with pytest.raises(BoardViewContractError):
            _validate_hlp(forged, path="meta.hlp")

    # A window with a non-numeric metric is rejected.
    broken = _valid_state()
    broken["windows"]["allTime"]["annualized_pct"] = "big"
    with pytest.raises(BoardViewContractError):
        _validate_hlp(broken, path="meta.hlp")
