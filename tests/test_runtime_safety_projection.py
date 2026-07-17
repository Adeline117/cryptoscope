"""Public runtime safety is bounded, fail-closed, and batch-atomic."""
from __future__ import annotations

import json
from copy import deepcopy
from types import SimpleNamespace

import pytest


def _row(source, stream, *, status="live", stale=False, gaps=0, details=None,
         **extra):
    return {
        "source": source, "stream": stream, "status": status,
        "stale": stale, "open_gaps": gaps, "details": details,
        **extra,
    }


def _sources(monkeypatch, *, disk="ok", solana="live", maintenance="live",
             evm=("live", "live"), retained=True):
    from src.ops import health
    from src.pipeline import (
        board_export, evm_factory_stream, evm_launch_bridge, stream_health,
    )

    specs = (
        SimpleNamespace(chain="bsc", stream="factory-a", ws_urls=("wss://secret",)),
        SimpleNamespace(chain="base", stream="factory-b", ws_urls=("wss://secret",)),
    )
    raw = [
        _row("solana", "pump_fun_launches", status=solana,
             stale=solana != "live", token="SECRET_TOKEN"),
        _row("solana", "pump_fun_maintenance", status=maintenance,
             stale=maintenance == "stale", last_error="SECRET_ERROR"),
        _row(
            "hyperliquid", "raw_trade_retention",
            status="live" if retained else "degraded",
            details={
                "raw_trades_retained": retained, "measurement_failed": False,
                "url": "https://secret.invalid", "token": "SECRET_TOKEN",
            },
        ),
    ]
    combined = [
        _row(
            spec.chain, spec.stream, status=status,
            stale=status == "stale", coverage_verified=status == "live",
            coverage={"url": "https://secret.invalid"},
        )
        for spec, status in zip(specs, evm)
    ]
    monkeypatch.setattr(
        health, "_disk_health",
        lambda: {
            "state": disk, "path": "/SECRET/PATH", "error": "SECRET_ERROR",
            "thresholds": {"token": "SECRET_TOKEN"},
        },
    )
    monkeypatch.setattr(stream_health, "snapshot", lambda: deepcopy(raw))
    monkeypatch.setattr(evm_factory_stream, "configured_specs", lambda: specs)
    monkeypatch.setattr(
        evm_launch_bridge, "configured_stream_health", lambda: deepcopy(combined),
    )
    return board_export


def _meta(board_export, runtime):
    return board_export._envelope({
        "views": [], "view_status": {},
        "launch_protocol_join": {
            "version": 1, "state": "incomplete",
            "cross_view_edge_usable": False, "reason_codes": [], "members": {},
        },
        "runtime_safety": runtime,
        "perp_identity_policy": {
            "version": 1, "status": "invalid",
            "blocks_identity_dependent_scans": True,
            "auto_execution_allowed": False,
            "reason_codes": ["identity_cache_invalid"],
            "market_count": 0, "research_mapped": 0,
            "actionable_identity_count": 0,
            "independent_source_count": 0, "observed_path_count": 0,
            "cache_age_seconds": None, "cache_ttl_seconds": 26 * 60 * 60,
        },
        "risk_budget": board_export._risk_budget(),
        "hlp": {"available": False, "reason": "test"},
    }, view="meta")


def _validate_meta(board_export, payload):
    from src.contract.board_view import validate_board_view

    cadence, grace = board_export.VIEW_FRESHNESS["meta"]
    return validate_board_view(
        "meta", payload, cadence_min=cadence, grace_min=grace,
    )


def test_runtime_safety_healthy_projection_is_exact_and_secret_free(monkeypatch):
    board_export = _sources(monkeypatch)

    got = board_export._runtime_safety()

    assert got == {
        "version": 1, "state": "healthy", "blocks_actionability": False,
        "auto_execution_allowed": False, "storage_pressure": "ok",
        "reason_codes": [],
        "streams": {
            "solana": {
                "state": "healthy", "live": 1, "configured": 1,
                "maintenance": "healthy",
            },
            "evm": {"state": "healthy", "live": 2, "configured": 2},
        },
        "hyperliquid_raw_trade_retention": "retained",
    }
    rendered = json.dumps(got, sort_keys=True)
    assert "SECRET" not in rendered and "url" not in rendered and "path" not in rendered
    _validate_meta(board_export, _meta(board_export, got))


def test_risk_budget_projection_matches_probe_discipline_and_rejects_tampering(
        monkeypatch):
    import pytest

    from src.contract.board_view import BoardViewContractError
    from src.contract.launch_probe import (
        MAX_CONCURRENT_MANUAL_PROBES,
        MAX_PROBE_NOTIONAL_USD,
    )

    board_export = _sources(monkeypatch)
    budget = board_export._risk_budget()

    assert budget == {
        "version": 1,
        "auto_execution_allowed": False,
        "per_probe_cap_usd": MAX_PROBE_NOTIONAL_USD,
        "max_concurrent_probes": MAX_CONCURRENT_MANUAL_PROBES,
        "max_concurrent_notional_usd": (
            MAX_PROBE_NOTIONAL_USD * MAX_CONCURRENT_MANUAL_PROBES
        ),
        "basis": "manual_probe_frozen_caps_not_real_fills",
    }
    _validate_meta(board_export, _meta(board_export, board_export._runtime_safety()))

    tampering = (
        ("per_probe_cap_usd", 5000.0),
        ("max_concurrent_probes", 30),
        ("max_concurrent_notional_usd", 999999.0),
        ("auto_execution_allowed", True),
        ("basis", "real_fills"),
        ("version", 2),
    )
    for field, forged in tampering:
        meta = _meta(board_export, board_export._runtime_safety())
        meta["risk_budget"] = {**budget, field: forged}
        with pytest.raises(BoardViewContractError):
            _validate_meta(board_export, meta)

    missing = _meta(board_export, board_export._runtime_safety())
    del missing["risk_budget"]
    with pytest.raises(BoardViewContractError):
        _validate_meta(board_export, missing)


def test_runtime_safety_critical_storage_blocks(monkeypatch):
    board_export = _sources(monkeypatch, disk="critical")

    got = board_export._runtime_safety()

    assert got["state"] == "blocked" and got["blocks_actionability"] is True
    assert got["reason_codes"] == ["storage_pressure_critical"]


def test_runtime_safety_warn_evm_gap_and_trade_shedding_only_degrade(monkeypatch):
    board_export = _sources(
        monkeypatch, disk="warn", evm=("live", "degraded"), retained=False,
    )

    got = board_export._runtime_safety()

    assert got["state"] == "degraded" and got["blocks_actionability"] is False
    assert got["streams"]["evm"] == {
        "state": "degraded", "live": 1, "configured": 2,
    }
    assert got["reason_codes"] == [
        "storage_pressure_warn", "evm_streams_unhealthy",
        "hyperliquid_raw_trade_retention_shed",
    ]


def test_runtime_safety_source_failures_are_unknown_without_error_leak(
        monkeypatch):
    from src.ops import health
    from src.pipeline import (
        board_export, evm_factory_stream, evm_launch_bridge, stream_health,
    )

    def fail():
        raise OSError("SECRET_URL_TOKEN_PATH")

    monkeypatch.setattr(health, "_disk_health", fail)
    monkeypatch.setattr(stream_health, "snapshot", fail)
    monkeypatch.setattr(evm_factory_stream, "configured_specs", fail)
    monkeypatch.setattr(evm_launch_bridge, "configured_stream_health", fail)

    got = board_export._runtime_safety()

    assert got["state"] == "unknown" and got["blocks_actionability"] is True
    assert got["storage_pressure"] == "unknown"
    assert got["streams"]["solana"]["live"] is None
    assert got["streams"]["evm"]["configured"] is None
    assert got["reason_codes"] == ["runtime_health_unavailable"]
    assert "SECRET" not in json.dumps(got)


def test_runtime_retention_requires_explicit_successful_measurement(monkeypatch):
    from src.pipeline import stream_health

    board_export = _sources(monkeypatch)
    monkeypatch.setattr(stream_health, "snapshot", lambda: [
        _row("solana", "pump_fun_launches"),
        _row("solana", "pump_fun_maintenance"),
        _row(
            "hyperliquid", "raw_trade_retention",
            details={"raw_trades_retained": True},
        ),
    ])

    got = board_export._runtime_safety()

    assert got["hyperliquid_raw_trade_retention"] == "unknown"
    assert got["state"] == "unknown" and got["blocks_actionability"] is True
    assert got["reason_codes"] == ["runtime_health_unavailable"]


@pytest.mark.parametrize("mutate", [
    lambda value: value.update(path="/secret"),
    lambda value: value["streams"]["solana"].update(details={"token": "secret"}),
    lambda value: value["streams"]["evm"].update(error="secret"),
    lambda value: value.update(state="blocked"),
    lambda value: value.update(blocks_actionability=True),
    lambda value: value.update(reason_codes=["storage_pressure_warn"]),
    lambda value: value["streams"]["solana"].update(live=0),
    lambda value: value["streams"]["evm"].update(state="degraded"),
])
def test_runtime_safety_contract_rejects_extra_or_inconsistent_fields(
        monkeypatch, mutate):
    board_export = _sources(monkeypatch)
    runtime = board_export._runtime_safety()
    mutate(runtime)

    with pytest.raises(ValueError):
        _validate_meta(board_export, _meta(board_export, runtime))


@pytest.mark.parametrize("failure", ["extra", "raise"])
def test_invalid_runtime_projection_preserves_every_old_view_and_meta(
        tmp_path, monkeypatch, failure):
    board_export = _sources(monkeypatch)
    monkeypatch.setattr(board_export, "EXPORT_DIR", tmp_path)
    old = board_export._envelope({
        "events": [], "product_metadata_at": None,
        "product_metadata_time_semantics": (
            "current_inventory_metadata_not_event_time_evidence"
        ),
    }, view="structure")
    board_export.write_views(structure=old)
    before = {path.name: path.read_bytes() for path in tmp_path.glob("*.json")}
    new = deepcopy(old)
    new["generated_at"] = board_export._envelope({}, view="structure")["generated_at"]
    cadence = new["refresh_cadence_min"]
    grace = new["freshness_grace_min"]
    from datetime import datetime, timedelta
    generated = datetime.fromisoformat(new["generated_at"])
    new["next_expected_at"] = (generated + timedelta(minutes=cadence)).isoformat()
    new["stale_after_at"] = (
        generated + timedelta(minutes=cadence + grace)
    ).isoformat()
    if failure == "extra":
        invalid = board_export._runtime_safety()
        invalid["error"] = "SECRET_ERROR"
        monkeypatch.setattr(board_export, "_runtime_safety", lambda: invalid)
    else:
        monkeypatch.setattr(
            board_export, "_runtime_safety",
            lambda: (_ for _ in ()).throw(OSError("SECRET_ERROR")),
        )

    with pytest.raises((ValueError, OSError)):
        board_export.write_views(structure=new)

    assert {path.name: path.read_bytes() for path in tmp_path.glob("*.json")} == before
    assert not list(tmp_path.glob("*.tmp"))
