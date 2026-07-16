"""Public perp identity policy is bounded, scoped, and fail-closed."""
from __future__ import annotations

import json
from copy import deepcopy
from datetime import datetime, timedelta, timezone

import pytest


NOW = datetime(2026, 7, 16, 12, 0, tzinfo=timezone.utc)
TTL_SECONDS = 26 * 60 * 60


def _result(
    *,
    status: str = "research_only",
    reasons: list | None = None,
    research: dict | None = None,
    actionable: dict | None = None,
    generated_at: datetime | None = None,
    market_count: int = 408,
    independent: int = 1,
    observed_paths: int = 2,
) -> dict:
    generated = generated_at or NOW - timedelta(minutes=5)
    return {
        "schema_version": 2,
        "status": status,
        "reason_codes": (
            ["heuristic_mapping_not_actionable"]
            if reasons is None and status == "research_only"
            else [] if reasons is None else reasons
        ),
        "generated_at": generated.isoformat(),
        "expires_at": (generated + timedelta(seconds=TTL_SECONDS)).isoformat(),
        "market_count": market_count,
        "source_counts": {
            "independent_source_count": independent,
            "observed_path_count": observed_paths,
        },
        "research_universe": research if research is not None else {
            "BTC": {
                "actionability": "research_only",
                "address": "SECRET_RESEARCH_ADDRESS",
                "url": "https://secret.invalid/research",
            },
        },
        "actionable_universe": actionable if actionable is not None else {},
        # These are deliberately hostile public fields.  Projection must ignore all.
        "cache_path": "/SECRET/CACHE/PATH",
        "universe": {"SECRET_SYMBOL": {"token": "SECRET_TOKEN"}},
        "mapping_source": {"url": "https://secret.invalid/raw"},
    }


def _verified_result() -> dict:
    return _result(
        status="verified",
        reasons=[],
        research={},
        actionable={
            "TEST": {
                "chain": "base",
                "address": "0x" + "11" * 20,
                "actionability": "verified",
                "url": "https://secret.invalid/actionable",
            },
        },
        market_count=10,
        independent=1,
        observed_paths=1,
    )


def _runtime() -> dict:
    return {
        "version": 1,
        "state": "healthy",
        "blocks_actionability": False,
        "auto_execution_allowed": False,
        "storage_pressure": "ok",
        "reason_codes": [],
        "streams": {
            "solana": {
                "state": "healthy", "live": 1, "configured": 1,
                "maintenance": "healthy",
            },
            "evm": {"state": "healthy", "live": 1, "configured": 1},
        },
        "hyperliquid_raw_trade_retention": "retained",
    }


def _invalid_policy() -> dict:
    return {
        "version": 1,
        "status": "invalid",
        "blocks_identity_dependent_scans": True,
        "auto_execution_allowed": False,
        "reason_codes": ["identity_cache_invalid"],
        "market_count": 0,
        "research_mapped": 0,
        "actionable_identity_count": 0,
        "independent_source_count": 0,
        "observed_path_count": 0,
        "cache_age_seconds": None,
        "cache_ttl_seconds": TTL_SECONDS,
    }


def _research_policy() -> dict:
    return {
        "version": 1,
        "status": "research_only",
        "blocks_identity_dependent_scans": True,
        "auto_execution_allowed": False,
        "reason_codes": ["heuristic_mapping_not_actionable"],
        "market_count": 408,
        "research_mapped": 2,
        "actionable_identity_count": 0,
        "independent_source_count": 1,
        "observed_path_count": 2,
        "cache_age_seconds": 300,
        "cache_ttl_seconds": TTL_SECONDS,
    }


def _meta(board_export, policy: dict) -> dict:
    return board_export._envelope({
        "views": [],
        "view_status": {},
        "launch_protocol_join": {
            "version": 1,
            "state": "incomplete",
            "cross_view_edge_usable": False,
            "reason_codes": [],
            "members": {},
        },
        "runtime_safety": _runtime(),
        "perp_identity_policy": policy,
    }, view="meta")


def _validate_meta(board_export, policy: dict) -> dict:
    from src.contract.board_view import validate_board_view

    cadence, grace = board_export.VIEW_FRESHNESS["meta"]
    return validate_board_view(
        "meta", _meta(board_export, policy),
        cadence_min=cadence, grace_min=grace,
    )


def test_research_projection_reads_once_and_exposes_only_bounded_counts(monkeypatch):
    from src.onchain import perp_universe
    from src.pipeline import board_export

    calls = []
    raw = _result(research={
        "BTC": {"actionability": "research_only", "address": "SECRET_ADDRESS"},
        "ETH": {
            "actionability": "research_only",
            "url": "https://secret.invalid/token",
        },
    })

    def load_result(*, _now):
        calls.append(_now)
        return raw

    monkeypatch.setattr(perp_universe, "load_result", load_result)

    got = board_export._perp_identity_policy(_now=NOW)

    assert calls == [NOW]
    assert got == _research_policy()
    rendered = json.dumps(got, sort_keys=True)
    for secret in (
        "SECRET", "BTC", "ETH", "address", "symbol", "cache_path",
        "universe", "url", "secret.invalid",
    ):
        assert secret not in rendered
    _validate_meta(board_export, got)


def test_verified_projection_unblocks_only_identity_dependent_scans(monkeypatch):
    from src.onchain import perp_universe
    from src.pipeline import board_export

    monkeypatch.setattr(
        perp_universe, "load_result", lambda *, _now: _verified_result(),
    )

    got = board_export._perp_identity_policy(_now=NOW)

    assert got == {
        "version": 1,
        "status": "verified",
        "blocks_identity_dependent_scans": False,
        "auto_execution_allowed": False,
        "reason_codes": [],
        "market_count": 10,
        "research_mapped": 0,
        "actionable_identity_count": 1,
        "independent_source_count": 1,
        "observed_path_count": 1,
        "cache_age_seconds": 300,
        "cache_ttl_seconds": TTL_SECONDS,
    }
    assert "TEST" not in json.dumps(got)
    assert "secret.invalid" not in json.dumps(got)
    _validate_meta(board_export, got)


@pytest.mark.parametrize(
    ("status", "expected_reason"),
    [
        ("blocked", "identity_collection_blocked"),
        ("invalid", "identity_cache_invalid"),
        ("stale", "identity_cache_stale"),
        ("unavailable", "identity_cache_unavailable"),
    ],
)
def test_nonusable_statuses_are_categorized_without_raw_reason_leak(
    monkeypatch, status, expected_reason,
):
    from src.onchain import perp_universe
    from src.pipeline import board_export

    raw = _result(
        status=status, reasons=["secret_token"], research={}, actionable={},
    )
    raw.pop("generated_at")
    raw.pop("expires_at")
    raw.pop("market_count")
    raw.pop("source_counts")
    monkeypatch.setattr(perp_universe, "load_result", lambda *, _now: raw)

    got = board_export._perp_identity_policy(_now=NOW)

    assert got == {
        **_invalid_policy(),
        "status": status,
        "reason_codes": [expected_reason],
    }
    rendered = json.dumps(got)
    assert "secret_token" not in rendered
    assert "SECRET" not in rendered
    _validate_meta(board_export, got)


@pytest.mark.parametrize(
    "raw",
    [
        None,
        {"status": "research_only", "reason_codes": [["unhashable"]]},
        _result(reasons=["https://secret.invalid/reason"]),
        _result(research={"btc": {"actionability": "research_only"}}),
        _result(
            research={"BTC": {"actionability": "research_only"}},
            actionable={
                "TEST": {
                    "chain": "base", "address": "0x" + "11" * 20,
                    "actionability": "verified",
                },
            },
        ),
        _result(
            status="verified",
            reasons=[],
            research={"BTC": {"actionability": "research_only"}},
            actionable={
                "TEST": {
                    "chain": "base", "address": "0x" + "11" * 20,
                    "actionability": "verified",
                },
            },
            market_count=10,
        ),
        _result(
            research={
                "BTC": {"actionability": "research_only"},
                "ETH": {"actionability": "research_only"},
            },
            market_count=1,
        ),
        _result(independent=2, observed_paths=1),
        _result(generated_at=NOW + timedelta(seconds=1)),
        _result(generated_at=NOW - timedelta(seconds=TTL_SECONDS)),
    ],
)
def test_malformed_or_contradictory_results_fail_closed_without_leak(
    monkeypatch, raw,
):
    from src.onchain import perp_universe
    from src.pipeline import board_export

    monkeypatch.setattr(perp_universe, "load_result", lambda *, _now: raw)

    got = board_export._perp_identity_policy(_now=NOW)

    assert got == {
        **_invalid_policy(),
        "reason_codes": ["identity_projection_invalid"],
    }
    assert "secret.invalid" not in json.dumps(got)
    _validate_meta(board_export, got)


def test_load_exception_is_contained_without_exception_text(monkeypatch):
    from src.onchain import perp_universe
    from src.pipeline import board_export

    def fail(*, _now):
        raise OSError("https://secret.invalid/TOKEN /SECRET/PATH")

    monkeypatch.setattr(perp_universe, "load_result", fail)

    got = board_export._perp_identity_policy(_now=NOW)

    assert got == {
        **_invalid_policy(),
        "status": "unavailable",
        "reason_codes": ["identity_load_failed"],
    }
    assert "SECRET" not in json.dumps(got)
    assert "secret.invalid" not in json.dumps(got)
    _validate_meta(board_export, got)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda value: value.update(cache_path="/secret"),
        lambda value: value.update(blocks_identity_dependent_scans=False),
        lambda value: value.update(actionable_identity_count=1),
        lambda value: value.update(cache_age_seconds=None),
        lambda value: value.update(cache_age_seconds=TTL_SECONDS),
        lambda value: value.update(market_count=1),
        lambda value: value.update(
            independent_source_count=2, observed_path_count=1,
        ),
        lambda value: value.update(reason_codes=["identity_cache_stale"]),
        lambda value: value.update(reason_codes=[["unhashable"]]),
        lambda value: value.update(status=[]),
        lambda value: value.update(auto_execution_allowed=True),
        lambda value: value.update(research_mapped=True),
    ],
)
def test_meta_contract_rejects_extra_or_inconsistent_policy_fields(mutate):
    from src.pipeline import board_export

    policy = _research_policy()
    mutate(policy)

    with pytest.raises(ValueError):
        _validate_meta(board_export, policy)


def test_meta_contract_requires_identity_policy():
    from src.contract.board_view import validate_board_view
    from src.pipeline import board_export

    payload = _meta(board_export, _invalid_policy())
    payload.pop("perp_identity_policy")
    cadence, grace = board_export.VIEW_FRESHNESS["meta"]

    with pytest.raises(ValueError):
        validate_board_view(
            "meta", payload, cadence_min=cadence, grace_min=grace,
        )


def test_meta_contract_rejects_verified_status_with_research_rows():
    from src.pipeline import board_export

    policy = {
        "version": 1,
        "status": "verified",
        "blocks_identity_dependent_scans": False,
        "auto_execution_allowed": False,
        "reason_codes": [],
        "market_count": 10,
        "research_mapped": 1,
        "actionable_identity_count": 1,
        "independent_source_count": 1,
        "observed_path_count": 1,
        "cache_age_seconds": 300,
        "cache_ttl_seconds": TTL_SECONDS,
    }

    with pytest.raises(ValueError):
        _validate_meta(board_export, policy)


def test_write_views_reads_cache_once_and_publishes_policy(tmp_path, monkeypatch):
    from src.onchain import perp_universe
    from src.pipeline import board_export

    calls = []

    def load_result(*, _now):
        calls.append(_now)
        return _result(research={
            "BTC": {"actionability": "research_only"},
            "ETH": {"actionability": "research_only"},
        }, generated_at=_now - timedelta(minutes=5))

    monkeypatch.setattr(perp_universe, "load_result", load_result)
    monkeypatch.setattr(board_export, "EXPORT_DIR", tmp_path)
    monkeypatch.setattr(board_export, "_runtime_safety", _runtime)
    structure = board_export._envelope({
        "events": [],
        "product_metadata_at": None,
        "product_metadata_time_semantics": (
            "current_inventory_metadata_not_event_time_evidence"
        ),
    }, view="structure")

    paths = board_export.write_views(structure=structure)

    assert len(calls) == 1
    assert {path.name for path in paths} == {"structure.json", "meta.json"}
    meta = json.loads((tmp_path / "meta.json").read_text())
    assert meta["perp_identity_policy"] == _research_policy()
    assert meta["runtime_safety"]["blocks_actionability"] is False


def test_invalid_policy_contract_preserves_old_view_and_meta(tmp_path, monkeypatch):
    from src.pipeline import board_export

    monkeypatch.setattr(board_export, "EXPORT_DIR", tmp_path)
    monkeypatch.setattr(board_export, "_runtime_safety", _runtime)
    monkeypatch.setattr(board_export, "_perp_identity_policy", _invalid_policy)
    old = board_export._envelope({
        "events": [],
        "product_metadata_at": None,
        "product_metadata_time_semantics": (
            "current_inventory_metadata_not_event_time_evidence"
        ),
    }, view="structure")
    board_export.write_views(structure=old)
    before = {path.name: path.read_bytes() for path in tmp_path.glob("*.json")}

    invalid = _invalid_policy()
    invalid["raw_universe"] = {"SECRET": "SECRET_ADDRESS"}
    monkeypatch.setattr(board_export, "_perp_identity_policy", lambda: invalid)
    new = deepcopy(old)
    refreshed = board_export._envelope({}, view="structure")
    for key in (
        "generated_at", "next_expected_at", "stale_after_at",
        "refresh_cadence_min", "freshness_grace_min",
    ):
        new[key] = refreshed[key]

    with pytest.raises(ValueError):
        board_export.write_views(structure=new)

    assert {path.name: path.read_bytes() for path in tmp_path.glob("*.json")} == before
    assert not list(tmp_path.glob("*.tmp"))
