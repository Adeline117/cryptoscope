"""Scheduler boundaries for a verified perpetual actionable universe."""

from __future__ import annotations

import json
import threading
from datetime import datetime, timedelta, timezone

import pytest

_VERIFIED_ADDRESS = "0x" + "11" * 20


class _Logs:
    def __init__(self) -> None:
        self.rows: list[tuple[str, str, dict]] = []

    def info(self, event: str, **fields) -> None:
        self.rows.append(("info", event, fields))

    def warning(self, event: str, **fields) -> None:
        self.rows.append(("warning", event, fields))

    def debug(self, event: str, **fields) -> None:
        self.rows.append(("debug", event, fields))

    def event(self, name: str) -> tuple[str, dict]:
        level, _event, fields = next(row for row in self.rows if row[1] == name)
        return level, fields


def _universe_result(
    *,
    status: str = "research_only",
    actionable: dict | None = None,
    research: dict | None = None,
    reasons: list[str] | None = None,
    refresh_status: str | None = None,
    cache_preserved: bool | None = None,
) -> dict:
    result = {
        "status": status,
        "reason_codes": reasons if reasons is not None else [],
        "research_universe": research if research is not None else {},
        "actionable_universe": actionable if actionable is not None else {},
        "source_counts": {
            "independent_source_count": 1,
            "observed_path_count": 2,
        },
    }
    if refresh_status is not None:
        result["refresh_status"] = refresh_status
    if cache_preserved is not None:
        result["cache_preserved"] = cache_preserved
    return result


def _verified() -> dict:
    return {
        "TEST": {
            "chain": "base",
            "address": _VERIFIED_ADDRESS,
            "actionability": "verified",
        },
    }


def test_perp_universe_schedule_is_daily_and_below_cache_ttl(monkeypatch):
    from src.onchain.perp_universe import CACHE_TTL_SECONDS
    from src.pipeline import scheduler

    monkeypatch.setattr(
        scheduler,
        "load_settings",
        lambda: {"schedule": {"daily_run": "08:00"}},
    )
    configured = scheduler.create_scheduler()
    cron = configured.get_job("perp_universe_refresh").trigger
    trigger = str(cron)

    assert "hour='3'" in trigger
    assert "minute='30'" in trigger
    assert "day_of_week" not in trigger

    fires = []
    previous = None
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    for _ in range(370):
        previous = cron.get_next_fire_time(previous, now if previous is None else previous)
        fires.append(previous)
    maximum_interval = max(
        later.astimezone(timezone.utc) - earlier.astimezone(timezone.utc)
        for earlier, later in zip(fires, fires[1:])
    )
    assert maximum_interval == timedelta(hours=25)
    assert maximum_interval.total_seconds() < CACHE_TTL_SECONDS


@pytest.mark.asyncio
async def test_refresh_reports_research_actionability_and_source_counts(
    monkeypatch,
):
    from src.onchain import perp_universe
    from src.pipeline import scheduler

    logs = _Logs()
    refreshed = _universe_result(
        research={"BTC": {"actionability": "research_only"}},
        reasons=["heuristic_mapping_not_actionable"],
        refresh_status="written",
    )
    calls = []
    monkeypatch.setattr(scheduler, "logger", logs)
    monkeypatch.setattr(
        perp_universe,
        "refresh_result",
        lambda: calls.append("refresh_result") or refreshed,
    )

    result = await scheduler._run_perp_universe_refresh()

    assert calls == ["refresh_result"]
    assert result["status"] == "research_only"
    assert result["contract_valid"] is True
    assert result["research_mapped"] == 1
    assert result["actionable"] == 0
    assert result["independent_source_count"] == 1
    assert result["observed_path_count"] == 2
    level, fields = logs.event("perp_universe_refresh_written")
    assert level == "info"
    assert fields == {
        "status": "research_only",
        "refresh_status": "written",
        "reason_codes": ["heuristic_mapping_not_actionable"],
        "research_mapped": 1,
        "actionable": 0,
        "independent_source_count": 1,
        "observed_path_count": 2,
        "cache_preserved": None,
        "market_count": 0,
    }
    assert "research_universe" not in result
    assert "actionable_universe" not in result
    assert all(event != "perp_universe_done" for _level, event, _fields in logs.rows)


@pytest.mark.asyncio
async def test_usable_unchanged_refresh_is_honest_info_not_written_or_failed(
    monkeypatch,
):
    from src.onchain import perp_universe
    from src.pipeline import scheduler

    logs = _Logs()
    monkeypatch.setattr(scheduler, "logger", logs)
    monkeypatch.setattr(
        perp_universe,
        "refresh_result",
        lambda: _universe_result(
            research={"BTC": {"actionability": "research_only"}},
            reasons=["heuristic_mapping_not_actionable"],
            refresh_status="unchanged",
            cache_preserved=True,
        ),
    )

    result = await scheduler._run_perp_universe_refresh()

    assert result["contract_valid"] is True
    assert result["refresh_status"] == "unchanged"
    level, fields = logs.event("perp_universe_refresh_unchanged")
    assert level == "info"
    assert fields["status"] == "research_only"
    assert fields["cache_preserved"] is True
    assert all(
        event not in {"perp_universe_refresh_written", "perp_universe_refresh_failed"}
        for _level, event, _fields in logs.rows
    )


@pytest.mark.asyncio
async def test_stale_unchanged_refresh_is_warning_but_not_failed(monkeypatch):
    from src.onchain import perp_universe
    from src.pipeline import scheduler

    logs = _Logs()
    monkeypatch.setattr(scheduler, "logger", logs)
    monkeypatch.setattr(
        perp_universe,
        "refresh_result",
        lambda: _universe_result(
            status="stale",
            reasons=["cache_stale"],
            refresh_status="unchanged",
            cache_preserved=True,
        ),
    )

    result = await scheduler._run_perp_universe_refresh()

    assert result["contract_valid"] is True
    assert result["status"] == "stale"
    assert result["refresh_status"] == "unchanged"
    assert logs.event("perp_universe_refresh_unchanged")[0] == "warning"
    assert all(
        event not in {"perp_universe_refresh_written", "perp_universe_refresh_failed"}
        for _level, event, _fields in logs.rows
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "cache_preserved", "refresh_status"),
    [
        ("blocked", True, "unchanged"),
        ("invalid", True, "unchanged"),
        ("unavailable", True, "unchanged"),
        ("research_only", False, "unchanged"),
        ("research_only", None, "unchanged"),
        ("research_only", True, "unchanged "),
        ("research_only", True, []),
        ("research_only", True, {}),
        ("research_only", True, True),
    ],
)
async def test_malformed_unchanged_claim_is_contract_invalid_and_failed(
    monkeypatch,
    status,
    cache_preserved,
    refresh_status,
):
    from src.onchain import perp_universe
    from src.pipeline import scheduler

    logs = _Logs()
    research = (
        {"BTC": {"actionability": "research_only"}}
        if status == "research_only" else {}
    )
    monkeypatch.setattr(scheduler, "logger", logs)
    monkeypatch.setattr(
        perp_universe,
        "refresh_result",
        lambda: _universe_result(
            status=status,
            research=research,
            reasons=["cache_publish_claim_invalid"],
            refresh_status=refresh_status,
            cache_preserved=cache_preserved,
        ),
    )

    result = await scheduler._run_perp_universe_refresh()

    assert result["contract_valid"] is False
    assert result["refresh_status"] is None
    assert logs.event("perp_universe_refresh_failed")[0] == "warning"
    assert all(
        event != "perp_universe_refresh_unchanged"
        for _level, event, _fields in logs.rows
    )


@pytest.mark.asyncio
async def test_failed_refresh_is_not_logged_as_done(monkeypatch):
    from src.onchain import perp_universe
    from src.pipeline import scheduler

    logs = _Logs()
    monkeypatch.setattr(scheduler, "logger", logs)
    monkeypatch.setattr(
        perp_universe,
        "refresh_result",
        lambda: _universe_result(
            status="unavailable",
            reasons=["source_unavailable"],
            cache_preserved=True,
        ),
    )

    result = await scheduler._run_perp_universe_refresh()

    assert result["status"] == "unavailable"
    assert result["cache_preserved"] is True
    level, fields = logs.event("perp_universe_refresh_failed")
    assert level == "warning"
    assert fields["status"] == "unavailable"
    assert fields["reason_codes"] == ["source_unavailable"]
    assert fields["research_mapped"] == fields["actionable"] == 0
    assert all("done" not in event for _level, event, _fields in logs.rows)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("reason_code", "cache_preserved"),
    [
        ("cache_write_failed_before_replace", True),
        ("cache_durability_unknown_after_replace", False),
    ],
)
async def test_cache_publish_failures_remain_bounded_scheduler_warnings(
    monkeypatch,
    reason_code,
    cache_preserved,
):
    from src.onchain import perp_universe
    from src.pipeline import scheduler

    logs = _Logs()
    raw = {
        "schema_version": 2,
        "status": "unavailable",
        "reason_codes": [reason_code],
        "cache_path": "perp_universe.json",
        "cache_preserved": cache_preserved,
        "universe": {},
        "research_universe": {},
        "actionable_universe": {},
    }
    assert "refresh_status" not in raw
    monkeypatch.setattr(scheduler, "logger", logs)
    monkeypatch.setattr(perp_universe, "refresh_result", lambda: raw)

    result = await scheduler._run_perp_universe_refresh()

    assert result == {
        "contract_valid": True,
        "status": "unavailable",
        "reason_codes": [reason_code],
        "research_mapped": 0,
        "actionable": 0,
        "independent_source_count": 0,
        "observed_path_count": 0,
        "cache_preserved": cache_preserved,
        "market_count": 0,
        "refresh_status": None,
    }
    level, fields = logs.event("perp_universe_refresh_failed")
    assert level == "warning"
    assert fields == {
        "status": "unavailable",
        "refresh_status": None,
        "reason_codes": [reason_code],
        "research_mapped": 0,
        "actionable": 0,
        "independent_source_count": 0,
        "observed_path_count": 0,
        "cache_preserved": cache_preserved,
        "market_count": 0,
    }
    assert all(
        event != "perp_universe_refresh_written"
        for _level, event, _fields in logs.rows
    )


@pytest.mark.asyncio
async def test_invalid_written_refresh_is_warning_with_bounded_fields(monkeypatch):
    from src.onchain import perp_universe
    from src.pipeline import scheduler

    logs = _Logs()
    monkeypatch.setattr(scheduler, "logger", logs)
    monkeypatch.setattr(perp_universe, "refresh_result", lambda: {
        "status": "https://secret.invalid/status",
        "refresh_status": "written",
        "reason_codes": ["x" * 64] * 9,
        "research_universe": {f"T{i}": {} for i in range(1_000)},
        "actionable_universe": _verified(),
        "source_counts": {"independent_source_count": 999_999},
    })

    result = await scheduler._run_perp_universe_refresh()

    assert result == {
        "contract_valid": False,
        "status": "invalid",
        "reason_codes": ["universe_result_invalid"],
        "research_mapped": 0,
        "actionable": 0,
        "independent_source_count": 0,
        "observed_path_count": 0,
        "cache_preserved": None,
        "market_count": 0,
        "refresh_status": None,
    }
    assert logs.event("perp_universe_refresh_failed")[0] == "warning"
    assert all(
        event != "perp_universe_refresh_written" for _level, event, _fields in logs.rows
    )
    assert "secret.invalid" not in repr(logs.rows)


@pytest.mark.asyncio
async def test_valid_status_with_malformed_reason_never_logs_refresh_success(
    monkeypatch,
):
    from src.onchain import perp_universe
    from src.pipeline import scheduler

    logs = _Logs()
    monkeypatch.setattr(scheduler, "logger", logs)
    monkeypatch.setattr(
        perp_universe,
        "refresh_result",
        lambda: _universe_result(
            status="research_only",
            reasons=["https://secret.invalid/key?token=private"],
            refresh_status="written",
        ),
    )

    result = await scheduler._run_perp_universe_refresh()

    assert result["contract_valid"] is False
    assert result["status"] == "invalid"
    assert result["refresh_status"] is None
    assert logs.event("perp_universe_refresh_failed")[0] == "warning"
    assert all(
        event != "perp_universe_refresh_written" for _level, event, _fields in logs.rows
    )
    assert "secret.invalid" not in repr(logs.rows)


@pytest.mark.asyncio
async def test_refresh_network_work_runs_off_event_loop(monkeypatch):
    from src.onchain import perp_universe
    from src.pipeline import scheduler

    loop_thread = threading.get_ident()
    call_threads = []

    def refresh_result():
        call_threads.append(threading.get_ident())
        return _universe_result(
            research={"BTC": {"actionability": "research_only"}},
            reasons=["heuristic_mapping_not_actionable"],
            refresh_status="written",
        )

    monkeypatch.setattr(perp_universe, "refresh_result", refresh_result)

    result = await scheduler._run_perp_universe_refresh()

    assert result["status"] == "research_only"
    assert call_threads and call_threads[0] != loop_thread


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["research_only", "unavailable", "exception"])
async def test_cex_is_blocked_before_every_side_effect(monkeypatch, case):
    from src.distribution import telegram_sender
    from src.onchain import operator_id, perp_universe
    from src.pipeline import (
        operator_sentinel,
        outcome_tracker,
        perp_scanner,
        scheduler,
    )

    logs = _Logs()
    calls = []
    monkeypatch.setattr(scheduler, "logger", logs)
    if case == "research_only":
        value = _universe_result(
            actionable=_verified(),
            research={"BTC": {"actionability": "research_only"}},
            reasons=["heuristic_mapping_not_actionable"],
        )
        monkeypatch.setattr(perp_universe, "load_result", lambda: value)
    elif case == "unavailable":
        value = _universe_result(
            status="unavailable", reasons=["cache_missing"],
        )
        monkeypatch.setattr(perp_universe, "load_result", lambda: value)
    else:
        monkeypatch.setattr(
            perp_universe,
            "load_result",
            lambda: (_ for _ in ()).throw(
                RuntimeError("https://secret.invalid/token?key=private")
            ),
        )

    monkeypatch.setattr(
        perp_scanner,
        "scan_cex_deposits",
        lambda **_kwargs: calls.append("scan") or [],
    )
    monkeypatch.setattr(
        operator_id, "_token_market",
        lambda *_args: calls.append("market") or {},
    )
    monkeypatch.setattr(
        outcome_tracker, "log_alert",
        lambda *_args, **_kwargs: calls.append("ledger"),
    )
    monkeypatch.setattr(
        operator_sentinel, "alerts_muted",
        lambda: calls.append("mute_check") or False,
    )

    async def send_alert(_message):
        calls.append("telegram")

    monkeypatch.setattr(telegram_sender, "send_alert", send_alert)

    result = await scheduler._run_perp_cex_scan()

    assert result["status"] == "blocked"
    assert result["block_reason"] == "no_verified_actionable_universe"
    assert result["actionable"] == 0
    assert calls == []
    assert logs.event("perp_cex_scan_blocked")[0] == "warning"
    assert all(event != "perp_cex_scan_done" for _level, event, _fields in logs.rows)
    if case == "exception":
        level, fields = logs.event("perp_universe_runtime_load_failed")
        assert level == "warning"
        assert fields == {"error_kind": "RuntimeError"}
        assert "secret.invalid" not in repr(logs.rows)


@pytest.mark.asyncio
async def test_spoofed_verified_envelope_is_blocked_as_invalid(monkeypatch):
    from src.onchain import perp_universe
    from src.pipeline import perp_scanner, scheduler

    calls = []
    monkeypatch.setattr(
        perp_universe,
        "load_result",
        lambda: _universe_result(
            status="verified",
            actionable={
                "TEST": {
                    "chain": "base",
                    "address": _VERIFIED_ADDRESS,
                    "actionability": "research_only",
                },
            },
            reasons=["verified_registry_loaded"],
        ),
    )
    monkeypatch.setattr(
        perp_scanner,
        "scan_cex_deposits",
        lambda **_kwargs: calls.append("scan") or [],
    )

    result = await scheduler._run_perp_cex_scan()

    assert result["status"] == "blocked"
    assert result["universe_contract_valid"] is False
    assert result["universe_status"] == "invalid"
    assert result["universe_reason_codes"] == ["universe_result_invalid"]
    assert result["actionable"] == 0
    assert calls == []


@pytest.mark.asyncio
async def test_mobilization_blocked_keeps_state_bytes_unchanged(
    tmp_path,
    monkeypatch,
):
    from src import config
    from src.distribution import telegram_sender
    from src.onchain import operator_id, perp_universe
    from src.pipeline import (
        operator_sentinel,
        outcome_tracker,
        perp_mobilization,
        scheduler,
    )

    logs = _Logs()
    calls = []
    state_file = tmp_path / "perp_mobilization_state.json"
    original = b'{"mobil":{"base:0xold":{"mobil_block":7}},"lp":{}}\n'
    state_file.write_bytes(original)
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)
    monkeypatch.setattr(scheduler, "logger", logs)
    monkeypatch.setattr(
        perp_universe,
        "load_result",
        lambda: _universe_result(
            research={"BTC": {"actionability": "research_only"}},
            reasons=["heuristic_mapping_not_actionable"],
        ),
    )
    monkeypatch.setattr(
        perp_mobilization, "scan_mobilization",
        lambda **_kwargs: calls.append("mobil_scan") or ([], {}),
    )
    monkeypatch.setattr(
        perp_mobilization, "scan_lp_unlock",
        lambda **_kwargs: calls.append("lp_scan") or ([], {}),
    )
    monkeypatch.setattr(
        operator_id, "_token_market",
        lambda *_args: calls.append("market") or {},
    )
    monkeypatch.setattr(
        outcome_tracker, "log_alert",
        lambda *_args, **_kwargs: calls.append("ledger"),
    )
    monkeypatch.setattr(
        operator_sentinel, "alerts_muted",
        lambda: calls.append("mute_check") or False,
    )

    async def send_alert(_message):
        calls.append("telegram")

    monkeypatch.setattr(telegram_sender, "send_alert", send_alert)

    result = await scheduler._run_perp_mobilization()

    assert result["status"] == "blocked"
    assert calls == []
    assert state_file.read_bytes() == original
    assert logs.event("perp_mobilization_blocked")[0] == "warning"
    assert all(
        event != "perp_mobilization_done" for _level, event, _fields in logs.rows
    )


@pytest.mark.asyncio
async def test_mobilization_ready_injects_one_snapshot_before_state_write(
    tmp_path,
    monkeypatch,
):
    from src import config
    from src.onchain import perp_universe
    from src.pipeline import perp_mobilization, scheduler

    verified = _verified()
    calls = []
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)
    monkeypatch.setattr(
        perp_universe,
        "load_result",
        lambda: _universe_result(
            status="verified",
            actionable=verified,
            research={"TEST": {"actionability": "research_only"}},
            reasons=[],
        ),
    )

    def mobil(*, prev_state, verified_universe):
        calls.append(("mobil", verified_universe))
        return [], dict(prev_state or {})

    def lp(*, prev_state, verified_universe):
        calls.append(("lp", verified_universe))
        return [], dict(prev_state or {})

    monkeypatch.setattr(perp_mobilization, "scan_mobilization", mobil)
    monkeypatch.setattr(perp_mobilization, "scan_lp_unlock", lp)

    result = await scheduler._run_perp_mobilization()

    assert result["status"] == "complete"
    assert result["actionable"] == 1
    assert calls == [("mobil", verified), ("lp", verified)]
    state = json.loads((tmp_path / "perp_mobilization_state.json").read_text())
    assert state == {"mobil": {}, "lp": {}}


@pytest.mark.asyncio
async def test_cex_ready_injects_the_verified_snapshot(monkeypatch):
    from src.onchain import perp_universe
    from src.pipeline import perp_scanner, scheduler

    verified = _verified()
    injected = []
    monkeypatch.setattr(
        perp_universe,
        "load_result",
        lambda: _universe_result(
            status="verified",
            actionable=verified,
            reasons=[],
        ),
    )

    def scan_cex_deposits(*, verified_universe):
        injected.append(verified_universe)
        return []

    monkeypatch.setattr(perp_scanner, "scan_cex_deposits", scan_cex_deposits)

    result = await scheduler._run_perp_cex_scan()

    assert result["status"] == "complete"
    assert result["actionable"] == 1
    assert injected == [verified]


def test_explicit_empty_scanner_snapshots_never_fall_back_to_load(monkeypatch):
    from src.onchain import cex_addresses, perp_universe
    from src.pipeline import perp_mobilization, perp_scanner

    monkeypatch.setattr(
        perp_universe,
        "load",
        lambda: pytest.fail("explicit empty snapshot must not call load()"),
    )
    monkeypatch.setattr(cex_addresses, "evm_exchanges", lambda: {})

    assert perp_scanner.scan_cex_deposits(verified_universe={}) == []
    assert perp_mobilization.scan_mobilization(
        prev_state={"keep": {"cursor": 7}},
        verified_universe={},
    ) == ([], {"keep": {"cursor": 7}})
    assert perp_mobilization.scan_lp_unlock(
        prev_state={"keep": {"locked": True}},
        verified_universe={},
    ) == ([], {"keep": {"locked": True}})


@pytest.mark.parametrize(
    "bad_row",
    [
        {
            "chain": "base",
            "address": "abc",
            "actionability": "verified",
        },
        {
            "chain": "base",
            "address": "0x" + "g" * 40,
            "actionability": "verified",
        },
        {
            "chain": "solana",
            "address": "111",
            "actionability": "verified",
        },
        {
            "chain": "solana",
            "address": "0" * 44,
            "actionability": "verified",
        },
    ],
)
def test_scanners_reject_one_malformed_injected_row_as_a_whole(
    monkeypatch,
    bad_row,
):
    from src.onchain import cex_addresses
    from src.pipeline import perp_mobilization, perp_scanner

    calls = []
    mixed = {
        **_verified(),
        "BAD": bad_row,
    }
    monkeypatch.setattr(
        cex_addresses,
        "evm_exchanges",
        lambda: calls.append("cex") or {},
    )
    monkeypatch.setattr(
        perp_mobilization,
        "_whales",
        lambda *_args: calls.append("whales") or ([], True),
    )

    assert perp_scanner.scan_cex_deposits(verified_universe=mixed) == []
    assert perp_mobilization.scan_mobilization(
        prev_state={"keep": {"cursor": 7}},
        verified_universe=mixed,
    ) == ([], {"keep": {"cursor": 7}})
    assert perp_mobilization.scan_lp_unlock(
        prev_state={"keep": {"locked": True}},
        verified_universe=mixed,
    ) == ([], {"keep": {"locked": True}})
    assert calls == []


def test_verified_universe_accepts_chain_valid_evm_and_solana_addresses():
    from src.pipeline.perp_scanner import validated_verified_universe

    universe = {
        **_verified(),
        "SOL": {
            "chain": "solana",
            "address": "1" * 32,
            "actionability": "verified",
        },
    }

    assert validated_verified_universe(universe) == universe


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "research",
    [
        {},
        {"BTC": {"actionability": "verified"}},
    ],
)
async def test_malformed_research_rows_never_log_written_success(
    monkeypatch,
    research,
):
    from src.onchain import perp_universe
    from src.pipeline import scheduler

    logs = _Logs()
    monkeypatch.setattr(scheduler, "logger", logs)
    monkeypatch.setattr(
        perp_universe,
        "refresh_result",
        lambda: _universe_result(
            status="research_only",
            research=research,
            reasons=["heuristic_mapping_not_actionable"],
            refresh_status="written",
        ),
    )

    result = await scheduler._run_perp_universe_refresh()

    assert result["contract_valid"] is False
    assert result["status"] == "invalid"
    assert result["refresh_status"] is None
    assert logs.event("perp_universe_refresh_failed")[0] == "warning"
    assert all(
        event != "perp_universe_refresh_written" for _level, event, _fields in logs.rows
    )


@pytest.mark.asyncio
async def test_verified_cex_market_failure_logs_kind_without_exception_text(
    monkeypatch,
):
    from src.onchain import operator_id, perp_universe
    from src.pipeline import operator_sentinel, perp_scanner, scheduler

    logs = _Logs()
    secret = "https://secret.invalid/token?key=private"
    monkeypatch.setattr(scheduler, "logger", logs)
    monkeypatch.setattr(
        perp_universe,
        "load_result",
        lambda: _universe_result(
            status="verified", actionable=_verified(), reasons=[]
        ),
    )
    monkeypatch.setattr(
        perp_scanner,
        "scan_cex_deposits",
        lambda **_kwargs: [{
            "address": _VERIFIED_ADDRESS,
            "chain": "base",
            "symbol": "TEST",
            "cex_outflow": 1,
            "pct_of_cluster": 1,
        }],
    )
    monkeypatch.setattr(
        operator_id,
        "_token_market",
        lambda *_args: (_ for _ in ()).throw(RuntimeError(secret)),
    )
    monkeypatch.setattr(operator_sentinel, "alerts_muted", lambda: True)

    result = await scheduler._run_perp_cex_scan()

    assert result["status"] == "complete"
    assert result["logged"] == 0
    level, fields = logs.event("perp_event_log_failed")
    assert level == "warning"
    assert fields == {
        "symbol": "TEST",
        "reason_code": "event_persistence_failed",
        "error_kind": "RuntimeError",
    }
    assert "secret.invalid" not in repr(logs.rows)


@pytest.mark.asyncio
async def test_holder_snapshots_keep_sentinels_and_mark_perp_blocked(
    tmp_path,
    monkeypatch,
):
    from src import config
    from src.onchain import holder_snapshot, perp_universe
    from src.pipeline import scheduler

    logs = _Logs()
    registry = {
        "sentinel": {"token": "0xSentinel", "chain": "base"},
    }
    (tmp_path / "operator_sentinels.json").write_text(json.dumps(registry))
    fetched = []
    saved = []
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)
    monkeypatch.setattr(scheduler, "logger", logs)
    monkeypatch.setattr(
        perp_universe,
        "load_result",
        lambda: _universe_result(
            research={
                "BTC": {
                    "chain": "ethereum",
                    "address": _VERIFIED_ADDRESS,
                    "actionability": "research_only",
                },
            },
            reasons=["heuristic_mapping_not_actionable"],
        ),
    )

    def fetch(token, *, chain_id, max_pages):
        fetched.append((token, chain_id, max_pages))
        return [{"address": "0xholder", "balance": 1}]

    def save(token, chain, holders):
        saved.append((token, chain, holders))

    monkeypatch.setattr(holder_snapshot, "fetch_holders_evm", fetch)
    monkeypatch.setattr(holder_snapshot, "save_snapshot", save)

    result = await scheduler._run_holder_snapshots()

    assert fetched == [("0xsentinel", 8453, 4)]
    assert len(saved) == 1 and saved[0][:2] == ("0xsentinel", "base")
    assert result["status"] == "partial"
    assert result["perp_status"] == "blocked"
    assert result["sentinel_targets"] == 1
    assert result["perp_targets"] == 0
    assert result["research_mapped"] == 1
    assert logs.event("holder_snapshots_perp_blocked")[0] == "warning"
    _level, done = logs.event("holder_snapshots_done")
    assert done["perp_status"] == "blocked"
    assert done["saved"] == 1
