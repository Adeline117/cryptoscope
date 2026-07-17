"""Browser-side Launch/Stats protocol joins fail closed without hiding live events."""
from __future__ import annotations

import json
import subprocess
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse

import pytest

from src.pipeline.validation_overview import build_validation_overview
from tests.test_validation_overview import _lanes as _validation_lanes


ROOT = Path(__file__).parents[1]
BOARD = ROOT / "board" / "public" / "index.html"
JOIN = ROOT / "board" / "public" / "protocol-join.js"
CHARTS = ROOT / "board" / "public" / "vendor" / "lightweight-charts-5.2.0.js"
IDENTITY = {
    "protocol_id": "launch-forward-spa-v3",
    "cohort_version": 6,
    "protocol_start_at": "2026-08-03T00:00:00+00:00",
}
SAFETY_FIELDS = (
    "state", "enrollment_open", "armed_at", "opened_at", "breached_at",
    "auto_execution_allowed",
)


def _healthy_runtime() -> dict:
    return {
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


def _carry_contract() -> tuple[dict, dict]:
    episode = {"symbol": "CARRY-CONTRACT-UNAFFECTED"}
    paper = {
        "cohort_kind": "descriptive_quote_proxy",
        "n_open": 1, "n_closed": 1, "n_proxy_closed": 1,
        "real_edge_n": 0, "n_exit_pending": 0,
        "n_quarantined_total": 0, "n_closed_total": 2,
        "n_closed_excluded": 1,
        "excluded_by_reason": {"legacy_episode": 1},
        "cost_completeness": "partial", "all_in_total_pct": None,
        "is_real_fill": False, "real_edge_eligible": False,
        "open_positions": [episode], "recent": [episode],
    }
    evidence = {
        "n": 1, "n_proxy": 1, "hits": 1, "real_edge_n": 0,
        "total_closed": 2, "excluded_closed": 1,
        "excluded_by_reason": {"legacy_episode": 1}, "pending": 0,
        "metric": "quote_rate_integral_minus_book_quotes_and_modeled_fee_proxy",
        "cohort_kind": "descriptive_quote_proxy",
        "cost_completeness": "partial", "all_in_total_pct": None,
        "cost_is_real_fill": False,
        "execution_mode": "paper_orderbook_measurement",
        "real_edge_eligible": False,
        "verdict": "不可判", "edge_verdict": "不可判",
    }
    return paper, evidence


def _perps() -> dict:
    paper, _ = _carry_contract()
    return {
        "schema_version": 1, "perps": [], "carry": [],
        "cascade_events": [], "carry_paper": paper,
    }


def _admission(state: str, updated_at: str) -> dict:
    return {
        **IDENTITY,
        "state": state,
        "enrollment_open": state == "open",
        "armed_at": updated_at if state in {"armed", "open", "breached"} else None,
        "opened_at": updated_at if state in {"open", "breached"} else None,
        "breached_at": updated_at if state == "breached" else None,
        "auto_execution_allowed": False,
        "updated_at": updated_at,
    }


def _launch(generated_at: str, admission: dict, symbol: str = "LIVE-EVENT") -> dict:
    stale = (datetime.fromisoformat(generated_at) + timedelta(hours=1)).isoformat()
    return {
        "schema_version": 1,
        "generated_at": generated_at,
        "stale_after_at": stale,
        "research_protocol": {
            **IDENTITY,
            "enrollment_state": admission["state"],
            "persistent_admission_state": admission["state"],
            "enrollment_open": admission["enrollment_open"],
            "reason_codes": ["protocol_admission_not_open"],
            "source_readiness_state": "blocked",
            "sample_kind": "forward_paper_selector",
            "selection_stage": "discovery_rule_before_security_and_route",
            "real_edge_n": 0,
            "real_edge_eligible": False,
            "execution_edge_eligible": False,
            "auto_execution_allowed": False,
        },
        "primary_sources": {"solana": {
            "available": False,
            "streams": [],
            "qualification": {},
            "source_readiness": {
                "state": "blocked", "ready": False,
                "reason_codes": ["live_stream_health_not_ready"],
            },
            "protocol_admission": admission,
        }},
        "events": [{
            "id": f"event-{symbol}", "symbol": symbol, "chain": "solana",
            "token": f"mint-{symbol}", "action_level": "A1_WATCH",
            "decision": "WATCH", "recorded_decision": "WATCH",
            "effective_decision": "WATCH", "max_notional_usd": 50,
        }],
    }


def _stats(generated_at: str, admission: dict, edge: str = "LAUNCH-EDGE") -> dict:
    validation_lanes = _validation_lanes()
    overview = build_validation_overview(validation_lanes)
    launch_row = next(row for row in overview["rows"] if row["lane"] == "launch")
    launch_row["result"]["summary"] = edge
    return {
        "schema_version": 1,
        "generated_at": generated_at,
        "stale_after_at": (datetime.fromisoformat(generated_at) + timedelta(hours=2)).isoformat(),
        "lanes": {
            "launch": {
                "verdict": "不可判", "edge_verdict": edge, "n": 17,
                "edge_validation": {
                    **IDENTITY, "protocol_admission": admission,
                    "state": "collecting", "eligible_n": {"SMALL_PROBE": 7, "WATCH": 9},
                    "look_n_per_arm": 100,
                },
            },
            "carry": validation_lanes["carry"],
        },
        "validation_overview": overview,
    }


def _member(view: str, payload: dict, admission: dict) -> dict:
    return {
        "view": view,
        "generated_at": payload["generated_at"],
        "identity": deepcopy(IDENTITY),
        "admission_updated_at": admission["updated_at"],
        "admission": {field: admission[field] for field in SAFETY_FIELDS},
    }


def _meta(launch: dict, stats: dict, launch_admission: dict, stats_admission: dict) -> dict:
    return {
        "schema_version": 1,
        "runtime_safety": _healthy_runtime(),
        "risk_budget": {
            "version": 1,
            "auto_execution_allowed": False,
            "per_probe_cap_usd": 500.0,
            "max_concurrent_probes": 3,
            "max_concurrent_notional_usd": 1500.0,
            "basis": "manual_probe_frozen_caps_not_real_fills",
        },
        "launch_protocol_join": {
            "version": 1, "state": "consistent", "cross_view_edge_usable": True,
            "reason_codes": [],
            "members": {
                "launch": _member("launch", launch, launch_admission),
                "stats": _member("stats", stats, stats_admission),
            },
        },
    }


def _evaluate(launch: dict, stats: dict | None, meta: dict | None) -> dict:
    script = (
        "const guard=require(process.argv[1]);"
        "const input=JSON.parse(process.argv[2]);"
        "process.stdout.write(JSON.stringify(guard.evaluate(input.launch,input.stats,input.meta)));"
    )
    result = subprocess.run(
        ["node", "-e", script, str(JOIN), json.dumps({
            "launch": launch, "stats": stats, "meta": meta,
        })],
        check=True, capture_output=True, text=True,
    )
    return json.loads(result.stdout)


def test_pure_join_requires_meta_to_bind_both_actual_payload_clocks():
    clock = "2026-07-16T00:00:00+00:00"
    admission = _admission("scheduled", clock)
    old_launch, stats = _launch(clock, admission), _stats(clock, admission)
    meta = _meta(old_launch, stats, admission, admission)

    assert _evaluate(old_launch, stats, meta)["edgeUsable"] is True

    new_launch = _launch("2026-07-16T00:01:00+00:00", admission)
    joined = _evaluate(new_launch, stats, meta)
    assert joined["state"] == "meta_unbound"
    assert joined["edgeUsable"] is False
    assert joined["actionBlock"] is False


@pytest.mark.parametrize(
    ("launch_state", "launch_clock", "stats_state", "stats_clock", "blocked"),
    [
        ("open", "2026-07-16T00:00:00+00:00", "breached", "2026-07-16T00:01:00+00:00", True),
        ("open", "2026-07-16T00:00:00+00:00", "armed", "2026-07-16T00:01:00+00:00", True),
        ("open", "2026-07-16T00:00:00+00:00", "open", "2026-07-16T00:01:00+00:00", True),
        ("armed", "2026-08-02T23:58:00+00:00", "scheduled", "2026-08-02T23:59:00+00:00", False),
        ("armed", "2026-08-02T23:59:00+00:00", "scheduled", "2026-08-03T00:00:00+00:00", True),
        ("breached", "2026-08-03T00:00:00+00:00", "open", "2026-08-03T00:01:00+00:00", True),
        ("open", "2026-07-16T00:02:00+00:00", "breached", "2026-07-16T00:01:00+00:00", True),
        ("open", "2026-07-16T00:00:00+00:00", "breached", "2026-07-16T00:00:00+00:00", True),
    ],
)
def test_a3_blocks_any_temporally_impossible_or_breached_cross_view_transition(
        launch_state, launch_clock, stats_state, stats_clock, blocked):
    launch_admission = _admission(launch_state, launch_clock)
    stats_admission = _admission(stats_state, stats_clock)
    launch = _launch(launch_clock, launch_admission)
    stats = _stats(stats_clock, stats_admission)

    assert _evaluate(launch, stats, None)["actionBlock"] is blocked


@pytest.mark.parametrize(
    ("newer_clock", "expected_state"),
    [
        ("2026-08-02T23:59:00+00:00", "sync_pending"),
        ("2026-08-03T00:00:00+00:00", "contradiction"),
    ],
)
def test_join_distinguishes_prestart_readiness_reset_from_unsafe_regression(
        newer_clock, expected_state):
    older_clock = "2026-08-02T23:58:00+00:00"
    launch_admission = _admission("armed", older_clock)
    stats_admission = _admission("scheduled", newer_clock)

    joined = _evaluate(
        _launch(older_clock, launch_admission),
        _stats(newer_clock, stats_admission),
        None,
    )

    assert joined["state"] == expected_state
    assert joined["edgeUsable"] is False
    assert joined["actionBlock"] is (expected_state == "contradiction")


def test_missing_or_old_identity_stats_quarantines_edge_without_dragging_a3():
    clock = "2026-07-16T00:00:00+00:00"
    admission = _admission("open", clock)
    launch = _launch(clock, admission)
    missing = _evaluate(launch, None, None)
    assert missing["edgeUsable"] is False and missing["actionBlock"] is False

    stats = _stats(clock, deepcopy(admission))
    stats["lanes"]["launch"]["edge_validation"].update({
        "protocol_id": "old-protocol", "cohort_version": 5,
    })
    stats["lanes"]["launch"]["edge_validation"]["protocol_admission"].update({
        "protocol_id": "old-protocol", "cohort_version": 5,
    })
    mismatch = _evaluate(launch, stats, None)
    assert mismatch["state"] == "identity_mismatch"
    assert mismatch["edgeUsable"] is False and mismatch["actionBlock"] is False


def test_projection_rejects_enrollment_flag_that_disagrees_with_state():
    clock = "2026-07-16T00:00:00+00:00"
    launch_admission = _admission("open", clock)
    malformed_stats_admission = _admission("open", clock)
    malformed_stats_admission["enrollment_open"] = False

    joined = _evaluate(
        _launch(clock, launch_admission),
        _stats(clock, malformed_stats_admission),
        None,
    )

    assert joined["state"] == "incomplete"
    assert joined["members"]["stats"] is None
    assert joined["edgeUsable"] is False and joined["actionBlock"] is False


def test_board_loads_meta_and_only_quarantines_launch_stats():
    html = BOARD.read_text()

    assert '"operators","stats","meta"' in html
    assert "if(me&&me.schema_version===1)data.meta=me" in html
    assert "function validationOverviewDisplayState" in html
    assert 'row.lane==="launch"?sentinel:row' in html
    assert "协议同步中·不可验证" in html
    assert "carryEvidenceUiState(data.perp?.carry_paper,data.stats?.lanes?.carry)" in html
    assert "const carryEvidence=data.stats?.lanes?.carry" not in html
    assert 'if(view==="launch"&&!launchJoin.edgeUsable)' in html
    assert 'if(level==="A3_MANUAL_PROBE"&&launchStatsJoinState().actionBlock)' in html


def test_browser_retains_failed_stats_but_hides_its_edge_after_new_launch():
    playwright = pytest.importorskip("playwright.sync_api")
    now = datetime.now(timezone.utc).replace(microsecond=0)
    old_clock, new_clock = now.isoformat(), (now + timedelta(seconds=1)).isoformat()
    admission = _admission("scheduled", old_clock)
    old_launch = _launch(old_clock, admission, "OLD-LAUNCH-EVENT")
    new_launch = _launch(new_clock, admission, "NEW-LAUNCH-EVENT")
    stats = _stats(old_clock, admission, "OLD-LAUNCH-EDGE-MUST-DISAPPEAR")
    meta = _meta(old_launch, stats, admission, admission)
    payloads = {
        "launch": old_launch, "stats": stats, "meta": meta,
        "structure": {"schema_version": 1, "events": [], "source_health": []},
        "airdrop": {"schema_version": 1, "events": []},
        "watch": {"schema_version": 1, "watch": []},
        "perps": _perps(),
        "opportunities": {"schema_version": 1, "opportunities": []},
        "operators": {"schema_version": 1, "operators": []},
    }
    phase = {"stats_fail": False}

    with playwright.sync_playwright() as driver:
        if not Path(driver.chromium.executable_path).exists():
            pytest.skip("Playwright Chromium is not installed")
        browser = driver.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1280, "height": 900})

        def route_request(route, request):
            path = urlparse(request.url).path
            if path == "/":
                route.fulfill(status=200, content_type="text/html", body=BOARD.read_text())
            elif path == "/protocol-join.js":
                route.fulfill(status=200, content_type="text/javascript", body=JOIN.read_text())
            elif path == "/vendor/lightweight-charts-5.2.0.js":
                route.fulfill(status=200, content_type="text/javascript", body=CHARTS.read_bytes())
            elif path.startswith("/data/") and path.endswith(".json"):
                name = Path(path).stem
                if name == "stats" and phase["stats_fail"]:
                    route.abort("failed")
                else:
                    route.fulfill(status=200, json=payloads[name])
            else:
                route.abort("blockedbyclient")

        page.route("**/*", route_request)
        page.goto("https://board.test/", wait_until="networkidle")
        # Evidence lives in collapsed drawers by design; open them so the
        # quarantine transition stays observable through inner_text().
        page.evaluate(
            "document.querySelectorAll('details.fold-drawer')"
            ".forEach(node => { node.open = true })"
        )
        body = page.locator("body")
        assert "OLD-LAUNCH-EDGE-MUST-DISAPPEAR" in body.inner_text()
        assert "CARRY-CONTRACT-UNAFFECTED" in body.inner_text()

        payloads["launch"] = new_launch
        phase["stats_fail"] = True
        page.evaluate("load()")
        text = body.inner_text()
        assert "协议同步中·不可验证" in text
        assert "OLD-LAUNCH-EDGE-MUST-DISAPPEAR" not in text
        assert "NEW-LAUNCH-EVENT" in text
        assert "CARRY-CONTRACT-UNAFFECTED" in text
        browser.close()


def test_fast_poll_fail_closes_on_old_meta_then_recovers_with_new_certificate():
    playwright = pytest.importorskip("playwright.sync_api")
    now = datetime.now(timezone.utc).replace(microsecond=0)
    old_clock, new_clock = now.isoformat(), (now + timedelta(seconds=1)).isoformat()
    admission = _admission("scheduled", old_clock)
    old_launch = _launch(old_clock, admission, "OLD-POLL-EVENT")
    new_launch = _launch(new_clock, admission, "NEW-POLL-EVENT")
    stats = _stats(old_clock, admission, "EDGE-RETURNS-AFTER-META-JOIN")
    payloads = {
        "launch": old_launch,
        "stats": stats,
        "meta": _meta(old_launch, stats, admission, admission),
        "structure": {"schema_version": 1, "events": [], "source_health": []},
        "airdrop": {"schema_version": 1, "events": []},
        "watch": {"schema_version": 1, "watch": []},
        "perps": _perps(),
        "opportunities": {"schema_version": 1, "opportunities": []},
        "operators": {"schema_version": 1, "operators": []},
    }
    fast_requests = []

    with playwright.sync_playwright() as driver:
        if not Path(driver.chromium.executable_path).exists():
            pytest.skip("Playwright Chromium is not installed")
        browser = driver.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1280, "height": 900})

        def route_request(route, request):
            parsed = urlparse(request.url)
            path = parsed.path
            if path == "/":
                route.fulfill(status=200, content_type="text/html", body=BOARD.read_text())
            elif path == "/protocol-join.js":
                route.fulfill(status=200, content_type="text/javascript", body=JOIN.read_text())
            elif path == "/vendor/lightweight-charts-5.2.0.js":
                route.fulfill(status=200, content_type="text/javascript", body=CHARTS.read_bytes())
            elif path.startswith("/data/") and path.endswith(".json"):
                name = Path(path).stem
                if name in {"launch", "meta"} and parsed.query:
                    fast_requests.append((name, parsed.query))
                route.fulfill(status=200, json=payloads[name])
            else:
                route.abort("blockedbyclient")

        page.route("**/*", route_request)
        page.goto("https://board.test/", wait_until="networkidle")
        # Evidence lives in collapsed drawers by design; open them so the
        # fail-close/recover cycle stays observable through inner_text().
        page.evaluate(
            "document.querySelectorAll('details.fold-drawer')"
            ".forEach(node => { node.open = true })"
        )
        body = page.locator("body")
        assert "EDGE-RETURNS-AFTER-META-JOIN" in body.inner_text()

        fast_requests.clear()
        payloads["launch"] = new_launch
        page.evaluate("loadLaunch()")
        text = body.inner_text()
        assert "协议同步中·不可验证" in text
        assert "EDGE-RETURNS-AFTER-META-JOIN" not in text
        assert "NEW-POLL-EVENT" in text
        first_poll = fast_requests.copy()

        fast_requests.clear()
        payloads["meta"] = _meta(new_launch, stats, admission, admission)
        page.evaluate("loadLaunch()")
        text = body.inner_text()
        assert "EDGE-RETURNS-AFTER-META-JOIN" in text
        assert "协议同步中·不可验证" not in text
        second_poll = fast_requests.copy()

        for poll in (first_poll, second_poll):
            assert {name for name, _ in poll} == {"launch", "meta"}
            assert len({query for _, query in poll}) == 1
        browser.close()
