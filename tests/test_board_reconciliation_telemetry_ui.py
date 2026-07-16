"""Launch UI makes bounded reconciliation RPC cost visible and fail explicit."""
from pathlib import Path
from urllib.parse import urlparse

import pytest


ROOT = Path(__file__).parents[1]
BOARD = ROOT / "board" / "public" / "index.html"
JOIN = ROOT / "board" / "public" / "protocol-join.js"
DELIVERY = ROOT / "board" / "public" / "launch-delivery.js"
CHARTS = ROOT / "board" / "public" / "vendor" / "lightweight-charts-5.2.0.js"


def _launch(details: dict | None, *, status: str = "degraded") -> dict:
    return {
        "schema_version": 1,
        "events": [],
        "primary_sources": {
            "solana": {
                "available": True,
                "streams": [],
                "qualification": {},
                "reconciliation": {
                    "status": status, "stale": False, "open_gaps": 0,
                    "details": details,
                },
                "source_readiness": {},
                "protocol_admission": {},
            },
            "evm": {"available": False, "streams": [], "qualification": {}},
        },
    }


def _details() -> dict:
    return {
        "schema_version": 1,
        "outcome": "rpc_pressure",
        "error_kind": "rate_limited",
        "rpc": {
            "version": 1,
            "algorithm": "signature_pagination_transaction_hydration_v1",
            "rpc_calls_total": 9,
            "rpc_failures_total": 1,
            "rpc_calls_by_method": {"getTransaction": 1},
            "rpc_failures_by_method": {"getTransaction": 1},
            "rpc_calls_by_role": {"archive": 1},
            "rpc_failures_by_role": {"archive": 1},
            "approx_success_response_bytes": 12_345,
            "run_elapsed_ms": 812,
        },
    }


def _open_circuit(*, state: str = "open") -> dict:
    return {
        "state": state,
        "consecutive_pressure_failures": 4,
        "live_provider": "solana_rpc:live.example",
        "archive_provider": "solana_rpc:archive.example",
        "next_retry_at": "2026-07-16T12:30:00+00:00",
        "pressure_kind": "rate_limited",
        "failed_method": "getTransaction",
    }


def _closed_circuit() -> dict:
    return {
        "state": "closed",
        "consecutive_pressure_failures": 0,
        "live_provider": "solana_rpc:live.example",
        "archive_provider": "solana_rpc:archive.example",
    }


def _successful_details(*, circuit: dict | None = None) -> dict:
    details = _details()
    details["outcome"] = "waiting_finality"
    details.pop("error_kind")
    details["rpc"]["rpc_failures_total"] = 0
    details["rpc"]["rpc_failures_by_method"] = {}
    details["rpc"]["rpc_failures_by_role"] = {}
    if circuit is not None:
        details["circuit"] = circuit
    return details


def test_board_contains_explicit_reconciliation_cost_contract():
    html = BOARD.read_text()

    assert "function launchReconciliationTelemetryState(d)" in html
    assert "独立对账 RPC 成本" in html
    assert "approx_success_response_bytes" in html
    assert "rpc_calls_total" in html and "rpc_failures_total" in html
    assert "consecutive_pressure_failures" in html
    assert "failed_method" in html and "next_retry_at" in html
    assert "熔断未观测" in html
    assert "指标只描述最近一次有界对账尝试" in html
    assert "最近一轮 reconciliation 成本 details 缺失或合同无效" in html


def test_real_browser_renders_costs_and_marks_missing_contract_unobserved():
    playwright = pytest.importorskip("playwright.sync_api")
    with playwright.sync_playwright() as driver:
        if not Path(driver.chromium.executable_path).exists():
            pytest.skip("Playwright Chromium is not installed")
        browser = driver.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1280, "height": 900})

        def route_request(route, request):
            path = urlparse(request.url).path
            if path == "/":
                route.fulfill(
                    status=200, content_type="text/html", body=BOARD.read_text(),
                )
            elif path == "/protocol-join.js":
                route.fulfill(
                    status=200, content_type="text/javascript", body=JOIN.read_text(),
                )
            elif path == "/launch-delivery.js":
                route.fulfill(
                    status=200, content_type="text/javascript",
                    body=DELIVERY.read_text(),
                )
            elif path == "/vendor/lightweight-charts-5.2.0.js":
                route.fulfill(
                    status=200, content_type="text/javascript", body=CHARTS.read_bytes(),
                )
            elif path.startswith("/data/"):
                route.fulfill(status=200, json={"schema_version": 1})
            else:
                route.abort("blockedbyclient")

        page.route("**/*", route_request)
        page.goto("https://board.test/", wait_until="domcontentloaded")
        page.wait_for_function(
            "() => typeof launchReconciliationTelemetryState === 'function'",
        )
        open_details = _details()
        open_details["circuit"] = _open_circuit()
        rendered = page.evaluate(
            "payload => renderLaunchCoverage(payload)", _launch(open_details),
        )
        retry_due = _details()
        retry_due["circuit"] = _open_circuit(state="retry_due")
        retry_due["circuit"]["next_retry_at"] = "2020-01-01T00:00:00Z"
        retry_rendered = page.evaluate(
            "payload => renderLaunchCoverage(payload)", _launch(retry_due),
        )
        legacy_success = _successful_details()
        legacy_state = page.evaluate(
            "payload => launchReconciliationTelemetryState(payload)",
            _launch(legacy_success, status="live"),
        )
        legacy_rendered = page.evaluate(
            "payload => renderLaunchCoverage(payload)",
            _launch(legacy_success, status="live"),
        )
        closed_success = _successful_details(circuit=_closed_circuit())
        closed_state = page.evaluate(
            "payload => launchReconciliationTelemetryState(payload)",
            _launch(closed_success, status="live"),
        )
        closed_rendered = page.evaluate(
            "payload => renderLaunchCoverage(payload)",
            _launch(closed_success, status="live"),
        )
        missing = page.evaluate(
            "payload => renderLaunchCoverage(payload)", _launch(None),
        )
        malicious = _details()
        malicious["rpc"]["algorithm"] = "<img src=x onerror=alert(1)>"
        invalid = page.evaluate(
            "payload => launchReconciliationTelemetryState(payload)",
            _launch(malicious),
        )
        secret = "https://secret-rpc.example/api-key"
        bad_circuits = []
        bad = _open_circuit()
        bad["archive_provider"] = f"solana_rpc:{secret}"
        bad_circuits.append(bad)
        bad = _open_circuit()
        bad["archive_provider"] = bad["live_provider"]
        bad_circuits.append(bad)
        bad = _open_circuit()
        bad["live_provider"] = "solana_rpc:same.example:8899"
        bad["archive_provider"] = "solana_rpc:same.example:9900"
        bad_circuits.append(bad)
        bad = _open_circuit()
        bad["failed_method"] = secret
        bad_circuits.append(bad)
        bad = _open_circuit()
        bad["pressure_kind"] = secret
        bad_circuits.append(bad)
        bad = _open_circuit()
        bad["state"] = "half_open"
        bad_circuits.append(bad)
        bad = _open_circuit()
        bad["next_retry_at"] = "2026-07-16T12:30:00"
        bad_circuits.append(bad)
        bad = _open_circuit()
        bad["consecutive_pressure_failures"] = True
        bad_circuits.append(bad)
        bad = _open_circuit()
        bad["endpoint_url"] = secret
        bad_circuits.append(bad)
        bad = _open_circuit()
        bad["consecutive_pressure_failures"] = 0
        bad_circuits.append(bad)
        bad = _closed_circuit()
        bad["next_retry_at"] = "2026-07-16T12:30:00+00:00"
        bad_circuits.append(bad)
        bad = _closed_circuit()
        bad["consecutive_pressure_failures"] = 1
        bad_circuits.append(bad)
        invalid_circuits = []
        invalid_rendered = []
        for circuit in bad_circuits:
            payload = _details()
            payload["circuit"] = circuit
            invalid_circuits.append(page.evaluate(
                "value => launchReconciliationTelemetryState(value)", _launch(payload),
            ))
            invalid_rendered.append(page.evaluate(
                "value => renderLaunchCoverage(value)", _launch(payload),
            ))
        browser.close()

    assert "signature_pagination_transaction_hydration_v1" in rendered
    assert "9 次调用 · 1 次失败" in rendered
    assert "成功响应约 12345 bytes" in rendered
    assert "整轮 812 ms" in rendered
    assert "结果 rpc_pressure" in rendered
    assert "持久熔断中" in rendered
    assert "连续压力 4 次" in rendered
    assert "failed_method getTransaction" in rendered
    assert "next_retry_at 2026-07-16 12:30:00+00:00" not in rendered
    assert "next_retry_at 2026-07-16 12:30:00 UTC" in rendered
    assert "重试已到期" in retry_rendered and "failed_method getTransaction" in retry_rendered
    assert legacy_state["available"] is True
    assert legacy_state["circuitObserved"] is False
    assert legacy_state["healthy"] is False
    assert "熔断未观测" in legacy_rendered
    assert "本轮 RPC 无失败" not in legacy_rendered
    assert closed_state["healthy"] is True
    assert closed_state["circuitObserved"] is True
    assert "本轮 RPC 无失败" in closed_rendered
    assert "连续压力 0 次" in closed_rendered
    assert "成本遥测缺失" in missing and "未观测" in missing
    assert invalid["available"] is False
    assert all(state["available"] is False for state in invalid_circuits)
    assert all(state["circuitInvalid"] is True for state in invalid_circuits)
    assert all("熔断合同无效" in value for value in invalid_rendered)
    assert secret not in str(invalid_circuits)
    assert secret not in "".join(invalid_rendered)
