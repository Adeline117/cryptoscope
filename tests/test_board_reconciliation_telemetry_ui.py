"""Launch UI makes bounded reconciliation RPC cost visible and fail explicit."""
from pathlib import Path
from urllib.parse import urlparse

import pytest


ROOT = Path(__file__).parents[1]
BOARD = ROOT / "board" / "public" / "index.html"
JOIN = ROOT / "board" / "public" / "protocol-join.js"
DELIVERY = ROOT / "board" / "public" / "launch-delivery.js"
CHARTS = ROOT / "board" / "public" / "vendor" / "lightweight-charts-5.2.0.js"


def _launch(details: dict | None) -> dict:
    return {
        "schema_version": 1,
        "events": [],
        "primary_sources": {
            "solana": {
                "available": True,
                "streams": [],
                "qualification": {},
                "reconciliation": {
                    "status": "degraded", "stale": False, "open_gaps": 0,
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


def test_board_contains_explicit_reconciliation_cost_contract():
    html = BOARD.read_text()

    assert "function launchReconciliationTelemetryState(d)" in html
    assert "独立对账 RPC 成本" in html
    assert "approx_success_response_bytes" in html
    assert "rpc_calls_total" in html and "rpc_failures_total" in html
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
        rendered = page.evaluate(
            "payload => renderLaunchCoverage(payload)", _launch(_details()),
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
        browser.close()

    assert "signature_pagination_transaction_hydration_v1" in rendered
    assert "9 次调用 · 1 次失败" in rendered
    assert "成功响应约 12345 bytes" in rendered
    assert "整轮 812 ms" in rendered
    assert "结果 rpc_pressure" in rendered
    assert "成本遥测缺失" in missing and "未观测" in missing
    assert invalid["available"] is False
