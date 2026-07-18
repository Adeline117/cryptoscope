"""The browser consumes runtime safety strictly and fails closed."""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse

import pytest


ROOT = Path(__file__).parents[1]
BOARD = ROOT / "board" / "public" / "index.html"
JOIN = ROOT / "board" / "public" / "protocol-join.js"
DELIVERY = ROOT / "board" / "public" / "launch-delivery.js"
CHARTS = ROOT / "board" / "public" / "vendor" / "lightweight-charts-5.2.0.js"


def _runtime(state="healthy") -> dict:
    storage = "ok"
    reasons = []
    blocks = False
    solana = {
        "state": "healthy", "live": 1, "configured": 1,
        "maintenance": "healthy",
    }
    evm = {"state": "healthy", "live": 2, "configured": 2}
    retention = "retained"
    if state == "blocked":
        storage = "critical"
        reasons = [
            "storage_pressure_critical", "solana_streams_unhealthy",
            "evm_streams_unhealthy",
            "hyperliquid_raw_trade_retention_shed",
        ]
        blocks = True
        solana = {
            "state": "blocked", "live": 0, "configured": 1,
            "maintenance": "healthy",
        }
        evm = {"state": "blocked", "live": 0, "configured": 5}
        retention = "shed"
    elif state == "degraded":
        storage = "warn"
        reasons = ["storage_pressure_warn"]
    return {
        "version": 1, "state": state, "blocks_actionability": blocks,
        "auto_execution_allowed": False, "storage_pressure": storage,
        "reason_codes": reasons,
        "streams": {
            "solana": solana,
            "evm": evm,
        },
        "hyperliquid_raw_trade_retention": retention,
    }


def _envelope(view: str, body: dict) -> dict:
    now = datetime.now(timezone.utc)
    return {
        "schema_version": 1, "view": view, "generated_at": now.isoformat(),
        "refresh_cadence_min": 5, "freshness_grace_min": 5,
        "next_expected_at": (now + timedelta(minutes=5)).isoformat(),
        "stale_after_at": (now + timedelta(minutes=10)).isoformat(),
        **body,
    }


def _payloads(runtime: dict | None) -> dict[str, dict]:
    payloads = {
        "launch": _envelope("launch", {"events": []}),
        "structure": _envelope("structure", {"events": [], "source_health": []}),
        "airdrop": _envelope("airdrop", {"events": []}),
        "watch": _envelope("watch", {"watch": []}),
        "perps": _envelope("perps", {
            "perps": [], "carry": [], "cascade_events": [],
            "carry_source_health": {"state": "unavailable"},
        }),
        "opportunities": _envelope("opportunities", {"opportunities": []}),
        "operators": _envelope("operators", {"operators": []}),
        "stats": _envelope("stats", {"lanes": {"launch": {}, "carry": {}}}),
        "meta": _envelope("meta", {
            "views": [], "view_status": {}, "launch_protocol_join": {},
        }),
    }
    if runtime is not None:
        payloads["meta"]["runtime_safety"] = runtime
    return payloads


def _route(page, payloads):
    def serve(route, request):
        path = urlparse(request.url).path
        if path == "/":
            route.fulfill(status=200, content_type="text/html", body=BOARD.read_text())
        elif path == "/protocol-join.js":
            route.fulfill(status=200, content_type="text/javascript", body=JOIN.read_text())
        elif path == "/launch-delivery.js":
            route.fulfill(status=200, content_type="text/javascript", body=DELIVERY.read_text())
        elif path == "/vendor/lightweight-charts-5.2.0.js":
            route.fulfill(status=200, content_type="text/javascript", body=CHARTS.read_bytes())
        elif path.startswith("/data/") and path.endswith(".json"):
            route.fulfill(status=200, json={}) if Path(path).stem not in payloads else route.fulfill(status=200, json=payloads[Path(path).stem])
        else:
            route.abort("blockedbyclient")

    page.route("**/*", serve)


def test_static_runtime_guard_is_strict_and_downgrades_a3():
    html = BOARD.read_text()
    guard = html[
        html.index("function runtimeSafetyUiState"):
        html.index("const ACTION_LEVELS")
    ]

    assert "runtime_health_unavailable" in guard
    assert 'if(raw.state!==expectedState||raw.blocks_actionability!==expectedBlocks)' in guard
    assert 'if(level==="A3_MANUAL_PROBE"&&runtimeSafetyUiState().blocks)' in html
    assert 'h+=runtimeSafetyBanner(runtimeSafety)' in html
    assert "采集受阻 · 当前不可行动" in html
    assert 'setLiveStatus(bad.length?"源阻断 · 视图异常":"源阻断 · 视图按时")' in html
    assert '.runtime-safety-banner[data-state="blocked"]' \
           '[data-blocks-actionability="true"]' in html
    assert 'role="region" aria-label="运行安全状态"' in html
    assert ".runtime-safety-banner{grid-column:1/-1" in html
    for forbidden in (".error", ".path", ".url", ".token", ".details"):
        assert forbidden not in guard


@pytest.mark.parametrize("width,height", [(1440, 900), (390, 844)])
def test_blocked_runtime_banner_is_first_screen_responsive(width, height):
    playwright = pytest.importorskip("playwright.sync_api")
    payloads = _payloads(_runtime("blocked"))
    with playwright.sync_playwright() as driver:
        if not Path(driver.chromium.executable_path).exists():
            pytest.skip("Playwright Chromium is not installed")
        browser = driver.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": width, "height": height})
        errors = []
        page.on("pageerror", lambda error: errors.append(str(error)))
        page.on("console", lambda message: errors.append(message.text)
                if message.type == "error" else None)
        page.on("requestfailed", lambda _: errors.append("requestfailed"))
        _route(page, payloads)
        page.goto("https://board.test/", wait_until="networkidle")

        banner = page.locator(
            '.runtime-safety-banner[data-state="blocked"]'
            '[data-blocks-actionability="true"]'
        )
        command = page.locator(".command")
        assert banner.count() == 1
        assert "采集受阻 · 当前不可行动" in banner.inner_text()
        assert "存储 CRITICAL" in banner.inner_text()
        assert "Solana 0/1（阻断，维护健康）" in banner.inner_text()
        assert "EVM 0/5（阻断）" in banner.inner_text()
        assert "HL 原始成交已停止留存" in banner.inner_text()
        assert "该字段不证明 BBO / books / asset ctx 在线" in banner.inner_text()
        assert "视图按时 ≠ 上游完整" in banner.inner_text()
        assert page.locator(".decision-kicker").inner_text().endswith("运行源阻断")
        assert page.locator(".decision-title").inner_text() == "采集受阻 · 当前不可行动"
        assert page.locator("#livet").inner_text() == "源阻断 · 视图按时"
        banner_box, command_box = banner.bounding_box(), command.bounding_box()
        assert banner_box and command_box and banner_box["y"] < command_box["y"]
        assert banner_box["y"] < height
        assert page.evaluate(
            "document.documentElement.scrollWidth <= window.innerWidth + 1"
        )
        assert page.evaluate("runtimeSafetyUiState().blocks") is True
        assert not errors
        browser.close()


@pytest.mark.parametrize(
    ("kind", "expected_state", "visible"),
    [("missing", "unknown", True), ("malicious", "unknown", True),
     ("healthy", "healthy", False), ("degraded", "degraded", True)],
)
def test_runtime_missing_or_invalid_fails_closed_and_healthy_hides(
        kind, expected_state, visible):
    playwright = pytest.importorskip("playwright.sync_api")
    runtime = None if kind == "missing" else _runtime(kind)
    if kind == "malicious":
        runtime = deepcopy(_runtime("healthy"))
        runtime["token"] = "LEAK_ME_SECRET_TOKEN"
    payloads = _payloads(runtime)
    with playwright.sync_playwright() as driver:
        if not Path(driver.chromium.executable_path).exists():
            pytest.skip("Playwright Chromium is not installed")
        browser = driver.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1280, "height": 900})
        errors = []
        page.on("pageerror", lambda error: errors.append(str(error)))
        page.on("console", lambda message: errors.append(message.text)
                if message.type == "error" else None)
        page.on("requestfailed", lambda _: errors.append("requestfailed"))
        _route(page, payloads)
        page.goto("https://board.test/", wait_until="networkidle")

        parsed = page.evaluate("runtimeSafetyUiState()")
        assert parsed["state"] == expected_state
        assert parsed["blocks"] is (expected_state == "unknown")
        assert bool(page.locator(".runtime-safety-banner").count()) is visible
        if expected_state == "unknown":
            assert "采集受阻 · 当前不可行动" in page.locator(
                '.runtime-safety-banner[data-state="unknown"]'
                '[data-blocks-actionability="true"]'
            ).inner_text()
            text = page.locator(".runtime-safety-banner").inner_text()
            assert "存储 ?" in text and "Solana ?/?" in text and "EVM ?/?" in text
        elif expected_state == "degraded":
            assert "采集降级" in page.locator(
                '.runtime-safety-banner[data-state="degraded"]'
                '[data-blocks-actionability="false"]'
            ).inner_text()
        assert "LEAK_ME_SECRET_TOKEN" not in page.locator("body").inner_text()
        assert not errors
        browser.close()
