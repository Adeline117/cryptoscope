"""Real-browser contract tests for the fail-closed five-lane Play overview."""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse

import pytest

from tests.test_board_protocol_join_ui import (
    BOARD,
    CHARTS,
    JOIN,
    _admission,
    _launch,
    _meta,
    _stats,
)


DELIVERY = BOARD.parent / "launch-delivery.js"
SYSTEM_CHROMIUM = (
    Path("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"),
    Path("/Applications/Chromium.app/Contents/MacOS/Chromium"),
    Path("/usr/bin/google-chrome"),
    Path("/usr/bin/chromium"),
    Path("/usr/bin/chromium-browser"),
)


def _paper_20() -> dict:
    recent = [{"symbol": f"PAPER-LIFECYCLE-{index}"} for index in range(8)]
    return {
        "cohort_kind": "descriptive_quote_proxy",
        "n_open": 0,
        "n_closed": 20,
        "n_proxy_closed": 20,
        "real_edge_n": 0,
        "n_exit_pending": 0,
        "n_quarantined_total": 0,
        "n_closed_total": 20,
        "n_closed_excluded": 0,
        "excluded_by_reason": {},
        "cost_completeness": "partial",
        "all_in_total_pct": None,
        "is_real_fill": False,
        "real_edge_eligible": False,
        "open_positions": [],
        "recent": recent,
    }


def _payloads(*, launch_summary: str = "JOINED-LAUNCH-SUMMARY") -> tuple[dict, dict, dict]:
    clock = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    admission = _admission("scheduled", clock)
    launch = _launch(clock, admission, "VISIBLE-LAUNCH-EVENT")
    stats = _stats(clock, admission, launch_summary)
    payloads = {
        "launch": launch,
        "stats": stats,
        "meta": _meta(launch, stats, admission, admission),
        "structure": {"schema_version": 1, "events": [], "source_health": []},
        "airdrop": {"schema_version": 1, "events": []},
        "watch": {"schema_version": 1, "watch": []},
        "perps": {
            "schema_version": 1,
            "perps": [],
            "carry": [],
            "cascade_events": [],
            # Intentionally newer than stats.validation_overview's Carry 16/20.
            "carry_paper": _paper_20(),
        },
        "opportunities": {"schema_version": 1, "opportunities": []},
        "operators": {"schema_version": 1, "operators": []},
    }
    return payloads, admission, stats


def _route_page(page, payloads: dict) -> None:
    def route_request(route, request):
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
            route.fulfill(status=200, json=payloads[Path(path).stem])
        else:
            route.abort("blockedbyclient")

    page.route("**/*", route_request)


def _chromium(playwright):
    managed = Path(playwright.chromium.executable_path)
    executable = managed if managed.exists() else next(
        (path for path in SYSTEM_CHROMIUM if path.exists()), None,
    )
    if executable is None:
        pytest.skip("Playwright Chromium or a system Chromium is not installed")
    return playwright.chromium.launch(headless=True, executable_path=str(executable))


def _capture_browser_errors(page) -> list[str]:
    errors: list[str] = []
    page.on("pageerror", lambda error: errors.append(f"pageerror: {error}"))
    page.on(
        "console",
        lambda message: errors.append(f"console: {message.text}")
        if message.type == "error" else None,
    )
    page.on(
        "requestfailed",
        lambda request: errors.append(f"requestfailed: {request.url}"),
    )
    return errors


def test_play_validator_has_one_server_projection_truth_source():
    html = BOARD.read_text()
    validator = html.split("function validationOverviewUiState", 1)[1].split(
        "function validationVerdictLabel", 1,
    )[0]
    render = html.split("function renderPlay(){", 1)[1].split(
        "function carryHealthHtml", 1,
    )[0]

    assert "data.stats?.validation_overview" in validator
    assert "data.stats?.lanes" not in validator
    assert "data.perp" not in validator
    assert "evidence-grid" not in render
    assert "carryEvidenceHtml(" not in render
    assert "validationOverviewDisplayState()" in render
    assert "validationSampleLabel(validationByLane.launch)" in render
    assert "validationSampleLabel(validationByLane.carry)" in render


def test_play_overview_leads_with_decision_and_keeps_evidence_one_click_away():
    playwright = pytest.importorskip("playwright.sync_api")
    payloads, _, _ = _payloads()
    payloads["meta"]["runtime_safety"].update({
        "state": "degraded",
        "storage_pressure": "warn",
        "reason_codes": ["storage_pressure_warn"],
    })

    with playwright.sync_playwright() as driver:
        browser = _chromium(driver)
        page = browser.new_page(viewport={"width": 1440, "height": 900})
        errors = _capture_browser_errors(page)
        _route_page(page, payloads)
        page.goto("https://board.test/", wait_until="networkidle")

        # The hero decision — not the statistics wall — is the first readable
        # content: verdict, plain-language conclusion, always-visible auto-off.
        command = page.locator(".command")
        assert command.locator(".decision-title").inner_text() != ""
        assert "今日结论" in command.locator(".decision-conclusion").inner_text()
        assert command.locator(".hero-auto-off").inner_text() == "自动交易 OFF"
        chips = command.locator(".hero-chips").inner_text()
        assert "风险预算 · 单探针≤$500 · 并发≤3" in chips
        assert "当前占用 $0/$1,500" in chips

        # A forged budget must fail closed to "?" without echoing its numbers.
        page.evaluate(
            "data.meta.risk_budget.per_probe_cap_usd = 50000; paint()"
        )
        page.wait_for_timeout(50)
        chips = command.locator(".hero-chips").inner_text()
        assert "风险预算 ? · 合同缺失或无效" in chips
        assert "50,000" not in chips and "50000" not in chips
        page.evaluate(
            "data.meta.risk_budget.per_probe_cap_usd = 500.0; paint()"
        )
        page.wait_for_timeout(50)
        assert page.locator(".runtime-safety-banner").evaluate(
            "node => node.compareDocumentPosition(document.querySelector('.command')) & Node.DOCUMENT_POSITION_FOLLOWING"
        )
        assert command.evaluate(
            "node => node.compareDocumentPosition(document.querySelector('#validation-drawer')) & Node.DOCUMENT_POSITION_FOLLOWING"
        )

        # Evidence stays honest but collapsed: the headline is on the summary
        # line; the five-lane snapshot is one click away and state survives
        # the poll repaint.
        drawer = page.locator("#validation-drawer")
        assert drawer.evaluate("node => !node.open")
        assert "尚无可执行优势" in drawer.locator("summary").inner_text()
        drawer.locator("summary").click()
        page.wait_for_timeout(50)

        overview = page.locator("#validation-overview")
        assert overview.locator(".validation-overview-title").inner_text() == "尚无可执行优势"
        assert overview.locator(".validation-auto-off").inner_text() == "自动交易 OFF"
        assert overview.locator(".validation-card").count() == 5
        assert overview.locator(".validation-card").evaluate_all(
            "nodes => nodes.map(node => node.dataset.validationLane)"
        ) == ["launch", "cascade", "carry", "airdrop", "structure"]

        page.evaluate("paint()")
        page.wait_for_timeout(50)
        assert page.locator("#validation-drawer").evaluate("node => node.open")

        for width, height, display, minimum_card_width in (
            (1440, 900, "grid", 150),
            (768, 1024, "flex", 280),
            (390, 844, "flex", 280),
        ):
            page.set_viewport_size({"width": width, "height": height})
            page.wait_for_timeout(50)
            title_box = page.locator(".decision-title").bounding_box()
            assert title_box is not None and title_box["y"] < height
            assert page.locator(".validation-overview-grid").evaluate(
                "node => getComputedStyle(node).display"
            ) == display
            widths = overview.locator(".validation-card").evaluate_all(
                "nodes => nodes.map(node => node.getBoundingClientRect().width)"
            )
            assert min(widths) >= minimum_card_width
            assert page.evaluate(
                "document.documentElement.scrollWidth <= document.documentElement.clientWidth"
            )
            if width <= 980:
                assert page.locator(".validation-overview-grid").evaluate(
                    "node => node.scrollWidth > node.clientWidth"
                )

        assert errors == []
        browser.close()


def test_play_uses_stats_carry_16_of_20_and_rejects_faster_paper_20_snapshot():
    playwright = pytest.importorskip("playwright.sync_api")
    payloads, _, _ = _payloads()

    with playwright.sync_playwright() as driver:
        browser = _chromium(driver)
        page = browser.new_page(viewport={"width": 1280, "height": 900})
        errors = _capture_browser_errors(page)
        _route_page(page, payloads)
        page.goto("https://board.test/", wait_until="networkidle")
        page.locator("#validation-drawer > summary").click()
        page.locator("#health-drawer > summary").click()

        carry = page.locator('[data-validation-lane="carry"]')
        assert carry.locator(".validation-sample").inner_text().splitlines()[0] == "16/20"
        assert "20/20" not in page.locator("#validation-overview").inner_text()
        assert "C 16/20" in page.locator(".funnel-step").nth(3).inner_text()
        assert page.locator("#view .sc-card").count() == 0
        state = page.evaluate(
            "carryEvidenceUiState(data.perp.carry_paper,data.stats.lanes.carry)"
        )
        assert state["available"] is False
        assert state["reason"] == "Carry paper/stats 关闭计数跨快照不一致"
        assert errors == []
        browser.close()


def test_malformed_or_old_overview_fails_closed_without_echoing_secret():
    playwright = pytest.importorskip("playwright.sync_api")
    payloads, admission, original_stats = _payloads()

    with playwright.sync_playwright() as driver:
        browser = _chromium(driver)
        page = browser.new_page(viewport={"width": 1280, "height": 900})
        errors = _capture_browser_errors(page)
        _route_page(page, payloads)
        page.goto("https://board.test/", wait_until="networkidle")
        page.locator("#validation-drawer > summary").click()

        malformed = deepcopy(original_stats)
        malformed["validation_overview"]["rows"][2]["result"][
            "MALICIOUS-OVERVIEW-SECRET"
        ] = "MALICIOUS-OVERVIEW-SECRET"
        payloads["stats"] = malformed
        payloads["meta"] = _meta(payloads["launch"], malformed, admission, admission)
        page.evaluate("load()")

        overview = page.locator("#validation-overview")
        assert overview.locator(".validation-overview-title").inner_text() == (
            "优势状态不可验证｜按不可行动处理"
        )
        assert overview.locator('[data-verdict="unverifiable"]').count() == 5
        assert errors == []
        assert "MALICIOUS-OVERVIEW-SECRET" not in page.locator("body").inner_text()
        assert overview.locator(".validation-auto-off").inner_text() == "自动交易 OFF"

        old = deepcopy(original_stats)
        old["validation_overview"]["version"] = 0
        payloads["stats"] = old
        payloads["meta"] = _meta(payloads["launch"], old, admission, admission)
        page.evaluate("load()")
        assert overview.locator(".validation-overview-title").inner_text() == (
            "优势状态不可验证｜按不可行动处理"
        )
        assert overview.locator('[data-verdict="unverifiable"]').count() == 5
        assert errors == []
        browser.close()


def test_new_launch_with_old_stats_isolates_only_launch_card_then_recovers():
    playwright = pytest.importorskip("playwright.sync_api")
    marker = "OLD-STATS-LAUNCH-SUMMARY-MUST-DISAPPEAR"
    payloads, admission, stats = _payloads(launch_summary=marker)
    old_launch = payloads["launch"]
    new_clock = (
        datetime.fromisoformat(old_launch["generated_at"]) + timedelta(seconds=1)
    ).isoformat()
    new_launch = _launch(new_clock, admission, "NEW-LAUNCH-STILL-VISIBLE")

    with playwright.sync_playwright() as driver:
        browser = _chromium(driver)
        page = browser.new_page(viewport={"width": 1280, "height": 900})
        errors = _capture_browser_errors(page)
        _route_page(page, payloads)
        page.goto("https://board.test/", wait_until="networkidle")
        # Evidence and the research queue live in collapsed drawers by design;
        # open them so the cross-snapshot quarantine behavior stays observable.
        page.evaluate(
            "document.querySelectorAll('details.fold-drawer')"
            ".forEach(node => { node.open = true })"
        )

        launch_card = page.locator('[data-validation-lane="launch"]')
        assert marker in launch_card.inner_text()
        preserved = {
            lane: page.locator(f'[data-validation-lane="{lane}"]').inner_text()
            for lane in ("cascade", "carry", "airdrop", "structure")
        }

        payloads["launch"] = new_launch
        page.evaluate("loadLaunch()")
        assert "协议同步中·不可验证" in launch_card.inner_text()
        assert marker not in page.locator("#validation-overview").inner_text()
        assert "NEW-LAUNCH-STILL-VISIBLE" in page.locator("body").inner_text()
        for lane, expected in preserved.items():
            assert page.locator(f'[data-validation-lane="{lane}"]').inner_text() == expected

        payloads["meta"] = _meta(new_launch, stats, admission, admission)
        page.evaluate("loadLaunch()")
        assert marker in launch_card.inner_text()
        assert "协议同步中·不可验证" not in launch_card.inner_text()
        assert errors == []
        browser.close()
