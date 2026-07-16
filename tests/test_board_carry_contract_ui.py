"""Browser-side Carry evidence rendering rejects fabricated real-edge claims."""
from pathlib import Path
from urllib.parse import urlparse

import pytest


ROOT = Path(__file__).parents[1]
BOARD = ROOT / "board" / "public" / "index.html"
JOIN = ROOT / "board" / "public" / "protocol-join.js"
DELIVERY = ROOT / "board" / "public" / "launch-delivery.js"
CHARTS = ROOT / "board" / "public" / "vendor" / "lightweight-charts-5.2.0.js"


def _carry_payloads() -> tuple[dict, dict]:
    episode = {"symbol": "BTC"}
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


def _page(driver):
    browser = driver.chromium.launch(headless=True)
    page = browser.new_page(viewport={"width": 1280, "height": 900})

    def route_request(route, request):
        path = urlparse(request.url).path
        assets = {
            "/": ("text/html", BOARD.read_bytes()),
            "/protocol-join.js": ("text/javascript", JOIN.read_bytes()),
            "/launch-delivery.js": ("text/javascript", DELIVERY.read_bytes()),
            "/vendor/lightweight-charts-5.2.0.js": (
                "text/javascript", CHARTS.read_bytes(),
            ),
        }
        if path in assets:
            content_type, body = assets[path]
            route.fulfill(status=200, content_type=content_type, body=body)
        elif path.startswith("/data/"):
            route.fulfill(status=200, json={"schema_version": 1})
        else:
            route.abort("blockedbyclient")

    page.route("**/*", route_request)
    page.goto("https://board.test/", wait_until="domcontentloaded")
    page.wait_for_function("() => typeof carryEvidenceUiState === 'function'")
    return browser, page


def test_carry_ui_accepts_only_frozen_proxy_contract():
    playwright = pytest.importorskip("playwright.sync_api")
    with playwright.sync_playwright() as driver:
        if not Path(driver.chromium.executable_path).exists():
            pytest.skip("Playwright Chromium is not installed")
        browser, page = _page(driver)
        paper, evidence = _carry_payloads()
        state = page.evaluate(
            "([paper,evidence]) => carryEvidenceUiState(paper,evidence)",
            [paper, evidence],
        )
        rendered = page.evaluate(
            "([paper,evidence]) => carryEvidenceHtml(paper,evidence,null)",
            [paper, evidence],
        )
        browser.close()

    assert state["available"] is True
    assert state["realEdgeDisplay"] == 0
    assert "真实优势样本" in rendered and "判决 不可判" in rendered


@pytest.mark.parametrize(
    ("target", "field", "value"),
    [
        ("paper", "real_edge_n", 13),
        ("paper", "real_edge_eligible", True),
        ("paper", "is_real_fill", True),
        ("paper", "n_proxy_closed", 0),
        ("evidence", "real_edge_n", 13),
        ("evidence", "real_edge_eligible", True),
        ("evidence", "cost_is_real_fill", True),
        ("evidence", "edge_verdict", "已获真实优势-SECRET"),
        ("evidence", "n_proxy", 0),
        ("evidence", "verdict", "measured"),
    ],
)
def test_carry_ui_hides_fabricated_claims(target, field, value):
    playwright = pytest.importorskip("playwright.sync_api")
    with playwright.sync_playwright() as driver:
        if not Path(driver.chromium.executable_path).exists():
            pytest.skip("Playwright Chromium is not installed")
        browser, page = _page(driver)
        paper, evidence = _carry_payloads()
        (paper if target == "paper" else evidence)[field] = value
        state = page.evaluate(
            "([paper,evidence]) => carryEvidenceUiState(paper,evidence)",
            [paper, evidence],
        )
        rendered = page.evaluate(
            "([paper,evidence]) => carryEvidenceHtml(paper,evidence,null)",
            [paper, evidence],
        )
        browser.close()

    assert state["available"] is False
    assert state["realEdgeDisplay"] == "不可判"
    assert "合同无效·不可判" in rendered
    assert "13" not in rendered
    assert "已获真实优势-SECRET" not in rendered
