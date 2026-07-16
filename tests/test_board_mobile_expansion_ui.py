"""Mobile evidence details stay readable inside the table viewport."""
from pathlib import Path
from urllib.parse import urlparse

import pytest


ROOT = Path(__file__).parents[1]
BOARD = ROOT / "board" / "public" / "index.html"
JOIN = ROOT / "board" / "public" / "protocol-join.js"
DELIVERY = ROOT / "board" / "public" / "launch-delivery.js"
CHARTS = ROOT / "board" / "public" / "vendor" / "lightweight-charts-5.2.0.js"


def _structure_row() -> dict:
    return {
        "id": "structure-mobile-width",
        "symbol": "MOBILE",
        "source": "exchange-with-a-long-name",
        "event_type": "instrument_inventory_addition",
        "instrument_class": "unclassified_spot",
        "inventory_detected_at": "2026-07-16T10:00:00+00:00",
        "markets": ["MOBILE-USDT"],
        "products": [{
            "classification": {
                "basis": "metadata_unavailable_for_this_inventory_observation",
            },
        }],
        "message": (
            "https://api.exchange.example/v5/market/instruments-info?"
            "category=spot&identifier=unbroken-mobile-detail-contract"
        ),
        "reasons": ["没有绑定官方公告与公告开盘时间，不得称为已核验上币"],
    }


def _launch_row() -> dict:
    transaction_hash = "0x" + "a" * 64
    return {
        "id": "launch-mobile-width",
        "symbol": "MOBILE",
        "token": "0x" + "b" * 40,
        "chain": "bsc",
        "source": "pancakeswap_v2_factory_plus_exact_pool_readback",
        "action_level": "A1_WATCH",
        "auto_execution_allowed": False,
        "detected_at": "2026-07-16T10:00:00+00:00",
        "event_at": "2026-07-16T10:00:00+00:00",
        "entry_price": 0.000000123,
        "invalidation_price": 0.0000001,
        "max_notional_usd": 25,
        "fdv": 100_000,
        "liquidity_usd": 20_000,
        "buys_m5": 2,
        "sells_m5": 1,
        "volume_m5": 100,
        "primary_evidence": {
            "transaction_hash": transaction_hash,
            "explorer_url": f"https://explorer.example/tx/{transaction_hash}",
            "pool": "0x" + "c" * 40,
            "block_number": 123,
        },
        "current_assessment": {
            "security_gate": {"state": "unknown", "reason": "not verified"},
            "execution_probe": {"state": "skipped", "reason": "not quoted"},
        },
        "reasons": [
            "https://evidence.example/" + "unbroken-evidence-segment-" * 6,
        ],
    }


def test_real_mobile_browser_keeps_structure_and_launch_details_in_viewport():
    playwright = pytest.importorskip("playwright.sync_api")
    with playwright.sync_playwright() as driver:
        if not Path(driver.chromium.executable_path).exists():
            pytest.skip("Playwright Chromium is not installed")
        browser = driver.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 390, "height": 844})

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
                    status=200, content_type="text/javascript",
                    body=CHARTS.read_bytes(),
                )
            elif path.startswith("/data/"):
                route.fulfill(status=200, json={"schema_version": 1})
            else:
                route.abort("blockedbyclient")

        page.route("**/*", route_request)
        page.goto("https://board.test/", wait_until="domcontentloaded")
        page.wait_for_function("() => typeof renderTable === 'function'")

        for lane, row in (("structure", _structure_row()), ("launch", _launch_row())):
            page.evaluate(
                """({lane,row}) => {
                    disposeEventCharts();
                    eventChartRows.clear();
                    eventChartSeq=0;
                    data[lane]={schema_version:1,events:[row]};
                    view=lane;
                    document.body.dataset.view=lane;
                    filterState[lane]=lane==="launch"?"all":undefined;
                    const config=LANES[lane];
                    document.getElementById("view").innerHTML=renderTable(
                        lane,[row],config.cols,config
                    );
                    wireRows();
                }""",
                {"lane": lane, "row": row},
            )
            page.locator("tr[data-row]").first.click()
            detail = page.locator("tr.exp:not([hidden]) .exp-detail")
            detail.wait_for()

            for scroll_to_middle in (False, True):
                metrics = detail.evaluate(
                    """(detail,scrollToMiddle) => {
                        const container=detail.closest(".scroll");
                        if(scrollToMiddle){
                            container.scrollLeft=(container.scrollWidth-container.clientWidth)/2;
                        }
                        const content=detail.getBoundingClientRect();
                        const viewport=container.getBoundingClientRect();
                        return {
                            detailClientWidth:detail.clientWidth,
                            detailScrollWidth:detail.scrollWidth,
                            contentLeft:content.left,
                            contentRight:content.right,
                            viewportLeft:viewport.left,
                            viewportRight:viewport.right,
                        };
                    }""",
                    scroll_to_middle,
                )
                assert metrics["detailScrollWidth"] <= metrics["detailClientWidth"]
                assert metrics["contentLeft"] >= metrics["viewportLeft"]
                assert metrics["contentRight"] <= metrics["viewportRight"]

        browser.close()
