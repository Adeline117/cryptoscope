"""The browser scopes the perp identity gate without leaking raw cache data."""
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
TTL_SECONDS = 26 * 60 * 60
SYSTEM_CHROMIUM = (
    Path("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"),
    Path("/Applications/Chromium.app/Contents/MacOS/Chromium"),
    Path("/usr/bin/google-chrome"),
    Path("/usr/bin/chromium"),
    Path("/usr/bin/chromium-browser"),
)
A3_PROBE = """
    () => {
      const savedRuntime=runtimeSafetyUiState;
      const savedProtocol=launchProtocolUiState;
      const savedReconciliation=launchReconciliationProofState;
      const savedJoin=launchStatsJoinState;
      const savedDelivery=launchDeliveryUiState;
      try{
        runtimeSafetyUiState=()=>({blocks:false});
        launchProtocolUiState=()=>({open:true});
        launchReconciliationProofState=()=>({state:"pass"});
        launchStatsJoinState=()=>({actionBlock:false});
        launchDeliveryUiState=()=>({state:"pass"});
        return actionLevel({
          action_level:"A3_MANUAL_PROBE",
          expires_at:new Date(Date.now()+60_000).toISOString(),
        },{});
      }finally{
        runtimeSafetyUiState=savedRuntime;
        launchProtocolUiState=savedProtocol;
        launchReconciliationProofState=savedReconciliation;
        launchStatsJoinState=savedJoin;
        launchDeliveryUiState=savedDelivery;
      }
    }
"""


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
            "evm": {"state": "healthy", "live": 2, "configured": 2},
        },
        "hyperliquid_raw_trade_retention": "retained",
    }


def _policy(status="research_only", *, age=300) -> dict:
    common = {
        "version": 1,
        "status": status,
        "blocks_identity_dependent_scans": True,
        "auto_execution_allowed": False,
        "reason_codes": ["heuristic_mapping_not_actionable"],
        "market_count": 408,
        "research_mapped": 189,
        "actionable_identity_count": 0,
        "independent_source_count": 1,
        "observed_path_count": 2,
        "cache_age_seconds": age,
        "cache_ttl_seconds": TTL_SECONDS,
    }
    if status == "verified":
        common.update({
            "blocks_identity_dependent_scans": False,
            "reason_codes": [],
            "market_count": 10,
            "research_mapped": 0,
            "actionable_identity_count": 1,
            "observed_path_count": 1,
        })
    return common


def _envelope(view: str, body: dict, *, now: datetime | None = None) -> dict:
    now = now or datetime.now(timezone.utc)
    return {
        "schema_version": 1,
        "view": view,
        "generated_at": now.isoformat(),
        "refresh_cadence_min": 5,
        "freshness_grace_min": 5,
        "next_expected_at": (now + timedelta(minutes=5)).isoformat(),
        "stale_after_at": (now + timedelta(minutes=10)).isoformat(),
        **body,
    }


def _payloads(policy: dict | None, *, meta_at: datetime | None = None) -> dict:
    payloads = {
        "launch": _envelope("launch", {"events": []}),
        "structure": _envelope(
            "structure", {"events": [], "source_health": []},
        ),
        "airdrop": _envelope("airdrop", {"events": []}),
        "watch": _envelope("watch", {"watch": []}),
        "perps": _envelope("perps", {
            "perps": [],
            "carry": [],
            "cascade_events": [],
            "carry_source_health": {
                "state": "ok",
                "hl": {"state": "ok", "rows": 5},
                "okx": {
                    "state": "ok", "observed": 4, "requested": 4,
                    "unsupported": 0, "request_failed": 0,
                    "request_timeout": 0, "rate_stale": 0,
                    "rate_invalid": 0, "request_cap": 40,
                },
                "paper": {"state": "ok"},
                "open_observed": 0,
                "open_requested": 0,
            },
        }),
        "opportunities": _envelope(
            "opportunities", {"opportunities": []},
        ),
        "operators": _envelope("operators", {"operators": []}),
        "stats": _envelope(
            "stats", {"lanes": {"launch": {}, "carry": {}}},
        ),
        "meta": _envelope("meta", {
            "views": [],
            "view_status": {},
            "launch_protocol_join": {},
            "runtime_safety": _runtime(),
        }, now=meta_at),
    }
    if policy is not None:
        payloads["meta"]["perp_identity_policy"] = policy
    return payloads


def _route(page, payloads):
    def serve(route, request):
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
                status=200, content_type="text/javascript", body=DELIVERY.read_text(),
            )
        elif path == "/vendor/lightweight-charts-5.2.0.js":
            route.fulfill(
                status=200, content_type="text/javascript", body=CHARTS.read_bytes(),
            )
        elif path.startswith("/data/") and path.endswith(".json"):
            route.fulfill(status=200, json=payloads[Path(path).stem])
        else:
            route.abort("blockedbyclient")

    page.route("**/*", serve)


def _open_page(driver, payloads, *, width=1280, height=900, view="perp"):
    managed = Path(driver.chromium.executable_path)
    executable = managed if managed.exists() else next(
        (path for path in SYSTEM_CHROMIUM if path.exists()), None,
    )
    if executable is None:
        pytest.skip("Playwright Chromium or a system Chromium is not installed")
    browser = driver.chromium.launch(
        headless=True, executable_path=str(executable),
    )
    page = browser.new_page(viewport={"width": width, "height": height})
    errors = []
    page.on("pageerror", lambda error: errors.append(str(error)))
    page.on(
        "console",
        lambda message: errors.append(message.text)
        if message.type == "error" else None,
    )
    page.on("requestfailed", lambda _: errors.append("requestfailed"))
    _route(page, payloads)
    page.goto(f"https://board.test/#{view}", wait_until="networkidle")
    return browser, page, errors


def test_static_identity_guard_is_exact_scoped_and_client_expiring():
    html = BOARD.read_text()
    guard = html[
        html.index("function perpIdentityPolicyUiState"):
        html.index("function perpIdentityCacheAgeText")
    ]
    action_guard = html[
        html.index("function actionLevel"):
        html.index("function actionChip")
    ]

    assert "PERP_IDENTITY_KEYS" in guard
    assert "Object.keys(value).length===keys.length" in guard
    assert "raw.cache_age_seconds+elapsed" in guard
    assert "measuredAge>=raw.cache_ttl_seconds" in guard
    assert 'status:expired?"stale":raw.status' in guard
    assert "perpIdentityCanonicalUtcMillis(meta?.generated_at)" in guard
    assert "projectedAt>nowMs" in guard
    assert "nowMs+60_000" not in guard
    assert "research_mapped+raw.actionable_identity_count>raw.market_count" in guard
    for forbidden in (
        ".cache_path", ".research_universe", ".actionable_universe",
        ".universe", ".url", ".address", ".symbol", ".token",
    ):
        assert forbidden not in guard
    assert "perpIdentity" not in action_guard
    assert 'if(level==="A3_MANUAL_PROBE"&&runtimeSafetyUiState().blocks)' \
           in action_guard
    assert "let html=perpIdentityPolicyPanel();" in html
    assert html.index("let html=perpIdentityPolicyPanel();") \
           < html.index("html+=carryHealthHtml(health);")
    assert "identityCoverage=perpIdentityCoverageState()" in html
    assert "[launchState,cascadeState,structureState,airdropState,carryState]" in html
    assert "schedulePerpIdentityExpiry(me)" in html
    assert "remaining*1000+25" in html
    assert 'view==="launch"||view==="play"||view==="perp"' in html
    assert '.perp-identity-policy[data-status="research_only"]' in html
    assert '.perp-identity-policy[data-status="verified"]' \
           '[data-blocks-identity="false"]' in html
    assert ".identity-policy-grid{grid-template-columns:1fr 1fr}" in html


@pytest.mark.parametrize("width,height", [(1440, 900), (390, 844)])
def test_research_policy_panel_is_first_scoped_and_responsive(width, height):
    playwright = pytest.importorskip("playwright.sync_api")
    payloads = _payloads(_policy())
    with playwright.sync_playwright() as driver:
        browser, page, errors = _open_page(
            driver, payloads, width=width, height=height,
        )

        panel = page.locator(
            '.perp-identity-policy[data-status="research_only"]'
            '[data-blocks-identity="true"]'
        )
        health = page.locator(".carry-health")
        assert panel.count() == 1 and health.count() == 1
        text = panel.inner_text()
        assert "研究可见 · 身份策略阻断" in text
        assert "符号 + 市值启发式映射不证明" in text
        assert "holder→CEX 充值、mobilization 与 LP 解锁扫描未运行" in text
        assert "Hyperliquid Cascade 与 HL/OKX Carry" in text
        assert "不受此身份门影响" in text
        assert "自动交易始终关闭" in text
        assert "408" in text and "189" in text and "5m / 26h" in text
        panel_box, health_box = panel.bounding_box(), health.bounding_box()
        assert panel_box and health_box and panel_box["y"] < health_box["y"]
        assert page.evaluate("perpIdentityPolicyUiState().blocks") is True
        assert page.evaluate("runtimeSafetyUiState().blocks") is False
        assert page.locator(".runtime-safety-banner").count() == 0
        assert page.evaluate(
            "document.documentElement.scrollWidth <= window.innerWidth + 1"
        )

        page.evaluate("setView('play',false)")
        cascade = page.locator(".market-coverage-card").filter(
            has_text="Cascade"
        ).first
        assert "身份扩展 · 研究 189 / 扫描阻断" in cascade.inner_text()
        assert "Hyperliquid · 本轮快照正常" in cascade.inner_text()
        assert "采集受阻 · 当前不可行动" not in page.locator(
            ".decision-title"
        ).inner_text()
        assert not errors
        browser.close()


def test_client_elapsed_time_expires_cache_without_global_block():
    playwright = pytest.importorskip("playwright.sync_api")
    meta_at = datetime.now(timezone.utc) - timedelta(seconds=20)
    payloads = _payloads(
        _policy(age=TTL_SECONDS - 10), meta_at=meta_at,
    )
    with playwright.sync_playwright() as driver:
        browser, page, errors = _open_page(driver, payloads)

        state = page.evaluate("perpIdentityPolicyUiState()")
        assert state["sourceStatus"] == "research_only"
        assert state["status"] == "stale"
        assert state["expired"] is True and state["blocks"] is True
        assert state["cacheAgeSeconds"] == TTL_SECONDS
        panel = page.locator(
            '.perp-identity-policy[data-status="stale"]'
            '[data-blocks-identity="true"]'
        )
        assert "身份缓存过期 · 依赖扫描阻断" in panel.inner_text()
        assert "≥26h / 26h" in panel.inner_text()
        assert page.evaluate("runtimeSafetyUiState().blocks") is False
        assert page.locator(".runtime-safety-banner").count() == 0
        assert not errors
        browser.close()


def test_generated_clock_is_canonical_utc_and_future_fails_closed():
    playwright = pytest.importorskip("playwright.sync_api")
    payloads = _payloads(_policy())
    with playwright.sync_playwright() as driver:
        browser, page, errors = _open_page(driver, payloads)

        result = page.evaluate("""
            () => {
              const policy={...data.meta.perp_identity_policy};
              const evaluate=(generatedAt,nowMs)=>perpIdentityPolicyUiState({
                generated_at:generatedAt,perp_identity_policy:policy,
              },nowMs);
              const now=Date.parse("2026-07-16T12:00:00.123Z");
              return{
                z:evaluate("2026-07-16T12:00:00.123Z",now),
                offsetZero:evaluate("2026-07-16T12:00:00.123000+00:00",now),
                locale:evaluate("2026-07-16 12:00:00+00:00",now),
                nonZeroOffset:evaluate("2026-07-16T13:00:00.123+01:00",now),
                impossible:evaluate("2026-02-30T12:00:00.123Z",now),
                future:evaluate("2026-07-16T12:00:00.124Z",now),
              };
            }
        """)
        assert result["z"]["available"] is True
        assert result["offsetZero"]["available"] is True
        for key in ("locale", "nonZeroOffset", "impossible", "future"):
            assert result[key]["available"] is False
            assert result[key]["status"] == "invalid"
            assert result[key]["blocks"] is True
        assert not errors
        browser.close()


def test_exact_26h_boundary_and_timer_repaint_are_fail_closed_and_scoped():
    playwright = pytest.importorskip("playwright.sync_api")
    payloads = _payloads(_policy("verified"))
    with playwright.sync_playwright() as driver:
        browser, page, errors = _open_page(driver, payloads)

        boundary = page.evaluate("""
            () => {
              const meta={
                generated_at:"2026-07-16T12:00:00.000Z",
                perp_identity_policy:{
                  ...data.meta.perp_identity_policy,cache_age_seconds:300,
                },
              };
              const projected=Date.parse(meta.generated_at);
              const expiry=projected+(PERP_IDENTITY_TTL_SECONDS-300)*1000;
              return{
                before:perpIdentityPolicyUiState(meta,expiry-1),
                at:perpIdentityPolicyUiState(meta,expiry),
              };
            }
        """)
        assert boundary["before"]["status"] == "verified"
        assert boundary["before"]["expired"] is False
        assert boundary["before"]["blocks"] is False
        assert boundary["before"]["cacheAgeSeconds"] == TTL_SECONDS - 1
        assert boundary["at"]["status"] == "stale"
        assert boundary["at"]["expired"] is True
        assert boundary["at"]["blocks"] is True
        assert boundary["at"]["cacheAgeSeconds"] == TTL_SECONDS

        page.evaluate("setView('play',false)")
        cascade = page.locator(".market-coverage-card").filter(
            has_text="Cascade",
        ).first
        carry = page.locator(".market-coverage-card").filter(
            has_text="Carry",
        ).first
        lane_before = {
            "cascade_class": cascade.get_attribute("class"),
            "carry_class": carry.get_attribute("class"),
            "decision": page.locator(".decision-title").inner_text(),
            "a3": page.evaluate(A3_PROBE),
        }
        assert lane_before["a3"] == "A3_MANUAL_PROBE"
        page.evaluate("setView('perp',false)")
        timer_start = page.evaluate("""
            () => {
              const generatedAt=new Date(Date.now()).toISOString();
              data.meta={...data.meta,generated_at:generatedAt,
                perp_identity_policy:{...data.meta.perp_identity_policy,
                  cache_age_seconds:PERP_IDENTITY_TTL_SECONDS-1}};
              schedulePerpIdentityExpiry(data.meta);
              paint();
              return{
                policy:perpIdentityPolicyUiState(),
                runtime:runtimeSafetyUiState(),
              };
            }
        """)
        assert timer_start["policy"]["status"] == "verified"
        assert timer_start["policy"]["expired"] is False
        assert timer_start["policy"]["blocks"] is False
        assert timer_start["runtime"]["blocks"] is False
        carry_health_before = page.locator(".carry-health").inner_text()

        page.locator(
            '.perp-identity-policy[data-status="stale"]'
            '[data-blocks-identity="true"]'
        ).wait_for(state="visible", timeout=3500)
        assert page.evaluate("perpIdentityPolicyUiState().expired") is True
        assert page.evaluate("runtimeSafetyUiState().blocks") is False
        assert page.locator(".carry-health").inner_text() == carry_health_before
        assert page.evaluate(A3_PROBE) == lane_before["a3"]

        page.evaluate("setView('play',false)")
        cascade_after = page.locator(".market-coverage-card").filter(
            has_text="Cascade",
        ).first
        carry_after = page.locator(".market-coverage-card").filter(
            has_text="Carry",
        ).first
        assert cascade_after.get_attribute("class") == lane_before["cascade_class"]
        assert carry_after.get_attribute("class") == lane_before["carry_class"]
        assert page.locator(".decision-title").inner_text() == lane_before["decision"]
        assert "Hyperliquid · 本轮快照正常" in cascade_after.inner_text()
        assert "身份扩展 · 缓存过期 / 扫描阻断" in cascade_after.inner_text()
        assert not errors
        browser.close()


@pytest.mark.parametrize("kind", ["missing", "extra", "contradictory"])
def test_missing_or_malicious_policy_fails_closed_without_secret_or_lane_spill(
    kind,
):
    playwright = pytest.importorskip("playwright.sync_api")
    policy = None if kind == "missing" else _policy()
    if kind == "extra":
        policy = deepcopy(policy)
        policy["cache_path"] = "/LEAK_ME_SECRET_PATH"
    elif kind == "contradictory":
        policy = deepcopy(policy)
        policy["blocks_identity_dependent_scans"] = False
        policy["reason_codes"] = ["identity_cache_unavailable"]
    payloads = _payloads(policy)
    with playwright.sync_playwright() as driver:
        browser, page, errors = _open_page(driver, payloads)

        state = page.evaluate("perpIdentityPolicyUiState()")
        assert state["available"] is False
        assert state["status"] == "invalid" and state["blocks"] is True
        assert state["marketCount"] == state["researchMapped"] == 0
        panel = page.locator(
            '.perp-identity-policy[data-status="invalid"]'
            '[data-blocks-identity="true"]'
        )
        assert "身份状态不可验证 · 依赖扫描阻断" in panel.inner_text()
        assert page.locator(".carry-health").count() == 1
        assert page.evaluate("runtimeSafetyUiState().blocks") is False
        assert "LEAK_ME_SECRET_PATH" not in page.locator("body").inner_text()
        assert not errors
        browser.close()


def test_verified_identity_does_not_claim_trade_actionability():
    playwright = pytest.importorskip("playwright.sync_api")
    payloads = _payloads(_policy("verified"))
    with playwright.sync_playwright() as driver:
        browser, page, errors = _open_page(driver, payloads)

        panel = page.locator(
            '.perp-identity-policy[data-status="verified"]'
            '[data-blocks-identity="false"]'
        )
        text = panel.inner_text()
        assert "身份准入可用 · 仍非交易信号" in text
        assert "身份依赖扫描只解除资产身份门" in text
        assert "事件本身仍须单独验证，绝不自动执行" in text
        assert "已核验扫描身份\n1" in text
        assert "扫描未运行" not in text
        assert page.evaluate("perpIdentityPolicyUiState().blocks") is False
        assert page.evaluate("runtimeSafetyUiState().blocks") is False

        page.evaluate("setView('play',false)")
        cascade = page.locator(".market-coverage-card").filter(
            has_text="Cascade"
        ).first
        assert "身份扩展 · 已核验 1" in cascade.inner_text()
        assert "Hyperliquid · 本轮快照正常" in cascade.inner_text()
        assert not errors
        browser.close()
