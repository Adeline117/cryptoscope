"""The board charts only recorded event prices and preserves OSS provenance."""
from __future__ import annotations

import hashlib
from pathlib import Path


ROOT = Path(__file__).parents[1]
BOARD = ROOT / "board" / "public" / "index.html"
VENDOR = ROOT / "board" / "public" / "vendor"


def test_lightweight_charts_is_pinned_vendored_and_licensed():
    html = BOARD.read_text()
    asset = VENDOR / "lightweight-charts-5.2.0.js"
    license_text = (VENDOR / "lightweight-charts-LICENSE.txt").read_text()
    notice_text = (VENDOR / "lightweight-charts-NOTICE.txt").read_text()
    third_party = (VENDOR / "THIRD_PARTY_NOTICES.txt").read_text()

    assert asset.exists()
    assert hashlib.sha256(asset.read_bytes()).hexdigest() == (
        "c0992580867c4912cc9385b3c2728315bcc1a76c7f1087dca908430fccdf31d7"
    )
    assert "Apache License" in license_text and "Version 2.0" in license_text
    assert "TradingView Lightweight Charts" in notice_text
    assert "fancy-canvas@2.1.0 (MIT)" in third_party
    assert "tslib@2.8.1 (0BSD)" in third_party
    assert '<script src="/vendor/lightweight-charts-5.2.0.js"></script>' in html
    assert "cdn.jsdelivr" not in html and "unpkg.com" not in html
    assert "Charts by TradingView" in html


def test_event_timeline_draws_only_discrete_recorded_prices_and_explicit_gaps():
    html = BOARD.read_text()

    for token in (
        "function eventEvidenceTimelineHtml(r)",
        "function eventChartModel(r)",
        "function mountEventChart(node)",
        "lineVisible:false,pointMarkersVisible:true",
        "series.setData([{time:point.time,value:point.value}])",
        "anchor.createPriceLine",
        "attributionLogo:true",
        "outcome.unavailable_horizons",
        "历史价格不可得 · 数据断点",
        "点与点之间没有价格路径",
        "缺失时保留时钟断点，不插值、不补 K 线",
        "固定评估说成真实平仓",
        "没有真实价格点，拒绝绘制推测曲线",
        "论文失效·非止损单",
        "系统没有成交、持仓或自动退出",
    ):
        assert token in html

    assert "CandlestickSeries" not in html
    assert "addCandlestickSeries" not in html
    assert "纸面关闭" not in html


def test_event_timeline_uses_lane_specific_read_only_quote_evidence():
    html = BOARD.read_text()

    assert "ca.entry_reference_price??probe.average_price" in html
    assert "ca.quote_at||probe.quote_at||r?.quote_at" in html
    assert "首次发现观测（非成交）" in html
    assert "当前只读报价（非成交）" in html
    assert "报价窗关闭不等于离场" in html


def test_launch_and_cascade_details_mount_and_dispose_charts_lazily():
    html = BOARD.read_text()

    assert "${eventEvidenceTimelineHtml(r)}${launchLifecycleHtml(r)}" in html
    assert "${eventEvidenceTimelineHtml(e)}${launchLifecycleHtml(e)}" in html
    assert "requestAnimationFrame(()=>mountEventCharts(ex))" in html
    assert "else disposeEventCharts(ex)" in html
    assert "details[data-chart-details]" in html
    assert "disposeEventCharts();eventChartRows.clear();eventChartSeq=0" in html
