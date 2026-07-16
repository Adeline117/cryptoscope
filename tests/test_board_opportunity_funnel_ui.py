"""Static contracts for the homepage opportunity funnel and decision cards."""
from pathlib import Path


BOARD = Path(__file__).parents[1] / "board" / "public" / "index.html"


def test_homepage_exposes_the_full_opportunity_funnel_and_data_health():
    html = BOARD.read_text()

    for phrase in (
        'aria-label="机会漏斗"',
        "发现 → 证据门 → 可执行 → 跟踪验证",
        "已发现 / 入账",
        "Launch 证据门",
        "当前可执行",
        "跟踪验证样本",
        "只统计已加载记录；不是全市场机会总数",
        "覆盖与视图按时只说明当前可判范围，不证明没有漏掉机会",
    ):
        assert phrase in html
    assert "function opportunityViewFreshness()" in html
    assert "stale_after_at" in html
    assert "freshness.stale" in html and "freshness.missing" in html


def test_launch_cards_show_execution_bounds_without_inventing_missing_values():
    html = BOARD.read_text()

    for phrase in (
        "function launchOpportunityCard(r)",
        "入场窗口 · 只读",
        "失效 / 离场",
        "系统无真实持仓，也不会自动离场",
        "仓位上限",
        "最大名义金额；未知时不得自行补值",
        "证据完整度",
        "风险未知 / 未齐",
        "跟踪不可判",
    ):
        assert phrase in html
    assert "const capRaw=ca.notional_usd??r?.max_notional_usd" in html
    assert 'Number.isFinite(cap)&&cap>0?usd(cap):"未知"' in html
    assert 'entry==null?"不可得"' in html
    assert 'invalidation==null?"不可判"' in html


def test_unknown_or_incomplete_evidence_cannot_receive_a_green_pass():
    html = BOARD.read_text()

    assert 'securityProviders&&securityClock&&!String(sg.reason||"").trim()?"pass":"unknown"' in html
    assert 'function launchCostGateState(contract)' in html
    assert 'contract.purpose!=="current_action"' in html
    assert 'allIn>=0&&allIn<=5' in html
    assert 'Math.abs(allIn-total)<=1e-6?"pass":"unknown"' in html
    assert 'quoteAt<=now&&ca.kind==="read_only_quote"' in html
    assert 'securityExpiry-securityAt<=300_000' in html
    assert 'ep.network_fees_included===true' in html
    assert 'launchCardStage(x).key==="actionable"' in html
    assert 'checked.passed===checked.gates.length&&!checked.blocked&&!checked.unknown' in html
    assert "A3 字段待复核" in html
    assert 'const riskCls=risk.state==="pass"?"b-good":risk.state==="block"?"b-danger":"b-warn"' in html
    assert 'g.state==="pass"?"通过":g.state==="block"?"阻断":"未知"' in html
    assert "不等于无风险" in html


def test_opportunity_funnel_and_cards_collapse_for_mobile():
    html = BOARD.read_text()

    assert ".opportunity-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr))" in html
    assert ".opportunity-grid{grid-template-columns:1fr}" in html
    assert ".opportunity-metrics{grid-template-columns:1fr}" in html
    assert ".funnel-steps{grid-template-columns:1fr 1fr}" in html
