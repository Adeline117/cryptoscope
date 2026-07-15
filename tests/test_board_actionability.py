from pathlib import Path


BOARD = Path(__file__).parents[1] / "board" / "public" / "index.html"


def test_board_uses_effective_decision_and_client_side_expiry():
    html = BOARD.read_text()

    assert "function effectiveDecision(r)" in html
    assert 'Date.now()>=expiry' in html
    assert 'effectiveDecision(x)==="SMALL_PROBE"' in html
    assert "原报价已经过期，禁止按旧价格行动" in html
    assert "已过期·历史方向" in html
    assert "只读报价非成交" in html


def test_board_exposes_carry_entry_exit_clocks_without_calling_them_fills():
    html = BOARD.read_text()

    assert "function utcClock(ts)" in html
    assert "Carry 进入 / 离开生命周期" in html
    assert "p.entry_at" in html and "p.last_measured_at" in html
    assert "p.closed_at" in html and "p.close_reason" in html
    assert "测得纸面成本·非成交" in html
    assert "只读盘口纸面测量" in html


def test_board_exposes_launch_quote_invalidation_and_measurement_clocks_honestly():
    html = BOARD.read_text()

    assert "function launchLifecycleHtml(r)" in html
    assert 'const clocks=[["1h",1],["24h",24],["7d",168]]' in html
    assert "进入窗口" in html
    assert "价格失效" in html
    assert "固定评估（不是平仓）" in html
    assert "收益扣除冻结的估算成本" in html
    assert "系统没有成交、没有持仓，也不会自动平仓" in html


def test_legacy_discovery_views_cannot_bypass_canonical_trade_gates():
    html = BOARD.read_text()

    assert "未过统一安全/路由门" in html
    assert "任何行在进入 Launch 账本并通过完整门禁前都只能观察" in html
    assert "不属于统一机会账本，也不是入场清单" in html
    assert "只用于避开，不生成做空指令" in html
    assert "🐋 钱包活动" in html and "🔎 旧版线索" in html
    assert "快查合约再小仓埋伏" not in html
    assert "能埋伏,小仓+止损" not in html
    assert "要动只能空" not in html
    assert "派发(可做空/避开)" not in html


def test_structure_view_discloses_per_source_coverage_failures():
    html = BOARD.read_text()

    assert "来源本轮可达" in html
    assert "配置了来源不等于成功扫描" in html
    assert 'x.status!=="ok"' in html
    assert "失败源不会计入覆盖" in html


def test_cascade_distinguishes_watch_from_expired_and_shows_full_lifecycle():
    html = BOARD.read_text()

    assert "观察·历史方向" in html
    assert "ed==='EXPIRED'?'已过期·历史方向" in html
    assert "${launchLifecycleHtml(e)}" in html
    assert "e.actionability_reason" in html


def test_launch_action_requires_statistical_evidence_gate():
    html = BOARD.read_text()

    assert "安全+路由+报价+证据门" in html
    assert "成本后 24h 试验组相对同期 WATCH 对照的证据门" in html
    assert "只做纸面结算，不可行动" in html
    assert "优势门:" in html
