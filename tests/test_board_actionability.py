from pathlib import Path


BOARD = Path(__file__).parents[1] / "board" / "public" / "index.html"


def test_board_uses_effective_decision_and_client_side_expiry():
    html = BOARD.read_text()

    assert "function effectiveDecision(r)" in html
    assert 'Date.now()>=expiry' in html
    assert 'actionLevel(x)==="A3_MANUAL_PROBE"' in html
    assert '["A2_PAPER_READY","A3_MANUAL_PROBE"].includes(level)' in html
    assert "旧数据、未知项或报价过期，一律只观察" in html
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

    assert "完整成本+报价+证据+送达SLA" in html
    assert "成本后 24h 试验组相对同期 WATCH 对照的证据门" in html
    assert "安全与路由可供纸面追踪，但成本、优势或送达条件未齐，不可入场" in html
    assert "优势门:" in html


def test_carry_hypothesis_never_claims_proven_edge_before_evidence():
    html = BOARD.read_text()

    assert "它尚不是已证明的正 EV" in html
    assert "Carry 优势证据" in html
    assert "当前没有通过证据门的真 edge" in html
    assert "模型净额/年" in html and "纸面结构" in html
    assert "唯一「个人真能做出正EV」" not in html
    assert "唯一有结构 edge 的" not in html


def test_decision_overview_separates_actionable_windows_from_paper_candidates():
    html = BOARD.read_text()

    assert 'aria-label="当前决策"' in html
    assert "等待 · 当前不入场" in html
    assert "新鲜报价+完整成本+证据门+送达SLA" in html
    assert "模型净额为正，不等于已获证" in html
    assert "不代表全市场覆盖" in html
    assert 'data-jump="launch"' in html
    assert 'data-jump="perp"' in html
    assert 'data-jump="avoid"' in html


def test_board_navigation_and_details_are_keyboard_accessible_and_shareable():
    html = BOARD.read_text()

    assert 'role="tabpanel"' in html
    assert 'aria-selected' in html
    assert 'aria-expanded="false"' in html
    assert 'e.key==="Enter"||e.key===" "' in html
    assert 'location.hash.slice(1)' in html
    assert 'addEventListener("popstate"' in html
    assert "左右滑动查看更多" in html


def test_launch_defaults_to_actionable_only_instead_of_burying_decision_in_watch_rows():
    html = BOARD.read_text()

    assert 'filterState={launch:"actionable"}' in html
    assert "当前没有通过全部门禁且报价仍有效的机会" in html
    assert 'label:"A1 观察"' in html
    assert 'type="search"' in html
    assert "搜索代币 / 合约" in html


def test_launch_uses_fail_closed_a0_to_a4_action_levels():
    html = BOARD.read_text()

    assert "function actionLevel(r)" in html
    for level in ("A0_BLOCKED", "A1_WATCH", "A2_PAPER_READY",
                  "A3_MANUAL_PROBE", "A4_REAL_FILL_VALIDATED"):
        assert level in html
    assert "首次发现价（不是当前入场价）" in html
    assert "当前只读报价参考" in html
    assert "自动交易: <b>永不允许</b>" in html
    assert "入场参考" not in html


def test_launch_discloses_primary_stream_coverage_and_known_gaps():
    html = BOARD.read_text()

    assert "function renderLaunchCoverage(d)" in html
    assert "Pump.fun 主链发行流" in html
    assert "资格检查队列" in html
    assert "已知覆盖缺口" in html
    assert "不代表全市场覆盖" in html
    assert "失败会退避，不会伪装成无机会" in html
    assert "主链原始证据:" in html
    assert "EVM 官方工厂流" in html
    assert "EVM 精确池资格" in html
    assert "BSC / Base / Ethereum 共 5 条工厂流" in html
    assert "只匹配工厂原始 pool" in html
