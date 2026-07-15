from pathlib import Path


BOARD = Path(__file__).parents[1] / "board" / "public" / "index.html"
BOARD_EXPORT = Path(__file__).parents[1] / "src" / "pipeline" / "board_export.py"
HYPERLIQUID = Path(__file__).parents[1] / "src" / "onchain" / "hyperliquid.py"
CARRY_PAPER = Path(__file__).parents[1] / "src" / "pipeline" / "carry_paper.py"
OPPORTUNITY_OUTCOMES = (
    Path(__file__).parents[1] / "src" / "pipeline" / "opportunity_outcomes.py"
)


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
    assert "盘口与费用假设，非全量成本" in html
    assert "订单簿纸面报价代理" in html
    assert "非实盘成交、实际资金费结算或完整收益" in html


def test_board_exposes_launch_quote_invalidation_and_measurement_clocks_honestly():
    html = BOARD.read_text()

    assert "function launchLifecycleHtml(r)" in html
    assert 'const clocks=[["1h",1],["24h",24],["7d",168]]' in html
    assert "进入窗口" in html
    assert "价格失效" in html
    assert "固定评估（不是平仓）" in html
    assert "收益扣除冻结的估算成本" in html
    assert "系统没有成交、没有持仓，也不会自动平仓" in html


def test_board_uses_side_aware_invalidation_for_short_events():
    html = BOARD.read_text()
    short_event = {"side": "SHORT", "entry_price": 100, "invalidation_price": 103}

    assert short_event["invalidation_price"] > short_event["entry_price"]
    assert "function invalidationCondition(r)" in html
    assert 'String(r?.side||"LONG").toUpperCase()==="SHORT"?"≥":"≤"' in html
    assert '价格 ${invalidationCondition(r)} 时论文失效' in html
    assert '价格 ≤ $${esc(r?.invalidation_price??"—")} 时论文失效' not in html


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


def test_structure_view_separates_legacy_inventory_deltas_from_new_listings():
    html = BOARD.read_text()

    assert "legacy_inventory_deltas" in html
    assert "legacy_inventory_delta" in html
    assert "旧库存差分" in html
    assert "不算独立新上币" in html
    assert "未独立核验为官方上币公告" in html


def test_airdrop_view_exposes_source_coverage_cost_and_risk_without_guessing():
    html = BOARD.read_text()

    assert 'r?.source_state==="source_verified"' in html
    assert "official_page_verified===true" in html
    assert "evidence_page_verified===true" in html
    assert "活动页 + 独立证据页" in html
    assert "人工维护且覆盖不完整" in html
    assert "不是全市场发现器" in html
    assert "未验证·不跳转" in html
    assert "（未验证，不提供跳转）" in html
    assert "source_evidence_url" in html
    assert "代码信任根" in html and "sv.checked_at" in html
    assert 'r.estimated_cost_usd==null?"未知"' in html
    assert "r.capital_required_usd" in html
    assert "r.kyc_required" in html
    assert "r.risk_notes" in html
    assert "域名已验证活动" not in html


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


def test_launch_detail_prefers_current_assessment_gate_evidence():
    html = BOARD.read_text()

    assert "const ca=r.current_assessment||{}" in html
    assert "sg=ca.security_gate||r.security_gate||{}" in html
    assert "ep=ca.execution_probe||r.execution_probe||{}" in html
    assert "sg=r.security_gate||{},ep=r.execution_probe||{}" not in html


def test_launch_coverage_counts_only_traceable_ledger_readbacks():
    html = BOARD.read_text()

    assert "traceable_unique_ledger_events" in html
    assert "orphan_unique_ledger_ids" in html
    assert "可追溯唯一账本事件" in html
    assert "orphan 隔离" in html
    assert "只有能从 opportunity ledger 精确回读的唯一 ID 才计入账" in html


def test_carry_hypothesis_never_claims_proven_edge_before_evidence():
    html = BOARD.read_text()

    assert "它尚不是已证明的正 EV" in html
    assert "Carry 优势证据" in html
    assert "当前没有通过证据门的真 edge" in html
    assert "部分模型代理/年" in html and "纸面结构" in html
    assert "唯一「个人真能做出正EV」" not in html
    assert "唯一有结构 edge 的" not in html


def test_carry_health_precedes_empty_state_and_distinguishes_unknown_from_empty():
    html = BOARD.read_text()
    render = html.split("function renderPerp(d){", 1)[1].split("// ---------- paint", 1)[0]

    assert "carryHealthHtml(health)" in render
    assert render.index("carryHealthHtml(health)") < render.index("if(carry.length)")
    for phrase in (
        "数据源正常",
        "数据源部分缺失",
        "数据源不可用",
        "缺失不会触发平仓，未知区间不累计资金费",
        "本轮无法判断是否存在 Carry 候选",
        "不代表全市场没有机会",
    ):
        assert phrase in html
    assert "okx.request_timeout" in html
    assert "当前无正资金费机会,市场杠杆也不拥挤" not in html


def test_carry_lifecycle_exposes_exit_pending_and_source_gaps_fail_closed():
    html = BOARD.read_text()

    for token in (
        'p.status==="exit_pending"',
        "p.measurement_state",
        '"source_gap","migration_gap"',
        "p.last_valid_at",
        "p.last_attempt_at",
        "p.unmeasured_h",
        "p.exit_signal_at",
        "p.exit_signal_diff_ann_pct",
        "退出待报价",
        "数据缺口·保持开启",
        "缺失不是退出",
        "不伪造平仓、成本或净额",
    ):
        assert token in html


def test_carry_evidence_separates_valid_total_and_quarantined_samples():
    html = BOARD.read_text()

    assert "n_closed_total" in html
    assert "n_closed_excluded" in html
    assert "excluded_by_reason" in html
    assert "有效报价代理关闭" in html
    assert "排除，不进入优势判决" in html
    assert "总关闭" in html
    assert "真实优势样本" in html
    assert "real_edge_n" in html
    assert "inconsistent_proxy_math" in html
    assert "代理数学不一致" in html


def test_carry_ui_forbids_realized_or_complete_cost_claims():
    combined = BOARD.read_text() + BOARD_EXPORT.read_text() + HYPERLIQUID.read_text()

    for forbidden in (
        "或市场缺失",
        "已实现年化(毛)",
        "这才是真数",
        "给真实持仓天数和净额",
        "绝对净收益",
        "唯一对个人可复制的正EV核",
        "ONE replicable positive-EV core",
        '"realized_ann"',
        "已实现carry",
        "pp.avg_net_return_pct",
        "pp.avg_funding_accrued_pct",
        "pp.avg_cost_pct",
        "c.net_ann",
        "x.net_ann",
        "c.hold_measured",
        "c.hold_days",
        "净>0·验证中",
    ):
        assert forbidden not in combined
    assert "报价费率年化代理" in combined
    assert "不是仓位建议或可成交上限" in combined
    assert "不能当作 all-in 净收益或仓位建议" in combined
    assert "x.cross===true&&x.partial_model_proxy_ann_pct>0" in combined
    assert "单所现货对冲" in combined
    assert "不进入双腿账本" in combined


def test_carry_public_contract_forbids_legacy_profit_metric_names():
    html = BOARD.read_text()
    tracker = CARRY_PAPER.read_text()
    outcomes = OPPORTUNITY_OUTCOMES.read_text()

    assert "c.edge_ann" not in html
    assert "c.gross_funding_diff_ann_pct" in html
    assert "毛资金费差" in html
    assert "pct2(p.entry_diff_ann_pct)" in html
    assert "pct2(p.last_diff_ann_pct)" in html
    assert '"funding_accrued_pct"' not in tracker
    assert '"net_return_pct"' not in tracker
    assert "absolute_net_return_after_complete_paper_book_costs" not in outcomes
    assert "quote_rate_integral_minus_book_quotes_and_modeled_fee_proxy" in outcomes


def test_carry_mobile_layout_prioritizes_health_and_lifecycle():
    html = BOARD.read_text()

    assert 'body[data-view="perp"] .stat{grid-template-columns:repeat(2,1fr)}' in html
    assert ".carry-health-grid{grid-template-columns:1fr 1fr}" in html
    assert ".carry-life-grid{grid-template-columns:1fr}" in html
    assert ".sc-card .k,.evidence-reasons{grid-column:1/-1}" in html
    assert "左右滑动查看更多 →" in html


def test_carry_ui_discloses_quarantined_legacy_open_episodes():
    html = BOARD.read_text()

    assert "n_quarantined_total" in html
    assert "旧协议 open 隔离" in html


def test_carry_ui_discloses_bounded_entry_quote_capacity():
    html = BOARD.read_text()

    assert "new_entry_quote_attempted" in html
    assert "new_entry_quote_cap" in html
    assert "new_entry_candidates_deferred" in html
    assert "容量延后" in html
    assert 'new_entry_quote_attempted??"?"' in html
    assert 'new_entry_candidates_deferred??"?"' in html


def test_decision_overview_separates_actionable_windows_from_paper_candidates():
    html = BOARD.read_text()

    assert 'aria-label="当前决策"' in html
    assert "等待 · 当前不入场" in html
    assert "新鲜报价+完整成本+证据门+送达SLA" in html
    assert "模型代理为正，成本组件仍不全" in html
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
