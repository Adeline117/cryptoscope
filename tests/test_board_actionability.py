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
