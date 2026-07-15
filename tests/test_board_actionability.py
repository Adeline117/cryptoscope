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
