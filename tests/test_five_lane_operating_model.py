from pathlib import Path


MODEL = Path(__file__).parents[1] / "docs" / "FIVE_LANE_OPERATING_MODEL.md"


def test_operating_model_matches_the_five_live_product_lanes_and_boundaries():
    text = MODEL.read_text()

    for lane in ("| Launch |", "| Cascade |", "| Structure |", "| Airdrop |",
                 "| Carry |"):
        assert text.count(lane) == 1
    assert "| Convexity |" not in text
    assert "不是全链覆盖" in text
    assert "仅覆盖 Hyperliquid" in text
    assert "失败源单列" in text
    assert "覆盖不完整" in text
    assert "尚无实际结算和完整 all-in 成本" in text
    assert "real_edge_n" in text and "不可判" in text
    assert "资金费净额和领取净回报账本" not in text
