"""Board measurement layer — honesty gates frozen (no network; resolve needs live
price so it's exercised at runtime, not in CI)."""
from __future__ import annotations

import sqlite3

import pytest


@pytest.fixture
def bo(tmp_path, monkeypatch):
    import src.pipeline.board_outcomes as bo
    monkeypatch.setattr(bo, "DB", tmp_path / "picks.db")
    return bo


def test_log_picks_dedups_same_token_within_window(bo):
    p = {"symbol": "X", "chain": "bsc", "token": "0xabc", "price0": 0.001}
    assert bo.log_picks("opp", [p]) == 1
    # same (lane, token) again within DEDUP_HOURS → not re-logged (episode discipline)
    assert bo.log_picks("opp", [p]) == 0
    n = sqlite3.connect(str(bo.DB)).execute("SELECT COUNT(*) FROM picks").fetchone()[0]
    assert n == 1


def test_log_picks_skips_no_price(bo):
    # a pick with no entry price can never be scored — never logged
    assert bo.log_picks("opp", [{"symbol": "X", "chain": "bsc", "token": "0xd", "price0": 0}]) == 0
    assert bo.log_picks("opp", [{"symbol": "X", "chain": "bsc", "token": "0xe"}]) == 0


def test_lane_stats_refuses_number_below_min_n(bo):
    # inject 5 resolved picks (< MIN_N) → must be 不可判, never a quoted rate
    c = bo._conn()
    for i in range(5):
        c.execute("INSERT INTO picks(ts,lane,symbol,chain,token,price0,price_24h,resolved) "
                  "VALUES ('2026-01-01T00:00:00+00:00','opp','T',?,?,1.0,1.1,1)",
                  ("bsc", f"0x{i}"))
    c.commit(); c.close()
    st = bo.lane_stats()["opp"]
    assert st["verdict"] == "不可判"
    assert "rate" not in st


def test_lane_stats_computes_lift_and_edge_at_min_n(bo):
    # 7 of MIN_N hit (+6% > flat 5%), base_rate 0.25 each → apples-to-apples lift
    c = bo._conn()
    for i in range(bo.MIN_N):
        c.execute("INSERT INTO picks(ts,lane,symbol,chain,token,price0,price_24h,base_rate,resolved) "
                  "VALUES ('2026-01-01T00:00:00+00:00','opp','T',?,?,1.0,?,0.25,1)",
                  ("bsc", f"0x{i}", 1.06 if i < 7 else 0.98))
    c.commit(); c.close()
    st = bo.lane_stats()["opp"]
    assert st["verdict"] == "measured"
    assert st["n"] == bo.MIN_N and st["hits"] == 7           # scored at flat 5% from prices
    assert st["lo"] < st["rate"] < st["hi"]                  # Wilson brackets the point
    assert st["base_rate"] == 0.25 and st["lift"] == round((7 / bo.MIN_N) / 0.25, 2)
    assert st["edge"] in ("有edge迹象", "接近随机", "无edge/负")


def test_stats_render_is_read_only_and_never_runs_legacy_price_backfill(monkeypatch):
    from src.pipeline import board_export, board_outcomes, opportunity_outcomes

    def forbidden(*args, **kwargs):
        raise AssertionError("stats render attempted a legacy write or price backfill")

    monkeypatch.setattr(board_outcomes, "log_picks", forbidden)
    monkeypatch.setattr(board_outcomes, "resolve", forbidden)
    monkeypatch.setattr(board_outcomes, "lane_stats", lambda: {
        "opp": {"verdict": "measured", "edge": "无edge/负"},
    })
    monkeypatch.setattr(opportunity_outcomes, "lane_stats", lambda: {
        "launch": {"verdict": "不可判", "edge_verdict": "不可判"},
    })

    payload = board_export.render_stats({"opportunities": [{"price": 1.0}]})
    assert payload["lanes"]["opp"]["edge"] == "无edge/负"
    assert payload["lanes"]["launch"]["edge_verdict"] == "不可判"
    assert "冻结历史" in payload["note"]
