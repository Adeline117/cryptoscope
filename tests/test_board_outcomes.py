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
        c.execute("INSERT INTO picks(ts,lane,symbol,chain,token,price0,hit_24h,resolved) "
                  "VALUES ('2026-01-01T00:00:00+00:00','opp','T',?,?,0.001,1,1)",
                  ("bsc", f"0x{i}"))
    c.commit(); c.close()
    st = bo.lane_stats()["opp"]
    assert st["verdict"] == "不可判"
    assert "rate" not in st


def test_lane_stats_quotes_wilson_at_min_n(bo):
    c = bo._conn()
    for i in range(bo.MIN_N):
        c.execute("INSERT INTO picks(ts,lane,symbol,chain,token,price0,hit_24h,resolved) "
                  "VALUES ('2026-01-01T00:00:00+00:00','opp','T',?,?,0.001,?,1)",
                  ("bsc", f"0x{i}", 1 if i < 7 else 0))
    c.commit(); c.close()
    st = bo.lane_stats()["opp"]
    assert st["verdict"] == "measured"
    assert st["n"] == bo.MIN_N and st["hits"] == 7
    assert st["lo"] < st["rate"] < st["hi"]      # Wilson interval brackets the point
