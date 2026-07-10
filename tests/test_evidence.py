"""The scoreboard must not be able to lie again.

The 44% that got quoted to the user came from 40 alert ROWS fired inside 17 minutes
on ONE token, counted as 40 independent trials. These tests pin the contract that
makes that impossible.
"""

import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

from src.pipeline.evidence import EPISODE_GAP_MIN, episodes, wilson


def _db(tmp_path, rows):
    """rows = [(ts, token, chain, symbol, direction, phase, hit_24h, resolved)]"""
    p = tmp_path / "alerts.db"
    c = sqlite3.connect(p)
    c.execute("""CREATE TABLE alerts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts TEXT, token TEXT, chain TEXT, symbol TEXT, kind TEXT, direction TEXT,
        price0 REAL, liquidity REAL, price_4h REAL, price_24h REAL,
        hit_4h INTEGER, hit_24h INTEGER, resolved INTEGER DEFAULT 0, phase TEXT)""")
    for ts, tok, ch, sym, d, ph, hit, res in rows:
        c.execute("INSERT INTO alerts (ts,token,chain,symbol,kind,direction,price0,"
                  "liquidity,hit_24h,resolved,phase) VALUES (?,?,?,?,'k',?,1.0,1e6,?,?,?)",
                  (ts, tok, ch, sym, d, hit, res, ph))
    c.commit()
    c.close()
    return p


def test_burst_of_repeats_is_one_episode(tmp_path):
    """THE REGRESSION: 40 fires in 17 minutes on one token = ONE bet, not 40 wins."""
    t0 = datetime(2026, 6, 16, 9, 6, 18, tzinfo=timezone.utc)
    rows = [((t0 + timedelta(seconds=26 * i)).isoformat(),
             "0xsiren", "bsc", "SIREN", "short", "sell", 1, 1) for i in range(40)]
    eps = episodes(_db(tmp_path, rows))
    assert len(eps) == 1, f"40 repeats collapsed to {len(eps)} episodes, expected 1"
    assert eps[0]["n_fires"] == 40
    assert eps[0]["hit_24h"] == 1          # the episode did hit — once.


def test_gap_starts_a_new_episode(tmp_path):
    t0 = datetime(2026, 6, 16, 9, 0, tzinfo=timezone.utc)
    later = t0 + timedelta(minutes=EPISODE_GAP_MIN + 1)
    rows = [(t0.isoformat(), "0xa", "bsc", "A", "short", "sell", 0, 1),
            (later.isoformat(), "0xa", "bsc", "A", "short", "sell", 1, 1)]
    assert len(episodes(_db(tmp_path, rows))) == 2


def test_phase_change_starts_a_new_episode_inside_cooldown(tmp_path):
    """A operator flipping buy->sell is a NEW event even 1 minute later."""
    t0 = datetime(2026, 6, 16, 9, 0, tzinfo=timezone.utc)
    rows = [(t0.isoformat(), "0xa", "bsc", "A", "short", "buy", 0, 1),
            ((t0 + timedelta(minutes=1)).isoformat(), "0xa", "bsc", "A", "short", "sell", 1, 1)]
    assert len(episodes(_db(tmp_path, rows))) == 2


def test_majors_sentiment_rows_are_excluded(tmp_path):
    """chain='majors' rows are BTC/ETH/SOL sentiment, not operator calls. They padded
    the old denominator with 40 rows that could never be operator hits."""
    t0 = datetime(2026, 6, 16, 9, 0, tzinfo=timezone.utc)
    rows = [(t0.isoformat(), "ETH", "majors", "ETH", "short", None, 0, 1),
            (t0.isoformat(), "0xa", "bsc", "A", "short", "sell", 1, 1)]
    eps = episodes(_db(tmp_path, rows))
    assert len(eps) == 1 and eps[0]["symbol"] == "A"


def test_episode_entry_is_the_first_fire(tmp_path):
    """You could only have traded the FIRST fire's price. Later fires are hindsight."""
    t0 = datetime(2026, 6, 16, 9, 0, tzinfo=timezone.utc)
    p = tmp_path / "alerts.db"
    c = sqlite3.connect(p)
    c.execute("""CREATE TABLE alerts (id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts TEXT, token TEXT, chain TEXT, symbol TEXT, kind TEXT, direction TEXT,
        price0 REAL, liquidity REAL, price_4h REAL, price_24h REAL,
        hit_4h INTEGER, hit_24h INTEGER, resolved INTEGER DEFAULT 0, phase TEXT)""")
    for i, px in enumerate([10.0, 8.0, 6.0]):     # price falling during the episode
        c.execute("INSERT INTO alerts (ts,token,chain,symbol,kind,direction,price0,"
                  "liquidity,hit_24h,resolved,phase) VALUES (?,?,?,?,'k','short',?,1e6,1,1,'sell')",
                  ((t0 + timedelta(minutes=i)).isoformat(), "0xa", "bsc", "A", px))
    c.commit(); c.close()
    eps = episodes(p)
    assert len(eps) == 1
    assert eps[0]["price0"] == 10.0, "entry must be the first fire, not a later one"


@pytest.mark.parametrize("k,n,lo_max,hi_min", [(1, 16, 0.05, 0.20), (1, 6, 0.10, 0.40)])
def test_wilson_is_wide_at_small_n(k, n, lo_max, hi_min):
    """At n=16 the interval must be wide enough that no one quotes the point estimate."""
    lo, hi = wilson(k, n)
    assert lo < lo_max and hi > hi_min
    assert 0.0 <= lo <= hi <= 1.0
