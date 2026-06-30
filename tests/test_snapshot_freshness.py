"""Tests for the holder-snapshot freshness guard (catch stalled / frozen feeds).

Motivated by the live SIREN failure: its holder_snapshots froze ~2026-06-18
with byte-identical rows, and the stale top-holder list invented a 48% "whale"
that on-chain had already emptied. The guard must flag both failure modes:
  - STALLED: feed stops producing new rows → latest silently ages.
  - FROZEN:  feed keeps writing identical cached metrics.
"""

from datetime import datetime, timedelta, timezone

import pytest

from src.onchain.holder_snapshot import (
    _connect,
    save_snapshot,
    snapshot_freshness,
    find_stale_snapshots,
)


@pytest.fixture
def db(tmp_path):
    return tmp_path / "snap.db"


def _holders(top_bal: float):
    # Two holders; metrics derive from balances so changing top_bal changes the row.
    return [{"address": "0xaaa", "balance": top_bal},
            {"address": "0xbbb", "balance": 1000.0}]


def _save_at(db, token, chain, holders, dt):
    save_snapshot(token, chain, holders, snapshot_at=dt.isoformat(), db_path=db)


def test_no_snapshots_is_stale(db):
    v = snapshot_freshness("0xtok", "bsc", db_path=db)
    assert v["stale"] is True
    assert v["reason"] == "no_snapshots"


def test_recent_distinct_snapshot_is_fresh(db):
    now = datetime(2026, 6, 25, tzinfo=timezone.utc)
    _save_at(db, "0xtok", "bsc", _holders(5000), now - timedelta(hours=12))
    _save_at(db, "0xtok", "bsc", _holders(4000), now - timedelta(hours=6))
    v = snapshot_freshness("0xtok", "bsc", now=now, db_path=db)
    assert v["stale"] is False
    assert v["reason"] == "fresh"
    assert v["age_hours"] == 6.0


def test_stalled_feed_flagged(db):
    now = datetime(2026, 6, 25, tzinfo=timezone.utc)
    # Last (and only) snapshot is 30h old — past the 18h stall threshold.
    _save_at(db, "0xtok", "bsc", _holders(5000), now - timedelta(hours=30))
    v = snapshot_freshness("0xtok", "bsc", now=now, db_path=db)
    assert v["stale"] is True
    assert v["reason"] == "stalled"
    assert v["age_hours"] == 30.0


def test_frozen_identical_rows_flagged(db):
    now = datetime(2026, 6, 25, tzinfo=timezone.utc)
    # Three recent but byte-identical metric rows = a frozen cache (the SIREN bug).
    for h in (2, 4, 6):
        _save_at(db, "0xtok", "bsc", _holders(5000), now - timedelta(hours=h))
    v = snapshot_freshness("0xtok", "bsc", now=now, db_path=db)
    assert v["stale"] is True
    assert v["reason"] == "frozen"
    assert v["identical_run"] >= 3


def test_changing_metrics_not_frozen(db):
    now = datetime(2026, 6, 25, tzinfo=timezone.utc)
    for i, h in enumerate((2, 4, 6)):
        _save_at(db, "0xtok", "bsc", _holders(5000 + i * 100), now - timedelta(hours=h))
    v = snapshot_freshness("0xtok", "bsc", now=now, db_path=db)
    assert v["stale"] is False
    assert v["identical_run"] == 1


def test_find_stale_allowlist_only_watched_tokens(db):
    now = datetime(2026, 6, 25, tzinfo=timezone.utc)
    # watched + stalled (the SIREN case): flag it
    _save_at(db, "0xSiReN", "bsc", _holders(5000), now - timedelta(hours=40))
    # watched + fresh: don't flag
    _save_at(db, "0xfresh", "bsc", _holders(5000), now - timedelta(hours=12))
    _save_at(db, "0xfresh", "bsc", _holders(4000), now - timedelta(hours=3))
    # NOT watched but stalled: ignore (dormant candidate)
    _save_at(db, "0xdormant", "bsc", _holders(5000), now - timedelta(hours=300))
    # allowlist is case-insensitive
    bad = find_stale_snapshots(tokens=["0xsiren", "0xfresh"], now=now, db_path=db)
    toks = {b["token"] for b in bad}
    assert "0xSiReN" in toks
    assert "0xfresh" not in toks
    assert "0xdormant" not in toks


def test_find_stale_cadence_fallback_needs_min_snapshots(db):
    now = datetime(2026, 6, 25, tzinfo=timezone.utc)
    # 1 stale snapshot, no allowlist → below min_snapshots cadence → not flagged
    _save_at(db, "0xone", "bsc", _holders(5000), now - timedelta(hours=40))
    assert find_stale_snapshots(now=now, db_path=db) == []
    # 4 stale snapshots → had a cadence then stopped → flagged
    for h in (40, 46, 52, 58):
        _save_at(db, "0xfour", "bsc", _holders(5000 + h), now - timedelta(hours=h))
    bad = find_stale_snapshots(now=now, db_path=db)
    assert {b["token"] for b in bad} == {"0xfour"}
