"""Regression tests for stale-data-in-realtime bugs.

A 3-month-old item ("iOS Warfare", 2026-03-01) once leaked into the realtime 2h
highlight because the recency filter never checked the timestamp. The same class
of bug could pollute the accumulation slope with stale snapshots.
"""

from datetime import datetime, timedelta, timezone

from src.collectors.base import CollectedItem
from src.pipeline.highlight_pipeline import _is_genuinely_recent, HIGHLIGHT_MAX_AGE_HOURS


def _item(hours_old=None, naive_old=False, **meta):
    if naive_old:
        pub = datetime(2026, 3, 1)  # tz-naive, 3 months old
    elif hours_old is None:
        pub = None
    else:
        pub = datetime.now(timezone.utc) - timedelta(hours=hours_old)
    return CollectedItem(id="x", title="t", published_at=pub, metadata=meta)


def test_highlight_drops_stale_dated_item():
    # The exact reported case: a dated item far outside the window.
    stale = CollectedItem(
        id="ios", title="iOS Warfare",
        published_at=datetime(2026, 3, 1, tzinfo=timezone.utc),
    )
    assert _is_genuinely_recent(stale) is False


def test_highlight_keeps_fresh_item():
    assert _is_genuinely_recent(_item(hours_old=2)) is True


def test_highlight_drops_naive_stale_item():
    # tz-naive old timestamps must also be caught (assumed UTC).
    assert _is_genuinely_recent(_item(naive_old=True)) is False


def test_highlight_keeps_undated_item():
    # On-chain/GitHub items often have no date — kept (inherently "now").
    assert _is_genuinely_recent(_item(hours_old=None)) is True


def test_highlight_boundary():
    assert _is_genuinely_recent(_item(hours_old=HIGHLIGHT_MAX_AGE_HOURS - 1)) is True
    assert _is_genuinely_recent(_item(hours_old=HIGHLIGHT_MAX_AGE_HOURS + 1)) is False


def test_snapshot_history_since_window(tmp_path):
    from src.onchain import holder_snapshot as hs

    db = tmp_path / "snap.db"
    old_ts = "2026-03-01T00:00:00+00:00"
    new_ts = datetime.now(timezone.utc).isoformat()
    hs.save_snapshot("TKN", "ethereum", [{"address": "0x1", "balance": 100}],
                     snapshot_at=old_ts, db_path=db)
    hs.save_snapshot("TKN", "ethereum", [{"address": "0x1", "balance": 200}],
                     snapshot_at=new_ts, db_path=db)

    all_hist = hs.get_holders_history("TKN", "ethereum", db_path=db)
    assert len(all_hist) == 2  # unbounded sees both

    cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
    windowed = hs.get_holders_history("TKN", "ethereum", since=cutoff, db_path=db)
    assert len(windowed) == 1  # stale 3-month-old snapshot excluded
    assert windowed[0][1][0]["balance"] == 200


def test_health_collect_and_format():
    from src.ops import health

    stats = health.collect_stats()
    assert "snapshots" in stats and "signals" in stats and "scheduler" in stats
    report = health.format_report(stats)
    assert "健康看板" in report
    tg = health.format_telegram(stats)
    assert "系统健康" in tg
