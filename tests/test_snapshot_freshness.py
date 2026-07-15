"""Tests for the holder-snapshot freshness guard.

Motivated by the live SIREN failure: its holder_snapshots froze ~2026-06-18
with byte-identical rows, and the stale top-holder list invented a 48% "whale"
that on-chain had already emptied. A feed that stops producing rows must fail
closed; an inactive token whose freshly fetched state is unchanged must not be
misclassified as a provider failure.
"""

from datetime import datetime, timedelta, timezone

import pytest

from src.onchain.holder_snapshot import (
    _connect,
    get_holders_history,
    list_tokens,
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


def _save_legacy_exact_case(db, token, chain, holders, dt):
    """Insert a pre-canonicalization row exactly as an older DB stored it."""
    import json

    conn = _connect(db)
    metrics = {
        "holder_count": len(holders),
        "top10_pct": 100.0,
        "top25_pct": 100.0,
        "gini": 0.0,
        "total_supply_observed": sum(h["balance"] for h in holders),
    }
    try:
        conn.execute(
            """INSERT INTO holder_snapshots
               (token, chain, snapshot_at, holder_count, top10_pct, top25_pct,
                gini, total_supply_observed, holders_json)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                token, chain, dt.isoformat(), metrics["holder_count"],
                metrics["top10_pct"], metrics["top25_pct"], metrics["gini"],
                metrics["total_supply_observed"], json.dumps(holders),
            ),
        )
        conn.commit()
    finally:
        conn.close()


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
    # The opportunistic snapshot path has had no observation for well over 18h.
    _save_at(db, "0xtok", "bsc", _holders(5000), now - timedelta(hours=31))
    v = snapshot_freshness("0xtok", "bsc", now=now, db_path=db)
    assert v["stale"] is True
    assert v["reason"] == "stalled"
    assert v["age_hours"] == 31.0


def test_identical_fresh_rows_are_static_not_source_failure(db):
    now = datetime(2026, 6, 25, tzinfo=timezone.utc)
    # A freshly queried inactive token may have exactly the same holder state.
    for h in (2, 4, 6):
        _save_at(db, "0xtok", "bsc", _holders(5000), now - timedelta(hours=h))
    v = snapshot_freshness("0xtok", "bsc", now=now, db_path=db)
    assert v["stale"] is False
    assert v["reason"] == "static"
    assert v["identical_run"] >= 3


def test_stalled_feed_remains_failure_when_old_rows_are_identical(db):
    now = datetime(2026, 6, 25, tzinfo=timezone.utc)
    for h in (36, 42, 48):
        _save_at(db, "0xtok", "bsc", _holders(5000), now - timedelta(hours=h))
    v = snapshot_freshness("0xtok", "bsc", now=now, db_path=db)
    assert v["stale"] is True
    assert v["reason"] == "stalled"
    assert v["identical_run"] == 3


def test_daily_snapshot_is_fresh_during_scheduler_grace(db):
    now = datetime(2026, 6, 25, tzinfo=timezone.utc)
    _save_at(db, "0xgrace", "bsc", _holders(5000), now - timedelta(hours=29))
    _save_at(db, "0xlate", "bsc", _holders(5000), now - timedelta(hours=31))

    # find_stale_snapshots is the daily tracked-universe/health path: 24h cadence
    # plus 6h grace. The lower-level opportunistic freshness default remains 18h.
    bad = find_stale_snapshots(
        tokens=["0xgrace", "0xlate"], now=now, db_path=db,
    )
    assert {row["token"] for row in bad} == {"0xlate"}


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
    assert "0xsiren" in toks
    assert "0xfresh" not in toks
    assert "0xdormant" not in toks


def test_evm_checksum_aliases_merge_without_stale_false_positive(db):
    now = datetime(2026, 7, 15, tzinfo=timezone.utc)
    checksum = "0xAbCdEf1234567890"
    lower = checksum.lower()

    # Reproduce production: an old checksum-cased row stopped, while the scheduler
    # continued writing the same EVM contract under its lowercase canonical id.
    _save_legacy_exact_case(
        db, checksum, "bsc", _holders(5000), now - timedelta(hours=80),
    )
    _save_at(db, checksum, "bsc", _holders(4000), now - timedelta(hours=2))

    verdict = snapshot_freshness(checksum, "bsc", now=now, db_path=db)
    assert verdict["stale"] is False
    assert verdict["latest"] == (now - timedelta(hours=2)).isoformat()
    assert find_stale_snapshots(
        tokens=[checksum], now=now, db_path=db,
    ) == []
    assert list_tokens(db_path=db) == [(lower, "bsc")]
    assert len(get_holders_history(checksum, "bsc", db_path=db)) == 2


def test_solana_mint_identity_remains_case_sensitive(db):
    now = datetime(2026, 7, 15, tzinfo=timezone.utc)
    mint = "AbCdEfSolanaMint"
    _save_at(db, mint, "solana", _holders(5000), now - timedelta(hours=2))

    assert list_tokens(db_path=db) == [(mint, "solana")]
    assert snapshot_freshness(
        mint.lower(), "solana", now=now, db_path=db,
    )["reason"] == "no_snapshots"
    assert get_holders_history(mint.lower(), "solana", db_path=db) == []


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
