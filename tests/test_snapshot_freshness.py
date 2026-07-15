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
    get_snapshots,
    list_tokens,
    save_snapshot,
    snapshot_token,
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
    assert v["source_healthy"] is False
    assert v["dynamic_evidence_eligible"] is False
    assert v["reason"] == "no_snapshots"


def test_unsupported_chain_freshness_fails_closed_without_raising(db):
    verdict = snapshot_freshness("0xtok", "etheruem", db_path=db)

    assert verdict["stale"] is True
    assert verdict["source_healthy"] is False
    assert verdict["reason"] == "unsupported_chain"
    assert verdict["dynamic_evidence_eligible"] is False
    assert get_holders_history("0xtok", "etheruem", db_path=db) == []


def test_recent_distinct_snapshot_is_fresh(db):
    now = datetime(2026, 6, 25, tzinfo=timezone.utc)
    _save_at(db, "0xtok", "bsc", _holders(5000), now - timedelta(hours=12))
    _save_at(db, "0xtok", "bsc", _holders(4000), now - timedelta(hours=6))
    v = snapshot_freshness("0xtok", "bsc", now=now, db_path=db)
    assert v["stale"] is False
    assert v["source_healthy"] is True
    assert v["reason"] == "fresh"
    assert v["currentness"] == "observed_change"
    assert v["dynamic_evidence_eligible"] is True
    assert v["age_hours"] == 6.0


def test_stalled_feed_flagged(db):
    now = datetime(2026, 6, 25, tzinfo=timezone.utc)
    # The opportunistic snapshot path has had no observation for well over 18h.
    _save_at(db, "0xtok", "bsc", _holders(5000), now - timedelta(hours=31))
    v = snapshot_freshness("0xtok", "bsc", now=now, db_path=db)
    assert v["stale"] is True
    assert v["source_healthy"] is False
    assert v["dynamic_evidence_eligible"] is False
    assert v["reason"] == "stalled"
    assert v["age_hours"] == 31.0


def test_identical_fresh_rows_are_static_not_source_failure(db):
    now = datetime(2026, 6, 25, tzinfo=timezone.utc)
    # A freshly queried inactive token may have exactly the same holder state.
    for h in (2, 4, 6):
        _save_at(db, "0xtok", "bsc", _holders(5000), now - timedelta(hours=h))
    v = snapshot_freshness("0xtok", "bsc", now=now, db_path=db)
    assert v["stale"] is False
    assert v["source_healthy"] is True
    assert v["reason"] == "static"
    assert v["currentness"] == "unknown_static"
    assert v["dynamic_evidence_eligible"] is False
    assert v["identical_run"] >= 3


def test_one_repeated_latest_row_is_not_dynamic_evidence(db):
    now = datetime(2026, 6, 25, tzinfo=timezone.utc)
    _save_at(db, "0xtok", "bsc", _holders(4000), now - timedelta(hours=6))
    _save_at(db, "0xtok", "bsc", _holders(5000), now - timedelta(hours=4))
    _save_at(db, "0xtok", "bsc", _holders(5000), now - timedelta(hours=2))

    verdict = snapshot_freshness("0xtok", "bsc", now=now, db_path=db)

    # The fetch path is recent/healthy, but the newest state has no independent
    # provenance and cannot create the zero final delta used by a slope signal.
    assert verdict["stale"] is False
    assert verdict["source_healthy"] is True
    assert verdict["reason"] == "fresh"
    assert verdict["currentness"] == "unknown_static"
    assert verdict["dynamic_evidence_eligible"] is False


@pytest.mark.parametrize(
    "broken_json",
    [
        "{broken",
        "{}",
        "[]",
        '[{"address":"   ","balance":1}]',
        '[{"balance":1}]',
        '[{"address":"0x1","balance":NaN}]',
        '[{"address":"0x1","balance":Infinity}]',
        '[{"address":"0x1","balance":-1}]',
        '[{"address":"0x1","balance":0}]',
    ],
)
def test_invalid_latest_holder_json_fails_closed(db, broken_json):
    now = datetime(2026, 6, 25, tzinfo=timezone.utc)
    _save_at(db, "0xtok", "bsc", _holders(4000), now - timedelta(hours=4))
    _save_at(db, "0xtok", "bsc", _holders(5000), now - timedelta(hours=2))
    conn = _connect(db)
    try:
        conn.execute(
            "UPDATE holder_snapshots SET holders_json = ? WHERE id = "
            "(SELECT MAX(id) FROM holder_snapshots)",
            (broken_json,),
        )
        conn.commit()
    finally:
        conn.close()

    verdict = snapshot_freshness("0xtok", "bsc", now=now, db_path=db)

    assert verdict["stale"] is True
    assert verdict["source_healthy"] is False
    assert verdict["reason"] == "invalid_snapshot"
    assert verdict["currentness"] == "unknown_invalid_snapshot"
    assert verdict["dynamic_evidence_eligible"] is False
    assert get_holders_history("0xtok", "bsc", db_path=db) == []
    assert get_snapshots("0xtok", "bsc", db_path=db) == []


@pytest.mark.parametrize(
    "holders",
    [
        [],
        [{"address": "   ", "balance": 1}],
        [{"balance": 1}],
        [{"address": "0x1", "balance": float("nan")}],
        [{"address": "0x1", "balance": float("inf")}],
        [{"address": "0x1", "balance": -1}],
        [{"address": "0x1", "balance": 0}],
    ],
)
def test_save_snapshot_rejects_semantically_invalid_holder_payload(db, holders):
    with pytest.raises(ValueError, match="invalid holder snapshot payload"):
        save_snapshot("0xtok", "bsc", holders, db_path=db)

    conn = _connect(db)
    try:
        assert conn.execute("SELECT COUNT(*) FROM holder_snapshots").fetchone()[0] == 0
    finally:
        conn.close()


def test_save_snapshot_rejects_invalid_timestamp(db):
    with pytest.raises(ValueError, match="invalid holder snapshot timestamp"):
        save_snapshot(
            "0xtok", "bsc", _holders(5000), snapshot_at="not-a-time", db_path=db,
        )

    conn = _connect(db)
    try:
        assert conn.execute("SELECT COUNT(*) FROM holder_snapshots").fetchone()[0] == 0
    finally:
        conn.close()


def test_save_snapshot_rejects_materially_future_timestamp(db):
    future = datetime.now(timezone.utc) + timedelta(minutes=10)

    with pytest.raises(ValueError, match="future holder snapshot timestamp"):
        save_snapshot(
            "0xtok", "bsc", _holders(5000),
            snapshot_at=future.isoformat(), db_path=db,
        )

    conn = _connect(db)
    try:
        assert conn.execute("SELECT COUNT(*) FROM holder_snapshots").fetchone()[0] == 0
    finally:
        conn.close()


def test_future_changed_snapshot_fails_closed_on_every_read_path(db):
    now = datetime.now(timezone.utc)
    _save_legacy_exact_case(
        db, "0xtok", "bsc", _holders(4000), now - timedelta(hours=1),
    )
    # Bypass the write guard to reproduce a legacy/corrupt future DB row whose
    # changed state would otherwise look like fresh dynamic evidence.
    _save_legacy_exact_case(
        db, "0xtok", "bsc", _holders(5000), now + timedelta(minutes=10),
    )

    verdict = snapshot_freshness("0xtok", "bsc", now=now, db_path=db)

    assert verdict["stale"] is True
    assert verdict["source_healthy"] is False
    assert verdict["reason"] == "future_snapshot_timestamp"
    assert verdict["currentness"] == "unknown_future_timestamp"
    assert verdict["dynamic_evidence_eligible"] is False
    assert get_holders_history("0xtok", "bsc", db_path=db) == []
    assert get_snapshots("0xtok", "bsc", db_path=db) == []


def test_invalid_latest_snapshot_timestamp_fails_closed(db):
    now = datetime(2026, 6, 25, tzinfo=timezone.utc)
    _save_at(db, "0xtok", "bsc", _holders(4000), now - timedelta(hours=4))
    _save_at(db, "0xtok", "bsc", _holders(5000), now - timedelta(hours=2))
    conn = _connect(db)
    try:
        conn.execute(
            "UPDATE holder_snapshots SET snapshot_at = 'not-a-timestamp' WHERE id = "
            "(SELECT MAX(id) FROM holder_snapshots)"
        )
        conn.commit()
    finally:
        conn.close()

    verdict = snapshot_freshness("0xtok", "bsc", now=now, db_path=db)

    assert verdict["stale"] is True
    assert verdict["source_healthy"] is False
    assert verdict["reason"] == "invalid_snapshot_timestamp"
    assert verdict["currentness"] == "unknown_invalid_timestamp"
    assert verdict["dynamic_evidence_eligible"] is False
    assert get_holders_history("0xtok", "bsc", db_path=db) == []


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


def test_avalanche_checksum_aliases_are_canonical_evm_identity(db):
    now = datetime(2026, 7, 15, tzinfo=timezone.utc)
    checksum = "0xAbCdEf1234567890"
    lower = checksum.lower()
    _save_legacy_exact_case(
        db, checksum, "avax", _holders(5000), now - timedelta(hours=8),
    )
    _save_at(
        db, lower, "avalanche", _holders(4000), now - timedelta(hours=2),
    )

    verdict = snapshot_freshness(checksum, "avax", now=now, db_path=db)

    assert verdict["dynamic_evidence_eligible"] is True
    assert list_tokens(db_path=db) == [(lower, "avalanche")]
    assert len(get_holders_history(checksum, "avax", db_path=db)) == 2
    assert len(get_holders_history(checksum, "avalanche", db_path=db)) == 2


def test_legacy_eth_storage_alias_reads_through_canonical_chain(db):
    now = datetime(2026, 7, 15, tzinfo=timezone.utc)
    checksum = "0xAbCdEf1234567890"
    _save_legacy_exact_case(
        db, checksum, "eth", _holders(5000), now - timedelta(hours=8),
    )
    _save_at(
        db, checksum.lower(), "ethereum", _holders(4000), now - timedelta(hours=2),
    )

    verdict = snapshot_freshness(checksum, "ethereum", now=now, db_path=db)

    assert verdict["dynamic_evidence_eligible"] is True
    assert list_tokens(db_path=db) == [(checksum.lower(), "ethereum")]
    assert len(get_holders_history(checksum, "eth", db_path=db)) == 2
    assert len(get_holders_history(checksum, "ethereum", db_path=db)) == 2


def test_solana_mint_identity_remains_case_sensitive(db):
    now = datetime(2026, 7, 15, tzinfo=timezone.utc)
    mint = "AbCdEfSolanaMint"
    _save_at(db, mint, "solana", _holders(5000), now - timedelta(hours=2))

    assert list_tokens(db_path=db) == [(mint, "solana")]
    assert snapshot_freshness(
        mint.lower(), "solana", now=now, db_path=db,
    )["reason"] == "no_snapshots"
    assert get_holders_history(mint.lower(), "solana", db_path=db) == []


def test_snapshot_token_routes_bsc_to_chain_id_56(db, monkeypatch):
    calls = []

    def fake_fetch(token, chain_id):
        calls.append((token, chain_id))
        return _holders(5000)

    monkeypatch.setattr(
        "src.onchain.holder_snapshot.fetch_holders_evm", fake_fetch,
    )
    result = snapshot_token("0xAbCd", "bsc", source="test", db_path=db)

    assert calls == [("0xAbCd", 56)]
    assert result is not None
    assert list_tokens(db_path=db) == [("0xabcd", "bsc")]


def test_snapshot_token_routes_avalanche_alias_to_43114(db, monkeypatch):
    calls = []

    def fake_fetch(token, chain_id):
        calls.append((token, chain_id))
        return _holders(5000)

    monkeypatch.setattr(
        "src.onchain.holder_snapshot.fetch_holders_evm", fake_fetch,
    )
    result = snapshot_token("0xAbCd", "avax", source="test", db_path=db)

    assert calls == [("0xAbCd", 43114)]
    assert result is not None
    assert list_tokens(db_path=db) == [("0xabcd", "avalanche")]


def test_snapshot_token_unknown_chain_never_falls_back_to_ethereum(db, monkeypatch):
    calls = []
    monkeypatch.setattr(
        "src.onchain.holder_snapshot.fetch_holders_evm",
        lambda *args, **kwargs: calls.append((args, kwargs)),
    )

    assert snapshot_token("0xAbCd", "avalanch-typo", db_path=db) is None
    assert calls == []
    conn = _connect(db)
    try:
        assert conn.execute("SELECT COUNT(*) FROM token_birth").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM holder_snapshots").fetchone()[0] == 0
    finally:
        conn.close()


def test_holder_history_limit_keeps_most_recent_rows(db):
    start = datetime(2026, 7, 10, tzinfo=timezone.utc)
    for i in range(105):
        _save_at(
            db,
            "0xtok",
            "bsc",
            _holders(4000 + i),
            start + timedelta(minutes=i),
        )

    for since in (None, (start - timedelta(minutes=1)).isoformat()):
        history = get_holders_history(
            "0xtok", "bsc", limit=100, since=since, db_path=db,
        )

        assert len(history) == 100
        assert history[0][0] == (start + timedelta(minutes=5)).isoformat()
        assert history[-1][0] == (start + timedelta(minutes=104)).isoformat()
        assert history[-1][1] == _holders(4104)

    metric_history = get_snapshots("0xtok", "bsc", limit=100, db_path=db)
    assert len(metric_history) == 100
    assert metric_history[0]["snapshot_at"] == (
        start + timedelta(minutes=5)
    ).isoformat()
    assert metric_history[-1]["snapshot_at"] == (
        start + timedelta(minutes=104)
    ).isoformat()


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
