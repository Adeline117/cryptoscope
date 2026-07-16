from __future__ import annotations

import json

import pytest

from src.onchain import smart_wallets


def _fetch_ok(payload: dict) -> dict:
    return {
        "state": "ok", "payload": payload, "error_kind": None,
        "http_status": None, "detail": None,
    }


def _fetch_failed(error_kind: str = "challenge_or_blocked") -> dict:
    return {
        "state": "failed", "payload": None, "error_kind": error_kind,
        "http_status": 403, "detail": None,
    }


def _rank_row(address: str = "wallet-new", *, winrate=0.7, realized=20_000,
              buys=30) -> dict:
    return {
        "address": address,
        "winrate_7d": winrate,
        "realized_profit_7d": realized,
        "buy_7d": buys,
    }


def _seed_wallet(chain: str = "sol", wallet: str = "wallet-old") -> None:
    connection = smart_wallets._conn()
    try:
        connection.execute(
            "INSERT INTO watchlist VALUES (?,?,?,?,?,?)",
            (wallet, chain, 0.8, 25_000, 40, "2026-01-01T00:00:00+00:00"),
        )
        connection.commit()
    finally:
        connection.close()


@pytest.mark.parametrize(
    ("fetch", "error_kind"),
    [
        (_fetch_failed("challenge_or_blocked"), "challenge_or_blocked"),
        (_fetch_ok({"code": 0, "data": {}}), "missing_activities"),
        (_fetch_ok({"code": 0, "data": {"activities": {}}}),
         "invalid_activities_schema"),
        (_fetch_ok({"code": 0, "data": {"activities": [None]}}),
         "invalid_activity_row"),
    ],
)
def test_recent_activity_failure_and_schema_drift_are_not_empty(
        monkeypatch, fetch, error_kind):
    monkeypatch.setattr(smart_wallets, "_fs_get_result", lambda *_args, **_kwargs: fetch)

    result = smart_wallets.recent_buys_result(
        "wallet", "sol", now_ts=1_000_000)

    assert result["state"] == "failed"
    assert result["error_kind"] == error_kind
    assert smart_wallets.recent_buys("wallet", "sol") is None


def test_explicit_empty_activity_list_is_verified_empty(monkeypatch):
    monkeypatch.setattr(
        smart_wallets,
        "_fs_get_result",
        lambda *_args, **_kwargs: _fetch_ok({
            "code": 0, "data": {"activities": []},
        }),
    )

    result = smart_wallets.recent_buys_result("wallet", "sol", now_ts=1_000_000)

    assert result == {
        "state": "ok", "buys": [], "error_kind": None,
        "received": 0, "accepted": 0,
    }
    assert smart_wallets.recent_buys("wallet", "sol") == []


def test_small_future_activity_clock_skew_is_clamped(monkeypatch):
    monkeypatch.setattr(
        smart_wallets,
        "_fs_get_result",
        lambda *_args, **_kwargs: _fetch_ok({
            "code": 0,
            "data": {"activities": [{
                "event_type": "buy", "timestamp": 1_000_060, "cost_usd": "25",
                "token": {"address": "token-1", "symbol": "ONE"},
            }]},
        }),
    )

    result = smart_wallets.recent_buys_result(
        "wallet", "sol", now_ts=1_000_000)

    assert result["state"] == "ok"
    assert result["buys"][0]["ts"] == 1_000_000


def test_activity_cache_only_reads_versioned_verified_wrappers(monkeypatch, tmp_path):
    monkeypatch.setattr(smart_wallets, "DB", tmp_path / "wallets.db")
    connection = smart_wallets._conn()
    try:
        connection.executemany(
            "INSERT INTO activity_cache VALUES (?,?,?,?)",
            [
                ("legacy", "sol", "[]", 1_000),
                ("bad-json", "sol", "{", 1_000),
                ("bad-row", "sol", json.dumps({
                    "schema_version": smart_wallets.ACTIVITY_CACHE_SCHEMA_VERSION,
                    "activities": [{}],
                }), 1_000),
                ("bool-version", "sol", json.dumps({
                    "schema_version": True,
                    "activities": [],
                }), 1_000),
            ],
        )
        connection.commit()
    finally:
        connection.close()

    jobs = [
        ("sol", "legacy"), ("sol", "bad-json"), ("sol", "bad-row"),
        ("sol", "bool-version"),
    ]
    assert smart_wallets._fresh_cached_activity(jobs, 40, 1_100) == []

    smart_wallets._update_activity_cache([("sol", "verified", [])], 1_200)
    assert smart_wallets._fresh_cached_activity(
        jobs + [("sol", "verified")], 40, 1_200) == [("sol", "verified", [])]

    connection = smart_wallets._conn()
    try:
        payload = connection.execute(
            "SELECT payload FROM activity_cache WHERE wallet='verified' AND chain='sol'"
        ).fetchone()[0]
    finally:
        connection.close()
    assert json.loads(payload) == {
        "schema_version": smart_wallets.ACTIVITY_CACHE_SCHEMA_VERSION,
        "activities": [],
    }


def test_failed_activity_does_not_overwrite_payload_or_checked_at(monkeypatch, tmp_path):
    monkeypatch.setattr(smart_wallets, "DB", tmp_path / "wallets.db")
    monkeypatch.setattr(smart_wallets, "usable", lambda: True)
    monkeypatch.setattr(
        smart_wallets, "watchlist", lambda chain: [{"wallet": "wallet-1"}],
    )
    original_payload = json.dumps({
        "schema_version": smart_wallets.ACTIVITY_CACHE_SCHEMA_VERSION,
        "activities": [],
    })
    connection = smart_wallets._conn()
    try:
        connection.execute(
            "INSERT INTO activity_cache VALUES (?,?,?,?)",
            ("wallet-1", "sol", original_payload, 100),
        )
        connection.commit()
    finally:
        connection.close()
    monkeypatch.setattr(
        smart_wallets,
        "recent_buys_result",
        lambda *_args, **_kwargs: {
            "state": "failed", "buys": [], "error_kind": "missing_activities",
        },
    )

    result = smart_wallets.fresh_smart_buys_result(
        chain_codes=("sol",), window_min=40, now_ts=200)

    connection = smart_wallets._conn()
    try:
        payload, checked_at = connection.execute(
            "SELECT payload,checked_at FROM activity_cache "
            "WHERE wallet='wallet-1' AND chain='sol'"
        ).fetchone()
    finally:
        connection.close()
    assert payload == original_payload
    assert checked_at == 100
    assert result["source_health"]["state"] == "failed"
    assert result["source_health"]["observed"] == 0
    assert result["source_health"]["error_counts"] == {"missing_activities": 1}
    assert result["source_health"]["chains"][0]["state"] == "failed"


def test_verified_empty_activity_updates_cache_and_checked_at(monkeypatch, tmp_path):
    monkeypatch.setattr(smart_wallets, "DB", tmp_path / "wallets.db")
    monkeypatch.setattr(smart_wallets, "usable", lambda: True)
    monkeypatch.setattr(
        smart_wallets, "watchlist", lambda chain: [{"wallet": "wallet-1"}],
    )
    monkeypatch.setattr(
        smart_wallets,
        "recent_buys_result",
        lambda *_args, **_kwargs: {
            "state": "ok", "buys": [], "error_kind": None,
        },
    )

    result = smart_wallets.fresh_smart_buys_result(
        chain_codes=("sol",), window_min=40, now_ts=200)

    connection = smart_wallets._conn()
    try:
        payload, checked_at = connection.execute(
            "SELECT payload,checked_at FROM activity_cache "
            "WHERE wallet='wallet-1' AND chain='sol'"
        ).fetchone()
    finally:
        connection.close()
    assert json.loads(payload)["activities"] == []
    assert checked_at == 200
    assert result["source_health"]["state"] == "ok"
    assert result["source_health"]["observed"] == 1


def test_chain_without_configured_wallets_keeps_global_health_partial(
        monkeypatch, tmp_path):
    monkeypatch.setattr(smart_wallets, "DB", tmp_path / "wallets.db")
    monkeypatch.setattr(smart_wallets, "usable", lambda: True)
    monkeypatch.setattr(
        smart_wallets,
        "watchlist",
        lambda chain: [{"wallet": "sol-wallet"}] if chain == "sol" else [],
    )
    monkeypatch.setattr(
        smart_wallets,
        "recent_buys_result",
        lambda *_args, **_kwargs: {
            "state": "ok", "buys": [], "error_kind": None,
        },
    )

    result = smart_wallets.fresh_smart_buys_result(
        chain_codes=("sol", "eth"), window_min=40, now_ts=200)

    assert result["source_health"]["state"] == "partial"
    assert result["source_health"]["error_kind"] == "chain_coverage_gap"
    assert [(row["chain"], row["state"]) for row in
            result["source_health"]["chains"]] == [
                ("sol", "ok"), ("eth", "failed"),
            ]


@pytest.mark.parametrize(
    ("fetch", "error_kind"),
    [
        (_fetch_failed("challenge_or_blocked"), "challenge_or_blocked"),
        (_fetch_ok({"code": 0, "data": {}}), "missing_rank"),
        (_fetch_ok({"code": 0, "data": {"rank": {}}}), "invalid_rank_schema"),
        (_fetch_ok({"code": 0, "data": {"rank": []}}), "suspicious_empty_rank"),
        (_fetch_ok({"code": 0, "data": {"rank": [{}]}}), "invalid_rank_row"),
        (_fetch_ok({"code": 0, "data": {"rank": [_rank_row(), {}]}}),
         "invalid_rank_row"),
    ],
)
def test_unverified_harvest_preserves_existing_watchlist(
        monkeypatch, tmp_path, fetch, error_kind):
    monkeypatch.setattr(smart_wallets, "DB", tmp_path / "wallets.db")
    _seed_wallet()
    monkeypatch.setattr(smart_wallets, "_fs_get_result", lambda *_args, **_kwargs: fetch)

    result = smart_wallets.harvest_result("sol")

    assert result["state"] == "failed"
    assert result["error_kind"] == error_kind
    assert result["preserved"] is True
    assert result["kept"] == 1
    assert [row["wallet"] for row in smart_wallets.watchlist("sol")] == ["wallet-old"]


def test_verified_nonempty_rank_may_filter_to_zero_and_clear(monkeypatch, tmp_path):
    monkeypatch.setattr(smart_wallets, "DB", tmp_path / "wallets.db")
    _seed_wallet()
    monkeypatch.setattr(
        smart_wallets,
        "_fs_get_result",
        lambda *_args, **_kwargs: _fetch_ok({
            "code": 0,
            "data": {"rank": [_rank_row(winrate=0.1, realized=100, buys=2)]},
        }),
    )

    result = smart_wallets.harvest_result("sol")

    assert result["state"] == "ok"
    assert result["received"] == 1
    assert result["validated"] == 1
    assert result["kept"] == 0
    assert result["preserved"] is False
    assert smart_wallets.watchlist("sol") == []


def test_harvest_all_exposes_per_chain_health_and_preserves_failed_chain(
        monkeypatch, tmp_path):
    monkeypatch.setattr(smart_wallets, "DB", tmp_path / "wallets.db")
    _seed_wallet(chain="eth", wallet="eth-old")
    monkeypatch.setattr(smart_wallets, "usable", lambda: True)

    def fake_fetch(url, **_kwargs):
        if "/sol/" in url:
            return _fetch_ok({"code": 0, "data": {"rank": [_rank_row()]}})
        return _fetch_ok({"code": 0, "data": {}})

    monkeypatch.setattr(smart_wallets, "_fs_get_result", fake_fetch)

    result = smart_wallets.harvest_all(("sol", "eth"))

    assert result["harvested"] == 1
    assert result["watchlisted"] == 2
    assert result["source_health"]["state"] == "partial"
    assert result["source_health"]["successful_chains"] == 1
    assert result["source_health"]["failed_chains"] == 1
    assert [(row["chain"], row["state"]) for row in
            result["source_health"]["chains"]] == [
                ("sol", "ok"), ("eth", "failed"),
            ]
    assert [row["wallet"] for row in smart_wallets.watchlist("eth")] == ["eth-old"]
