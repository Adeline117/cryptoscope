"""Launch Radar must preserve first-observed facts and never turn thin pools into bets."""
from datetime import datetime, timezone

import pytest


def _pair(**overrides):
    p = {
        "chainId": "solana", "pairAddress": "pool", "priceUsd": "0.0001",
        "pairCreatedAt": 1_700_000_000_000, "fdv": 100_000,
        "liquidity": {"usd": 20_000}, "volume": {"m5": 1_000},
        "txns": {"m5": {"buys": 10, "sells": 3}}, "boosts": {"active": 0},
        "baseToken": {"address": "Token", "symbol": "T", "name": "Test"},
    }
    p.update(overrides)
    return p


def test_qualify_emits_small_probe_only_for_fresh_tradeable_flow():
    from src.pipeline.launch_radar import qualify
    now = datetime.fromtimestamp(1_700_000_000_000 / 1000 + 60 * 30, tz=timezone.utc)
    got = qualify(_pair(), now=now)
    assert got["decision"] == "SMALL_PROBE"
    assert got["invalidation_price"] == pytest.approx(0.00007)
    assert got["max_notional_usd"] == 60  # 0.3% of pool, not an unbounded suggestion


def test_qualify_rejects_untradeable_or_late_pools():
    from src.pipeline.launch_radar import qualify
    now = datetime.fromtimestamp(1_700_000_000_000 / 1000 + 60 * 30, tz=timezone.utc)
    assert qualify(_pair(liquidity={"usd": 4_999}), now=now) is None
    late = datetime.fromtimestamp(1_700_000_000_000 / 1000 + 25 * 3600, tz=timezone.utc)
    assert qualify(_pair(), now=late) is None


def test_ledger_keeps_first_seen_entry_when_event_is_refreshed(tmp_path, monkeypatch):
    import src.pipeline.opportunity_ledger as ol
    monkeypatch.setattr(ol, "DB", tmp_path / "ledger.db")
    first = {"lane": "launch", "chain": "solana", "token": "Token", "symbol": "T",
             "entry_price": 0.01, "state": "live", "decision": "WATCH",
             "roundtrip_cost_pct_est": 1.2, "cost_model": "first_snapshot"}
    _, inserted = ol.record(first)
    assert inserted
    second = {**first, "entry_price": 0.10, "decision": "SMALL_PROBE",
              "roundtrip_cost_pct_est": 9.9, "cost_model": "later_refresh"}
    _, inserted = ol.record(second)
    assert not inserted
    row = ol.active("launch")[0]
    assert row["entry_price"] == 0.01
    assert row["cost_pct_est"] == 1.2 and row["cost_model"] == "first_snapshot"
    assert row["cohort_version"] == 2
    # The live card may refresh to SMALL_PROBE, but the experiment cohort is frozen
    # at first observation so a later pump cannot relabel an old WATCH as a winner.
    assert row["decision"] == "SMALL_PROBE"
    assert ol.outcome_rows()[0]["decision"] == "WATCH"


def test_cascade_only_records_strong_timed_events(tmp_path, monkeypatch):
    import src.pipeline.opportunity_ledger as ol
    import src.pipeline.cascade_radar as cr
    monkeypatch.setattr(ol, "DB", tmp_path / "ledger.db")
    now = datetime(2026, 7, 13, 12, tzinfo=timezone.utc)
    weak = {"symbol": "NOPE", "signal": "cascade", "strength": "中", "mark_price": 10}
    firing = {"symbol": "X", "signal": "cascade", "strength": "强", "direction": "longs_crowded",
              "mark_price": 10, "funding_ann": 200, "oi_usd": 2_000_000, "oi_chg_pct": -4}
    def book(_symbol):
        return {"coin": "X", "time": int(now.timestamp() * 1000), "levels": [
            [{"px": "10.00", "sz": "20"}], [{"px": "10.01", "sz": "20"}],
        ]}

    assert cr.record_signals([weak, firing], now=now, quote_fetch=book) == 1
    row = ol.active("cascade")[0]
    assert row["side"] == "SHORT" and row["entry_price"] == 10
    assert row["invalidation_price"] == pytest.approx(10.3)
    assert row["decision"] == "SMALL_PROBE"
    assert row["quote_at"] == now.isoformat()
    assert row["expires_at"] == "2026-07-13T12:01:00+00:00"
    assert row["executable_at"] is None
    assert row["execution_probe"]["is_real_fill"] is False


def test_cascade_without_fresh_executable_book_is_watch_only(tmp_path, monkeypatch):
    import src.pipeline.opportunity_ledger as ol
    import src.pipeline.cascade_radar as cr

    monkeypatch.setattr(ol, "DB", tmp_path / "ledger.db")
    now = datetime(2026, 7, 13, 12, tzinfo=timezone.utc)
    firing = {"symbol": "X", "signal": "ignition", "strength": "强",
              "direction": "up", "mark_price": 10, "oi_usd": 2_000_000,
              "oi_chg_pct": 20}
    stale = now.timestamp() * 1000 - 11_000

    cr.record_signals([firing], now=now, quote_fetch=lambda _: {
        "coin": "X", "time": stale, "levels": [
            [{"px": "10.00", "sz": "20"}], [{"px": "10.01", "sz": "20"}],
        ],
    })

    row = ol.active("cascade")[0]
    assert row["decision"] == "WATCH"
    assert row["quote_at"] is None and row["expires_at"] is None
    assert row["execution_probe"]["state"] == "unknown"
    assert "stale book" in row["execution_probe"]["reason"]
