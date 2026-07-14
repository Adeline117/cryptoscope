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
    assert row["decision"] == "SMALL_PROBE"


def test_cascade_only_records_strong_timed_events(tmp_path, monkeypatch):
    import src.pipeline.opportunity_ledger as ol
    import src.pipeline.cascade_radar as cr
    monkeypatch.setattr(ol, "DB", tmp_path / "ledger.db")
    now = datetime(2026, 7, 13, 12, tzinfo=timezone.utc)
    weak = {"symbol": "NOPE", "signal": "cascade", "strength": "中", "mark_price": 10}
    firing = {"symbol": "X", "signal": "cascade", "strength": "强", "direction": "longs_crowded",
              "mark_price": 10, "funding_ann": 200, "oi_usd": 2_000_000, "oi_chg_pct": -4}
    assert cr.record_signals([weak, firing], now=now) == 1
    row = ol.active("cascade")[0]
    assert row["side"] == "SHORT" and row["entry_price"] == 10
    assert row["invalidation_price"] == pytest.approx(10.3)
