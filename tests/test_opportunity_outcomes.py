"""Five-lane outcomes must measure forward results without manufacturing edge."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest


@pytest.fixture
def ledger(tmp_path, monkeypatch):
    from src.pipeline import opportunity_ledger as ol

    monkeypatch.setattr(ol, "DB", tmp_path / "opportunities.db")
    return ol


def _launch(detected_at: str, token: str = "token", decision: str = "SMALL_PROBE") -> dict:
    return {"lane": "launch", "chain": "solana", "token": token, "symbol": token,
            "detected_at": detected_at, "entry_price": 100.0, "decision": decision,
            "state": "live", "max_notional_usd": 50,
            "roundtrip_cost_pct_est": 1.0, "cost_model": "test_frozen_cost"}


def test_resolver_settles_one_horizon_per_event_and_preserves_entry(ledger):
    from src.pipeline import opportunity_outcomes as oo

    now = datetime(2026, 7, 14, 12, tzinfo=timezone.utc)
    ident, _ = ledger.record(_launch((now - timedelta(hours=25)).isoformat()))
    calls = []

    def price_at(row, when):
        calls.append(when)
        return 110.0

    first = oo.resolve(now=now, price_at=price_at, max_lookups=20)
    assert first["lookups"] == first["settled"] == 1
    row = ledger.outcome_rows()[0]
    assert set(row["outcome"]["horizons"]) == {"1h"}
    assert row["outcome"]["horizons"]["1h"]["net_return_pct_est"] == pytest.approx(9.0)
    assert row["outcome_state"] == "open"

    oo.resolve(now=now, price_at=price_at, max_lookups=20)
    row = ledger.outcome_rows()[0]
    assert set(row["outcome"]["horizons"]) == {"1h", "24h"}
    assert len(calls) == 2
    assert row["entry_price"] == 100.0
    assert row["id"] == ident


def test_short_cascade_return_has_correct_sign(ledger):
    from src.pipeline import opportunity_outcomes as oo

    now = datetime(2026, 7, 14, 12, tzinfo=timezone.utc)
    ledger.record({"lane": "cascade", "chain": "hyperliquid", "token": "X",
                   "symbol": "X", "detected_at": (now - timedelta(hours=2)).isoformat(),
                   "entry_price": 100, "decision": "SMALL_PROBE", "state": "firing",
                   "side": "SHORT", "roundtrip_cost_pct_est": 0.2,
                   "cost_model": "test_perp_cost"})
    oo.resolve(now=now, price_at=lambda row, when: 90.0)
    point = ledger.outcome_rows()[0]["outcome"]["horizons"]["1h"]
    assert point["gross_return_pct"] == pytest.approx(10.0)
    assert point["net_return_pct_est"] == pytest.approx(9.8)
    assert point["positive_after_cost"] is True


def test_missing_old_1h_price_does_not_retire_valid_24h_outcome(ledger):
    from src.pipeline import opportunity_outcomes as oo

    now = datetime(2026, 7, 30, 12, tzinfo=timezone.utc)
    ledger.record(_launch((now - timedelta(days=22)).isoformat()))
    prices = iter((None, 110.0, 120.0))
    oo.resolve(now=now, price_at=lambda row, when: next(prices))
    first = ledger.outcome_rows()[0]
    assert first["outcome_state"] == "open"
    assert first["outcome"]["unavailable_horizons"] == ["1h"]

    oo.resolve(now=now, price_at=lambda row, when: next(prices))
    second = ledger.outcome_rows()[0]
    assert "24h" in second["outcome"]["horizons"]
    assert second["outcome_state"] == "open"

    oo.resolve(now=now, price_at=lambda row, when: next(prices))
    final = ledger.outcome_rows()[0]
    assert "7d" in final["outcome"]["horizons"]
    assert final["outcome_state"] == "resolved"


def test_stats_refuse_rate_below_minimum_sample(ledger):
    from src.pipeline import opportunity_outcomes as oo

    ident, _ = ledger.record(_launch(datetime.now(timezone.utc).isoformat()))
    ledger.save_outcome(ident, {"horizons": {"24h": {"net_return_pct_est": 20.0}}})
    stat = oo.lane_stats()["launch"]
    assert stat["verdict"] == "不可判"
    assert "rate" not in stat
    assert stat["n"] == 1


def test_launch_edge_requires_and_uses_watch_control(ledger):
    from src.pipeline import opportunity_outcomes as oo

    ts = datetime.now(timezone.utc).isoformat()
    for i in range(oo.MIN_N):
        ident, _ = ledger.record(_launch(ts, f"probe-{i}", "SMALL_PROBE"))
        ledger.save_outcome(ident, {"horizons": {"24h": {"net_return_pct_est": 8.0}}},
                            "resolved")
    for i in range(oo.MIN_N):
        ident, _ = ledger.record(_launch(ts, f"watch-{i}", "WATCH"))
        ledger.save_outcome(ident, {"horizons": {"24h": {"net_return_pct_est": -2.0}}},
                            "resolved")

    stat = oo.lane_stats()["launch"]
    assert stat["verdict"] == "measured"
    assert stat["edge_verdict"] == "有edge迹象"
    assert stat["probe"]["n"] == stat["control"]["n"] == oo.MIN_N
    assert stat["probe"]["lo"] > stat["control"]["hi"]


def test_watch_only_structure_never_gets_directional_hit_rate(ledger):
    from src.pipeline import opportunity_outcomes as oo

    ledger.record({"lane": "structure", "chain": "cex", "token": "ABC-USDT",
                   "symbol": "ABC-USDT", "decision": "WATCH", "state": "live"})
    stat = oo.lane_stats()["structure"]
    assert stat["verdict"] == "not_directional"
    assert "rate" not in stat
    assert "不把事后涨跌" in stat["note"]
