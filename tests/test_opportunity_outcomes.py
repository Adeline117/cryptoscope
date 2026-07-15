"""Five-lane outcomes must measure forward results without manufacturing edge."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest


@pytest.fixture
def ledger(tmp_path, monkeypatch):
    from src.pipeline import opportunity_ledger as ol

    monkeypatch.setattr(ol, "DB", tmp_path / "opportunities.db")
    return ol


def _launch(detected_at: str, token: str = "token", decision: str = "SMALL_PROBE",
            *, v3: bool = False) -> dict:
    item = {"lane": "launch", "chain": "solana", "token": token, "symbol": token,
            "detected_at": detected_at, "entry_price": 100.0, "decision": decision,
            "state": "live", "max_notional_usd": 50,
            "roundtrip_cost_pct_est": 1.0, "cost_model": "test_frozen_cost"}
    if v3:
        from src.pipeline.execution_cost import discovery_contract
        item.update({"cohort_version": 3, "cost_contract": discovery_contract(
            notional_usd=50, modeled_roundtrip_pct=1.0,
            method="constant_product_roundtrip_plus_0.60pct_buffer_v1")})
    return item


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
    assert set(row["outcome"]["horizons"]) == {"24h"}
    assert row["outcome"]["horizons"]["24h"]["net_return_pct_est"] == pytest.approx(9.0)
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
    prices = iter((110.0, None, 120.0))
    oo.resolve(now=now, price_at=lambda row, when: next(prices))
    first = ledger.outcome_rows()[0]
    assert first["outcome_state"] == "open"
    assert "24h" in first["outcome"]["horizons"]

    oo.resolve(now=now, price_at=lambda row, when: next(prices))
    second = ledger.outcome_rows()[0]
    assert second["outcome"]["unavailable_horizons"] == ["1h"]
    assert second["outcome_state"] == "open"

    oo.resolve(now=now, price_at=lambda row, when: next(prices))
    final = ledger.outcome_rows()[0]
    assert "7d" in final["outcome"]["horizons"]
    assert final["outcome_state"] == "resolved"


def test_overdue_24h_is_not_starved_by_fresh_1h_tasks(ledger):
    from src.pipeline import opportunity_outcomes as oo

    now = datetime(2026, 7, 15, 12, tzinfo=timezone.utc)
    ledger.record(_launch((now - timedelta(hours=25)).isoformat(), "old"))
    for i in range(25):
        ledger.record(_launch((now - timedelta(hours=2)).isoformat(), f"fresh-{i}"))
    calls = []

    got = oo.resolve(now=now, price_at=lambda row, when: calls.append((row["token"], when)) or 101,
                     max_lookups=1)

    assert got["lookups_by_horizon"] == {"24h": 1}
    assert calls[0][0] == "old"
    row = next(r for r in ledger.outcome_rows() if r["token"] == "old")
    assert "24h" in row["outcome"]["horizons"]


def test_lookup_budget_reserves_a_slot_for_each_due_lane(ledger):
    from src.pipeline import opportunity_outcomes as oo

    now = datetime(2026, 7, 15, 12, tzinfo=timezone.utc)
    for i in range(10):
        ledger.record(_launch((now - timedelta(hours=25)).isoformat(), f"launch-{i}"))
    ledger.record({"lane": "cascade", "chain": "hyperliquid", "token": "CAS",
                   "symbol": "CAS", "detected_at": (now - timedelta(hours=25)).isoformat(),
                   "entry_price": 100, "decision": "SMALL_PROBE", "state": "firing",
                   "side": "SHORT", "roundtrip_cost_pct_est": 0.2,
                   "cost_model": "test"})
    lanes = []

    got = oo.resolve(now=now,
                     price_at=lambda row, when: lanes.append(row["lane"]) or 101,
                     max_lookups=2)

    assert got["lookups_by_lane"] == {"cascade": 1, "launch": 1}
    assert set(lanes) == {"launch", "cascade"}


def test_shared_ohlcv_rate_limit_stops_the_cycle_after_one_lookup(ledger):
    from src.pipeline import opportunity_outcomes as oo
    from src.pipeline.evidence import OhlcvRateLimited

    now = datetime(2026, 7, 15, 12, tzinfo=timezone.utc)
    for i in range(5):
        ledger.record(_launch((now - timedelta(hours=25)).isoformat(), f"launch-{i}"))
    calls = []

    def limited(row, when):
        calls.append(row["id"])
        raise OhlcvRateLimited("shared quota exhausted")

    got = oo.resolve(now=now, price_at=limited, max_lookups=5)

    assert len(calls) == 1
    assert got["lookups"] == 1
    assert got["source_backoff"] == "shared quota exhausted"
    attempted = [r for r in ledger.outcome_rows()
                 if (r["outcome"].get("attempts") or {}).get("24h")]
    assert len(attempted) == 1


def test_gecko_429_raises_without_per_token_sleep_retries(monkeypatch):
    import urllib.error
    import urllib.request
    from src.pipeline import evidence

    request = urllib.request.Request("https://api.geckoterminal.com/test")
    error = urllib.error.HTTPError(request.full_url, 429, "rate limited", {}, None)
    sleeps = []
    monkeypatch.setattr(urllib.request, "urlopen",
                        lambda *_args, **_kwargs: (_ for _ in ()).throw(error))
    monkeypatch.setattr("time.sleep", lambda seconds: sleeps.append(seconds))
    evidence._CANDLE_CACHE.clear()

    with pytest.raises(evidence.OhlcvRateLimited):
        evidence._ohlcv("solana", "Pool", before=123)
    assert sleeps == []


def test_stats_expose_due_and_unpriced_24h_backlog(ledger):
    from src.pipeline import opportunity_outcomes as oo

    now = datetime.now(timezone.utc)
    due_id, _ = ledger.record(_launch((now - timedelta(hours=26)).isoformat(), "due"))
    ledger.record(_launch((now - timedelta(hours=2)).isoformat(), "new"))
    row = next(r for r in ledger.outcome_rows() if r["id"] == due_id)
    ledger.save_outcome(due_id, {"attempts": {"24h": 1},
                                 "attempted_at": {"24h": now.isoformat()},
                                 "horizons": {}})

    stat = oo.lane_stats()["launch"]

    assert stat["resolved_24h"] == 0
    assert stat["not_due_24h"] == 1
    assert stat["due_24h"] == 1
    assert stat["attempted_unpriced_24h"] == 1
    assert stat["oldest_due_24h_hours"] >= 1.9


def test_stats_refuse_rate_below_minimum_sample(ledger):
    from src.pipeline import opportunity_outcomes as oo

    ident, _ = ledger.record(_launch(datetime.now(timezone.utc).isoformat()))
    ledger.save_outcome(ident, {"horizons": {"24h": {"net_return_pct_est": 20.0}}})
    stat = oo.lane_stats()["launch"]
    assert stat["verdict"] == "不可判"
    assert "rate" not in stat
    assert stat["n"] == 1
    assert stat["probe"]["n"] == 0 and stat["control"]["n"] == 0
    gate = oo.actionability_gate("launch")
    assert gate["state"] == "collecting"
    assert gate["probe_n"] == 0 and gate["control_n"] == 0


def test_legacy_mutable_decision_is_excluded_from_cohort(ledger):
    from src.pipeline import opportunity_outcomes as oo

    ident, _ = ledger.record(_launch(datetime.now(timezone.utc).isoformat()))
    ledger.save_outcome(ident, {"horizons": {"24h": {"net_return_pct_est": 20.0}}})
    c = ledger._conn()
    c.execute("UPDATE opportunities SET cohort_version=NULL WHERE id=?", (ident,))
    c.commit(); c.close()
    rows = ledger.outcome_rows()
    assert oo._cohort(rows, "SMALL_PROBE")["n"] == 0
    assert oo.lane_stats()["launch"]["legacy_unfrozen_n"] == 1


def test_launch_edge_requires_and_uses_watch_control(ledger):
    from src.pipeline import opportunity_outcomes as oo

    ts = datetime.now(timezone.utc).isoformat()
    for i in range(oo.MIN_N):
        ident, _ = ledger.record(_launch(ts, f"probe-{i}", "SMALL_PROBE", v3=True))
        ledger.save_outcome(ident, {"horizons": {"24h": {"net_return_pct_est": 8.0}}},
                            "resolved")
    for i in range(oo.MIN_N):
        ident, _ = ledger.record(_launch(ts, f"watch-{i}", "WATCH", v3=True))
        ledger.save_outcome(ident, {"horizons": {"24h": {"net_return_pct_est": -2.0}}},
                            "resolved")

    stat = oo.lane_stats()["launch"]
    assert stat["verdict"] == "measured"
    assert stat["edge_verdict"] == "有edge迹象"
    assert stat["probe"]["n"] == stat["control"]["n"] == oo.MIN_N
    assert stat["probe"]["lo"] > stat["control"]["hi"]
    assert oo.actionability_gate("launch")["state"] == "pass"


def test_cascade_has_no_action_gate_without_matched_control(ledger):
    from src.pipeline import opportunity_outcomes as oo

    gate = oo.actionability_gate("cascade")
    assert gate["state"] == "collecting"
    assert "同期可比 WATCH 对照" in gate["reason"]


def test_watch_only_structure_never_gets_directional_hit_rate(ledger):
    from src.pipeline import opportunity_outcomes as oo

    ledger.record({"lane": "structure", "chain": "cex", "token": "ABC-USDT",
                   "symbol": "ABC-USDT", "decision": "WATCH", "state": "live"})
    stat = oo.lane_stats()["structure"]
    assert stat["verdict"] == "not_directional"
    assert "rate" not in stat
    assert "不把事后涨跌" in stat["note"]


def test_carry_uses_absolute_non_directional_outcomes_without_claiming_real_edge(ledger):
    from src.pipeline import opportunity_outcomes as oo

    ts = datetime.now(timezone.utc).isoformat()
    for i in range(oo.MIN_N):
        ident, _ = ledger.record({
            "lane": "carry", "chain": "hyperliquid+okx", "token": f"C{i}",
            "event_key": f"paper:{i}", "symbol": f"C{i}", "detected_at": ts,
            "decision": "PAPER_OPEN", "state": "paper_closed",
        })
        ledger.save_outcome(ident, {
            "kind": "delta_neutral_carry_paper", "net_return_pct": 0.25,
            "cost_is_real_fill": False,
        }, "resolved")

    stat = oo.lane_stats()["carry"]
    assert stat["verdict"] == "measured"
    assert stat["edge_verdict"] == "不可判"
    assert stat["metric"] == "absolute_net_return_after_measured_book_costs"
    assert stat["median_net_return_pct"] == 0.25
    assert stat["cost_is_real_fill"] is False
    assert "实盘" in stat["note"]


def test_airdrop_sums_verified_claims_but_refuses_success_only_hit_rate(ledger):
    from src.pipeline import opportunity_outcomes as oo

    ident, _ = ledger.record({
        "lane": "airdrop", "chain": "ethereum", "token": "campaign",
        "symbol": "Campaign", "decision": "CLAIMED", "state": "claimed",
    })
    ledger.save_outcome(ident, {
        "kind": "airdrop_claim", "gross_reward_usd": 125,
        "actual_cost_usd": 5, "net_reward_usd": 120,
        "reward_is_claimed": True, "cost_is_actual": True,
    }, "resolved")
    ledger.record({
        "lane": "airdrop", "chain": "base", "token": "watching",
        "symbol": "Watching", "decision": "WATCH", "state": "active",
    })

    stat = oo.lane_stats()["airdrop"]
    assert stat["verdict"] == "realized_claims"
    assert stat["n_events"] == 2 and stat["n_claimed"] == 1 and stat["pending"] == 1
    assert stat["net_reward_usd"] == 120
    assert stat["edge_verdict"] == "不可判"
    assert "rate" not in stat and "命中率" in stat["note"]
