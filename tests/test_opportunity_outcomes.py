"""Five-lane outcomes must measure forward results without manufacturing edge."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest


@pytest.fixture(autouse=True)
def _explicit_test_source_authorities(monkeypatch):
    """Unit rows use deterministic authorities; SQLite integration is separate."""
    from src.pipeline import edge_validation as ev

    monkeypatch.setattr(
        ev, "_candidate_source_proof",
        lambda _row, snapshot: dict(snapshot["reconciliation_proof"]),
    )
    monkeypatch.setattr(
        ev, "_protocol_admission_state",
        lambda: {"state": "open", "enrollment_open": True, "reason_codes": []},
    )


@pytest.fixture
def ledger(tmp_path, monkeypatch):
    from src.pipeline import opportunity_ledger as ol

    monkeypatch.setattr(ol, "DB", tmp_path / "opportunities.db")
    return ol


def _freeze_test_source_snapshot(*, signature: str, token: str,
                                 detected_at: str, decision_at: str,
                                 slot: int = 1) -> dict:
    from src.contract.launch_selector import freeze_source_snapshot

    proof = {
        "version": 1, "epoch_id": "1" * 32,
        "from_slot": 0, "to_slot": max(1_000, slot),
        "status": "sealed_clean", "checked_at": detected_at,
        "live_provider": "solana_rpc:live.example",
        "archive_provider": "solana_rpc:archive.example",
        "genesis_hash": "mainnet-genesis", "evidence_hash": "e" * 64,
        "finalized_head": max(1_000, slot),
        "live_captured_at": detected_at,
        "live_observation_hash": "a" * 64,
        "archive_observation_hash": "a" * 64,
        "hydration_identity_hash": "b" * 64,
    }
    return freeze_source_snapshot(
        signature=signature, slot=slot, event_type="pump_fun_createv2",
        detected_at=detected_at, captured_at=detected_at,
        decision_at=decision_at, mint=token, raw_payload_hash="a" * 64,
        hydration_payload_hash="b" * 64, capture_mode="live_ws",
        source_provider="solana_rpc:live.example",
        reconciliation_state="verified_live", reconciled_at=detected_at,
        reconciliation_proof=proof,
    )


def _launch(detected_at: str, token: str = "token", decision: str = "SMALL_PROBE",
            *, protocol: bool = False) -> dict:
    item = {"lane": "launch", "chain": "solana", "token": token, "symbol": token,
            "detected_at": detected_at, "entry_price": 100.0, "decision": decision,
            "state": "live", "max_notional_usd": 50,
            "roundtrip_cost_pct_est": 1.0, "cost_model": "test_frozen_cost"}
    if protocol:
        from src.contract.launch_selector import (
            evaluate_selector_snapshot, freeze_selector_snapshot,
        )
        from src.pipeline.execution_cost import solana_launch_full_paper_contract
        from src.pipeline.edge_validation import COHORT_VERSION, LAUNCH_COST_METHOD
        decision_clock = datetime.fromisoformat(detected_at)
        event_clock = decision_clock - timedelta(minutes=30)
        selector_snapshot = freeze_selector_snapshot(
            pool_created_at=event_clock.isoformat(), liquidity_usd=8_000,
            fdv_usd=100_000,
            volume_m5_usd=500 if decision == "SMALL_PROBE" else 0,
            buys_m5=5 if decision == "SMALL_PROBE" else 1, sells_m5=2,
        )
        selector = evaluate_selector_snapshot(
            selector_snapshot, event_at=event_clock.isoformat(),
            decision_at=detected_at,
        )
        contract = solana_launch_full_paper_contract(
            notional_usd=selector["max_notional_usd"],
            modeled_route_roundtrip_pct=selector["modeled_route_roundtrip_pct"],
            method=LAUNCH_COST_METHOD,
        )
        item.update({
            "cohort_version": COHORT_VERSION,
            "source": "Pump.fun standard logs + DEX Screener pool",
            "event_at": event_clock.isoformat(),
            "decision_at": detected_at,
            "max_notional_usd": selector["max_notional_usd"],
            "liquidity_usd": selector["liquidity_usd"],
            "entry_observation": {
                "version": 1, "provider": "dexscreener_token_pairs_v1",
                "observed_at": detected_at, "chain": "solana",
                "base_token": token, "quote_token": "SOL",
                "pair": f"pool-{token}", "price": 100.0,
                "currency": "usd", "field": "priceUsd",
                "identity_verified": True,
                "selector_snapshot": selector_snapshot,
                "source_snapshot": _freeze_test_source_snapshot(
                    signature=f"signature-{token}", token=token,
                    detected_at=detected_at, decision_at=detected_at,
                ),
            },
            "roundtrip_cost_pct_est": contract["all_in_total_pct"],
            "cost_model": LAUNCH_COST_METHOD,
            "cost_contract": contract,
        })
    return item


def _record_with_clock(ledger, monkeypatch, candidate: dict, created_at: datetime):
    real_datetime = ledger.datetime

    class FrozenDatetime(real_datetime):
        @classmethod
        def now(cls, tz=None):
            return created_at.astimezone(tz or timezone.utc)

    with monkeypatch.context() as scoped:
        scoped.setattr(ledger, "datetime", FrozenDatetime)
        return ledger.record(candidate)


def _launch_with_entry(detected_at: str, observed_at: str, token: str = "token") -> dict:
    item = _launch(detected_at, token, protocol=True)
    selector_snapshot = item["entry_observation"]["selector_snapshot"]
    source_snapshot = item["entry_observation"]["source_snapshot"]
    item.update({
        "decision_at": observed_at,
        "entry_observation": {
            "version": 1,
            "provider": "dexscreener_token_pairs_v1",
            "observed_at": observed_at,
            "chain": "solana",
            "base_token": token,
            "quote_token": "So11111111111111111111111111111111111111112",
            "pair": f"pool-{token}",
            "price": 100.0,
            "currency": "usd",
            "field": "priceUsd",
            "identity_verified": True,
            "selector_snapshot": selector_snapshot,
            "source_snapshot": source_snapshot,
        },
    })
    return item


def _observed_price(row: dict, target: datetime, *, price: float = 110.0,
                    **overrides) -> dict:
    item = {
        "version": 1,
        "provider": "geckoterminal_public_v2",
        "network": row["chain"],
        "chain": row["chain"],
        "token": row["token"],
        "token_side": "base",
        "pair": row["entry_observation"]["pair"],
        "pool": row["entry_observation"]["pair"],
        "currency": "usd",
        "field": "close",
        "target_at": target.isoformat(),
        "candle_at": target.isoformat(),
        "distance_seconds": 0,
        "price": price,
        "retrieved_at": (target + timedelta(minutes=5)).isoformat(),
        "identity_verified": True,
    }
    item.update(overrides)
    return item


def _carry_proxy_outcome(*, net_proxy: float | None = 0.25,
                         exit_book_impact: float | None = 0.05) -> dict:
    from src.pipeline.carry_paper import (
        CURRENT_EPISODE_VERSION,
        MODELED_ROUNDTRIP_FEE_PCT,
    )
    from src.pipeline.execution_cost import carry_paper_contract

    contract = carry_paper_contract(
        notional_usd_per_leg=10_000,
        entry_book_impact_pct=0.05,
        exit_book_impact_pct=exit_book_impact,
        modeled_fee_pct=MODELED_ROUNDTRIP_FEE_PCT,
    )
    quoted_integral = (net_proxy + contract["modeled_proxy_total_pct"]
                       if net_proxy is not None
                       and contract["modeled_proxy_total_pct"] is not None else 0.54)
    return {
        "kind": "delta_neutral_carry_paper",
        "episode_version": CURRENT_EPISODE_VERSION,
        "net_proxy_after_book_quotes_and_modeled_fee_pct": net_proxy,
        "close_reason": "diff_below_floor",
        "book_quote_cost_complete": exit_book_impact is not None,
        "entry_book_impact_pct": 0.05, "exit_book_impact_pct": exit_book_impact,
        "cost_contract": contract,
        "unmeasured_h": 0, "hold_h": 48,
        "quoted_rate_integral_pct": quoted_integral,
        "observation_version": 1, "exit_quote_delay_s": 0,
        "cost_is_real_fill": False, "real_edge_eligible": False,
    }


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


def test_resolver_uses_entry_observation_clock_and_append_only_price_evidence(ledger):
    from src.pipeline import opportunity_outcomes as oo

    now = datetime(2026, 7, 15, 12, tzinfo=timezone.utc)
    detected = now - timedelta(hours=26)
    observed = now - timedelta(hours=23)
    ident, _ = ledger.record(_launch_with_entry(
        detected.isoformat(), observed.isoformat(), "anchored",
    ))
    ledger.save_outcome(ident, {
        "horizons": {"1h": {"net_return_pct_est": 0}},
    })
    calls = []

    not_due = oo.resolve(
        now=now,
        price_at=lambda row, when: calls.append(when) or _observed_price(row, when),
    )
    assert not_due["lookups"] == 0
    assert calls == []

    resolved_at = now + timedelta(hours=2)
    got = oo.resolve(
        now=resolved_at,
        price_at=lambda row, when: calls.append(when) or _observed_price(row, when),
    )
    assert got["settled"] == 1
    assert calls == [observed + timedelta(hours=24)]

    row = ledger.outcome_rows()[0]
    point = row["outcome"]["horizons"]["24h"]
    observation = row["price_observations"]["24h"]
    assert point["outcome_anchor_at"] == observed.isoformat()
    assert point["target_at"] == (observed + timedelta(hours=24)).isoformat()
    assert point["price_observation_id"] == observation["observation_id"]
    assert observation["opportunity_id"] == ident
    assert observation["pool"] == "pool-anchored"
    assert observation["token"] == "anchored"


def test_resolver_reuses_appended_price_after_outcome_write_interruption(
        ledger, monkeypatch):
    from src.pipeline import opportunity_outcomes as oo

    now = datetime(2026, 7, 15, 12, tzinfo=timezone.utc)
    observed = now - timedelta(hours=25)
    ident, _ = ledger.record(_launch_with_entry(
        (observed - timedelta(minutes=9)).isoformat(), observed.isoformat(), "resume",
    ))
    row = ledger.outcome_rows()[0]
    target = observed + timedelta(hours=24)
    evidence = _observed_price(row, target)
    observation_id, inserted = ledger.append_price_observation(ident, "24h", evidence)
    assert inserted is True

    def should_not_fetch(_row, _when):
        raise AssertionError("immutable observation should be reused")

    got = oo.resolve(now=now, price_at=should_not_fetch)
    point = ledger.outcome_rows()[0]["outcome"]["horizons"]["24h"]
    assert got["settled"] == 1
    assert point["price_observation_id"] == observation_id


def test_resolver_rejects_unbound_typed_price_instead_of_settling(ledger):
    from src.pipeline import opportunity_outcomes as oo

    now = datetime(2026, 7, 15, 12, tzinfo=timezone.utc)
    observed = now - timedelta(hours=25)
    ledger.record(_launch_with_entry(
        (observed - timedelta(minutes=3)).isoformat(), observed.isoformat(), "strict",
    ))

    def wrong_pool(row, when):
        return _observed_price(row, when, pool="attacker-selected-pool", price=999_999)

    got = oo.resolve(now=now, price_at=wrong_pool)
    row = ledger.outcome_rows()[0]
    assert got["settled"] == 0
    assert "24h" not in (row["outcome"].get("horizons") or {})
    assert "pool disagrees" in row["outcome"]["price_observation_rejected"]["24h"]["reason"]
    assert row["price_observations"] == {}


def test_default_launch_price_reader_uses_the_frozen_pair(monkeypatch):
    from src.pipeline import opportunity_outcomes as oo
    from src.pipeline import outcome_tracker

    when = datetime(2026, 7, 15, 12, tzinfo=timezone.utc)
    row = _launch_with_entry(
        (when - timedelta(hours=1)).isoformat(), when.isoformat(), "exact-token",
    )
    calls = []
    monkeypatch.setattr(
        outcome_tracker,
        "_price_observation_at",
        lambda token, chain, pool, target: calls.append(
            (token, chain, pool, target)
        ) or {"price": 1},
    )

    assert oo._default_price_at(row, when) == {"price": 1}
    assert calls == [("exact-token", "solana", "pool-exact-token", when)]


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
    assert stat["not_due_24h"] == stat["due_24h"] == 0
    legacy = stat["legacy_distribution"]
    assert legacy["not_due_24h"] == 1
    assert legacy["due_24h"] == 1
    assert legacy["attempted_unpriced_24h"] == 1
    assert legacy["oldest_due_24h_hours"] >= 1.9


def test_stats_refuse_rate_below_minimum_sample(ledger):
    from src.pipeline import opportunity_outcomes as oo

    ident, _ = ledger.record(_launch(datetime.now(timezone.utc).isoformat()))
    ledger.save_outcome(ident, {"horizons": {"24h": {"net_return_pct_est": 20.0}}})
    stat = oo.lane_stats()["launch"]
    assert stat["verdict"] == "不可判"
    assert "rate" not in stat
    assert stat["n"] == 0
    assert stat["legacy_distribution"]["n"] == 1
    assert stat["legacy_distribution"]["edge_eligible"] is False
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


def test_old_twenty_sample_wilson_separation_cannot_pass_edge_gate(
        ledger, monkeypatch):
    from src.pipeline import opportunity_outcomes as oo
    from src.pipeline import edge_validation as ev

    now = datetime.now(timezone.utc).replace(microsecond=0)
    boundary = (now - timedelta(hours=1)).isoformat()
    monkeypatch.setattr(ev, "PROTOCOL_START_AT", boundary)
    ts = now.isoformat()
    for i in range(oo.MIN_N):
        ident, _ = ledger.record(_launch(
            ts, f"probe-{i}", "SMALL_PROBE", protocol=True
        ))
        ledger.save_outcome(ident, {"horizons": {"24h": {"net_return_pct_est": 8.0}}},
                            "resolved")
    for i in range(oo.MIN_N):
        ident, _ = ledger.record(_launch(
            ts, f"watch-{i}", "WATCH", protocol=True
        ))
        ledger.save_outcome(ident, {"horizons": {"24h": {"net_return_pct_est": -2.0}}},
                            "resolved")

    stat = oo.lane_stats()["launch"]
    assert stat["verdict"] == "不可判"
    assert stat["n"] == 0
    assert stat["legacy_distribution"]["n"] == 0
    assert stat["edge_verdict"] == "不可判"
    assert stat["probe"]["eligible_n"] == stat["control"]["eligible_n"] == oo.MIN_N
    assert stat["probe"]["n"] == stat["control"]["n"] == 0
    assert stat["probe"]["invalid_n"] == stat["control"]["invalid_n"] == oo.MIN_N
    assert stat["edge_validation"]["state"] == "collecting"
    assert stat["edge_validation"]["next_look_n_per_arm"] == 100
    gate = oo.actionability_gate("launch")
    assert gate["state"] == "collecting"
    assert gate["measured_n"] == 0
    assert gate["real_edge_n"] == 0
    assert gate["execution_edge_eligible"] is False


def test_malformed_current_launch_row_cannot_hide_from_integrity_denominator(
        monkeypatch):
    from src.pipeline import edge_validation as ev
    from src.pipeline import opportunity_outcomes as oo

    now = datetime.now(timezone.utc).replace(microsecond=0)
    monkeypatch.setattr(
        ev, "PROTOCOL_START_AT", (now - timedelta(hours=1)).isoformat()
    )
    row = _launch_with_entry(
        (now - timedelta(minutes=31)).isoformat(),
        (now - timedelta(minutes=1)).isoformat(),
        "broken-current",
    )
    row.update({"id": "broken-current", "entry_price": 0})
    row.pop("entry_observation")
    monkeypatch.setattr(oo.opportunity_ledger, "outcome_rows", lambda *args, **kwargs: [row])

    stat = oo.lane_stats()["launch"]
    gate = oo.actionability_gate("launch")

    assert stat["n"] == 0
    assert stat["legacy_distribution"]["n_events"] == 0
    assert stat["edge_validation"]["state"] == "protocol_integrity_blocked"
    assert stat["current_protocol"]["integrity_candidate_n"] == 1
    assert stat["current_protocol"]["integrity_invalid_n"] == 1
    assert gate["state"] == "blocked"
    assert gate["integrity_invalid_n"] == 1


def test_v6_resolver_uses_complete_cost_and_strict_append_truth(ledger, monkeypatch):
    from src.pipeline import edge_validation as ev
    from src.pipeline import opportunity_outcomes as oo

    now = datetime.now(timezone.utc).replace(microsecond=0)
    boundary = (now - timedelta(days=3)).isoformat()
    monkeypatch.setattr(ev, "PROTOCOL_START_AT", boundary)
    observed = now - timedelta(hours=25)
    ident, _ = _record_with_clock(ledger, monkeypatch, _launch_with_entry(
        (observed - timedelta(minutes=2)).isoformat(),
        observed.isoformat(),
        "v6-cost",
    ), observed + timedelta(seconds=1))

    got = oo.resolve(
        now=now,
        price_at=lambda row, when: _observed_price(row, when, price=110.0),
    )
    row = ledger.outcome_rows()[0]
    point = row["outcome"]["horizons"]["24h"]

    assert got["settled"] == 1
    assert row["outcome"]["cost_model"] == ev.LAUNCH_COST_METHOD
    expected_cost = row["cost_contract"]["all_in_total_pct"]
    assert row["outcome"]["cost_pct_est"] == pytest.approx(expected_cost)
    assert point["net_return_pct_est"] == pytest.approx(10.0 - expected_cost)
    assert oo._cohort([row], "SMALL_PROBE")["n"] == 1

    tampered = dict(row["outcome"])
    tampered["horizons"] = dict(tampered["horizons"])
    tampered["horizons"]["24h"] = {
        **tampered["horizons"]["24h"], "net_return_pct_est": 999_999,
    }
    ledger.save_outcome(ident, tampered, "open")
    cohort = oo._cohort(ledger.outcome_rows(), "SMALL_PROBE")
    assert cohort["n"] == 0
    assert cohort["invalid_n"] == 1


def test_v6_invalid_frozen_cost_is_permanently_rejected_without_lookup(
        ledger, monkeypatch):
    from src.pipeline import edge_validation as ev
    from src.pipeline import opportunity_outcomes as oo

    now = datetime.now(timezone.utc).replace(microsecond=0)
    boundary = (now - timedelta(days=3)).isoformat()
    monkeypatch.setattr(ev, "PROTOCOL_START_AT", boundary)
    observed = now - timedelta(hours=25)
    ident, _ = _record_with_clock(ledger, monkeypatch, _launch_with_entry(
        (observed - timedelta(minutes=2)).isoformat(),
        observed.isoformat(),
        "v6-bad-cost",
    ), observed + timedelta(seconds=1))
    c = ledger._conn()
    c.execute("DROP TRIGGER launch_v6_snapshot_no_update")
    c.execute("DROP TRIGGER launch_v6_snapshot_no_update_v2")
    c.execute("UPDATE opportunities SET cost_model='tampered' WHERE id=?", (ident,))
    c.commit()
    c.close()
    calls = []

    first = oo.resolve(
        now=now,
        price_at=lambda row, when: calls.append((row, when)) or 110.0,
    )
    row = ledger.outcome_rows()[0]
    rejection = row["outcome"]["cost_evidence_rejected"]

    assert first["lookups"] == first["settled"] == 0
    assert first["cost_evidence_rejected"] == 1
    assert calls == []
    assert row["outcome_state"] == "open"
    assert rejection["reason_code"] == "launch_v6_cost_evidence_rejected"
    assert "row_cost_model_mismatch" in rejection["reasons"]
    assert rejection["permanent"] is True

    second = oo.resolve(now=now + timedelta(minutes=1), price_at=lambda *_: calls.append(1))
    assert second["cost_evidence_rejected"] == 0
    assert calls == []


def test_v6_source_authority_outage_defers_then_retries_settlement(
        ledger, monkeypatch):
    from src.pipeline import edge_validation as ev
    from src.pipeline import opportunity_outcomes as oo

    now = datetime.now(timezone.utc).replace(microsecond=0)
    monkeypatch.setattr(ev, "PROTOCOL_START_AT", (now - timedelta(days=3)).isoformat())
    observed = now - timedelta(hours=25)
    _record_with_clock(ledger, monkeypatch, _launch_with_entry(
        (observed - timedelta(minutes=2)).isoformat(), observed.isoformat(),
        "v6-source-retry",
    ), observed + timedelta(seconds=1))

    def unavailable(*_args, **_kwargs):
        raise OSError("source database locked")

    monkeypatch.setattr(ev, "_candidate_source_proof", unavailable)
    first = oo.resolve(now=now, price_at=lambda *_: pytest.fail("price lookup ran"))
    deferred = ledger.outcome_rows()[0]["outcome"]["settlement_deferred"]

    assert first["lookups"] == first["cost_evidence_rejected"] == 0
    assert first["source_evidence_deferred"] == 1
    assert deferred["reason_code"] == "launch_v6_source_authority_deferred"
    assert deferred["reasons"] == ["source_proof_unverifiable"]
    assert deferred["permanent"] is False

    monkeypatch.setattr(
        ev, "_candidate_source_proof",
        lambda _row, snapshot: dict(snapshot["reconciliation_proof"]),
    )
    second = oo.resolve(
        now=now + timedelta(minutes=1),
        price_at=lambda row, when: _observed_price(row, when, price=110.0),
    )
    row = ledger.outcome_rows()[0]
    assert second["settled"] == 1
    assert second["source_evidence_deferred"] == 0
    assert "settlement_blocked" not in row["outcome"]
    assert "settlement_deferred" not in row["outcome"]


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


def test_carry_uses_descriptive_quote_proxies_without_claiming_real_edge(ledger):
    from src.pipeline import opportunity_outcomes as oo

    ts = datetime.now(timezone.utc).isoformat()
    for i in range(oo.MIN_N):
        ident, _ = ledger.record({
            "lane": "carry", "chain": "hyperliquid+okx", "token": f"C{i}",
            "event_key": f"paper:{i}", "symbol": f"C{i}", "detected_at": ts,
            "decision": "WATCH", "state": "paper_closed",
        })
        ledger.save_outcome(ident, _carry_proxy_outcome(), "resolved")

    stat = oo.lane_stats()["carry"]
    assert stat["verdict"] == "measured"
    assert stat["edge_verdict"] == "不可判"
    assert stat["metric"] == "quote_rate_integral_minus_book_quotes_and_modeled_fee_proxy"
    assert stat["cohort_kind"] == "descriptive_quote_proxy"
    assert stat["median_net_proxy_pct"] == 0.25
    assert stat["real_edge_n"] == 0
    assert stat["real_edge_eligible"] is False
    assert stat["all_in_total_pct"] is None
    assert stat["cost_completeness"] == "partial"
    assert stat["cost_is_real_fill"] is False
    assert "不能据此判定正EV" in stat["note"]


def test_carry_quarantines_legacy_missing_market_and_incomplete_cost(ledger):
    from src.pipeline import opportunity_outcomes as oo

    ts = datetime.now(timezone.utc).isoformat()
    complete = _carry_proxy_outcome()
    outcomes = [
        {**_carry_proxy_outcome(net_proxy=9), "episode_version": None},
        {**_carry_proxy_outcome(net_proxy=8), "close_reason": "market_missing"},
        _carry_proxy_outcome(net_proxy=7, exit_book_impact=None),
        _carry_proxy_outcome(net_proxy=None),
        _carry_proxy_outcome(net_proxy=-0.25),
    ]
    for i, outcome in enumerate(outcomes):
        ident, _ = ledger.record({
            "lane": "carry", "chain": "hyperliquid+okx", "token": f"Q{i}",
            "event_key": f"paper:q{i}", "symbol": f"Q{i}", "detected_at": ts,
            "decision": "WATCH", "state": "paper_closed",
        })
        ledger.save_outcome(ident, outcome, "resolved")

    stat = oo.lane_stats()["carry"]
    assert stat["total_closed"] == 5
    assert stat["n"] == 1 and stat["hits"] == 0
    assert stat["excluded_closed"] == 4
    assert stat["excluded_by_reason"] == {
        "legacy_episode": 1,
        "market_missing_close": 1,
        "incomplete_book_quote_cost": 1,
        "missing_result": 1,
    }
    assert stat["real_edge_n"] == 0


def test_airdrop_sums_verified_claims_but_refuses_success_only_hit_rate(ledger):
    from src.pipeline import opportunity_outcomes as oo

    tx_hash = "0x" + "a" * 64
    ident, _ = ledger.record({
        "lane": "airdrop", "chain": "ethereum", "token": "campaign",
        "symbol": "Campaign", "decision": "CLAIMED", "state": "claimed",
        "source_state": "source_verified",
        "source_verification": {
            "official_page_verified": True,
            "evidence_page_verified": True,
        },
    })
    verification = {
        "tx_id": tx_hash, "confirmed_at": "2026-07-13T12:01:00+00:00",
        "onchain_success": True,
        "campaign_semantics_verified": True,
        "beneficiary_verified": True,
        "reward_amount_verified": True,
        "reward_usd_verified": True,
        "actual_cost_usd_verified": True,
        "verified_campaign_id": "campaign",
        "verified_beneficiary": "owned-wallet",
        "verified_reward_amount": 125,
        "verified_reward_asset": "USD",
        "verified_reward_usd": 125,
        "verified_actual_cost_usd": 5,
    }
    ledger.save_outcome(ident, {
        "version": 3, "kind": "airdrop_claim",
        "campaign_id": "campaign",
        "claim_verification_state": "fully_verified",
        "claimed_at": "2026-07-13T12:01:00+00:00",
        "reported_claimed_at": "2026-07-13T12:00:00+00:00",
        "tx_url": "https://etherscan.io/tx/" + tx_hash,
        "chain": "ethereum",
        "gross_reward_usd": 125,
        "actual_cost_usd": 5, "net_reward_usd": 120,
        "verified_reward_amount": 125, "verified_reward_asset": "USD",
        "reward_is_claimed": True, "cost_is_actual": True,
        "campaign_semantics_verified": True,
        "beneficiary_verified": True,
        "reward_amount_verified": True,
        "reward_usd_verified": True,
        "actual_cost_usd_verified": True,
        "transaction_verification": verification,
    }, "resolved")
    ledger.record({
        "lane": "airdrop", "chain": "base", "token": "watching",
        "symbol": "Watching", "decision": "WATCH", "state": "active",
    })

    stat = oo.lane_stats()["airdrop"]
    assert stat["verdict"] == "realized_claims"
    assert stat["n_events"] == 2 and stat["n_claimed"] == 1 and stat["pending"] == 1
    assert stat["n_transaction_verified"] == 1
    assert stat["n_claim_semantics_verified"] == 1
    assert stat["n_reward_valued"] == stat["n_fully_verified_claims"] == 1
    assert stat["net_reward_usd"] == 120
    assert stat["edge_verdict"] == "不可判"
    assert "rate" not in stat and "命中率" in stat["note"]


def test_airdrop_transaction_only_and_legacy_claims_never_enter_verified_pnl(ledger):
    from src.pipeline import opportunity_outcomes as oo

    legacy_id, _ = ledger.record({
        "lane": "airdrop", "chain": "ethereum", "token": "legacy",
        "symbol": "Legacy", "decision": "CLAIMED", "state": "claimed",
    })
    ledger.save_outcome(legacy_id, {
        "version": 2, "kind": "airdrop_claim", "gross_reward_usd": 1_000_000,
        "actual_cost_usd": 0, "net_reward_usd": 1_000_000,
        "reward_is_claimed": True, "cost_is_actual": True,
    }, "resolved")

    tx_id, _ = ledger.record({
        "lane": "airdrop", "chain": "starknet", "token": "tx-only",
        "symbol": "Tx only", "decision": "WATCH", "state": "claimed",
    })
    ledger.save_outcome(tx_id, {
        "version": 3, "kind": "airdrop_claim_evidence",
        "claim_verification_state": "transaction_only",
        "reported_reward_usd": 500_000,
        "reported_actual_cost_usd": 1,
        "transaction_verification": {
            "tx_id": "0xabc", "confirmed_at": "2026-07-13T12:01:00+00:00",
            "onchain_success": True, "campaign_semantics_verified": False,
        },
    }, "open")

    stat = oo.lane_stats()["airdrop"]

    assert stat["n_events"] == 2
    assert stat["n_transaction_verified"] == 1
    assert stat["n_claim_semantics_verified"] == stat["n_reward_valued"] == 0
    assert stat["n_fully_verified_claims"] == stat["n_claimed"] == 0
    assert stat["verdict"] == stat["edge_verdict"] == "不可判"
    assert "net_reward_usd" not in stat
    assert "不完整记录不计入净回报" in stat["note"]


def test_airdrop_stats_fail_closed_on_malformed_or_forged_v3_outcomes(ledger):
    from src.pipeline import opportunity_outcomes as oo

    tx_hash = "0x" + "a" * 64
    ident, _ = ledger.record({
        "lane": "airdrop", "chain": "ethereum", "token": "campaign",
        "symbol": "Campaign", "decision": "CLAIMED", "state": "claimed",
        "source_state": "source_verified",
        "source_verification": {
            "official_page_verified": True, "evidence_page_verified": True,
        },
    })

    for malformed in (["not", "an", "object"], "truthy string", 7, True):
        ledger.save_outcome(ident, malformed, "resolved")
        stat = oo.lane_stats()["airdrop"]
        assert stat["n_fully_verified_claims"] == 0
        assert "net_reward_usd" not in stat

    base_verification = {
        "tx_id": tx_hash, "confirmed_at": "2026-07-13T12:01:00+00:00",
        "onchain_success": True,
        "campaign_semantics_verified": True,
        "beneficiary_verified": True,
        "reward_amount_verified": True,
        "reward_usd_verified": True,
        "actual_cost_usd_verified": True,
        "verified_campaign_id": "campaign",
        "verified_beneficiary": "owned-wallet",
        "verified_reward_amount": 125,
        "verified_reward_asset": "USD",
        "verified_reward_usd": 125,
        "verified_actual_cost_usd": 5,
    }
    base_outcome = {
        "version": 3, "kind": "airdrop_claim", "campaign_id": "campaign",
        "claim_verification_state": "fully_verified",
        "claimed_at": "2026-07-13T12:01:00+00:00",
        "reported_claimed_at": "2026-07-13T12:00:00+00:00",
        "tx_url": "https://etherscan.io/tx/" + tx_hash,
        "chain": "ethereum",
        "gross_reward_usd": 125, "actual_cost_usd": 5, "net_reward_usd": 120,
        "verified_reward_amount": 125, "verified_reward_asset": "USD",
        "reward_is_claimed": True, "cost_is_actual": True,
        "campaign_semantics_verified": True, "beneficiary_verified": True,
        "reward_amount_verified": True, "reward_usd_verified": True,
        "actual_cost_usd_verified": True,
    }
    invalid_cases = (
        ("verification", "verified_campaign_id", "other-campaign"),
        ("verification", "verified_beneficiary", " "),
        ("verification", "verified_reward_asset", ""),
        ("verification", "verified_reward_amount", True),
        ("verification", "verified_reward_amount", float("nan")),
        ("verification", "verified_reward_amount", float("inf")),
        ("verification", "verified_reward_amount", -1),
        ("verification", "verified_reward_usd", float("nan")),
        ("verification", "verified_actual_cost_usd", float("inf")),
        ("verification", "confirmed_at", "9999-12-31T23:59:59-23:59"),
        ("outcome", "version", 3.0),
        ("outcome", "campaign_id", "other-campaign"),
        ("outcome", "claimed_at", None),
        ("outcome", "reported_claimed_at", None),
        ("outcome", "tx_url", "https://etherscan.io/tx/" + "b" * 64),
        ("outcome", "chain", "base"),
        ("outcome", "verified_reward_amount", 124),
        ("outcome", "verified_reward_asset", "ETH"),
        ("outcome", "gross_reward_usd", True),
        ("outcome", "net_reward_usd", float("nan")),
    )
    for target, key, value in invalid_cases:
        verification = {**base_verification}
        outcome = {**base_outcome}
        if target == "verification":
            verification[key] = value
        else:
            outcome[key] = value
        outcome["transaction_verification"] = verification
        ledger.save_outcome(ident, outcome, "resolved")
        stat = oo.lane_stats()["airdrop"]
        assert stat["n_fully_verified_claims"] == 0, (target, key, value)
        assert "net_reward_usd" not in stat, (target, key, value)


def test_airdrop_stats_reject_nonfinite_aggregate_even_when_rows_are_finite(ledger):
    from src.pipeline import opportunity_outcomes as oo

    for index in range(2):
        campaign_id = f"huge-{index}"
        tx_hash = "0x" + str(index + 1) * 64
        ident, _ = ledger.record({
            "lane": "airdrop", "chain": "ethereum", "token": campaign_id,
            "symbol": campaign_id, "decision": "CLAIMED", "state": "claimed",
            "source_state": "source_verified",
            "source_verification": {
                "official_page_verified": True, "evidence_page_verified": True,
            },
        })
        verification = {
            "tx_id": tx_hash,
            "confirmed_at": "2026-07-13T12:01:00+00:00",
            "onchain_success": True,
            "campaign_semantics_verified": True, "beneficiary_verified": True,
            "reward_amount_verified": True, "reward_usd_verified": True,
            "actual_cost_usd_verified": True,
            "verified_campaign_id": campaign_id,
            "verified_beneficiary": "owned-wallet",
            "verified_reward_amount": 1e308, "verified_reward_asset": "USD",
            "verified_reward_usd": 1e308, "verified_actual_cost_usd": 0,
        }
        ledger.save_outcome(ident, {
            "version": 3, "kind": "airdrop_claim", "campaign_id": campaign_id,
            "claim_verification_state": "fully_verified",
            "claimed_at": "2026-07-13T12:01:00+00:00",
            "reported_claimed_at": "2026-07-13T12:00:00+00:00",
            "tx_url": "https://etherscan.io/tx/" + tx_hash,
            "chain": "ethereum",
            "gross_reward_usd": 1e308, "actual_cost_usd": 0,
            "net_reward_usd": 1e308,
            "verified_reward_amount": 1e308, "verified_reward_asset": "USD",
            "reward_is_claimed": True, "cost_is_actual": True,
            "campaign_semantics_verified": True, "beneficiary_verified": True,
            "reward_amount_verified": True, "reward_usd_verified": True,
            "actual_cost_usd_verified": True,
            "transaction_verification": verification,
        }, "resolved")

    stat = oo.lane_stats()["airdrop"]

    assert stat["n_fully_verified_claims"] == 2
    assert stat["verdict"] == "不可判"
    assert "net_reward_usd" not in stat
    assert "聚合非有限" in stat["note"]


def test_empty_airdrop_lane_is_explicitly_scored_as_zero_events(ledger):
    from src.pipeline import opportunity_outcomes as oo

    stat = oo.lane_stats()["airdrop"]

    assert stat["n_events"] == stat["n_claimed"] == stat["pending"] == 0
    assert stat["verdict"] == stat["edge_verdict"] == "不可判"
    assert "尚无" in stat["note"]
