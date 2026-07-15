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


def _raw_solana_launch(tmp_path, monkeypatch, *, now):
    import src.pipeline.opportunity_ledger as ol
    from src.pipeline import solana_launch_stream as stream
    from src.pipeline import stream_health

    monkeypatch.setattr(ol, "DB", tmp_path / "ledger.db")
    monkeypatch.setattr(stream, "DB", tmp_path / "solana.db")
    monkeypatch.setattr(stream_health, "DB", tmp_path / "health.db")
    c = stream._conn()
    try:
        c.execute("""INSERT INTO raw_launches(
            signature,slot,program,event_type,creator,mint,detected_at,hydrated_at,
            raw_payload_hash,hydration_payload_hash,logs,evidence_state,qualification_state
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                  ("sig-primary", 123, stream.PUMP_FUN_PROGRAM, "pump_fun_createv2",
                   "creator", "Token", now.isoformat(), now.isoformat(), "a" * 64,
                   "b" * 64, "[]", "complete", "raw_unqualified"))
        c.commit()
    finally:
        c.close()
    return stream


def test_primary_solana_launch_is_bridged_to_ledger(tmp_path, monkeypatch):
    from src.pipeline import launch_radar as lr
    import src.pipeline.opportunity_ledger as ol

    now = datetime(2026, 7, 15, 12, tzinfo=timezone.utc)
    stream = _raw_solana_launch(tmp_path, monkeypatch, now=now)
    pair = _pair(pairCreatedAt=int((now.timestamp() - 60) * 1000))

    def fetch(url):
        if url == lr.PROFILES_URL:
            return []
        assert url.endswith("/solana/Token")
        return [pair]

    def assessor(event):
        event["decision"] = "WATCH"
        event["security_gate"] = {"state": "unknown", "reason": "test"}
        event["execution_probe"] = {"state": "skipped"}
        return event

    result = lr.scan(fetch=fetch, now=now, max_profiles=0, max_primary=1,
                     assessor=assessor)
    assert result["primary"] == {"available": True, "attempted": 1, "recorded": 1,
                                 "inserted": 1, "pending": 0, "errors": 0,
                                 "screened_out": 0}
    row = ol.active("launch", now=now)[0]
    assert row["primary_evidence"]["signature"] == "sig-primary"
    assert row["source"] == "Pump.fun standard logs + DEX Screener pool"
    c = stream._conn()
    try:
        state = c.execute("SELECT qualification_state,ledger_event_id FROM raw_launches").fetchone()
    finally:
        c.close()
    assert state == ("qualified_recorded", row["id"])


def test_primary_market_miss_remains_retryable(tmp_path, monkeypatch):
    from src.pipeline import launch_radar as lr

    now = datetime(2026, 7, 15, 12, tzinfo=timezone.utc)
    stream = _raw_solana_launch(tmp_path, monkeypatch, now=now)

    result = lr.scan(fetch=lambda url: [] if url == lr.PROFILES_URL else [], now=now,
                     max_profiles=0, max_primary=1, assessor=lambda event: event)
    assert result["primary"]["pending"] == 1
    c = stream._conn()
    try:
        state, error = c.execute(
            "SELECT qualification_state,qualification_error FROM raw_launches"
        ).fetchone()
    finally:
        c.close()
    assert state == "market_pending" and "not indexed" in error


def test_launch_view_exposes_primary_stream_coverage(tmp_path, monkeypatch):
    from src.pipeline import launch_radar as lr

    now = datetime.now(timezone.utc)
    _raw_solana_launch(tmp_path, monkeypatch, now=now)
    payload = lr.view()
    solana = payload["primary_sources"]["solana"]
    assert solana["available"] is True
    assert solana["qualification"]["raw_total"] == 1
    assert solana["qualification"]["recent_complete"] == 1


def test_forward_evm_factory_pool_is_exactly_bridged_as_watch(tmp_path, monkeypatch):
    from src.pipeline import evm_factory_stream as stream
    from src.pipeline import launch_radar as lr
    import src.pipeline.opportunity_ledger as ol

    now = datetime(2026, 7, 15, 12, tzinfo=timezone.utc)
    monkeypatch.setattr(ol, "DB", tmp_path / "ledger.db")
    monkeypatch.setattr(stream, "DB", tmp_path / "evm.db")
    stream.ensure_bridge_started_at(at=datetime(2026, 7, 15, 11, tzinfo=timezone.utc))
    token = "0x1111111111111111111111111111111111111111"
    quote = "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c"
    pool = "0x3333333333333333333333333333333333333333"
    c = stream._conn()
    try:
        c.execute("""INSERT INTO raw_pools(
          chain,venue,factory,transaction_hash,log_index,block_number,block_hash,
          transaction_index,token0,token1,pool,pair_index,block_at,detected_at,
          updated_at,raw_payload_hash,removed,evidence_state,qualification_state
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,'complete','raw_unqualified')""",
                  ("bsc", "pancakeswap_v2", stream.PANCAKE_V2_FACTORY, "0x" + "ab" * 32,
                   3, 100, "0x" + "cd" * 32, 2, token, quote, pool, 42,
                   (now.replace(second=0)).isoformat(), now.isoformat(), now.isoformat(),
                   "e" * 64))
        c.commit()
    finally:
        c.close()
    pair = _pair(chainId="bsc", pairAddress=pool, pairCreatedAt=int(
        now.replace(second=0).timestamp() * 1000),
        baseToken={"address": token, "symbol": "EVM", "name": "EVM"},
        quoteToken={"address": quote, "symbol": "WBNB"})

    def fetch(url):
        return [] if url == lr.PROFILES_URL else [pair]

    result = lr.scan(fetch=fetch, now=now, max_profiles=0, max_primary=0, max_evm=1,
                     assessor=lambda event: event)
    assert result["primary_evm"]["inserted"] == 1
    row = ol.active("launch", now=now)[0]
    assert row["decision"] == "WATCH" and row["primary_evidence"]["pool"] == pool
    assert row["event_at"] == now.replace(second=0).isoformat()
    c = stream._conn()
    try:
        assert c.execute("SELECT qualification_state,target_token FROM raw_pools").fetchone() \
            == ("qualified_recorded", token)
    finally:
        c.close()
