"""Launch Radar must preserve first-observed facts and never turn thin pools into bets."""
from datetime import datetime, timedelta, timezone

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
    assert got["cohort_version"] == 5
    assert got["cost_contract"]["purpose"] == "discovery_outcome"


def test_probe_and_watch_freeze_the_same_discovery_cost_method():
    from src.pipeline.launch_radar import qualify

    now = datetime.fromtimestamp(1_700_000_000_000 / 1000 + 60 * 30, tz=timezone.utc)
    probe = qualify(_pair(), now=now)
    watch = qualify(_pair(txns={"m5": {"buys": 1, "sells": 3}}), now=now)
    assert probe["decision"] == "SMALL_PROBE" and watch["decision"] == "WATCH"
    assert probe["cost_contract"] == watch["cost_contract"]
    assert probe["roundtrip_cost_pct_est"] == watch["roundtrip_cost_pct_est"]


def test_qualify_rejects_untradeable_or_late_pools():
    from src.pipeline.launch_radar import qualify
    now = datetime.fromtimestamp(1_700_000_000_000 / 1000 + 60 * 30, tz=timezone.utc)
    assert qualify(_pair(liquidity={"usd": 4_999}), now=now) is None
    late = datetime.fromtimestamp(1_700_000_000_000 / 1000 + 25 * 3600, tz=timezone.utc)
    assert qualify(_pair(), now=late) is None


def test_pair_hydration_rejects_unrelated_and_quote_side_deep_pools():
    from src.pipeline.launch_radar import _pair_for_token

    valid = _pair(pairAddress="valid", liquidity={"usd": 10_000},
                  baseToken={"address": "Target", "symbol": "T"})
    unrelated = _pair(pairAddress="unrelated", liquidity={"usd": 9_000_000},
                      baseToken={"address": "Other", "symbol": "O"})
    target_as_quote = _pair(
        pairAddress="quote-side", liquidity={"usd": 8_000_000},
        baseToken={"address": "Other", "symbol": "O"},
        quoteToken={"address": "Target", "symbol": "T"},
    )
    wrong_chain = _pair(pairAddress="wrong-chain", chainId="base",
                        liquidity={"usd": 7_000_000},
                        baseToken={"address": "Target", "symbol": "T"})

    got = _pair_for_token(
        "solana", "Target",
        fetch=lambda _url: [unrelated, target_as_quote, wrong_chain, valid],
    )

    assert got["pairAddress"] == "valid"
    assert _pair_for_token(
        "solana", "Target", fetch=lambda _url: [unrelated, target_as_quote]
    ) is None


def test_pair_hydration_matches_evm_address_case_insensitively():
    from src.pipeline.launch_radar import _pair_for_token

    pair = _pair(chainId="base", baseToken={"address": "0xabcdef", "symbol": "T"})
    assert _pair_for_token("base", "0xAbCdEf", fetch=lambda _url: [pair]) == pair
    solana = _pair(baseToken={"address": "CaseSensitive", "symbol": "T"})
    assert _pair_for_token(
        "solana", "casesensitive", fetch=lambda _url: [solana]
    ) is None


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
    # Discovery decision is now immutable on both read surfaces; current execution
    # measurements live in the append-only assessment table instead.
    assert row["decision"] == "WATCH"
    assert row["action_level"] == "A1_WATCH"
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

    now = datetime(2026, 7, 16, 12, tzinfo=timezone.utc)
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
                                 "screened_out": 0, "orphaned": 0}
    row = ol.active("launch", now=now)[0]
    assert row["primary_evidence"]["signature"] == "sig-primary"
    assert row["source"] == "Pump.fun standard logs + DEX Screener pool"
    c = stream._conn()
    try:
        state = c.execute("SELECT qualification_state,ledger_event_id FROM raw_launches").fetchone()
    finally:
        c.close()
    assert state == ("qualified_recorded", row["id"])


def test_primary_solana_launch_quarantines_failed_ledger_readback(tmp_path, monkeypatch):
    from src.pipeline import launch_radar as lr

    now = datetime(2026, 7, 15, 12, tzinfo=timezone.utc)
    stream = _raw_solana_launch(tmp_path, monkeypatch, now=now)
    pair = _pair(pairCreatedAt=int((now.timestamp() - 60) * 1000))
    monkeypatch.setattr(lr, "event_id_readback_matches", lambda *_args, **_kwargs: False)

    result = lr.scan(
        fetch=lambda url: [] if url == lr.PROFILES_URL else [pair],
        now=now, max_profiles=0, max_primary=1, assessor=lambda event: event,
    )

    assert result["primary"]["recorded"] == 0
    assert result["primary"]["inserted"] == 0
    assert result["primary"]["orphaned"] == 1
    c = stream._conn()
    try:
        state = c.execute(
            "SELECT qualification_state,qualification_error,ledger_event_id "
            "FROM raw_launches"
        ).fetchone()
    finally:
        c.close()
    assert state[0] == "ledger_orphan"
    assert "failed exact read-back" in state[1]
    assert state[2]


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


def test_fast_quote_refresh_appends_without_rediscovery(tmp_path, monkeypatch):
    from src.pipeline import launch_radar as lr
    import src.pipeline.opportunity_ledger as ol

    monkeypatch.setattr(ol, "DB", tmp_path / "ledger.db")
    now = datetime(2026, 7, 16, 12, tzinfo=timezone.utc)
    event = lr.qualify(_pair(pairCreatedAt=int(now.timestamp() * 1000)), now=now)
    event["detected_at"] = now.isoformat()
    ident, _ = ol.record(event)
    calls = []

    def assessor(candidate):
        calls.append(candidate["entry_price"])
        candidate["security_gate"] = {"state": "pass", "checked_at": now.isoformat()}
        candidate["execution_probe"] = {
            "state": "quoted", "source": "test", "api_mode": "test",
            "roundtrip_loss_pct": 2.0, "checked_at": now.isoformat(),
            "entry_reference_price": 0.00011,
            "invalidation_reference_price": 0.000077,
        }
        candidate["quote_at"] = now.isoformat()
        candidate["expires_at"] = (now + timedelta(seconds=60)).isoformat()
        return candidate

    first = lr.refresh_quotes(now=now, assessor=assessor)
    assert first["refreshed"] == 1 and calls == [event["entry_price"]]
    assert lr.refresh_quotes(now=now + timedelta(seconds=10), assessor=assessor)[
        "skipped_fresh"] == 1
    assert ol.outcome_rows()[0]["entry_price"] == event["entry_price"]
    assert ol.latest_execution_assessment(ident)["entry_reference_price"] == 0.00011


def _raw_evm_launch(tmp_path, monkeypatch, *, now):
    from src.pipeline import evm_factory_stream as stream
    import src.pipeline.opportunity_ledger as ol

    monkeypatch.setattr(ol, "DB", tmp_path / "ledger.db")
    monkeypatch.setattr(stream, "DB", tmp_path / "evm.db")
    stream.ensure_bridge_started_at(at=datetime(2026, 7, 15, 11, tzinfo=timezone.utc))
    token = "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"
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
        baseToken={"address": "0x" + token[2:].upper(),
                   "symbol": "EVM", "name": "EVM"},
        quoteToken={"address": quote, "symbol": "WBNB"})
    return stream, token, pool, pair


def test_forward_evm_factory_pool_is_exactly_bridged_as_paper_only(tmp_path, monkeypatch):
    from src.pipeline import launch_radar as lr
    import src.pipeline.opportunity_ledger as ol

    now = datetime(2026, 7, 15, 12, tzinfo=timezone.utc)
    stream, token, pool, pair = _raw_evm_launch(
        tmp_path, monkeypatch, now=now)

    def fetch(url):
        return [] if url == lr.PROFILES_URL else [pair]

    result = lr.scan(fetch=fetch, now=now, max_profiles=0, max_primary=0, max_evm=1,
                     assessor=lambda event: event)
    assert result["primary_evm"]["inserted"] == 1
    row = ol.active("launch", now=now)[0]
    assert row["decision"] == "SMALL_PROBE" and row["primary_evidence"]["pool"] == pool
    assert row["chain"] == "bsc" and row["token"] == token
    assert row["action_level"] == "A1_WATCH"
    assert row["current_assessment"]["route_state"] == "unknown"
    assert row["event_at"] == now.replace(second=0).isoformat()
    c = stream._conn()
    try:
        assert c.execute("SELECT qualification_state,target_token FROM raw_pools").fetchone() \
            == ("qualified_recorded", token)
    finally:
        c.close()


def test_launch_view_revalidates_evm_links_against_exact_ledger_identity(
        tmp_path, monkeypatch):
    from src.pipeline import launch_radar as lr
    from src.pipeline import solana_launch_stream, stream_health
    import src.pipeline.opportunity_ledger as ol

    now = datetime(2026, 7, 15, 12, tzinfo=timezone.utc)
    stream, token, _pool, pair = _raw_evm_launch(
        tmp_path, monkeypatch, now=now)
    monkeypatch.setattr(solana_launch_stream, "DB", tmp_path / "solana.db")
    monkeypatch.setattr(stream_health, "DB", tmp_path / "health.db")
    event = lr.qualify(pair, now=now, source="exact EVM pool")
    event["chain"], event["token"] = "bsc", token
    event["detected_at"] = now.isoformat()
    ident, inserted = ol.record_if_absent(event)
    assert inserted
    c = stream._conn()
    try:
        c.execute("UPDATE raw_pools SET qualification_state='qualified_recorded',"
                  "ledger_event_id=?,target_token=?", (ident, token))
        c.commit()
    finally:
        c.close()

    trace = lr.view()["primary_sources"]["evm"]["qualification"]
    assert trace["qualification"]["qualified_recorded"] == 1
    assert trace["traceability"]["traceable_unique_ledger_events"] == 1
    assert trace["traceability"]["state"] == "ok"

    c = stream._conn()
    try:
        c.execute("UPDATE raw_pools SET target_token=?",
                  ("0x9999999999999999999999999999999999999999",))
        c.commit()
    finally:
        c.close()
    mismatch = lr.view()["primary_sources"]["evm"]["qualification"]
    assert mismatch["qualification"]["qualified_recorded"] == 0
    assert mismatch["qualification"]["ledger_orphan"] == 1
    assert mismatch["traceability"]["state"] == "partial"


@pytest.mark.parametrize("readback_mode", ["mismatch", "unavailable"])
def test_forward_evm_factory_pool_quarantines_failed_exact_ledger_readback(
        tmp_path, monkeypatch, readback_mode):
    from src.pipeline import launch_radar as lr

    now = datetime(2026, 7, 15, 12, tzinfo=timezone.utc)
    stream, token, _pool, pair = _raw_evm_launch(
        tmp_path, monkeypatch, now=now)
    calls = []

    def reject(ident, **identity):
        calls.append((ident, identity))
        if readback_mode == "unavailable":
            raise RuntimeError("ledger database unavailable")
        return False

    monkeypatch.setattr(lr, "event_id_readback_matches", reject)
    result = lr.scan(
        fetch=lambda url: [] if url == lr.PROFILES_URL else [pair],
        now=now, max_profiles=0, max_primary=0, max_evm=1,
        assessor=lambda event: event,
    )

    assert result["primary_evm"]["recorded"] == 0
    assert result["primary_evm"]["inserted"] == 0
    assert result["primary_evm"]["orphaned"] == 1
    assert calls and calls[0][1] == {
        "lane": "launch", "chain": "bsc", "token": token}
    c = stream._conn()
    try:
        state = c.execute(
            "SELECT qualification_state,qualification_reason,target_token,"
            "ledger_event_id FROM raw_pools"
        ).fetchone()
    finally:
        c.close()
    assert state[0] == "ledger_orphan"
    expected = ("failed exact read-back" if readback_mode == "mismatch"
                else "exact read-back unavailable")
    assert expected in state[1]
    assert state[2] == token and state[3]


def test_forward_evm_duplicate_token_stays_separate_without_readback_or_assessment(
        tmp_path, monkeypatch):
    from src.pipeline import launch_radar as lr
    import src.pipeline.opportunity_ledger as ol

    now = datetime(2026, 7, 15, 12, tzinfo=timezone.utc)
    stream, token, _pool, pair = _raw_evm_launch(
        tmp_path, monkeypatch, now=now)
    existing = lr.qualify(pair, now=now, source="existing first launch")
    existing["detected_at"] = (now - timedelta(minutes=1)).isoformat()
    ident, inserted = ol.record_if_absent(existing)
    assert inserted
    monkeypatch.setattr(
        lr, "event_id_readback_matches",
        lambda *_args, **_kwargs: pytest.fail("duplicates must not claim new readback"),
    )

    result = lr.scan(
        fetch=lambda url: [] if url == lr.PROFILES_URL else [pair],
        now=now, max_profiles=0, max_primary=0, max_evm=1,
        assessor=lambda event: pytest.fail("duplicates must not be reassessed"),
    )

    assert result["primary_evm"]["duplicates"] == 1
    assert result["primary_evm"]["inserted"] == 0
    assert result["primary_evm"]["orphaned"] == 0
    assert ol.latest_execution_assessment(ident) is None
    c = stream._conn()
    try:
        state = c.execute(
            "SELECT qualification_state,qualification_reason,target_token,"
            "ledger_event_id FROM raw_pools"
        ).fetchone()
    finally:
        c.close()
    assert state[:3] == (
        "duplicate_token_existing", "token already has a first launch event", token)
    assert state[3] is None
