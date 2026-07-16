"""The opportunity ledger keeps evidence clocks explicit and immutable."""
from __future__ import annotations

import importlib
import sqlite3
from datetime import datetime, timedelta, timezone

import pytest


def _candidate(**overrides):
    item = {
        "lane": "launch", "chain": "solana", "token": "token", "symbol": "T",
        "event_at": "2026-07-14T11:59:00Z",
        "detected_at": "2026-07-14T12:00:00+00:00",
        "decision_at": "2026-07-14T12:00:02+00:00",
        "quote_at": "2026-07-14T12:00:03+00:00",
        "executable_at": "2026-07-14T12:00:04+00:00",
        "expires_at": "2026-07-14T12:03:00+00:00",
        "state": "live", "decision": "SMALL_PROBE", "entry_price": 1.0,
    }
    item.update(overrides)
    return item


def _entry_observation(**overrides):
    item = {
        "version": 1,
        "provider": "dexscreener",
        "observed_at": "2026-07-14T12:00:01+00:00",
        "chain": "solana",
        "base_token": "token",
        "quote_token": "SOL",
        "pair": "FrozenPool",
        "price": 1.0,
        "currency": "usd",
        "field": "priceUsd",
        "identity_verified": True,
    }
    item.update(overrides)
    return item


def _candidate_with_entry(**overrides):
    from src.pipeline.execution_cost import discovery_contract

    item = _candidate(
        entry_observation=_entry_observation(),
        max_notional_usd=50,
        roundtrip_cost_pct_est=1.0,
        cost_model="test_frozen_cost",
        cost_contract=discovery_contract(
            notional_usd=50,
            modeled_roundtrip_pct=1.0,
            method="test_discovery_cost",
        ),
    )
    item.update(overrides)
    return item


def _price_observation(*, horizon="24h", **overrides):
    anchor = datetime(2026, 7, 14, 12, 0, 1, tzinfo=timezone.utc)
    hours = {"1h": 1, "24h": 24, "7d": 7 * 24}[horizon]
    target = anchor + timedelta(hours=hours)
    item = {
        "version": 1,
        "provider": "geckoterminal",
        "chain": "solana",
        "pool": "FrozenPool",
        "token": "token",
        "token_side": "base",
        "currency": "usd",
        "identity_verified": True,
        "target_at": target.isoformat(),
        "candle_at": target.isoformat(),
        "distance_seconds": 0,
        "price": 1.25,
        "field": "close",
        "retrieved_at": (target + timedelta(minutes=5)).isoformat(),
    }
    item.update(overrides)
    return item


def test_clocks_are_canonical_and_first_observation_is_immutable(tmp_path, monkeypatch):
    from src.pipeline import opportunity_ledger as ledger

    monkeypatch.setattr(ledger, "DB", tmp_path / "ledger.db")
    ledger.record(_candidate())
    ledger.record(_candidate(
        detected_at="2026-07-14T13:00:00+00:00",
        quote_at="2026-07-14T13:00:01+00:00",
        executable_at="2026-07-14T13:00:02+00:00",
        expires_at="2026-07-14T13:03:00+00:00",
    ))

    row = ledger.outcome_rows()[0]
    assert row["event_at"] == "2026-07-14T11:59:00+00:00"
    assert row["detected_at"] == "2026-07-14T12:00:00+00:00"
    assert row["decision_at"] == "2026-07-14T12:00:02+00:00"
    assert row["quote_at"] == "2026-07-14T12:00:03+00:00"
    assert row["executable_at"] == "2026-07-14T12:00:04+00:00"
    assert row["expires_at"] == "2026-07-14T12:03:00+00:00"


def test_v6_launch_snapshot_is_database_immutable_but_outcome_remains_writable(
        tmp_path, monkeypatch):
    from src.pipeline import opportunity_ledger as ledger

    monkeypatch.setattr(ledger, "DB", tmp_path / "ledger.db")
    ident, _ = ledger.record(_candidate_with_entry(cohort_version=6))
    connection = ledger._conn()
    with pytest.raises(sqlite3.IntegrityError, match="snapshot is immutable"):
        connection.execute(
            "UPDATE opportunities SET entry_price=999, cost_model='after_result' "
            "WHERE id=?",
            (ident,),
        )
    connection.rollback()
    connection.execute(
        "UPDATE opportunities SET state='aged_out' WHERE id=?", (ident,)
    )
    connection.commit()
    connection.close()

    ledger.save_outcome(ident, {"horizons": {}, "note": "still appendable"})
    row = ledger.outcome_rows()[0]
    assert row["entry_price"] == 1.0
    assert row["cost_model"] == "test_frozen_cost"
    assert row["state"] == "aged_out"
    assert row["outcome"]["note"] == "still appendable"


def test_entry_observation_is_validated_normalized_and_frozen_on_first_insert(
        tmp_path, monkeypatch):
    from src.pipeline import opportunity_ledger as ledger

    monkeypatch.setattr(ledger, "DB", tmp_path / "ledger.db")
    first = _candidate_with_entry()
    normalized = ledger.validate_entry_observation(first)
    assert normalized["observed_at"] == "2026-07-14T12:00:01+00:00"
    assert normalized["price"] == 1.0
    assert normalized["identity_verified"] is True
    assert normalized["currency"] == "usd"
    assert normalized["field"] == "priceUsd"
    assert normalized["token_side"] == "base"

    ledger.record(first)
    ledger.record(_candidate_with_entry(
        entry_price=2.0,
        entry_observation=_entry_observation(
            observed_at="2026-07-14T12:00:02+00:00",
            pair="LaterPool",
            price=2.0,
        ),
    ))

    row = ledger.outcome_rows()[0]
    assert row["entry_price"] == 1.0
    assert row["entry_observation_version"] == 1
    assert row["entry_observation"] == normalized
    assert row["entry_observation"]["pair"] == "FrozenPool"


@pytest.mark.parametrize(("observation", "message"), [
    (_entry_observation(chain="base"), "chain disagrees"),
    (_entry_observation(base_token="different"), "base_token disagrees"),
    (_entry_observation(price=2), "price disagrees"),
    (_entry_observation(identity_verified=False), "identity_verified"),
    (_entry_observation(currency="eur"), "currency"),
    (_entry_observation(field="close"), "field"),
    (_entry_observation(token_side="quote"), "token_side"),
])
def test_entry_observation_rejects_unproven_or_mismatched_semantics(
        tmp_path, monkeypatch, observation, message):
    from src.pipeline import opportunity_ledger as ledger

    monkeypatch.setattr(ledger, "DB", tmp_path / "ledger.db")
    with pytest.raises(ValueError, match=message):
        ledger.record(_candidate(entry_observation=observation))
    assert not (tmp_path / "ledger.db").exists() or ledger.outcome_rows() == []


def test_record_validates_entry_against_its_computed_detection_clock(
        tmp_path, monkeypatch):
    from src.pipeline import opportunity_ledger as ledger

    monkeypatch.setattr(ledger, "DB", tmp_path / "ledger.db")
    candidate = _candidate(
        decision_at="2000-01-01T00:00:02+00:00",
        entry_observation=_entry_observation(
            observed_at="2000-01-01T00:00:01+00:00"
        ),
    )
    candidate.pop("detected_at")

    with pytest.raises(ValueError, match="before detected_at"):
        ledger.record(candidate)


def test_record_if_absent_never_refreshes_first_event_provenance(tmp_path, monkeypatch):
    from src.pipeline import opportunity_ledger as ledger

    monkeypatch.setattr(ledger, "DB", tmp_path / "ledger.db")
    ident, inserted = ledger.record_if_absent(_candidate(
        source="factory-a", primary_evidence={"pool": "first"}))
    assert inserted is True
    duplicate, inserted = ledger.record_if_absent(_candidate(
        source="factory-b", primary_evidence={"pool": "later"}, decision="WATCH"))
    assert duplicate == ident and inserted is False

    row = ledger.outcome_rows()[0]
    assert row["source"] == "factory-a"
    assert row["decision"] == "SMALL_PROBE"
    assert row["payload"]["primary_evidence"]["pool"] == "first"


def test_cross_store_ledger_id_requires_exact_unique_readback(tmp_path, monkeypatch):
    from src.pipeline import opportunity_ledger as ledger

    monkeypatch.setattr(ledger, "DB", tmp_path / "ledger.db")
    ident, inserted = ledger.record_if_absent(_candidate(token="MintAddress"))

    assert inserted is True
    assert ledger.event_id_readback_matches(
        ident, lane="launch", chain="solana", token="MintAddress")
    assert not ledger.event_id_readback_matches(
        ident, lane="launch", chain="solana", token="different")
    assert not ledger.event_id_readback_matches(
        "missing", lane="launch", chain="solana", token="MintAddress")


def test_airdrop_read_model_uses_latest_verified_campaign_state(tmp_path, monkeypatch):
    from src.pipeline import opportunity_ledger as ledger

    monkeypatch.setattr(ledger, "DB", tmp_path / "ledger.db")
    now = datetime(2026, 7, 15, 12, tzinfo=timezone.utc)
    base = {
        "lane": "airdrop", "chain": "starknet", "token": "campaign",
        "symbol": "Campaign", "detected_at": now.isoformat(),
        "decision_at": now.isoformat(), "state": "claimable",
        "expires_at": (now + timedelta(days=2)).isoformat(),
    }
    ledger.record({**base, "decision": "CLAIM_CHECK",
                   "official_state": "source_verified"})
    ledger.record({**base, "decision": "WATCH", "state": "research",
                   "decision_at": (now + timedelta(minutes=1)).isoformat(),
                   "expires_at": None, "official_state": "source_unverified"})

    failed_closed = ledger.active("airdrop", now=now)[0]
    assert failed_closed["initial_recorded_decision"] == "CLAIM_CHECK"
    assert failed_closed["recorded_decision"] == "WATCH"
    assert failed_closed["effective_decision"] == "WATCH"
    assert failed_closed["official_state"] == "source_unverified"
    assert failed_closed["expires_at"] is None

    ledger.record({**base, "decision": "CLAIM_CHECK",
                   "decision_at": (now + timedelta(minutes=2)).isoformat(),
                   "official_state": "source_verified"})
    restored = ledger.active("airdrop", now=now)[0]
    assert restored["initial_recorded_decision"] == "CLAIM_CHECK"
    assert restored["recorded_decision"] == "CLAIM_CHECK"
    assert restored["effective_decision"] == "CLAIM_CHECK"


def test_carry_refresh_corrects_legacy_measurement_size_without_weakening_other_lanes(
        tmp_path, monkeypatch):
    from src.pipeline import opportunity_ledger as ledger

    monkeypatch.setattr(ledger, "DB", tmp_path / "ledger.db")
    legacy = _candidate(
        lane="carry", chain="hyperliquid+okx", token="BTC", event_key="paper:1",
        decision="PAPER_OPEN", max_notional_usd=10_000,
    )
    ident, _ = ledger.record(legacy)
    ledger.record({
        **legacy, "decision": "WATCH", "max_notional_usd": None,
        "measurement_notional_usd_per_leg": 10_000,
        "measurement_gross_notional_usd": 20_000,
        "position_limit_status": "unknown",
    })
    launch_id, _ = ledger.record(_candidate(token="launch", max_notional_usd=50))
    ledger.record(_candidate(token="launch", decision="WATCH", max_notional_usd=None))

    c = sqlite3.connect(tmp_path / "ledger.db")
    stored = c.execute(
        "SELECT decision,max_notional_usd FROM opportunities WHERE id=?", (ident,)
    ).fetchone()
    launch_stored = c.execute(
        "SELECT decision,max_notional_usd FROM opportunities WHERE id=?", (launch_id,)
    ).fetchone()
    c.close()
    assert stored == ("WATCH", None)
    assert launch_stored == ("SMALL_PROBE", 50)
    row = next(item for item in ledger.outcome_rows() if item["lane"] == "carry")
    assert row["max_notional_usd"] is None
    assert row["measurement_notional_usd_per_leg"] == 10_000
    assert row["position_limit_status"] == "unknown"
    assert row["action_level"] == "A1_WATCH"
    assert row["actionable_now"] is False
    assert row["auto_execution_allowed"] is False


def test_invalidated_source_event_leaves_audit_row_but_not_active_list(tmp_path, monkeypatch):
    from src.pipeline import opportunity_ledger as ledger

    monkeypatch.setattr(ledger, "DB", tmp_path / "ledger.db")
    ident, _ = ledger.record(_candidate())
    assert ledger.invalidate(ident, state="reorg_removed", reason="factory reorg")
    assert ledger.active("launch") == []
    row = ledger.outcome_rows()[0]
    assert row["state"] == "reorg_removed"
    assert row["outcome_state"] == "unresolvable"
    assert row["outcome"]["invalidation"]["reason"] == "factory reorg"


def test_clock_requires_timezone_and_preserves_late_discovery(tmp_path, monkeypatch):
    from src.pipeline import opportunity_ledger as ledger

    monkeypatch.setattr(ledger, "DB", tmp_path / "ledger.db")
    with pytest.raises(ValueError, match="timezone"):
        ledger.record(_candidate(detected_at="2026-07-14T12:00:00"))
    # A source can be discovered only after its deadline. Preserve that miss instead
    # of rejecting the event or pretending the system saw it while actionable.
    ledger.record(_candidate(expires_at="2026-07-14T11:00:00Z"))
    row = ledger.outcome_rows()[0]
    assert row["expires_at"] < row["detected_at"]


def test_legacy_schema_backfills_only_provable_decision_clock(tmp_path, monkeypatch):
    from src.pipeline import opportunity_ledger as ledger

    path = tmp_path / "legacy.db"
    conn = sqlite3.connect(path)
    conn.execute("""CREATE TABLE opportunities(
        id TEXT PRIMARY KEY, lane TEXT NOT NULL, chain TEXT, token TEXT,
        symbol TEXT, detected_at TEXT NOT NULL, event_at TEXT, source TEXT,
        state TEXT NOT NULL, decision TEXT NOT NULL, entry_price REAL,
        invalidation_price REAL, max_notional_usd REAL, payload TEXT NOT NULL,
        outcome_state TEXT NOT NULL DEFAULT 'open', outcome TEXT, updated_at TEXT NOT NULL
    )""")
    conn.execute("""INSERT INTO opportunities(
        id,lane,detected_at,state,decision,payload,updated_at
    ) VALUES ('old','launch','2026-07-14T12:00:00+00:00','live','WATCH','{}',
              '2026-07-14T12:00:00+00:00')""")
    conn.commit()
    conn.close()
    monkeypatch.setattr(ledger, "DB", path)

    row = ledger.outcome_rows()[0]
    assert row["decision_at"] == row["detected_at"]
    assert row["quote_at"] is None
    assert row["executable_at"] is None
    assert row["expires_at"] is None
    assert row["entry_observation_version"] is None
    assert row["entry_observation"] is None
    assert row["price_observations"] == {}

    conn = sqlite3.connect(path)
    opportunity_columns = {
        column[1] for column in conn.execute("PRAGMA table_info(opportunities)")
    }
    price_columns = {
        column[1]
        for column in conn.execute("PRAGMA table_info(outcome_price_observations)")
    }
    triggers = {
        trigger[0]
        for trigger in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='trigger'"
        )
    }
    conn.close()
    assert {"entry_observation_version", "entry_observation"} <= opportunity_columns
    assert {
        "observation_id", "opportunity_id", "horizon",
        "entry_observation_hash", "cost_contract_hash", "payload",
    } <= price_columns
    assert {
        "launch_v6_snapshot_no_update",
        "outcome_price_observations_no_update",
        "outcome_price_observations_no_delete",
    } <= triggers


def test_price_observation_is_idempotent_bound_and_returned_with_outcome_rows(
        tmp_path, monkeypatch):
    from src.pipeline import opportunity_ledger as ledger

    monkeypatch.setattr(ledger, "DB", tmp_path / "ledger.db")
    ident, _ = ledger.record(_candidate_with_entry())
    observation = _price_observation()

    first_id, inserted = ledger.append_price_observation(ident, "24h", observation)
    assert inserted is True
    second_id, inserted = ledger.append_price_observation(
        ident, "24h", dict(reversed(list(observation.items())))
    )
    assert (second_id, inserted) == (first_id, False)

    stored = ledger.price_observations(ident)
    assert len(stored) == 1
    assert stored[0]["observation_id"] == first_id
    assert stored[0]["opportunity_id"] == ident
    assert stored[0]["horizon"] == "24h"
    assert stored[0]["pool"] == "FrozenPool"
    assert len(stored[0]["entry_observation_hash"]) == 64
    assert len(stored[0]["cost_contract_hash"]) == 64
    assert ledger.price_observations() == stored
    row = ledger.outcome_rows()[0]
    assert row["price_observations"] == {"24h": stored[0]}


def test_stored_price_observation_revalidates_all_frozen_bindings(
        tmp_path, monkeypatch):
    from src.pipeline import opportunity_ledger as ledger

    monkeypatch.setattr(ledger, "DB", tmp_path / "ledger.db")
    ident, _ = ledger.record(_candidate_with_entry())
    observation_id, _ = ledger.append_price_observation(
        ident, "24h", _price_observation()
    )
    row = ledger.outcome_rows()[0]

    proven = ledger.validate_stored_price_observation(
        row, "24h", row["price_observations"]["24h"]
    )

    assert proven["observation_id"] == observation_id
    assert proven["opportunity_id"] == ident
    assert proven["horizon"] == "24h"
    assert proven["chain"] == "solana"
    assert proven["token"] == "token"
    assert proven["pool"] == "FrozenPool"
    assert proven["price"] == 1.25


def test_stored_price_observation_rejects_id_hash_and_merge_tampering(
        tmp_path, monkeypatch):
    from src.pipeline import opportunity_ledger as ledger

    monkeypatch.setattr(ledger, "DB", tmp_path / "ledger.db")
    ident, _ = ledger.record(_candidate_with_entry())
    ledger.append_price_observation(ident, "24h", _price_observation())
    row = ledger.outcome_rows()[0]
    stored = row["price_observations"]["24h"]
    cases = []
    bad = {**stored, "observation_id": "0" * 32}
    cases.append((bad, "observation_id"))
    bad = {**stored, "entry_observation_hash": "0" * 64}
    cases.append((bad, "entry_observation_hash binding disagrees"))
    bad = {**stored, "cost_contract_hash": "0" * 64}
    cases.append((bad, "cost_contract_hash binding disagrees"))
    bad = {**stored, "price": 999_999.0}
    cases.append((bad, "observation_id"))
    bad = {**stored, "opportunity_id": "attacker-selected-event"}
    cases.append((bad, "opportunity_id binding disagrees"))
    bad = {**stored, "horizon": "1h"}
    cases.append((bad, "horizon binding disagrees"))

    for tampered, message in cases:
        with pytest.raises(ValueError, match=message):
            ledger.validate_stored_price_observation(row, "24h", tampered)

    changed_contract = {**row, "cost_contract": {**row["cost_contract"], "version": 99}}
    with pytest.raises(ValueError, match="cost_contract_hash binding disagrees"):
        ledger.validate_stored_price_observation(changed_contract, "24h", stored)


@pytest.mark.parametrize(("overrides", "message"), [
    ({"target_at": "2026-07-15T12:00:02+00:00"}, "target_at disagrees"),
    ({"retrieved_at": "2026-07-15T11:59:59+00:00"}, "before its target"),
    ({
        "candle_at": "2026-07-15T09:00:01+00:00",
        "distance_seconds": 10_800,
    }, "more than 7200"),
    ({"distance_seconds": float("nan")}, "finite and nonnegative"),
    ({"price": float("inf")}, "positive finite"),
    ({"currency": "eur"}, "currency"),
    ({"field": "open"}, "field"),
    ({"identity_verified": False}, "identity_verified"),
    ({"chain": "base"}, "chain disagrees"),
    ({"token": "other"}, "token disagrees"),
    ({"pool": "OtherPool"}, "pool disagrees"),
])
def test_stored_price_observation_rejects_semantic_and_clock_tampering(
        tmp_path, monkeypatch, overrides, message):
    from src.pipeline import opportunity_ledger as ledger

    monkeypatch.setattr(ledger, "DB", tmp_path / "ledger.db")
    ident, _ = ledger.record(_candidate_with_entry())
    ledger.append_price_observation(ident, "24h", _price_observation())
    row = ledger.outcome_rows()[0]
    stored = {**row["price_observations"]["24h"], **overrides}

    with pytest.raises(ValueError, match=message):
        ledger.validate_stored_price_observation(row, "24h", stored)


def test_stored_price_observation_survives_restart_and_wall_clock_rollback(
        tmp_path, monkeypatch):
    from src.pipeline import opportunity_ledger as ledger

    path = tmp_path / "ledger.db"
    monkeypatch.setattr(ledger, "DB", path)
    observed = datetime(2099, 1, 1, tzinfo=timezone.utc)

    class AppendClock(datetime):
        @classmethod
        def now(cls, tz=None):
            value = datetime(2099, 1, 2, tzinfo=timezone.utc)
            return value.astimezone(tz) if tz is not None else value.replace(tzinfo=None)

    monkeypatch.setattr(ledger, "datetime", AppendClock)
    candidate = _candidate_with_entry(
        event_at=(observed - timedelta(minutes=2)).isoformat(),
        detected_at=(observed - timedelta(minutes=1)).isoformat(),
        decision_at=(observed + timedelta(seconds=1)).isoformat(),
        quote_at=(observed + timedelta(seconds=2)).isoformat(),
        executable_at=(observed + timedelta(seconds=3)).isoformat(),
        expires_at=(observed + timedelta(minutes=3)).isoformat(),
        entry_observation=_entry_observation(observed_at=observed.isoformat()),
    )
    ident, _ = ledger.record(candidate)
    target = observed + timedelta(hours=1)
    observation_id, _ = ledger.append_price_observation(
        ident,
        "1h",
        _price_observation(
            horizon="1h",
            target_at=target.isoformat(),
            candle_at=target.isoformat(),
            retrieved_at=(target + timedelta(minutes=5)).isoformat(),
        ),
    )

    # Reload restores the real 2026 wall clock, which is earlier than this stored
    # fixture. Validation must use its immutable append clock, not process ``now``.
    restarted = importlib.reload(ledger)
    monkeypatch.setattr(restarted, "DB", path)
    row = restarted.outcome_rows()[0]
    proven = restarted.validate_stored_price_observation(
        row, "1h", row["price_observations"]["1h"]
    )
    assert proven["observation_id"] == observation_id
    assert proven["retrieved_at"] == (target + timedelta(minutes=5)).isoformat()


@pytest.mark.parametrize("reserved", ["observation_id", "created_at", "payload"])
def test_price_observation_rejects_ledger_reserved_payload_fields(
        tmp_path, monkeypatch, reserved):
    from src.pipeline import opportunity_ledger as ledger

    monkeypatch.setattr(ledger, "DB", tmp_path / "ledger.db")
    ident, _ = ledger.record(_candidate_with_entry())
    with pytest.raises(ValueError, match="ledger-reserved"):
        ledger.append_price_observation(
            ident, "24h", _price_observation(**{reserved: "forged"})
        )


def test_price_observation_rejects_conflict_and_frozen_binding_mismatches(
        tmp_path, monkeypatch):
    from src.pipeline import opportunity_ledger as ledger

    monkeypatch.setattr(ledger, "DB", tmp_path / "ledger.db")
    ident, _ = ledger.record(_candidate_with_entry())
    observation = _price_observation()
    ledger.append_price_observation(ident, "24h", observation)

    with pytest.raises(ValueError, match="conflicting price observation"):
        ledger.append_price_observation(
            ident, "24h", _price_observation(price=1.5)
        )
    with pytest.raises(ValueError, match="pool disagrees"):
        ledger.append_price_observation(
            ident, "1h", _price_observation(horizon="1h", pool="OtherPool")
        )
    with pytest.raises(ValueError, match="entry_observation_hash binding disagrees"):
        ledger.append_price_observation(
            ident,
            "1h",
            _price_observation(horizon="1h", entry_observation_hash="forged"),
        )
    assert len(ledger.price_observations(ident)) == 1


@pytest.mark.parametrize(("overrides", "message"), [
    ({"version": 2}, "version"),
    ({"identity_verified": False}, "identity_verified"),
    ({"currency": "eur"}, "currency"),
    ({"token": "different"}, "token disagrees"),
    ({"field": "open"}, "field"),
    ({"retrieved_at": "2099-01-01T00:00:00+00:00"}, "after the ledger clock"),
    ({
        "candle_at": "2026-07-15T13:00:01+00:00",
        "distance_seconds": 0,
    }, "after its target"),
    ({
        "candle_at": "2026-07-15T09:00:01+00:00",
        "distance_seconds": 10_800,
    }, "more than 7200"),
    ({
        "candle_at": "2026-07-15T11:00:01+00:00",
        "distance_seconds": 0,
    }, "distance_seconds disagrees"),
])
def test_price_observation_rejects_unproven_identity_or_invalid_candle(
        tmp_path, monkeypatch, overrides, message):
    from src.pipeline import opportunity_ledger as ledger

    monkeypatch.setattr(ledger, "DB", tmp_path / "ledger.db")
    ident, _ = ledger.record(_candidate_with_entry())

    with pytest.raises(ValueError, match=message):
        ledger.append_price_observation(
            ident, "24h", _price_observation(**overrides)
        )
    assert ledger.price_observations(ident) == []


def test_price_observation_preserves_verified_quote_side_identity(
        tmp_path, monkeypatch):
    from src.pipeline import opportunity_ledger as ledger

    monkeypatch.setattr(ledger, "DB", tmp_path / "ledger.db")
    ident, _ = ledger.record(_candidate_with_entry())

    _, inserted = ledger.append_price_observation(
        ident, "24h", _price_observation(token_side="quote")
    )

    assert inserted is True
    assert ledger.price_observations(ident)[0]["token_side"] == "quote"


def test_price_observation_table_rejects_update_and_delete(tmp_path, monkeypatch):
    from src.pipeline import opportunity_ledger as ledger

    monkeypatch.setattr(ledger, "DB", tmp_path / "ledger.db")
    ident, _ = ledger.record(_candidate_with_entry())
    observation_id, _ = ledger.append_price_observation(
        ident, "24h", _price_observation()
    )

    conn = sqlite3.connect(tmp_path / "ledger.db")
    with pytest.raises(sqlite3.IntegrityError, match="append-only"):
        conn.execute(
            "UPDATE outcome_price_observations SET price=2 WHERE observation_id=?",
            (observation_id,),
        )
    conn.rollback()
    with pytest.raises(sqlite3.IntegrityError, match="append-only"):
        conn.execute(
            "DELETE FROM outcome_price_observations WHERE observation_id=?",
            (observation_id,),
        )
    conn.rollback()
    conn.close()
    assert ledger.price_observations(ident)[0]["price"] == 1.25


def test_price_observation_requires_frozen_entry_and_cost_evidence(
        tmp_path, monkeypatch):
    from src.pipeline import opportunity_ledger as ledger

    monkeypatch.setattr(ledger, "DB", tmp_path / "ledger.db")
    legacy_id, _ = ledger.record(_candidate())
    with pytest.raises(ValueError, match="frozen entry_observation"):
        ledger.append_price_observation(
            legacy_id, "24h", _price_observation()
        )

    no_cost_id, _ = ledger.record(_candidate(
        token="no-cost",
        entry_observation=_entry_observation(base_token="no-cost"),
    ))
    with pytest.raises(ValueError, match="frozen cost_contract"):
        ledger.append_price_observation(
            no_cost_id, "24h", _price_observation()
        )


def test_active_actionability_fails_closed_on_missing_or_expired_quote(tmp_path, monkeypatch):
    from src.pipeline import opportunity_ledger as ledger

    monkeypatch.setattr(ledger, "DB", tmp_path / "ledger.db")
    now = datetime(2026, 7, 14, 12, tzinfo=timezone.utc)
    ledger.record(_candidate(token="fresh", detected_at=now.isoformat(),
                             quote_at=now.isoformat(),
                             expires_at=(now + timedelta(seconds=60)).isoformat()))
    ledger.record(_candidate(token="expired", detected_at=now.isoformat(),
                             quote_at=now.isoformat(),
                             expires_at=(now - timedelta(seconds=1)).isoformat()))
    ledger.record(_candidate(token="legacy", detected_at=now.isoformat(),
                             quote_at=None, expires_at=None))

    rows = {row["token"]: row for row in ledger.active("launch", now=now)}
    assert rows["fresh"]["effective_decision"] == "WATCH"
    assert rows["fresh"]["actionable_now"] is False
    assert rows["fresh"]["evidence_gate"]["state"] == "collecting"
    assert "至少需要 100 个候选" in rows["fresh"]["actionability_reason"]
    assert "SMALL_PROBE 0, WATCH 0" in rows["fresh"]["actionability_reason"]
    assert rows["fresh"]["seconds_to_expiry"] == 60
    assert rows["expired"]["effective_decision"] == "WATCH"
    assert rows["expired"]["actionable_now"] is False
    assert rows["legacy"]["effective_decision"] == "WATCH"
    assert rows["legacy"]["actionability_reason"] == "missing quote or expiry clock"
    # Historical cohort label remains intact for outcome measurement.
    assert all(row["decision"] == "SMALL_PROBE" for row in rows.values())
    assert all(row["action_level"] == "A1_WATCH" for row in rows.values())


def test_active_probe_requires_proven_cost_after_control_edge(tmp_path, monkeypatch):
    from src.pipeline import opportunity_ledger as ledger

    monkeypatch.setattr(ledger, "DB", tmp_path / "ledger.db")
    now = datetime(2026, 7, 14, 12, tzinfo=timezone.utc)
    for i in range(20):
        ident, _ = ledger.record(_candidate(token=f"probe-{i}"))
        ledger.save_outcome(ident, {"horizons": {"24h": {"net_return_pct_est": 8.0}}},
                            "resolved")
    for i in range(20):
        ident, _ = ledger.record(_candidate(token=f"watch-{i}", decision="WATCH"))
        ledger.save_outcome(ident, {"horizons": {"24h": {"net_return_pct_est": -2.0}}},
                            "resolved")
    ledger.record(_candidate(token="fresh", detected_at=now.isoformat(),
                             quote_at=now.isoformat(),
                             expires_at=(now + timedelta(seconds=60)).isoformat()))

    fresh = {row["token"]: row for row in ledger.active("launch", now=now)}["fresh"]
    assert fresh["evidence_gate"]["state"] == "collecting"
    assert fresh["evidence_gate"]["edge_verdict"] == "不可判"
    assert fresh["effective_decision"] == "WATCH"
    assert fresh["action_level"] == "A1_WATCH"
    assert fresh["action_reason_codes"] == ["outside_frozen_edge_protocol"]
    assert fresh["actionable_now"] is False
