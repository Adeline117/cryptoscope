"""EVM factory launches require exact pool and unambiguous quote-side identity."""
import hashlib
from datetime import datetime, timedelta, timezone


WBNB = "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c"
TOKEN = "0x1111111111111111111111111111111111111111"
POOL = "0x3333333333333333333333333333333333333333"


def _pid(host):
    return "provider:" + hashlib.sha256(host.encode()).hexdigest()


def _raw(**overrides):
    item = {"chain": "bsc", "token0": TOKEN, "token1": WBNB, "pool": POOL}
    item.update(overrides)
    return item


def _pair(now, **overrides):
    item = {
        "chainId": "bsc", "pairAddress": POOL, "priceUsd": "0.001",
        "pairCreatedAt": int(now.timestamp() * 1000), "fdv": 100_000,
        "liquidity": {"usd": 20_000}, "volume": {"m5": 1_000},
        "txns": {"m5": {"buys": 10, "sells": 3}},
        "baseToken": {"address": TOKEN, "symbol": "NEW", "name": "New"},
        "quoteToken": {"address": WBNB, "symbol": "WBNB"},
    }
    item.update(overrides)
    return item


def test_quote_side_identity_works_in_both_token_orders():
    from src.pipeline.evm_launch_bridge import identify_target

    assert identify_target(_raw()) == (TOKEN, None)
    assert identify_target(_raw(token0=WBNB, token1=TOKEN)) == (TOKEN, None)


def test_ambiguous_or_double_quote_factory_pair_is_terminal():
    from src.pipeline.evm_launch_bridge import identify_target

    assert identify_target(_raw(token0=TOKEN, token1="0x2222222222222222222222222222222222222222")) \
        == (None, "ambiguous_target")
    assert identify_target(_raw(token0=WBNB,
                                token1="0x55d398326f99059ff775485246999027b3197955")) \
        == (None, "unsupported_quote_pair")


def test_exact_pool_never_substitutes_deeper_old_pool():
    from src.pipeline.evm_launch_bridge import exact_pair

    now = datetime(2026, 7, 15, 12, tzinfo=timezone.utc)
    old = _pair(now, pairAddress="0x4444444444444444444444444444444444444444",
                liquidity={"usd": 9_000_000})
    exact = _pair(now)
    assert exact_pair(_raw(), TOKEN, [old, exact]) is exact


def test_configured_health_includes_never_observed_streams(tmp_path, monkeypatch):
    from src.pipeline import evm_factory_stream, stream_health
    from src.pipeline.evm_launch_bridge import configured_stream_health

    monkeypatch.setattr(stream_health, "DB", tmp_path / "health.db")
    monkeypatch.setattr(evm_factory_stream, "DB", tmp_path / "evm.db")
    rows = configured_stream_health()
    assert len(rows) == 5
    assert all(row["status"] == "missing" for row in rows)
    assert {(row["chain"], row["venue"]) for row in rows} == {
        ("bsc", "pancakeswap_v2"), ("bsc", "pancakeswap_v3"),
        ("base", "pancakeswap_v2"), ("base", "aerodrome"),
        ("ethereum", "pancakeswap_v2"),
    }


def _persist_verified_coverage(evm, spec, now, *, safe_at=None, duration=5):
    safe_at = safe_at or now
    ws_provider = _pid("ws.example")
    http_provider = _pid("audit.example")
    epoch_start = (90 // evm.COVERAGE_EPOCH_BLOCKS) * evm.COVERAGE_EPOCH_BLOCKS
    c = evm._conn()
    try:
        c.execute("""INSERT INTO coverage_epochs(
            chain,venue,factory,topic,epoch_start_block,from_block,to_block,
            checked_at,ws_provider_id,http_provider_id,provider_independent,
            log_count,evidence_digest,segment_count,status
        ) VALUES (?,?,?,?,?,90,100,?,?,?,1,0,?,1,'open')""",
                  (spec.chain, spec.venue, spec.address, spec.topic, epoch_start,
                   now.isoformat(), ws_provider, http_provider,
                   hashlib.sha256(b"bridge-coverage-fixture").hexdigest()))
        c.execute("""INSERT INTO coverage_state(
            chain,venue,factory,topic,coverage_started_block,
            verified_through_block,verified_through_hash,safe_head_block,
            safe_head_hash,safe_head_at,audit_duration_ms,verified_at,
            ws_provider_id,http_provider_id,provider_independent,status,
            consecutive_failures,next_retry_at,last_error_kind,updated_at
        ) VALUES (?,?,?,?,90,?,?,100,?,?,?,?,?,?,1,'verified',0,NULL,NULL,?)""",
                  (spec.chain, spec.venue, spec.address, spec.topic,
                   100, "0x" + "ab" * 32, "0x" + "ab" * 32,
                   safe_at.isoformat(), duration, now.isoformat(),
                   ws_provider, http_provider, now.isoformat()))
        c.commit()
    finally:
        c.close()


def _report_verified_coverage(evm, stream_health, spec, now, *,
                              safe_at=None, duration=5, generation="a" * 32):
    safe_at = safe_at or now
    stream_health.report_worker(
        spec.chain, evm.coverage_stream(spec), status="live", at=now,
        details={
            "schema_version": 2, "state": "verified", "chain": spec.chain,
            "venue": spec.venue, "factory": spec.address,
            "provider_independent": True, "coverage_started_block": 90,
            "verified_through_block": 100,
            "verified_through_hash": "0x" + "ab" * 32,
            "safe_head_block": 100, "safe_head_hash": "0x" + "ab" * 32,
            "safe_head_at": safe_at.isoformat(), "audit_duration_ms": duration,
            "lag_blocks": 0, "verified_at": now.isoformat(),
            "ws_provider_id": _pid("ws.example"),
            "http_provider_id": _pid("audit.example"),
            "connection_generation": generation,
        },
    )


def test_transport_heads_without_finalized_coverage_are_never_live(tmp_path, monkeypatch):
    from src.pipeline import evm_factory_stream as evm, stream_health
    from src.pipeline.evm_launch_bridge import configured_stream_health

    monkeypatch.setattr(stream_health, "DB", tmp_path / "health.db")
    monkeypatch.setattr(evm, "DB", tmp_path / "evm.db")
    spec = evm.bsc_pancake_v2_spec()
    now = datetime.now(timezone.utc)
    stream_health.observe(
        spec.chain, spec.stream, cursor=100, received_at=now,
        expect_contiguous=True,
    )
    row = next(item for item in configured_stream_health()
               if item["chain"] == spec.chain and item["venue"] == spec.venue)
    assert row["transport_status"] == "live"
    assert row["status"] == "degraded"
    assert row["coverage_verified"] is False
    assert row["coverage"]["state"] == "missing"


def test_only_fresh_independent_finalized_coverage_allows_live(tmp_path, monkeypatch):
    from src.pipeline import evm_factory_stream as evm, stream_health
    from src.pipeline.evm_launch_bridge import configured_stream_health

    monkeypatch.setattr(stream_health, "DB", tmp_path / "health.db")
    monkeypatch.setattr(evm, "DB", tmp_path / "evm.db")
    spec = evm.bsc_pancake_v2_spec()
    now = datetime.now(timezone.utc)
    stream_health.observe(
        spec.chain, spec.stream, cursor=100, received_at=now,
        expect_contiguous=True,
    )
    _persist_verified_coverage(evm, spec, now)
    stream_health.report_worker(
        spec.chain, evm.coverage_stream(spec), status="live", at=now,
        details={"schema_version": 2, "state": "verified",
                 "chain": spec.chain, "venue": spec.venue,
                 "factory": spec.address,
                 "provider_independent": True, "verified_through_block": 100,
                 "coverage_started_block": 90,
                 "verified_through_hash": "0x" + "ab" * 32,
                 "safe_head_block": 100, "safe_head_hash": "0x" + "ab" * 32,
                 "safe_head_at": now.isoformat(), "audit_duration_ms": 5,
                 "lag_blocks": 0, "verified_at": now.isoformat(),
                 "ws_provider_id": _pid("ws.example"),
                 "http_provider_id": _pid("audit.example"),
                 "connection_generation": "a" * 32},
    )
    row = next(item for item in configured_stream_health()
               if item["chain"] == spec.chain and item["venue"] == spec.venue)
    assert row["transport_status"] == "live"
    assert row["status"] == "live"
    assert row["coverage_verified"] is True
    assert row["coverage"]["lag_blocks"] == 0


def test_stale_coverage_heartbeat_revokes_live_claim(tmp_path, monkeypatch):
    from src.pipeline import evm_factory_stream as evm, stream_health
    from src.pipeline.evm_launch_bridge import configured_stream_health

    monkeypatch.setattr(stream_health, "DB", tmp_path / "health.db")
    monkeypatch.setattr(evm, "DB", tmp_path / "evm.db")
    spec = evm.bsc_pancake_v2_spec()
    now = datetime.now(timezone.utc)
    stream_health.observe(
        spec.chain, spec.stream, cursor=100, received_at=now,
        expect_contiguous=True,
    )
    _persist_verified_coverage(evm, spec, now)
    stream_health.report_worker(
        spec.chain, evm.coverage_stream(spec), status="live",
        at=now - timedelta(minutes=5),
        details={"schema_version": 2, "state": "verified",
                 "chain": spec.chain, "venue": spec.venue,
                 "factory": spec.address,
                 "provider_independent": True, "verified_through_block": 100,
                 "coverage_started_block": 90,
                 "verified_through_hash": "0x" + "ab" * 32,
                 "safe_head_block": 100, "safe_head_hash": "0x" + "ab" * 32,
                 "safe_head_at": now.isoformat(), "audit_duration_ms": 5,
                 "lag_blocks": 0, "verified_at": now.isoformat(),
                 "ws_provider_id": _pid("ws.example"),
                 "http_provider_id": _pid("audit.example"),
                 "connection_generation": "a" * 32},
    )
    row = next(item for item in configured_stream_health()
               if item["chain"] == spec.chain and item["venue"] == spec.venue)
    assert row["transport_status"] == "live"
    assert row["status"] == "degraded"
    assert row["coverage_verified"] is False
    assert row["coverage_health"]["status"] == "stale"


def test_fresh_worker_cannot_hide_stale_finalized_header(tmp_path, monkeypatch):
    from src.pipeline import evm_factory_stream as evm, stream_health
    from src.pipeline.evm_launch_bridge import configured_stream_health

    monkeypatch.setattr(stream_health, "DB", tmp_path / "health.db")
    monkeypatch.setattr(evm, "DB", tmp_path / "evm.db")
    spec = evm.bsc_pancake_v2_spec()
    now = datetime.now(timezone.utc)
    safe_at = now - timedelta(
        seconds=evm.FINALIZED_HEAD_MAX_AGE_SECONDS[spec.chain] + 1,
    )
    stream_health.observe(
        spec.chain, spec.stream, cursor=100, event_at=now, received_at=now,
        expect_contiguous=True,
    )
    _persist_verified_coverage(evm, spec, now, safe_at=safe_at)
    _report_verified_coverage(evm, stream_health, spec, now, safe_at=safe_at)
    row = next(item for item in configured_stream_health(now=now)
               if item["chain"] == spec.chain and item["venue"] == spec.venue)
    assert row["transport_status"] == "live"
    assert row["status"] == "degraded"
    assert row["coverage_gate_error"] == "finalized_head_stale"
    assert row["safe_head_age_seconds"] > row["max_safe_head_age_seconds"]


def test_three_chain_tip_to_finalized_operational_bounds(tmp_path, monkeypatch):
    from src.pipeline import evm_factory_stream as evm, stream_health
    from src.pipeline.evm_launch_bridge import configured_stream_health

    monkeypatch.setattr(stream_health, "DB", tmp_path / "health.db")
    monkeypatch.setattr(evm, "DB", tmp_path / "evm.db")
    now = datetime.now(timezone.utc)
    specs = (
        evm.bsc_pancake_v2_spec(), evm.base_pancake_v2_spec(),
        evm.ethereum_pancake_v2_spec(),
    )
    for spec in specs:
        limit = evm.FINALIZED_HEAD_MAX_LAG_BLOCKS[spec.chain]
        stream_health.observe(
            spec.chain, spec.stream, cursor=100 + limit,
            event_at=now, received_at=now, expect_contiguous=True,
        )
        _persist_verified_coverage(evm, spec, now)
        _report_verified_coverage(evm, stream_health, spec, now)
    rows = {(row["chain"], row["venue"]): row
            for row in configured_stream_health(now=now)}
    for spec in specs:
        row = rows[(spec.chain, spec.venue)]
        assert row["status"] == "live"
        assert row["tip_to_finalized_lag_blocks"] == (
            evm.FINALIZED_HEAD_MAX_LAG_BLOCKS[spec.chain]
        )

    for spec in specs:
        limit = evm.FINALIZED_HEAD_MAX_LAG_BLOCKS[spec.chain]
        stream_health.observe(
            spec.chain, spec.stream, cursor=101 + limit,
            event_at=now, received_at=now, expect_contiguous=True,
        )
    rows = {(row["chain"], row["venue"]): row
            for row in configured_stream_health(now=now)}
    assert all(
        rows[(spec.chain, spec.venue)]["coverage_gate_error"]
        == "finality_block_lag_exceeded" for spec in specs
    )


def test_corrupt_proof_and_type_confused_cursor_never_live(tmp_path, monkeypatch):
    from src.pipeline import evm_factory_stream as evm, stream_health
    from src.pipeline.evm_launch_bridge import configured_stream_health

    monkeypatch.setattr(stream_health, "DB", tmp_path / "health.db")
    monkeypatch.setattr(evm, "DB", tmp_path / "evm.db")
    spec = evm.bsc_pancake_v2_spec()
    now = datetime.now(timezone.utc)
    stream_health.observe(
        spec.chain, spec.stream, cursor=100, event_at=now, received_at=now,
        expect_contiguous=True,
    )
    _persist_verified_coverage(evm, spec, now)
    _report_verified_coverage(evm, stream_health, spec, now)
    c = evm._conn()
    try:
        c.execute(
            "UPDATE coverage_state SET safe_head_block=50 WHERE chain=? AND venue=?",
            (spec.chain, spec.venue),
        )
        c.commit()
    finally:
        c.close()
    row = next(item for item in configured_stream_health(now=now)
               if item["chain"] == spec.chain and item["venue"] == spec.venue)
    assert row["status"] == "degraded"
    assert row["coverage"]["state"] == "blocked"

    # Restore an exact proof, then corrupt only the read projection's cursor type.
    c = evm._conn()
    try:
        c.execute(
            "UPDATE coverage_state SET safe_head_block=100 WHERE chain=? AND venue=?",
            (spec.chain, spec.venue),
        )
        c.commit()
    finally:
        c.close()
    actual = stream_health.snapshot(now=now)
    original_snapshot = stream_health.snapshot
    for confused in (True, "100", 100.9):
        projected = [dict(item) for item in actual]
        next(item for item in projected
             if item["source"] == spec.chain and item["stream"] == spec.stream)[
                 "cursor"] = confused
        monkeypatch.setattr(stream_health, "snapshot", lambda **_kwargs: projected)
        row = next(item for item in configured_stream_health(now=now)
                   if item["chain"] == spec.chain and item["venue"] == spec.venue)
        assert row["status"] == "degraded"
    monkeypatch.setattr(stream_health, "snapshot", original_snapshot)


def test_public_health_projection_never_echoes_legacy_secrets(tmp_path, monkeypatch):
    from src.pipeline import evm_factory_stream as evm, stream_health
    from src.pipeline.evm_launch_bridge import configured_stream_health

    monkeypatch.setattr(stream_health, "DB", tmp_path / "health.db")
    monkeypatch.setattr(evm, "DB", tmp_path / "evm.db")
    spec = evm.bsc_pancake_v2_spec()
    secret = "tenant-secret.example/private/token"
    stream_health.mark_disconnected(spec.chain, spec.stream, f"failed https://{secret}")
    stream_health.report_worker(
        spec.chain, evm.coverage_stream(spec), status="degraded",
        error=f"failed https://{secret}", details={
            "schema_version": 2, "state": f"https://{secret}",
            "chain": f"https://{secret}", "venue": spec.venue,
            "factory": spec.address,
            "ws_provider_id": f"https://{secret}",
            "http_provider_id": f"provider:{secret}",
            "last_error_kind": f"https://{secret}",
        },
    )
    rows = configured_stream_health()
    assert secret not in repr(rows)
    target = next(row for row in rows
                  if row["chain"] == spec.chain and row["venue"] == spec.venue)
    assert target["transport_status"] == "disconnected"
    assert target["coverage_health"]["details"]["ws_provider_id"] is None
