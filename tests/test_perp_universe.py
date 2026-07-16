"""Fail-closed contracts for the perpetual research universe.

Every collector call is mocked.  These tests intentionally prove exclusion and
cache preservation more heavily than the happy path: a heuristic token join is
useful for research display, but it is never an actionable scanner universe.
"""

from __future__ import annotations

import copy
import errno
import hashlib
import io
import json
import multiprocessing
import threading
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

import src.onchain.perp_universe as pu


NOW = datetime(2026, 7, 16, 12, 0, tzinfo=timezone.utc)


class _Guard:
    def __init__(self, states: list[str] | None = None) -> None:
        self.states = list(states or ["ok", "ok"])
        self.calls: list[str] = []

    def require_evidence_write(self, source: str) -> dict[str, object]:
        self.calls.append(source)
        state = self.states.pop(0) if self.states else "ok"
        return {"state": state, "sample_id": len(self.calls)}


@pytest.fixture(autouse=True)
def _isolated(tmp_path, monkeypatch):
    guard = _Guard()
    monkeypatch.setattr(pu, "_CACHE", tmp_path / "perp_universe.json")
    monkeypatch.setattr(pu.stream_disk_guard, "GUARD", guard)
    monkeypatch.setattr(pu, "_utc_now", lambda: NOW)
    monkeypatch.delenv(pu.CCXT_MODE_ENV, raising=False)

    def no_network(*_args, **_kwargs):
        raise AssertionError("test attempted real network access")

    class NoNetworkOpener:
        open = staticmethod(no_network)

    monkeypatch.setattr(pu.urllib.request, "urlopen", no_network)
    monkeypatch.setattr(
        pu.urllib.request, "build_opener", lambda *_handlers: NoNetworkOpener(),
    )
    return guard


def _install_http_open(monkeypatch, open_call):
    installed_handlers = []

    class Opener:
        def open(self, request, timeout):
            return open_call(request, timeout)

    def build_opener(*handlers):
        installed_handlers.extend(handlers)
        return Opener()

    monkeypatch.setattr(pu.urllib.request, "build_opener", build_opener)
    return installed_handlers


def _evidence(payload: object) -> dict[str, object]:
    raw = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), allow_nan=False,
    ).encode()
    return {
        "payload": copy.deepcopy(payload),
        "sha256": hashlib.sha256(raw).hexdigest(),
        "bytes": len(raw),
    }


def _market_rows(count: int = 300, *, btc_size: str = "0.01") -> list[dict]:
    rows = []
    for index in range(count):
        base = "BTC" if index == 0 else f"T{index:03d}"
        rows.append({
            "instId": f"{base}-USDT-SWAP",
            "instType": "SWAP",
            "state": "live",
            "ctType": "linear",
            "settleCcy": "USDT",
            "ctValCcy": base,
            "ctVal": btc_size if index == 0 else "1",
        })
    return rows


def _install_sources(
    monkeypatch,
    *,
    rows: list[dict] | None = None,
    address: str = "0xAbC",
    mapped_count: int = 1_000,
    market_page_mutator=None,
) -> list[str]:
    rows = copy.deepcopy(rows if rows is not None else _market_rows())
    assert 0 <= mapped_count <= pu._COINGECKO_MARKET_EXPECTED_UNIQUE_IDS
    coin_list = []
    ranked_rows = []
    for index in range(pu._COINGECKO_MARKET_EXPECTED_UNIQUE_IDS):
        coin_id = "bitcoin" if index == 0 else f"coin-{index:03d}"
        symbol = "btc" if index == 0 else f"t{index:03d}"
        platforms = {"ethereum": address} if index < mapped_count else {}
        coin_list.append({
            "id": coin_id,
            "symbol": symbol,
            "platforms": platforms,
        })
        ranked_rows.append({"id": coin_id, "symbol": symbol})
    coin_list.extend([
        {"id": "official-empty-symbol", "symbol": "", "platforms": {}},
        {"id": "official-spaced-symbol", "symbol": "yee\N{NO-BREAK SPACE}",
         "platforms": {}},
    ])
    market_pages = {
        page: ranked_rows[
            (page - 1) * pu._COINGECKO_MARKET_PAGE_SIZE:
            page * pu._COINGECKO_MARKET_PAGE_SIZE
        ]
        for page in range(1, pu._COINGECKO_MARKET_PAGE_COUNT + 1)
    }
    if market_page_mutator is not None:
        market_page_mutator(market_pages)
    calls: list[str] = []

    def fetch(url: str, timeout: int = 20) -> dict[str, object]:
        assert timeout == 20
        calls.append(url)
        if url == pu._OKX_TIME_URL:
            return _evidence({
                "code": "0",
                "msg": "",
                "data": [{"ts": str(int(NOW.timestamp() * 1000))}],
            })
        if url == pu._OKX_INSTRUMENTS_URL:
            return _evidence({"code": "0", "msg": "", "data": rows})
        if url == pu._COINGECKO_LIST_URL:
            return _evidence(coin_list)
        for page in range(1, pu._COINGECKO_MARKET_PAGE_COUNT + 1):
            if url == pu._COINGECKO_MARKETS_URL.format(page=page):
                return _evidence(market_pages[page])
        raise AssertionError("unexpected mocked URL")

    monkeypatch.setattr(pu, "_fetch_json", fetch)
    return calls


def _shadow_snapshot(rows: list[dict] | None = None) -> dict[str, object]:
    rows = rows if rows is not None else _market_rows()
    now_ms = int(NOW.timestamp() * 1000)
    markets = [{
        "market_id": row["instId"],
        "symbol": f"{row['ctValCcy']}/USDT:USDT",
        "base": row["ctValCcy"],
        "quote": "USDT",
        "settle": "USDT",
        "active": True,
        "linear": True,
        "amount_unit": "contracts",
        "contract_size": row["ctVal"],
        "contract_size_unit": row["ctValCcy"],
    } for row in rows]
    return {
        "schema_version": 1,
        "status": "observed",
        "exchange": "okx",
        "market_type": "swap",
        "mode": "shadow",
        "adapter": {
            "name": "ccxt",
            "version": "4.5.58",
            "exchange_class": "okx",
            "transport": "public_rest",
        },
        "source_identity": {
            "authority_key": "exchange:okx",
            "dataset_key": "okx:public-rest:swap-markets",
            "path_key": "ccxt-rest:4.5.58",
        },
        "exchange_time": {
            "server_time_ms": now_ms - 1_000,
            "probe_started_at_ms": now_ms - 2_000,
            "probe_completed_at_ms": now_ms - 1_000,
        },
        "request_timing": {
            "markets_started_at_ms": now_ms - 1_000,
            "markets_completed_at_ms": now_ms,
        },
        "raw_responses": {
            "exchange_time": {
                "sha256": "a" * 64, "bytes": 10,
                "body": "must not reach cache", "json": {"secret": False},
            },
            "markets": {
                "sha256": "b" * 64, "bytes": 20,
                "body": "must not reach cache", "json": {"secret": False},
            },
        },
        "markets": markets,
    }


def _read_cache() -> dict:
    return json.loads(pu._CACHE.read_text(encoding="utf-8"))


def _write_cache(payload: dict, *, resign: bool = True) -> None:
    if resign:
        payload["cache_digest"] = pu._cache_digest(payload)
    pu._CACHE.write_text(json.dumps(payload), encoding="utf-8")


def _retime_payload(
    payload: dict,
    *,
    source_observed_at: datetime,
    mapping_observed_at: datetime,
    generated_at: datetime | None = None,
) -> dict:
    for source in payload["source_health"]["sources"]:
        previous_observed = datetime.fromisoformat(source["observed_at"])
        shift = source_observed_at - previous_observed
        for field in (
            "local_started_at",
            "local_completed_at",
            "exchange_time_at",
            "observed_at",
        ):
            source[field] = (
                datetime.fromisoformat(source[field]) + shift
            ).isoformat()
    payload["mapping_source"]["observed_at"] = mapping_observed_at.isoformat()
    if generated_at is not None:
        payload["generated_at"] = generated_at.isoformat()
        payload["expires_at"] = (
            generated_at + timedelta(seconds=pu.CACHE_TTL_SECONDS)
        ).isoformat()
    payload["cache_digest"] = pu._cache_digest(payload)
    return payload


def _make_valid_cache(
    monkeypatch,
    *,
    age_source_vector: bool = True,
    **source_kwargs,
) -> dict:
    _install_sources(monkeypatch, **source_kwargs)
    result = pu.refresh_result()
    assert result["status"] == "research_only"
    payload = _read_cache()
    if age_source_vector:
        _retime_payload(
            payload,
            source_observed_at=NOW - timedelta(seconds=2),
            mapping_observed_at=NOW - timedelta(seconds=1),
        )
        _write_cache(payload)
    return payload


class _SpawnGuard:
    def require_evidence_write(self, _source: str) -> dict[str, object]:
        return {"state": "ok"}


def _spawn_publish_candidate(
    cache_path: str,
    payload: dict,
    now_iso: str,
    ready,
    start,
    results,
) -> None:
    pu._CACHE = Path(cache_path)
    pu._utc_now = lambda: datetime.fromisoformat(now_iso)
    pu.stream_disk_guard.GUARD = _SpawnGuard()
    ready.set()
    if not start.wait(10):
        results.put(("timeout", None))
        return
    try:
        result = pu._publish_candidate(payload)
    except pu._ContractError as exc:
        results.put(("rejected", exc.reason_code))
    except Exception as exc:
        results.put(("unexpected", type(exc).__name__))
    else:
        results.put(("ok", result.get("refresh_status")))


def _spawn_hold_publish_lock(
    cache_path: str,
    entered,
    release,
    results,
) -> None:
    pu._CACHE = Path(cache_path)
    try:
        with pu._cache_publish_lock():
            entered.set()
            if not release.wait(10):
                results.put(("timeout", None))
                return
    except Exception as exc:
        results.put(("unexpected", type(exc).__name__))
    else:
        results.put(("ok", None))


def test_off_refresh_is_research_only_and_compatibility_loaders_are_empty(
    monkeypatch,
    _isolated,
):
    calls = _install_sources(monkeypatch)
    monkeypatch.setattr(
        pu, "_ccxt_shadow_snapshot",
        lambda: pytest.fail("CCXT must have zero calls while mode is off"),
    )

    result = pu.refresh_result()

    assert result["status"] == "research_only"
    assert result["refresh_status"] == "written"
    assert result["source_counts"] == {
        "independent_source_count": 1,
        "observed_path_count": 1,
    }
    assert len(calls) == 7
    assert _isolated.calls == ["perp_universe"] * 4
    research = result["research_universe"]["BTC"]
    assert research["mapping_method"] == "symbol_market_cap_heuristic"
    assert research["actionability"] == "research_only"
    assert research["address"] == "0xabc"
    assert result["actionable_universe"] == {}
    assert pu.load() == {}
    assert pu.by_chain() == {}

    cached = _read_cache()
    assert cached["expires_at"] == (
        NOW + timedelta(seconds=pu.CACHE_TTL_SECONDS)
    ).isoformat()
    assert not pu._contains_forbidden_cache_key(cached)
    assert set(cached["source_health"]["sources"][0]["response_sha256"]) == {
        "exchange_time", "instruments",
    }
    assert cached["mapping_source"]["list_row_count"] == 1_002
    assert cached["mapping_source"]["list_usable_row_count"] == 1_000
    assert cached["mapping_source"]["list_excluded_row_count"] == 2
    assert cached["mapping_source"]["market_unique_id_count"] == 1_000
    assert [
        page["row_count"] for page in cached["mapping_source"]["market_pages"]
    ] == [250, 250, 250, 250]


def test_refresh_wrapper_preserves_signature_but_never_promotes_heuristic(monkeypatch):
    _install_sources(monkeypatch)
    assert pu.refresh() == {}
    assert pu.load_result()["research_universe"]["BTC"]["actionability"] == (
        "research_only"
    )


def test_shadow_is_one_authority_two_paths_and_raw_bodies_are_not_cached(
    monkeypatch,
):
    _install_sources(monkeypatch)
    monkeypatch.setenv(pu.CCXT_MODE_ENV, "shadow")
    monkeypatch.setattr(pu, "_ccxt_shadow_snapshot", _shadow_snapshot)

    result = pu.refresh_result()

    assert result["source_counts"] == {
        "independent_source_count": 1,
        "observed_path_count": 2,
    }
    health = result["source_health"]
    assert {row["authority_key"] for row in health["sources"]} == {"exchange:okx"}
    assert {row["path_key"] for row in health["sources"]} == {
        "native-urllib:v2", "ccxt-rest:4.5.58",
    }
    assert not pu._contains_forbidden_cache_key(_read_cache())


@pytest.mark.parametrize(
    ("mutation", "reason"),
    [
        ("unavailable", "ccxt_shadow_unavailable"),
        ("missing_market", "ccxt_shadow_inventory_conflict"),
        ("unit_conflict", "ccxt_shadow_unit_conflict"),
    ],
)
def test_shadow_failure_or_conflict_never_overwrites_good_cache(
    monkeypatch,
    mutation,
    reason,
):
    _make_valid_cache(monkeypatch)
    before = pu._CACHE.read_bytes()
    _install_sources(monkeypatch)
    monkeypatch.setenv(pu.CCXT_MODE_ENV, "shadow")
    shadow = _shadow_snapshot()
    if mutation == "unavailable":
        shadow["status"] = "unavailable"
    elif mutation == "missing_market":
        shadow["markets"].pop()
    else:
        shadow["markets"][0]["contract_size"] = "0.010"
    monkeypatch.setattr(pu, "_ccxt_shadow_snapshot", lambda: shadow)

    result = pu.refresh_result()

    assert result["reason_codes"] == [reason]
    assert result["cache_preserved"] is True
    assert pu._CACHE.read_bytes() == before


def test_initial_critical_disk_guard_means_zero_http_and_zero_write(
    monkeypatch,
    _isolated,
):
    _isolated.states = ["critical"]
    monkeypatch.setattr(
        pu, "_fetch_json", lambda *_a, **_k: pytest.fail("HTTP call after CRITICAL"),
    )
    monkeypatch.setattr(
        pu, "_atomic_write_cache",
        lambda *_a, **_k: pytest.fail("write after CRITICAL"),
    )

    result = pu.refresh_result()

    assert result["status"] == "blocked"
    assert result["reason_codes"] == ["disk_critical"]
    assert not pu._CACHE.exists()


def test_critical_transition_before_write_spends_http_but_writes_nothing(
    monkeypatch,
    _isolated,
):
    _isolated.states = ["ok", "critical"]
    calls = _install_sources(monkeypatch)

    result = pu.refresh_result()

    assert result["reason_codes"] == ["disk_critical_before_write"]
    assert len(calls) == 7
    assert not pu._CACHE.exists()


@pytest.mark.parametrize(
    "states",
    [
        ["ok", "ok", "critical"],
        ["ok", "ok", "ok", "critical"],
    ],
)
def test_critical_after_lock_or_at_atomic_commit_never_writes(
    monkeypatch,
    _isolated,
    states,
):
    _isolated.states = states
    _install_sources(monkeypatch)
    writes = []
    monkeypatch.setattr(
        pu,
        "_atomic_write_cache",
        lambda *_args: writes.append("write") or pytest.fail("write under CRITICAL"),
    )

    result = pu.refresh_result()

    assert result["status"] == "blocked"
    assert result["reason_codes"] == ["disk_critical_before_write"]
    assert writes == []
    assert not pu._CACHE.exists()


def test_operational_floor_rejects_truncated_native_catalog_before_mapping(
    monkeypatch,
):
    _make_valid_cache(monkeypatch)
    before = pu._CACHE.read_bytes()
    calls = _install_sources(monkeypatch, rows=_market_rows(299))

    result = pu.refresh_result()

    assert result["reason_codes"] == ["okx_inventory_below_operational_floor"]
    assert calls == [pu._OKX_TIME_URL, pu._OKX_INSTRUMENTS_URL]
    assert pu._CACHE.read_bytes() == before


def test_large_drop_from_last_valid_inventory_is_rejected(monkeypatch):
    _make_valid_cache(monkeypatch, rows=_market_rows(500))
    before = pu._CACHE.read_bytes()
    calls = _install_sources(monkeypatch, rows=_market_rows(374))

    result = pu.refresh_result()

    assert result["reason_codes"] == ["okx_inventory_operational_drop"]
    assert len(calls) == 7
    assert pu._CACHE.read_bytes() == before


def test_market_inventory_retention_exact_75_percent_passes(monkeypatch):
    _make_valid_cache(monkeypatch, rows=_market_rows(500))
    _install_sources(monkeypatch, rows=_market_rows(375))

    result = pu.refresh_result()

    assert result["status"] == "research_only"
    assert result["market_count"] == 375
    assert result["mapped_count"] == 375


def test_refresh_reads_one_locked_incumbent_for_both_retention_gates(monkeypatch):
    _make_valid_cache(
        monkeypatch,
        rows=_market_rows(500),
        mapped_count=400,
    )
    _install_sources(
        monkeypatch,
        rows=_market_rows(375),
        mapped_count=300,
    )
    baseline_reads = []
    real_read = pu._read_cache_bytes

    def read_cache():
        baseline_reads.append("read")
        return real_read()

    monkeypatch.setattr(
        pu,
        "_read_cache_bytes",
        read_cache,
    )

    result = pu.refresh_result()

    assert result["status"] == "research_only"
    assert result["market_count"] == 375
    assert result["mapped_count"] == 300
    # One incumbent read plus one post-write verification readback.
    assert baseline_reads == ["read", "read"]


def test_14_day_stale_cache_still_protects_both_inventory_baselines(monkeypatch):
    _make_valid_cache(monkeypatch, rows=_market_rows(500), mapped_count=400)
    monkeypatch.setattr(
        pu, "_utc_now",
        lambda: NOW + timedelta(days=14),
    )

    assert pu.load_result()["status"] == "stale"
    counts = pu._previous_inventory_counts()
    assert counts == (500, 400)
    with pytest.raises(pu._ContractError) as caught:
        pu._reject_operational_inventory_drop(374, counts[0])
    assert caught.value.reason_code == "okx_inventory_operational_drop"
    with pytest.raises(pu._ContractError) as caught:
        pu._reject_mapping_completeness(375, 299, counts[1])
    assert caught.value.reason_code == "coingecko_mapping_operational_drop"


def test_baseline_older_than_14_days_or_from_future_is_ignored(monkeypatch):
    _make_valid_cache(monkeypatch, rows=_market_rows(500), mapped_count=400)

    assert pu._previous_inventory_counts(
        publish_now=NOW + timedelta(days=14, microseconds=1),
    ) is None
    assert pu._previous_inventory_counts(
        publish_now=NOW - timedelta(microseconds=1),
    ) is None


@pytest.mark.parametrize(
    ("case", "reason"),
    [
        ("short_page", "coingecko_markets_page_size_invalid"),
        ("long_page", "coingecko_markets_page_size_invalid"),
        ("cross_page_duplicate", "coingecko_markets_id_duplicate"),
    ],
)
def test_coingecko_ranked_inventory_is_exact_and_preserves_old_cache(
    monkeypatch,
    case,
    reason,
):
    _make_valid_cache(monkeypatch)
    before = pu._CACHE.read_bytes()

    def mutate(pages):
        if case == "short_page":
            pages[1].pop()
        elif case == "long_page":
            pages[1].append({"id": "overflow", "symbol": "overflow"})
        else:
            pages[2][0] = copy.deepcopy(pages[1][0])

    _install_sources(monkeypatch, market_page_mutator=mutate)

    result = pu.refresh_result()

    assert result["reason_codes"] == [reason]
    assert result["cache_preserved"] is True
    assert pu._CACHE.read_bytes() == before


def test_initial_mapping_coverage_exact_third_passes(monkeypatch):
    _install_sources(
        monkeypatch,
        rows=_market_rows(300),
        mapped_count=100,
    )

    result = pu.refresh_result()

    assert result["status"] == "research_only"
    assert result["market_count"] == 300
    assert result["mapped_count"] == 100


def test_initial_mapping_coverage_one_below_third_is_rejected(monkeypatch):
    _install_sources(
        monkeypatch,
        rows=_market_rows(300),
        mapped_count=99,
    )

    result = pu.refresh_result()

    assert result["reason_codes"] == ["coingecko_mapping_coverage_below_floor"]
    assert not pu._CACHE.exists()


def test_mapping_coverage_rejection_preserves_old_cache_byte_for_byte(monkeypatch):
    _make_valid_cache(monkeypatch)
    before = pu._CACHE.read_bytes()
    _install_sources(
        monkeypatch,
        rows=_market_rows(300),
        mapped_count=99,
    )

    result = pu.refresh_result()

    assert result["reason_codes"] == ["coingecko_mapping_coverage_below_floor"]
    assert result["cache_preserved"] is True
    assert pu._CACHE.read_bytes() == before


def test_mapping_retention_exact_75_percent_passes(monkeypatch):
    _make_valid_cache(monkeypatch, mapped_count=300)
    _install_sources(monkeypatch, mapped_count=225)

    result = pu.refresh_result()

    assert result["status"] == "research_only"
    assert result["mapped_count"] == 225


def test_mapping_retention_one_below_75_percent_preserves_old_cache(monkeypatch):
    _make_valid_cache(monkeypatch, mapped_count=300)
    before = pu._CACHE.read_bytes()
    _install_sources(monkeypatch, mapped_count=224)

    result = pu.refresh_result()

    assert result["reason_codes"] == ["coingecko_mapping_operational_drop"]
    assert result["cache_preserved"] is True
    assert pu._CACHE.read_bytes() == before


@pytest.mark.parametrize(
    ("mutation", "reason"),
    [
        ("page_count", "mapping_source_evidence_invalid"),
        ("unique_count", "mapping_source_unavailable"),
        ("coverage", "coingecko_mapping_coverage_below_floor"),
        ("adapter_float", "mapping_source_unavailable"),
        ("adapter_bool", "mapping_source_unavailable"),
        ("page_float", "mapping_source_evidence_invalid"),
        ("page_bool", "mapping_source_evidence_invalid"),
    ],
)
def test_load_revalidates_ranked_inventory_and_mapping_coverage(
    monkeypatch,
    mutation,
    reason,
):
    payload = _make_valid_cache(monkeypatch, mapped_count=100)
    if mutation == "page_count":
        payload["mapping_source"]["market_pages"][0]["row_count"] = 249
    elif mutation == "unique_count":
        payload["mapping_source"]["market_unique_id_count"] = 999
    elif mutation == "coverage":
        payload["universe"] = dict(list(payload["universe"].items())[:33])
        payload["mapped_count"] = 33
    elif mutation == "adapter_float":
        payload["mapping_source"]["adapter_version"] = 2.0
    elif mutation == "adapter_bool":
        payload["mapping_source"]["adapter_version"] = True
    elif mutation == "page_float":
        payload["mapping_source"]["market_pages"][0]["page"] = 1.0
    else:
        payload["mapping_source"]["market_pages"][0]["page"] = True
    _write_cache(payload)

    assert pu.load_result()["reason_codes"] == [reason]
    assert pu.load() == {}


@pytest.mark.parametrize(
    ("case", "reason"),
    [
        ("code", "okx_instruments_schema_invalid"),
        ("msg", "okx_instruments_schema_invalid"),
        ("empty", "okx_instruments_schema_invalid"),
        ("duplicate", "okx_instrument_duplicate"),
        ("inverse", "okx_instrument_unit_conflict"),
        ("settle", "okx_instrument_unit_conflict"),
        ("unit", "okx_instrument_unit_conflict"),
        ("zero", "okx_instrument_contract_size_invalid"),
        ("nonfinite", "okx_instrument_contract_size_invalid"),
        ("malformed_id", "okx_instrument_identity_invalid"),
    ],
)
def test_native_okx_schema_and_unit_contract_is_strict(case, reason):
    payload = {"code": "0", "msg": "", "data": _market_rows()}
    if case == "code":
        payload["code"] = "1"
    elif case == "msg":
        payload["msg"] = "partial"
    elif case == "empty":
        payload["data"] = []
    elif case == "duplicate":
        payload["data"].append(copy.deepcopy(payload["data"][0]))
    elif case == "inverse":
        payload["data"][0]["ctType"] = "inverse"
    elif case == "settle":
        payload["data"][0]["settleCcy"] = "USD"
    elif case == "unit":
        payload["data"][0]["ctValCcy"] = "ETH"
    elif case == "zero":
        payload["data"][0]["ctVal"] = "0"
    elif case == "nonfinite":
        payload["data"][0]["ctVal"] = "NaN"
    else:
        payload["data"][0]["instId"] = "BAD-BASE-USDT-SWAP"

    with pytest.raises(pu._ContractError) as caught:
        pu._validate_okx_instruments(_evidence(payload))
    assert caught.value.reason_code == reason


def test_native_accepts_only_live_linear_standard_usdt_rows():
    rows = _market_rows()
    rows.extend([
        {
            "instId": "ETH-USD-SWAP", "instType": "SWAP", "state": "live",
            "ctType": "inverse", "settleCcy": "ETH", "ctValCcy": "USD",
            "ctVal": "100",
        },
        {
            "instId": "SOL-USDT-SWAP", "instType": "SWAP", "state": "suspend",
            "ctType": "linear", "settleCcy": "USDT", "ctValCcy": "SOL",
            "ctVal": "1",
        },
    ])
    catalog = pu._validate_okx_instruments(_evidence({
        "code": "0", "msg": "", "data": rows,
    }))
    assert len(catalog) == 300
    assert "ETH-USD-SWAP" not in catalog
    assert "SOL-USDT-SWAP" not in catalog


def test_cache_load_enforces_the_same_300_market_floor(monkeypatch):
    payload = _make_valid_cache(monkeypatch)
    assert pu.load_result()["market_count"] == 300

    payload["universe"].pop(next(reversed(payload["universe"])))
    payload["market_count"] = 299
    payload["mapped_count"] = 299
    _write_cache(payload)

    assert pu.load_result()["reason_codes"] == [
        "okx_inventory_below_operational_floor",
    ]
    assert pu.load() == {}


def test_legacy_plain_cache_is_untrusted_and_not_actionable():
    pu._CACHE.write_text(
        json.dumps({"BTC": {"chain": "ethereum", "address": "0xabc"}}),
        encoding="utf-8",
    )
    assert pu.load_result()["reason_codes"] == ["legacy_cache_untrusted"]
    assert pu.load() == {}


def test_digest_stale_and_future_caches_all_fail_closed(monkeypatch):
    valid = _make_valid_cache(monkeypatch)

    corrupted = copy.deepcopy(valid)
    corrupted["universe"]["BTC"]["address"] = "0xdef"
    _write_cache(corrupted, resign=False)
    assert pu.load_result()["reason_codes"] == ["cache_digest_mismatch"]
    assert pu.load() == {}

    _write_cache(copy.deepcopy(valid))
    stale_at = NOW + timedelta(seconds=pu.CACHE_TTL_SECONDS)
    assert pu.load_result(_now=stale_at)["reason_codes"] == ["cache_stale"]

    before_generated = NOW - timedelta(microseconds=1)
    assert pu.load_result(_now=before_generated)["reason_codes"] == [
        "cache_generated_in_future"
    ]


@pytest.mark.parametrize(
    ("case", "reason"),
    [
        ("source_unavailable", "source_unavailable"),
        ("inventory_flag", "inventory_conflict"),
        ("inventory_count", "inventory_conflict"),
        ("raw_body", "cache_schema_invalid"),
        ("generated_z", "cache_generated_at_invalid"),
        ("naive_local", "source_time_invalid"),
    ],
)
def test_cached_source_conflict_raw_data_and_timezone_fail_closed(
    monkeypatch,
    case,
    reason,
):
    payload = _make_valid_cache(monkeypatch)
    if case == "source_unavailable":
        payload["source_health"]["state"] = "unavailable"
    elif case == "inventory_flag":
        payload["source_health"]["inventory_conflict"] = True
    elif case == "inventory_count":
        payload["source_health"]["sources"][0]["market_count"] -= 1
    elif case == "raw_body":
        payload["body"] = "raw response must never be accepted"
    elif case == "generated_z":
        payload["generated_at"] = payload["generated_at"].replace("+00:00", "Z")
    else:
        payload["source_health"]["sources"][0]["local_started_at"] = (
            payload["source_health"]["sources"][0]["local_started_at"]
            .replace("+00:00", "")
        )
    _write_cache(payload)

    assert pu.load_result()["reason_codes"] == [reason]
    assert pu.load() == {}


def test_same_projection_is_unchanged_without_extending_bytes_mtime_or_ttl(
    monkeypatch,
):
    incumbent = _make_valid_cache(
        monkeypatch, age_source_vector=False,
    )
    before_bytes = pu._CACHE.read_bytes()
    before_stat = pu._CACHE.stat()
    before_expiry = incumbent["expires_at"]
    later = NOW + timedelta(seconds=1)
    candidate = _retime_payload(
        copy.deepcopy(incumbent),
        source_observed_at=NOW,
        mapping_observed_at=NOW,
        generated_at=later,
    )
    monkeypatch.setattr(pu, "_utc_now", lambda: later)

    result = pu._publish_candidate(candidate)

    after_stat = pu._CACHE.stat()
    assert result["status"] == "research_only"
    assert result["refresh_status"] == "unchanged"
    assert result["cache_preserved"] is True
    assert result["expires_at"] == before_expiry
    assert pu._CACHE.read_bytes() == before_bytes
    assert after_stat.st_mtime_ns == before_stat.st_mtime_ns
    assert _read_cache()["expires_at"] == before_expiry


def test_refresh_surfaces_same_evidence_as_unchanged_not_written(monkeypatch):
    _make_valid_cache(monkeypatch, age_source_vector=False)
    before = pu._CACHE.read_bytes()
    _install_sources(monkeypatch)

    result = pu.refresh_result()

    assert result["status"] == "research_only"
    assert result["refresh_status"] == "unchanged"
    assert result["cache_preserved"] is True
    assert pu._CACHE.read_bytes() == before


def test_same_vector_different_projection_is_bounded_conflict(monkeypatch):
    _make_valid_cache(monkeypatch, age_source_vector=False)
    before = pu._CACHE.read_bytes()
    _install_sources(monkeypatch, address="0xDef")

    result = pu.refresh_result()

    assert result["status"] == "invalid"
    assert result["reason_codes"] == ["cache_publish_evidence_conflict"]
    assert result["cache_preserved"] is True
    assert "refresh_status" not in result
    assert pu._CACHE.read_bytes() == before


def test_equal_native_component_requires_equal_projection(monkeypatch):
    incumbent = _make_valid_cache(
        monkeypatch, age_source_vector=False,
    )
    later = NOW + timedelta(seconds=1)
    candidate = _retime_payload(
        copy.deepcopy(incumbent),
        source_observed_at=NOW,
        mapping_observed_at=later,
        generated_at=later,
    )
    candidate["market_catalog_digest"] = "f" * 64
    candidate["cache_digest"] = pu._cache_digest(candidate)
    pu._validate_cache_payload(candidate, later)

    with pytest.raises(pu._CachePublishRejected) as caught:
        pu._publication_decision(candidate, incumbent)

    assert caught.value.reason_code == "cache_publish_evidence_conflict"


def test_source_vector_incomparable_and_clock_regression_are_rejected(monkeypatch):
    incumbent = _make_valid_cache(
        monkeypatch, age_source_vector=False,
    )
    _retime_payload(
        incumbent,
        source_observed_at=NOW - timedelta(seconds=4),
        mapping_observed_at=NOW - timedelta(seconds=2),
    )

    incomparable = _retime_payload(
        copy.deepcopy(incumbent),
        source_observed_at=NOW - timedelta(seconds=3),
        mapping_observed_at=NOW - timedelta(seconds=3),
    )
    pu._validate_cache_payload(incomparable, NOW)
    with pytest.raises(pu._CachePublishRejected) as caught:
        pu._publication_decision(incomparable, incumbent)
    assert caught.value.reason_code == "cache_publish_source_incomparable"

    clock_regressed = _retime_payload(
        copy.deepcopy(incumbent),
        source_observed_at=NOW - timedelta(seconds=2),
        mapping_observed_at=NOW - timedelta(seconds=1),
        generated_at=NOW - timedelta(seconds=1),
    )
    pu._validate_cache_payload(clock_regressed, NOW)
    with pytest.raises(pu._CachePublishRejected) as caught:
        pu._publication_decision(clock_regressed, incumbent)
    assert caught.value.reason_code == "cache_publish_clock_regression"


def test_latest_locked_baseline_rejects_candidate_that_passed_older_baseline(
    monkeypatch,
):
    incumbent = _make_valid_cache(
        monkeypatch,
        rows=_market_rows(500),
        mapped_count=500,
        age_source_vector=False,
    )
    before = pu._CACHE.read_bytes()
    candidate = copy.deepcopy(incumbent)
    candidate["universe"] = dict(list(candidate["universe"].items())[:374])
    candidate["market_count"] = 374
    candidate["mapped_count"] = 374
    candidate["market_catalog_digest"] = "e" * 64
    for source in candidate["source_health"]["sources"]:
        source["market_count"] = 374
    candidate["cache_digest"] = pu._cache_digest(candidate)
    pu._validate_cache_payload(candidate, NOW)

    with pytest.raises(pu._ContractError) as caught:
        pu._publish_candidate(candidate)

    assert caught.value.reason_code == "okx_inventory_operational_drop"
    assert pu._CACHE.read_bytes() == before


def test_existing_invalid_incumbent_fails_closed_without_replacement(monkeypatch):
    pu._CACHE.write_bytes(b'{"schema_version":2,"token":"private"}')
    before = pu._CACHE.read_bytes()
    _install_sources(monkeypatch)
    writes = []
    monkeypatch.setattr(
        pu,
        "_atomic_write_cache",
        lambda *_args: writes.append("write") or pytest.fail("invalid overwrite"),
    )

    result = pu.refresh_result()

    assert result["status"] == "invalid"
    assert result["reason_codes"] == ["cache_incumbent_invalid"]
    assert result["cache_preserved"] is True
    assert writes == []
    assert pu._CACHE.read_bytes() == before
    assert "private" not in repr(result)


def test_contract_enforces_latest_exchange_path_before_mapping(monkeypatch):
    _install_sources(monkeypatch)
    monkeypatch.setenv(pu.CCXT_MODE_ENV, "shadow")
    monkeypatch.setattr(pu, "_ccxt_shadow_snapshot", _shadow_snapshot)
    assert pu.refresh_result()["refresh_status"] == "written"
    payload = _read_cache()
    native = next(
        source for source in payload["source_health"]["sources"]
        if source["path_key"] == pu._NATIVE_PATH_KEY
    )
    shift = timedelta(seconds=-2)
    for field in (
        "local_started_at", "local_completed_at",
        "exchange_time_at", "observed_at",
    ):
        native[field] = (
            datetime.fromisoformat(native[field]) + shift
        ).isoformat()
    payload["mapping_source"]["observed_at"] = (
        NOW - timedelta(seconds=1)
    ).isoformat()
    _write_cache(payload)

    assert pu.load_result()["reason_codes"] == [
        "mapping_source_precedes_market_observation",
    ]


def test_slow_older_thread_cannot_overwrite_newer_publication(monkeypatch):
    incumbent = _make_valid_cache(
        monkeypatch, age_source_vector=False,
    )
    _retime_payload(
        incumbent,
        source_observed_at=NOW - timedelta(seconds=6),
        mapping_observed_at=NOW - timedelta(seconds=5),
    )
    _write_cache(incumbent)
    old = _retime_payload(
        copy.deepcopy(incumbent),
        source_observed_at=NOW - timedelta(seconds=4),
        mapping_observed_at=NOW - timedelta(seconds=3),
    )
    old["universe"]["BTC"]["address"] = "0xold"
    old["cache_digest"] = pu._cache_digest(old)
    new = _retime_payload(
        copy.deepcopy(incumbent),
        source_observed_at=NOW - timedelta(seconds=2),
        mapping_observed_at=NOW - timedelta(seconds=1),
    )
    new["universe"]["BTC"]["address"] = "0xnew"
    new["cache_digest"] = pu._cache_digest(new)
    old_ready = threading.Event()
    old_start = threading.Event()
    outcomes = {}

    def publish(name, payload, ready, start):
        ready.set()
        assert start.wait(5)
        try:
            outcomes[name] = ("ok", pu._publish_candidate(payload))
        except pu._ContractError as exc:
            outcomes[name] = ("rejected", exc.reason_code)

    old_thread = threading.Thread(
        target=publish, args=("old", old, old_ready, old_start),
    )
    new_start = threading.Event()
    new_start.set()
    new_thread = threading.Thread(
        target=publish,
        args=("new", new, threading.Event(), new_start),
    )
    old_thread.start()
    assert old_ready.wait(5)
    new_thread.start()
    new_thread.join(5)
    assert not new_thread.is_alive()
    old_start.set()
    old_thread.join(5)
    assert not old_thread.is_alive()

    assert outcomes["new"][0] == "ok"
    assert outcomes["new"][1]["refresh_status"] == "written"
    assert outcomes["old"] == (
        "rejected", "cache_publish_source_regression",
    )
    assert _read_cache()["universe"]["BTC"]["address"] == "0xnew"


def test_slow_older_spawn_process_cannot_overwrite_newer_publication(monkeypatch):
    incumbent = _make_valid_cache(
        monkeypatch, age_source_vector=False,
    )
    _retime_payload(
        incumbent,
        source_observed_at=NOW - timedelta(seconds=6),
        mapping_observed_at=NOW - timedelta(seconds=5),
    )
    _write_cache(incumbent)
    old = _retime_payload(
        copy.deepcopy(incumbent),
        source_observed_at=NOW - timedelta(seconds=4),
        mapping_observed_at=NOW - timedelta(seconds=3),
    )
    old["universe"]["BTC"]["address"] = "0xold"
    old["cache_digest"] = pu._cache_digest(old)
    new = _retime_payload(
        copy.deepcopy(incumbent),
        source_observed_at=NOW - timedelta(seconds=2),
        mapping_observed_at=NOW - timedelta(seconds=1),
    )
    new["universe"]["BTC"]["address"] = "0xnew"
    new["cache_digest"] = pu._cache_digest(new)
    context = multiprocessing.get_context("spawn")
    results = context.Queue()
    old_ready = context.Event()
    old_start = context.Event()
    new_ready = context.Event()
    new_start = context.Event()
    old_process = context.Process(
        target=_spawn_publish_candidate,
        args=(
            str(pu._CACHE), old, NOW.isoformat(),
            old_ready, old_start, results,
        ),
    )
    new_process = context.Process(
        target=_spawn_publish_candidate,
        args=(
            str(pu._CACHE), new, NOW.isoformat(),
            new_ready, new_start, results,
        ),
    )
    processes = [old_process, new_process]
    try:
        old_process.start()
        assert old_ready.wait(10)
        new_process.start()
        assert new_ready.wait(10)
        new_start.set()
        new_result = results.get(timeout=10)
        new_process.join(10)
        assert new_process.exitcode == 0
        old_start.set()
        old_result = results.get(timeout=10)
        old_process.join(10)
        assert old_process.exitcode == 0
    finally:
        for process in processes:
            if process.is_alive():
                process.terminate()
            process.join(5)

    assert new_result == ("ok", "written")
    assert old_result == (
        "rejected", "cache_publish_source_regression",
    )
    assert _read_cache()["universe"]["BTC"]["address"] == "0xnew"


def test_atomic_write_orders_file_fsync_replace_and_directory_fsync(
    monkeypatch,
):
    _install_sources(monkeypatch)
    real_atomic_write = pu._atomic_write_cache
    real_replace = pu.os.replace
    replaced: list[tuple[str, object]] = []
    real_fsync = pu.os.fsync
    real_open = pu.os.open
    directory_fds: set[int] = set()
    events: list[str] = []
    states: list[pu._CachePublishState] = []

    def atomic_write(envelope):
        state = real_atomic_write(envelope)
        states.append(state)
        return state

    def replace(source, destination):
        events.append("replace")
        replaced.append((source, destination))
        return real_replace(source, destination)

    def open_file(path, flags, *args):
        descriptor = real_open(path, flags, *args)
        if str(path) == str(pu._CACHE.parent) and flags == pu.os.O_RDONLY:
            events.append("directory_open")
            directory_fds.add(descriptor)
        return descriptor

    def fsync(descriptor):
        events.append(
            "directory_fsync"
            if descriptor in directory_fds
            else "file_fsync"
        )
        return real_fsync(descriptor)

    monkeypatch.setattr(pu, "_atomic_write_cache", atomic_write)
    monkeypatch.setattr(pu.os, "replace", replace)
    monkeypatch.setattr(pu.os, "open", open_file)
    monkeypatch.setattr(pu.os, "fsync", fsync)

    result = pu.refresh_result()

    assert result["status"] == "research_only"
    assert result["refresh_status"] == "written"
    assert states == [pu._CachePublishState(
        namespace_replaced=True,
        directory_synced=True,
    )]
    assert events == [
        "file_fsync", "replace", "directory_open", "directory_fsync",
    ]
    source, destination = replaced[0]
    assert pu.os.path.dirname(source) == str(pu._CACHE.parent)
    assert pu.os.path.basename(source).startswith(f".{pu._CACHE.name}.")
    assert source != str(pu._CACHE) + ".tmp"
    assert destination == pu._CACHE
    assert list(pu._CACHE.parent.glob("*.tmp")) == []


def test_lockfile_is_stable_owned_and_tightened_without_unlink(monkeypatch):
    lock_path = pu._cache_publish_lock_path()
    lock_path.write_bytes(b"")
    lock_path.chmod(0o666)
    before_inode = lock_path.stat().st_ino
    unlinks = []
    real_unlink = pu.os.unlink

    def unlink(path):
        unlinks.append(str(path))
        return real_unlink(path)

    monkeypatch.setattr(pu.os, "unlink", unlink)
    with pu._cache_publish_lock():
        assert lock_path.stat().st_ino == before_inode
        assert lock_path.stat().st_mode & 0o777 == 0o600
    with pu._cache_publish_lock():
        assert lock_path.stat().st_ino == before_inode

    assert lock_path.exists()
    assert str(lock_path) not in unlinks


@pytest.mark.parametrize(
    ("failure", "reason"),
    [
        ("error", "cache_publish_lock_failed"),
        ("timeout", "cache_publish_lock_timeout"),
    ],
)
def test_lock_acquisition_failure_is_bounded_and_closes_descriptor(
    monkeypatch,
    failure,
    reason,
):
    real_open = pu.os.open
    descriptors = []
    secret = "https://secret.invalid/lock TOKEN"

    def open_file(path, flags, *args):
        descriptor = real_open(path, flags, *args)
        if str(path) == str(pu._cache_publish_lock_path()):
            descriptors.append(descriptor)
        return descriptor

    def flock(_descriptor, operation):
        if operation & pu.fcntl.LOCK_EX:
            if failure == "timeout":
                raise BlockingIOError(errno.EAGAIN, secret)
            raise OSError(errno.EIO, secret)
        raise AssertionError("unlock after failed acquisition")

    monkeypatch.setattr(pu.os, "open", open_file)
    monkeypatch.setattr(pu.fcntl, "flock", flock)
    if failure == "timeout":
        monkeypatch.setattr(pu, "_CACHE_PUBLISH_LOCK_TIMEOUT_SECONDS", 0)

    with pytest.raises(pu._CachePublishLockError) as caught:
        with pu._cache_publish_lock():
            pytest.fail("lock unexpectedly acquired")

    assert caught.value.reason_code == reason
    assert len(descriptors) == 1
    with pytest.raises(OSError) as descriptor_error:
        pu.os.fstat(descriptors[0])
    assert descriptor_error.value.errno == errno.EBADF
    assert pu._cache_publish_lock_path().exists()
    assert "secret.invalid" not in repr(caught.value)
    assert pu._CACHE_PUBLISH_THREAD_LOCK.acquire(blocking=False)
    pu._CACHE_PUBLISH_THREAD_LOCK.release()


def test_keyboard_interrupt_during_flock_poll_releases_fd_and_thread_lock(
    monkeypatch,
):
    real_open = pu.os.open
    descriptors = []

    def open_file(path, flags, *args):
        descriptor = real_open(path, flags, *args)
        if str(path) == str(pu._cache_publish_lock_path()):
            descriptors.append(descriptor)
        return descriptor

    monkeypatch.setattr(pu.os, "open", open_file)
    monkeypatch.setattr(
        pu.fcntl,
        "flock",
        lambda *_args: (_ for _ in ()).throw(
            BlockingIOError(errno.EAGAIN, "busy"),
        ),
    )
    monkeypatch.setattr(
        pu.time,
        "sleep",
        lambda *_args: (_ for _ in ()).throw(KeyboardInterrupt()),
    )

    with pytest.raises(KeyboardInterrupt):
        with pu._cache_publish_lock():
            pytest.fail("lock unexpectedly acquired")

    assert len(descriptors) == 1
    with pytest.raises(OSError) as descriptor_error:
        pu.os.fstat(descriptors[0])
    assert descriptor_error.value.errno == errno.EBADF
    assert pu._CACHE_PUBLISH_THREAD_LOCK.acquire(blocking=False)
    pu._CACHE_PUBLISH_THREAD_LOCK.release()


def test_refresh_lock_failure_returns_only_bounded_fields_and_never_writes(
    monkeypatch,
):
    _install_sources(monkeypatch)
    writes = []
    warnings = []
    secret = "https://secret.invalid/flock?token=private"

    class Logs:
        def warning(self, event, **fields):
            warnings.append((event, fields))

    def flock(_descriptor, operation):
        if operation & pu.fcntl.LOCK_EX:
            raise OSError(errno.EIO, secret)
        raise AssertionError("unexpected unlock")

    monkeypatch.setattr(pu, "logger", Logs())
    monkeypatch.setattr(pu.fcntl, "flock", flock)
    monkeypatch.setattr(
        pu,
        "_atomic_write_cache",
        lambda *_args: writes.append("write") or pytest.fail("write without lock"),
    )

    result = pu.refresh_result()

    assert result["status"] == "unavailable"
    assert result["reason_codes"] == ["cache_publish_lock_failed"]
    assert "refresh_status" not in result
    assert writes == []
    assert warnings == [(
        "perp_universe_cache_publish_lock_failed",
        {
            "reason_code": "cache_publish_lock_failed",
            "error_kind": "_CachePublishLockError",
        },
    )]
    assert "secret.invalid" not in repr(result)
    assert "secret.invalid" not in repr(warnings)


def test_foreign_lockfile_owner_fails_closed_and_closes_descriptor(monkeypatch):
    real_open = pu.os.open
    real_fstat = pu.os.fstat
    lock_descriptors = []

    def open_file(path, flags, *args):
        descriptor = real_open(path, flags, *args)
        if str(path) == str(pu._cache_publish_lock_path()):
            lock_descriptors.append(descriptor)
        return descriptor

    class ForeignMetadata:
        def __init__(self, metadata):
            self.st_mode = metadata.st_mode
            self.st_nlink = metadata.st_nlink
            self.st_uid = metadata.st_uid + 1

    def fstat(descriptor):
        metadata = real_fstat(descriptor)
        return (
            ForeignMetadata(metadata)
            if descriptor in lock_descriptors else metadata
        )

    monkeypatch.setattr(pu.os, "open", open_file)
    monkeypatch.setattr(pu.os, "fstat", fstat)

    with pytest.raises(pu._CachePublishLockError) as caught:
        with pu._cache_publish_lock():
            pytest.fail("foreign lockfile was trusted")

    assert caught.value.reason_code == "cache_publish_lock_failed"
    assert len(lock_descriptors) == 1
    with pytest.raises(OSError) as descriptor_error:
        real_fstat(lock_descriptors[0])
    assert descriptor_error.value.errno == errno.EBADF


def test_unlock_and_close_double_failure_does_not_leak_or_hold_thread_lock(
    monkeypatch,
):
    real_flock = pu.fcntl.flock
    real_close = pu.os.close
    lock_descriptors = []
    warnings = []

    class Logs:
        def warning(self, event, **fields):
            warnings.append((event, fields))

    real_open = pu.os.open

    def open_file(path, flags, *args):
        descriptor = real_open(path, flags, *args)
        if str(path) == str(pu._cache_publish_lock_path()):
            lock_descriptors.append(descriptor)
        return descriptor

    def flock(descriptor, operation):
        result = real_flock(descriptor, operation)
        if operation == pu.fcntl.LOCK_UN:
            raise OSError("https://secret.invalid/unlock TOKEN")
        return result

    def close(descriptor):
        result = real_close(descriptor)
        if descriptor in lock_descriptors:
            raise OSError("https://secret.invalid/close TOKEN")
        return result

    monkeypatch.setattr(pu, "logger", Logs())
    monkeypatch.setattr(pu.os, "open", open_file)
    monkeypatch.setattr(pu.fcntl, "flock", flock)
    monkeypatch.setattr(pu.os, "close", close)

    with pu._cache_publish_lock():
        pass

    assert len(lock_descriptors) == 1
    with pytest.raises(OSError) as descriptor_error:
        pu.os.fstat(lock_descriptors[0])
    assert descriptor_error.value.errno == errno.EBADF
    assert pu._CACHE_PUBLISH_THREAD_LOCK.acquire(blocking=False)
    pu._CACHE_PUBLISH_THREAD_LOCK.release()
    assert [fields["reason_code"] for _event, fields in warnings] == [
        "cache_publish_unlock_failed",
        "cache_publish_lock_close_failed",
    ]
    assert "secret.invalid" not in repr(warnings)


def test_spawn_process_lock_contention_times_out_without_hanging(monkeypatch):
    context = multiprocessing.get_context("spawn")
    entered = context.Event()
    release = context.Event()
    results = context.Queue()
    holder = context.Process(
        target=_spawn_hold_publish_lock,
        args=(str(pu._CACHE), entered, release, results),
    )
    try:
        holder.start()
        assert entered.wait(10)
        monkeypatch.setattr(pu, "_CACHE_PUBLISH_LOCK_TIMEOUT_SECONDS", 0)
        with pytest.raises(pu._CachePublishLockError) as caught:
            with pu._cache_publish_lock():
                pytest.fail("cross-process lock was not exclusive")
        assert caught.value.reason_code == "cache_publish_lock_timeout"
        release.set()
        assert results.get(timeout=10) == ("ok", None)
        holder.join(10)
        assert holder.exitcode == 0
    finally:
        release.set()
        if holder.is_alive():
            holder.terminate()
        holder.join(5)


def test_post_write_readback_mismatch_is_never_acknowledged(monkeypatch):
    _install_sources(monkeypatch)
    real_read = pu._read_cache_bytes
    reads = 0

    def read_cache():
        nonlocal reads
        reads += 1
        if reads == 1:
            return real_read()
        return b'{}\n'

    monkeypatch.setattr(pu, "_read_cache_bytes", read_cache)

    result = pu.refresh_result()

    assert reads == 2
    assert result["status"] == "unavailable"
    assert result["reason_codes"] == [
        "cache_readback_mismatch_after_replace",
    ]
    assert result["cache_preserved"] is False
    assert "refresh_status" not in result
    assert pu._CACHE.exists()


def test_keyboard_interrupt_before_fdopen_transfer_closes_fd_and_temp(monkeypatch):
    real_mkstemp = pu.tempfile.mkstemp
    descriptors = []
    temporary_paths = []

    def mkstemp(*args, **kwargs):
        descriptor, path = real_mkstemp(*args, **kwargs)
        descriptors.append(descriptor)
        temporary_paths.append(path)
        return descriptor, path

    monkeypatch.setattr(pu.tempfile, "mkstemp", mkstemp)
    monkeypatch.setattr(
        pu.os,
        "fdopen",
        lambda *_args: (_ for _ in ()).throw(KeyboardInterrupt()),
    )

    with pytest.raises(KeyboardInterrupt):
        pu._atomic_write_cache({})

    assert len(descriptors) == len(temporary_paths) == 1
    with pytest.raises(OSError) as descriptor_error:
        pu.os.fstat(descriptors[0])
    assert descriptor_error.value.errno == errno.EBADF
    assert not pu.os.path.exists(temporary_paths[0])
    assert list(pu._CACHE.parent.glob("*.tmp")) == []


def test_keyboard_interrupt_during_directory_fsync_closes_directory_fd(
    monkeypatch,
):
    real_open = pu.os.open
    real_fsync = pu.os.fsync
    directory_descriptors = []

    def open_file(path, flags, *args):
        descriptor = real_open(path, flags, *args)
        if str(path) == str(pu._CACHE.parent) and flags == pu.os.O_RDONLY:
            directory_descriptors.append(descriptor)
        return descriptor

    def fsync(descriptor):
        if descriptor in directory_descriptors:
            raise KeyboardInterrupt
        return real_fsync(descriptor)

    monkeypatch.setattr(pu.os, "open", open_file)
    monkeypatch.setattr(pu.os, "fsync", fsync)

    with pytest.raises(KeyboardInterrupt):
        pu._atomic_write_cache({})

    assert len(directory_descriptors) == 1
    with pytest.raises(OSError) as descriptor_error:
        pu.os.fstat(directory_descriptors[0])
    assert descriptor_error.value.errno == errno.EBADF
    assert list(pu._CACHE.parent.glob("*.tmp")) == []


@pytest.mark.parametrize("previous_cache", [False, True])
@pytest.mark.parametrize("cleanup_close_raises", [False, True])
def test_fdopen_failure_closes_raw_descriptor_and_preserves_cache_state(
    monkeypatch,
    previous_cache,
    cleanup_close_raises,
):
    before = None
    if previous_cache:
        _make_valid_cache(monkeypatch)
        before = pu._CACHE.read_bytes()
    _install_sources(monkeypatch, address="0xDef")
    real_mkstemp = pu.tempfile.mkstemp
    real_close = pu.os.close
    descriptors: list[int] = []
    temporary_paths: list[str] = []
    close_calls: list[int] = []
    secret = "https://secret.invalid/fdopen TOKEN"
    close_secret = "https://secret.invalid/raw-close TOKEN"

    def mkstemp(*args, **kwargs):
        descriptor, path = real_mkstemp(*args, **kwargs)
        descriptors.append(descriptor)
        temporary_paths.append(path)
        return descriptor, path

    def fdopen(_descriptor, _mode):
        raise OSError(secret)

    def close(descriptor):
        close_calls.append(descriptor)
        result = real_close(descriptor)
        if cleanup_close_raises:
            raise OSError(close_secret)
        return result

    monkeypatch.setattr(pu.tempfile, "mkstemp", mkstemp)
    monkeypatch.setattr(pu.os, "fdopen", fdopen)
    monkeypatch.setattr(pu.os, "close", close)

    result = pu.refresh_result()

    assert len(descriptors) == len(temporary_paths) == 1
    assert all(descriptor in close_calls for descriptor in descriptors)
    with pytest.raises(OSError) as caught:
        pu.os.fstat(descriptors[0])
    assert caught.value.errno == errno.EBADF
    assert not pu.os.path.exists(temporary_paths[0])
    assert list(pu._CACHE.parent.glob("*.tmp")) == []
    assert result["status"] == "unavailable"
    assert result["reason_codes"] == ["cache_write_failed_before_replace"]
    assert result["cache_preserved"] is previous_cache
    assert "refresh_status" not in result
    assert "secret.invalid" not in repr(result)
    if previous_cache:
        assert pu._CACHE.read_bytes() == before
    else:
        assert not pu._CACHE.exists()


@pytest.mark.parametrize(
    "phase", ["file_write", "file_flush", "file_fsync", "replace"],
)
def test_pre_replace_failures_preserve_previous_cache_and_clean_temp(
    monkeypatch,
    phase,
):
    _make_valid_cache(monkeypatch)
    before = pu._CACHE.read_bytes()
    _install_sources(monkeypatch, address="0xDef")
    secret = "https://secret.invalid/cache-write TOKEN"
    real_fdopen = pu.os.fdopen
    real_fsync = pu.os.fsync
    real_replace = pu.os.replace
    fsync_calls = 0
    replace_calls: list[tuple[object, object]] = []

    if phase in {"file_write", "file_flush"}:
        class PhaseFailingFile:
            def __init__(self, descriptor, mode):
                self._handle = real_fdopen(descriptor, mode)

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                self._handle.close()

            def write(self, _encoded):
                if phase == "file_write":
                    raise OSError(secret)
                return self._handle.write(_encoded)

            def flush(self):
                if phase == "file_flush":
                    raise OSError(secret)
                return self._handle.flush()

            def fileno(self):
                return self._handle.fileno()

        monkeypatch.setattr(
            pu.os, "fdopen",
            lambda descriptor, mode: PhaseFailingFile(descriptor, mode),
        )

    def fsync(descriptor):
        nonlocal fsync_calls
        fsync_calls += 1
        if phase == "file_fsync" and fsync_calls == 1:
            raise OSError(secret)
        return real_fsync(descriptor)

    def replace(source, destination):
        replace_calls.append((source, destination))
        if phase == "replace":
            raise OSError(secret)
        return real_replace(source, destination)

    monkeypatch.setattr(pu.os, "fsync", fsync)
    monkeypatch.setattr(pu.os, "replace", replace)

    result = pu.refresh_result()

    assert result["status"] == "unavailable"
    assert result["reason_codes"] == ["cache_write_failed_before_replace"]
    assert result["cache_preserved"] is True
    assert "refresh_status" not in result
    assert pu._CACHE.read_bytes() == before
    assert len(replace_calls) == (1 if phase == "replace" else 0)
    assert list(pu._CACHE.parent.glob("*.tmp")) == []
    assert "secret.invalid" not in repr(result)


def test_pre_replace_failure_without_previous_cache_reports_not_preserved(
    monkeypatch,
):
    _install_sources(monkeypatch)
    real_fsync = pu.os.fsync
    fsync_calls = 0

    def fsync(descriptor):
        nonlocal fsync_calls
        fsync_calls += 1
        if fsync_calls == 1:
            raise OSError("file fsync")
        return real_fsync(descriptor)

    monkeypatch.setattr(pu.os, "fsync", fsync)

    result = pu.refresh_result()

    assert result["status"] == "unavailable"
    assert result["reason_codes"] == ["cache_write_failed_before_replace"]
    assert result["cache_preserved"] is False
    assert "refresh_status" not in result
    assert not pu._CACHE.exists()
    assert list(pu._CACHE.parent.glob("*.tmp")) == []


@pytest.mark.parametrize(
    "phase",
    ["directory_open", "directory_fsync", "directory_fsync_and_close"],
)
def test_post_replace_failures_report_visible_but_not_durable(
    monkeypatch,
    phase,
):
    _make_valid_cache(monkeypatch)
    before = pu._CACHE.read_bytes()
    _install_sources(monkeypatch, address="0xDef")
    real_open = pu.os.open
    real_fsync = pu.os.fsync
    real_close = pu.os.close
    real_replace = pu.os.replace
    directory_fds: set[int] = set()
    replace_calls: list[tuple[object, object]] = []
    directory_close_calls = 0
    secret = "https://secret.invalid/directory TOKEN"
    close_secret = "https://secret.invalid/directory-close TOKEN"

    def open_file(path, flags, *args):
        if str(path) == str(pu._CACHE.parent) and flags == pu.os.O_RDONLY:
            if phase == "directory_open":
                raise OSError(secret)
            descriptor = real_open(path, flags, *args)
            directory_fds.add(descriptor)
            return descriptor
        return real_open(path, flags, *args)

    def fsync(descriptor):
        if (
            phase in {"directory_fsync", "directory_fsync_and_close"}
            and descriptor in directory_fds
        ):
            raise OSError(secret)
        return real_fsync(descriptor)

    def close(descriptor):
        nonlocal directory_close_calls
        result = real_close(descriptor)
        if phase == "directory_fsync_and_close" and descriptor in directory_fds:
            directory_close_calls += 1
            raise OSError(close_secret)
        return result

    def replace(source, destination):
        result = real_replace(source, destination)
        replace_calls.append((source, destination))
        return result

    monkeypatch.setattr(pu.os, "open", open_file)
    monkeypatch.setattr(pu.os, "fsync", fsync)
    monkeypatch.setattr(pu.os, "close", close)
    monkeypatch.setattr(pu.os, "replace", replace)

    result = pu.refresh_result()

    assert len(replace_calls) == 1
    assert result["status"] == "unavailable"
    assert result["reason_codes"] == [
        "cache_durability_unknown_after_replace",
    ]
    assert result["cache_preserved"] is False
    assert "refresh_status" not in result
    assert pu._CACHE.read_bytes() != before
    assert _read_cache()["universe"]["BTC"]["address"] == "0xdef"
    assert list(pu._CACHE.parent.glob("*.tmp")) == []
    assert "secret.invalid" not in repr(result)
    assert directory_close_calls == (
        1 if phase == "directory_fsync_and_close" else 0
    )


def test_directory_close_failure_after_sync_remains_durable(monkeypatch):
    _install_sources(monkeypatch)
    real_open = pu.os.open
    real_close = pu.os.close
    directory_fds: set[int] = set()
    warnings = []

    class Logs:
        def warning(self, event, **fields):
            warnings.append((event, fields))

    def open_file(path, flags, *args):
        descriptor = real_open(path, flags, *args)
        if str(path) == str(pu._CACHE.parent) and flags == pu.os.O_RDONLY:
            directory_fds.add(descriptor)
        return descriptor

    def close(descriptor):
        result = real_close(descriptor)
        if descriptor in directory_fds:
            raise OSError("https://secret.invalid/directory-close TOKEN")
        return result

    monkeypatch.setattr(pu, "logger", Logs())
    monkeypatch.setattr(pu.os, "open", open_file)
    monkeypatch.setattr(pu.os, "close", close)

    result = pu.refresh_result()

    assert result["status"] == "research_only"
    assert result["refresh_status"] == "written"
    assert warnings == [(
        "perp_universe_cache_directory_close_failed",
        {
            "reason_code": "cache_directory_close_failed_after_sync",
            "error_kind": "OSError",
        },
    )]
    assert "secret.invalid" not in repr(warnings)


def test_unexpected_cache_validation_exception_is_contained(monkeypatch):
    _make_valid_cache(monkeypatch)
    monkeypatch.setattr(
        pu, "_validate_cache_payload",
        lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("private URL")),
    )
    result = pu.load_result()
    assert result["reason_codes"] == ["cache_validation_failed"]
    assert result["actionable_universe"] == {}
    assert pu.load() == {}


class _RedirectResponse:
    def __init__(self) -> None:
        self.closed = False
        self.read_called = False

    def read(self, _size: int | None = None) -> bytes:
        self.read_called = True
        return b""

    def close(self) -> None:
        self.closed = True


@pytest.mark.parametrize(
    "redirect",
    [
        "https://attacker.invalid/data?token=private",
        "http://www.okx.com/api/v5/public/time?token=private",
        "https://www.okx.com:444/api/v5/public/time?token=private",
    ],
)
def test_redirect_handler_blocks_cross_origin_before_second_connection(
    redirect,
):
    initial_url = "https://www.okx.com/api/v5/public/time"
    response = _RedirectResponse()
    second_connections = []

    class Parent:
        def open(self, request, *, timeout):
            second_connections.append((request.full_url, timeout))
            raise AssertionError("cross-origin redirect opened a second connection")

    handler = pu._SameOriginRedirectHandler(
        pu._https_origin(initial_url, "request_target_invalid"),
    )
    handler.add_parent(Parent())
    request = urllib.request.Request(initial_url)
    request.timeout = 20

    with pytest.raises(pu._ContractError) as caught:
        handler.http_error_302(
            request, response, 302, "redirect", {"location": redirect},
        )

    assert caught.value.reason_code == "redirect_target_invalid"
    assert "attacker.invalid" not in repr(caught.value)
    assert "token=private" not in repr(caught.value)
    assert response.closed is True
    assert response.read_called is False
    assert second_connections == []


def test_redirect_handler_allows_relative_same_origin_and_closes_30x():
    initial_url = "https://www.okx.com/api/v5/public/time"
    response = _RedirectResponse()
    final_response = object()
    second_connections = []

    class Parent:
        def open(self, request, *, timeout):
            second_connections.append((request.full_url, timeout))
            return final_response

    handler = pu._SameOriginRedirectHandler(
        pu._https_origin(initial_url, "request_target_invalid"),
    )
    handler.add_parent(Parent())
    request = urllib.request.Request(initial_url)
    request.timeout = 20

    result = handler.http_error_302(
        request,
        response,
        302,
        "redirect",
        {"location": "../instruments?instType=SWAP"},
    )

    assert result is final_response
    assert response.closed is True
    assert response.read_called is True
    assert second_connections == [(
        "https://www.okx.com/api/v5/instruments?instType=SWAP",
        20,
    )]


@pytest.mark.parametrize("status", [301, 302, 303, 307, 308])
def test_same_origin_redirect_read_failure_closes_and_never_reconnects(status):
    initial_url = "https://www.okx.com/api/v5/public/time"
    reads = []
    second_connections = []

    class FailingResponse(_RedirectResponse):
        def read(self, size=None):
            reads.append(size)
            raise OSError("https://secret.invalid/redirect-body")

    class Parent:
        def open(self, request, *, timeout):
            second_connections.append((request.full_url, timeout))
            raise AssertionError("failed redirect drain opened a connection")

    response = FailingResponse()
    handler = pu._SameOriginRedirectHandler(
        pu._https_origin(initial_url, "request_target_invalid"),
    )
    handler.add_parent(Parent())
    request = urllib.request.Request(initial_url)
    request.timeout = 20
    method = getattr(handler, f"http_error_{status}")

    with pytest.raises(OSError):
        method(
            request,
            response,
            status,
            "redirect",
            {"location": "/api/v5/public/instruments"},
        )

    assert reads == [pu._MAX_REDIRECT_DRAIN_BYTES]
    assert response.closed is True
    assert second_connections == []


def test_fetch_preserves_final_url_validation_and_always_closes(monkeypatch):
    class Response:
        def __init__(self) -> None:
            self.closed = False
            self.read_called = False

        def geturl(self) -> str:
            return "https://attacker.invalid/data"

        def read(self, size: int) -> bytes:
            assert size == pu._MAX_RETAINED_HTTP_BYTES + 1
            self.read_called = True
            return b"{}"

        def close(self) -> None:
            self.closed = True

    response = Response()
    handlers = _install_http_open(
        monkeypatch, lambda _request, _timeout: response,
    )
    with pytest.raises(pu._ContractError) as caught:
        pu._fetch_json("https://www.okx.com/api/v5/public/time")
    assert caught.value.reason_code == "response_target_invalid"
    assert len(handlers) == 1
    assert isinstance(handlers[0], pu._SameOriginRedirectHandler)
    assert response.closed is True
    assert response.read_called is False


@pytest.mark.parametrize(
    "redirect",
    [
        "https://www.okx.com:444/api/v5/public/time",
        "https://user@www.okx.com/api/v5/public/time",
    ],
)
def test_fetch_rejects_same_host_cross_authority_redirect(monkeypatch, redirect):
    class Response:
        closed = False

        def geturl(self) -> str:
            return redirect

        def read(self, _size: int) -> bytes:
            raise AssertionError("redirected body must not be read")

        def close(self) -> None:
            self.closed = True

    response = Response()
    _install_http_open(monkeypatch, lambda _request, _timeout: response)
    with pytest.raises(pu._ContractError) as caught:
        pu._fetch_json("https://www.okx.com/api/v5/public/time")
    assert caught.value.reason_code == "response_target_invalid"
    assert response.closed is True


def test_http_and_cache_reads_are_bounded_before_size_rejection(monkeypatch):
    class Response:
        def __init__(self) -> None:
            self.requested = None
            self.closed = False

        def geturl(self) -> str:
            return "https://www.okx.com/api/v5/public/time"

        def read(self, size: int) -> bytes:
            self.requested = size
            return b"x" * size

        def close(self) -> None:
            self.closed = True

    response = Response()
    _install_http_open(monkeypatch, lambda _request, _timeout: response)
    with pytest.raises(pu._ContractError) as caught:
        pu._fetch_json("https://www.okx.com/api/v5/public/time")
    assert caught.value.reason_code == "response_size_invalid"
    assert response.requested == pu._MAX_RETAINED_HTTP_BYTES + 1
    assert response.closed is True

    pu._CACHE.write_bytes(b"x" * (pu._MAX_CACHE_BYTES + 1))
    result = pu.load_result()
    assert result["reason_codes"] == ["cache_size_invalid"]


def test_fetch_explicitly_closes_http_error(monkeypatch):
    error = urllib.error.HTTPError(
        "https://www.okx.com/hidden", 503, "unavailable", {}, io.BytesIO(b"x"),
    )
    closed: list[bool] = []
    monkeypatch.setattr(error, "close", lambda: closed.append(True))
    _install_http_open(
        monkeypatch,
        lambda _request, _timeout: (_ for _ in ()).throw(error),
    )
    with pytest.raises(pu._ContractError) as caught:
        pu._fetch_json("https://www.okx.com/api/v5/public/time")
    assert caught.value.reason_code == "http_status_error"
    assert closed == [True]
