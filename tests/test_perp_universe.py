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
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone

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


def _make_valid_cache(monkeypatch, **source_kwargs) -> dict:
    _install_sources(monkeypatch, **source_kwargs)
    result = pu.refresh_result()
    assert result["status"] == "research_only"
    return _read_cache()


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
    assert _isolated.calls == ["perp_universe", "perp_universe"]
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
    assert calls == [pu._OKX_TIME_URL, pu._OKX_INSTRUMENTS_URL]
    assert pu._CACHE.read_bytes() == before


def test_market_inventory_retention_exact_75_percent_passes(monkeypatch):
    _make_valid_cache(monkeypatch, rows=_market_rows(500))
    _install_sources(monkeypatch, rows=_market_rows(375))

    result = pu.refresh_result()

    assert result["status"] == "research_only"
    assert result["market_count"] == 375
    assert result["mapped_count"] == 375


def test_refresh_reads_one_baseline_for_both_retention_gates(monkeypatch):
    _install_sources(
        monkeypatch,
        rows=_market_rows(375),
        mapped_count=300,
    )
    baseline_reads = []
    monkeypatch.setattr(
        pu,
        "_previous_inventory_counts",
        lambda: baseline_reads.append("read") or (500, 400),
    )

    result = pu.refresh_result()

    assert result["status"] == "research_only"
    assert result["market_count"] == 375
    assert result["mapped_count"] == 300
    assert baseline_reads == ["read"]


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
    assert close_calls == descriptors
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
