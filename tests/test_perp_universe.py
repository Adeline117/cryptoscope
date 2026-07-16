"""Fail-closed contracts for the perpetual research universe.

Every collector call is mocked.  These tests intentionally prove exclusion and
cache preservation more heavily than the happy path: a heuristic token join is
useful for research display, but it is never an actionable scanner universe.
"""

from __future__ import annotations

import copy
import hashlib
import io
import json
import urllib.error
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

    monkeypatch.setattr(pu.urllib.request, "urlopen", no_network)
    return guard


def _evidence(payload: object) -> dict[str, object]:
    raw = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), allow_nan=False,
    ).encode()
    return {
        "payload": copy.deepcopy(payload),
        "sha256": hashlib.sha256(raw).hexdigest(),
        "bytes": len(raw),
    }


def _market_rows(count: int = 100, *, btc_size: str = "0.01") -> list[dict]:
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
) -> list[str]:
    rows = copy.deepcopy(rows if rows is not None else _market_rows())
    coin_list = [
        {"id": "bitcoin", "symbol": "btc", "platforms": {"ethereum": address}},
        {"id": "dummy-2", "symbol": "d2", "platforms": {}},
        {"id": "dummy-3", "symbol": "d3", "platforms": {}},
        {"id": "dummy-4", "symbol": "d4", "platforms": {}},
        {"id": "official-empty-symbol", "symbol": "", "platforms": {}},
        {"id": "official-spaced-symbol", "symbol": "yee\N{NO-BREAK SPACE}",
         "platforms": {}},
    ]
    market_pages = {
        1: [{"id": "bitcoin", "symbol": "btc"}],
        2: [{"id": "dummy-2", "symbol": "d2"}],
        3: [{"id": "dummy-3", "symbol": "d3"}],
        4: [{"id": "dummy-4", "symbol": "d4"}],
    }
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
        for page in range(1, 5):
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
    assert cached["mapping_source"]["list_row_count"] == 6
    assert cached["mapping_source"]["list_usable_row_count"] == 4
    assert cached["mapping_source"]["list_excluded_row_count"] == 2


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
    calls = _install_sources(monkeypatch, rows=_market_rows(99))

    result = pu.refresh_result()

    assert result["reason_codes"] == ["okx_inventory_below_operational_floor"]
    assert calls == [pu._OKX_TIME_URL, pu._OKX_INSTRUMENTS_URL]
    assert pu._CACHE.read_bytes() == before


def test_large_drop_from_last_valid_inventory_is_rejected(monkeypatch):
    _make_valid_cache(monkeypatch, rows=_market_rows(140))
    before = pu._CACHE.read_bytes()
    calls = _install_sources(monkeypatch, rows=_market_rows(100))

    result = pu.refresh_result()

    assert result["reason_codes"] == ["okx_inventory_operational_drop"]
    assert calls == [pu._OKX_TIME_URL, pu._OKX_INSTRUMENTS_URL]
    assert pu._CACHE.read_bytes() == before


def test_stale_valid_cache_still_protects_refresh_inventory_baseline(monkeypatch):
    _make_valid_cache(monkeypatch, rows=_market_rows(140))
    monkeypatch.setattr(
        pu, "_utc_now",
        lambda: NOW + timedelta(seconds=pu.CACHE_TTL_SECONDS + 1),
    )

    assert pu.load_result()["status"] == "stale"
    assert pu._previous_market_count() == 140
    with pytest.raises(pu._ContractError) as caught:
        pu._reject_operational_inventory_drop(100)
    assert caught.value.reason_code == "okx_inventory_operational_drop"


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
    assert len(catalog) == 100
    assert "ETH-USD-SWAP" not in catalog
    assert "SOL-USDT-SWAP" not in catalog


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


def test_atomic_write_uses_unique_same_directory_temp_fsync_and_replace(
    monkeypatch,
):
    _install_sources(monkeypatch)
    real_replace = pu.os.replace
    replaced: list[tuple[str, object]] = []
    real_fsync = pu.os.fsync
    fsync_calls: list[int] = []

    def replace(source, destination):
        replaced.append((source, destination))
        return real_replace(source, destination)

    def fsync(descriptor):
        fsync_calls.append(descriptor)
        return real_fsync(descriptor)

    monkeypatch.setattr(pu.os, "replace", replace)
    monkeypatch.setattr(pu.os, "fsync", fsync)

    assert pu.refresh_result()["status"] == "research_only"
    source, destination = replaced[0]
    assert pu.os.path.dirname(source) == str(pu._CACHE.parent)
    assert pu.os.path.basename(source).startswith(f".{pu._CACHE.name}.")
    assert source != str(pu._CACHE) + ".tmp"
    assert destination == pu._CACHE
    assert len(fsync_calls) >= 2
    assert list(pu._CACHE.parent.glob("*.tmp")) == []


def test_atomic_replace_failure_keeps_previous_cache_and_cleans_temp(monkeypatch):
    _make_valid_cache(monkeypatch)
    before = pu._CACHE.read_bytes()
    _install_sources(monkeypatch, address="0xDef")
    monkeypatch.setattr(
        pu.os, "replace", lambda *_a: (_ for _ in ()).throw(OSError("replace")),
    )

    result = pu.refresh_result()

    assert result["reason_codes"] == ["cache_write_failed"]
    assert pu._CACHE.read_bytes() == before
    assert list(pu._CACHE.parent.glob("*.tmp")) == []


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


def test_fetch_rejects_cross_host_redirect_and_always_closes(monkeypatch):
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
    monkeypatch.setattr(pu.urllib.request, "urlopen", lambda *_a, **_k: response)
    with pytest.raises(pu._ContractError) as caught:
        pu._fetch_json("https://www.okx.com/api/v5/public/time")
    assert caught.value.reason_code == "response_target_invalid"
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
    monkeypatch.setattr(pu.urllib.request, "urlopen", lambda *_a, **_k: response)
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
    monkeypatch.setattr(pu.urllib.request, "urlopen", lambda *_a, **_k: response)
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
    monkeypatch.setattr(
        pu.urllib.request, "urlopen",
        lambda *_a, **_k: (_ for _ in ()).throw(error),
    )
    with pytest.raises(pu._ContractError) as caught:
        pu._fetch_json("https://www.okx.com/api/v5/public/time")
    assert caught.value.reason_code == "http_status_error"
    assert closed == [True]
