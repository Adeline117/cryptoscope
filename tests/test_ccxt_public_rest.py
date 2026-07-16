"""Offline safety contract for the keyless CCXT public REST shadow path."""

from __future__ import annotations

import ast
import copy
import hashlib
import inspect
import json
from pathlib import Path

import pytest
import requests

import src.collectors.ccxt_public_rest as adapter


NOW_MS = 1_784_200_000_000


def _info(**changes):
    row = {
        "instId": "BTC-USDT-SWAP",
        "instType": "SWAP",
        "state": "live",
        "ctType": "linear",
        "settleCcy": "USDT",
        "ctVal": "0.01",
        "ctValCcy": "BTC",
    }
    row.update(changes)
    return row


def _market(info=None, **changes):
    info = copy.deepcopy(info or _info())
    row = {
        "id": info["instId"],
        "symbol": "BTC/USDT:USDT",
        "base": "BTC",
        "quote": "USDT",
        "settle": "USDT",
        "type": "swap",
        "active": True,
        "spot": False,
        "swap": True,
        "future": False,
        "contract": True,
        "linear": True,
        "inverse": False,
        "contractSize": 0.01,
        "info": info,
    }
    row.update(changes)
    return row


class _Session:
    def __init__(self, *, close_error=False):
        self.closed = 0
        self.close_error = close_error

    def close(self):
        self.closed += 1
        if self.close_error:
            raise RuntimeError("must not escape")


class _Exchange:
    id = "okx"

    def __init__(
        self,
        *,
        time_payload=None,
        time_value=NOW_MS,
        market_payload=None,
        markets=None,
        fail_time=False,
        fail_markets=False,
        session=None,
    ):
        self.time_payload = time_payload or {
            "code": "0", "data": [{"ts": str(NOW_MS)}], "msg": "",
        }
        info = _info()
        self.market_payload = market_payload or {
            "code": "0", "data": [info], "msg": "",
        }
        self.markets = markets or [_market(info)]
        self.time_value = time_value
        self.fail_time = fail_time
        self.fail_markets = fail_markets
        self.session = _Session() if session is None else session
        self.calls = []
        self.last_http_response = None
        self.last_json_response = None

    def fetch_time(self):
        self.calls.append("fetch_time")
        if self.fail_time:
            raise RuntimeError(
                "https://bad.invalid/?apiKey=TOP_SECRET should never escape",
            )
        self.last_json_response = self.time_payload
        self.last_http_response = json.dumps(
            self.time_payload, separators=(",", ":"),
        )
        return self.time_value

    def fetch_markets(self):
        self.calls.append("fetch_markets")
        if self.fail_markets:
            raise RuntimeError(
                "https://bad.invalid/?secret=TOP_SECRET should never escape",
            )
        self.last_json_response = self.market_payload
        self.last_http_response = json.dumps(
            self.market_payload, separators=(",", ":"),
        )
        return self.markets


def _clock(*values):
    values = values or (NOW_MS - 10, NOW_MS + 10, NOW_MS + 11, NOW_MS + 20)
    iterator = iter(values)
    return lambda: next(iterator)


def _shadow(monkeypatch):
    monkeypatch.setenv(adapter.MODE_ENV, "shadow")


def _fetch(monkeypatch, exchange=None, **kwargs):
    _shadow(monkeypatch)
    exchange = exchange or _Exchange()
    configs = []

    def factory(config):
        configs.append(copy.deepcopy(config))
        return exchange

    result = adapter.fetch_public_snapshot(
        _exchange_factory=factory,
        _now_ms=kwargs.pop("_now_ms", _clock()),
        **kwargs,
    )
    return result, exchange, configs


@pytest.fixture(autouse=True)
def _network_forbidden(monkeypatch):
    def reject(*_args, **_kwargs):
        raise AssertionError("CCXT public contract tests must stay offline")

    monkeypatch.setattr(requests.Session, "request", reject)


def test_default_off_performs_no_client_or_network_work(monkeypatch):
    monkeypatch.delenv(adapter.MODE_ENV, raising=False)

    def forbidden(_config):
        raise AssertionError("disabled path created a client")

    result = adapter.fetch_public_snapshot(_exchange_factory=forbidden)

    assert result["status"] == "disabled"
    assert result["mode"] == "off"
    assert result["source_counts"]["independent_source_count"] == 0
    assert result["source_counts"]["observed_path_count"] == 0


@pytest.mark.parametrize(
    ("kwargs", "reason"),
    [
        ({"exchange": "binance"}, "unsupported_exchange"),
        ({"market_type": "spot"}, "unsupported_market_type"),
    ],
)
def test_only_okx_swap_is_accepted(monkeypatch, kwargs, reason):
    _shadow(monkeypatch)
    result = adapter.fetch_public_snapshot(
        _exchange_factory=lambda _config: pytest.fail("client must not be created"),
        **kwargs,
    )
    assert result["status"] == "invalid"
    assert result["reason_codes"] == [reason]


def test_invalid_feature_mode_fails_before_client_creation(monkeypatch):
    monkeypatch.setenv(
        adapter.MODE_ENV,
        "https://bad.invalid/?secret=TOP_SECRET",
    )
    result = adapter.fetch_public_snapshot(
        _exchange_factory=lambda _config: pytest.fail("client must not be created"),
    )
    assert result["status"] == "invalid"
    assert result["mode"] == "invalid"
    assert result["reason_codes"] == ["invalid_feature_mode"]
    assert "bad.invalid" not in json.dumps(result)
    assert "TOP_SECRET" not in json.dumps(result)


def test_success_is_keyless_public_and_retained_evidence_is_bounded(monkeypatch):
    result, exchange, configs = _fetch(monkeypatch)

    assert result["status"] == "observed"
    assert result["reason_codes"] == []
    assert exchange.calls == ["fetch_time", "fetch_markets"]
    assert exchange.session.closed == 1
    assert configs == [{
        "enableRateLimit": True,
        "timeout": 8000,
        "enableLastHttpResponse": True,
        "enableLastJsonResponse": True,
        "options": {
            "defaultType": "swap",
            "fetchMarkets": {"types": ["swap"]},
        },
    }]
    config_text = json.dumps(configs).lower()
    assert all(word not in config_text for word in (
        "apikey", "api_key", "secret", "password", "passphrase",
    ))
    assert result["adapter"] == {
        "name": "ccxt",
        "version": "4.5.58",
        "exchange_class": "okx",
        "transport": "public_rest",
    }
    assert result["source_counts"]["independent_source_count"] == 1
    assert result["source_counts"]["observed_path_count"] == 1
    assert result["markets"] == [{
        "market_id": "BTC-USDT-SWAP",
        "symbol": "BTC/USDT:USDT",
        "base": "BTC",
        "quote": "USDT",
        "settle": "USDT",
        "active": True,
        "linear": True,
        "amount_unit": "contracts",
        "contract_size": "0.01",
        "contract_size_unit": "BTC",
    }]
    for raw in result["raw_responses"].values():
        assert raw["bytes"] == len(raw["body"].encode())
        assert raw["sha256"] == hashlib.sha256(raw["body"].encode()).hexdigest()
    serialized_result = json.dumps(result)
    assert "enableLastHttpResponse" not in serialized_result
    assert "enableLastJsonResponse" not in serialized_result


def test_real_ccxt_okx_constructor_enables_evidence_without_credentials():
    config = adapter._client_config(8000)
    exchange = adapter.ccxt.okx(config)
    try:
        assert exchange.enableLastHttpResponse is True
        assert exchange.enableLastJsonResponse is True
        assert exchange.options["defaultType"] == "swap"
        assert exchange.options["fetchMarkets"] == {"types": ["swap"]}
        assert {
            name: getattr(exchange, name, None)
            for name in (
                "apiKey", "secret", "password", "uid", "privateKey",
                "walletAddress",
            )
        } == {
            "apiKey": None,
            "secret": None,
            "password": None,
            "uid": None,
            "privateKey": None,
            "walletAddress": None,
        }
        # Constructor-only contract: no request, URL or response evidence exists.
        assert exchange.last_request_url is None
        assert exchange.last_http_response is None
        assert exchange.last_json_response is None
    finally:
        session = getattr(exchange, "session", None)
        if session is not None:
            session.close()


def test_raw_json_is_deep_copied_before_the_next_request(monkeypatch):
    exchange = _Exchange()
    original_time = exchange.time_payload
    original_markets = exchange.market_payload
    result, _, _ = _fetch(monkeypatch, exchange)

    original_time["data"][0]["ts"] = "1"
    original_markets["data"][0]["ctVal"] = "999"

    assert result["raw_responses"]["exchange_time"]["json"]["data"][0]["ts"] \
        == str(NOW_MS)
    assert result["raw_responses"]["markets"]["json"]["data"][0]["ctVal"] \
        == "0.01"


def test_version_and_requirements_pin_are_one_exact_contract():
    requirements = Path("requirements.txt").read_text().splitlines()
    assert adapter.SUPPORTED_CCXT_VERSION == "4.5.58"
    assert adapter.ccxt.__version__ == adapter.SUPPORTED_CCXT_VERSION
    assert [line for line in requirements if line.startswith("ccxt==")] == [
        f"ccxt=={adapter.SUPPORTED_CCXT_VERSION}",
    ]


def test_version_mismatch_fails_before_client_creation(monkeypatch):
    _shadow(monkeypatch)
    monkeypatch.setattr(adapter.ccxt, "__version__", "4.5.59")
    result = adapter.fetch_public_snapshot(
        _exchange_factory=lambda _config: pytest.fail("client must not be created"),
    )
    assert result["status"] == "invalid"
    assert result["reason_codes"] == ["ccxt_version_mismatch"]


@pytest.mark.parametrize(
    ("timeout", "reason"),
    [
        (True, "invalid_timeout"),
        (0, "invalid_timeout"),
        (31, "invalid_timeout"),
        (0.0001, "invalid_timeout"),
        (1.0001, "invalid_timeout"),
    ],
)
def test_invalid_timeout_fails_before_client_creation(monkeypatch, timeout, reason):
    _shadow(monkeypatch)
    result = adapter.fetch_public_snapshot(
        timeout_s=timeout,
        _exchange_factory=lambda _config: pytest.fail("client must not be created"),
    )
    assert result["status"] == "invalid"
    assert result["reason_codes"] == [reason]


def test_one_millisecond_is_the_exact_minimum_timeout(monkeypatch):
    result, _, configs = _fetch(monkeypatch, timeout_s=0.001)
    assert result["status"] == "observed"
    assert configs[0]["timeout"] == 1


def test_exchange_credentials_in_environment_are_never_read_or_echoed(monkeypatch):
    fake_credentials = {
        "OKX_API_KEY": "FAKE_OKX_API_KEY_VALUE",
        "CCXT_API_KEY": "FAKE_CCXT_API_KEY_VALUE",
        "OKX_API_SECRET": "FAKE_OKX_SECRET_VALUE",
        "CCXT_API_SECRET": "FAKE_CCXT_SECRET_VALUE",
        "OKX_PASSPHRASE": "FAKE_OKX_PASSPHRASE_VALUE",
        "CCXT_PASSPHRASE": "FAKE_CCXT_PASSPHRASE_VALUE",
        "secret": "FAKE_LOWERCASE_SECRET_VALUE",
        "passphrase": "FAKE_LOWERCASE_PASSPHRASE_VALUE",
    }
    for name, value in fake_credentials.items():
        monkeypatch.setenv(name, value)

    result, _, configs = _fetch(monkeypatch)
    serialized_config = json.dumps(configs)
    serialized_result = json.dumps(result)

    assert result["status"] == "observed"
    for value in fake_credentials.values():
        assert value not in serialized_config
        assert value not in serialized_result


@pytest.mark.parametrize(
    ("failure", "reason"),
    [
        ({"fail_time": True}, "exchange_time_request_failed"),
        ({"fail_markets": True}, "markets_request_failed"),
    ],
)
def test_request_failures_emit_only_reason_codes_and_close(monkeypatch, failure, reason):
    exchange = _Exchange(**failure)
    result, _, _ = _fetch(monkeypatch, exchange)
    serialized = json.dumps(result)

    assert result["status"] == "invalid"
    assert result["reason_codes"] == [reason]
    assert exchange.session.closed == 1
    assert "bad.invalid" not in serialized
    assert "TOP_SECRET" not in serialized


def test_factory_failure_is_sanitized(monkeypatch):
    _shadow(monkeypatch)

    def fail(_config):
        raise RuntimeError("https://bad.invalid/?secret=TOP_SECRET")

    result = adapter.fetch_public_snapshot(_exchange_factory=fail)
    assert result["status"] == "unavailable"
    assert result["reason_codes"] == ["client_initialization_failed"]
    assert "TOP_SECRET" not in json.dumps(result)


@pytest.mark.parametrize(
    ("session", "reason"),
    [
        (object(), "client_session_close_unavailable"),
        (_Session(close_error=True), "client_session_close_failed"),
    ],
)
def test_session_close_is_part_of_the_fail_closed_contract(monkeypatch, session, reason):
    result, _, _ = _fetch(monkeypatch, _Exchange(session=session))
    assert result["status"] == "invalid"
    assert result["reason_codes"] == [reason]
    assert result["markets"] == []


def test_exchange_time_must_match_raw_exchange_evidence(monkeypatch):
    result, _, _ = _fetch(monkeypatch, _Exchange(time_value=NOW_MS + 1))
    assert result["status"] == "invalid"
    assert result["reason_codes"] == ["exchange_time_projection_mismatch"]


def test_exchange_clock_skew_is_rejected(monkeypatch):
    exchange = _Exchange(time_value=NOW_MS)
    exchange.time_payload["data"][0]["ts"] = str(NOW_MS)
    result, _, _ = _fetch(
        monkeypatch,
        exchange,
        _now_ms=_clock(1_000, 1_001, 1_002, 1_003),
    )
    assert result["reason_codes"] == ["exchange_time_clock_skew"]


def test_exchange_time_probe_cannot_age_out_during_market_request(monkeypatch):
    result, _, _ = _fetch(
        monkeypatch,
        _now_ms=_clock(
            NOW_MS - 10, NOW_MS + 10, NOW_MS + 11,
            NOW_MS + adapter.MAX_TIME_PROBE_AGE_MS + 11,
        ),
    )
    assert result["reason_codes"] == ["exchange_time_probe_stale"]


@pytest.mark.parametrize(
    "values",
    [
        (NOW_MS, NOW_MS - 1, NOW_MS, NOW_MS),
        (NOW_MS, NOW_MS, NOW_MS - 1, NOW_MS),
        (NOW_MS, NOW_MS, NOW_MS + 1, NOW_MS),
    ],
)
def test_local_clock_regression_is_always_fail_closed(monkeypatch, values):
    result, exchange, _ = _fetch(monkeypatch, _now_ms=_clock(*values))
    assert result["status"] == "invalid"
    assert result["reason_codes"] == ["local_clock_regression"]
    assert exchange.session.closed == 1


@pytest.mark.parametrize(
    "values",
    [
        (True, NOW_MS, NOW_MS, NOW_MS),
        (NOW_MS, float(NOW_MS), NOW_MS, NOW_MS),
        (NOW_MS, NOW_MS, str(NOW_MS), NOW_MS),
        (NOW_MS, NOW_MS, NOW_MS, False),
    ],
)
def test_local_clock_type_confusion_is_rejected(monkeypatch, values):
    result, exchange, _ = _fetch(monkeypatch, _now_ms=_clock(*values))
    assert result["status"] == "invalid"
    assert result["reason_codes"] == ["local_clock_invalid"]
    assert exchange.session.closed == 1


def test_local_clock_exception_is_sanitized(monkeypatch):
    exchange = _Exchange()

    def broken_clock():
        raise RuntimeError("https://bad.invalid/?secret=TOP_SECRET")

    result, _, _ = _fetch(monkeypatch, exchange, _now_ms=broken_clock)
    serialized = json.dumps(result)
    assert result["status"] == "invalid"
    assert result["reason_codes"] == ["local_clock_read_failed"]
    assert "bad.invalid" not in serialized
    assert "TOP_SECRET" not in serialized
    assert exchange.session.closed == 1


def test_raw_body_projection_mismatch_is_rejected(monkeypatch):
    exchange = _Exchange()
    original = exchange.fetch_time

    def mismatched():
        value = original()
        exchange.last_json_response = {
            "code": "0", "data": [{"ts": "1"}], "msg": "",
        }
        return value

    exchange.fetch_time = mismatched
    result, _, _ = _fetch(monkeypatch, exchange)
    assert result["reason_codes"] == ["raw_response_projection_mismatch"]


def test_raw_projection_requires_exact_json_scalar_types(monkeypatch):
    exchange = _Exchange()
    original = exchange.fetch_time

    def mismatched():
        value = original()
        exchange.last_json_response = {
            "code": "0", "data": [{"ts": str(NOW_MS)}], "msg": False,
        }
        exchange.last_http_response = json.dumps({
            "code": "0", "data": [{"ts": str(NOW_MS)}], "msg": 0,
        })
        return value

    exchange.fetch_time = mismatched
    result, _, _ = _fetch(monkeypatch, exchange)
    assert result["reason_codes"] == ["raw_response_projection_mismatch"]


@pytest.mark.parametrize(
    ("body", "reported", "reason"),
    [
        (
            '{"code":"0","code":"0","data":[{"ts":"1784200000000"}]}',
            {"code": "0", "data": [{"ts": str(NOW_MS)}]},
            "raw_response_duplicate_key",
        ),
        (
            '{"code":"0","data":[{"ts":NaN}]}',
            {"code": "0", "data": [{"ts": float("nan")}]},
            "raw_response_nonfinite",
        ),
        (
            '{"code":"0","apiKey":"TOP_SECRET","data":[]}',
            {"code": "0", "apiKey": "TOP_SECRET", "data": []},
            "raw_response_sensitive_field",
        ),
    ],
)
def test_unsafe_raw_evidence_is_never_returned(monkeypatch, body, reported, reason):
    exchange = _Exchange()

    def unsafe_time():
        exchange.calls.append("fetch_time")
        exchange.last_http_response = body
        exchange.last_json_response = reported
        return NOW_MS

    exchange.fetch_time = unsafe_time
    result, _, _ = _fetch(monkeypatch, exchange)
    assert result["status"] == "invalid"
    assert result["reason_codes"] == [reason]
    assert result["raw_responses"] == {}
    assert "TOP_SECRET" not in json.dumps(result)


def test_post_download_retained_raw_evidence_limit_is_fail_closed(monkeypatch):
    exchange = _Exchange()
    monkeypatch.setattr(adapter, "MAX_RAW_RESPONSE_BYTES", 10)
    result, _, _ = _fetch(monkeypatch, exchange)
    assert result["reason_codes"] == ["raw_response_too_large"]


@pytest.mark.parametrize(
    ("raw_changes", "market_changes", "reason"),
    [
        ({"instId": "btc-USDT-SWAP"}, {}, "market_identity_invalid"),
        ({"instType": "FUTURES"}, {}, "market_identity_invalid"),
        ({"ctType": "inverse"}, {}, "market_unit_ambiguous"),
        ({"settleCcy": "USD"}, {}, "market_unit_ambiguous"),
        ({"ctValCcy": "ETH"}, {}, "market_contract_size_unit_mismatch"),
        ({"ctVal": "0"}, {}, "market_contract_size_invalid"),
        ({"ctVal": "NaN"}, {}, "market_contract_size_invalid"),
        ({"ctVal": "1e-2"}, {}, "market_contract_size_invalid"),
        ({}, {"contractSize": 1}, "market_contract_size_projection_mismatch"),
        ({}, {"symbol": "XBT/USDT:USDT"}, "market_projection_conflict"),
        ({}, {"active": False}, "market_projection_conflict"),
        ({}, {"active": 1}, "market_projection_conflict"),
        ({}, {"amount": 1}, None),
    ],
)
def test_symbol_contract_size_and_unit_ambiguity_fail_closed(
    monkeypatch, raw_changes, market_changes, reason,
):
    info = _info(**raw_changes)
    market = _market(info, **market_changes)
    exchange = _Exchange(
        market_payload={"code": "0", "data": [info], "msg": ""},
        markets=[market],
    )
    result, _, _ = _fetch(monkeypatch, exchange)
    if reason is None:
        assert result["status"] == "observed"
    else:
        assert result["status"] == "invalid"
        assert result["reason_codes"] == [reason]


def test_projected_raw_info_must_be_the_exact_upstream_row(monkeypatch):
    info = _info()
    market = _market(info)
    market["info"] = {**info, "ctVal": "1"}
    result, _, _ = _fetch(
        monkeypatch,
        _Exchange(
            market_payload={"code": "0", "data": [info], "msg": ""},
            markets=[market],
        ),
    )
    assert result["reason_codes"] == ["market_raw_projection_mismatch"]


@pytest.mark.parametrize("duplicate_side", ["raw", "projected"])
def test_duplicate_market_identity_rejects_the_whole_snapshot(monkeypatch, duplicate_side):
    info = _info()
    raw_rows = [info, copy.deepcopy(info)] if duplicate_side == "raw" else [info]
    markets = [_market(info), _market(info)] if duplicate_side == "projected" else [_market(info)]
    result, _, _ = _fetch(
        monkeypatch,
        _Exchange(
            market_payload={"code": "0", "data": raw_rows, "msg": ""},
            markets=markets,
        ),
    )
    assert result["status"] == "invalid"
    assert result["reason_codes"] == ["duplicate_market_id"]


def test_non_live_rows_are_not_promoted_but_do_not_fake_a_conflict(monkeypatch):
    live = _info()
    suspended = _info(instId="ETH-USDT-SWAP", state="suspend", ctValCcy="ETH")
    result, _, _ = _fetch(
        monkeypatch,
        _Exchange(
            market_payload={"code": "0", "data": [live, suspended], "msg": ""},
            markets=[
                _market(live),
                _market(
                    suspended,
                    id="ETH-USDT-SWAP",
                    symbol="ETH/USDT:USDT",
                    base="ETH",
                    active=False,
                ),
            ],
        ),
    )
    assert result["status"] == "observed"
    assert [row["market_id"] for row in result["markets"]] == ["BTC-USDT-SWAP"]


def test_inverse_and_nonstandard_swap_rows_remain_raw_but_out_of_scope(monkeypatch):
    live = _info()
    inverse = _info(
        instId="ETH-USD-SWAP",
        ctType="inverse",
        settleCcy="ETH",
        ctValCcy="USD",
    )
    nonstandard = _info(
        instId="XAAPL-USD_UM-SWAP",
        settleCcy="USDT",
        ctValCcy="XAAPL",
    )
    result, _, _ = _fetch(
        monkeypatch,
        _Exchange(
            market_payload={
                "code": "0", "data": [live, inverse, nonstandard], "msg": "",
            },
            markets=[
                _market(live),
                _market(
                    inverse,
                    id="ETH-USD-SWAP",
                    symbol="ETH/USD:ETH",
                    base="ETH",
                    quote="USD",
                    settle="ETH",
                    linear=False,
                    inverse=True,
                    contractSize=100,
                ),
                _market(
                    nonstandard,
                    id="XAAPL-USD_UM-SWAP",
                    symbol="XAAPL/USDT:USDT",
                    base="XAAPL",
                ),
            ],
        ),
    )
    assert result["status"] == "observed"
    assert [row["market_id"] for row in result["markets"]] == ["BTC-USDT-SWAP"]
    assert len(result["raw_responses"]["markets"]["json"]["data"]) == 3


def test_no_live_market_is_not_a_successful_empty_inventory(monkeypatch):
    info = _info(state="suspend")
    result, _, _ = _fetch(
        monkeypatch,
        _Exchange(
            market_payload={"code": "0", "data": [info], "msg": ""},
            markets=[_market(info, active=False)],
        ),
    )
    assert result["status"] == "invalid"
    assert result["reason_codes"] == ["no_live_markets"]


def test_native_and_ccxt_paths_remain_one_exchange_authority():
    native = {
        "authority_key": "exchange:okx",
        "dataset_key": "okx:public-rest:swap-markets",
        "path_key": "native-urllib:v1",
    }
    ccxt_path = adapter._identity(adapter.SUPPORTED_CCXT_VERSION)
    counts = adapter.summarize_source_paths([native, ccxt_path])

    assert counts == {
        "state": "ok",
        "reason_codes": [],
        "independent_source_count": 1,
        "observed_path_count": 2,
        "authority_keys": ["exchange:okx"],
        "path_keys": ["ccxt-rest:4.5.58", "native-urllib:v1"],
    }


def test_invalid_source_identity_never_inflates_counts():
    counts = adapter.summarize_source_paths([
        {"authority_key": "exchange:okx", "dataset_key": "x", "path_key": ""},
    ])
    assert counts == {
        "state": "invalid",
        "reason_codes": ["invalid_source_identity"],
        "independent_source_count": 0,
        "observed_path_count": 0,
    }


def test_source_identity_values_cannot_echo_urls_or_secrets():
    counts = adapter.summarize_source_paths([{
        "authority_key": "exchange:okx",
        "dataset_key": "okx:public-rest:swap-markets",
        "path_key": "https://bad.invalid/?secret=TOP_SECRET",
    }])
    assert counts["state"] == "invalid"
    assert "bad.invalid" not in json.dumps(counts)
    assert "TOP_SECRET" not in json.dumps(counts)


def test_module_has_only_the_two_allowlisted_ccxt_fetch_calls():
    tree = ast.parse(inspect.getsource(adapter))
    called_attributes = {
        node.func.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
    }
    assert {"fetch_time", "fetch_markets"} <= called_attributes
    assert called_attributes.isdisjoint({
        "create_order", "set_leverage", "fetch_balance", "fetch_positions",
        "fetch_orders", "cancel_order", "load_markets", "fetch_currencies",
        "set_markets", "write_text", "write_bytes", "mkdir", "replace",
    })
    direct_client_calls = {
        node.func.attr
        for node in ast.walk(tree)
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "client"
        )
    }
    assert direct_client_calls == {"fetch_time", "fetch_markets"}
