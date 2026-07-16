"""Bybit listing failures must stay distinct from verified empty deltas."""
from __future__ import annotations

import json

import httpx
import pytest


class _JsonResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


def _valid_payload(*symbols: str) -> dict:
    return {
        "retCode": 0,
        "retMsg": "OK",
        "result": {
            "category": "spot",
            "list": [{"symbol": symbol, "status": "Trading"}
                     for symbol in symbols],
            "nextPageCursor": "",
        },
    }


def test_bybit_country_block_is_classified_and_never_overwrites_baseline(
        tmp_path, monkeypatch):
    import src.collectors.listing_detector as ld

    monkeypatch.setattr(ld, "SNAPSHOT_DIR", tmp_path)
    ld._save_snapshot("bybit", {"BTCUSDT", "ETHUSDT"})

    def blocked(*_args, **_kwargs):
        request = httpx.Request("GET", ld.EXCHANGES["bybit"]["urls"][0])
        response = httpx.Response(
            403, request=request,
            text="CloudFront distribution is configured to block access from your country")
        raise httpx.HTTPStatusError(
            "403 Forbidden", request=request, response=response)

    monkeypatch.setattr(ld.httpx, "get", blocked)
    result = ld.check_exchange_result("bybit")

    assert result["status"] == "failed"
    assert result["error_kind"] == "geo_blocked"
    assert result["http_status"] == 403
    assert result["symbol_count"] is None and result["alerts"] == []
    assert set(json.loads((tmp_path / "bybit_symbols.json").read_text())) == {
        "BTCUSDT", "ETHUSDT"}


def test_unproven_bybit_403_is_forbidden_not_assumed_geographic(monkeypatch):
    import src.collectors.listing_detector as ld

    def forbidden(*_args, **_kwargs):
        request = httpx.Request("GET", ld.EXCHANGES["bybit"]["urls"][0])
        response = httpx.Response(403, request=request, text="Forbidden request")
        raise httpx.HTTPStatusError(
            "403 Forbidden", request=request, response=response)

    monkeypatch.setattr(ld.httpx, "get", forbidden)
    result = ld.check_exchange_result("bybit")

    assert result["status"] == "failed"
    assert result["error_kind"] == "forbidden"
    assert result["http_status"] == 403


@pytest.mark.parametrize(
    ("payload", "error_kind", "upstream_code"),
    [
        ({"retCode": 10006, "retMsg": "Too many visits!", "result": {}},
         "rate_limited", 10006),
        ({"retCode": 429, "retMsg": "System level frequency protection", "result": {}},
         "rate_limited", 429),
        ({"retCode": 10000, "retMsg": "Server Timeout", "result": {}},
         "request_timeout", 10000),
        ({"retCode": 10009, "retMsg": "IP has been banned", "result": {}},
         "geo_blocked", 10009),
        ({"retCode": 500000, "retMsg": "Internal error", "result": {}},
         "upstream_api_error", 500000),
        ({"retMsg": "OK", "result": {"category": "spot", "list": []}},
         "invalid_response_schema", None),
        ({"retCode": 0, "retMsg": "OK", "result": {"category": "linear", "list": []}},
         "unexpected_market_category", None),
        ({"retCode": 0, "retMsg": "OK", "result": {"category": "spot"}},
         "invalid_response_schema", None),
        (_valid_payload(), "suspicious_empty_inventory", None),
    ],
)
def test_bybit_http_200_failure_envelopes_are_not_empty_successes(
        tmp_path, monkeypatch, payload, error_kind, upstream_code):
    import src.collectors.listing_detector as ld

    monkeypatch.setattr(ld, "SNAPSHOT_DIR", tmp_path)
    monkeypatch.setattr(ld.httpx, "get", lambda *_args, **_kwargs: _JsonResponse(payload))
    result = ld.check_exchange_result("bybit")

    assert result["status"] == "failed"
    assert result["error_kind"] == error_kind
    assert result["symbol_count"] is None and result["baseline_ready"] is False
    assert result["alerts"] == []
    assert not (tmp_path / "bybit_symbols.json").exists()
    if upstream_code is not None:
        assert result["upstream_code"] == upstream_code


@pytest.mark.parametrize(
    "rows",
    [
        ["not-an-object"],
        [{"symbol": "BTCUSDT"}],
        [{"symbol": "", "status": "Trading"}],
        [{"symbol": "BTCUSDT", "status": None}],
    ],
)
def test_malformed_bybit_instrument_rows_fail_the_whole_snapshot(
        tmp_path, monkeypatch, rows):
    import src.collectors.listing_detector as ld

    payload = _valid_payload("ETHUSDT")
    payload["result"]["list"] = [
        {"symbol": "ETHUSDT", "status": "Trading"}, *rows]
    monkeypatch.setattr(ld, "SNAPSHOT_DIR", tmp_path)
    monkeypatch.setattr(ld.httpx, "get", lambda *_args, **_kwargs: _JsonResponse(payload))

    result = ld.check_exchange_result("bybit")

    assert result["status"] == "failed"
    assert result["error_kind"] == "malformed_instrument_rows"
    assert not (tmp_path / "bybit_symbols.json").exists()


def test_schema_verified_bybit_inventory_can_establish_a_baseline(tmp_path, monkeypatch):
    import src.collectors.listing_detector as ld

    monkeypatch.setattr(ld, "SNAPSHOT_DIR", tmp_path)
    monkeypatch.setattr(
        ld.httpx, "get",
        lambda *_args, **_kwargs: _JsonResponse(_valid_payload("BTCUSDT", "ETHUSDT")))

    result = ld.check_exchange_result("bybit")

    assert result["status"] == "ok"
    assert result["symbol_count"] == 2
    assert result["baseline_ready"] is False
    assert result["alerts"] == []
    assert set(json.loads((tmp_path / "bybit_symbols.json").read_text())) == {
        "BTCUSDT", "ETHUSDT"}
