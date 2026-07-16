"""Strict, keyless CCXT REST evidence for the OKX USDT-swap catalog.

This adapter is deliberately narrower than CryptoScope's private exchange module:
it exposes one feature-gated market snapshot, never accepts credentials or arbitrary
CCXT methods, and never writes evidence to disk.  CCXT and the native OKX collector
are two adapter paths to the same exchange authority, not independent sources.
"""

from __future__ import annotations

import copy
import hashlib
import json
import math
import os
import re
import time
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

import ccxt


SCHEMA_VERSION = 1
SUPPORTED_CCXT_VERSION = "4.5.58"
MODE_ENV = "CRYPTOSCOPE_CCXT_OKX_SWAP_MARKETS_MODE"
ALLOWED_MODE = "shadow"
MAX_RAW_RESPONSE_BYTES = 4 * 1024 * 1024
MAX_TOTAL_RAW_RESPONSE_BYTES = 8 * 1024 * 1024
# Post-download retention guards only: CCXT/requests has already downloaded and
# decoded the HTTP body before these byte checks run.  They bound what this adapter
# retains and returns as evidence; they are not transport or peak-memory limits.
MAX_EXCHANGE_CLOCK_SKEW_MS = 30_000
MAX_TIME_PROBE_AGE_MS = 30_000

_AUTHORITY_KEY = "exchange:okx"
_DATASET_KEY = "okx:public-rest:swap-markets"
_MARKET_ID = re.compile(r"^(?P<base>[A-Z0-9]+)-USDT-SWAP$")
_ANY_SWAP_ID = re.compile(r"^[A-Z0-9_]+(?:-[A-Z0-9_]+)+$")
_POSITIVE_DECIMAL = re.compile(r"^(?:0|[1-9][0-9]*)(?:\.[0-9]+)?$")
_INTEGER_TEXT = re.compile(r"^[0-9]+$")
_IDENTITY_VALUE = re.compile(r"^[a-z0-9][a-z0-9:._-]{0,127}$")
_VERSION_TEXT = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+$")
_SENSITIVE_KEYS = {
    "apikey",
    "api_key",
    "authorization",
    "ok-access-key",
    "ok-access-passphrase",
    "ok-access-sign",
    "password",
    "passphrase",
    "secret",
}


class _ContractError(ValueError):
    def __init__(self, reason_code: str) -> None:
        super().__init__(reason_code)
        self.reason_code = reason_code


def _identity(version: str | None = None) -> dict[str, str]:
    version = (
        version
        if isinstance(version, str) and _VERSION_TEXT.fullmatch(version)
        else "unknown"
    )
    return {
        "authority_key": _AUTHORITY_KEY,
        "dataset_key": _DATASET_KEY,
        "path_key": f"ccxt-rest:{version}",
        "independence_basis": "distinct_exchange_authority_v1",
    }


def summarize_source_paths(
    source_identities: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    """Count distinct exchange authorities separately from adapter paths."""
    if isinstance(source_identities, (str, bytes)):
        return {
            "state": "invalid",
            "reason_codes": ["invalid_source_identity"],
            "independent_source_count": 0,
            "observed_path_count": 0,
        }
    authorities: set[str] = set()
    paths: set[tuple[str, str]] = set()
    try:
        for identity in source_identities:
            if not isinstance(identity, Mapping):
                raise _ContractError("invalid_source_identity")
            authority = identity.get("authority_key")
            dataset = identity.get("dataset_key")
            path = identity.get("path_key")
            if not all(
                isinstance(value, str)
                and value == value.strip()
                and _IDENTITY_VALUE.fullmatch(value)
                for value in (authority, dataset, path)
            ):
                raise _ContractError("invalid_source_identity")
            authorities.add(authority)
            paths.add((authority, path))
    except (TypeError, _ContractError):
        return {
            "state": "invalid",
            "reason_codes": ["invalid_source_identity"],
            "independent_source_count": 0,
            "observed_path_count": 0,
        }
    return {
        "state": "ok",
        "reason_codes": [],
        "independent_source_count": len(authorities),
        "observed_path_count": len(paths),
        "authority_keys": sorted(authorities),
        "path_keys": sorted(path for _, path in paths),
    }


def _result(
    status: str,
    reason_code: str | None = None,
    *,
    version: str | None = None,
) -> dict[str, object]:
    identity = _identity(version)
    observed = status == "observed"
    counts = summarize_source_paths([identity] if observed else [])
    configured_mode = os.environ.get(MODE_ENV, "off")
    reported_mode = configured_mode if configured_mode in {"off", ALLOWED_MODE} else "invalid"
    return {
        "schema_version": SCHEMA_VERSION,
        "status": status,
        "exchange": "okx",
        "market_type": "swap",
        "mode": reported_mode,
        "reason_codes": [reason_code] if reason_code else [],
        "adapter": {
            "name": "ccxt",
            "version": version,
            "exchange_class": "okx",
            "transport": "public_rest",
        },
        "source_identity": identity,
        "source_counts": counts,
        "exchange_time": None,
        "raw_responses": {},
        "markets": [],
    }


def _requirements_pin() -> str | None:
    path = Path(__file__).resolve().parents[2] / "requirements.txt"
    try:
        pins = [
            line.strip().split("==", 1)[1]
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip().startswith("ccxt==")
        ]
    except (OSError, IndexError):
        return None
    return pins[0] if len(pins) == 1 and pins[0] else None


def _reject_json_constant(_value: str) -> object:
    raise _ContractError("raw_response_nonfinite")


def _unique_json_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise _ContractError("raw_response_duplicate_key")
        result[key] = value
    return result


def _has_sensitive_key(value: object) -> bool:
    if isinstance(value, dict):
        for key, item in value.items():
            if str(key).strip().lower() in _SENSITIVE_KEYS:
                return True
            if _has_sensitive_key(item):
                return True
    elif isinstance(value, list):
        return any(_has_sensitive_key(item) for item in value)
    return False


def _exact_json_equal(left: object, right: object) -> bool:
    """JSON equality that does not accept Python's bool/int coercion."""
    if type(left) is not type(right):
        return False
    if isinstance(left, dict):
        return (
            left.keys() == right.keys()
            and all(_exact_json_equal(left[key], right[key]) for key in left)
        )
    if isinstance(left, list):
        return len(left) == len(right) and all(
            _exact_json_equal(lvalue, rvalue)
            for lvalue, rvalue in zip(left, right)
        )
    return bool(left == right)


def _capture_raw_response(exchange: object) -> dict[str, object]:
    body = getattr(exchange, "last_http_response", None)
    reported_json = getattr(exchange, "last_json_response", None)
    if not isinstance(body, str) or not body.strip() or reported_json is None:
        raise _ContractError("raw_response_missing")
    encoded = body.encode("utf-8")
    if len(encoded) > MAX_RAW_RESPONSE_BYTES:
        raise _ContractError("raw_response_too_large")
    try:
        parsed = json.loads(
            body,
            parse_constant=_reject_json_constant,
            object_pairs_hook=_unique_json_object,
        )
    except _ContractError:
        raise
    except (json.JSONDecodeError, UnicodeError, TypeError, ValueError):
        raise _ContractError("raw_response_invalid_json") from None
    if not _exact_json_equal(parsed, reported_json):
        raise _ContractError("raw_response_projection_mismatch")
    if _has_sensitive_key(parsed):
        raise _ContractError("raw_response_sensitive_field")
    return {
        "body": str(body),
        "json": copy.deepcopy(parsed),
        "sha256": hashlib.sha256(encoded).hexdigest(),
        "bytes": len(encoded),
    }


def _validate_exchange_time(
    server_time: object,
    raw: Mapping[str, object],
    started_at_ms: int,
    completed_at_ms: int,
) -> dict[str, object]:
    if (
        not isinstance(server_time, int)
        or isinstance(server_time, bool)
        or server_time <= 0
        or completed_at_ms < started_at_ms
    ):
        raise _ContractError("exchange_time_invalid")
    payload = raw.get("json")
    if (
        not isinstance(payload, dict)
        or payload.get("code") != "0"
        or payload.get("msg") != ""
    ):
        raise _ContractError("exchange_time_schema_invalid")
    rows = payload.get("data")
    if not isinstance(rows, list) or len(rows) != 1 or not isinstance(rows[0], dict):
        raise _ContractError("exchange_time_schema_invalid")
    raw_ts = rows[0].get("ts")
    if not isinstance(raw_ts, str) or not _INTEGER_TEXT.fullmatch(raw_ts):
        raise _ContractError("exchange_time_schema_invalid")
    if int(raw_ts) != server_time:
        raise _ContractError("exchange_time_projection_mismatch")
    if (
        server_time < started_at_ms - MAX_EXCHANGE_CLOCK_SKEW_MS
        or server_time > completed_at_ms + MAX_EXCHANGE_CLOCK_SKEW_MS
    ):
        raise _ContractError("exchange_time_clock_skew")
    return {
        "server_time_ms": server_time,
        "probe_started_at_ms": started_at_ms,
        "probe_completed_at_ms": completed_at_ms,
        "raw_sha256": raw["sha256"],
    }


def _positive_decimal_text(value: object, reason_code: str) -> Decimal:
    if (
        not isinstance(value, str)
        or value != value.strip()
        or not _POSITIVE_DECIMAL.fullmatch(value)
    ):
        raise _ContractError(reason_code)
    try:
        parsed = Decimal(value)
    except InvalidOperation:
        raise _ContractError(reason_code) from None
    if not parsed.is_finite() or parsed <= 0:
        raise _ContractError(reason_code)
    return parsed


def _projected_decimal(value: object, reason_code: str) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (int, float, str, Decimal)):
        raise _ContractError(reason_code)
    if isinstance(value, float) and not math.isfinite(value):
        raise _ContractError(reason_code)
    try:
        parsed = Decimal(str(value))
    except InvalidOperation:
        raise _ContractError(reason_code) from None
    if not parsed.is_finite() or parsed <= 0:
        raise _ContractError(reason_code)
    return parsed


def _timeout_milliseconds(timeout_s: object) -> int:
    if isinstance(timeout_s, bool) or not isinstance(timeout_s, (int, float)):
        raise _ContractError("invalid_timeout")
    try:
        milliseconds = Decimal(str(timeout_s)) * 1000
    except InvalidOperation:
        raise _ContractError("invalid_timeout") from None
    if (
        not milliseconds.is_finite()
        or milliseconds != milliseconds.to_integral_value()
        or not 1 <= milliseconds <= 30_000
    ):
        raise _ContractError("invalid_timeout")
    return int(milliseconds)


def _read_local_clock(now_ms: Callable[[], int]) -> int:
    try:
        value = now_ms()
    except Exception:
        raise _ContractError("local_clock_read_failed") from None
    if type(value) is not int or value < 0:
        raise _ContractError("local_clock_invalid")
    return value


def _require_clock_order(*values: int) -> None:
    if any(later < earlier for earlier, later in zip(values, values[1:])):
        raise _ContractError("local_clock_regression")


def _validate_live_market(
    raw: Mapping[str, object],
    projected: Mapping[str, object],
    base: str,
) -> dict[str, object]:
    market_id = raw["instId"]
    if not _exact_json_equal(projected.get("info"), raw):
        raise _ContractError("market_raw_projection_mismatch")
    exact_values = {
        "id": market_id,
        "symbol": f"{base}/USDT:USDT",
        "base": base,
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
    }
    if any(
        not _exact_json_equal(projected.get(key), expected)
        for key, expected in exact_values.items()
    ):
        raise _ContractError("market_projection_conflict")
    if raw.get("ctType") != "linear" or raw.get("settleCcy") != "USDT":
        raise _ContractError("market_unit_ambiguous")
    ct_val_ccy = raw.get("ctValCcy")
    if ct_val_ccy != base:
        raise _ContractError("market_contract_size_unit_mismatch")
    raw_size = _positive_decimal_text(
        raw.get("ctVal"), "market_contract_size_invalid",
    )
    projected_size = _projected_decimal(
        projected.get("contractSize"), "market_contract_size_invalid",
    )
    if raw_size != projected_size:
        raise _ContractError("market_contract_size_projection_mismatch")
    return {
        "market_id": market_id,
        "symbol": projected["symbol"],
        "base": base,
        "quote": "USDT",
        "settle": "USDT",
        "active": True,
        "linear": True,
        "amount_unit": "contracts",
        "contract_size": str(raw["ctVal"]),
        "contract_size_unit": base,
    }


def _validate_markets(
    markets: object,
    raw: Mapping[str, object],
) -> list[dict[str, object]]:
    payload = raw.get("json")
    if (
        not isinstance(payload, dict)
        or payload.get("code") != "0"
        or payload.get("msg") != ""
    ):
        raise _ContractError("markets_response_schema_invalid")
    raw_rows = payload.get("data")
    if not isinstance(raw_rows, list) or not raw_rows:
        raise _ContractError("markets_response_schema_invalid")
    if not isinstance(markets, list) or not markets:
        raise _ContractError("markets_projection_invalid")

    raw_by_id: dict[str, dict[str, object]] = {}
    for row in raw_rows:
        if not isinstance(row, dict):
            raise _ContractError("market_row_invalid")
        market_id = row.get("instId")
        if (
            not isinstance(market_id, str)
            or market_id != market_id.strip()
            or not market_id
            or not _ANY_SWAP_ID.fullmatch(market_id)
            or row.get("instType") != "SWAP"
            or not isinstance(row.get("state"), str)
        ):
            raise _ContractError("market_identity_invalid")
        # The OKX SWAP response also contains inverse and other non-standard
        # contracts.  They remain in the raw proof but are outside this adapter's
        # deliberately narrow standard linear-USDT identity contract.  A malformed
        # spelling of that standard identity is different: it is ambiguity, not an
        # out-of-scope instrument, and invalidates the snapshot.
        if market_id.endswith("-USDT-SWAP") and not _MARKET_ID.fullmatch(market_id):
            raise _ContractError("market_identity_invalid")
        if market_id in raw_by_id:
            raise _ContractError("duplicate_market_id")
        raw_by_id[market_id] = row

    projected_by_id: dict[str, Mapping[str, object]] = {}
    for market in markets:
        if not isinstance(market, Mapping):
            raise _ContractError("markets_projection_invalid")
        market_id = market.get("id")
        if not isinstance(market_id, str) or market_id not in raw_by_id:
            raise _ContractError("market_projection_conflict")
        if market_id in projected_by_id:
            raise _ContractError("duplicate_market_id")
        projected_by_id[market_id] = market

    accepted: list[dict[str, object]] = []
    for market_id, row in raw_by_id.items():
        matched = _MARKET_ID.fullmatch(market_id)
        if matched is None:
            continue
        if row["state"] != "live":
            continue
        projected = projected_by_id.get(market_id)
        if projected is None:
            raise _ContractError("market_projection_missing")
        accepted.append(_validate_live_market(row, projected, matched.group("base")))
    if not accepted:
        raise _ContractError("no_live_markets")
    return sorted(accepted, key=lambda row: str(row["market_id"]))


def _close_client(client: object) -> str | None:
    try:
        session = getattr(client, "session", None)
        close = getattr(session, "close", None)
        if not callable(close):
            return "client_session_close_unavailable"
        close()
    except Exception:
        return "client_session_close_failed"
    return None


def _client_config(timeout_ms: int) -> dict[str, object]:
    """Build the exact keyless config required to retain auditable raw evidence."""
    return {
        "enableRateLimit": True,
        "timeout": timeout_ms,
        # CCXT 4.5.58 leaves last_json_response disabled by default.  Both caches
        # must be explicit because the evidence contract compares the exact body
        # with CCXT's parsed response immediately after each public request.
        "enableLastHttpResponse": True,
        "enableLastJsonResponse": True,
        "options": {
            "defaultType": "swap",
            "fetchMarkets": {"types": ["swap"]},
        },
    }


def fetch_public_snapshot(
    exchange: str = "okx",
    market_type: str = "swap",
    *,
    timeout_s: float = 8.0,
    _exchange_factory: Callable[[dict[str, object]], object] | None = None,
    _now_ms: Callable[[], int] | None = None,
) -> dict[str, object]:
    """Fetch one all-or-nothing OKX swap-market snapshot in shadow mode."""
    version = getattr(ccxt, "__version__", None)
    if exchange != "okx":
        return _result("invalid", "unsupported_exchange", version=version)
    if market_type != "swap":
        return _result("invalid", "unsupported_market_type", version=version)
    mode = os.environ.get(MODE_ENV, "off")
    if mode == "off":
        return _result("disabled", version=version)
    if mode != ALLOWED_MODE:
        return _result("invalid", "invalid_feature_mode", version=version)
    if version != SUPPORTED_CCXT_VERSION:
        return _result("invalid", "ccxt_version_mismatch", version=version)
    if _requirements_pin() != SUPPORTED_CCXT_VERSION:
        return _result("invalid", "ccxt_requirements_pin_mismatch", version=version)
    try:
        timeout_ms = _timeout_milliseconds(timeout_s)
    except _ContractError:
        return _result("invalid", "invalid_timeout", version=version)

    factory = _exchange_factory or ccxt.okx
    now_ms = _now_ms or (lambda: time.time_ns() // 1_000_000)
    client: object | None = None
    result: dict[str, object]
    try:
        config = _client_config(timeout_ms)
        client = factory(config)
        if getattr(client, "id", None) != "okx":
            raise _ContractError("client_identity_mismatch")

        time_started_ms = _read_local_clock(now_ms)
        try:
            server_time = client.fetch_time()
        except Exception:
            raise _ContractError("exchange_time_request_failed") from None
        time_completed_ms = _read_local_clock(now_ms)
        _require_clock_order(time_started_ms, time_completed_ms)
        time_raw = _capture_raw_response(client)
        exchange_time = _validate_exchange_time(
            server_time, time_raw, time_started_ms, time_completed_ms,
        )

        markets_started_ms = _read_local_clock(now_ms)
        _require_clock_order(
            time_started_ms, time_completed_ms, markets_started_ms,
        )
        try:
            # No set_markets(): P0 returns this catalog and invokes no later
            # symbol-dependent CCXT method on the short-lived client.
            markets = client.fetch_markets()
        except Exception:
            raise _ContractError("markets_request_failed") from None
        markets_completed_ms = _read_local_clock(now_ms)
        _require_clock_order(
            time_started_ms,
            time_completed_ms,
            markets_started_ms,
            markets_completed_ms,
        )
        if markets_completed_ms - time_completed_ms > MAX_TIME_PROBE_AGE_MS:
            raise _ContractError("exchange_time_probe_stale")
        markets_raw = _capture_raw_response(client)
        if int(time_raw["bytes"]) + int(markets_raw["bytes"]) > MAX_TOTAL_RAW_RESPONSE_BYTES:
            raise _ContractError("raw_response_total_too_large")
        projected = _validate_markets(markets, markets_raw)

        result = _result("observed", version=version)
        result.update({
            "exchange_time": exchange_time,
            "raw_responses": {
                "exchange_time": time_raw,
                "markets": markets_raw,
            },
            "request_timing": {
                "markets_started_at_ms": markets_started_ms,
                "markets_completed_at_ms": markets_completed_ms,
            },
            "markets": projected,
        })
    except _ContractError as exc:
        result = _result("invalid", exc.reason_code, version=version)
    except Exception:
        result = _result("unavailable", "client_initialization_failed", version=version)
    finally:
        if client is not None:
            close_error = _close_client(client)
            if close_error is not None:
                result = _result("invalid", close_error, version=version)
    return result
