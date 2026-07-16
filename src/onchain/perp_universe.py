"""Fail-closed perpetual-market research universe with bounded provenance.

OKX proves that a standard live linear USDT swap exists.  CoinGecko then supplies a
best-effort ticker-to-contract mapping ordered by reported market cap.  That second
join is a heuristic, not asset-identity proof, so refreshed rows remain
``research_only`` and are never returned by :func:`load` to scanners.  A future
versioned human registry may mark individually verified rows; this module contains
no such entries and makes no directional or execution claim.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Mapping

import structlog

from src.config import DATA_DIR
from src.ops import stream_disk_guard

logger = structlog.get_logger()

_CACHE = DATA_DIR / "perp_universe.json"
CACHE_SCHEMA_VERSION = 2
CACHE_TTL_SECONDS = 26 * 60 * 60
NATIVE_ADAPTER_VERSION = 2
COINGECKO_ADAPTER_VERSION = 2
MAPPING_METHOD = "symbol_market_cap_heuristic"
RESEARCH_ACTIONABILITY = "research_only"
CCXT_MODE_ENV = "CRYPTOSCOPE_CCXT_OKX_SWAP_MARKETS_MODE"

_COINGECKO_MARKET_PAGE_COUNT = 4
_COINGECKO_MARKET_PAGE_SIZE = 250
_COINGECKO_MARKET_EXPECTED_UNIQUE_IDS = (
    _COINGECKO_MARKET_PAGE_COUNT * _COINGECKO_MARKET_PAGE_SIZE
)

_OKX_TIME_URL = "https://www.okx.com/api/v5/public/time"
_OKX_INSTRUMENTS_URL = (
    "https://www.okx.com/api/v5/public/instruments?instType=SWAP"
)
_COINGECKO_LIST_URL = (
    "https://api.coingecko.com/api/v3/coins/list?include_platform=true"
)
_COINGECKO_MARKETS_URL = (
    "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd"
    f"&order=market_cap_desc&per_page={_COINGECKO_MARKET_PAGE_SIZE}&page={{page}}"
)

# Hard retained-byte limits.  Every reader asks for at most one byte beyond the
# boundary, so an oversized remote body or corrupt local cache cannot first consume
# unbounded memory and only then be rejected.
_MAX_RETAINED_HTTP_BYTES = 32 * 1024 * 1024
_MAX_CACHE_BYTES = 8 * 1024 * 1024
_MAX_REDIRECT_DRAIN_BYTES = 64 * 1024
_MAX_EXCHANGE_CLOCK_SKEW_SECONDS = 30
_MAX_REFRESH_DURATION_SECONDS = 5 * 60
# Operational completeness controls, not OKX protocol guarantees.  A catalog
# below this floor, or more than 25% below the last still-valid snapshot, is more
# likely a partial response than a real market event and must not replace cache.
_MIN_OPERATIONAL_MARKET_COUNT = 300
_MAX_OPERATIONAL_DROP_NUMERATOR = 3
_MAX_OPERATIONAL_DROP_DENOMINATOR = 4
_MAX_OPERATIONAL_BASELINE_AGE = timedelta(days=14)
_MIN_MAPPING_COVERAGE_NUMERATOR = 1
_MIN_MAPPING_COVERAGE_DENOMINATOR = 3
_MIN_MAPPING_RETENTION_NUMERATOR = 3
_MIN_MAPPING_RETENTION_DENOMINATOR = 4

_AUTHORITY_KEY = "exchange:okx"
_DATASET_KEY = "okx:public-rest:swap-markets"
_NATIVE_PATH_KEY = "native-urllib:v2"
_STANDARD_MARKET_ID = re.compile(r"^(?P<base>[A-Z0-9]+)-USDT-SWAP$")
_ANY_SWAP_ID = re.compile(r"^[A-Z0-9_]+(?:-[A-Z0-9_]+)+$")
_SYMBOL = re.compile(r"^[A-Z0-9]+$")
_POSITIVE_DECIMAL = re.compile(r"^(?:0|[1-9][0-9]*)(?:\.[0-9]+)?$")
_INTEGER_TEXT = re.compile(r"^[0-9]+$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SEMVER = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+$")
_FORBIDDEN_CACHE_KEYS = {
    "body", "json", "raw_response", "raw_responses",
    "last_http_response", "last_json_response",
}

# CoinGecko platform id -> internal chain id, in deterministic preference order.
_CHAIN_PRIORITY = (
    ("ethereum", "ethereum"),
    ("binance-smart-chain", "bsc"),
    ("solana", "solana"),
    ("base", "base"),
    ("arbitrum-one", "arbitrum"),
    ("optimistic-ethereum", "optimism"),
    ("polygon-pos", "polygon"),
    ("avalanche", "avalanche"),
)


class _ContractError(ValueError):
    def __init__(self, reason_code: str) -> None:
        super().__init__(reason_code)
        self.reason_code = reason_code


@dataclass(frozen=True)
class _CachePublishState:
    """Namespace and crash-durability milestones for one cache publication."""

    namespace_replaced: bool
    directory_synced: bool


class _CachePublishError(_ContractError):
    """Sanitized write failure carrying only the completed publication stages."""

    def __init__(self, state: _CachePublishState) -> None:
        reason_code = (
            "cache_durability_unknown_after_replace"
            if state.namespace_replaced
            else "cache_write_failed_before_replace"
        )
        super().__init__(reason_code)
        self.state = state


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _require_utc(value: object, reason_code: str) -> datetime:
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() != timedelta(0)
    ):
        raise _ContractError(reason_code)
    return value


def _utc_iso(value: datetime) -> str:
    value = _require_utc(value, "local_time_invalid")
    return value.isoformat()


def _parse_utc_iso(value: object, reason_code: str) -> datetime:
    if not isinstance(value, str) or not value or value != value.strip():
        raise _ContractError(reason_code)
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        raise _ContractError(reason_code) from None
    parsed = _require_utc(parsed, reason_code)
    if parsed.isoformat() != value:
        raise _ContractError(reason_code)
    return parsed


def _unique_json_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise _ContractError("response_duplicate_key")
        result[key] = value
    return result


def _reject_json_constant(_value: str) -> object:
    raise _ContractError("response_nonfinite")


def _json_loads(text: str) -> object:
    try:
        return json.loads(
            text,
            parse_constant=_reject_json_constant,
            object_pairs_hook=_unique_json_object,
        )
    except _ContractError:
        raise
    except (json.JSONDecodeError, TypeError, ValueError):
        raise _ContractError("response_invalid_json") from None


def _canonical_bytes(value: object) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError):
        raise _ContractError("cache_not_canonical_json") from None


def _sha256(value: object) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _https_origin(url: object, reason_code: str) -> tuple[str, str, int]:
    """Return a normalized HTTPS origin without retaining URL details in errors."""
    if not isinstance(url, str):
        raise _ContractError(reason_code)
    try:
        parsed = urllib.parse.urlsplit(url)
        explicit_port = parsed.port
    except (TypeError, ValueError):
        raise _ContractError(reason_code) from None
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
    ):
        raise _ContractError(reason_code)
    effective_port = 443 if explicit_port is None else explicit_port
    return parsed.scheme, parsed.hostname, effective_port


def _close_http_response(response: object) -> None:
    """Best-effort close without allowing response details to escape in errors."""
    try:
        response.close()  # type: ignore[attr-defined]
    except Exception:
        pass


class _BoundedClosingRedirectResponse:
    """Let urllib drain at most a small body while closing on every outcome."""

    def __init__(self, response: object) -> None:
        self._response = response

    def read(self) -> bytes:
        try:
            value = self._response.read(  # type: ignore[attr-defined]
                _MAX_REDIRECT_DRAIN_BYTES,
            )
            return value if isinstance(value, bytes) else b""
        finally:
            _close_http_response(self._response)

    def close(self) -> None:
        _close_http_response(self._response)


class _SameOriginRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Reject a redirect before urllib opens a connection to another origin."""

    def __init__(self, initial_origin: tuple[str, str, int]) -> None:
        super().__init__()
        self._initial_origin = initial_origin

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        try:
            redirected_origin = _https_origin(newurl, "redirect_target_invalid")
        except _ContractError:
            _close_http_response(fp)
            raise
        if redirected_origin != self._initial_origin:
            # HTTPRedirectHandler calls parent.open() only after this method returns.
            # Close the 30x response here and fail before any second-origin socket can
            # be created.
            _close_http_response(fp)
            raise _ContractError("redirect_target_invalid")
        return super().redirect_request(req, fp, code, msg, headers, newurl)

    def http_error_302(self, req, fp, code, msg, headers):
        # The stdlib handler drains ``fp.read()`` without a size and closes only
        # afterwards.  A slow or failed same-origin 30x can therefore consume
        # unbounded memory or leak its descriptor.  This proxy gives the standard
        # redirect/loop/method logic a bounded read that closes in ``finally``.
        bounded = _BoundedClosingRedirectResponse(fp)
        return super().http_error_302(req, bounded, code, msg, headers)

    http_error_301 = http_error_302
    http_error_303 = http_error_302
    http_error_307 = http_error_302
    http_error_308 = http_error_302


def _fetch_json(url: str, timeout: int = 20) -> dict[str, object]:
    """Fetch JSON and retain only its digest/size after the caller validates it."""
    requested_origin = _https_origin(url, "request_target_invalid")
    request = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    opener = urllib.request.build_opener(
        _SameOriginRedirectHandler(requested_origin),
    )
    try:
        response = opener.open(request, timeout=timeout)
    except _ContractError:
        raise
    except urllib.error.HTTPError as exc:
        _close_http_response(exc)
        raise _ContractError("http_status_error") from None
    except (urllib.error.URLError, OSError, ValueError):
        raise _ContractError("http_transport_error") from None
    try:
        final_origin = _https_origin(
            response.geturl(), "response_target_invalid",
        )
        if final_origin != requested_origin:
            raise _ContractError("response_target_invalid")
        raw = response.read(_MAX_RETAINED_HTTP_BYTES + 1)
    finally:
        _close_http_response(response)
    if not isinstance(raw, bytes) or not raw or len(raw) > _MAX_RETAINED_HTTP_BYTES:
        raise _ContractError("response_size_invalid")
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        raise _ContractError("response_encoding_invalid") from None
    return {
        "payload": _json_loads(text),
        "sha256": hashlib.sha256(raw).hexdigest(),
        "bytes": len(raw),
    }


def _evidence_payload(value: object, reason_code: str) -> object:
    if not isinstance(value, Mapping):
        raise _ContractError(reason_code)
    payload = value.get("payload")
    digest = value.get("sha256")
    size = value.get("bytes")
    if (
        not isinstance(digest, str)
        or not _SHA256.fullmatch(digest)
        or type(size) is not int
        or not 0 < size <= _MAX_RETAINED_HTTP_BYTES
    ):
        raise _ContractError(reason_code)
    return payload


def _positive_decimal(value: object, reason_code: str) -> Decimal:
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


def _validate_okx_time(evidence: Mapping[str, object]) -> datetime:
    payload = _evidence_payload(evidence, "okx_time_evidence_invalid")
    if (
        not isinstance(payload, dict)
        or payload.get("code") != "0"
        or payload.get("msg") != ""
    ):
        raise _ContractError("okx_time_schema_invalid")
    rows = payload.get("data")
    if not isinstance(rows, list) or len(rows) != 1 or not isinstance(rows[0], dict):
        raise _ContractError("okx_time_schema_invalid")
    raw_ts = rows[0].get("ts")
    if not isinstance(raw_ts, str) or not _INTEGER_TEXT.fullmatch(raw_ts):
        raise _ContractError("okx_time_schema_invalid")
    try:
        observed = datetime.fromtimestamp(int(raw_ts) / 1000, tz=timezone.utc)
    except (OverflowError, OSError, ValueError):
        raise _ContractError("okx_time_invalid") from None
    return _require_utc(observed, "okx_time_invalid")


def _validate_okx_instruments(
    evidence: Mapping[str, object],
) -> dict[str, dict[str, str]]:
    payload = _evidence_payload(evidence, "okx_instruments_evidence_invalid")
    if (
        not isinstance(payload, dict)
        or payload.get("code") != "0"
        or payload.get("msg") != ""
    ):
        raise _ContractError("okx_instruments_schema_invalid")
    rows = payload.get("data")
    if not isinstance(rows, list) or not rows:
        raise _ContractError("okx_instruments_schema_invalid")

    seen_ids: set[str] = set()
    accepted: dict[str, dict[str, str]] = {}
    seen_bases: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            raise _ContractError("okx_instrument_row_invalid")
        market_id = row.get("instId")
        if (
            not isinstance(market_id, str)
            or market_id != market_id.strip()
            or not _ANY_SWAP_ID.fullmatch(market_id)
            or row.get("instType") != "SWAP"
            or not isinstance(row.get("state"), str)
        ):
            raise _ContractError("okx_instrument_identity_invalid")
        if market_id in seen_ids:
            raise _ContractError("okx_instrument_duplicate")
        seen_ids.add(market_id)
        matched = _STANDARD_MARKET_ID.fullmatch(market_id)
        if market_id.endswith("-USDT-SWAP") and matched is None:
            raise _ContractError("okx_instrument_identity_invalid")
        if matched is None or row["state"] != "live":
            continue
        base = matched.group("base")
        if (
            row.get("ctType") != "linear"
            or row.get("settleCcy") != "USDT"
            or row.get("ctValCcy") != base
        ):
            raise _ContractError("okx_instrument_unit_conflict")
        _positive_decimal(row.get("ctVal"), "okx_instrument_contract_size_invalid")
        if base in seen_bases:
            raise _ContractError("okx_instrument_base_conflict")
        seen_bases.add(base)
        accepted[market_id] = {
            "market_id": market_id,
            "base": base,
            "quote": "USDT",
            "settle": "USDT",
            "amount_unit": "contracts",
            "contract_size": row["ctVal"],
            "contract_size_unit": base,
        }
    if len(accepted) < _MIN_OPERATIONAL_MARKET_COUNT:
        raise _ContractError("okx_inventory_below_operational_floor")
    return dict(sorted(accepted.items()))


def _native_okx_snapshot() -> tuple[dict[str, dict[str, str]], dict[str, object]]:
    local_started = _require_utc(_utc_now(), "local_time_invalid")
    time_evidence = _fetch_json(_OKX_TIME_URL)
    instruments_evidence = _fetch_json(_OKX_INSTRUMENTS_URL)
    local_completed = _require_utc(_utc_now(), "local_time_invalid")
    if local_completed < local_started:
        raise _ContractError("local_time_regression")
    exchange_time = _validate_okx_time(time_evidence)
    if (
        exchange_time < local_started - timedelta(
            seconds=_MAX_EXCHANGE_CLOCK_SKEW_SECONDS,
        )
        or exchange_time > local_completed
    ):
        raise _ContractError("okx_exchange_time_outside_local_window")
    catalog = _validate_okx_instruments(instruments_evidence)
    source = {
        "status": "observed",
        "authority_key": _AUTHORITY_KEY,
        "dataset_key": _DATASET_KEY,
        "path_key": _NATIVE_PATH_KEY,
        "adapter_version": NATIVE_ADAPTER_VERSION,
        "local_started_at": _utc_iso(local_started),
        "local_completed_at": _utc_iso(local_completed),
        "exchange_time_at": _utc_iso(exchange_time),
        "observed_at": _utc_iso(local_completed),
        "market_count": len(catalog),
        "response_sha256": {
            "exchange_time": time_evidence["sha256"],
            "instruments": instruments_evidence["sha256"],
        },
        "response_bytes": {
            "exchange_time": time_evidence["bytes"],
            "instruments": instruments_evidence["bytes"],
        },
    }
    return catalog, source


def _read_cache_bytes() -> bytes:
    """Read no more than the cache contract allows, plus one rejection byte."""
    try:
        with _CACHE.open("rb") as handle:
            return handle.read(_MAX_CACHE_BYTES + 1)
    except FileNotFoundError:
        raise
    except OSError:
        raise


def _previous_inventory_counts(
    *,
    publish_now: datetime | None = None,
) -> tuple[int, int] | None:
    """Recover one bounded-age baseline for both inventory retention gates.

    Freshness controls whether scanners may consume a snapshot.  It must not disable
    the independent completeness alarms on the next refresh.  Baselines older than
    14 days are no longer comparable to the current market regime and are ignored.
    """
    try:
        raw = _read_cache_bytes()
        if not raw or len(raw) > _MAX_CACHE_BYTES:
            return None
        payload = _json_loads(raw.decode("utf-8"))
        if not isinstance(payload, dict):
            return None
        generated_at = _parse_utc_iso(
            payload.get("generated_at"), "cache_generated_at_invalid",
        )
        now = _require_utc(
            publish_now if publish_now is not None else _utc_now(),
            "local_time_invalid",
        )
        if (
            generated_at > now
            or now - generated_at > _MAX_OPERATIONAL_BASELINE_AGE
        ):
            return None
        validated = _validate_cache_payload(payload, generated_at)
        market_count = validated.get("market_count")
        mapped_count = validated.get("mapped_count")
        if type(market_count) is int and type(mapped_count) is int:
            return market_count, mapped_count
        return None
    except (OSError, UnicodeDecodeError, _ContractError):
        return None
    except Exception as exc:
        logger.warning(
            "perp_universe_baseline_validation_failed",
            reason_code="cache_validation_failed",
            error_kind=type(exc).__name__,
        )
        return None


def _reject_operational_inventory_drop(
    current_count: int,
    previous_count: int | None,
) -> None:
    if (
        type(previous_count) is int
        and current_count * _MAX_OPERATIONAL_DROP_DENOMINATOR
        < previous_count * _MAX_OPERATIONAL_DROP_NUMERATOR
    ):
        raise _ContractError("okx_inventory_operational_drop")


def _reject_mapping_completeness(
    market_count: int,
    mapped_count: int,
    previous_count: int | None,
) -> None:
    if (
        mapped_count * _MIN_MAPPING_COVERAGE_DENOMINATOR
        < market_count * _MIN_MAPPING_COVERAGE_NUMERATOR
    ):
        raise _ContractError("coingecko_mapping_coverage_below_floor")
    if (
        type(previous_count) is int
        and mapped_count * _MIN_MAPPING_RETENTION_DENOMINATOR
        < previous_count * _MIN_MAPPING_RETENTION_NUMERATOR
    ):
        raise _ContractError("coingecko_mapping_operational_drop")


def _platform_to_hit(platforms: object) -> dict[str, str] | None:
    if not isinstance(platforms, dict):
        raise _ContractError("coingecko_platforms_invalid")
    for platform_id, chain in _CHAIN_PRIORITY:
        address = platforms.get(platform_id)
        if address in (None, ""):
            continue
        if (
            not isinstance(address, str)
            or address != address.strip()
            or not address
        ):
            raise _ContractError("coingecko_contract_address_invalid")
        return {
            "chain": chain,
            "address": address if chain == "solana" else address.lower(),
            "platform_id": platform_id,
        }
    return None


def _coingecko_research_mapping(
    market_catalog: Mapping[str, Mapping[str, str]],
) -> tuple[dict[str, dict[str, object]], dict[str, object]]:
    list_evidence = _fetch_json(_COINGECKO_LIST_URL)
    list_payload = _evidence_payload(list_evidence, "coingecko_list_evidence_invalid")
    if not isinstance(list_payload, list) or not list_payload:
        raise _ContractError("coingecko_list_schema_invalid")
    platforms_by_id: dict[str, dict[str, object]] = {}
    symbols_by_id: dict[str, str] = {}
    seen_list_ids: set[str] = set()
    excluded_list_rows = 0
    for row in list_payload:
        if not isinstance(row, dict):
            raise _ContractError("coingecko_list_row_invalid")
        coin_id = row.get("id")
        symbol = row.get("symbol")
        platforms = row.get("platforms")
        if (
            not isinstance(coin_id, str)
            or not coin_id
            or coin_id != coin_id.strip()
            or not isinstance(symbol, str)
            or not isinstance(platforms, dict)
        ):
            raise _ContractError("coingecko_list_row_invalid")
        if coin_id in seen_list_ids:
            raise _ContractError("coingecko_id_duplicate")
        seen_list_ids.add(coin_id)
        # CoinGecko's live catalog contains a tiny number of rows whose official
        # symbol is empty.  They cannot participate in a ticker join, but must not
        # make the other 17k rows unavailable.  Count and persist the exclusion so
        # the research coverage denominator remains auditable.
        if not symbol or symbol != symbol.strip():
            excluded_list_rows += 1
            continue
        platforms_by_id[coin_id] = platforms
        symbols_by_id[coin_id] = symbol.upper()

    ranked_ids: dict[str, list[str]] = defaultdict(list)
    ranked_seen: set[str] = set()
    page_evidence: list[dict[str, object]] = []
    for page in range(1, _COINGECKO_MARKET_PAGE_COUNT + 1):
        evidence = _fetch_json(_COINGECKO_MARKETS_URL.format(page=page))
        payload = _evidence_payload(evidence, "coingecko_markets_evidence_invalid")
        if not isinstance(payload, list):
            raise _ContractError("coingecko_markets_schema_invalid")
        if len(payload) != _COINGECKO_MARKET_PAGE_SIZE:
            raise _ContractError("coingecko_markets_page_size_invalid")
        for row in payload:
            if not isinstance(row, dict):
                raise _ContractError("coingecko_markets_row_invalid")
            coin_id = row.get("id")
            symbol = row.get("symbol")
            if (
                not isinstance(coin_id, str)
                or not coin_id
                or coin_id != coin_id.strip()
                or not isinstance(symbol, str)
                or not symbol
                or symbol != symbol.strip()
            ):
                raise _ContractError("coingecko_markets_row_invalid")
            if coin_id in ranked_seen:
                raise _ContractError("coingecko_markets_id_duplicate")
            if coin_id not in platforms_by_id or symbols_by_id[coin_id] != symbol.upper():
                raise _ContractError("coingecko_catalog_conflict")
            ranked_seen.add(coin_id)
            ranked_ids[symbol.upper()].append(coin_id)
        page_evidence.append({
            "page": page,
            "sha256": evidence["sha256"],
            "bytes": evidence["bytes"],
            "row_count": len(payload),
        })
    if len(ranked_seen) != _COINGECKO_MARKET_EXPECTED_UNIQUE_IDS:
        raise _ContractError("coingecko_markets_unique_count_invalid")

    universe: dict[str, dict[str, object]] = {}
    for market_id, market in sorted(market_catalog.items()):
        base = market["base"]
        for rank, coin_id in enumerate(ranked_ids.get(base, ()), start=1):
            hit = _platform_to_hit(platforms_by_id[coin_id])
            if hit is None:
                continue
            universe[base] = {
                "chain": hit["chain"],
                "address": hit["address"],
                "market_id": market_id,
                "contract_size": market["contract_size"],
                "contract_size_unit": market["contract_size_unit"],
                "coingecko_id": coin_id,
                "coingecko_market_cap_rank_in_symbol": rank,
                "coingecko_platform_id": hit["platform_id"],
                "mapping_method": MAPPING_METHOD,
                "actionability": RESEARCH_ACTIONABILITY,
                "reason_codes": ["ticker_identity_not_independently_verified"],
            }
            break
    if not universe:
        raise _ContractError("coingecko_mapping_empty")
    mapping_source = {
        "status": "observed",
        "provider_key": "provider:coingecko",
        "adapter_version": COINGECKO_ADAPTER_VERSION,
        "observed_at": _utc_iso(_require_utc(_utc_now(), "local_time_invalid")),
        "list_sha256": list_evidence["sha256"],
        "list_bytes": list_evidence["bytes"],
        "list_row_count": len(list_payload),
        "list_usable_row_count": len(platforms_by_id),
        "list_excluded_row_count": excluded_list_rows,
        "market_unique_id_count": len(ranked_seen),
        "market_pages": page_evidence,
    }
    return dict(sorted(universe.items())), mapping_source


def _ccxt_shadow_snapshot() -> dict[str, object]:
    from src.collectors.ccxt_public_rest import fetch_public_snapshot

    return fetch_public_snapshot(exchange="okx", market_type="swap")


def _validated_sha_size(value: object, reason_code: str) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise _ContractError(reason_code)
    digest = value.get("sha256")
    size = value.get("bytes")
    if (
        not isinstance(digest, str)
        or not _SHA256.fullmatch(digest)
        or type(size) is not int
        or size <= 0
    ):
        raise _ContractError(reason_code)
    return {"sha256": digest, "bytes": size}


def _validate_ccxt_shadow(
    snapshot: object,
    native_catalog: Mapping[str, Mapping[str, str]],
) -> dict[str, object]:
    if not isinstance(snapshot, Mapping) or snapshot.get("status") != "observed":
        raise _ContractError("ccxt_shadow_unavailable")
    identity = snapshot.get("source_identity")
    if (
        not isinstance(identity, Mapping)
        or identity.get("authority_key") != _AUTHORITY_KEY
        or identity.get("dataset_key") != _DATASET_KEY
        or not isinstance(identity.get("path_key"), str)
        or not identity["path_key"].startswith("ccxt-rest:")
    ):
        raise _ContractError("ccxt_shadow_identity_invalid")
    adapter = snapshot.get("adapter")
    if (
        snapshot.get("schema_version") != 1
        or snapshot.get("exchange") != "okx"
        or snapshot.get("market_type") != "swap"
        or snapshot.get("mode") != "shadow"
        or not isinstance(adapter, Mapping)
        or adapter.get("name") != "ccxt"
        or not isinstance(adapter.get("version"), str)
        or not _SEMVER.fullmatch(adapter["version"])
        or identity["path_key"] != f"ccxt-rest:{adapter['version']}"
        or adapter.get("exchange_class") != "okx"
        or adapter.get("transport") != "public_rest"
    ):
        raise _ContractError("ccxt_shadow_identity_invalid")
    rows = snapshot.get("markets")
    if not isinstance(rows, list) or not rows:
        raise _ContractError("ccxt_shadow_inventory_invalid")
    shadow_catalog: dict[str, dict[str, str]] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            raise _ContractError("ccxt_shadow_inventory_invalid")
        market_id = row.get("market_id")
        matched = (
            _STANDARD_MARKET_ID.fullmatch(market_id)
            if isinstance(market_id, str) else None
        )
        if matched is None or market_id in shadow_catalog:
            raise _ContractError("ccxt_shadow_inventory_invalid")
        base = matched.group("base")
        if (
            row.get("base") != base
            or row.get("quote") != "USDT"
            or row.get("settle") != "USDT"
            or row.get("active") is not True
            or row.get("linear") is not True
            or row.get("amount_unit") != "contracts"
            or row.get("contract_size_unit") != base
        ):
            raise _ContractError("ccxt_shadow_unit_conflict")
        size = row.get("contract_size")
        _positive_decimal(size, "ccxt_shadow_unit_conflict")
        shadow_catalog[market_id] = {
            "market_id": market_id,
            "base": base,
            "quote": "USDT",
            "settle": "USDT",
            "amount_unit": "contracts",
            "contract_size": size,
            "contract_size_unit": base,
        }
    if set(shadow_catalog) != set(native_catalog):
        raise _ContractError("ccxt_shadow_inventory_conflict")
    for market_id, native in native_catalog.items():
        shadow = shadow_catalog[market_id]
        if (
            shadow["base"] != native["base"]
            or shadow["contract_size_unit"] != native["contract_size_unit"]
            or shadow["contract_size"] != native["contract_size"]
        ):
            raise _ContractError("ccxt_shadow_unit_conflict")

    exchange_time = snapshot.get("exchange_time")
    timing = snapshot.get("request_timing")
    if not isinstance(exchange_time, Mapping) or not isinstance(timing, Mapping):
        raise _ContractError("ccxt_shadow_time_invalid")
    server_ms = exchange_time.get("server_time_ms")
    probe_started_ms = exchange_time.get("probe_started_at_ms")
    probe_completed_ms = exchange_time.get("probe_completed_at_ms")
    markets_started_ms = timing.get("markets_started_at_ms")
    observed_ms = timing.get("markets_completed_at_ms")
    if (
        type(server_ms) is not int
        or type(probe_started_ms) is not int
        or type(probe_completed_ms) is not int
        or type(markets_started_ms) is not int
        or type(observed_ms) is not int
        or server_ms < 0
        or not (
            probe_started_ms
            <= probe_completed_ms
            <= markets_started_ms
            <= observed_ms
        )
        or server_ms < probe_started_ms - (_MAX_EXCHANGE_CLOCK_SKEW_SECONDS * 1000)
        or server_ms > probe_completed_ms
        or markets_started_ms - probe_completed_ms
        > _MAX_EXCHANGE_CLOCK_SKEW_SECONDS * 1000
    ):
        raise _ContractError("ccxt_shadow_time_invalid")
    try:
        exchange_at = datetime.fromtimestamp(server_ms / 1000, tz=timezone.utc)
        observed_at = datetime.fromtimestamp(observed_ms / 1000, tz=timezone.utc)
    except (OverflowError, OSError, ValueError):
        raise _ContractError("ccxt_shadow_time_invalid") from None
    raw = snapshot.get("raw_responses")
    if not isinstance(raw, Mapping):
        raise _ContractError("ccxt_shadow_evidence_invalid")
    time_raw = _validated_sha_size(
        raw.get("exchange_time"), "ccxt_shadow_evidence_invalid",
    )
    markets_raw = _validated_sha_size(
        raw.get("markets"), "ccxt_shadow_evidence_invalid",
    )
    return {
        "status": "observed",
        "authority_key": _AUTHORITY_KEY,
        "dataset_key": _DATASET_KEY,
        "path_key": identity["path_key"],
        "adapter_version": adapter["version"],
        "exchange_time_at": _utc_iso(exchange_at),
        "observed_at": _utc_iso(observed_at),
        "local_started_at": _utc_iso(datetime.fromtimestamp(
            probe_started_ms / 1000, tz=timezone.utc,
        )),
        "local_completed_at": _utc_iso(observed_at),
        "market_count": len(shadow_catalog),
        "response_sha256": {
            "exchange_time": time_raw["sha256"],
            "instruments": markets_raw["sha256"],
        },
        "response_bytes": {
            "exchange_time": time_raw["bytes"],
            "instruments": markets_raw["bytes"],
        },
    }


def _source_health(sources: list[dict[str, object]]) -> dict[str, object]:
    authorities = {source["authority_key"] for source in sources}
    paths = {source["path_key"] for source in sources}
    return {
        "state": "ok",
        "inventory_conflict": False,
        "independence_basis": "authority_deduplicated_v1",
        "independent_source_count": len(authorities),
        "observed_path_count": len(paths),
        "sources": sources,
    }


def _cache_digest(envelope: Mapping[str, object]) -> str:
    unsigned = {key: value for key, value in envelope.items() if key != "cache_digest"}
    return _sha256(unsigned)


def _contains_forbidden_cache_key(value: object) -> bool:
    if isinstance(value, dict):
        return any(
            key in _FORBIDDEN_CACHE_KEYS or _contains_forbidden_cache_key(item)
            for key, item in value.items()
        )
    if isinstance(value, list):
        return any(_contains_forbidden_cache_key(item) for item in value)
    return False


def _atomic_write_cache(envelope: Mapping[str, object]) -> _CachePublishState:
    if _contains_forbidden_cache_key(envelope):
        raise _ContractError("cache_contains_raw_response")
    encoded = _canonical_bytes(envelope) + b"\n"
    if len(encoded) > _MAX_CACHE_BYTES:
        raise _ContractError("cache_size_invalid")
    temporary: str | None = None
    raw_descriptor: int | None = None
    state = _CachePublishState(
        namespace_replaced=False,
        directory_synced=False,
    )
    try:
        _CACHE.parent.mkdir(parents=True, exist_ok=True)
        raw_descriptor, temporary = tempfile.mkstemp(
            prefix=f".{_CACHE.name}.",
            suffix=".tmp",
            dir=_CACHE.parent,
        )
        # Until fdopen returns successfully the raw descriptor is still ours.  Keep
        # that ownership explicit so a constructor failure cannot leak an FD.
        handle = os.fdopen(raw_descriptor, "wb")
        raw_descriptor = None
        with handle:
            if handle.write(encoded) != len(encoded):
                raise OSError("short cache write")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, _CACHE)
        state = _CachePublishState(
            namespace_replaced=True,
            directory_synced=False,
        )
        directory_fd = os.open(str(_CACHE.parent), os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        except Exception:
            # Preserve the directory-fsync failure as the durability verdict.  A
            # secondary close failure must neither mask it nor claim the rename is
            # durable.
            try:
                os.close(directory_fd)
            except Exception:
                pass
            raise
        state = _CachePublishState(
            namespace_replaced=True,
            directory_synced=True,
        )
        try:
            os.close(directory_fd)
        except Exception as exc:
            # The parent-directory fsync is the commit point.  A later close error
            # is a cleanup warning, not evidence that the acknowledged rename lost
            # durability.  Never include exception text: it can contain paths.
            try:
                logger.warning(
                    "perp_universe_cache_directory_close_failed",
                    reason_code="cache_directory_close_failed_after_sync",
                    error_kind=type(exc).__name__,
                )
            except Exception:
                pass
        return state
    except Exception as exc:
        # A failed fdopen never transferred ownership.  Close the raw descriptor
        # before unlinking its directory entry; cleanup failures are secondary and
        # must never replace the staged publication verdict.
        try:
            if raw_descriptor is not None:
                os.close(raw_descriptor)
        except Exception:
            pass
        try:
            if temporary is not None:
                os.unlink(temporary)
        except FileNotFoundError:
            pass
        except OSError:
            pass
        if isinstance(exc, _ContractError):
            raise
        raise _CachePublishError(state) from None


def _failure(
    status: str,
    reason_code: str,
    *,
    cache_preserved: bool | None = None,
) -> dict[str, object]:
    if cache_preserved is None:
        try:
            cache_preserved = _CACHE.exists()
        except OSError:
            cache_preserved = False
    return {
        "schema_version": CACHE_SCHEMA_VERSION,
        "status": status,
        "reason_codes": [reason_code],
        "cache_path": _CACHE.name,
        "cache_preserved": cache_preserved,
        "universe": {},
        "research_universe": {},
        "actionable_universe": {},
    }


def _require_disk_write() -> dict[str, object]:
    snapshot = stream_disk_guard.GUARD.require_evidence_write("perp_universe")
    if isinstance(snapshot, Mapping) and snapshot.get("state") == "critical":
        raise stream_disk_guard.StreamDiskCritical("perp_universe", dict(snapshot))
    return dict(snapshot)


def refresh_result() -> dict[str, object]:
    """Refresh the latest research envelope without promoting heuristic mappings."""
    try:
        # This first guard is intentionally before every HTTP call and filesystem
        # mutation.  A positively observed CRITICAL disk state means zero collection.
        _require_disk_write()
    except stream_disk_guard.StreamDiskCritical:
        return _failure("blocked", "disk_critical")
    except Exception as exc:
        logger.warning(
            "perp_universe_disk_guard_failed",
            reason_code="disk_guard_failed",
            error_kind=type(exc).__name__,
        )
        return _failure("unavailable", "disk_guard_failed")

    mode = os.environ.get(CCXT_MODE_ENV, "off")
    if mode not in {"off", "shadow"}:
        return _failure("invalid", "ccxt_mode_invalid")
    try:
        market_catalog, native_source = _native_okx_snapshot()
        baseline_counts = _previous_inventory_counts()
        previous_market_count, previous_mapped_count = (
            baseline_counts if baseline_counts is not None else (None, None)
        )
        _reject_operational_inventory_drop(
            len(market_catalog), previous_market_count,
        )
        sources = [native_source]
        if mode == "shadow":
            sources.append(_validate_ccxt_shadow(
                _ccxt_shadow_snapshot(), market_catalog,
            ))
        universe, mapping_source = _coingecko_research_mapping(market_catalog)
        _reject_mapping_completeness(
            len(market_catalog), len(universe), previous_mapped_count,
        )
        generated_at = _require_utc(_utc_now(), "local_time_invalid")
        observed_times = [
            _parse_utc_iso(source["observed_at"], "source_time_invalid")
            for source in sources
        ]
        observed_times.append(_parse_utc_iso(
            mapping_source["observed_at"], "mapping_source_time_invalid",
        ))
        if any(observed > generated_at for observed in observed_times):
            raise _ContractError("source_time_in_future")
        source_health = _source_health(sources)
        if (
            source_health["independent_source_count"] != 1
            or source_health["observed_path_count"] != len(sources)
        ):
            raise _ContractError("source_count_invalid")
        envelope: dict[str, object] = {
            "schema_version": CACHE_SCHEMA_VERSION,
            "generated_at": _utc_iso(generated_at),
            "expires_at": _utc_iso(
                generated_at + timedelta(seconds=CACHE_TTL_SECONDS),
            ),
            "source_health": source_health,
            "mapping_source": mapping_source,
            "mapping_policy": {
                "mapping_method": MAPPING_METHOD,
                "default_actionability": RESEARCH_ACTIONABILITY,
                "reason_code": "ticker_identity_not_independently_verified",
            },
            "market_catalog_digest": _sha256(market_catalog),
            "market_count": len(market_catalog),
            "mapped_count": len(universe),
            "universe": universe,
        }
        envelope["cache_digest"] = _cache_digest(envelope)
        # Validate the exact persisted contract before it can replace a known-good
        # latest snapshot.  This also catches implementation drift between refresh
        # and load without briefly publishing an unreadable cache.
        _validate_cache_payload(envelope, generated_at)
        # Recheck immediately before the temp-file write.  A transition to CRITICAL
        # may spend the completed HTTP work, but never writes into the pressured disk.
        _require_disk_write()
        publish_state = _atomic_write_cache(envelope)
        if not (
            publish_state.namespace_replaced
            and publish_state.directory_synced
        ):
            raise _CachePublishError(publish_state)
        loaded = load_result(_now=generated_at)
        loaded["refresh_status"] = "written"
        return loaded
    except stream_disk_guard.StreamDiskCritical:
        return _failure("blocked", "disk_critical_before_write")
    except _CachePublishError as exc:
        logger.warning(
            "perp_universe_cache_publish_failed",
            reason_code=exc.reason_code,
            error_kind=type(exc).__name__,
        )
        return _failure(
            "unavailable",
            exc.reason_code,
            cache_preserved=(
                False if exc.state.namespace_replaced else None
            ),
        )
    except _ContractError as exc:
        logger.warning(
            "perp_universe_refresh_rejected",
            reason_code=exc.reason_code,
            error_kind=type(exc).__name__,
        )
        return _failure("invalid", exc.reason_code)
    except Exception as exc:
        logger.warning(
            "perp_universe_refresh_failed",
            reason_code="source_unavailable",
            error_kind=type(exc).__name__,
        )
        return _failure("unavailable", "source_unavailable")


def _validate_source_health(
    value: object,
    *,
    generated_at: datetime,
) -> list[dict[str, object]]:
    if (
        not isinstance(value, dict)
        or set(value) != {
            "state",
            "inventory_conflict",
            "independence_basis",
            "independent_source_count",
            "observed_path_count",
            "sources",
        }
    ):
        raise _ContractError("source_health_invalid")
    if value.get("state") != "ok":
        raise _ContractError("source_unavailable")
    if value.get("inventory_conflict") is not False:
        raise _ContractError("inventory_conflict")
    if value.get("independence_basis") != "authority_deduplicated_v1":
        raise _ContractError("source_identity_invalid")
    independent = value.get("independent_source_count")
    path_count = value.get("observed_path_count")
    sources = value.get("sources")
    if (
        type(independent) is not int
        or independent != 1
        or type(path_count) is not int
        or path_count not in {1, 2}
        or not isinstance(sources, list)
        or len(sources) != path_count
    ):
        raise _ContractError("source_count_invalid")
    paths: set[str] = set()
    validated_sources: list[dict[str, object]] = []
    for source in sources:
        if (
            not isinstance(source, dict)
            or set(source) != {
                "status",
                "authority_key",
                "dataset_key",
                "path_key",
                "adapter_version",
                "local_started_at",
                "local_completed_at",
                "exchange_time_at",
                "observed_at",
                "market_count",
                "response_sha256",
                "response_bytes",
            }
            or source.get("status") != "observed"
        ):
            raise _ContractError("source_unavailable")
        path = source.get("path_key")
        if (
            source.get("authority_key") != _AUTHORITY_KEY
            or source.get("dataset_key") != _DATASET_KEY
            or not isinstance(path, str)
            or not path
            or path in paths
            or type(source.get("market_count")) is not int
            or source["market_count"] <= 0
        ):
            raise _ContractError("source_identity_invalid")
        if path == _NATIVE_PATH_KEY:
            if source.get("adapter_version") != NATIVE_ADAPTER_VERSION:
                raise _ContractError("source_identity_invalid")
        elif path.startswith("ccxt-rest:"):
            version = source.get("adapter_version")
            if (
                not isinstance(version, str)
                or not _SEMVER.fullmatch(version)
                or path != f"ccxt-rest:{version}"
            ):
                raise _ContractError("source_identity_invalid")
        else:
            raise _ContractError("source_identity_invalid")
        paths.add(path)
        local_started = _parse_utc_iso(
            source.get("local_started_at"), "source_time_invalid",
        )
        local_completed = _parse_utc_iso(
            source.get("local_completed_at"), "source_time_invalid",
        )
        exchange_time = _parse_utc_iso(
            source.get("exchange_time_at"), "source_time_invalid",
        )
        observed_at = _parse_utc_iso(
            source.get("observed_at"), "source_time_invalid",
        )
        if not (
            local_started <= local_completed == observed_at <= generated_at
            and local_completed - local_started
            <= timedelta(seconds=_MAX_REFRESH_DURATION_SECONDS)
            and exchange_time
            >= local_started - timedelta(seconds=_MAX_EXCHANGE_CLOCK_SKEW_SECONDS)
            and exchange_time <= observed_at
            and generated_at - observed_at
            <= timedelta(seconds=_MAX_REFRESH_DURATION_SECONDS)
        ):
            raise _ContractError("source_time_invalid")
        response_sha = source.get("response_sha256")
        response_bytes = source.get("response_bytes")
        if (
            not isinstance(response_sha, dict)
            or not isinstance(response_bytes, dict)
            or set(response_sha) != {"exchange_time", "instruments"}
            or set(response_bytes) != {"exchange_time", "instruments"}
            or not all(
                isinstance(digest, str) and _SHA256.fullmatch(digest)
                for digest in response_sha.values()
            )
            or not all(
                type(size) is int and 0 < size <= _MAX_RETAINED_HTTP_BYTES
                for size in response_bytes.values()
            )
        ):
            raise _ContractError("source_evidence_invalid")
        validated_sources.append(dict(source))
    if _NATIVE_PATH_KEY not in paths:
        raise _ContractError("native_source_missing")
    if len(paths) == 2 and not any(path.startswith("ccxt-rest:") for path in paths):
        raise _ContractError("source_identity_invalid")
    return validated_sources


def _validate_mapping_source(value: object, *, generated_at: datetime) -> None:
    if (
        not isinstance(value, dict)
        or set(value) != {
            "status",
            "provider_key",
            "adapter_version",
            "observed_at",
            "list_sha256",
            "list_bytes",
            "list_row_count",
            "list_usable_row_count",
            "list_excluded_row_count",
            "market_unique_id_count",
            "market_pages",
        }
        or value.get("status") != "observed"
        or value.get("provider_key") != "provider:coingecko"
        or type(value.get("adapter_version")) is not int
        or value["adapter_version"] != COINGECKO_ADAPTER_VERSION
        or not isinstance(value.get("list_sha256"), str)
        or not _SHA256.fullmatch(value["list_sha256"])
        or type(value.get("list_bytes")) is not int
        or not 0 < value["list_bytes"] <= _MAX_RETAINED_HTTP_BYTES
        or type(value.get("list_row_count")) is not int
        or value["list_row_count"] <= 0
        or type(value.get("list_usable_row_count")) is not int
        or value["list_usable_row_count"] <= 0
        or type(value.get("list_excluded_row_count")) is not int
        or value["list_excluded_row_count"] < 0
        or value["list_row_count"]
        != value["list_usable_row_count"] + value["list_excluded_row_count"]
        or type(value.get("market_unique_id_count")) is not int
        or value["market_unique_id_count"]
        != _COINGECKO_MARKET_EXPECTED_UNIQUE_IDS
        or value["list_usable_row_count"] < value["market_unique_id_count"]
    ):
        raise _ContractError("mapping_source_unavailable")
    observed = _parse_utc_iso(
        value.get("observed_at"), "mapping_source_time_invalid",
    )
    if (
        observed > generated_at
        or generated_at - observed
        > timedelta(seconds=_MAX_REFRESH_DURATION_SECONDS)
    ):
        raise _ContractError("mapping_source_time_invalid")
    pages = value.get("market_pages")
    if (
        not isinstance(pages, list)
        or len(pages) != _COINGECKO_MARKET_PAGE_COUNT
    ):
        raise _ContractError("mapping_source_evidence_invalid")
    for expected_page, page in enumerate(pages, start=1):
        if (
            not isinstance(page, dict)
            or set(page) != {"page", "sha256", "bytes", "row_count"}
            or type(page.get("page")) is not int
            or page["page"] != expected_page
            or not isinstance(page.get("sha256"), str)
            or not _SHA256.fullmatch(page["sha256"])
            or type(page.get("bytes")) is not int
            or not 0 < page["bytes"] <= _MAX_RETAINED_HTTP_BYTES
            or type(page.get("row_count")) is not int
            or page["row_count"] != _COINGECKO_MARKET_PAGE_SIZE
        ):
            raise _ContractError("mapping_source_evidence_invalid")


def _validate_universe(value: object) -> dict[str, dict[str, object]]:
    if not isinstance(value, dict) or not value:
        raise _ContractError("universe_invalid")
    validated: dict[str, dict[str, object]] = {}
    for symbol, row in value.items():
        if (
            not isinstance(symbol, str)
            or not _SYMBOL.fullmatch(symbol)
            or not isinstance(row, dict)
            or set(row) != {
                "chain",
                "address",
                "market_id",
                "contract_size",
                "contract_size_unit",
                "coingecko_id",
                "coingecko_market_cap_rank_in_symbol",
                "coingecko_platform_id",
                "mapping_method",
                "actionability",
                "reason_codes",
            }
            or row.get("mapping_method") != MAPPING_METHOD
            or row.get("actionability") != RESEARCH_ACTIONABILITY
            or row.get("reason_codes")
            != ["ticker_identity_not_independently_verified"]
        ):
            raise _ContractError("universe_mapping_contract_invalid")
        market_id = row.get("market_id")
        matched = (
            _STANDARD_MARKET_ID.fullmatch(market_id)
            if isinstance(market_id, str) else None
        )
        if (
            matched is None
            or matched.group("base") != symbol
            or row.get("contract_size_unit") != symbol
            or not isinstance(row.get("chain"), str)
            or not row["chain"]
            or not isinstance(row.get("address"), str)
            or not row["address"]
            or not isinstance(row.get("coingecko_id"), str)
            or not row["coingecko_id"]
            or type(row.get("coingecko_market_cap_rank_in_symbol")) is not int
            or row["coingecko_market_cap_rank_in_symbol"] <= 0
            or not isinstance(row.get("coingecko_platform_id"), str)
            or not row["coingecko_platform_id"]
        ):
            raise _ContractError("universe_mapping_contract_invalid")
        _positive_decimal(
            row.get("contract_size"), "universe_contract_size_invalid",
        )
        validated[symbol] = dict(row)
    return dict(sorted(validated.items()))


def _validate_cache_payload(
    payload: object,
    now: datetime,
) -> dict[str, object]:
    if (
        not isinstance(payload, dict)
        or type(payload.get("schema_version")) is not int
        or payload.get("schema_version") != CACHE_SCHEMA_VERSION
    ):
        raise _ContractError("legacy_cache_untrusted")
    if set(payload) != {
        "schema_version",
        "generated_at",
        "expires_at",
        "source_health",
        "mapping_source",
        "mapping_policy",
        "market_catalog_digest",
        "market_count",
        "mapped_count",
        "universe",
        "cache_digest",
    }:
        raise _ContractError("cache_schema_invalid")
    if _contains_forbidden_cache_key(payload):
        raise _ContractError("cache_contains_raw_response")
    digest = payload.get("cache_digest")
    if not isinstance(digest, str) or not _SHA256.fullmatch(digest):
        raise _ContractError("cache_digest_invalid")
    if digest != _cache_digest(payload):
        raise _ContractError("cache_digest_mismatch")
    generated_at = _parse_utc_iso(
        payload.get("generated_at"), "cache_generated_at_invalid",
    )
    expires_at = _parse_utc_iso(
        payload.get("expires_at"), "cache_expires_at_invalid",
    )
    if generated_at > now:
        raise _ContractError("cache_generated_in_future")
    if expires_at != generated_at + timedelta(seconds=CACHE_TTL_SECONDS):
        raise _ContractError("cache_ttl_invalid")
    if now >= expires_at:
        raise _ContractError("cache_stale")
    market_count = payload.get("market_count")
    mapped_count = payload.get("mapped_count")
    if (
        type(market_count) is not int
        or market_count <= 0
        or type(mapped_count) is not int
        or mapped_count <= 0
        or mapped_count > market_count
        or not isinstance(payload.get("market_catalog_digest"), str)
        or not _SHA256.fullmatch(payload["market_catalog_digest"])
    ):
        raise _ContractError("market_catalog_evidence_invalid")
    if market_count < _MIN_OPERATIONAL_MARKET_COUNT:
        raise _ContractError("okx_inventory_below_operational_floor")
    if (
        mapped_count * _MIN_MAPPING_COVERAGE_DENOMINATOR
        < market_count * _MIN_MAPPING_COVERAGE_NUMERATOR
    ):
        raise _ContractError("coingecko_mapping_coverage_below_floor")
    sources = _validate_source_health(
        payload.get("source_health"), generated_at=generated_at,
    )
    if any(source["market_count"] != market_count for source in sources):
        raise _ContractError("inventory_conflict")
    _validate_mapping_source(
        payload.get("mapping_source"), generated_at=generated_at,
    )
    if payload.get("mapping_policy") != {
        "mapping_method": MAPPING_METHOD,
        "default_actionability": RESEARCH_ACTIONABILITY,
        "reason_code": "ticker_identity_not_independently_verified",
    }:
        raise _ContractError("mapping_policy_invalid")
    universe = _validate_universe(payload.get("universe"))
    if mapped_count != len(universe):
        raise _ContractError("mapped_count_invalid")
    source_health = payload["source_health"]
    result = dict(payload)
    result.update({
        "status": "research_only",
        "reason_codes": ["heuristic_mapping_not_actionable"],
        "research_universe": universe,
        "actionable_universe": {},
        "source_counts": {
            "independent_source_count": source_health["independent_source_count"],
            "observed_path_count": source_health["observed_path_count"],
        },
    })
    return result


def load_result(*, _now: datetime | None = None) -> dict[str, object]:
    """Load and validate the current research cache with explicit exclusion reasons."""
    try:
        now = _require_utc(
            _now if _now is not None else _utc_now(), "local_time_invalid",
        )
    except _ContractError as exc:
        return _failure("invalid", exc.reason_code)
    except Exception as exc:
        logger.warning(
            "perp_universe_cache_validation_failed",
            reason_code="cache_validation_failed",
            error_kind=type(exc).__name__,
        )
        return _failure("invalid", "cache_validation_failed")
    try:
        raw = _read_cache_bytes()
    except FileNotFoundError:
        return _failure("unavailable", "cache_missing")
    except OSError:
        return _failure("unavailable", "cache_read_failed")
    if not raw or len(raw) > _MAX_CACHE_BYTES:
        return _failure("invalid", "cache_size_invalid")
    try:
        payload = _json_loads(raw.decode("utf-8"))
    except UnicodeDecodeError:
        return _failure("invalid", "cache_encoding_invalid")
    except _ContractError as exc:
        return _failure("invalid", exc.reason_code)
    except Exception as exc:
        logger.warning(
            "perp_universe_cache_validation_failed",
            reason_code="cache_validation_failed",
            error_kind=type(exc).__name__,
        )
        return _failure("invalid", "cache_validation_failed")
    try:
        return _validate_cache_payload(payload, now)
    except _ContractError as exc:
        status = "stale" if exc.reason_code == "cache_stale" else "invalid"
        return _failure(status, exc.reason_code)
    except Exception as exc:
        logger.warning(
            "perp_universe_cache_validation_failed",
            reason_code="cache_validation_failed",
            error_kind=type(exc).__name__,
        )
        return _failure("invalid", "cache_validation_failed")


def refresh() -> dict:
    """Compatibility wrapper returning only verified actionable mappings."""
    result = refresh_result()
    actionable = result.get("actionable_universe")
    return dict(actionable) if isinstance(actionable, dict) else {}


def load() -> dict:
    """Return only ``actionability=verified`` rows; research heuristics stay hidden."""
    result = load_result()
    actionable = result.get("actionable_universe")
    return dict(actionable) if isinstance(actionable, dict) else {}


def by_chain() -> dict[str, list[dict[str, str]]]:
    """Compatibility grouping over the verified actionable universe only."""
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for symbol, row in load().items():
        grouped[row["chain"]].append({
            "symbol": symbol,
            "address": row["address"],
        })
    return dict(grouped)
