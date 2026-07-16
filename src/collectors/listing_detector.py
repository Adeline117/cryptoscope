"""Exchange new-listing detector.

Polls official Binance, OKX, Bybit, and Coinbase instrument endpoints every cycle, compares
against the previous snapshot, and emits alerts for newly listed trading pairs.
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
import structlog

from src.config import DATA_DIR

logger = structlog.get_logger()

SNAPSHOT_DIR = DATA_DIR / "listing_snapshots"

EXCHANGES: dict[str, dict[str, Any]] = {
    "binance": {
        # Official market-data mirror is reachable in regions where api.binance.com
        # returns HTTP 451. Keep the primary API as a fallback, never a proxy.
        "urls": ["https://data-api.binance.vision/api/v3/exchangeInfo",
                 "https://api.binance.com/api/v3/exchangeInfo"],
        "parser": "_parse_binance",
    },
    "okx": {
        "urls": ["https://www.okx.com/api/v5/public/instruments?instType=SPOT"],
        "parser": "_parse_okx",
    },
    "bybit": {
        "urls": ["https://api.bybit.com/v5/market/instruments-info?category=spot"],
        "parser": "_parse_bybit",
    },
    "coinbase": {
        # Coinbase Exchange market-data endpoints are public and the product status
        # is explicit. Keep disabled/offline/delisted products out of the baseline so
        # a re-enabled market is observable as a fresh structure event.
        "urls": ["https://api.exchange.coinbase.com/products"],
        "parser": "_parse_coinbase",
    },
}

POLL_INTERVAL_SECONDS = 60
FAILURE_LOG_INTERVAL_SECONDS = 3600
MIN_INVENTORY_RETAIN_RATIO = 0.75
_LAST_FAILURE_LOG: dict[str, float] = {}


class _SourcePayloadError(ValueError):
    """A successful HTTP response that cannot prove a complete source snapshot."""

    def __init__(self, error_kind: str, detail: str, *,
                 upstream_code: int | None = None) -> None:
        super().__init__(detail)
        self.error_kind = error_kind
        self.upstream_code = upstream_code


def _response_excerpt(response: httpx.Response | None) -> str:
    """Return a finite response hint for evidence-based HTTP classification."""
    if response is None:
        return ""
    try:
        return " ".join(response.text.split())[:240]
    except (httpx.HTTPError, RuntimeError):
        return ""


def _http_error_kind(exchange: str, status: int | None, body: str) -> str:
    """Classify only what the HTTP status/body proves; never guess geography."""
    body_lower = body.lower()
    if status in (408, 504):
        return "request_timeout"
    if status == 429:
        return "rate_limited"
    if status == 451:
        return "geo_blocked"
    if status == 403:
        if exchange == "bybit" and any(marker in body_lower for marker in (
                "block access from your country", "country is restricted",
                "region is restricted")):
            return "geo_blocked"
        if any(marker in body_lower for marker in (
                "access too frequent", "too many visits", "rate limit")):
            return "rate_limited"
        return "forbidden"
    if status is not None and status >= 500:
        return "upstream_http_error"
    return "http_error"


def _log_fetch_failure(exchange: str, error: str, *, now: float | None = None) -> None:
    """Keep per-scan health evidence while rate-limiting expected geo-block noise."""
    now = time.monotonic() if now is None else now
    last = _LAST_FAILURE_LOG.get(exchange)
    if last is None or now - last >= FAILURE_LOG_INTERVAL_SECONDS:
        _LAST_FAILURE_LOG[exchange] = now
        logger.warning("listing_source_unavailable", exchange=exchange, error=error)
    else:
        logger.debug("listing_source_still_unavailable", exchange=exchange)


# ---------------------------------------------------------------------------
# Parsers: extract a set of trading pair strings from each exchange response
# ---------------------------------------------------------------------------

def _parse_binance(data: dict) -> set[str]:
    """Extract symbols from Binance exchangeInfo response."""
    symbols: set[str] = set()
    for s in data.get("symbols", []):
        if s.get("status") == "TRADING":
            symbols.add(s["symbol"])
    return symbols


def _parse_okx(data: dict) -> set[str]:
    """Extract instId from OKX instruments response."""
    symbols: set[str] = set()
    for inst in data.get("data", []):
        if inst.get("state") == "live":
            symbols.add(inst["instId"])
    return symbols


def _parse_bybit(data: dict) -> set[str]:
    """Extract a schema-verified Bybit spot inventory.

    Bybit reports API failures inside HTTP-200 JSON through ``retCode``.  Treating
    those envelopes, or a missing/wrong ``result.list``, as an empty inventory would
    make an unavailable source look like a completed scan and could poison the next
    listing delta.  A real spot venue inventory is also never expected to be empty.
    """
    if not isinstance(data, dict):
        raise _SourcePayloadError(
            "invalid_response_schema", "Bybit response must be an object")
    ret_code = data.get("retCode")
    # bool and string "0" are not valid API evidence even though False == 0.
    if not isinstance(ret_code, int) or isinstance(ret_code, bool):
        raise _SourcePayloadError(
            "invalid_response_schema", "Bybit retCode is missing or invalid")
    if ret_code != 0:
        ret_msg = str(data.get("retMsg") or "Bybit API rejected the request")[:160]
        kind = {
            429: "rate_limited",
            10000: "request_timeout",
            10006: "rate_limited",
            10009: "geo_blocked",
        }.get(ret_code, "upstream_api_error")
        raise _SourcePayloadError(kind, ret_msg, upstream_code=ret_code)
    result = data.get("result")
    if not isinstance(result, dict):
        raise _SourcePayloadError(
            "invalid_response_schema", "Bybit result must be an object")
    if result.get("category") != "spot":
        raise _SourcePayloadError(
            "unexpected_market_category", "Bybit result category is not spot")
    rows = result.get("list")
    if not isinstance(rows, list):
        raise _SourcePayloadError(
            "invalid_response_schema", "Bybit result.list must be an array")
    if not rows:
        raise _SourcePayloadError(
            "suspicious_empty_inventory", "Bybit returned an empty spot inventory")

    symbols: set[str] = set()
    for index, item in enumerate(rows):
        if not isinstance(item, dict):
            raise _SourcePayloadError(
                "malformed_instrument_rows", f"Bybit row {index} is not an object")
        symbol, status = item.get("symbol"), item.get("status")
        if (not isinstance(symbol, str) or not symbol.strip()
                or not isinstance(status, str)):
            raise _SourcePayloadError(
                "malformed_instrument_rows", f"Bybit row {index} lacks symbol/status")
        if status == "Trading":
            symbols.add(symbol.strip())
    if not symbols:
        raise _SourcePayloadError(
            "suspicious_empty_inventory", "Bybit returned no Trading spot instruments")
    return symbols


def _parse_coinbase(data: object) -> set[str]:
    """Extract currently tradable Coinbase Exchange product IDs."""
    if not isinstance(data, list):
        return set()
    return {
        str(item["id"])
        for item in data
        if (isinstance(item, dict) and item.get("id")
            and item.get("status") == "online"
            and item.get("trading_disabled") is not True)
    }


_PARSERS = {
    "_parse_binance": _parse_binance,
    "_parse_okx": _parse_okx,
    "_parse_bybit": _parse_bybit,
    "_parse_coinbase": _parse_coinbase,
}


# ---------------------------------------------------------------------------
# Snapshot persistence
# ---------------------------------------------------------------------------

def _snapshot_path(exchange: str) -> Path:
    return SNAPSHOT_DIR / f"{exchange}_symbols.json"


def _load_snapshot(exchange: str) -> set[str]:
    """Load the previous symbol snapshot from disk."""
    path = _snapshot_path(exchange)
    if path.exists():
        try:
            with open(path) as f:
                return set(json.load(f))
        except (json.JSONDecodeError, OSError):
            pass
    return set()


def _save_snapshot(exchange: str, symbols: set[str]) -> None:
    """Atomically persist a complete symbol set without exposing a partial file."""
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    path = _snapshot_path(exchange)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w") as f:
        json.dump(sorted(symbols), f)
    tmp.replace(path)


# ---------------------------------------------------------------------------
# Core detection
# ---------------------------------------------------------------------------

def check_exchange_result(
    exchange: str,
    timeout: float = 25.0,
) -> dict[str, Any]:
    """Fetch one exchange and return alerts plus explicit source health.

    A failed request must remain distinguishable from a successful scan that found
    zero listings.  Otherwise callers can accidentally claim full exchange coverage
    while a geo-blocked endpoint silently returns an empty alert list.

    Args:
        exchange: One of ``"binance"``, ``"okx"``, ``"bybit"``.
        timeout: HTTP timeout in seconds.

    Returns a dict with ``status`` (``ok`` or ``failed``), source evidence and an
    ``alerts`` list.  ``baseline_ready`` is false on the first successful scan,
    because that scan can establish inventory but cannot detect a delta.
    """
    checked_at = datetime.now(timezone.utc).isoformat()
    result: dict[str, Any] = {
        "exchange": exchange, "checked_at": checked_at, "status": "failed",
        "symbol_count": None, "baseline_ready": False, "new_count": 0,
        "alerts": [], "error_kind": None,
    }
    config = EXCHANGES.get(exchange)
    if config is None:
        logger.warning("unknown_exchange", exchange=exchange)
        result["error"] = "unknown exchange"
        result["error_kind"] = "unknown_exchange"
        return result

    urls = config.get("urls") or [config["url"]]
    parser_name = config["parser"]
    parser_fn = _PARSERS[parser_name]

    data: object | None = None
    errors: list[dict[str, Any]] = []
    selected_url = None
    for url in urls:
        try:
            resp = httpx.get(url, timeout=timeout)
            resp.raise_for_status()
            data = resp.json()
            selected_url = url
            break
        except httpx.HTTPStatusError as exc:
            status = exc.response.status_code if exc.response is not None else None
            body = _response_excerpt(exc.response)
            errors.append({
                "endpoint": url,
                "error_kind": _http_error_kind(exchange, status, body),
                "http_status": status,
                "detail": body or str(exc)[:160],
            })
        except httpx.TimeoutException as exc:
            errors.append({"endpoint": url, "error_kind": "request_timeout",
                           "detail": str(exc)[:160]})
        except httpx.HTTPError as exc:
            errors.append({"endpoint": url, "error_kind": "transport_error",
                           "detail": str(exc)[:160]})
        except (json.JSONDecodeError, ValueError) as exc:
            errors.append({"endpoint": url, "error_kind": "invalid_json",
                           "detail": str(exc)[:160]})
    result["attempted_endpoints"] = len(errors) + int(selected_url is not None)
    if data is None:
        error = " | ".join(
            f"{item['endpoint']}: {item['detail']}" for item in errors)[:300]
        _log_fetch_failure(exchange, error)
        result["error"] = error or "source returned no response payload"
        result["error_kind"] = (
            errors[-1]["error_kind"] if errors else "empty_response_payload")
        if errors and errors[-1].get("http_status") is not None:
            result["http_status"] = errors[-1]["http_status"]
        if len(errors) > 1:
            result["endpoint_errors"] = errors
        return result
    result["endpoint"] = selected_url

    try:
        current_symbols = parser_fn(data)
    except _SourcePayloadError as exc:
        result["error"] = str(exc)[:200]
        result["error_kind"] = exc.error_kind
        if exc.upstream_code is not None:
            result["upstream_code"] = exc.upstream_code
        _log_fetch_failure(exchange, result["error"])
        return result
    except (AttributeError, KeyError, TypeError, ValueError) as exc:
        result["error"] = f"response schema invalid: {str(exc)[:160]}"
        result["error_kind"] = "invalid_response_schema"
        _log_fetch_failure(exchange, result["error"])
        return result
    if not current_symbols:
        logger.warning("no_symbols_parsed", exchange=exchange)
        result["error"] = "response parsed zero live symbols"
        result["error_kind"] = "suspicious_empty_inventory"
        return result

    previous_symbols = _load_snapshot(exchange)
    # A non-empty response can still be truncated by a gateway or upstream partial
    # failure. Replacing a healthy inventory with a sharply smaller one would make
    # the next recovery look like hundreds of fake new listings. Fail closed and
    # retain the last complete baseline. Genuine mass delistings can be reviewed
    # manually; this lane only needs additions quickly.
    if (len(previous_symbols) >= 100
            and len(current_symbols) < len(previous_symbols) * MIN_INVENTORY_RETAIN_RATIO):
        result["error"] = (f"inventory truncated: {len(current_symbols)} current vs "
                           f"{len(previous_symbols)} previous")
        result["error_kind"] = "inventory_truncated"
        return result
    try:
        _save_snapshot(exchange, current_symbols)
    except OSError as exc:
        result["error"] = f"snapshot write failed: {str(exc)[:120]}"
        result["error_kind"] = "snapshot_write_failed"
        return result
    result.update({"status": "ok", "symbol_count": len(current_symbols),
                   "baseline_ready": bool(previous_symbols)})

    if not previous_symbols:
        # First run — nothing to compare against
        logger.info(
            "listing_baseline_saved",
            exchange=exchange,
            count=len(current_symbols),
        )
        return result

    new_symbols = current_symbols - previous_symbols
    if not new_symbols:
        return result

    now = datetime.now(timezone.utc).isoformat()
    alerts: list[dict[str, Any]] = []
    for sym in sorted(new_symbols):
        alert = {
            "exchange": exchange,
            "symbol": sym,
            "detected_at": now,
            "message": f"[新上币] {exchange.upper()} 新增交易对: {sym}",
        }
        alerts.append(alert)
        logger.info("new_listing_detected", exchange=exchange, symbol=sym)

    result.update({"alerts": alerts, "new_count": len(alerts)})
    return result


def check_exchange(
    exchange: str,
    timeout: float = 10.0,
) -> list[dict[str, Any]]:
    """Backward-compatible alert-only interface for one exchange."""
    return check_exchange_result(exchange, timeout=timeout)["alerts"]


def check_all_exchanges_with_status(timeout: float = 25.0) -> dict[str, Any]:
    """Check every configured source without converting failures to empty scans."""
    sources = []
    for exchange in EXCHANGES:
        try:
            sources.append(check_exchange_result(exchange, timeout=timeout))
        except Exception as exc:
            # One broken parser or local snapshot must not erase the health evidence
            # from every other exchange in the same scan.
            sources.append({
                "exchange": exchange,
                "checked_at": datetime.now(timezone.utc).isoformat(),
                "status": "failed", "symbol_count": None,
                "baseline_ready": False, "new_count": 0, "alerts": [],
                "error_kind": "unexpected_source_failure",
                "error": f"unexpected source failure: {str(exc)[:160]}",
            })
    return {
        "alerts": [alert for source in sources for alert in source["alerts"]],
        "sources": [{k: v for k, v in source.items() if k != "alerts"}
                    for source in sources],
    }


def check_all_exchanges(timeout: float = 25.0) -> list[dict[str, Any]]:
    """Check all configured exchanges for new listings.

    Returns:
        Combined list of alert dicts from all exchanges.
    """
    all_alerts: list[dict[str, Any]] = []
    for exchange in EXCHANGES:
        alerts = check_exchange(exchange, timeout=timeout)
        all_alerts.extend(alerts)
    return all_alerts


def run_listing_monitor(poll_interval: int = POLL_INTERVAL_SECONDS) -> None:
    """Run the listing detector in a continuous loop.

    This is intended to be called as a standalone process or background task.
    Prints alerts to stdout and logs via structlog.

    Args:
        poll_interval: Seconds between each check cycle (default 60).
    """
    logger.info("listing_monitor_started", interval=poll_interval)
    while True:
        alerts = check_all_exchanges()
        for alert in alerts:
            print(alert["message"])
        time.sleep(poll_interval)
