"""Exchange new-listing detector.

Polls Binance, OKX, and Bybit instrument endpoints every cycle, compares
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
        "url": "https://api.binance.com/api/v3/exchangeInfo",
        "parser": "_parse_binance",
    },
    "okx": {
        "url": "https://www.okx.com/api/v5/public/instruments?instType=SPOT",
        "parser": "_parse_okx",
    },
    "bybit": {
        "url": "https://api.bybit.com/v5/market/instruments-info?category=spot",
        "parser": "_parse_bybit",
    },
}

POLL_INTERVAL_SECONDS = 60


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
    """Extract symbols from Bybit instruments response."""
    symbols: set[str] = set()
    result = data.get("result", {})
    for item in result.get("list", []):
        if item.get("status") == "Trading":
            symbols.add(item["symbol"])
    return symbols


_PARSERS = {
    "_parse_binance": _parse_binance,
    "_parse_okx": _parse_okx,
    "_parse_bybit": _parse_bybit,
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
    """Persist the current symbol set to disk."""
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    path = _snapshot_path(exchange)
    with open(path, "w") as f:
        json.dump(sorted(symbols), f)


# ---------------------------------------------------------------------------
# Core detection
# ---------------------------------------------------------------------------

def check_exchange(
    exchange: str,
    timeout: float = 10.0,
) -> list[dict[str, Any]]:
    """Fetch current listings for one exchange and return new pairs.

    Args:
        exchange: One of ``"binance"``, ``"okx"``, ``"bybit"``.
        timeout: HTTP timeout in seconds.

    Returns:
        List of alert dicts, each with ``exchange``, ``symbol``,
        ``detected_at``, and ``message``.
    """
    config = EXCHANGES.get(exchange)
    if config is None:
        logger.warning("unknown_exchange", exchange=exchange)
        return []

    url = config["url"]
    parser_name = config["parser"]
    parser_fn = _PARSERS[parser_name]

    try:
        resp = httpx.get(url, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()
    except (httpx.HTTPError, json.JSONDecodeError) as exc:
        logger.error("listing_fetch_failed", exchange=exchange, error=str(exc))
        return []

    current_symbols = parser_fn(data)
    if not current_symbols:
        logger.warning("no_symbols_parsed", exchange=exchange)
        return []

    previous_symbols = _load_snapshot(exchange)
    _save_snapshot(exchange, current_symbols)

    if not previous_symbols:
        # First run — nothing to compare against
        logger.info(
            "listing_baseline_saved",
            exchange=exchange,
            count=len(current_symbols),
        )
        return []

    new_symbols = current_symbols - previous_symbols
    if not new_symbols:
        return []

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

    return alerts


def check_all_exchanges(timeout: float = 10.0) -> list[dict[str, Any]]:
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
