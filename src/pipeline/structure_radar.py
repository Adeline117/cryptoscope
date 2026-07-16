"""Public-structure opportunity lane: exchange listings first, never rumors.

Listings are not automatically long signals. They are timestamped public market
structure events that can change liquidity and access. Recording them separately
lets us measure the post-listing distribution before assigning a trading rule.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

from src.config import DATA_DIR
from src.pipeline.opportunity_ledger import active, record


SOURCE_HEALTH_FILE = DATA_DIR / "structure_source_health.json"

_KNOWN_QUOTES = ("FDUSD", "USDT", "USDC", "TUSD", "BUSD", "DAI", "USD",
                 "EUR", "TRY", "BTC", "ETH", "BNB")
VIEW_LEDGER_LIMIT = 500


def _base_asset(symbol: str) -> str:
    """Normalize a spot market into its base asset for one listing event."""
    symbol = str(symbol).upper()
    if "-" in symbol:
        return symbol.split("-", 1)[0]
    for quote in _KNOWN_QUOTES:
        if symbol.endswith(quote) and len(symbol) > len(quote):
            return symbol[:-len(quote)]
    return symbol


def _save_source_health(sources: list[dict]) -> None:
    """Atomically preserve the last per-source evidence for read-only exports."""
    SOURCE_HEALTH_FILE.parent.mkdir(parents=True, exist_ok=True)
    payload = {"updated_at": datetime.now(timezone.utc).isoformat(), "sources": sources}
    tmp = SOURCE_HEALTH_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
    tmp.replace(SOURCE_HEALTH_FILE)


def _load_source_health() -> dict:
    try:
        payload = json.loads(SOURCE_HEALTH_FILE.read_text())
        if isinstance(payload.get("sources"), list):
            return payload
    except (OSError, json.JSONDecodeError, AttributeError):
        pass
    return {"updated_at": None, "sources": []}


def record_listings(listings: list[dict]) -> int:
    grouped: dict[tuple[str, str, str], dict] = {}
    for listing in listings:
        exchange, symbol = listing.get("exchange"), listing.get("symbol")
        event_at = listing.get("detected_at")
        if not exchange or not symbol or not event_at:
            continue
        base = _base_asset(symbol)
        key = (str(exchange), base, str(event_at))
        group = grouped.setdefault(key, {**listing, "base_asset": base, "markets": []})
        group["markets"].append(str(symbol))

    inserted = 0
    for (exchange, base, event_at), listing in grouped.items():
        markets = sorted(set(listing["markets"]))
        event_at = listing.get("detected_at")
        event = {
            "lane": "structure", "chain": "cex", "token": base,
            "event_key": f"listing:{exchange}:{base}:{event_at}", "symbol": base,
            "source": exchange, "event_at": event_at, "state": "live",
            "decision": "WATCH", "event_type": "new_listing",
            "message": f"{exchange.upper()} 新增 {base} 市场: {', '.join(markets)}",
            "markets": markets,
            "reasons": ["公开交易所新增交易对", "先记录流动性变化，未验证为方向信号"],
        }
        _, new = record(event)
        inserted += int(new)
    return inserted


def _view_events(rows: list[dict]) -> tuple[list[dict], dict]:
    """Collapse pre-canonical pair rows and label their weaker evidence honestly.

    Older detector versions wrote one ledger row per quote market.  A single OKX
    inventory response could therefore look like dozens of independent listings.
    The ledger remains immutable, so the read model folds those rows by
    ``(source, base asset, detector batch)`` and keeps them visibly separate from
    canonical asset-level events written by :func:`record_listings`.
    """
    canonical: list[dict] = []
    legacy_groups: dict[tuple[str, str, str], list[dict]] = {}
    for row in rows:
        markets = row.get("markets")
        if row.get("event_type") != "new_listing" or (
                isinstance(markets, list) and markets):
            canonical.append({**row, "auto_execution_allowed": False})
            continue
        source = str(row.get("source") or "unknown")
        base = _base_asset(str(row.get("symbol") or row.get("token") or "?"))
        batch_at = str(row.get("event_at") or row.get("detected_at") or "unknown")
        legacy_groups.setdefault((source, base, batch_at), []).append(row)

    legacy: list[dict] = []
    for (source, base, batch_at), group in legacy_groups.items():
        markets = sorted({str(row.get("symbol") or "") for row in group
                          if row.get("symbol")})
        first = min(group, key=lambda row: str(row.get("detected_at") or ""))
        legacy.append({
            **first,
            "id": f"legacy-inventory:{source}:{base}:{batch_at}",
            "token": base,
            "symbol": base,
            "markets": markets,
            "event_type": "legacy_inventory_delta",
            "state": "legacy_observation",
            "decision": "WATCH",
            "recorded_decision": "WATCH",
            "effective_decision": "WATCH",
            "actionable_now": False,
            "auto_execution_allowed": False,
            "evidence_state": "inventory_delta_only",
            "legacy_row_count": len(group),
            "ledger_event_ids": [row.get("id") for row in group if row.get("id")],
            "message": (f"{source.upper()} 旧版 instrument inventory delta: "
                        f"{', '.join(markets)}"),
            "reasons": [
                "旧版按交易对写入，已按基础资产折叠",
                "仅证明公开 instruments 库存差分，未独立核验为官方上币公告",
                "只观察，不进入方向优势判决",
            ],
        })

    events = sorted(canonical + legacy,
                    key=lambda row: str(row.get("event_at") or row.get("detected_at") or ""),
                    reverse=True)
    legacy_rows = sum(len(group) for group in legacy_groups.values())
    return events, {
        "canonical_events": len(canonical),
        "canonical_listings": sum(row.get("event_type") == "new_listing"
                                  for row in canonical),
        "legacy_inventory_deltas": len(legacy),
        "legacy_rows": legacy_rows,
        "legacy_rows_collapsed": legacy_rows - len(legacy),
        "raw_open_rows": len(rows),
    }


def scan() -> dict:
    from src.collectors.listing_detector import check_all_exchanges_with_status
    result = check_all_exchanges_with_status()
    sources = result["sources"]
    _save_source_health(sources)
    reachable = sum(source.get("status") == "ok" for source in sources)
    inserted = record_listings(result["alerts"])
    events, summary = _view_events(active("structure", limit=VIEW_LEDGER_LIMIT))
    return {"scanned": reachable, "configured_sources": len(sources),
            "source_health": sources, "source_health_at": datetime.now(timezone.utc).isoformat(),
            "inserted": inserted, "events": events, **summary,
            "source": "Public exchange instruments with per-source health"}


def view() -> dict:
    health = _load_source_health()
    sources = health["sources"]
    events, summary = _view_events(active("structure", limit=VIEW_LEDGER_LIMIT))
    return {"events": events, **summary, "source_health": sources,
            "source_health_at": health["updated_at"],
            "scanned": sum(source.get("status") == "ok" for source in sources),
            "configured_sources": len(sources),
            "source": "Public structure event ledger with per-source health"}
