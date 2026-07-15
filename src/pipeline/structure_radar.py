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
    inserted = 0
    for listing in listings:
        exchange, symbol = listing.get("exchange"), listing.get("symbol")
        if not exchange or not symbol:
            continue
        event_at = listing.get("detected_at")
        event = {
            "lane": "structure", "chain": "cex", "token": symbol,
            "event_key": f"listing:{exchange}:{symbol}:{event_at}", "symbol": symbol,
            "source": exchange, "event_at": event_at, "state": "live",
            "decision": "WATCH", "event_type": "new_listing",
            "message": listing.get("message", ""),
            "reasons": ["公开交易所新增交易对", "先记录流动性变化，未验证为方向信号"],
        }
        _, new = record(event)
        inserted += int(new)
    return inserted


def scan() -> dict:
    from src.collectors.listing_detector import check_all_exchanges_with_status
    result = check_all_exchanges_with_status()
    sources = result["sources"]
    _save_source_health(sources)
    reachable = sum(source.get("status") == "ok" for source in sources)
    return {"scanned": reachable, "configured_sources": len(sources),
            "source_health": sources, "source_health_at": datetime.now(timezone.utc).isoformat(),
            "inserted": record_listings(result["alerts"]), "events": active("structure"),
            "source": "Public exchange instruments with per-source health"}


def view() -> dict:
    health = _load_source_health()
    sources = health["sources"]
    return {"events": active("structure"), "source_health": sources,
            "source_health_at": health["updated_at"],
            "scanned": sum(source.get("status") == "ok" for source in sources),
            "configured_sources": len(sources),
            "source": "Public structure event ledger with per-source health"}
