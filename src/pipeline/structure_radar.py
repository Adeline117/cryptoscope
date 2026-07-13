"""Public-structure opportunity lane: exchange listings first, never rumors.

Listings are not automatically long signals. They are timestamped public market
structure events that can change liquidity and access. Recording them separately
lets us measure the post-listing distribution before assigning a trading rule.
"""
from __future__ import annotations

from src.pipeline.opportunity_ledger import active, record


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
    from src.collectors.listing_detector import check_all_exchanges
    listings = check_all_exchanges()
    return {"scanned": 3, "inserted": record_listings(listings), "events": active("structure"),
            "source": "Binance / OKX / Bybit public instruments"}


def view() -> dict:
    return {"events": active("structure"), "source": "Public structure event ledger"}
