"""Event ledger adapter for leverage cascades and ignition.

The signal module deliberately distinguishes a standing crowded state from the
tradable *moment* when OI begins unwinding. This adapter keeps that distinction
in the database: only strong, timed events receive a plan; crowded context stays
on the perp screen but never becomes a fake entry.
"""
from __future__ import annotations

from datetime import datetime, timezone

from src.pipeline.opportunity_ledger import active, record


def record_signals(signals: list[dict], now: datetime | None = None) -> int:
    now = now or datetime.now(timezone.utc)
    inserted = 0
    for sig in signals:
        if sig.get("strength") != "强" or sig.get("signal") not in {"cascade", "ignition"}:
            continue
        price = sig.get("mark_price")
        if not price:
            continue
        direction = sig.get("direction")
        # These episodes may recur. Bucket at 6h to prevent refresh rows becoming
        # independent trials while allowing a later market regime to be measured.
        bucket = now.strftime("%Y-%m-%dT") + f"{now.hour // 6:02d}"
        event_key = f"{sig.get('symbol')}:{sig.get('signal')}:{direction}:{bucket}"
        is_down = direction in {"longs_crowded", "down"}
        plan = {
            "lane": "cascade", "chain": "hyperliquid", "token": sig.get("symbol"),
            "event_key": event_key, "symbol": sig.get("symbol"), "source": "Hyperliquid",
            "event_at": now.isoformat(), "state": "firing",
            "decision": "SMALL_PROBE", "side": "SHORT" if is_down else "LONG",
            "entry_price": float(price),
            "invalidation_price": round(float(price) * (1.03 if is_down else 0.97), 8),
            "max_notional_usd": 100.0,  # fixed probe while the lane earns its data
            "signal": sig.get("signal"), "direction": direction,
            "funding_ann": sig.get("funding_ann"), "oi_usd": sig.get("oi_usd"),
            "oi_chg_pct": sig.get("oi_chg_pct"), "why": sig.get("why"),
        }
        _, new = record(plan)
        inserted += int(new)
    return inserted


def view() -> dict:
    return {"events": active("cascade"), "source": "Hyperliquid cascade event ledger"}
