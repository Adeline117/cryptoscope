"""Event ledger adapter for leverage cascades and ignition.

The signal module deliberately distinguishes a standing crowded state from the
tradable *moment* when OI begins unwinding. This adapter keeps that distinction
in the database: only strong, timed events receive a plan; crowded context stays
on the perp screen but never becomes a fake entry.
"""
from __future__ import annotations

import json
import urllib.request
from datetime import datetime, timedelta, timezone

from src.pipeline.opportunity_ledger import active, record

INFO_URL = "https://api.hyperliquid.xyz/info"
PROBE_NOTIONAL_USD = 100.0
QUOTE_TTL_SECONDS = 60
MAX_QUOTE_AGE_SECONDS = 10
MAX_SPREAD_BPS = 50.0
MAX_BOOK_IMPACT_BPS = 25.0


def _fetch_book(symbol: str) -> dict:
    request = urllib.request.Request(
        INFO_URL,
        data=json.dumps({"type": "l2Book", "coin": symbol}).encode(),
        headers={"Content-Type": "application/json", "User-Agent": "CryptoScope/1.0"},
    )
    with urllib.request.urlopen(request, timeout=8) as response:
        return json.loads(response.read().decode())


def executable_quote(symbol: str, side: str, *, fetch=_fetch_book,
                     now: datetime | None = None) -> dict:
    """Simulate a fixed-size marketable fill from a fresh public L2 snapshot."""
    now = now or datetime.now(timezone.utc)
    try:
        book = fetch(symbol)
        if not isinstance(book, dict) or book.get("coin") != symbol:
            raise ValueError("book identity mismatch")
        event_ms = int(book["time"])
        quote_at = datetime.fromtimestamp(event_ms / 1000, tz=timezone.utc)
        age_seconds = (now - quote_at).total_seconds()
        if age_seconds < -5 or age_seconds > MAX_QUOTE_AGE_SECONDS:
            raise ValueError(f"stale book ({age_seconds:.1f}s)")
        levels = book.get("levels")
        if not isinstance(levels, list) or len(levels) != 2:
            raise ValueError("missing two-sided book")
        bids, asks = levels
        if not bids or not asks:
            raise ValueError("empty book side")
        best_bid, best_ask = float(bids[0]["px"]), float(asks[0]["px"])
        if best_bid <= 0 or best_ask <= best_bid:
            raise ValueError("crossed or invalid book")
        spread_bps = (best_ask / best_bid - 1) * 10_000
        if spread_bps > MAX_SPREAD_BPS:
            raise ValueError(f"spread too wide ({spread_bps:.1f}bps)")

        fill_levels = asks if side == "LONG" else bids
        remaining = PROBE_NOTIONAL_USD
        base_filled = 0.0
        usd_filled = 0.0
        for level in fill_levels:
            px, size = float(level["px"]), float(level["sz"])
            if px <= 0 or size <= 0:
                continue
            take_usd = min(remaining, px * size)
            base_filled += take_usd / px
            usd_filled += take_usd
            remaining -= take_usd
            if remaining <= 1e-6:
                break
        if remaining > 0.01 or base_filled <= 0:
            raise ValueError(f"insufficient book depth (${usd_filled:.2f})")
        average_price = usd_filled / base_filled
        top = best_ask if side == "LONG" else best_bid
        impact_bps = ((average_price / top - 1) if side == "LONG"
                      else (1 - average_price / top)) * 10_000
        if impact_bps > MAX_BOOK_IMPACT_BPS:
            raise ValueError(f"book impact too high ({impact_bps:.1f}bps)")
        return {
            "state": "quoted", "source": "Hyperliquid public L2",
            "side": side, "notional_usd": PROBE_NOTIONAL_USD,
            "average_price": average_price, "best_bid": best_bid,
            "best_ask": best_ask, "spread_bps": round(spread_bps, 3),
            "book_impact_bps": round(max(0.0, impact_bps), 3),
            "quote_at": quote_at.isoformat(), "age_seconds": round(age_seconds, 3),
            "read_only": True, "is_real_fill": False,
        }
    except Exception as exc:
        return {"state": "unknown", "source": "Hyperliquid public L2",
                "reason": str(exc)[:100], "read_only": True, "is_real_fill": False}


def record_signals(signals: list[dict], now: datetime | None = None,
                   quote_fetch=_fetch_book) -> int:
    now = now or datetime.now(timezone.utc)
    inserted = 0
    for sig in signals:
        if sig.get("strength") != "强" or sig.get("signal") not in {"cascade", "ignition"}:
            continue
        mark_price = sig.get("mark_price")
        if not mark_price:
            continue
        direction = sig.get("direction")
        if direction not in {"longs_crowded", "shorts_crowded", "up", "down"}:
            continue
        is_down = direction in {"longs_crowded", "down"}
        side = "SHORT" if is_down else "LONG"
        quote = executable_quote(sig.get("symbol"), side, fetch=quote_fetch, now=now)
        ready = quote.get("state") == "quoted"
        price = float(quote["average_price"]) if ready else float(mark_price)
        # These episodes may recur. Bucket at 6h to prevent refresh rows becoming
        # independent trials while allowing a later market regime to be measured.
        bucket = now.strftime("%Y-%m-%dT") + f"{now.hour // 6:02d}"
        event_key = f"{sig.get('symbol')}:{sig.get('signal')}:{direction}:{bucket}"
        plan = {
            "lane": "cascade", "chain": "hyperliquid", "token": sig.get("symbol"),
            "event_key": event_key, "symbol": sig.get("symbol"), "source": "Hyperliquid",
            "event_at": now.isoformat(), "decision_at": now.isoformat(),
            "quote_at": quote.get("quote_at"),
            "expires_at": ((now + timedelta(seconds=QUOTE_TTL_SECONDS)).isoformat()
                           if ready else None),
            "state": "firing", "decision": "SMALL_PROBE" if ready else "WATCH",
            "side": side,
            "entry_price": float(price),
            "invalidation_price": round(float(price) * (1.03 if is_down else 0.97), 8),
            "max_notional_usd": PROBE_NOTIONAL_USD,
            # Fixed conservative paper-cost buffer until live entry/exit book replay is
            # wired. It is explicitly an estimate and never presented as a real fill.
            "roundtrip_cost_pct_est": 0.20,
            "cost_model": "perp_roundtrip_0.20pct_buffer",
            "execution_probe": quote,
            "signal": sig.get("signal"), "direction": direction,
            "funding_ann": sig.get("funding_ann"), "oi_usd": sig.get("oi_usd"),
            "oi_chg_pct": sig.get("oi_chg_pct"), "why": sig.get("why"),
        }
        _, new = record(plan)
        inserted += int(new)
    return inserted


def view() -> dict:
    return {"events": active("cascade"), "source": "Hyperliquid cascade event ledger"}
