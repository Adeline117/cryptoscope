"""Distribution / exit signal — whale派发 is the exit you most want to front-run.

After accumulation, the whale eventually distributes: sustained wallet→CEX flow
(depositing to sell) and LP being topped up to absorb the dump. This is the
mirror of the accumulation signal and the highest-value exit trigger.

Reuses the accumulation/distribution disambiguation already in
`whale_tracker._classify_transfer`:
  CEX → wallet = accumulation (吸筹)
  wallet → CEX = distribution / selling pressure (派发/出货)

`classify_flows` turns a list of transfers into net flow counts; the signal
fires an exit when distribution clearly dominates on a token we previously saw
accumulating.

Input `market_data` keys:
  - "to_cex_count":   int  wallet→CEX deposits in the window
  - "from_cex_count": int  CEX→wallet withdrawals in the window
  - "had_accumulation": bool  did this token previously trigger accumulation?
  - "token_symbol", "token_address", "chain": labels (optional)
"""

from __future__ import annotations

from typing import Any

import structlog

from src.signals.base import TradeSignal

logger = structlog.get_logger()


def classify_flows(transfers: list[dict[str, Any]]) -> dict[str, int]:
    """Count accumulation vs distribution transfers using whale_tracker logic.

    Each transfer needs "from_label" and "to_label" ("unknown" = private wallet,
    anything else = a known exchange).
    """
    try:
        from src.collectors.whale_tracker import WhaleTrackerCollector

        classify = WhaleTrackerCollector()._classify_transfer
    except Exception:
        def classify(from_label: str, to_label: str) -> dict:  # type: ignore
            f, t = from_label != "unknown", to_label != "unknown"
            if f and not t:
                return {"signal": "accumulation"}
            if not f and t:
                return {"signal": "selling_pressure"}
            return {"signal": "other"}

    to_cex = from_cex = 0
    for tr in transfers:
        sig = classify(tr.get("from_label", "unknown"), tr.get("to_label", "unknown"))
        if sig["signal"] == "selling_pressure":
            to_cex += 1
        elif sig["signal"] == "accumulation":
            from_cex += 1
    return {"to_cex_count": to_cex, "from_cex_count": from_cex}


class DistributionExitSignal:
    """派发出场信号 — fires when distribution dominates post-accumulation."""

    MIN_TO_CEX = 3          # need this many wallet→CEX deposits to call it
    DOMINANCE_RATIO = 2.0   # deposits must outnumber withdrawals by this factor

    signal_type = "distribution_exit"

    async def evaluate(self, market_data: dict) -> TradeSignal | None:
        to_cex = int(market_data.get("to_cex_count", 0) or 0)
        from_cex = int(market_data.get("from_cex_count", 0) or 0)
        had_accum = bool(market_data.get("had_accumulation", False))

        if not had_accum:
            return None
        if to_cex < self.MIN_TO_CEX:
            return None
        if to_cex < self.DOMINANCE_RATIO * max(from_cex, 1):
            return None

        # Confidence grows with how lopsided the distribution is.
        ratio = to_cex / max(from_cex, 1)
        confidence = min(100, 50 + int(min(ratio, 6) * 8) + min(to_cex, 10))

        symbol = market_data.get("token_symbol", "?")
        return TradeSignal(
            name="庄家派发出场",
            direction="EXIT",
            confidence=confidence,
            signal_type=self.signal_type,
            components={
                "to_cex_count": to_cex,
                "from_cex_count": from_cex,
                "distribution_ratio": round(ratio, 2),
                "token_symbol": symbol,
                "token_address": market_data.get("token_address", ""),
                "chain": market_data.get("chain", ""),
            },
            reasoning=(
                f"{symbol}：吸筹后出现 {to_cex} 笔钱包→CEX 充值（出货），"
                f"是回流的 {ratio:.1f} 倍，庄家开始派发 — 出场信号"
            ),
        )
