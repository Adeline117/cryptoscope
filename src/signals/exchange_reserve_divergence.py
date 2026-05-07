"""交易所储备异常信号。

逻辑：
- 大量流出（reserve_change_24h 显著负值）
- DeFi TVL 增长（资金流向链上）
- OI 下降（投机头寸减少）
→ LONG（供应紧缩，链上吸筹）

反向：
- 大量流入 + DeFi TVL 下降 + OI 上升 → SHORT（抛压增加）
"""

from __future__ import annotations

from src.signals.base import TradeSignal


class ExchangeReserveDivergenceSignal:
    """检测交易所储备变化与 DeFi/OI 的异常背离。"""

    OUTFLOW_THRESHOLD = -5.0  # 24h 储备变化 < -5%
    INFLOW_THRESHOLD = 5.0  # 24h 储备变化 > +5%
    DEFI_GROWTH_THRESHOLD = 1.0  # TVL 变化 > 1%
    OI_DECLINE_THRESHOLD = -2.0  # OI 变化 < -2%
    CONSECUTIVE_OUTFLOW_STRONG = 3  # 连续流出天数强信号

    async def evaluate(self, market_data: dict) -> TradeSignal | None:
        reserve_change: float = market_data.get("reserve_change_24h", 0)
        defi_tvl_change: float = market_data.get("defi_tvl_change_24h", 0)
        oi_change: float = market_data.get("oi_change_24h", 0)
        consecutive_outflow: int = market_data.get("consecutive_outflow_days", 0)

        # --- LONG：储备流出 + DeFi 增长 + OI 下降 ---
        if (
            reserve_change < self.OUTFLOW_THRESHOLD
            and defi_tvl_change > self.DEFI_GROWTH_THRESHOLD
            and oi_change < self.OI_DECLINE_THRESHOLD
        ):
            base_confidence = 50
            outflow_boost = min(20, int(abs(reserve_change) - abs(self.OUTFLOW_THRESHOLD)) * 2)
            defi_boost = min(10, int(defi_tvl_change) * 3)
            streak_boost = min(15, consecutive_outflow * 3) if consecutive_outflow >= self.CONSECUTIVE_OUTFLOW_STRONG else 0
            confidence = min(100, base_confidence + outflow_boost + defi_boost + streak_boost)

            return TradeSignal(
                name="交易所储备异常",
                direction="LONG",
                confidence=confidence,
                signal_type="exchange_reserve_divergence",
                components={
                    "reserve_change_24h_pct": reserve_change,
                    "defi_tvl_change_24h_pct": defi_tvl_change,
                    "oi_change_24h_pct": oi_change,
                    "consecutive_outflow_days": consecutive_outflow,
                },
                reasoning=(
                    f"交易所储备24h流出{abs(reserve_change):.1f}%，"
                    f"DeFi TVL增长{defi_tvl_change:.1f}%（资金流向链上），"
                    f"OI下降{abs(oi_change):.1f}%（投机减少），"
                    f"{'连续' + str(consecutive_outflow) + '天流出，' if consecutive_outflow >= self.CONSECUTIVE_OUTFLOW_STRONG else ''}"
                    f"供应紧缩做多信号"
                ),
            )

        # --- SHORT：储备流入 + DeFi 下降 + OI 上升 ---
        if (
            reserve_change > self.INFLOW_THRESHOLD
            and defi_tvl_change < -self.DEFI_GROWTH_THRESHOLD
            and oi_change > abs(self.OI_DECLINE_THRESHOLD)
        ):
            base_confidence = 45
            inflow_boost = min(20, int(reserve_change - self.INFLOW_THRESHOLD) * 2)
            confidence = min(100, base_confidence + inflow_boost)

            return TradeSignal(
                name="交易所储备异常",
                direction="SHORT",
                confidence=confidence,
                signal_type="exchange_reserve_divergence",
                components={
                    "reserve_change_24h_pct": reserve_change,
                    "defi_tvl_change_24h_pct": defi_tvl_change,
                    "oi_change_24h_pct": oi_change,
                    "consecutive_outflow_days": consecutive_outflow,
                },
                reasoning=(
                    f"交易所储备24h流入{reserve_change:.1f}%，"
                    f"DeFi TVL下降{abs(defi_tvl_change):.1f}%（资金回流交易所），"
                    f"OI上升{oi_change:.1f}%（投机增加），"
                    f"抛压增加做空信号"
                ),
            )

        return None
