"""Funding Rate 均值回归信号。

逻辑：
- funding > 0.1% 连续3期 + F&G > 75 + L/S > 2.0 → SHORT（过度贪婪，做空回归）
- funding < -0.1% 连续3期 + F&G < 25 + L/S < 0.5 → LONG（过度恐惧，做多回归）
"""

from __future__ import annotations

from src.signals.base import TradeSignal


class FundingMeanReversionSignal:
    """检测 funding rate 极端偏离并预期均值回归。"""

    FUNDING_THRESHOLD = 0.001  # 0.1%
    CONSECUTIVE_PERIODS = 3
    FG_HIGH = 75
    FG_LOW = 25
    LS_HIGH = 2.0
    LS_LOW = 0.5

    async def evaluate(self, market_data: dict) -> TradeSignal | None:
        funding_history: list[float] = market_data.get("funding_history", [])
        fear_greed: float = market_data.get("fear_greed", 50)
        long_short_ratio: float = market_data.get("long_short_ratio", 1.0)

        if len(funding_history) < self.CONSECUTIVE_PERIODS:
            return None

        recent = funding_history[-self.CONSECUTIVE_PERIODS :]

        # --- SHORT 条件：funding 持续正高 + 贪婪 + 多头拥挤 ---
        all_high = all(f > self.FUNDING_THRESHOLD for f in recent)
        if all_high and fear_greed > self.FG_HIGH and long_short_ratio > self.LS_HIGH:
            avg_funding = sum(recent) / len(recent)
            confidence = min(100, int(
                40
                + (fear_greed - self.FG_HIGH) * 0.8
                + (long_short_ratio - self.LS_HIGH) * 10
                + (avg_funding - self.FUNDING_THRESHOLD) * 5000
            ))
            return TradeSignal(
                name="Funding 均值回归",
                direction="SHORT",
                confidence=confidence,
                signal_type="funding_reversion",
                components={
                    "recent_funding": recent,
                    "avg_funding_pct": round(avg_funding * 100, 4),
                    "fear_greed": fear_greed,
                    "long_short_ratio": long_short_ratio,
                },
                reasoning=(
                    f"Funding 连续{self.CONSECUTIVE_PERIODS}期高于0.1%"
                    f"（均值{avg_funding * 100:.3f}%），"
                    f"F&G={fear_greed}极度贪婪，多空比{long_short_ratio}多头拥挤，"
                    f"均值回归做空信号"
                ),
            )

        # --- LONG 条件：funding 持续负低 + 恐惧 + 空头拥挤 ---
        all_low = all(f < -self.FUNDING_THRESHOLD for f in recent)
        if all_low and fear_greed < self.FG_LOW and long_short_ratio < self.LS_LOW:
            avg_funding = sum(recent) / len(recent)
            confidence = min(100, int(
                40
                + (self.FG_LOW - fear_greed) * 0.8
                + (self.LS_LOW - long_short_ratio) * 10
                + (abs(avg_funding) - self.FUNDING_THRESHOLD) * 5000
            ))
            return TradeSignal(
                name="Funding 均值回归",
                direction="LONG",
                confidence=confidence,
                signal_type="funding_reversion",
                components={
                    "recent_funding": recent,
                    "avg_funding_pct": round(avg_funding * 100, 4),
                    "fear_greed": fear_greed,
                    "long_short_ratio": long_short_ratio,
                },
                reasoning=(
                    f"Funding 连续{self.CONSECUTIVE_PERIODS}期低于-0.1%"
                    f"（均值{avg_funding * 100:.3f}%），"
                    f"F&G={fear_greed}极度恐惧，多空比{long_short_ratio}空头拥挤，"
                    f"均值回归做多信号"
                ),
            )

        return None
