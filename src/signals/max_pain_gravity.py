"""Max Pain 引力信号。

逻辑：
- 到期前 3 天内，价格偏离 max pain > 5% → 向 max pain 方向的 bias
- 偏离越大、到期越近，置信度越高
- Put/Call Ratio 和 ATM IV 辅助确认
"""

from __future__ import annotations

from src.signals.base import TradeSignal


class MaxPainGravitySignal:
    """检测期权到期前价格向 Max Pain 回归的引力效应。"""

    MAX_DAYS_TO_EXPIRY = 3
    MIN_DEVIATION_PCT = 5.0

    async def evaluate(self, market_data: dict) -> TradeSignal | None:
        max_pain: float | None = market_data.get("max_pain")
        price: float | None = market_data.get("underlying_price")
        days_to_expiry: int | None = market_data.get("days_to_expiry")
        put_call_ratio: float = market_data.get("put_call_ratio", 1.0)
        atm_iv: float = market_data.get("atm_iv", 50)

        if max_pain is None or price is None or days_to_expiry is None:
            return None

        if days_to_expiry > self.MAX_DAYS_TO_EXPIRY:
            return None

        deviation_pct = ((price - max_pain) / max_pain) * 100

        if abs(deviation_pct) < self.MIN_DEVIATION_PCT:
            return None

        # 价格高于 max pain → 预期下行向 max pain 回归 → SHORT
        # 价格低于 max pain → 预期上行向 max pain 回归 → LONG
        direction = "SHORT" if deviation_pct > 0 else "LONG"

        # 置信度：偏离越大越强，到期越近越强
        base_confidence = 40
        deviation_boost = min(30, int(abs(deviation_pct) - self.MIN_DEVIATION_PCT) * 3)
        expiry_boost = (self.MAX_DAYS_TO_EXPIRY - days_to_expiry) * 10  # 0天到期+30
        # Put/Call > 1 增加下行信心，< 1 增加上行信心
        pcr_boost = 0
        if direction == "SHORT" and put_call_ratio < 0.8:
            pcr_boost = 5  # 看跌保护少，更容易跌
        elif direction == "LONG" and put_call_ratio > 1.2:
            pcr_boost = 5  # 看跌保护多，做市商 gamma 推动上行

        confidence = min(100, base_confidence + deviation_boost + expiry_boost + pcr_boost)

        return TradeSignal(
            name="Max Pain 引力",
            direction=direction,
            confidence=confidence,
            signal_type="max_pain_gravity",
            components={
                "max_pain": max_pain,
                "underlying_price": price,
                "deviation_pct": round(deviation_pct, 2),
                "days_to_expiry": days_to_expiry,
                "put_call_ratio": put_call_ratio,
                "atm_iv": atm_iv,
            },
            reasoning=(
                f"距到期{days_to_expiry}天，现价${price:,.0f}偏离Max Pain"
                f"（${max_pain:,.0f}）{abs(deviation_pct):.1f}%，"
                f"做市商 gamma 对冲将推动价格向Max Pain回归，"
                f"PCR={put_call_ratio}，IV={atm_iv}，"
                f"{'做空' if direction == 'SHORT' else '做多'}偏向"
            ),
        )
