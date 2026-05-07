"""净流动性拐点信号。

逻辑：
- 计算 Net Liquidity 和 Global M2 的 13 周变化率
- 当变化率从负穿越零轴变正 → LONG（流动性扩张拐点）
- 当变化率从正穿越零轴变负 → SHORT（流动性收缩拐点）
"""

from __future__ import annotations

from src.signals.base import TradeSignal


class LiquidityInflectionSignal:
    """检测净流动性和全球 M2 的 13 周变化率零轴穿越。"""

    LOOKBACK_WEEKS = 13

    async def evaluate(self, market_data: dict) -> TradeSignal | None:
        nl_series: list[float] = market_data.get("net_liquidity_series", [])
        m2_series: list[float] = market_data.get("global_m2_series", [])

        if len(nl_series) < self.LOOKBACK_WEEKS + 2 or len(m2_series) < self.LOOKBACK_WEEKS + 2:
            return None

        nl_roc_prev = self._rate_of_change(nl_series, -2)
        nl_roc_curr = self._rate_of_change(nl_series, -1)
        m2_roc_prev = self._rate_of_change(m2_series, -2)
        m2_roc_curr = self._rate_of_change(m2_series, -1)

        nl_cross_up = nl_roc_prev < 0 and nl_roc_curr >= 0
        nl_cross_down = nl_roc_prev > 0 and nl_roc_curr <= 0
        m2_cross_up = m2_roc_prev < 0 and m2_roc_curr >= 0
        m2_cross_down = m2_roc_prev > 0 and m2_roc_curr <= 0

        # 双重确认最强，单一确认也给信号但置信度低
        direction: str | None = None
        confidence = 0

        if nl_cross_up and m2_cross_up:
            direction = "LONG"
            confidence = 80
        elif nl_cross_down and m2_cross_down:
            direction = "SHORT"
            confidence = 80
        elif nl_cross_up or m2_cross_up:
            direction = "LONG"
            confidence = 55
        elif nl_cross_down or m2_cross_down:
            direction = "SHORT"
            confidence = 55
        else:
            return None

        # 用变化率幅度微调置信度
        roc_magnitude = abs(nl_roc_curr) + abs(m2_roc_curr)
        confidence = min(100, confidence + int(roc_magnitude * 100))

        return TradeSignal(
            name="净流动性拐点",
            direction=direction,
            confidence=confidence,
            signal_type="liquidity_inflection",
            components={
                "nl_roc_13w_prev": round(nl_roc_prev, 4),
                "nl_roc_13w_curr": round(nl_roc_curr, 4),
                "m2_roc_13w_prev": round(m2_roc_prev, 4),
                "m2_roc_13w_curr": round(m2_roc_curr, 4),
                "nl_cross_up": nl_cross_up,
                "nl_cross_down": nl_cross_down,
                "m2_cross_up": m2_cross_up,
                "m2_cross_down": m2_cross_down,
            },
            reasoning=(
                f"净流动性13周变化率{'上穿' if nl_cross_up else '下穿' if nl_cross_down else '未穿越'}零轴"
                f"（{nl_roc_prev:.2%}→{nl_roc_curr:.2%}），"
                f"全球M2{'上穿' if m2_cross_up else '下穿' if m2_cross_down else '未穿越'}零轴"
                f"（{m2_roc_prev:.2%}→{m2_roc_curr:.2%}），"
                f"流动性{'扩张' if direction == 'LONG' else '收缩'}拐点确认"
            ),
        )

    @staticmethod
    def _rate_of_change(series: list[float], offset: int) -> float:
        """计算以 offset 为终点的 13 周变化率。offset=-1 表示最新一期。"""
        idx = len(series) + offset
        lookback_idx = idx - 13
        if lookback_idx < 0 or idx < 0:
            return 0.0
        base = series[lookback_idx]
        if base == 0:
            return 0.0
        return (series[idx] - base) / abs(base)
