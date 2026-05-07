"""波动率制度切换信号。

逻辑：
- DVOL 从 >80 压缩到 <40 → 波动率挤压，大行情即将到来
- 结合 risk reversal 25-delta 判断方向：
  - risk_reversal > 0（看涨偏斜）→ LONG
  - risk_reversal < 0（看跌偏斜）→ SHORT
- VIX 和 HY Spread 作为宏观风险辅助
"""

from __future__ import annotations

from src.signals.base import TradeSignal


class VolatilityRegimeSignal:
    """检测波动率制度切换（高波压缩到低波）和方向偏斜。"""

    DVOL_HIGH = 80
    DVOL_LOW = 40
    DVOL_HISTORY_MIN = 5  # 至少需要5期历史
    VIX_RISK_ON = 20
    VIX_RISK_OFF = 30
    HY_SPREAD_RISK_OFF = 500  # bps

    async def evaluate(self, market_data: dict) -> TradeSignal | None:
        dvol_current: float = market_data.get("dvol_current", 50)
        dvol_history: list[float] = market_data.get("dvol_history", [])
        risk_reversal: float = market_data.get("risk_reversal_25d", 0)
        vix: float = market_data.get("vix", 20)
        hy_spread: float = market_data.get("hy_spread", 350)

        # 检测波动率挤压：历史曾高于 80，现在低于 40
        recent_high = max(dvol_history[-20:]) if len(dvol_history) >= self.DVOL_HISTORY_MIN else 0

        if not (recent_high > self.DVOL_HIGH and dvol_current < self.DVOL_LOW):
            return None

        # 计算压缩幅度
        compression = recent_high - dvol_current
        compression_ratio = compression / recent_high if recent_high > 0 else 0

        # 方向判断：risk reversal
        if risk_reversal > 2:
            direction = "LONG"
            skew_desc = f"看涨偏斜+{risk_reversal:.1f}"
        elif risk_reversal < -2:
            direction = "SHORT"
            skew_desc = f"看跌偏斜{risk_reversal:.1f}"
        else:
            # risk reversal 中性，用宏观条件辅助判断
            if vix < self.VIX_RISK_ON and hy_spread < self.HY_SPREAD_RISK_OFF:
                direction = "LONG"
                skew_desc = f"偏斜中性但宏观风险偏好良好（VIX={vix}）"
            elif vix > self.VIX_RISK_OFF:
                direction = "SHORT"
                skew_desc = f"偏斜中性但宏观避险（VIX={vix}）"
            else:
                direction = None
                skew_desc = "方向不确定"

        if direction is None:
            return None

        # 置信度
        base_confidence = 45
        compression_boost = min(25, int(compression_ratio * 50))
        skew_boost = min(15, int(abs(risk_reversal)) * 3)
        # 宏观环境调整
        macro_boost = 0
        if direction == "LONG" and vix < self.VIX_RISK_ON:
            macro_boost = 5
        elif direction == "SHORT" and vix > self.VIX_RISK_OFF:
            macro_boost = 5

        confidence = min(100, base_confidence + compression_boost + skew_boost + macro_boost)

        return TradeSignal(
            name="波动率制度切换",
            direction=direction,
            confidence=confidence,
            signal_type="volatility_regime",
            components={
                "dvol_current": dvol_current,
                "dvol_recent_high": recent_high,
                "compression": round(compression, 1),
                "compression_ratio": round(compression_ratio, 3),
                "risk_reversal_25d": risk_reversal,
                "vix": vix,
                "hy_spread": hy_spread,
            },
            reasoning=(
                f"DVOL从{recent_high:.0f}压缩至{dvol_current:.0f}，"
                f"压缩比{compression_ratio:.0%}，波动率挤压即将释放大行情，"
                f"{skew_desc}，VIX={vix}/HY={hy_spread}bps，"
                f"{'做多' if direction == 'LONG' else '做空'}方向"
            ),
        )
