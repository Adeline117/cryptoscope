"""清算级联信号。

逻辑：
- 当前价 +/-10% 范围内 > $500M 清算量 → 级联风险高
- 接近支撑位 + 24h 清算量已升高 → 下行级联风险
- 远离支撑位 + 清算量集中在上方 → 上行挤空可能
"""

from __future__ import annotations

from src.signals.base import TradeSignal


class LiquidationCascadeSignal:
    """检测清算热力图中的级联风险。"""

    LIQ_AT_RISK_THRESHOLD = 500_000_000  # $500M
    LIQ_24H_WARNING = 150_000_000  # $150M 24h 清算量预警

    async def evaluate(self, market_data: dict) -> TradeSignal | None:
        liq_at_risk: float = market_data.get("liq_at_risk", 0)
        liq_24h_total: float = market_data.get("liq_24h_total", 0)
        price_near_support: bool = market_data.get("price_near_support", False)
        gas_gwei: float = market_data.get("gas_gwei", 0)

        if liq_at_risk < self.LIQ_AT_RISK_THRESHOLD:
            return None

        # 高清算量 + 靠近支撑位 → 下行级联（多头止损引发瀑布）
        # 高清算量 + 远离支撑位 → 可能是空头集中区，挤空向上
        if price_near_support:
            direction = "SHORT"
            scenario = "下行级联"
            detail = "价格接近支撑位，密集多头清算将引发瀑布式下跌"
        else:
            direction = "LONG"
            scenario = "空头挤压"
            detail = "清算量集中在上方，突破可能触发空头级联清算推动上行"

        # 置信度
        base_confidence = 45
        liq_boost = min(25, int((liq_at_risk - self.LIQ_AT_RISK_THRESHOLD) / 100_000_000))
        activity_boost = min(15, int(liq_24h_total / 100_000_000))
        # 高 gas 暗示链上活跃，可能加速级联
        gas_boost = min(10, int(gas_gwei / 50) * 5) if gas_gwei > 30 else 0

        confidence = min(100, base_confidence + liq_boost + activity_boost + gas_boost)

        return TradeSignal(
            name="清算级联",
            direction=direction,
            confidence=confidence,
            signal_type="liquidation_cascade",
            components={
                "liq_at_risk": liq_at_risk,
                "liq_at_risk_B": round(liq_at_risk / 1e9, 2),
                "liq_24h_total": liq_24h_total,
                "price_near_support": price_near_support,
                "gas_gwei": gas_gwei,
            },
            reasoning=(
                f"±10%范围内清算量${liq_at_risk / 1e9:.1f}B超过阈值，"
                f"24h已清算${liq_24h_total / 1e6:.0f}M，"
                f"Gas={gas_gwei}gwei，{detail}，{scenario}风险信号"
            ),
        )
