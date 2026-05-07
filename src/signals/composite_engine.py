"""复合信号聚合引擎。

将七个独立信号统一运行，加权投票产生共识方向和强度。
"""

from __future__ import annotations

import asyncio
import structlog

from src.signals.base import TradeSignal
from src.signals.funding_mean_reversion import FundingMeanReversionSignal
from src.signals.liquidity_inflection import LiquidityInflectionSignal
from src.signals.max_pain_gravity import MaxPainGravitySignal
from src.signals.liquidation_cascade import LiquidationCascadeSignal
from src.signals.smart_money_social import SmartMoneySocialSignal
from src.signals.exchange_reserve_divergence import ExchangeReserveDivergenceSignal
from src.signals.volatility_regime import VolatilityRegimeSignal

logger = structlog.get_logger()


# 信号权重配置（根据历史回测有效性分配）
SIGNAL_WEIGHTS: dict[str, float] = {
    "funding_reversion": 1.0,
    "liquidity_inflection": 1.2,  # 宏观流动性是最强驱动力
    "max_pain_gravity": 0.8,  # 短期有效但范围有限
    "liquidation_cascade": 1.1,
    "smart_money_social": 1.5,  # 聪明钱+社交背离历史胜率最高
    "exchange_reserve_divergence": 1.0,
    "volatility_regime": 0.9,
}

# 共识阈值
CONSENSUS_MIN_SIGNALS = 4  # 至少4个信号同向
CONSENSUS_MIN_CONFIDENCE = 65  # 平均置信度 > 65%


class CompositeSignalEngine:
    """聚合所有信号评估器，运行加权投票产生共识。"""

    def __init__(self) -> None:
        self.signals = [
            FundingMeanReversionSignal(),
            LiquidityInflectionSignal(),
            MaxPainGravitySignal(),
            LiquidationCascadeSignal(),
            SmartMoneySocialSignal(),
            ExchangeReserveDivergenceSignal(),
            VolatilityRegimeSignal(),
        ]
        self.log = logger.bind(component="composite_engine")

    async def evaluate_all(self, market_data: dict) -> list[TradeSignal]:
        """并发运行所有信号，返回触发的信号列表（忽略返回 None 的）。"""
        tasks = [sig.evaluate(market_data) for sig in self.signals]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        triggered: list[TradeSignal] = []
        for sig_instance, result in zip(self.signals, results):
            name = sig_instance.__class__.__name__
            if isinstance(result, Exception):
                self.log.warning("signal_evaluation_error", signal=name, error=str(result))
                continue
            if result is not None:
                triggered.append(result)
                self.log.info(
                    "signal_triggered",
                    signal=result.name,
                    direction=result.direction,
                    confidence=result.confidence,
                )

        self.log.info(
            "evaluation_complete",
            total_signals=len(self.signals),
            triggered=len(triggered),
        )
        return triggered

    def get_consensus(self, signals: list[TradeSignal]) -> dict:
        """加权投票产生共识方向和强度。

        规则：
        - 4+ 个信号同向 + 加权平均置信度 > 65% → 强信号
        - 否则 NEUTRAL

        Returns:
            {
                "direction": "LONG" | "SHORT" | "NEUTRAL",
                "strength": 0-100,
                "triggered_count": int,
                "consensus_count": int,
                "signals": [TradeSignal, ...],
                "reasoning": str,
            }
        """
        if not signals:
            return {
                "direction": "NEUTRAL",
                "strength": 0,
                "triggered_count": 0,
                "consensus_count": 0,
                "signals": [],
                "reasoning": "无信号触发",
            }

        # 统计加权票数
        long_weighted = 0.0
        short_weighted = 0.0
        long_signals: list[TradeSignal] = []
        short_signals: list[TradeSignal] = []

        for sig in signals:
            weight = SIGNAL_WEIGHTS.get(sig.signal_type, 1.0)
            weighted_vote = weight * sig.confidence

            if sig.direction == "LONG":
                long_weighted += weighted_vote
                long_signals.append(sig)
            elif sig.direction == "SHORT":
                short_weighted += weighted_vote
                short_signals.append(sig)

        # 确定多数方向
        if long_weighted >= short_weighted:
            majority_direction = "LONG"
            majority_signals = long_signals
            majority_weighted = long_weighted
        else:
            majority_direction = "SHORT"
            majority_signals = short_signals
            majority_weighted = short_weighted

        consensus_count = len(majority_signals)

        # 计算加权平均置信度
        if majority_signals:
            total_weight = sum(
                SIGNAL_WEIGHTS.get(s.signal_type, 1.0) for s in majority_signals
            )
            weighted_avg_confidence = majority_weighted / total_weight if total_weight > 0 else 0
        else:
            weighted_avg_confidence = 0

        # 判断是否达到共识
        is_consensus = (
            consensus_count >= CONSENSUS_MIN_SIGNALS
            and weighted_avg_confidence >= CONSENSUS_MIN_CONFIDENCE
        )

        if is_consensus:
            direction = majority_direction
            # 强度 = 加权平均置信度 * (同向数/总触发数) 归一化到 0-100
            ratio = consensus_count / len(signals) if signals else 0
            strength = min(100, int(weighted_avg_confidence * (0.5 + 0.5 * ratio)))
        else:
            direction = "NEUTRAL"
            strength = int(weighted_avg_confidence * 0.5)  # 衰减显示

        signal_names = [s.name for s in majority_signals]
        reasoning = (
            f"共{len(signals)}个信号触发，{consensus_count}个指向{majority_direction}，"
            f"加权平均置信度{weighted_avg_confidence:.0f}%，"
            f"{'达成' if is_consensus else '未达成'}共识"
            f"（需≥{CONSENSUS_MIN_SIGNALS}个同向且置信度≥{CONSENSUS_MIN_CONFIDENCE}%），"
            f"共识信号：{'、'.join(signal_names) if signal_names else '无'}"
        )

        return {
            "direction": direction,
            "strength": strength,
            "triggered_count": len(signals),
            "consensus_count": consensus_count,
            "signals": signals,
            "reasoning": reasoning,
        }
