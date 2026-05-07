"""聪明钱 + 社交背离信号。

逻辑：
- 3+ tier-1 钱包集中买入/积累
- 社交媒体安静（无 spike）
- F&G < 40（恐惧区间）
→ 最强 LONG 信号（聪明钱在恐惧中悄悄建仓）

反向：
- 聪明钱分发 + 社交狂热 + F&G > 75 → SHORT
"""

from __future__ import annotations

from src.signals.base import TradeSignal


class SmartMoneySocialSignal:
    """检测聪明钱链上行为与社交情绪的背离。"""

    FG_FEAR_THRESHOLD = 40
    FG_GREED_THRESHOLD = 75

    async def evaluate(self, market_data: dict) -> TradeSignal | None:
        smart_money_cluster: bool = market_data.get("smart_money_cluster", False)
        cluster_token: str = market_data.get("cluster_token", "BTC")
        social_spike: bool = market_data.get("social_spike", False)
        fear_greed: float = market_data.get("fear_greed", 50)
        smart_money_direction: str = market_data.get("smart_money_direction", "neutral")

        # --- LONG：聪明钱积累 + 社交安静 + 恐惧 ---
        if (
            smart_money_cluster
            and smart_money_direction == "accumulating"
            and not social_spike
            and fear_greed < self.FG_FEAR_THRESHOLD
        ):
            confidence = min(100, int(
                60
                + (self.FG_FEAR_THRESHOLD - fear_greed) * 1.0
                + 10  # 社交安静 bonus
            ))
            return TradeSignal(
                name="聪明钱社交背离",
                direction="LONG",
                confidence=confidence,
                signal_type="smart_money_social",
                components={
                    "smart_money_cluster": smart_money_cluster,
                    "cluster_token": cluster_token,
                    "smart_money_direction": smart_money_direction,
                    "social_spike": social_spike,
                    "fear_greed": fear_greed,
                },
                reasoning=(
                    f"3+个tier-1钱包集中积累{cluster_token}，"
                    f"社交媒体无异常热度，F&G={fear_greed}处于恐惧区间，"
                    f"聪明钱在市场恐慌时悄悄建仓，最强做多信号"
                ),
            )

        # --- SHORT：聪明钱分发 + 社交狂热 + 贪婪 ---
        if (
            smart_money_cluster
            and smart_money_direction == "distributing"
            and social_spike
            and fear_greed > self.FG_GREED_THRESHOLD
        ):
            confidence = min(100, int(
                55
                + (fear_greed - self.FG_GREED_THRESHOLD) * 1.0
                + 10  # 社交狂热 bonus
            ))
            return TradeSignal(
                name="聪明钱社交背离",
                direction="SHORT",
                confidence=confidence,
                signal_type="smart_money_social",
                components={
                    "smart_money_cluster": smart_money_cluster,
                    "cluster_token": cluster_token,
                    "smart_money_direction": smart_money_direction,
                    "social_spike": social_spike,
                    "fear_greed": fear_greed,
                },
                reasoning=(
                    f"tier-1钱包集中分发{cluster_token}，"
                    f"社交媒体出现狂热spike，F&G={fear_greed}极度贪婪，"
                    f"聪明钱在散户狂欢时出货，强做空信号"
                ),
            )

        return None
