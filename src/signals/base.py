"""Trade signal base data structure."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone


@dataclass
class TradeSignal:
    """A single trade signal emitted by a signal evaluator."""

    name: str  # 信号名称
    direction: str | None  # "LONG" / "SHORT" / None
    confidence: int  # 0-100
    signal_type: str  # "funding_reversion" / "liquidity_inflection" / etc
    components: dict  # 触发条件的具体值
    reasoning: str  # 中文一句话解释
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
