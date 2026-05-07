"""Collector registry — only trading-relevant collectors."""

from __future__ import annotations

import importlib
from typing import Any

import structlog

logger = structlog.get_logger()

# Only trading-relevant collectors
COLLECTORS = [
    ("src.collectors.fear_greed", "FearGreedCollector"),
    ("src.collectors.price_tracker", "CoinGeckoCollector"),
    ("src.collectors.derivatives", "CoinglassCollector"),
    ("src.collectors.derivatives", "BinanceFuturesCollector"),
    ("src.collectors.exchange_reserves", "ExchangeReserveCollector"),
    ("src.collectors.liquidation_monitor", "LiquidationMonitor"),
    ("src.collectors.new_pool_detector", "NewPoolDetector"),
    ("src.collectors.token_unlocks", "TokenUnlockCollector"),
    ("src.collectors.meme_scanner", "PumpFunCollector"),
    ("src.collectors.meme_scanner", "DexScreenerTrendingCollector"),
    ("src.collectors.smart_money", "SmartMoneyCollector"),
]


def try_import_collector(module_path: str, class_name: str) -> Any | None:
    try:
        mod = importlib.import_module(module_path)
        return getattr(mod, class_name)
    except (ImportError, AttributeError) as e:
        logger.debug("collector_unavailable", cls=class_name, error=str(e))
        return None


def build_collector_instances() -> list:
    collectors = []
    for mod_path, cls_name in COLLECTORS:
        cls = try_import_collector(mod_path, cls_name)
        if cls:
            collectors.append(cls())
    return collectors
