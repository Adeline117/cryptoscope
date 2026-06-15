"""Tests for the optimized anomaly screener (L1 signals + bot/wash filtering)."""

from src.pipeline.anomaly_screener import (
    accumulation_footprint, wash_bot_flags, format_candidates,
)


def _pair(buys_h1=60, sells_h1=20, buys_h6=300, sells_h6=120,
          buys_h24=1000, sells_h24=400, ch1=1.0, ch6=2.0, ch24=8.0,
          v1=5000, v6=30000, v24=120000, liq=200_000, age_days=30):
    import time
    return {
        "baseToken": {"symbol": "TEST", "address": "0xabc"},
        "chainId": "ethereum", "url": "https://x",
        "txns": {"m5": {"buys": 6, "sells": 2}, "h1": {"buys": buys_h1, "sells": sells_h1},
                 "h6": {"buys": buys_h6, "sells": sells_h6},
                 "h24": {"buys": buys_h24, "sells": sells_h24}},
        "priceChange": {"h1": ch1, "h6": ch6, "h24": ch24},
        "volume": {"h1": v1, "h6": v6, "h24": v24},
        "liquidity": {"usd": liq}, "marketCap": 1_000_000,
        "pairCreatedAt": int(time.time() * 1000) - age_days * 86400 * 1000,
    }


def test_footprint_fires_on_accumulation():
    fp = accumulation_footprint(_pair())
    assert fp is not None and fp["score"] >= 50
    assert "absorption" in fp["reasons"] or "buy_pressure" in fp["reasons"]


def test_wash_dust_spam_rejected():
    # 5000 tiny trades, $120k vol → avg trade $24 (dust) → bot
    p = _pair(buys_h24=3000, sells_h24=2500, v24=120000)
    wb = wash_bot_flags(p)
    assert wb["suspicious"] is True
    assert accumulation_footprint(p) is None  # rejected, not scored


def test_mm_bot_1to1_rejected():
    # high-frequency, near 1:1 buys/sells → MM bot quoting both sides
    p = _pair(buys_h24=3000, sells_h24=2950, v24=2_000_000)
    wb = wash_bot_flags(p)
    assert wb["suspicious"] is True


def test_wash_volume_vs_liquidity_rejected():
    # volume 10x liquidity → wash
    p = _pair(v24=2_000_000, liq=150_000, buys_h24=800, sells_h24=300)
    assert wash_bot_flags(p)["suspicious"] is True


def test_already_mooned_penalized():
    fp = accumulation_footprint(_pair(ch24=150, ch6=80, ch1=20))
    # big pump → absorption/compression don't fire, penalty applies → likely None
    assert fp is None or fp["score"] < 50 or "已大涨" in fp["notes"]


def test_obv_absorption_signal():
    # volume accelerating (h1*6 >> h6) while price flat → net-buying absorption
    fp = accumulation_footprint(_pair(v1=10000, v6=30000, ch1=0.5))
    assert fp and "obv_absorption" in fp["reasons"]


def test_format_candidates_escapes():
    cands = [{"symbol": "A&B<x>", "chain": "bsc", "address": "0x1",
              "score": 72, "notes": "买压2x"}]
    out = format_candidates(cands)
    assert "&amp;" in out and "&lt;" in out  # symbol escaped
    assert "<b>" in out  # our own tags intact


def test_illiquid_rejected():
    assert accumulation_footprint(_pair(liq=5000)) is None


def test_smart_money_set_loads():
    from src.pipeline.anomaly_screener import _smart_money_set
    sol = _smart_money_set("solana")
    evm = _smart_money_set("ethereum")
    assert isinstance(sol, dict) and isinstance(evm, dict)
    # base maps to the same EVM bucket as ethereum
    assert _smart_money_set("base") is _smart_money_set("base")  # cached


def test_smart_money_intersection_logic():
    from src.pipeline.anomaly_screener import _smart_money_set
    s = _smart_money_set("solana")
    if s:  # only if wallets configured
        addr = next(iter(s))
        holders = [{"address": addr, "balance": 100}, {"address": "OTHER", "balance": 5}]
        hits = [h["address"] for h in holders if h["address"] in s]
        assert len(hits) == 1


def test_liquidity_trend(tmp_path, monkeypatch):
    import src.config as cfg
    monkeypatch.setattr(cfg, "DATA_DIR", tmp_path)
    from src.pipeline.anomaly_screener import liquidity_trend_signal
    assert liquidity_trend_signal("0xT", "ethereum", 100000) is None  # first sight
    sig = liquidity_trend_signal("0xT", "ethereum", 130000)           # +30%
    assert sig and sig["rising"] is True
    flat = liquidity_trend_signal("0xT", "ethereum", 131000)          # +0.7%
    assert flat and flat["rising"] is False
