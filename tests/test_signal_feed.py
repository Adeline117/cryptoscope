"""The signal feed ranks the four get-rich directions with evidence, honestly."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from src.pipeline import signal_feed as sf


NOW = datetime(2026, 7, 17, 12, 0, tzinfo=timezone.utc)


def _launch(sym, mins_ago):
    at = (NOW - timedelta(minutes=mins_ago)).isoformat()
    return {"token": f"0x{sym}", "symbol": sym, "chain": "solana", "detected_at": at}


def _op(sym, phase, *, conf=50, buys=100, sells=100, acq="bought"):
    return {"token": f"0x{sym}", "symbol": sym, "chain": "bsc", "live_phase": phase,
            "acquisition": acq, "confidence": conf, "largest_entity_pct": 12,
            "liquidity_usd": 200000, "buys_h24": buys, "sells_h24": sells}


def _buy(sym, n):
    return {"token": f"0x{sym}", "symbol": sym, "chain": "Base",
            "n_buyers": n, "usd_bought": 5000, "mins_ago": 4}


def test_build_feed_sorts_each_direction_and_carries_evidence():
    feed = sf.build_feed(
        launch_events=[_launch("NEW", 2), _launch("OLD", 30)],
        operators=[
            _op("ACC", "buy", conf=70),
            _op("DUMP", "distribute", conf=40, buys=100, sells=900),
        ],
        smart_buys=[_buy("CONV3", 3), _buy("CONV2", 2), _buy("SOLO", 1)],
        now=NOW,
    )
    assert feed["view"] == "signals"
    d = feed["directions"]
    # 打新: newest first.
    assert [c["symbol"] for c in d["打新"]] == ["NEW", "OLD"]
    # 吸筹: buy-phase operator, evidence names the phase.
    assert d["吸筹"][0]["symbol"] == "ACC" and "买入相" in d["吸筹"][0]["why"]
    # 派发做空: distribute-phase operator, ranked by exit pressure.
    assert d["派发做空"][0]["symbol"] == "DUMP" and "派发相" in d["派发做空"][0]["why"]
    # 聪明钱: >=2 wallets only, sorted by buyer count; solo dropped.
    assert [c["symbol"] for c in d["聪明钱"]] == ["CONV3", "CONV2"]
    assert feed["n_candidates"] == 6
    assert "不是买入指令" in feed["disclaimer"]


def test_launch_safety_gate_drops_honeypots_and_flags_risk():
    events = [_launch("HONEY", 1), _launch("RISKY", 2),
              _launch("CLEAN", 3), _launch("UNCHK", 4)]
    verdicts = {
        "0xHONEY": {"available": True, "honeypot": True, "facts": ["蜜罐:买得进卖不出"]},
        "0xRISKY": {"available": True, "honeypot": False, "facts": ["可增发(owner 能凭空铸币稀释)"]},
        "0xCLEAN": {"available": True, "honeypot": False, "facts": []},
        "0xUNCHK": {"available": False},   # e.g. Solana / GoPlus miss
    }
    feed = sf.build_feed(launch_events=events, now=NOW,
                         launch_safety_fn=lambda t, c: verdicts[t])
    launch = feed["directions"]["打新"]
    syms = [c["symbol"] for c in launch]
    assert "HONEY" not in syms                       # honeypot dropped entirely
    assert {"CLEAN", "RISKY", "UNCHK"} <= set(syms)
    by = {c["symbol"]: c for c in launch}
    assert "✅ 安全已检" in by["CLEAN"]["why"]
    assert "🚨" in by["RISKY"]["why"] and "可增发" in by["RISKY"]["why"]
    assert "安全未检" in by["UNCHK"]["why"]
    # A flagged token is demoted below a clean one of similar age.
    assert by["CLEAN"]["score"] > by["RISKY"]["score"]


def test_launch_without_safety_fn_keeps_recency_only():
    # No safety_fn (the default) preserves the original recency behavior.
    feed = sf.build_feed(launch_events=[_launch("A", 1), _launch("B", 9)], now=NOW)
    assert [c["symbol"] for c in feed["directions"]["打新"]] == ["A", "B"]
    assert "安全未检" in feed["directions"]["打新"][0]["why"]


def test_high_sell_ratio_counts_as_distribute_even_without_phase():
    feed = sf.build_feed(operators=[
        _op("SILENT", "", buys=100, sells=500),   # no phase, but heavy selling
        _op("BALANCED", "", buys=100, sells=110),  # not a signal
    ], now=NOW)
    shorts = [c["symbol"] for c in feed["directions"]["派发做空"]]
    assert "SILENT" in shorts and "BALANCED" not in shorts


def test_smart_money_farm_filter_demotes_same_farm_convergence():
    # DIVERSE (independent wallets) outranks FARM (a set that keeps converging
    # together), even though FARM has more buyers, once the farm is flagged.
    def farm_fn(buyers):
        return 0.9 if "farm" in buyers[0] else 0.1
    feed = sf.build_feed(
        smart_buys=[
            {"token": "0xFARM", "symbol": "FARM", "chain": "Base", "n_buyers": 4,
             "buyers": ["farm1", "farm2", "farm3"], "usd_bought": 5000, "mins_ago": 4},
            {"token": "0xDIV", "symbol": "DIV", "chain": "Base", "n_buyers": 3,
             "buyers": ["z1", "z2", "z3"], "usd_bought": 5000, "mins_ago": 4},
        ],
        now=NOW, smart_farm_fn=farm_fn,
    )
    rows = {c["symbol"]: c for c in feed["directions"]["聪明钱"]}
    assert "疑似同农场" in rows["FARM"]["why"]
    assert "疑似同农场" not in rows["DIV"]["why"]
    assert rows["DIV"]["score"] > rows["FARM"]["score"]   # diverse wins despite fewer buyers


def test_empty_sources_produce_an_honest_empty_feed():
    feed = sf.build_feed(now=NOW)
    assert feed["n_candidates"] == 0
    assert all(feed["directions"][d] == [] for d in sf.DIRECTIONS)


def test_launch_coverage_distinguishes_blind_from_backfill(monkeypatch):
    from src.pipeline import signal_feed, stream_health

    # A stale live launch stream = currently blind → loud warning.
    monkeypatch.setattr(stream_health, "snapshot", lambda now=None: [
        {"source": "solana", "stream": "pump_fun_launches", "status": "disconnected",
         "stale": True, "open_gaps": 2},
    ])
    cov = signal_feed._launch_coverage()
    assert cov["blind"] is True and "当前新盘可能漏采" in cov["note"]

    # A live stream with only historical gaps = backfilling, not blind.
    monkeypatch.setattr(stream_health, "snapshot", lambda now=None: [
        {"source": "solana", "stream": "pump_fun_launches", "status": "degraded",
         "stale": False, "open_gaps": 2},
    ])
    cov = signal_feed._launch_coverage()
    assert cov["blind"] is False and "历史缺口回补中" in cov["note"]


def test_coverage_note_rides_the_feed_into_the_digest():
    feed = sf.build_feed(launch_events=[_launch("A", 1)], now=NOW,
                         coverage={"note": "⚠️ 实时采集异常", "blind": True})
    assert "覆盖:⚠️ 实时采集异常" in sf.format_text(feed)


def test_format_text_groups_by_direction_and_omits_empty():
    feed = sf.build_feed(
        launch_events=[_launch("NEW", 1)],
        smart_buys=[_buy("CONV", 3)],
        now=NOW,
    )
    text = sf.format_text(feed)
    assert "🚀 打新" in text and "NEW" in text
    assert "🐋 聪明钱" in text and "CONV" in text
    # No operators supplied → those sections are omitted, not shown empty.
    assert "吸筹" not in text and "派发做空" not in text
    assert "不是买入指令" in text


def test_run_writes_signals_and_skips_telegram(monkeypatch, tmp_path):
    monkeypatch.setattr(sf, "EXPORT_DIR", tmp_path)
    monkeypatch.setattr(sf, "SIGNALS_FILE", tmp_path / "signals.json")
    (tmp_path / "launch.json").write_text(
        '{"events":[{"token":"0xA","symbol":"A","chain":"solana",'
        f'"detected_at":"{NOW.isoformat()}"}}]}}')
    monkeypatch.setattr(
        "src.onchain.smart_wallets.fresh_smart_buys_result",
        lambda *a, **k: {"buys": [_buy("C", 3)]})
    # Isolate from the real Solana new-pool DB (run() merges it into 打新).
    monkeypatch.setattr("src.pipeline.solana_new_pools.recent", lambda **k: [])

    # push_blob=False: never touch the real Vercel blob from a test — src.config
    # auto-loads .env, so BLOB_READ_WRITE_TOKEN is present and an unguarded push
    # would clobber the live signals.json with fixture data.
    out = sf.run(now=NOW, push_telegram=False, push_blob=False)
    assert out["n_candidates"] == 2 and out["telegram_pushed"] is False

    import json
    written = json.loads((tmp_path / "signals.json").read_text())
    assert written["view"] == "signals" and written["n_candidates"] == 2
