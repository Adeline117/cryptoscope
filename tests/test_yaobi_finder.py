"""妖币 finder: only VERIFIED real operators enter the watchlist — no ghosts, no
issuers, no subset-ratio concentration. The night's whole point, applied to discovery.
"""

import src.pipeline.yaobi_finder as yf


def _cand(**o):
    base = {"address": "0xt", "chain": "bsc", "symbol": "X", "price": 1.0,
            "liq": 100_000, "vol": 50_000, "age_days": 5, "mcap": 5e6, "ch24": 10}
    base.update(o)
    return base


def _sig(monkeypatch, **s):
    monkeypatch.setattr("src.onchain.holder_snapshot.fetch_holders_evm",
                        lambda *a, **k: [{"address": "0xa", "balance": 1}])
    base = {"supply_verified": True, "cluster_confidence": 60, "largest_entity_pct": 20,
            "concentration_gap": 10, "dominant_cluster_wallets": ["0x1", "0x2", "0x3", "0x4"]}
    base.update(s)
    monkeypatch.setattr("src.pipeline.anomaly_screener.effective_concentration_signal",
                        lambda *a, **k: base)


def test_concentration_routes_to_short(monkeypatch):
    """Concentration is a DUMP tell now — a verified operator cluster routes to SHORT."""
    _sig(monkeypatch)  # gap=10 -> operator concentration
    monkeypatch.setattr("src.onchain.operator_id.acquisition_mode",
                        lambda *a, **k: {"verdict": "bought"})
    r = yf.classify(_cand(txns={"h1": {"buys": 5, "sells": 50}, "h6": {"buys": 30}}))
    assert r is not None and r["direction"] == "short"


def test_allocated_issuer_is_rejected(monkeypatch):
    _sig(monkeypatch)
    monkeypatch.setattr("src.onchain.operator_id.acquisition_mode",
                        lambda *a, **k: {"verdict": "allocated"})
    assert yf.classify(_cand(txns={"h1": {"buys": 5, "sells": 5}, "h6": {"buys": 30}})) is None


def test_unverified_supply_is_rejected(monkeypatch):
    _sig(monkeypatch, supply_verified=False)
    assert yf.classify(_cand()) is None


def test_healthy_but_no_demand_is_none(monkeypatch):
    """Healthy distribution but no buy pressure = not a long yet."""
    _sig(monkeypatch, cluster_confidence=10, concentration_gap=1,
         largest_entity_pct=3, dominant_cluster_wallets=["0x1"])
    r = yf.classify(_cand(txns={"h1": {"buys": 2, "sells": 5}, "h6": {"buys": 20}}))
    assert r is None


def test_healthy_plus_demand_plus_smart_money_is_long(monkeypatch):
    """会涨: healthy dist + buy pressure + smart-money convergence = LONG."""
    _sig(monkeypatch, cluster_confidence=10, concentration_gap=1,
         largest_entity_pct=4, dominant_cluster_wallets=[])
    monkeypatch.setattr("src.onchain.goplus_client.rug_risk",
                        lambda t, c: {"available": True, "facts": []})
    monkeypatch.setattr("src.onchain.smart_money.convergence",
                        lambda t, c, **k: {"verdict": "convergence", "skilled_entities": 3})
    r = yf.classify(_cand(txns={"h1": {"buys": 40, "sells": 10}, "h6": {"buys": 120}}))
    assert r is not None and r["direction"] == "long" and "聪明钱" in r["shape"]


def test_empty_holders_is_rejected(monkeypatch):
    monkeypatch.setattr("src.onchain.holder_snapshot.fetch_holders_evm", lambda *a, **k: [])
    assert yf.classify(_cand()) is None


def test_persist_and_watchlist_roundtrip(tmp_path, monkeypatch):
    monkeypatch.setattr(yf, "DB_PATH", tmp_path / "wl.db")
    yf._persist({"address": "0xAbC", "chain": "bsc", "symbol": "Q", "price": 0.01,
                 "liq": 1e5, "mcap": 2e6, "age_days": 4, "op_score": 55, "shape": "隐藏簇",
                 "acquisition": "bought", "largest_pct": 20, "gap": 12, "cluster_n": 5})
    wl = yf.watchlist()
    assert len(wl) == 1 and wl[0]["symbol"] == "Q" and wl[0]["token"] == "0xabc"
    # idempotent: same token not double-added
    yf._persist({"address": "0xABC", "chain": "bsc", "symbol": "Q", "price": 0.02,
                 "liq": 1e5, "mcap": 2e6, "age_days": 5, "op_score": 60, "shape": "隐藏簇",
                 "acquisition": "bought", "largest_pct": 20, "gap": 12, "cluster_n": 5})
    assert len(yf.watchlist()) == 1, "first-seen must be preserved, not overwritten"


def test_capture_and_monitor_funnel(tmp_path, monkeypatch):
    """Fresh candidates are captured to the monitor pool and re-surface as due for
    re-analysis while still in the age window — the funnel that turns a sparse per-scan
    yield into an accumulating one."""
    monkeypatch.setattr(yf, "DB_PATH", tmp_path / "wl.db")
    yf._monitor_add([{"address": "0xA", "chain": "bsc", "symbol": "A"},
                     {"address": "0xB", "chain": "bsc", "symbol": "B"}])
    due = yf._monitor_due()
    assert {d["token"] for d in due} == {"0xa", "0xb"}
    assert yf._monitor_size() == 2

    # promoting one removes it from the due/monitor-open pool
    yf._monitor_touch("0xA", "bsc", promoted=True)
    assert {d["token"] for d in yf._monitor_due()} == {"0xb"}
    assert yf._monitor_size() == 1


def test_monitor_add_is_idempotent(tmp_path, monkeypatch):
    monkeypatch.setattr(yf, "DB_PATH", tmp_path / "wl.db")
    yf._monitor_add([{"address": "0xA", "chain": "bsc", "symbol": "A"}])
    yf._monitor_add([{"address": "0xa", "chain": "bsc", "symbol": "A"}])   # same, lower
    assert yf._monitor_size() == 1, "first_seen must be preserved, not re-added"
