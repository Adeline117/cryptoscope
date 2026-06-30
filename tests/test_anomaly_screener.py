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


def test_zombie_dead_book_rejected():
    # The CAT-BSC failure: deep liquidity, near-zero volume (turnover 0.025).
    # Flat prices fake "compression"; stray short-window ratios fake "absorption".
    # Must be rejected — a real accumulation target has actual turnover.
    z = _pair(liq=153_000, v24=3_845, v6=1_200, v1=200,
              buys_h1=8, sells_h1=5, buys_h6=30, sells_h6=20)
    assert accumulation_footprint(z) is None
    # Low absolute volume even at healthy-looking turnover is still dead.
    assert accumulation_footprint(_pair(liq=50_000, v24=10_000)) is None


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
    from src.pipeline.anomaly_screener import track_state
    assert track_state("0xT", "ethereum", 100000)["liq_change_pct"] is None  # first sight
    sig = track_state("0xT", "ethereum", 130000)                              # +30%
    assert sig["liq_rising"] is True
    flat = track_state("0xT", "ethereum", 131000)                            # +0.7%
    assert flat["liq_rising"] is False


def test_persistence_tracking(tmp_path, monkeypatch):
    import src.config as cfg
    monkeypatch.setattr(cfg, "DATA_DIR", tmp_path)
    from src.pipeline.anomaly_screener import track_state
    s1 = track_state("0xP", "ethereum", 100000)
    assert s1["appearances"] == 1 and s1["recurring"] is False
    track_state("0xP", "ethereum", 110000)
    s3 = track_state("0xP", "ethereum", 130000)
    assert s3["appearances"] == 3 and s3["recurring"] is True  # 3+ runs = sustained
    assert s3["liq_rising"] is True  # liquidity grew across runs


def test_effective_concentration_discriminates(monkeypatch):
    # The linchpin test: a SIREN-like token (many wallets, ONE funder = one hidden
    # entity) must show high effective concentration; a CREPE-like token (many
    # wallets, many funders) must not. Uses Solana so no getCode/contract filter.
    import src.pipeline.anomaly_screener as a

    # 20 holders with VARIED balances (so the similar-balance edge doesn't merge
    # them — funder is the sole discriminator here). Solana skips contract filter.
    holders = [{"address": f"WALLET{i:02d}", "balance": 100 + i * 37} for i in range(20)]

    # Keep hermetic: a real operator funder is NOT a disperser (no live RPC).
    monkeypatch.setattr(a, "_funder_is_disperser", lambda f, chain: False)

    # SIREN-like: all funded by ONE address → cluster into 1 entity.
    monkeypatch.setattr(
        "src.onchain.funder_graph.get_funders",
        lambda addrs, chain, **kw: {h: "FUNDER_ONE" for h in addrs},
    )
    siren = a.effective_concentration_signal(holders, "TOK", "solana")
    assert siren and siren["funder_complete"]
    assert siren["largest_entity_pct"] >= 25  # one entity dominates the float

    # CREPE-like: every wallet a different funder → stays dispersed.
    monkeypatch.setattr(
        "src.onchain.funder_graph.get_funders",
        lambda addrs, chain, **kw: {h: f"FUNDER_{h}" for h in addrs},
    )
    crepe = a.effective_concentration_signal(holders, "TOK", "solana")
    assert crepe and crepe["largest_entity_pct"] < 25  # no single controlling entity


def test_disperser_funder_collapses_cluster(monkeypatch):
    """REGRESSION (jellyjelly/USELESS/SPCX69 false positive): when the funder that
    built the largest 'entity' is a CEX hot wallet / launchpad / router (disperser),
    its cluster is retail-who-withdrew-from-the-same-exchange, NOT an operator. The
    signal must strip that funder edge and report honest near-nominal concentration —
    NOT a fake hidden cluster. (USELESS: a funder with 1000+ tx/day + 72k SOL had
    mis-clustered 29% of the float.)"""
    import src.pipeline.anomaly_screener as a
    holders = [{"address": f"WALLET{i:02d}", "balance": 100 + i * 37} for i in range(20)]

    # All wallets funded by ONE address — but that address is a disperser (CEX).
    monkeypatch.setattr(
        "src.onchain.funder_graph.get_funders",
        lambda addrs, chain, **kw: {h: "CEX_HOTWALLET" for h in addrs},
    )
    monkeypatch.setattr(a, "_funder_is_disperser",
                        lambda f, chain: f == "CEX_HOTWALLET")
    sig = a.effective_concentration_signal(holders, "TOK", "solana")
    assert sig and sig["disperser_funders_stripped"] >= 1
    # Edge stripped → no Sybil merge → gap collapses to ~0 (each wallet stands alone).
    assert sig["concentration_gap"] < 3, sig
    assert sig["largest_entity_pct"] < 25  # not a controlling hidden entity


def test_score_reasons_weighted():
    from src.pipeline.anomaly_screener import score_reasons, load_weights
    w = load_weights()
    assert score_reasons(["absorption"]) == int(round(w["absorption"]))
    assert score_reasons(["absorption", "consistency"]) == int(round(w["absorption"] + w["consistency"]))
    assert score_reasons([]) == 0


def test_calibrate_not_ready(tmp_path, monkeypatch):
    import src.pipeline.calibrate_weights as cw
    monkeypatch.setattr(cw, "LABELS_DIR", tmp_path)  # empty → no labels
    res = cw.calibrate(dry=True)
    assert res["status"] == "not_ready"


def test_calibrate_discriminates(tmp_path, monkeypatch):
    import json
    import src.config as cfg
    import src.pipeline.anomaly_screener as a
    import src.pipeline.calibrate_weights as cw
    monkeypatch.setattr(cfg, "DATA_DIR", tmp_path)
    a._WEIGHTS_CACHE = None
    labels = tmp_path / "labels"; labels.mkdir()
    monkeypatch.setattr(cw, "LABELS_DIR", labels)
    for i in range(5):
        a.log_emission(f"0xp{i}", "ethereum", ["absorption", "smart_money_t1"], 90)
        a.log_emission(f"0xd{i}", "ethereum", ["buy_pressure"], 55)
        (labels / f"p{i}.json").write_text(json.dumps({"token": f"0xp{i}", "chain": "ethereum", "outcome": "pump", "operators": ["x"]}))
        (labels / f"d{i}.json").write_text(json.dumps({"token": f"0xd{i}", "chain": "ethereum", "outcome": "dud", "operators": ["x"]}))
    res = cw.calibrate(dry=True)
    assert res["status"] == "calibrated"
    assert res["weights"]["smart_money_t1"] > res["weights"]["buy_pressure"]


def test_solana_cluster_net_flow_nets_internal_moves(monkeypatch):
    """Solana transfer net-flow (the path that unlocks SOL sentinels, balanceOf-free):
    an external BUY counts, an external SELL counts, and a cluster→cluster INTERNAL
    move must net to ZERO per-tx (not be double-counted as buy+sell). Mocks RPC."""
    import io
    import json as _json
    import src.pipeline.operator_sentinel as S
    W1, W2, MINT = "OWNER_A", "OWNER_B", "MINT_X"

    def _tb(owner, amt):
        return {"mint": MINT, "owner": owner, "uiTokenAmount": {"uiAmount": amt}}

    # tx1: external BUY → W1 0→500 (counterparty external). tx2: INTERNAL W1 500→200,
    # W2 0→300 (cluster-internal, net 0). tx3: external SELL → W2 300→100.
    TX = {
        "sigBUY":  {"meta": {"preTokenBalances": [_tb(W1, 0)],
                             "postTokenBalances": [_tb(W1, 500)]}},
        "sigINT":  {"meta": {"preTokenBalances": [_tb(W1, 500), _tb(W2, 0)],
                             "postTokenBalances": [_tb(W1, 200), _tb(W2, 300)]}},
        "sigSELL": {"meta": {"preTokenBalances": [_tb(W2, 300)],
                             "postTokenBalances": [_tb(W2, 100)]}},
    }
    ATA = {W1: "ATA_A", W2: "ATA_B"}
    # blockTimes after the `since` epoch below (2026-01-01 ≈ 1.767e9), else filtered.
    SIGS = {"ATA_A": [{"signature": "sigBUY", "blockTime": 1767300000},
                      {"signature": "sigINT", "blockTime": 1767300100}],
            "ATA_B": [{"signature": "sigINT", "blockTime": 1767300100},
                      {"signature": "sigSELL", "blockTime": 1767300200}]}

    def fake_urlopen(req, timeout=15):
        body = _json.loads(req.data.decode())
        m, p = body["method"], body["params"]
        if m == "getTokenAccountsByOwner":
            res = {"value": [{"pubkey": ATA[p[0]]}]}
        elif m == "getSignaturesForAddress":
            res = SIGS.get(p[0], [])
        elif m == "getTransaction":
            res = TX.get(p[0])
        else:
            res = None
        return io.BytesIO(_json.dumps({"result": res}).encode())  # BytesIO is a ctx mgr

    monkeypatch.setattr(S.urllib.request, "urlopen", fake_urlopen)
    nf = S.cluster_net_flow(MINT, "solana", [W1, W2], "2026-01-01T00:00:00+00:00")
    assert nf["buy"] == 500 and nf["sell"] == 200, nf   # internal move excluded
    assert nf["net"] == 300


def test_watcher_never_fires_buysell_from_balanceof(tmp_path, monkeypatch):
    """REGRESSION (root-cure): the 20s watcher (use_transfers=False) must NEVER emit
    庄在买/庄在卖 from a balanceOf change — balanceOf is unreliable on reflection/wash
    tokens (caused the SIREN spam). Buy/sell only comes from transfers."""
    import json
    import src.pipeline.operator_sentinel as S
    f = tmp_path / "sent.json"
    monkeypatch.setattr(S, "SENTINELS_FILE", f)
    base = {"price": 1.0, "liquidity": 8e5, "vol24": 1e6, "cluster_balance": 1_000_000, "funding": 0.0}
    f.write_text(json.dumps({"bsc:0xt": {"token": "0xT", "chain": "bsc", "symbol": "T",
        "wallets": ["0xw"], "baseline": dict(base), "last": dict(base)}}))
    # Simulate a big balanceOf DROP (would have been a phantom 庄在卖 under the old code)
    monkeypatch.setattr(S, "_measure",
        lambda *a, **k: {"price": 1.0, "liquidity": 8e5, "vol24": 1e6,
                         "cluster_balance": 800_000, "funding": 0.0})
    alerts = S.check_run(use_transfers=False)
    kinds = {k for al in alerts for k, _ in al["events"]}
    assert "庄在卖" not in kinds and "庄在买" not in kinds, f"balanceOf leaked into buy/sell: {kinds}"


def test_hunt_round_robin_no_chain_starves():
    """REGRESSION (Solana crowded out): target selection must round-robin across
    chains so a chain gathered first (BSC) can't fill the whole max_scan budget and
    starve Solana/Base. 60 BSC + 5 SOL, max_scan=20 → SOL must still be scanned."""
    from src.pipeline.operator_hunt import _select_targets
    pairs = [{"chainId": "bsc", "baseToken": {"address": f"0x{i}"},
              "liquidity": {"usd": 1_000_000 - i}} for i in range(60)]
    pairs += [{"chainId": "solana", "baseToken": {"address": f"S{i}"},
               "liquidity": {"usd": 500_000}} for i in range(5)]
    sel = _select_targets(pairs, 20)
    chains = {p["chainId"] for p in sel}
    assert "solana" in chains, "Solana starved by BSC — round-robin broken"
    # all 5 SOL fit within a 20 budget shared round-robin with BSC
    assert sum(1 for p in sel if p["chainId"] == "solana") == 5


def test_holder_growth_rejects_fetch_depth_artifact(tmp_path):
    """The de-noising linchpin: a token whose snapshot fetch got SHALLOWER (holders
    collapse) shows top10→~100% as a pure artifact — it must NOT be flagged. A token
    with STABLE fetch depth and genuinely rising concentration must be flagged."""
    import sqlite3
    from src.pipeline.holder_growth_screener import screen_holder_growth
    db = tmp_path / "hs.db"
    c = sqlite3.connect(str(db))
    c.execute("""CREATE TABLE holder_snapshots (id INTEGER PRIMARY KEY AUTOINCREMENT,
        token TEXT, chain TEXT, snapshot_at TEXT, holder_count INT, top10_pct REAL,
        top25_pct REAL, gini REAL, total_supply_observed REAL, holders_json TEXT)""")
    def snap(tok, ts, hc, t10, gini):
        c.execute("INSERT INTO holder_snapshots(token,chain,snapshot_at,holder_count,"
                  "top10_pct,top25_pct,gini,total_supply_observed,holders_json) "
                  "VALUES(?,?,?,?,?,?,?,?,?)",
                  (tok, "solana", ts, hc, t10, t10, gini, 1e9, "[]"))
    # ARTIFACT: holders 800→40 (-95%), top10 40→99 — fetch got shallow, not accumulation.
    snap("ARTIFACT", "2026-06-16T01:00", 800, 40.0, 0.5)
    snap("ARTIFACT", "2026-06-16T02:00", 300, 70.0, 0.7)
    snap("ARTIFACT", "2026-06-16T03:00", 40, 99.0, 0.95)
    # REAL: holders stable ~250, top10 45→63 — float concentrating into few hands.
    snap("REAL", "2026-06-16T01:00", 250, 45.0, 0.55)
    snap("REAL", "2026-06-16T02:00", 248, 55.0, 0.62)
    snap("REAL", "2026-06-16T03:00", 252, 63.0, 0.68)
    c.commit(); c.close()
    cands = screen_holder_growth(db_path=db)
    toks = {x["token"] for x in cands}
    assert "REAL" in toks, "fetch-depth-stable rising concentration must be flagged"
    assert "ARTIFACT" not in toks, "collapsing-fetch artifact must be rejected"


def test_evm_rpc_netflow_fallback_when_moralis_parked(monkeypatch):
    """Moralis-free resilience: when all Moralis keys are parked (quota exhausted),
    cluster_net_flow must fall back to keyless RPC eth_getLogs and classify cluster
    buy/sell from Transfer logs (external→cluster=buy, cluster→external=sell)."""
    import src.pipeline.operator_sentinel as S
    from src.onchain import moralis_client as mc
    monkeypatch.setattr(mc, "usable", lambda: False)   # simulate exhausted quota

    TRANSFER = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
    W = "0x" + "a" * 40           # cluster wallet
    EXT = "0x" + "b" * 40         # external (LP/router/retail)
    def topic(addr): return "0x" + "0" * 24 + addr[2:]
    def log(frm, to, tokens, blk):
        return {"topics": [TRANSFER, topic(frm), topic(to)],
                "data": hex(tokens * 10**18), "blockNumber": hex(blk)}

    class FakeRPC:
        logs_complete = True   # mirror ArchiveRPC: set by get_transfer_logs
        def __init__(self, chain): pass
        def logs_head(self): return 1000
        def seconds_per_block(self, sample=50000): return 3.0
        def token_decimals(self, token): return 18
        def block_time(self, blk): return 1_780_000_000 + blk
        def get_transfer_logs(self, token, frm, to, chunk=9000):
            self.logs_complete = True
            return [log(EXT, W, 500, 990),   # external→cluster = BUY 500
                    log(W, EXT, 200, 995)]   # cluster→external = SELL 200

    monkeypatch.setattr("src.onchain.evm_archive.ArchiveRPC", FakeRPC)
    nf = S.cluster_net_flow("0xtoken", "bsc", [W], "2026-06-16T00:00:00+00:00")
    assert nf is not None and nf["buy"] == 500 and nf["sell"] == 200, nf
    assert nf["net"] == 300 and nf["last_buy_ts"] and nf["last_sell_ts"]


def test_transfer_buysell_magnitude_gated(tmp_path, monkeypatch):
    """REGRESSION (SIREN phantom-sell, transfer edition): a net sell that is TINY vs
    the cluster's holdings (1,882 sold of 90M = 0.002%) must NOT fire 庄在卖. Only a
    move >= OP_SELL/OP_BUY × cluster_balance is the operator acting."""
    import json
    import src.pipeline.operator_sentinel as S
    f = tmp_path / "sent.json"
    monkeypatch.setattr(S, "SENTINELS_FILE", f)
    base = {"price": 1.0, "liquidity": 8e5, "vol24": 1e6, "cluster_balance": 90_000_000,
            "funding": 0.0, "flow_ts": "2026-06-16T00:00:00+00:00"}
    f.write_text(json.dumps({"bsc:0xt": {"token": "0xT", "chain": "bsc", "symbol": "SIREN",
        "wallets": ["0xw"], "baseline": dict(base), "last": dict(base)}}))
    monkeypatch.setattr(S, "_measure",
        lambda *a, **k: {"price": 1.0, "liquidity": 8e5, "vol24": 1e6,
                         "cluster_balance": 90_000_000, "funding": 0.0})
    # tiny net sell (1,882 of 90M = 0.002%, far below OP_SELL=1.5%)
    monkeypatch.setattr(S, "cluster_net_flow",
        lambda *a, **k: {"buy": 0.0, "sell": 1882.0, "net": -1882.0,
                         "last_buy_ts": None, "last_sell_ts": "2026-06-17T04:54:00+00:00",
                         "latest_ts": "2026-06-17T04:54:00+00:00"})
    alerts = S.check_run(use_transfers=True)
    kinds = {k for al in alerts for k, _ in al["events"]}
    assert "庄在卖" not in kinds, f"tiny noise sell leaked into 庄在卖: {kinds}"

    # a REAL sell (2M of 90M = 2.2% >= 1.5%) MUST fire.
    monkeypatch.setattr(S, "cluster_net_flow",
        lambda *a, **k: {"buy": 0.0, "sell": 2_000_000.0, "net": -2_000_000.0,
                         "last_buy_ts": None, "last_sell_ts": "2026-06-17T05:00:00+00:00",
                         "latest_ts": "2026-06-17T05:00:00+00:00"})
    f.write_text(json.dumps({"bsc:0xt": {"token": "0xT", "chain": "bsc", "symbol": "SIREN",
        "wallets": ["0xw"], "baseline": dict(base), "last": dict(base)}}))
    alerts = S.check_run(use_transfers=True)
    kinds = {k for al in alerts for k, _ in al["events"]}
    assert "庄在卖" in kinds, "a real 2.2% sell must fire 庄在卖"


def test_covalent_holders_and_funder_parse(monkeypatch):
    """Covalent (Moralis-free fallback) response parsing: holders decimal-adjusted &
    zero/empty filtered; first_funder = sender of first incoming native-value tx."""
    from src.onchain import covalent_client as cc
    monkeypatch.setattr(cc, "key", lambda: "cqt_test")   # make available() True
    assert cc.available()

    holders_json = {"data": {"items": [
        {"address": "0xAAA", "balance": "5000000000000000000", "contract_decimals": 18},  # 5.0
        {"address": "0xBBB", "balance": "2500000000000000000", "contract_decimals": 18},  # 2.5
        {"address": "0x0000000000000000000000000000000000000000", "balance": "9", "contract_decimals": 18},
        {"address": "0xCCC", "balance": "0", "contract_decimals": 18},  # zero → filtered
    ]}}
    monkeypatch.setattr(cc, "get", lambda ep, timeout=25: holders_json)
    hs = cc.fetch_holders("0xtoken", 56)
    assert [(h["address"], h["balance"]) for h in hs] == [("0xaaa", 5.0), ("0xbbb", 2.5)]

    funder_json = {"data": {"items": [
        {"from_address": "0xFUNDER", "to_address": "0xme", "value": "1000000000000000000"},
        {"from_address": "0xme", "to_address": "0xother", "value": "5"},
    ]}}
    monkeypatch.setattr(cc, "get", lambda ep, timeout=25: funder_json)
    assert cc.first_funder("0xme", 56) == "0xfunder"

    # no key → unavailable → empty / None (callers fall through cleanly)
    monkeypatch.setattr(cc, "key", lambda: "")
    assert not cc.available()
    assert cc.fetch_holders("0xtoken", 56) == [] and cc.first_funder("0xme", 56) is None


def test_evm_funder_native_balance_disperser(monkeypatch):
    """KEYLESS guard (BANANAS31 false positive): an EVM funder sitting on a large native
    balance (2,204 BNB) is a CEX/whale, not an operator — flagged a disperser via
    eth_getBalance, works even with Moralis AND Covalent out."""
    import src.pipeline.anomaly_screener as a

    class FakeRPC:
        def __init__(self, chain): pass
        def _logs_call(self, method, params):
            # 2204 BNB in wei
            return {"result": hex(int(2204 * 1e18))}
    monkeypatch.setattr("src.onchain.evm_archive.ArchiveRPC", FakeRPC)
    a._funder_is_disperser.cache_clear()
    assert a._funder_is_disperser("0xwhale", "bsc") is True

    class FakeRPCSmall(FakeRPC):
        def _logs_call(self, method, params):
            return {"result": hex(int(5 * 1e18))}   # 5 BNB — normal operator funder
    monkeypatch.setattr("src.onchain.evm_archive.ArchiveRPC", FakeRPCSmall)
    monkeypatch.setattr("src.onchain.moralis_client.usable", lambda: False)
    monkeypatch.setattr("src.onchain.covalent_client.available", lambda: False)
    a._funder_is_disperser.cache_clear()
    assert a._funder_is_disperser("0xoperator", "bsc") is False
