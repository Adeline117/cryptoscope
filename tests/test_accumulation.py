"""Tests for the 二级妖币 accumulation detection system.

Covers the genuinely-new alpha layer: concentration metrics, Sybil clustering /
effective concentration, the divergence signal, and Kelly sizing. All pure /
SQLite — no network.
"""

import pytest

from src.onchain import holder_snapshot as hs
from src.onchain.entity_clustering import cluster_addresses, effective_concentration
from src.signals.accumulation_divergence import (
    AccumulationDivergenceSignal,
    _slope,
    is_decelerating,
)
from src.trading.position_sizer import calculate_kelly_position, kelly_fraction


# --------------------------------------------------------------------------
# holder_snapshot: concentration metrics
# --------------------------------------------------------------------------

def test_top_n_pct():
    balances = [100, 50, 30, 20, 10]  # total 210
    assert hs.top_n_pct(balances, 2) == pytest.approx((150 / 210) * 100, abs=0.01)
    assert hs.top_n_pct([], 10) == 0.0
    assert hs.top_n_pct([0, 0], 10) == 0.0


def test_gini_bounds():
    assert hs.gini([]) == 0.0
    assert hs.gini([10, 10, 10, 10]) == pytest.approx(0.0, abs=0.05)  # equal → ~0
    concentrated = hs.gini([1000, 1, 1, 1, 1])
    assert 0.5 < concentrated <= 1.0  # highly concentrated → high gini


def test_concentration_metrics():
    holders = [{"address": f"a{i}", "balance": b} for i, b in enumerate([100, 50, 30, 0])]
    m = hs.concentration_metrics(holders)
    assert m["holder_count"] == 3  # zero-balance excluded
    assert m["top10_pct"] == pytest.approx(100.0)
    assert m["total_supply_observed"] == pytest.approx(180.0)


def test_snapshot_roundtrip(tmp_path):
    db = tmp_path / "snap.db"
    hs.record_token_birth("TKN", "solana", "test", db_path=db)
    hs.save_snapshot("TKN", "solana", [{"address": "a", "balance": 100}], db_path=db)
    hs.save_snapshot("TKN", "solana", [{"address": "a", "balance": 200}], db_path=db)
    snaps = hs.get_snapshots("TKN", "solana", db_path=db)
    assert len(snaps) == 2
    hist = hs.get_holders_history("TKN", "solana", db_path=db)
    assert len(hist) == 2 and hist[0][1][0]["balance"] == 100


# --------------------------------------------------------------------------
# entity_clustering
# --------------------------------------------------------------------------

def test_cluster_common_funder():
    addrs = ["0xa", "0xb", "0xc", "0xd"]
    funders = {"0xa": "0xfund", "0xb": "0xfund", "0xc": "0xother", "0xd": "0xother"}
    mapping = cluster_addresses(addrs, funders=funders, exclude=set())
    # a,b share a funder → same entity; c,d share another → same entity
    assert mapping["0xa"] == mapping["0xb"]
    assert mapping["0xc"] == mapping["0xd"]
    assert mapping["0xa"] != mapping["0xc"]


def test_cluster_excludes_cex():
    addrs = ["0xa", "0xcex"]
    mapping = cluster_addresses(addrs, exclude={"0xcex"})
    assert "0xcex" not in mapping
    assert "0xa" in mapping


def test_effective_concentration_rises_after_clustering():
    # 4 addresses each holding 20 (nominal looks spread), all one entity via funder.
    holders = [{"address": f"0x{i}", "balance": 20} for i in range(4)]
    holders += [{"address": f"0xz{i}", "balance": 5} for i in range(10)]  # noise
    funders = {f"0x{i}": "0xwhale" for i in range(4)}
    m = effective_concentration(holders, funders=funders, top_n=1)
    # Single largest entity (the clustered whale) should dominate effective top-1
    assert m["effective_top_n_pct"] > m["nominal_top_n_pct"]
    assert m["concentration_gap"] > 0
    assert m["entity_count"] < m["nominal_holder_count"]


def test_effective_concentration_empty():
    m = effective_concentration([], top_n=10)
    assert m["concentration_gap"] == 0.0 and m["entity_count"] == 0


def test_cluster_root_funder_chain():
    # a<-b<-c and d<-c → a, b, d share root funder c
    from src.onchain.entity_clustering import cluster_addresses

    funders = {"0xa": "0xb", "0xb": "0xc", "0xd": "0xc"}
    m = cluster_addresses(["0xa", "0xb", "0xd"], funders=funders, exclude=set())
    assert m["0xa"] == m["0xb"] == m["0xd"]


def test_cluster_similar_balance():
    from src.onchain.entity_clustering import _similar_balance_groups, cluster_addresses

    bals = {f"0x{i:040x}": 1000.0 for i in range(4)}
    bals["0xother"] = 37.0
    groups = _similar_balance_groups(bals)
    assert any(len(g) >= 4 for g in groups)

    m = cluster_addresses(list(bals), balances=bals, exclude=set())
    roots = {m[f"0x{i:040x}"] for i in range(4)}
    assert len(roots) == 1  # the 4 equal-balance马甲 merge into one entity


def test_effective_concentration_catches_split_position():
    # 5 wallets each holding 20 (looks spread) — equal balances → one entity.
    holders = [{"address": f"0x{i:040x}", "balance": 20} for i in range(5)]
    holders += [{"address": f"0xreal{i}", "balance": 3} for i in range(20)]
    m = effective_concentration(holders, top_n=1)
    assert m["effective_top_n_pct"] > m["nominal_top_n_pct"]
    assert m["concentration_gap"] > 0


# --------------------------------------------------------------------------
# accumulation_divergence signal
# --------------------------------------------------------------------------

def test_slope():
    assert _slope([1, 2, 3, 4]) == pytest.approx(1.0)
    assert _slope([4, 3, 2, 1]) == pytest.approx(-1.0)
    assert _slope([5]) == 0.0


def test_is_decelerating():
    assert is_decelerating([0, 8, 14, 18, 20]) is True   # diffs 8,6,4,2 ↓
    assert is_decelerating([0, 2, 6, 14, 30]) is False   # accelerating
    assert is_decelerating([10, 10]) is False


def _good_market_data(**over):
    md = {
        "gap_series": [2, 5, 9, 12, 14],
        "effective_series": [20, 28, 34, 38, 40],  # rising, decelerating
        "float_active": 0.6,
        "security_passed": True,
        "token_symbol": "T",
    }
    md.update(over)
    return md


@pytest.mark.asyncio
async def test_signal_fires_on_divergence():
    sig = await AccumulationDivergenceSignal().evaluate(_good_market_data())
    assert sig is not None and sig.direction == "LONG"
    assert 0 <= sig.confidence <= 100


@pytest.mark.asyncio
async def test_signal_no_divergence():
    # Flat gap → no divergence → no signal
    sig = await AccumulationDivergenceSignal().evaluate(
        _good_market_data(gap_series=[5, 5, 5, 5, 5])
    )
    assert sig is None


@pytest.mark.asyncio
async def test_signal_not_decelerating():
    # Accelerating effective series → not the saturation inflection → no signal
    sig = await AccumulationDivergenceSignal().evaluate(
        _good_market_data(effective_series=[20, 22, 26, 34, 50])
    )
    assert sig is None


@pytest.mark.asyncio
async def test_signal_low_float():
    sig = await AccumulationDivergenceSignal().evaluate(
        _good_market_data(float_active=0.1)
    )
    assert sig is None


@pytest.mark.asyncio
async def test_signal_insufficient_history():
    sig = await AccumulationDivergenceSignal().evaluate(
        _good_market_data(gap_series=[2, 5], effective_series=[20, 30])
    )
    assert sig is None


@pytest.mark.asyncio
async def test_signal_security_failed():
    sig = await AccumulationDivergenceSignal().evaluate(
        _good_market_data(security_passed=False)
    )
    assert sig is None


# --------------------------------------------------------------------------
# position_sizer (Kelly)
# --------------------------------------------------------------------------

def test_kelly_fraction():
    assert kelly_fraction(0.5, 1.0) == pytest.approx(0.0)
    assert kelly_fraction(0.7, 2.0) == pytest.approx(0.7 - 0.3 / 2.0)
    assert kelly_fraction(0.9, 0) == 0.0  # guard against zero payoff
    assert 0.0 <= kelly_fraction(0.99, 5.0) <= 1.0


def test_kelly_position_no_history():
    out = calculate_kelly_position("unknown_sig", summary={})
    assert out["pct"] == 0.02  # conservative default


def test_kelly_position_few_samples():
    summary = {"accumulation_divergence": {"completed": 3, "win_rate_24h": "70%",
                                           "avg_pnl_24h": "+10%"}}
    out = calculate_kelly_position("accumulation_divergence", summary=summary)
    assert out["pct"] == 0.02  # below MIN_SAMPLES → fallback


def test_kelly_position_with_history_bounded():
    summary = {"accumulation_divergence": {"completed": 50, "win_rate_24h": "65%",
                                           "avg_pnl_24h": "+15%"}}
    out = calculate_kelly_position("accumulation_divergence", summary=summary)
    assert 0.005 <= out["pct"] <= 0.10  # within hard floor/cap


def test_arkham_entity_map_clustering():
    # Arkham ground-truth entity merges addresses sharing an entity.
    from src.onchain.entity_clustering import cluster_addresses, effective_concentration

    emap = {"0xa": "whale1", "0xb": "whale1", "0xc": "whale2"}
    m = cluster_addresses(["0xa", "0xb", "0xc"], entity_map=emap, exclude=set())
    assert m["0xa"] == m["0xb"]      # same Arkham entity → merged
    assert m["0xa"] != m["0xc"]

    holders = [{"address": "0xa", "balance": 30}, {"address": "0xb", "balance": 30},
               {"address": "0xc", "balance": 40}]
    base = effective_concentration(holders, top_n=1)
    arkm = effective_concentration(holders, top_n=1, entity_map=emap)
    # Merging a+b makes the top-1 entity bigger than any single address.
    assert arkm["effective_top_n_pct"] >= base["effective_top_n_pct"]


def test_exclude_share_above_drops_pool():
    from src.onchain.entity_clustering import effective_concentration

    holders = [{"address": "pool", "balance": 900}]  # 90% = pool/vault
    holders += [{"address": f"h{i}", "balance": 10} for i in range(10)]
    m = effective_concentration(holders, top_n=3, exclude_share_above=0.30)
    # The 90% pool account is excluded → metrics reflect only real holders.
    assert m["nominal_holder_count"] == 10
