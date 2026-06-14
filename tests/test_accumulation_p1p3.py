"""Tests for P1 (distribution exit) + P2 (funder cache) + P3 (walk-forward)."""

import pytest

from src.onchain import funder_graph as fg
from src.signals.distribution_exit import DistributionExitSignal, classify_flows
from src.backtest import walk_forward as wf


# --------------------------------------------------------------------------
# P2: funder_graph cache + key pool
# --------------------------------------------------------------------------

def test_funder_keys_from_pool(monkeypatch):
    monkeypatch.setenv("ETHERSCAN_API_KEYS", "k1, k2 ,k3")
    monkeypatch.delenv("ETHERSCAN_API_KEY", raising=False)
    assert fg._keys() == ["k1", "k2", "k3"]


def test_funder_keys_single_fallback(monkeypatch):
    monkeypatch.delenv("ETHERSCAN_API_KEYS", raising=False)
    monkeypatch.setenv("ETHERSCAN_API_KEY", "solo")
    assert fg._keys() == ["solo"]


def test_funder_cache_roundtrip(tmp_path):
    db = tmp_path / "f.db"
    fg._cache_put("0xabc", "ethereum", "0xfunder", db_path=db)
    fg._cache_put("0xdef", "ethereum", None, db_path=db)  # cache "unknown" too
    got = fg._cache_get(["0xabc", "0xdef", "0xmissing"], "ethereum", db_path=db)
    assert got["0xabc"] == "0xfunder"
    assert got["0xdef"] is None
    assert "0xmissing" not in got  # never looked up


def test_cex_label_for():
    from src.onchain.cex_addresses import label_for

    # Solana is case-sensitive; EVM is not.
    assert label_for("2ojv9BAiHUrvsm9gxDe7fJSzbNZSJcxZvf8dqmWGHG8S", "solana") == "Binance"
    assert label_for("0x28C6c06298d514Db089934071355E5743bf21d60", "ethereum") == "Binance"
    assert label_for("notacex", "solana") == "unknown"
    assert label_for("", "ethereum") == "unknown"


def test_solana_ui_balance_parse():
    from src.pipeline.exit_monitor import _ui

    assert _ui({"uiTokenAmount": {"uiAmount": 12.5}}) == 12.5
    assert _ui({"uiTokenAmount": {"uiAmount": None}}) == 0.0
    assert _ui({}) == 0.0


def test_funder_solana_uses_cache(tmp_path):
    # Pre-seed the cache so no network call is made; verify Solana path reads it.
    db = tmp_path / "f.db"
    fg._cache_put("SoLaddr1", "solana", "SoLfunder", db_path=db)
    fg._cache_put("SoLaddr2", "solana", None, db_path=db)
    got = fg.get_funders(["SoLaddr1", "SoLaddr2"], "solana", db_path=db)
    assert got == {"SoLaddr1": "SoLfunder"}  # only resolved funders returned


# --------------------------------------------------------------------------
# P1: distribution / exit signal
# --------------------------------------------------------------------------

def test_classify_flows():
    transfers = [
        {"from_label": "unknown", "to_label": "Binance"},   # selling pressure
        {"from_label": "unknown", "to_label": "Coinbase"},  # selling pressure
        {"from_label": "Binance", "to_label": "unknown"},   # accumulation
        {"from_label": "unknown", "to_label": "unknown"},   # neither
    ]
    flows = classify_flows(transfers)
    assert flows["to_cex_count"] == 2
    assert flows["from_cex_count"] == 1


@pytest.mark.asyncio
async def test_distribution_signal_fires():
    sig = await DistributionExitSignal().evaluate({
        "to_cex_count": 6, "from_cex_count": 1,
        "had_accumulation": True, "token_symbol": "X",
    })
    assert sig is not None and sig.direction == "EXIT"
    assert 0 <= sig.confidence <= 100


@pytest.mark.asyncio
async def test_distribution_needs_prior_accumulation():
    sig = await DistributionExitSignal().evaluate({
        "to_cex_count": 10, "from_cex_count": 0, "had_accumulation": False,
    })
    assert sig is None


@pytest.mark.asyncio
async def test_distribution_needs_dominance():
    # deposits not dominating withdrawals → no exit
    sig = await DistributionExitSignal().evaluate({
        "to_cex_count": 4, "from_cex_count": 4, "had_accumulation": True,
    })
    assert sig is None


# --------------------------------------------------------------------------
# P3: walk-forward backtest
# --------------------------------------------------------------------------

def test_is_launch_fixed_label():
    assert wf.is_launch(2.5) is True
    assert wf.is_launch(1.4) is False  # fake move, not a launch


def test_walk_forward_split():
    samples = [
        {"timestamp": "2026-01-01", "x": 1},
        {"timestamp": "2026-03-01", "x": 2},
        {"timestamp": "2026-06-01", "x": 3},
    ]
    train, test = wf.walk_forward_split(samples, "2026-02-01")
    assert len(train) == 1 and len(test) == 2


def _fired_features():
    return {
        "gap_series": [2, 5, 9, 12, 14],
        "effective_series": [20, 28, 34, 38, 40],
        "float_active": 0.6, "security_passed": True,
    }


def _quiet_features():
    return {"gap_series": [5, 5, 5, 5, 5], "effective_series": [10, 10, 10, 10, 10],
            "float_active": 0.6, "security_passed": True}


def test_evaluate_out_of_sample():
    # Two test-window samples: a true launcher the signal catches, and a dud it
    # correctly ignores. Plus a non-survivor (fizzled) in the denominator.
    samples = [
        {"timestamp": "2026-05-01", "features": _fired_features(), "max_return": 5.0},
        {"timestamp": "2026-05-02", "features": _quiet_features(), "max_return": 1.0},
        {"timestamp": "2026-05-03", "features": _quiet_features(), "max_return": 0.5},
    ]
    m = wf.evaluate(samples, cutoff_ts="2026-04-30")
    assert m.n == 3
    assert m.tp == 1 and m.fp == 0
    assert m.precision == 1.0
    assert m.launchers == 1
    assert m.survivorship_warning is False  # only 1/3 launched → healthy denominator


def test_build_outcomes_from_scorecard(tmp_path, monkeypatch):
    import src.trading.signal_scorecard as sc
    from src.backtest import outcomes as oc

    monkeypatch.setattr(sc, "DB_PATH", tmp_path / "scorecard.db")
    # Record two accumulation signals with entry prices + token_address metadata.
    sc.record_signal("accumulation_divergence", "WIN", "ethereum", "LONG", 80,
                     entry_price=1.0, metadata={"token_address": "0xwin"})
    sc.record_signal("accumulation_divergence", "DUD", "ethereum", "LONG", 70,
                     entry_price=2.0, metadata={"token_address": "0xdud"})

    prices = {"0xwin": 3.0, "0xdud": 1.0}  # win → 3x, dud → 0.5x (a loser)
    outs = oc.build_outcomes_from_scorecard(
        "accumulation_divergence", fetch_price=lambda t, c: prices.get(t)
    )
    assert outs["0xwin"] == 3.0          # 3.0 / 1.0
    assert outs["0xdud"] == 0.5          # 1.0 / 2.0 — loser kept in denominator


def test_sweep_thresholds():
    def feat(gap, eff):
        return {"gap_series": gap, "effective_series": eff,
                "float_active": 0.6, "security_passed": True}
    samples = [
        {"timestamp": "2026-05-01", "features": feat([2, 5, 9, 12, 14], [20, 28, 34, 38, 40]), "max_return": 5.0},
        {"timestamp": "2026-05-02", "features": feat([5, 5, 5, 5, 5], [10, 10, 10, 10, 10]), "max_return": 0.5},
    ]
    res = wf.sweep_thresholds(samples, cutoff_ts="2026-04-30")
    assert len(res) > 1
    # results sorted best-first; each row carries thresholds + metrics
    assert "min_gap_slope" in res[0] and "precision" in res[0]
    assert res == sorted(res, key=lambda r: (r["precision"], r["fired"]), reverse=True)


def test_watch_tokens_loader():
    from src.pipeline.accumulation_pipeline import _load_watch_tokens

    # The shipped config is an empty template → returns a list (no crash).
    assert isinstance(_load_watch_tokens(), list)


def test_survivorship_warning():
    # All samples "launched" → survivor-heavy → warn.
    samples = [
        {"timestamp": "2026-05-01", "features": _fired_features(), "max_return": 5.0},
        {"timestamp": "2026-05-02", "features": _fired_features(), "max_return": 4.0},
    ]
    m = wf.evaluate(samples, cutoff_ts="2026-04-30")
    assert m.survivorship_warning is True


def test_build_samples_from_snapshots(tmp_path):
    from src.onchain import holder_snapshot as hs

    db = tmp_path / "snap.db"
    # Two snapshots for one token so a series can be built.
    hs.save_snapshot("TKN", "ethereum", [{"address": "0x1", "balance": 100},
                                          {"address": "0x2", "balance": 50}], db_path=db)
    hs.save_snapshot("TKN", "ethereum", [{"address": "0x1", "balance": 200},
                                          {"address": "0x2", "balance": 50}], db_path=db)
    # Only winners-and-losers outcome map keeps the denominator honest.
    samples = wf.build_samples_from_snapshots(
        {"TKN": 3.0, "DEAD": 0.4}, chain="ethereum", db_path=db
    )
    assert len(samples) == 1  # DEAD not in snapshots → skipped
    s = samples[0]
    assert s["token"] == "TKN" and s["max_return"] == 3.0
    assert len(s["features"]["effective_series"]) == 2
