"""Fail-to-unknown consumer gates — frozen so a green-by-omission bug can't regress.

The fail-to-unknown flags (`available` / `complete` / `supply_verified` / verdict
enums) are enforced at their PRODUCER sites but honored at CONSUMER sites by
hand-written `if` checks with nothing freezing them. This session that surface
produced two live bugs:

  · class 5 — pretrade's `min(..., default=CONSIDER)` green-lit an unrecognized /
    loaded verdict by omission (the avoidance filter did the OPPOSITE of its job).
  · class 4 — an ALLOCATED (issuer) cluster was output as a bought-from-market
    operator because the acquisition gate wasn't consulted.

Each test below feeds the adversarial payload directly to a consumer gate and
asserts it does NOT emit a positive / green / promotable result. These are the
external friction that catches the fluency-as-truth error class at code-change
time (red CI), not by hoping the author re-checks.
"""

from __future__ import annotations

import pytest


# --------------------------------------------------------------------------- #
# promotable() — the one reusable verdict gate. Must default-DENY.
# --------------------------------------------------------------------------- #
def test_promotable_denies_non_promotable_verdicts():
    from src.onchain.operator_id import NON_PROMOTABLE, promotable
    for v in NON_PROMOTABLE:
        assert promotable({"verdict": v, "confidence": 99}) is False, \
            f"{v} is NON_PROMOTABLE yet promotable() allowed it"


def test_promotable_denies_low_confidence():
    from src.onchain.operator_id import promotable
    assert promotable({"verdict": "live_operator", "confidence": 54}) is False
    assert promotable({"verdict": "live_operator", "confidence": 55}) is True


def test_promotable_denies_borderline_caveat():
    from src.onchain.operator_id import promotable
    v = {"verdict": "live_operator", "confidence": 90,
         "caveats": ["borderline: 20d velocity within jitter of the cliff"]}
    assert promotable(v) is False, "a borderline verdict must not promote"


def test_promotable_denies_unknown_new_verdict():
    # A verdict string added later that nobody remembered to classify must not
    # sail through just because it isn't in NON_PROMOTABLE... it CAN promote only
    # if confidence clears the floor, so pin the confidence-floor behaviour too.
    from src.onchain.operator_id import promotable
    assert promotable({"verdict": "some_future_verdict", "confidence": 10}) is False


# --------------------------------------------------------------------------- #
# operator_hunt.early_accumulation_candidates — class 4: issuer ≠ operator.
# --------------------------------------------------------------------------- #
def _suspect(**over):
    base = {"symbol": "X", "chain": "bsc", "address": "0xabc",
            "acquisition": "bought", "supply_verified": True,
            "age_days": 5, "concentration_gap": 8}
    base.update(over)
    return base


def test_early_accumulation_rejects_allocated_issuer():
    from src.pipeline.operator_hunt import early_accumulation_candidates
    # ALLOCATED = issuer/treasury, not a bought-from-market operator (class 4).
    out = early_accumulation_candidates([_suspect(acquisition="allocated")])
    assert out == [], "an allocated (issuer) cluster must not be an operator find"


def test_early_accumulation_rejects_unverified_supply():
    from src.pipeline.operator_hunt import early_accumulation_candidates
    # supply_verified False = subset ratio, not supply share — a supply-RPC outage
    # must read as 'couldn't check', never as a confirmed concentrated operator.
    out = early_accumulation_candidates([_suspect(supply_verified=False)])
    assert out == []


def test_early_accumulation_rejects_unknown_acquisition():
    from src.pipeline.operator_hunt import early_accumulation_candidates
    out = early_accumulation_candidates([_suspect(acquisition="unknown")])
    assert out == []


def test_early_accumulation_accepts_the_verified_positive():
    # Guard against the test passing simply because the gate rejects everything.
    from src.pipeline.operator_hunt import early_accumulation_candidates
    out = early_accumulation_candidates([_suspect()])
    assert len(out) == 1


# --------------------------------------------------------------------------- #
# pretrade.check — class 5: never green-light a loaded / unknown verdict.
# --------------------------------------------------------------------------- #
def _op_verdict(verdict, conf=70):
    """A verdict dict shaped like identify_operator's output, with a CLEAN contract
    and live pool so ONLY the operator-verdict branch can drive the level. If the
    verdict logic green-lights by omission, the level will wrongly be CONSIDER."""
    return {
        "verdict": verdict, "confidence": conf,
        "current": {"current_graph_available": True, "holders_fetched": 50,
                    "largest_entity_pct": 5, "acquisition": {"verdict": "unknown"},
                    "market": {"available": True, "liquidity_usd": 500_000,
                               "volume_h24": 200_000}},
        "rug_risk": {"available": True, "is_open_source": 1, "flags": {}, "facts": [],
                     "owner_renounced": True, "lp_all_locked": True},
    }


@pytest.fixture
def _clean_pretrade(monkeypatch):
    """Patch identify_operator + deployer_history so pretrade.check runs offline and
    the level is decided purely by the operator verdict we inject."""
    import src.pipeline.pretrade as pt
    import src.onchain.deployer_history as dhmod
    monkeypatch.setattr(dhmod, "deployer_history",
                        lambda *a, **k: {"verdict": "unknown"})
    return pt


@pytest.mark.parametrize("verdict", ["loaded_live_operator", "live_operator"])
def test_pretrade_loaded_operator_is_not_greenlit(_clean_pretrade, monkeypatch, verdict):
    pt = _clean_pretrade
    monkeypatch.setattr(pt, "identify_operator", lambda t, c: _op_verdict(verdict))
    res = pt.check("0xtok", "bsc")
    # a verified loaded operator holding the ammo is the MOST dangerous live setup,
    # never a green light.
    assert res["level"] != pt.CONSIDER, f"{verdict} was green-lit → avoidance filter inverted"
    assert res["level"] in (pt.CAUTION, pt.AVOID)


@pytest.mark.parametrize("verdict", ["unknown", "indeterminate_emptied",
                                     "some_unrecognized_future_verdict"])
def test_pretrade_unknown_verdict_is_not_greenlit(_clean_pretrade, monkeypatch, verdict):
    pt = _clean_pretrade
    monkeypatch.setattr(pt, "identify_operator", lambda t, c: _op_verdict(verdict))
    res = pt.check("0xtok", "bsc")
    # "we couldn't classify it" is UNKNOWN, never CONSIDER — missing data is caution,
    # not permission (the min(default=CONSIDER) omission bug).
    assert res["level"] != pt.CONSIDER, f"{verdict} defaulted to green by omission"


def test_pretrade_selling_operator_is_avoid(_clean_pretrade, monkeypatch):
    pt = _clean_pretrade
    monkeypatch.setattr(pt, "identify_operator", lambda t, c: _op_verdict("distributing"))
    res = pt.check("0xtok", "bsc")
    assert res["level"] == pt.AVOID


# --------------------------------------------------------------------------- #
# yaobi_finder.classify — class 4: an allocated / unverified cluster is no setup.
# --------------------------------------------------------------------------- #
def _patch_yaobi(monkeypatch, *, supply_verified=True, acquisition="bought",
                 largest=25, gap=8, cluster_n=4):
    """Stub the network functions classify() calls so we test only its gate logic."""
    import src.onchain.holder_snapshot as hs
    import src.pipeline.anomaly_screener as an
    import src.onchain.operator_id as oid
    import src.pipeline.yaobi_finder as yf
    monkeypatch.setattr(hs, "fetch_holders_evm",
                        lambda *a, **k: [{"address": "0x1", "balance": 1.0}])
    monkeypatch.setattr(an, "effective_concentration_signal", lambda *a, **k: {
        "supply_verified": supply_verified, "largest_entity_pct": largest,
        "concentration_gap": gap,
        "dominant_cluster_wallets": [f"0x{i}" for i in range(cluster_n)]})
    monkeypatch.setattr(oid, "acquisition_mode",
                        lambda *a, **k: {"verdict": acquisition, "bought": 0, "allocated": 9})
    monkeypatch.setattr(yf, "_buy_pressure",
                        lambda cand: {"ratio_h1": 1.0, "sells_h1": 0, "buys_h1": 0})
    return yf


def test_yaobi_rejects_allocated_cluster(monkeypatch):
    yf = _patch_yaobi(monkeypatch, acquisition="allocated")
    out = yf.classify({"chain": "bsc", "address": "0xtok"})
    assert out is None, "an allocated (issuer) concentrated bag is not a tradeable short"


def test_yaobi_rejects_unverified_supply(monkeypatch):
    yf = _patch_yaobi(monkeypatch, supply_verified=False)
    out = yf.classify({"chain": "bsc", "address": "0xtok"})
    assert out is None, "supply-unverified concentration is 'couldn't check', not a short"


def test_yaobi_short_fires_on_verified_bought_concentration(monkeypatch):
    # Positive control: the gate must still let the real, verified setup through.
    yf = _patch_yaobi(monkeypatch, acquisition="bought")
    out = yf.classify({"chain": "bsc", "address": "0xtok"})
    assert out is not None and out["direction"] == "short"
