"""The early-capture shortlist: only a VERIFIED-BOUGHT young cluster qualifies.

Every 'operator' in a 45-coin survey was an issuer until acquisition_mode ran, and
two sentinel verdicts were built on ghost data until on-chain verification ran. The
early-capture list must clear both, or it is just the old false positives on fresh
coins.
"""

from src.pipeline.operator_hunt import early_accumulation_candidates


def _s(**o):
    base = {"acquisition": "bought", "supply_verified": True, "age_days": 5,
            "concentration_gap": 10, "symbol": "X"}
    base.update(o)
    return base


def test_verified_bought_young_cluster_qualifies():
    assert len(early_accumulation_candidates([_s()])) == 1


def test_allocated_cluster_is_rejected():
    """An issuer allocation is not an operator, however concentrated."""
    assert early_accumulation_candidates([_s(acquisition="allocated")]) == []


def test_unverified_supply_is_rejected():
    """A subset ratio masquerading as concentration must not qualify."""
    assert early_accumulation_candidates([_s(supply_verified=False)]) == []


def test_old_token_is_rejected():
    """Past the accumulation window the operator is distributing, not building."""
    assert early_accumulation_candidates([_s(age_days=120)]) == []


def test_unknown_age_is_rejected():
    """No age = can't confirm it's early. Not a silent pass."""
    assert early_accumulation_candidates([_s(age_days=None)]) == []


def test_weak_cluster_is_rejected():
    assert early_accumulation_candidates([_s(concentration_gap=2)]) == []


def test_unknown_acquisition_is_rejected():
    """Only an AFFIRMATIVE 'bought' qualifies; 'unknown' (data failure) never does."""
    assert early_accumulation_candidates([_s(acquisition="unknown")]) == []
