"""The pre-trade check is a DON'T-LOSE filter. These tests pin that missing data
never becomes a green light, and that the severity ordering can't be inverted.
"""

import src.pipeline.pretrade as pt


def _op(monkeypatch, verdict="dispersed", rug=None, current=None):
    monkeypatch.setattr(pt, "identify_operator", lambda t, c: {
        "verdict": verdict, "confidence": 60,
        "current": current or {"holders_fetched": 100, "current_graph_available": True},
        "rug_risk": rug or {"available": True, "flags": {}, "facts": [],
                            "owner_renounced": True, "lp_all_locked": True}})


def test_distributing_is_avoid(monkeypatch):
    _op(monkeypatch, verdict="distributing")
    assert pt.check("0xt", "bsc")["level"] == pt.AVOID


def test_honeypot_is_avoid(monkeypatch):
    _op(monkeypatch, rug={"available": True, "flags": {"is_honeypot": 1}, "facts": [],
                          "owner_renounced": True, "lp_all_locked": True})
    assert pt.check("0xt", "bsc")["level"] == pt.AVOID


def test_fake_renounce_mintable_is_avoid(monkeypatch):
    _op(monkeypatch, rug={"available": True,
                          "flags": {"is_mintable": 1, "owner_change_balance": 1},
                          "facts": ["放弃所有权不可信(存在隐藏owner/可收回)"],
                          "owner_renounced": None, "lp_all_locked": False,
                          "lp_locked_n": 0, "lp_holders_n": 3})
    assert pt.check("0xt", "base")["level"] == pt.AVOID


def test_missing_contract_data_is_not_green(monkeypatch):
    """'We couldn't check the contract' must never read as safe."""
    _op(monkeypatch, verdict="dispersed", rug={"available": False, "reason": "429"})
    assert pt.check("0xt", "bsc")["level"] != pt.CONSIDER


def test_allocated_cluster_is_caution_not_operator(monkeypatch):
    _op(monkeypatch, verdict="treasury",
        current={"holders_fetched": 100, "current_graph_available": True,
                 "largest_entity_pct": 79, "acquisition": {"verdict": "allocated",
                                                           "bought": 0, "allocated": 4}})
    res = pt.check("0xt", "ethereum")
    assert res["level"] == pt.CAUTION
    assert any("发行方" in why for _, why in res["reasons"])


def test_clean_token_reaches_consider(monkeypatch):
    _op(monkeypatch, verdict="dispersed")
    assert pt.check("0xt", "bsc")["level"] == pt.CONSIDER


def test_engine_crash_is_unknown_not_safe(monkeypatch):
    def boom(t, c):
        raise RuntimeError("rpc down")
    monkeypatch.setattr(pt, "identify_operator", boom)
    assert pt.check("0xt", "bsc")["level"] == pt.UNKNOWN


def test_loaded_live_operator_is_not_green(monkeypatch):
    """THE HOLE: loaded_live_operator (the engine's 'most dangerous live setup', and
    the default when velocity can't be computed) had no branch → green-lit."""
    _op(monkeypatch, verdict="loaded_live_operator")
    assert pt.check("0xt", "bsc")["level"] != pt.CONSIDER


def test_unknown_verdict_string_never_green(monkeypatch):
    """A future/unrecognized verdict must not default to green by omission."""
    _op(monkeypatch, verdict="some_new_verdict_added_later")
    assert pt.check("0xt", "bsc")["level"] == pt.UNKNOWN


def test_closed_source_contract_is_not_green(monkeypatch):
    _op(monkeypatch, verdict="dispersed",
        rug={"available": True, "is_open_source": 0, "flags": {}, "facts": []})
    assert pt.check("0xt", "bsc")["level"] != pt.CONSIDER
