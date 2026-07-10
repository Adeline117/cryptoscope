"""Widening the event surface must not widen the lying surface.

Every guard here encodes the same rule: a scan that did not complete contributes NO
event. A failed read is not a quiet market.
"""

import src.pipeline.perp_mobilization as pm


def _uni(monkeypatch, coins):
    monkeypatch.setattr("src.onchain.perp_universe.load", lambda: coins)


def test_incomplete_holder_read_emits_no_event(monkeypatch):
    """Supply or holder fetch failed -> we don't know who the whales are -> silence."""
    _uni(monkeypatch, {"X": {"chain": "bsc", "address": "0xt"}})
    monkeypatch.setattr(pm, "_whales", lambda t, c: ([], False))
    called = []
    monkeypatch.setattr("src.onchain.mobilization.approval_scan",
                        lambda *a, **k: called.append(1) or {"complete": True, "approvals": []})
    events, _ = pm.scan_mobilization()
    assert events == [] and called == [], "must not even scan a coin whose whales are unknown"


def test_approval_scan_failure_does_not_advance_cursor(monkeypatch):
    _uni(monkeypatch, {"X": {"chain": "bsc", "address": "0xt"}})
    monkeypatch.setattr(pm, "_whales", lambda t, c: (["0xw"], True))
    monkeypatch.setattr("src.onchain.mobilization.approval_scan",
                        lambda *a, **k: {"complete": False, "approvals": [], "to_block": None})
    monkeypatch.setattr("src.onchain.mobilization.gas_topup_scan",
                        lambda *a, **k: {"armed": False, "balances": {}, "topups": []})
    events, state = pm.scan_mobilization(prev_state={"bsc:0xt": {"mobil_block": 500}})
    assert events == []
    assert state["bsc:0xt"]["mobil_block"] == 500, "a failed scan must not advance the cursor"


def test_router_approval_is_an_event(monkeypatch):
    _uni(monkeypatch, {"X": {"chain": "bsc", "address": "0xt"}})
    monkeypatch.setattr(pm, "_whales", lambda t, c: (["0xw"], True))
    monkeypatch.setattr("src.onchain.mobilization.approval_scan", lambda *a, **k: {
        "complete": True, "to_block": 900,
        "approvals": [{"owner": "0xw", "spender": "0xr", "spender_kind": "router"},
                      {"owner": "0xw", "spender": "0xz", "spender_kind": "other"}]})
    monkeypatch.setattr("src.onchain.mobilization.gas_topup_scan",
                        lambda *a, **k: {"armed": True, "balances": {}, "topups": []})
    events, state = pm.scan_mobilization()
    assert len(events) == 1 and events[0]["kind"] == "授权路由"
    assert events[0]["n"] == 1, "only router approvals count; 'other' spenders don't"
    assert state["bsc:0xt"]["mobil_block"] == 900


def test_lp_unlock_fires_only_on_transition(monkeypatch):
    _uni(monkeypatch, {"X": {"chain": "bsc", "address": "0xt"}})
    sec = {"available": True, "lp": {"all_locked": False, "n_locked": 0, "n_holders_seen": 4}}
    monkeypatch.setattr("src.onchain.goplus_client.token_security", lambda t, c: sec)

    # First sight of an always-unlocked pool: a standing condition, NOT a moment.
    events, state = pm.scan_lp_unlock()
    assert events == [] and state["bsc:0xt"]["lp_all_locked"] is False

    # Still unlocked next pass: still not an event.
    events, state = pm.scan_lp_unlock(prev_state=state)
    assert events == []

    # locked -> unlocked IS the moment.
    events, _ = pm.scan_lp_unlock(prev_state={"bsc:0xt": {"lp_all_locked": True}})
    assert len(events) == 1 and events[0]["kind"] == "LP解锁"


def test_unavailable_goplus_is_not_an_unlock(monkeypatch):
    _uni(monkeypatch, {"X": {"chain": "bsc", "address": "0xt"}})
    monkeypatch.setattr("src.onchain.goplus_client.token_security",
                        lambda t, c: {"available": False, "reason": "429"})
    events, state = pm.scan_lp_unlock(prev_state={"bsc:0xt": {"lp_all_locked": True}})
    assert events == [], "a failed security read must never look like an unlock"
    assert state["bsc:0xt"]["lp_all_locked"] is True, "state must not be overwritten"


def test_no_lp_data_is_unknown_not_unlocked(monkeypatch):
    _uni(monkeypatch, {"X": {"chain": "bsc", "address": "0xt"}})
    monkeypatch.setattr("src.onchain.goplus_client.token_security", lambda t, c: {
        "available": True, "lp": {"all_locked": None, "n_locked": 0, "n_holders_seen": 0}})
    events, _ = pm.scan_lp_unlock(prev_state={"bsc:0xt": {"lp_all_locked": True}})
    assert events == []
