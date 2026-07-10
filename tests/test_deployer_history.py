"""Deployer track record. Fail-safe: never fabricates a verdict, and a factory-
deployed token (creator has no readable prior tokens) reads UNKNOWN, not clean.
"""

import src.onchain.deployer_history as dh


def _wire(monkeypatch, creator, created, liq_map):
    monkeypatch.setattr(dh, "_creator", lambda t, c: creator)
    monkeypatch.setattr(dh, "_created_contracts", lambda cr, c, **k: created)
    monkeypatch.setattr(dh, "_token_liquidity", lambda t, c: liq_map.get(t))


def test_serial_rugger_all_dead(monkeypatch):
    _wire(monkeypatch, "0xdev", ["0xa", "0xb", "0xc"],
          {"0xa": 100, "0xb": 0, "0xc": 500})     # all below dead threshold
    r = dh.deployer_history("0xtok", "bsc")
    assert r["verdict"] == "serial_rugger" and r["dead"] == 3 and r["alive"] == 0


def test_survivors_are_weak_positive(monkeypatch):
    _wire(monkeypatch, "0xdev", ["0xa", "0xb"], {"0xa": 80_000, "0xb": 120_000})
    r = dh.deployer_history("0xtok", "bsc")
    assert r["verdict"] == "prior_survived" and r["alive"] == 2


def test_unindexed_priors_are_unknown_not_dead(monkeypatch):
    """A factory that created non-token contracts must read UNKNOWN, not serial rug.
    (The real POD case: creator made 12 contracts, none tradeable.)"""
    _wire(monkeypatch, "0xfactory", ["0x1", "0x2", "0x3"],
          {"0x1": None, "0x2": None, "0x3": None})
    r = dh.deployer_history("0xtok", "base")
    assert r["verdict"] == "unknown"
    assert r["dead"] == 0, "un-indexed contracts must not count as deaths"


def test_no_creations_is_first_timer(monkeypatch):
    _wire(monkeypatch, "0xdev", [], {})
    assert dh.deployer_history("0xtok", "bsc")["verdict"] == "first_timer"


def test_no_creator_is_unknown_not_available(monkeypatch):
    monkeypatch.setattr(dh, "_creator", lambda t, c: None)
    r = dh.deployer_history("0xtok", "bsc")
    assert r["available"] is False and r["verdict"] == "unknown"


def test_the_token_itself_is_excluded(monkeypatch):
    """The token being checked must not count as its own deployer's prior work."""
    _wire(monkeypatch, "0xdev", ["0xtok", "0xa", "0xb"],
          {"0xa": 0, "0xb": 0})           # 0xtok excluded; only 2 dead priors
    r = dh.deployer_history("0xTOK", "bsc")   # case-insensitive
    assert r["dead"] == 2
