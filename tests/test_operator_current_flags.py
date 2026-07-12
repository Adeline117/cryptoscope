"""identify_operator must expose its trust flags as ALWAYS-PRESENT booleans.

The bug this freezes: `out["current"]` was a lossy projection of the real analysis
— `supply_verified` was left inside `conc`, `current_graph_available` was only set
in the replay branch. A consumer reading `out["current"]` for those flags got None
(absent), which is indistinguishable from "verification failed". That ambiguity let
a LIVE, fully-verified holder graph (supply_verified actually True) be misread as
broken/unverified. Absence must be impossible: each trust flag is True or False, and
its meaning is unambiguous.
"""

from __future__ import annotations

import src.onchain.operator_id as oid


def _stub_common(monkeypatch):
    """Stub the heavy downstream calls (historical ledger, supply, market) so the
    test exercises only the current-graph flag wiring, offline."""
    monkeypatch.setattr(oid, "_historical_ledger",
                        lambda *a, **k: {"available": False, "exited": [], "holding": []})
    monkeypatch.setattr(oid, "_token_market", lambda *a, **k: {"available": False})

    class _RPC:
        def __init__(self, *a, **k): pass
        def token_decimals(self, *a, **k): return 18
        def total_supply(self, *a, **k): return None
    monkeypatch.setattr("src.onchain.evm_archive.ArchiveRPC", _RPC)


def test_current_flags_present_and_boolean_on_success(monkeypatch):
    _stub_common(monkeypatch)
    monkeypatch.setattr("src.onchain.holder_snapshot.fetch_holders_evm",
                        lambda *a, **k: [{"address": "0x1", "balance": 100.0}])
    monkeypatch.setattr("src.pipeline.anomaly_screener.effective_concentration_signal",
                        lambda *a, **k: {"supply_verified": True, "largest_entity_pct": 12.9,
                                         "largest_address_pct": 1.5, "concentration_gap": 11.3,
                                         "entity_count": 38, "cluster_confidence": 50,
                                         "dominant_cluster_wallets": ["0xa", "0xb"]})
    cur = oid.identify_operator("0xtok", "bsc")["current"]
    # flags PRESENT and boolean — never None, never absent
    assert cur["current_graph_available"] is True
    assert cur["supply_verified"] is True
    assert isinstance(cur["current_graph_available"], bool)
    assert isinstance(cur["supply_verified"], bool)


def test_current_flags_false_when_supply_unverified(monkeypatch):
    # A subset-ratio read (supply_verified falsy in conc) must surface as False,
    # not None — so a consumer can distinguish "share of real supply" from "subset".
    _stub_common(monkeypatch)
    monkeypatch.setattr("src.onchain.holder_snapshot.fetch_holders_evm",
                        lambda *a, **k: [{"address": "0x1", "balance": 100.0}])
    monkeypatch.setattr("src.pipeline.anomaly_screener.effective_concentration_signal",
                        lambda *a, **k: {"largest_entity_pct": 5.0})   # no supply_verified
    cur = oid.identify_operator("0xtok", "bsc")["current"]
    assert cur["current_graph_available"] is True
    assert cur["supply_verified"] is False


def test_current_flags_false_when_graph_fetch_fails(monkeypatch):
    # The current-graph fetch raising must land both flags at False, never absent —
    # a real failure is unambiguous, not a None that reads as "maybe fine".
    _stub_common(monkeypatch)
    def _boom(*a, **k):
        raise RuntimeError("RPC down")
    monkeypatch.setattr("src.onchain.holder_snapshot.fetch_holders_evm", _boom)
    cur = oid.identify_operator("0xtok", "bsc")["current"]
    assert cur["current_graph_available"] is False
    assert cur["supply_verified"] is False
