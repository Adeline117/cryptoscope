"""A truncated holder reconstruction must never be returned as 'current holders'.

WOO's top reconstructed wallet showed 1.49B tokens (50% of supply); balanceOf said 0.
24 of its top 30 'holders' held nothing on-chain. effective_concentration_signal
built cluster_confidence 56 on that, and it read as a hidden operator cluster.
"""

import src.onchain.holder_snapshot as hs


class FakeRPC:
    """balances: {addr: value}. None means the RPC read FAILED (not 'empty')."""

    def __init__(self, chain):
        pass

    balances: dict = {}

    def balance_of(self, token, holder, block="latest"):
        return type(self).balances.get(holder.lower(), 0.0)


def test_stale_holders_are_dropped(monkeypatch):
    FakeRPC.balances = {"0xa": 0.0, "0xb": 500.0, "0xc": 0.0}
    monkeypatch.setattr("src.onchain.evm_archive.ArchiveRPC", FakeRPC)
    stale = [{"address": "0xa", "balance": 1_490_970_000},   # the WOO ghost
             {"address": "0xb", "balance": 12},
             {"address": "0xc", "balance": 600_000_000}]
    out = hs._verify_onchain("0xt", 1, stale)
    assert [h["address"] for h in out] == ["0xb"]
    assert out[0]["balance"] == 500.0, "balance must be the on-chain truth, not the snapshot"


def test_unreadable_wallet_is_excluded_not_kept_stale(monkeypatch):
    """balance_of returns None on an RPC failure and 0.0 for an empty wallet — a
    wallet we could not read is UNKNOWN, so it is dropped, never kept at its stale
    value."""
    class FlakyRPC(FakeRPC):
        def balance_of(self, token, holder, block="latest"):
            return None if holder == "0xa" else 42.0
    monkeypatch.setattr("src.onchain.evm_archive.ArchiveRPC", FlakyRPC)
    out = hs._verify_onchain("0xt", 1, [{"address": "0xa", "balance": 9e9},
                                        {"address": "0xb", "balance": 1}])
    assert [h["address"] for h in out] == ["0xb"]


def test_nothing_verifiable_returns_empty_not_stale(monkeypatch):
    """No data beats wrong data: if not one wallet could be read, refuse the list."""
    class DeadRPC(FakeRPC):
        def balance_of(self, token, holder, block="latest"):
            return None
    monkeypatch.setattr("src.onchain.evm_archive.ArchiveRPC", DeadRPC)
    assert hs._verify_onchain("0xt", 1, [{"address": "0xa", "balance": 9e9}]) == []


def test_verified_list_is_sorted_by_real_balance(monkeypatch):
    FakeRPC.balances = {"0xa": 10.0, "0xb": 999.0}
    monkeypatch.setattr("src.onchain.evm_archive.ArchiveRPC", FakeRPC)
    out = hs._verify_onchain("0xt", 1, [{"address": "0xa", "balance": 9e9},
                                        {"address": "0xb", "balance": 1}])
    assert [h["address"] for h in out] == ["0xb", "0xa"], "rank by on-chain truth"


def test_concentration_denominator_is_real_supply(monkeypatch):
    """`largest_entity_pct` must be a share of REAL totalSupply.

    It used to be `sum(fetched holder balances)` while being called 'share of supply'
    and gating loaded verdicts (lg>=10, lg>=15). Shrink the fetched list — which the
    on-chain verification does, keeping only ~30 verified wallets — and the number
    inflates. That is how a small holder list manufactures an operator.
    """
    import src.pipeline.anomaly_screener as a

    class RPC:
        def __init__(self, chain):
            pass

        def total_supply(self, token):
            return 1000.0
    monkeypatch.setattr("src.onchain.evm_archive.ArchiveRPC", RPC)
    monkeypatch.setattr("src.onchain.funder_graph.get_funders",
                        lambda addrs, chain, **k: {a: "0xf" for a in addrs})
    monkeypatch.setattr(a, "_funder_is_disperser", lambda f, c: False)

    # Solana skips the EVM supply path -> subset fallback, and must SAY so rather
    # than presenting a subset ratio as a share of supply.
    holders = [{"address": f"WALLET{i:02d}", "balance": 100 + i * 13} for i in range(12)]
    sig = a.effective_concentration_signal(holders, "TOK", "solana")
    assert sig is not None
    assert sig["supply_verified"] is False


def test_unverified_supply_blocks_the_loaded_gate(monkeypatch):
    """A subset ratio must never gate a loaded verdict — it just says 'unverified'."""
    import src.onchain.operator_id as oi
    monkeypatch.setattr(oi, "_historical_ledger",
                        lambda *a, **k: {"available": True, "exited": [], "holding": []})
    monkeypatch.setattr(oi, "_token_age_onchain", lambda *a, **k: 100.0)
    monkeypatch.setattr(oi, "_token_market", lambda t: {"available": False})
    monkeypatch.setattr(oi, "_cluster_holds_onchain", lambda *a, **k: True)
    monkeypatch.setattr("src.onchain.holder_snapshot.fetch_holders_evm",
                        lambda *a, **k: [{"address": "0xa", "balance": 1}])
    # dom=6, lg=90 would normally fire loaded_live_operator — but supply is unverified
    monkeypatch.setattr("src.pipeline.anomaly_screener.effective_concentration_signal",
                        lambda *a, **k: {"cluster_confidence": 30, "largest_entity_pct": 90,
                                         "dominant_cluster_wallets": [f"0x{i}" for i in range(6)],
                                         "supply_verified": False})

    class RPC:
        def __init__(self, chain):
            pass

        def token_decimals(self, t):
            return 18

        def total_supply(self, t):
            return None
    monkeypatch.setattr("src.onchain.evm_archive.ArchiveRPC", RPC)
    out = oi.identify_operator("0xt", "bsc")
    assert out["verdict"] != "loaded_live_operator"
    assert any("supply_unverified" in str(c) for c in out["caveats"])
