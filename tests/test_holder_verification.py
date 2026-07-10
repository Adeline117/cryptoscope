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
