"""Operator vs issuer: an allocated cluster is a treasury, not an operator.

Two coins in a 45-coin survey of shortable small caps looked exactly like operators —
23 wallets, one shared non-disperser funder, 67% of real supply, all EOAs, ownership
renounced. Both were issuers: AT minted 5 of 6 wallets straight from 0x0; CHIP funded
7 of 8 from one distributor. Neither ever bought a token.
"""

import src.onchain.operator_id as oi

PAIR = "0xpair"
ROUTER = "0xrouter"
ZERO = "0x0000000000000000000000000000000000000000"


def _setup(monkeypatch, first_from: dict):
    monkeypatch.setattr(oi, "_token_pairs", lambda t, c: {PAIR})
    monkeypatch.setattr(oi, "_infra", lambda c: {"routers": {ROUTER}, "bridges": set(),
                                                 "disperse": set(), "burn": set()})
    monkeypatch.setattr("src.onchain.goplus_client.token_security",
                        lambda t, c: {"available": True, "creator_address": "0xcreator"})
    monkeypatch.setattr("src.onchain.entity_classify.classify_address",
                        lambda a, c: {"type": "eoa"})
    import src.onchain.moralis_client as mc
    monkeypatch.setattr(mc, "usable", lambda: True)

    def fake_get(path):
        w = path.split("/")[0].lower()
        src = first_from.get(w)
        if src is None:
            return {"result": []}
        return {"result": [{"token_address": "0xt", "to_address": w, "from_address": src}]}
    monkeypatch.setattr(mc, "get", fake_get)


def test_minted_cluster_is_allocated(monkeypatch):
    """AT: first inflow straight from the zero address = minted, never bought."""
    _setup(monkeypatch, {f"0xw{i}": ZERO for i in range(4)})
    out = oi.acquisition_mode("0xt", "bsc", [f"0xw{i}" for i in range(4)])
    assert out["verdict"] == "allocated" and out["bought"] == 0
    assert out["top_source"] == ZERO


def test_single_distributor_is_allocated(monkeypatch):
    """CHIP: one wallet hands the float to everyone. Concentration = the issuer."""
    _setup(monkeypatch, {f"0xw{i}": "0xdistributor" for i in range(5)})
    out = oi.acquisition_mode("0xt", "arbitrum", [f"0xw{i}" for i in range(5)])
    assert out["verdict"] == "allocated"


def test_bought_from_pool_is_an_operator(monkeypatch):
    """An operator accumulates FROM THE MARKET — first inflow from the pair/router."""
    _setup(monkeypatch, {"0xw0": PAIR, "0xw1": ROUTER, "0xw2": PAIR})
    out = oi.acquisition_mode("0xt", "bsc", ["0xw0", "0xw1", "0xw2"])
    assert out["verdict"] == "bought" and out["allocated"] == 0


def test_unresolvable_wallet_is_not_counted_as_either(monkeypatch):
    """No transfer history = we don't know how it acquired. Never a silent default."""
    _setup(monkeypatch, {"0xw0": PAIR})           # 0xw1 has no rows
    out = oi.acquisition_mode("0xt", "bsc", ["0xw0", "0xw1"])
    assert out["unresolved"] == 1 and out["bought"] == 1 and out["allocated"] == 0


def test_all_unresolvable_is_unknown_not_operator(monkeypatch):
    _setup(monkeypatch, {})
    out = oi.acquisition_mode("0xt", "bsc", ["0xw0", "0xw1"])
    assert out["verdict"] == "unknown", "no data must not read as 'bought'"


def test_no_source_available_is_unknown(monkeypatch):
    import src.onchain.moralis_client as mc
    monkeypatch.setattr(mc, "usable", lambda: False)
    out = oi.acquisition_mode("0xt", "bsc", ["0xw0"])
    assert out["available"] is False and out["verdict"] == "unknown"
