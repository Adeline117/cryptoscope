"""A backtest that sees the future manufactures an edge that does not exist.

These tests pin the replay cutoff at the level where a leak would actually enter:
the row filter and the balance read. They are hermetic — no network — so they keep
guarding after the APIs change.
"""

import src.onchain.operator_id as oi


def test_before_excludes_rows_past_cutoff():
    assert oi._before({"block_number": "100"}, 100) is True     # inclusive
    assert oi._before({"block_number": "101"}, 100) is False
    assert oi._before({"block_number": 99}, 100) is True


def test_before_is_a_noop_when_not_replaying():
    """as_of_block=None must preserve live behavior exactly (zero regression)."""
    assert oi._before({}, None) is True
    assert oi._before({"block_number": None}, None) is True


def test_undatable_row_is_excluded_under_replay():
    """A row with no block_number cannot be placed in time. Including it risks leaking
    the future; excluding it merely understates activity. Only the former can fake an
    edge, so we exclude."""
    assert oi._before({}, 100) is False
    assert oi._before({"block_number": None}, 100) is False
    assert oi._before({"block_number": "not-a-number"}, 100) is False


def test_early_inflow_stops_at_cutoff(monkeypatch):
    """ASC walk: once a page contains a post-cutoff transfer, later pages are later
    still — the walk must stop rather than page on and silently include them."""
    pages = [
        {"result": [{"block_number": "10", "to_address": "0xA", "value": str(10 * 10**18)},
                    {"block_number": "20", "to_address": "0xB", "value": str(20 * 10**18)}],
         "cursor": "c1"},
        {"result": [{"block_number": "30", "to_address": "0xC", "value": str(30 * 10**18)},
                    {"block_number": "40", "to_address": "0xD", "value": str(40 * 10**18)}],
         "cursor": "c2"},
        {"result": [{"block_number": "50", "to_address": "0xE", "value": str(50 * 10**18)}],
         "cursor": None},
    ]
    calls = {"n": 0}

    def fake_get(path):
        i = calls["n"]
        calls["n"] += 1
        return pages[i] if i < len(pages) else None

    monkeypatch.setattr(oi, "_before", oi._before)      # keep the real filter
    import src.onchain.moralis_client as mc
    monkeypatch.setattr(mc, "usable", lambda: True)
    monkeypatch.setattr(mc, "get", fake_get)

    inflow = oi._early_inflow_moralis("0xtok", "bsc", 18, max_pages=10, as_of_block=35)
    assert set(inflow) == {"0xa", "0xb", "0xc"}, f"leaked past cutoff: {sorted(inflow)}"
    assert "0xd" not in inflow and "0xe" not in inflow
    assert calls["n"] == 2, "must stop paging once past the cutoff, not walk to the end"


def test_early_inflow_unrestricted_when_live(monkeypatch):
    pages = [{"result": [{"block_number": "10", "to_address": "0xA", "value": str(10**18)},
                         {"block_number": "999", "to_address": "0xZ", "value": str(10**18)}],
              "cursor": None}]
    import src.onchain.moralis_client as mc
    monkeypatch.setattr(mc, "usable", lambda: True)
    monkeypatch.setattr(mc, "get", lambda p: pages[0])
    inflow = oi._early_inflow_moralis("0xtok", "bsc", 18, as_of_block=None)
    assert set(inflow) == {"0xa", "0xz"}     # live sees everything


def test_cluster_holds_reads_balance_at_as_of_block(monkeypatch):
    """The main leak: balance_of was pinned to "latest", so a wallet that emptied
    AFTER the replay block looked already-exited at that block."""
    seen = []

    class FakeRPC:
        def __init__(self, chain):
            pass

        def balance_of(self, token, holder, block="latest"):
            seen.append(block)
            return 0.0 if block == "latest" else 5.0

    monkeypatch.setattr("src.onchain.evm_archive.ArchiveRPC", FakeRPC)
    assert oi._cluster_holds_onchain("0xt", "bsc", ["0xw"], as_of_block=1234) is True
    assert seen == [1234], f"replay must read at the as-of block, got {seen}"

    seen.clear()
    assert oi._cluster_holds_onchain("0xt", "bsc", ["0xw"]) is False
    assert seen == ["latest"], "live path must be unchanged"


def test_replay_disables_the_undateable_dimensions(monkeypatch):
    """Current holder graph and DexScreener market are TODAY-only. Under replay they
    must be marked unavailable, never served stale — that would leak the future."""
    monkeypatch.setattr(oi, "_historical_ledger",
                        lambda *a, **k: {"available": False, "exited": [], "holding": []})
    monkeypatch.setattr(oi, "_token_age_onchain", lambda *a, **k: 100.0)
    monkeypatch.setattr(oi, "_token_market", lambda t: (_ for _ in ()).throw(
        AssertionError("market must NOT be fetched under replay")))

    class FakeRPC:
        def __init__(self, chain):
            pass

        def token_decimals(self, t):
            return 18

        def total_supply(self, t):
            return 1e9

    monkeypatch.setattr("src.onchain.evm_archive.ArchiveRPC", FakeRPC)

    out = oi.identify_operator("0xt", "bsc", as_of_block=999)
    assert out["as_of_block"] == 999
    assert out["current"]["current_graph_available"] is False
    assert out["current"]["market"]["available"] is False
    assert any("replay" in str(c) for c in out["caveats"])
