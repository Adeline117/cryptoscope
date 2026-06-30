"""Tests for the reliability / correctness upgrades to the 妖币 detection system.

These are pure-logic / monkeypatched UNIT tests — fast, deterministic, NO network.
They lock in the fixes that killed the recurring false positives:

  - token_registry:   exclude custody/treasury tokens (LINK/WBTC/USDT/RLUSD/weETH)
                      from operator hunting, but never a real meme.
  - entity_classify:  burn / empty addresses short-circuit to non-operator before
                      any RPC getCode call.
  - balance_of:       scale by the token's ACTUAL decimals (not hardcoded 1e18) —
                      the bug that collapsed the magnitude gate on 9-decimal tokens.
  - combined_balance_at strict: an RPC failure yields UNKNOWN (None), never a
                      silently-understated sum that looked like distribution.
  - get_transfer_logs: a failed chunk sets logs_complete=False so a caller never
                      reads an RPC error as "0 transfers / operator idle".
  - _distribution_history age-gate: too few nonzero balance samples → refuse to
                      judge ("?") instead of fabricating a distribution profile.
"""

import pytest

from src.onchain import token_registry
from src.onchain.entity_classify import classify_address
from src.onchain import evm_archive
from src.onchain.evm_archive import ArchiveRPC, combined_balance_at


# ----------------------------------------------------------------------------
# 1. token_registry.is_non_operator — pure logic, no network
# ----------------------------------------------------------------------------

@pytest.mark.parametrize("sym", ["USDT", "WBTC", "LINK", "weETH", "wstETH", "RLUSD"])
def test_custody_tokens_are_non_operator(sym):
    assert token_registry.is_non_operator(sym) is True


def test_is_non_operator_case_insensitive_and_trims():
    assert token_registry.is_non_operator(" usdt ") is True
    assert token_registry.is_non_operator("WeEtH") is True


@pytest.mark.parametrize("sym", ["SIREN", "ESPORTS", "MAME", "PEPE"])
def test_real_memes_are_operator_candidates(sym):
    assert token_registry.is_non_operator(sym) is False


def test_is_non_operator_empty_or_none_is_false():
    assert token_registry.is_non_operator(None) is False
    assert token_registry.is_non_operator("") is False


# ----------------------------------------------------------------------------
# 2. entity_classify.classify_address — burn/empty short-circuit (no RPC)
# ----------------------------------------------------------------------------

@pytest.mark.parametrize("addr", [
    "0x000000000000000000000000000000000000dead",
    "0x0000000000000000000000000000000000000000",
])
def test_burn_addresses_classified_burn_no_network(addr):
    # rpc=None and the burn branch returns BEFORE any getCode call, so this is
    # deterministic and never touches the network.
    out = classify_address(addr, "ethereum", rpc=None)
    assert out["type"] == "burn"
    assert out["is_operator_candidate"] is False
    assert out["custody"] is False


def test_burn_address_case_insensitive():
    upper = "0x000000000000000000000000000000000000DEAD"
    out = classify_address(upper, "ethereum", rpc=None)
    assert out["type"] == "burn"
    assert out["is_operator_candidate"] is False


def test_empty_address_short_circuits_to_unknown():
    out = classify_address("", "ethereum", rpc=None)
    assert out["type"] == "unknown"
    assert out["is_operator_candidate"] is False


# ----------------------------------------------------------------------------
# 3. ArchiveRPC.balance_of — scale by ACTUAL token decimals, not 1e18
# ----------------------------------------------------------------------------

def test_balance_of_scales_by_token_decimals(monkeypatch):
    rpc = ArchiveRPC("ethereum")
    raw = 123_456_000_000  # raw integer balance
    # known hex balance returned by the (mocked) eth_call
    monkeypatch.setattr(rpc, "_call", lambda *a, **k: {"result": hex(raw)})
    # 9-decimal token (common on BSC) — the bug used to divide by 1e18 always.
    monkeypatch.setattr(rpc, "token_decimals", lambda token: 9)
    bal = rpc.balance_of("0xtoken", "0x" + "ab" * 20)
    assert bal == raw / 1e9
    # and definitely NOT the 1e18-scaled (understated) value
    assert bal != raw / 1e18


def test_balance_of_respects_18_decimals(monkeypatch):
    rpc = ArchiveRPC("ethereum")
    raw = 5 * 10 ** 18
    monkeypatch.setattr(rpc, "_call", lambda *a, **k: {"result": hex(raw)})
    monkeypatch.setattr(rpc, "token_decimals", lambda token: 18)
    assert rpc.balance_of("0xtoken", "0x" + "cd" * 20) == 5.0


@pytest.mark.parametrize("res", ["0x", "", None])
def test_balance_of_empty_result_is_none_not_zero(monkeypatch, res):
    # An empty/null RPC result is a SOFT failure (rate-limited node), NOT a real
    # zero — must return None so a flaky read can't collapse the cluster to 0 and
    # fire a phantom total-dump 庄在卖. (A genuinely-empty wallet returns 64 hex
    # zeros, tested separately below.)
    rpc = ArchiveRPC("ethereum")
    monkeypatch.setattr(rpc, "_call", lambda *a, **k: {"result": res})
    monkeypatch.setattr(rpc, "token_decimals", lambda token: 18)
    assert rpc.balance_of("0xtoken", "0x" + "ef" * 20) is None


def test_balance_of_real_zero_balance(monkeypatch):
    # 64 hex zeros = a genuinely-empty wallet → 0.0 (distinct from the soft-fail None).
    rpc = ArchiveRPC("ethereum")
    monkeypatch.setattr(rpc, "_call", lambda *a, **k: {"result": "0x" + "0" * 64})
    monkeypatch.setattr(rpc, "token_decimals", lambda token: 18)
    assert rpc.balance_of("0xtoken", "0x" + "ef" * 20) == 0.0


def test_balance_of_rpc_error_returns_none(monkeypatch):
    rpc = ArchiveRPC("ethereum")
    def boom(*a, **k):
        raise RuntimeError("all RPCs failed")
    monkeypatch.setattr(rpc, "_call", boom)
    monkeypatch.setattr(rpc, "token_decimals", lambda token: 18)
    assert rpc.balance_of("0xtoken", "0x" + "11" * 20) is None


# ----------------------------------------------------------------------------
# 4. combined_balance_at — strict refuses to understate on RPC failure
# ----------------------------------------------------------------------------

def _patched_rpc(monkeypatch, balances: dict):
    """An ArchiveRPC whose balance_of returns canned values keyed by holder."""
    rpc = ArchiveRPC("ethereum")
    monkeypatch.setattr(rpc, "balance_of",
                        lambda token, holder, block="latest": balances.get(holder))
    return rpc


def test_combined_balance_strict_returns_none_on_partial_failure(monkeypatch):
    # 0xa reads fine, 0xb fails (None = RPC error, NOT a real zero).
    rpc = _patched_rpc(monkeypatch, {"0xa": 100.0, "0xb": None})
    got = combined_balance_at("0xtok", ["0xa", "0xb"], "ethereum", 123, rpc=rpc,
                              strict=True)
    assert got is None  # UNKNOWN, not the understated 100.0


def test_combined_balance_lenient_returns_partial_sum(monkeypatch):
    rpc = _patched_rpc(monkeypatch, {"0xa": 100.0, "0xb": None})
    got = combined_balance_at("0xtok", ["0xa", "0xb"], "ethereum", 123, rpc=rpc,
                              strict=False)
    assert got == 100.0


def test_combined_balance_strict_full_success_sums(monkeypatch):
    rpc = _patched_rpc(monkeypatch, {"0xa": 100.0, "0xb": 50.0})
    got = combined_balance_at("0xtok", ["0xa", "0xb"], "ethereum", 123, rpc=rpc,
                              strict=True)
    assert got == 150.0


# ----------------------------------------------------------------------------
# 5. get_transfer_logs — logs_complete tracks chunk completeness
# ----------------------------------------------------------------------------

def test_get_transfer_logs_partial_sets_incomplete(monkeypatch):
    rpc = ArchiveRPC("bsc")
    monkeypatch.setattr(rpc, "logs_head", lambda: 20000)  # 3 chunks @ 9000
    calls = {"n": 0}

    def fake_logs_call(method, params, timeout=20):
        calls["n"] += 1
        if calls["n"] == 2:           # second chunk explodes
            raise RuntimeError("range too wide")
        return {"result": [{"chunk": calls["n"]}]}

    monkeypatch.setattr(rpc, "_logs_call", fake_logs_call)
    out = rpc.get_transfer_logs("0xtok", 0, "latest")
    assert rpc.logs_complete is False        # a failed chunk is NOT "no transfers"
    assert len(out) == 2                      # the two good chunks still returned


def test_get_transfer_logs_all_succeed_is_complete(monkeypatch):
    rpc = ArchiveRPC("bsc")
    monkeypatch.setattr(rpc, "logs_head", lambda: 20000)

    def fake_logs_call(method, params, timeout=20):
        return {"result": [{"ok": True}]}

    monkeypatch.setattr(rpc, "_logs_call", fake_logs_call)
    out = rpc.get_transfer_logs("0xtok", 0, "latest")
    assert rpc.logs_complete is True
    assert len(out) == 3                      # 0-8999, 9000-17999, 18000-20000


def test_get_transfer_logs_head_lookup_failure_incomplete(monkeypatch):
    rpc = ArchiveRPC("bsc")
    def boom():
        raise RuntimeError("head failed")
    monkeypatch.setattr(rpc, "logs_head", boom)
    out = rpc.get_transfer_logs("0xtok", 0, "latest")
    assert rpc.logs_complete is False
    assert out == []


# ----------------------------------------------------------------------------
# 6. _distribution_history age-gate — refuse to judge on too-few nonzero samples
# ----------------------------------------------------------------------------

def test_distribution_history_insufficient_samples_refuses(monkeypatch):
    from src.pipeline import operator_sentinel

    # The function does `from src.onchain.evm_archive import ArchiveRPC,
    # operator_curve_evm` INSIDE its body, so patching the module attributes
    # (and the ArchiveRPC methods that would touch the network) is enough.
    monkeypatch.setattr(ArchiveRPC, "available", lambda self: True)
    monkeypatch.setattr(ArchiveRPC, "latest_block", lambda self: 1_000_000)
    monkeypatch.setattr(ArchiveRPC, "seconds_per_block", lambda self, sample=50000: 0.75)

    # A curve with <4 nonzero balance points → cannot judge distribution history.
    def fake_curve(token, addresses, chain, from_block, to_block=None,
                   n_points=10, pause=0.05):
        return {"block_series": [10, 20, 30, 40, 50],
                "balance_series": [0.0, 0.0, 100.0, 0.0, 150.0],  # only 2 nonzero
                "n_addresses": len(addresses)}

    monkeypatch.setattr(evm_archive, "operator_curve_evm", fake_curve)

    out = operator_sentinel._distribution_history("0xtok", "ethereum", ["0xa"])
    assert out["profile"].startswith("?")
    assert out["max_drawdown_pct"] is None
    assert out.get("nonzero_samples") == 2
