"""Unknown chain labels never borrow Ethereum evidence."""
from __future__ import annotations

from types import SimpleNamespace

import pytest


def test_explicit_chain_identity_and_provider_coverage():
    from src.collectors.contract_security import CHAIN_IDS
    from src.onchain import covalent_client
    from src.onchain.chain_identity import (
        canonical_chain,
        evm_chain_id,
        moralis_chain_slug,
        security_chain_id,
    )
    from src.pipeline.exit_monitor import _EVM_CHAIN_IDS

    assert canonical_chain(" AVAX ") == "avalanche"
    assert evm_chain_id("avalanche") == 43114
    assert moralis_chain_slug("avalanche") == "avalanche"
    assert security_chain_id("avalanche") == 43114
    assert CHAIN_IDS["avalanche"] == 43114
    assert _EVM_CHAIN_IDS["avalanche"] == 43114
    # GoldRush paths in this repo do not list Avalanche, so funder resolution
    # must use Moralis/Etherscan instead of pretending this fallback supports it.
    assert covalent_client.supports_chain(43114) is False

    for unknown in ("etheruem", "avalanchee", "", None):
        assert canonical_chain(unknown) is None
        assert evm_chain_id(unknown) is None
        assert security_chain_id(unknown) is None


@pytest.mark.asyncio
async def test_stage2_unknown_is_unavailable_and_avalanche_uses_43114():
    from src.pipeline import stage2_detector as stage2

    calls = []
    result = SimpleNamespace(
        risk_score=80,
        is_honeypot=False,
        risks=["measured"],
        raw={"result": {"0xt": {"is_open_source": "1"}}},
    )

    class Checker:
        async def check_token(self, chain_id, token):
            calls.append((chain_id, token))
            return result

    unknown = await stage2._commit_security(
        "0xt", "etheruem", checker_factory=Checker,
    )
    assert unknown["state"] == "unknown"
    assert "unsupported chain identity" in unknown["reason"]
    assert calls == []

    avalanche = await stage2._commit_security(
        "0xt", "avax", checker_factory=Checker,
    )
    assert avalanche["state"] == "pass"
    assert calls == [(43114, "0xt")]


@pytest.mark.asyncio
async def test_contract_checker_rejects_unknown_before_provider_call(monkeypatch):
    from src.collectors.contract_security import ContractSecurityChecker

    checker = ContractSecurityChecker()
    calls = []

    async def noop():
        return None

    async def checked(chain_id, address):
        calls.append((chain_id, address))
        return "checked"

    monkeypatch.setattr(checker, "setup", noop)
    monkeypatch.setattr(checker, "teardown", noop)
    monkeypatch.setattr(checker, "_check_evm_token", checked)

    with pytest.raises(ValueError, match="unsupported chain identity"):
        await checker.check_token("etheruem", "0xt")
    assert calls == []
    assert await checker.check_token("AVAX", "0xt") == "checked"
    assert calls == [(43114, "0xt")]


def test_onchain_enrich_skips_unknown_and_routes_avalanche(monkeypatch):
    from src.onchain import holder_snapshot as hs
    from src.pipeline import anomaly_screener as screener

    calls = []
    monkeypatch.setattr(
        hs, "get_snapshots",
        lambda token, chain, limit: calls.append(("history", chain)) or [],
    )
    monkeypatch.setattr(
        hs, "fetch_holders_evm",
        lambda token, chain_id, max_pages: calls.append(("holders", chain_id)) or [
            {"address": "0xABC", "balance": 10},
        ],
    )
    monkeypatch.setattr(
        hs, "save_snapshot",
        lambda token, chain, holders: calls.append(("save", chain)),
    )
    monkeypatch.setattr(screener, "_smart_money_set", lambda chain: {})
    monkeypatch.setattr(
        screener, "effective_concentration_signal",
        lambda holders, token, chain: {"chain": chain},
    )

    assert screener.onchain_enrich("0xt", "etheruem") is None
    assert calls == []

    enriched = screener.onchain_enrich("0xt", "avax")
    assert enriched and enriched["holder_count"] == 1
    assert enriched["concentration"] == {"chain": "avalanche"}
    assert calls == [
        ("history", "avalanche"),
        ("holders", 43114),
        ("save", "avalanche"),
    ]


def test_funder_graph_unknown_never_queries_and_avalanche_uses_moralis(
    tmp_path, monkeypatch,
):
    from src.onchain import funder_graph as fg
    from src.onchain import moralis_client

    network_calls = []
    monkeypatch.setattr(fg, "_keys", lambda: ["etherscan-key"])
    monkeypatch.setattr(
        fg, "_fetch_first_funder_evm",
        lambda *args, **kwargs: network_calls.append(("etherscan", args)) or "wrong",
    )
    monkeypatch.setattr(moralis_client, "usable", lambda: True)
    monkeypatch.setattr(
        fg, "_fetch_first_funder_moralis",
        lambda address, chain: network_calls.append(("moralis", chain)) or "0xfunder",
    )

    unknown_db = tmp_path / "unknown.db"
    assert fg.get_funders(["0xABC"], "etheruem", db_path=unknown_db) == {}
    assert network_calls == []
    assert not unknown_db.exists()

    got = fg.get_funders(["0xABC"], "avax", db_path=tmp_path / "avax.db")
    assert got == {"0xabc": "0xfunder"}
    assert network_calls == [("moralis", "avalanche")]


@pytest.mark.asyncio
async def test_exit_monitor_skips_unknown_and_routes_avalanche(monkeypatch):
    from src.pipeline import exit_monitor

    monkeypatch.setattr(exit_monitor, "_recent_accumulation_tokens", lambda: [
        {"asset": "TYPO", "chain": "etheruem", "address": "0xtypo"},
        {"asset": "AVAX", "chain": "avax", "address": "0xavax"},
    ])
    calls = []
    monkeypatch.setattr(
        exit_monitor, "_fetch_labeled_transfers",
        lambda token, chain_id: calls.append((token, chain_id)) or [],
    )

    result = await exit_monitor.run_exit_monitor(send=False)

    assert result == {"status": "complete", "checked": 0, "exits": 0}
    assert calls == [("0xavax", 43114)]
