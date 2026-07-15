"""Finalized Solana mint facts are local evidence, never a transaction path."""
from __future__ import annotations

import json

import pytest

from src.onchain import solana_token_authority as authority


def _result(*, owner=authority.SPL_TOKEN_PROGRAM, slot=123, executable=False,
            mint_authority=None, freeze_authority=None, initialized=True,
            info_overrides=None):
    info = {
        "decimals": 6, "supply": "1000000", "isInitialized": initialized,
        "mintAuthority": mint_authority, "freezeAuthority": freeze_authority,
    }
    info.update(info_overrides or {})
    return {
        "context": {"slot": slot},
        "value": {
            "owner": owner, "executable": executable,
            "data": {"program": "spl-token", "parsed": {"type": "mint", "info": info}},
        },
    }


def test_legacy_spl_requires_finalized_parsed_null_authorities():
    calls = []

    def rpc(method, params):
        calls.append((method, params))
        return _result()

    got = authority.inspect_mint("Mint", rpc_call=rpc)

    assert calls == [("getAccountInfo", ["Mint", {
        "encoding": "jsonParsed", "commitment": "finalized",
    }])]
    assert got["state"] == "pass"
    assert got["slot"] == 123 and got["owner"] == authority.SPL_TOKEN_PROGRAM
    assert got["mint_authority"] is None and got["freeze_authority"] is None
    assert got["checked_at"].endswith("+00:00") and len(got["raw_hash"]) == 64


@pytest.mark.parametrize(
    "mint_authority, freeze_authority, expected",
    [("MintPower", None, {"mint_authority"}),
     (None, "FreezePower", {"freeze_authority"}),
     ("MintPower", "FreezePower", {"mint_authority", "freeze_authority"})],
)
def test_any_live_authority_is_avoid(mint_authority, freeze_authority, expected):
    got = authority.inspect_mint("Mint", rpc_call=lambda *_args: _result(
        mint_authority=mint_authority, freeze_authority=freeze_authority))

    assert got["state"] == "avoid"
    assert set(got["hard_flags"]) == expected


def test_token_2022_without_authorities_stays_caution_until_extensions_are_parsed():
    got = authority.inspect_mint("Mint", rpc_call=lambda *_args: _result(
        owner=authority.TOKEN_2022_PROGRAM))

    assert got["state"] == "caution"
    assert got["cautions"] == ["token_2022_extensions_not_fully_parsed"]


def test_rpc_failure_is_unknown():
    def down(*_args):
        raise TimeoutError("RPC offline")

    got = authority.inspect_mint("Mint", rpc_call=down)

    assert got["state"] == "unknown"
    assert "RPC unavailable" in got["reason"]


@pytest.mark.parametrize(
    "payload, reason",
    [(_result(owner="WrongProgram"), "owner"),
     (_result(slot=None), "slot"),
     (_result(executable=True), "executable"),
     (_result(info_overrides={"mintAuthority": None}), ""),
     ({"context": {"slot": 123}, "value": None}, "not found")],
)
def test_owner_and_malformed_reads_are_unknown(payload, reason):
    # The fourth case stays structurally valid; the assertion deliberately verifies
    # that a present null authority is not confused with a missing field.
    got = authority.inspect_mint("Mint", rpc_call=lambda *_args: payload)

    if reason:
        assert got["state"] == "unknown"
        assert reason in got["reason"]
    else:
        assert got["state"] == "pass"


def test_missing_parsed_authority_field_is_unknown():
    payload = _result()
    del payload["value"]["data"]["parsed"]["info"]["freezeAuthority"]

    got = authority.inspect_mint("Mint", rpc_call=lambda *_args: payload)

    assert got["state"] == "unknown"
    assert got["unknown_fields"] == ["freezeAuthority"]


def test_rpc_prefers_stream_endpoint_and_closes_response(monkeypatch):
    seen = {}

    class Response:
        closed = False

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            self.closed = True

        def read(self):
            return json.dumps({"jsonrpc": "2.0", "id": 1, "result": {"ok": True}}).encode()

    response = Response()

    def urlopen(request, timeout):
        seen.update(url=request.full_url, timeout=timeout)
        return response

    monkeypatch.setenv("SOLANA_STREAM_RPC_URL", "https://stream-rpc.example")
    monkeypatch.setenv("SOLANA_RPC_URL", "https://other-rpc.example")
    monkeypatch.setattr(authority.urllib.request, "urlopen", urlopen)

    assert authority._rpc("method", []) == {"ok": True}
    assert seen == {"url": "https://stream-rpc.example", "timeout": 12}
    assert response.closed is True
