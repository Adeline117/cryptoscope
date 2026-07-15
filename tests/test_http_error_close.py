"""Rejected HTTP responses must release their sockets before fallback/retry."""
from __future__ import annotations

import io
import urllib.error
import urllib.request

import pytest


class TrackedHTTPError(urllib.error.HTTPError):
    def __init__(self, url: str, code: int = 403):
        super().__init__(url, code, "rejected", {}, io.BytesIO(b"blocked"))
        self.was_closed = False

    def close(self):
        self.was_closed = True
        super().close()


def test_archive_rpc_closes_every_rejected_fallback_response(monkeypatch):
    from src.onchain import evm_archive

    errors = []

    def rejected(request, timeout):
        error = TrackedHTTPError(request.full_url)
        errors.append(error)
        raise error

    monkeypatch.setattr(evm_archive.urllib.request, "urlopen", rejected)
    rpc = evm_archive.ArchiveRPC("bsc")
    rpc.rpcs = ["https://rpc-one.invalid", "https://rpc-two.invalid"]
    monkeypatch.setitem(evm_archive._LOGS_RPCS, "bsc", rpc.rpcs)

    with pytest.raises(RuntimeError, match="all RPCs failed"):
        rpc._call("eth_blockNumber", [])
    with pytest.raises(RuntimeError, match="all logs RPCs failed"):
        rpc._logs_call("eth_blockNumber", [])

    assert len(errors) == 4
    assert all(error.was_closed and error.fp.closed for error in errors)


def test_moralis_closes_rejected_response_before_key_rotation(monkeypatch):
    from src.onchain import moralis_client

    error = TrackedHTTPError("https://deep-index.moralis.io/test", code=400)
    monkeypatch.setattr(moralis_client, "keys", lambda: ["test-key"])
    monkeypatch.setattr(
        moralis_client.urllib.request, "urlopen",
        lambda request, timeout: (_ for _ in ()).throw(error),
    )

    assert moralis_client.get("test") is None
    assert error.was_closed and error.fp.closed


def test_geckoterminal_closes_rejected_response_before_fallback(monkeypatch):
    from src.pipeline import anomaly_screener

    error = TrackedHTTPError("https://api.geckoterminal.com/api/v2/test", code=429)
    monkeypatch.setattr(
        anomaly_screener.urllib.request, "urlopen",
        lambda request, timeout: (_ for _ in ()).throw(error),
    )

    assert anomaly_screener._gt_base_addresses("test") == []
    assert error.was_closed and error.fp.closed


def test_realtime_sentinel_closes_market_and_funding_rejections(monkeypatch):
    from src.pipeline import operator_sentinel

    errors = []

    def rejected(request, timeout):
        error = TrackedHTTPError(request.full_url, code=403)
        errors.append(error)
        raise error

    monkeypatch.setattr(operator_sentinel.urllib.request, "urlopen", rejected)
    operator_sentinel._FUNDING_CACHE.clear()

    assert operator_sentinel._dex("0xtoken", "bsc") == {}
    assert operator_sentinel._funding_rate("FDLEAK") is None

    # DexScreener plus Gate and MEXC fallback: every swallowed HTTPError must have
    # released its response/socket before the 20-second watcher starts a new pass.
    assert len(errors) == 3
    assert all(error.was_closed and error.fp.closed for error in errors)


def test_factory_rpc_clients_close_rejected_responses(monkeypatch):
    from src.pipeline import evm_factory_stream, solana_launch_stream

    evm_errors = []

    def evm_rejected(request, timeout):
        error = TrackedHTTPError(request.full_url, code=429)
        evm_errors.append(error)
        raise error

    monkeypatch.setattr(evm_factory_stream.urllib.request, "urlopen", evm_rejected)
    with pytest.raises(RuntimeError, match="all EVM RPC endpoints failed"):
        evm_factory_stream.JsonRpc(
            ("https://rpc-one.invalid", "https://rpc-two.invalid"),
        ).call("eth_getLogs", [])

    solana_error = TrackedHTTPError("https://solana.invalid", code=413)
    monkeypatch.setattr(
        solana_launch_stream.urllib.request,
        "urlopen",
        lambda request, timeout: (_ for _ in ()).throw(solana_error),
    )
    with pytest.raises(urllib.error.HTTPError):
        solana_launch_stream.JsonRpc("https://solana.invalid").call("getBlock", [])

    assert len(evm_errors) == 2
    assert all(error.was_closed and error.fp.closed for error in evm_errors)
    assert solana_error.was_closed and solana_error.fp.closed
