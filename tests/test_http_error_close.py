"""Rejected HTTP responses must release their sockets before fallback/retry."""
from __future__ import annotations

import io
import urllib.error
import urllib.request

import pytest


class TrackedHTTPError(urllib.error.HTTPError):
    def __init__(self, url: str, code: int = 403, headers: dict | None = None):
        super().__init__(url, code, "rejected", headers or {}, io.BytesIO(b"blocked"))
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


def test_solana_rate_limit_closes_response_and_preserves_retry_after(monkeypatch):
    from src.pipeline import solana_launch_stream

    error = TrackedHTTPError(
        "https://solana.invalid", code=429, headers={"Retry-After": "37"},
    )
    monkeypatch.setattr(
        solana_launch_stream.urllib.request,
        "urlopen",
        lambda request, timeout: (_ for _ in ()).throw(error),
    )

    with pytest.raises(solana_launch_stream.RpcPressureError) as raised:
        solana_launch_stream.JsonRpc("https://solana.invalid").call("getBlock", [])

    assert raised.value.kind == "rate_limited"
    assert raised.value.retry_after_seconds == 37
    assert error.was_closed and error.fp.closed

    malformed = TrackedHTTPError(
        "https://solana.invalid", code=429,
        headers={"Retry-After": "Infinity"},
    )
    monkeypatch.setattr(
        solana_launch_stream.urllib.request,
        "urlopen",
        lambda request, timeout: (_ for _ in ()).throw(malformed),
    )
    with pytest.raises(solana_launch_stream.RpcPressureError) as malformed_result:
        solana_launch_stream.JsonRpc("https://solana.invalid").call("getBlock", [])
    assert malformed_result.value.retry_after_seconds is None
    assert malformed.was_closed and malformed.fp.closed


def test_dune_closes_billing_rejection_and_preserves_failure_type(monkeypatch):
    from src.onchain import dune_client

    error = TrackedHTTPError(
        "https://api.dune.com/api/v1/query/17/execute", code=402,
        headers={"Retry-After": "75"},
    )
    monkeypatch.setenv("DUNE_API_KEY", "test-key")
    monkeypatch.setenv("DUNE_402_COOLDOWN_SECONDS", "60")
    monkeypatch.setattr(dune_client, "CREDITS_EXHAUSTED", False)
    monkeypatch.setattr(dune_client, "_CREDITS_EXHAUSTED_UNTIL", 0.0)
    calls = []

    def rejected(request, timeout):
        calls.append(request.full_url)
        raise error

    monkeypatch.setattr(
        dune_client.urllib.request, "urlopen",
        rejected,
    )

    result = dune_client._request("POST", "/query/17/execute")
    blocked = dune_client.run_sql_result("select blocked")

    assert result["ok"] is False
    assert result["error_kind"] == "credits_exhausted"
    assert result["http_status"] == 402
    assert result["retry_after_seconds"] == 75
    assert blocked["state"] == "deferred"
    assert blocked["error_kind"] == "credits_cooldown"
    assert len(calls) == 1
    assert error.was_closed and error.fp.closed


def test_dune_query_management_402_does_not_block_cached_execution(monkeypatch):
    from src.onchain import dune_client

    error = TrackedHTTPError(
        "https://api.dune.com/api/v1/query", code=402,
    )
    monkeypatch.setenv("DUNE_API_KEY", "test-key")
    monkeypatch.setenv("DUNE_402_COOLDOWN_SECONDS", "60")
    monkeypatch.setattr(dune_client, "CREDITS_EXHAUSTED", False)
    monkeypatch.setattr(dune_client, "_CREDITS_EXHAUSTED_UNTIL", 0.0)
    monkeypatch.setattr(
        dune_client.urllib.request, "urlopen",
        lambda request, timeout: (_ for _ in ()).throw(error),
    )

    result = dune_client._request("POST", "/query")

    assert result["error_kind"] == "billing_or_plan_required"
    assert result["http_status"] == 402
    assert dune_client.CREDITS_EXHAUSTED is False
    assert dune_client._CREDITS_EXHAUSTED_UNTIL == 0.0
    assert error.was_closed and error.fp.closed
