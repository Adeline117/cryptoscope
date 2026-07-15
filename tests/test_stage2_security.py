"""Stage-2 ignition never turns unavailable contract data into a LONG signal."""
from __future__ import annotations

from types import SimpleNamespace

import pytest


def _factory(result=None, error=None):
    class Checker:
        async def check_token(self, chain_id, token):
            if error:
                raise error
            return result
    return Checker


@pytest.mark.asyncio
async def test_security_exception_and_neutral_fallback_are_unknown():
    from src.pipeline import stage2_detector as stage2

    failed = await stage2._commit_security(
        "0xt", "base", checker_factory=_factory(error=RuntimeError("rpc down")))
    assert failed["state"] == "unknown" and failed["score"] is None

    neutral = SimpleNamespace(risk_score=50, is_honeypot=False,
                              risks=["API fetch failed — unable to verify"], raw={})
    got = await stage2._commit_security("0xt", "base", checker_factory=_factory(neutral))
    assert got["state"] == "unknown"


@pytest.mark.asyncio
async def test_security_thresholds_are_explicit():
    from src.pipeline import stage2_detector as stage2

    for score, honeypot, expected in ((80, False, "pass"), (60, False, "caution"),
                                       (90, True, "avoid"), (40, False, "avoid")):
        result = SimpleNamespace(risk_score=score, is_honeypot=honeypot,
                                 risks=["measured"], raw={"result": {"0xt": {}}})
        got = await stage2._commit_security(
            "0xt", "base", checker_factory=_factory(result))
        assert got["state"] == expected


@pytest.mark.asyncio
async def test_unknown_security_records_no_directional_signal(monkeypatch):
    from src.pipeline import stage2_detector as stage2
    from src.trading import signal_scorecard

    calls = []
    monkeypatch.setattr(signal_scorecard, "record_signal",
                        lambda **kwargs: calls.append(kwargs))

    async def unknown(token, chain):
        return {"state": "unknown", "score": None, "reason": "not indexed"}

    result = await stage2._emit_launch(
        {"token": "0xt", "chain": "base", "symbol": "T"},
        {"priceUsd": "1", "baseToken": {"symbol": "T"}},
        {"confidence": 80}, send=False, security_check=unknown)
    assert result["directional_recorded"] is False
    assert calls == []


@pytest.mark.asyncio
async def test_passed_security_records_paper_only_experiment(monkeypatch):
    from src.pipeline import stage2_detector as stage2
    from src.trading import signal_scorecard

    calls = []
    monkeypatch.setattr(signal_scorecard, "record_signal",
                        lambda **kwargs: calls.append(kwargs))

    async def passed(token, chain):
        return {"state": "pass", "score": 82, "risks": []}

    result = await stage2._emit_launch(
        {"token": "0xt", "chain": "base", "symbol": "T"},
        {"priceUsd": "1", "baseToken": {"symbol": "T"}},
        {"confidence": 80}, send=False, security_check=passed)
    assert result["directional_recorded"] is True
    assert calls[0]["metadata"]["paper_only"] is True
    assert calls[0]["metadata"]["security_gate"]["state"] == "pass"


@pytest.mark.asyncio
async def test_blocked_candidate_stays_on_watchlist(monkeypatch):
    from src.onchain import watchlist
    from src.pipeline import stage2_detector as stage2

    monkeypatch.setattr(watchlist, "get_active", lambda: [
        {"token": "0xt", "chain": "base", "symbol": "T"}])
    monkeypatch.setattr(stage2, "_best_pair", lambda *args, **kwargs: {
        "priceUsd": "1", "volume": {"m5": 5_000, "h1": 10_000},
        "priceChange": {"m5": 5}, "txns": {"m5": {"buys": 10, "sells": 2}},
        "baseToken": {"symbol": "T"},
    })

    async def blocked(*args, **kwargs):
        return {"directional_recorded": False,
                "security": {"state": "unknown"}}

    monkeypatch.setattr(stage2, "_emit_launch", blocked)
    statuses = []
    monkeypatch.setattr(watchlist, "set_status",
                        lambda *args: statuses.append(args))
    result = await stage2.run_stage2_detector(send=False)
    assert result["candidates"] == 1 and result["events"] == 0
    assert result["blocked_security"] == 1 and statuses == []


@pytest.mark.asyncio
async def test_stage2_pair_requests_run_off_event_loop(monkeypatch):
    import threading

    from src.onchain import watchlist
    from src.pipeline import stage2_detector as stage2

    loop_thread = threading.get_ident()
    request_threads = []
    monkeypatch.setattr(watchlist, "get_active", lambda: [
        {"token": "0xt", "chain": "base", "symbol": "T"}])

    def best_pair(*_args, **_kwargs):
        request_threads.append(threading.get_ident())
        return None

    monkeypatch.setattr(stage2, "_best_pair", best_pair)

    result = await stage2.run_stage2_detector(send=False)

    assert result == {"status": "complete", "checked": 0, "candidates": 0,
                      "events": 0, "blocked_security": 0}
    assert request_threads and request_threads[0] != loop_thread
