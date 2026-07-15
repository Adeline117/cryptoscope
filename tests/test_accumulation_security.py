from types import SimpleNamespace

import pytest

from src.pipeline.accumulation_pipeline import _security_gate


def _result(*, score=90, honeypot=False, risks=None, raw=None):
    return SimpleNamespace(
        risk_score=score,
        is_honeypot=honeypot,
        risks=risks or [],
        raw={"result": {"token": {"verified": True}}} if raw is None else raw,
    )


def _checker(result=None, error=None):
    class Checker:
        async def check_token(self, chain, address):
            if error:
                raise error
            return result

    return Checker


@pytest.mark.asyncio
async def test_watch_token_cannot_bypass_failed_security_request():
    candidate = {"source": "watch_token", "chain": "base", "address": "0xabc"}

    passed = await _security_gate(
        [candidate], checker_factory=_checker(error=RuntimeError("offline"))
    )

    assert passed == []
    assert candidate["security_state"] == "unknown"
    assert candidate["security_passed"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "result, expected_state",
    [
        (_result(score=90, raw={}, risks=["API fetch failed - unable to verify"]), "unknown"),
        (_result(score=69), "caution"),
        (_result(score=95, honeypot=True), "avoid"),
    ],
)
async def test_security_gate_rejects_unverified_or_unsafe_results(result, expected_state):
    candidate = {"source": "watch_token", "chain": "base", "address": "0xabc"}

    passed = await _security_gate([candidate], checker_factory=_checker(result=result))

    assert passed == []
    assert candidate["security_state"] == expected_state
    assert candidate["security_passed"] is False


@pytest.mark.asyncio
async def test_security_gate_accepts_verified_low_risk_watch_token():
    candidate = {"source": "watch_token", "chain": "base", "address": "0xabc"}

    passed = await _security_gate(
        [candidate], checker_factory=_checker(result=_result(score=70))
    )

    assert passed == [candidate]
    assert candidate["security_state"] == "pass"
    assert candidate["security_passed"] is True


@pytest.mark.asyncio
async def test_security_gate_unknown_chain_never_falls_back_to_ethereum():
    calls = []

    class Checker:
        async def check_token(self, chain, address):
            calls.append((chain, address))
            return _result(score=90)

    candidate = {
        "source": "watch_token", "chain": "avalanch-typo", "address": "0xabc",
    }

    passed = await _security_gate([candidate], checker_factory=Checker)

    assert passed == []
    assert calls == []
    assert candidate["security_state"] == "unknown"
    assert candidate["security_passed"] is False
    assert "unsupported chain" in candidate["security_risks"][0]


@pytest.mark.asyncio
async def test_security_gate_routes_avalanche_alias_to_43114():
    calls = []

    class Checker:
        async def check_token(self, chain, address):
            calls.append((chain, address))
            return _result(score=90)

    candidate = {"source": "watch_token", "chain": "avax", "address": "0xabc"}

    passed = await _security_gate([candidate], checker_factory=Checker)

    assert passed == [candidate]
    assert calls == [(43114, "0xabc")]
