from types import SimpleNamespace

import pytest

from src.pipeline.meme_pipeline import _enrich_and_score


def _token():
    return {
        "token_symbol": "MEME",
        "token_address": "0xabc",
        "chain_id": "base",
        "liquidity_usd": 50_000,
    }


def _result(*, score=90, honeypot=False, risks=None, raw=None):
    return SimpleNamespace(
        risk_score=score,
        is_honeypot=honeypot,
        risks=risks or [],
        raw={"result": {"0xabc": {"verified": True}}} if raw is None else raw,
        info={"holder_count": 123},
    )


def _checker(result=None, error=None):
    class Checker:
        async def setup(self):
            return None

        async def teardown(self):
            return None

        async def check_token(self, chain, address):
            if error:
                raise error
            return result

    return Checker


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "result, expected_state",
    [
        (_result(raw={}, risks=["API fetch failed - unable to verify"]), "unknown"),
        (_result(score=69), "caution"),
        (_result(score=95, honeypot=True), "avoid"),
    ],
)
async def test_meme_recommendation_is_vetoed_without_passed_security(result, expected_state):
    token = _token()

    scored = await _enrich_and_score([token], security_checker_factory=_checker(result=result))

    assert scored == [token]
    assert token["security_state"] == expected_state
    assert token["security_qualified"] is False
    if result.is_honeypot:
        assert token["raw_alpha_score"] == 0
    else:
        assert token["raw_alpha_score"] > 0
    assert token["alpha_score"] == 0
    assert token["alpha_grade"] == "D"
    assert token["recommendation"] == "SKIP"


@pytest.mark.asyncio
async def test_meme_security_request_failure_is_not_scored_as_safe():
    token = _token()

    await _enrich_and_score(
        [token], security_checker_factory=_checker(error=RuntimeError("offline"))
    )

    assert token["security_state"] == "unknown"
    assert token["recommendation"] == "SKIP"
    assert "offline" in token["red_flags"][0]


@pytest.mark.asyncio
async def test_meme_verified_low_risk_result_can_retain_model_recommendation():
    token = _token()

    await _enrich_and_score(
        [token], security_checker_factory=_checker(result=_result(score=70))
    )

    assert token["security_state"] == "pass"
    assert token["security_qualified"] is True
    assert token["alpha_score"] == token["raw_alpha_score"]
    assert token["recommendation"] == token["raw_recommendation"] == "WATCH"
