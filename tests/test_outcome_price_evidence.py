"""Historical outcome prices must be exact-pool, exact-token and no-lookahead."""
from datetime import datetime, timedelta, timezone

import pytest


def _candle(opened: datetime, close: float) -> list:
    return [int(opened.timestamp()), close, close, close, close, 10]


def test_price_observation_uses_latest_closed_candle_without_lookahead(monkeypatch):
    from src.pipeline import outcome_tracker
    from src.pipeline import evidence

    target = datetime(2026, 7, 15, 12, tzinfo=timezone.utc)
    seen = []

    def ohlcv(chain, pool, token, before, timeframe):
        seen.append((chain, pool, token, before, timeframe))
        return {
            "network": "solana",
            "pool": pool,
            "token": token,
            "meta": {
                "base": {"address": "ExactToken"},
                "quote": {"address": "WrappedSol"},
            },
            "candles": [
                _candle(target - timedelta(hours=2), 90),
                _candle(target - timedelta(hours=1), 110),
                _candle(target, 999_999),
            ],
        }

    monkeypatch.setattr(evidence, "_ohlcv_evidence", ohlcv)
    got = outcome_tracker._price_observation_at(
        "ExactToken", "solana", "FrozenPool", target,
        retrieved_at=target + timedelta(minutes=5),
    )

    assert got["price"] == 110
    assert got["pool"] == got["pair"] == "FrozenPool"
    assert got["candle_at"] == target.isoformat()
    assert got["distance_seconds"] == 0
    assert got["identity_verified"] is True
    assert seen == [(
        "solana", "FrozenPool", "ExactToken",
        int((target + timedelta(hours=1)).timestamp()), "hour",
    )]


def test_price_observation_rejects_wrong_token_identity(monkeypatch):
    from src.pipeline import outcome_tracker
    from src.pipeline import evidence

    target = datetime(2026, 7, 15, 12, tzinfo=timezone.utc)
    monkeypatch.setattr(evidence, "_ohlcv_evidence", lambda *_args, **_kwargs: {
        "network": "solana",
        "pool": "FrozenPool",
        "token": "ExactToken",
        "meta": {"base": {"address": "OtherToken"}},
        "candles": [_candle(target - timedelta(hours=1), 110)],
    })

    with pytest.raises(outcome_tracker.PriceObservationInvalid,
                       match="token identity mismatch"):
        outcome_tracker._price_observation_at(
            "ExactToken", "solana", "FrozenPool", target,
            retrieved_at=target + timedelta(minutes=5),
        )


def test_price_observation_rejects_future_or_too_distant_candles(monkeypatch):
    from src.pipeline import outcome_tracker
    from src.pipeline import evidence

    target = datetime(2026, 7, 15, 12, tzinfo=timezone.utc)
    monkeypatch.setattr(evidence, "_ohlcv_evidence", lambda *_args, **_kwargs: {
        "network": "solana",
        "pool": "FrozenPool",
        "token": "ExactToken",
        "meta": {"base": {"address": "ExactToken"}},
        "candles": [
            _candle(target, 999_999),
            _candle(target - timedelta(hours=4), 1),
        ],
    })

    assert outcome_tracker._price_observation_at(
        "ExactToken", "solana", "FrozenPool", target,
        retrieved_at=target + timedelta(hours=2), max_distance_seconds=2 * 3600,
    ) is None


def test_price_observation_refuses_retrieval_before_target(monkeypatch):
    from src.pipeline import outcome_tracker
    from src.pipeline import evidence

    target = datetime(2026, 7, 15, 12, tzinfo=timezone.utc)
    monkeypatch.setattr(
        evidence,
        "_ohlcv_evidence",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("must not fetch")),
    )

    assert outcome_tracker._price_observation_at(
        "ExactToken", "solana", "FrozenPool", target,
        retrieved_at=target - timedelta(seconds=1),
    ) is None
