"""Tests covering core pure-logic helpers and regressions for bugs fixed
during the optimization pass.

These avoid network/DB so they run fast and deterministically.
"""

import math
from datetime import datetime, timedelta, timezone

import pytest

from src.collectors.base import CollectedItem


# --------------------------------------------------------------------------
# config: environment variable resolution
# --------------------------------------------------------------------------

def test_resolve_env_full_match(monkeypatch):
    from src import config

    monkeypatch.setenv("CS_TEST_TOKEN", "secret123")
    assert config._resolve_env_vars("${CS_TEST_TOKEN}") == "secret123"


def test_resolve_env_missing_returns_none(monkeypatch):
    from src import config

    monkeypatch.delenv("CS_TEST_MISSING", raising=False)
    # A bare ${VAR} that is unset resolves to None (so callers can detect it)
    assert config._resolve_env_vars("${CS_TEST_MISSING}") is None


def test_resolve_env_partial_substitution(monkeypatch):
    from src import config

    monkeypatch.setenv("CS_HOST", "db.example.com")
    assert config._resolve_env_vars("postgres://${CS_HOST}:5432") == "postgres://db.example.com:5432"


def test_resolve_env_non_template_passthrough():
    from src import config

    assert config._resolve_env_vars("plain string") == "plain string"


def test_load_all_sources_returns_list():
    from src import config

    sources = config.load_all_sources()
    assert isinstance(sources, list)
    # The repo ships many source YAMLs; sanity-check the shape of an entry.
    assert all(isinstance(s, dict) for s in sources)


# --------------------------------------------------------------------------
# max_pain_gravity: regression for ZeroDivisionError on max_pain <= 0
# --------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_max_pain_zero_returns_none():
    from src.signals.max_pain_gravity import MaxPainGravitySignal

    sig = MaxPainGravitySignal()
    # max_pain == 0 must not raise ZeroDivisionError; returns None instead.
    result = await sig.evaluate(
        {"max_pain": 0, "underlying_price": 100, "days_to_expiry": 1}
    )
    assert result is None


@pytest.mark.asyncio
async def test_max_pain_negative_returns_none():
    from src.signals.max_pain_gravity import MaxPainGravitySignal

    sig = MaxPainGravitySignal()
    result = await sig.evaluate(
        {"max_pain": -5, "underlying_price": 100, "days_to_expiry": 1}
    )
    assert result is None


@pytest.mark.asyncio
async def test_max_pain_small_deviation_returns_none():
    from src.signals.max_pain_gravity import MaxPainGravitySignal

    sig = MaxPainGravitySignal()
    # 2% deviation is below MIN_DEVIATION_PCT (5%) → no signal.
    result = await sig.evaluate(
        {"max_pain": 100, "underlying_price": 102, "days_to_expiry": 1}
    )
    assert result is None


@pytest.mark.asyncio
async def test_max_pain_large_deviation_produces_signal():
    from src.signals.max_pain_gravity import MaxPainGravitySignal

    sig = MaxPainGravitySignal()
    # Price 20% above max_pain → expect a SHORT signal toward max_pain.
    result = await sig.evaluate(
        {"max_pain": 100, "underlying_price": 120, "days_to_expiry": 0}
    )
    assert result is not None
    assert result.direction == "SHORT"


# --------------------------------------------------------------------------
# telegram_sender: alert functions exist + long-text splitting
# --------------------------------------------------------------------------

def test_alert_functions_exist():
    # Regression: these were imported in 8 modules but never defined,
    # causing ImportError at runtime across meme/sniper/watchdog pipelines.
    from src.distribution import telegram_sender

    assert callable(telegram_sender.send_meme_alert)
    assert callable(telegram_sender.send_critical_alert)


@pytest.mark.asyncio
async def test_alerts_return_false_when_unconfigured(monkeypatch):
    from src.distribution import telegram_sender

    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    monkeypatch.delenv("TG_REVIEW_CHANNEL", raising=False)
    assert await telegram_sender.send_meme_alert("hi") is False
    assert await telegram_sender.send_critical_alert("hi") is False


def test_split_text_respects_max_len():
    from src.distribution.telegram_sender import _split_text

    text = "\n".join(f"line {i} " * 20 for i in range(200))
    chunks = _split_text(text, 4000)
    assert len(chunks) > 1
    assert all(len(c) <= 4000 for c in chunks)
    # No content lost (modulo the newline joins).
    assert sum(c.count("line") for c in chunks) == text.count("line")


def test_split_text_short_is_single_chunk():
    from src.distribution.telegram_sender import _split_text

    assert _split_text("short", 4000) == ["short"]


# --------------------------------------------------------------------------
# thread_formatter: regression for tweets[-1] on an empty thread
# --------------------------------------------------------------------------

def test_format_copyable_empty_thread(monkeypatch):
    from src.content import thread_formatter

    monkeypatch.setattr(thread_formatter, "format_for_twitter", lambda *a, **k: [])
    # Must not raise IndexError on tweets[-1]; returns empty string.
    assert thread_formatter.format_twitter_thread_copyable(object()) == ""


# --------------------------------------------------------------------------
# priority_scorer: pure scoring helpers stay within declared bounds
# --------------------------------------------------------------------------

def test_recency_score_decays_with_age():
    from src.analysis.priority_scorer import _recency_score

    now = datetime.now(timezone.utc)
    fresh = _recency_score(now, max_score=25)
    old = _recency_score(now - timedelta(days=3), max_score=25)
    assert fresh > old
    assert 0 <= old <= fresh <= 25


def test_recency_score_unknown_date():
    from src.analysis.priority_scorer import _recency_score

    assert _recency_score(None, max_score=25) == pytest.approx(25 * 0.3)


def test_score_item_capped_at_100():
    from src.analysis.priority_scorer import score_item

    item = CollectedItem(
        id="x",
        title="Major exploit hack drains billions in critical vulnerability",
        content="exploit hack rug critical " * 50,
        url="https://example.com",
        published_at=datetime.now(timezone.utc),
        metadata={"priority": "high", "category": "security"},
    )
    score = score_item(item)
    assert 0 <= score <= 100
