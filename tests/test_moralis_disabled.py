"""Moralis is off by default; it takes an explicit MORALIS_ENABLED to use it."""
from __future__ import annotations

import pytest

from src.onchain import moralis_client


def test_disabled_by_default_even_with_keys_configured(monkeypatch):
    monkeypatch.setenv("MORALIS_API_KEY", "SECRET-KEY-1")
    monkeypatch.setenv("MORALIS_API_KEY_2", "SECRET-KEY-2")
    monkeypatch.delenv("MORALIS_ENABLED", raising=False)

    assert moralis_client.enabled() is False
    assert moralis_client.keys() == []
    assert moralis_client.available() is False
    assert moralis_client.usable() is False


def test_get_makes_no_request_when_disabled(monkeypatch):
    monkeypatch.setenv("MORALIS_API_KEY", "SECRET-KEY")
    monkeypatch.delenv("MORALIS_ENABLED", raising=False)

    def forbidden(*_args, **_kwargs):  # any network attempt is a bug
        raise AssertionError("Moralis get() must not open a connection when disabled")

    monkeypatch.setattr(moralis_client.urllib.request, "urlopen", forbidden)
    assert moralis_client.get("erc20/0xabc/owners") is None


@pytest.mark.parametrize("flag", ["1", "true", "YES", "on"])
def test_explicit_opt_in_exposes_keys(monkeypatch, flag):
    monkeypatch.setenv("MORALIS_ENABLED", flag)
    monkeypatch.setenv("MORALIS_API_KEY", "KEY-A")
    monkeypatch.setenv("MORALIS_API_KEY_2", "KEY-B")
    monkeypatch.delenv("MORALIS_API_KEYS", raising=False)

    assert moralis_client.enabled() is True
    assert moralis_client.keys() == ["KEY-A", "KEY-B"]
    assert moralis_client.available() is True


def test_falsey_flag_stays_disabled(monkeypatch):
    monkeypatch.setenv("MORALIS_API_KEY", "KEY-A")
    for flag in ("", "0", "false", "no", "off"):
        monkeypatch.setenv("MORALIS_ENABLED", flag)
        assert moralis_client.enabled() is False
        assert moralis_client.keys() == []
