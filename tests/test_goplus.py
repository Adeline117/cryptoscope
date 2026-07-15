"""GoPlus code-risk facts. The tests here guard the failure modes, not the happy path.

This codebase's recurring disease is turning a missing read into a confident
conclusion. A security API is the most dangerous place for that: "we couldn't check"
must never render as "safe".
"""

import json

import src.onchain.goplus_client as gp


def _sec(**over):
    base = {"available": True, "checked_at": "2026-07-10T00:00:00+00:00",
            "is_open_source": 1, "flags_knowable": True,
            "flags": {"is_honeypot": 0, "is_mintable": 0, "transfer_pausable": 0,
                      "owner_change_balance": 0, "hidden_owner": 0,
                      "can_take_back_ownership": 0, "is_blacklisted": 0,
                      "trading_cooldown": 0, "cannot_sell_all": 0, "is_proxy": 0,
                      "buy_tax": 0.0, "sell_tax": 0.0},
            "owner_address": "0x0000000000000000000000000000000000000000",
            "creator_address": "0xdead",
            "lp": {"holder_count": 2, "n_holders_seen": 2, "n_locked": 2,
                   "all_locked": True, "holders": []},
            "holder_count": 100, "top_holders": []}
    for k, v in over.items():
        if k in ("flags", "lp"):
            base[k] = {**base[k], **v}
        else:
            base[k] = v
    return base


class _JsonResponse:
    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self):
        return json.dumps(self.payload).encode()


def test_response_token_match_is_exact_after_evm_normalization(monkeypatch):
    token = "0xAbC0000000000000000000000000000000000000"
    payload = {"code": 1, "result": {token.lower(): {"is_open_source": "1"}}}
    monkeypatch.setattr(gp.urllib.request, "urlopen",
                        lambda *_args, **_kwargs: _JsonResponse(payload))
    gp._CACHE.clear()

    result = gp.token_security(token, "base")

    assert result["available"] is True


def test_response_for_different_token_is_unavailable(monkeypatch):
    token = "0xAbC0000000000000000000000000000000000000"
    other = "0xDef0000000000000000000000000000000000000"
    payload = {"code": 1, "result": {other: {"is_open_source": "1"}}}
    monkeypatch.setattr(gp.urllib.request, "urlopen",
                        lambda *_args, **_kwargs: _JsonResponse(payload))
    gp._CACHE.clear()

    result = gp.token_security(token, "base")

    assert result["available"] is False
    assert "token mismatch" in result["reason"]


def test_fetch_failure_is_unchecked_not_clean(monkeypatch):
    monkeypatch.setattr(gp, "token_security",
                        lambda t, c: {"available": False, "reason": "fetch failed"})
    r = gp.rug_risk("0xt", "bsc")
    assert r["available"] is False
    assert "未检查" in r["note"] and "不等于安全" in r["note"]
    assert "facts" not in r          # must not present an empty fact list as "clean"


def test_closed_source_flags_are_unknown_not_false(monkeypatch):
    """A closed contract is a NON-read. Its flags are unknowable, not zero."""
    monkeypatch.setattr(gp, "token_security",
                        lambda t, c: _sec(is_open_source=0, flags_knowable=False))
    r = gp.rug_risk("0xt", "bsc")
    assert r["available"] is True
    assert any("未开源" in u for u in r["unknowns"])
    assert r["owner_renounced"] is None, "cannot verify a renounce on closed source"


def test_fake_renounce_is_not_reported_as_renounced(monkeypatch):
    """POD: owner_address=0x0 while hidden_owner=1 and owner_change_balance=1.
    Reporting 'renounced' there is a false all-clear on the highest-risk token."""
    monkeypatch.setattr(gp, "token_security", lambda t, c: _sec(
        flags={"hidden_owner": 1, "owner_change_balance": 1, "is_mintable": 1}))
    r = gp.rug_risk("0xt", "base")
    assert r["owner_renounced"] is None
    assert any("不可信" in f for f in r["facts"])
    assert any("可增发" in f for f in r["facts"])


def test_takeback_ownership_also_invalidates_renounce(monkeypatch):
    monkeypatch.setattr(gp, "token_security",
                        lambda t, c: _sec(flags={"can_take_back_ownership": 1}))
    assert gp.rug_risk("0xt", "bsc")["owner_renounced"] is None


def test_genuine_renounce_is_reported(monkeypatch):
    monkeypatch.setattr(gp, "token_security", lambda t, c: _sec())
    r = gp.rug_risk("0xt", "bsc")
    assert r["owner_renounced"] is True
    assert r["facts"] == []


def test_unlocked_lp_is_a_fact_not_a_score(monkeypatch):
    monkeypatch.setattr(gp, "token_security", lambda t, c: _sec(
        lp={"n_holders_seen": 4, "n_locked": 0, "all_locked": False}))
    r = gp.rug_risk("0xt", "base")
    assert any("LP 未锁定" in f for f in r["facts"])
    assert "risk_score" not in r and "score" not in r   # never a fused number
    assert "≠" in r["note"]        # 'can pull' != 'will pull'


def test_no_lp_data_is_unknown_not_locked(monkeypatch):
    monkeypatch.setattr(gp, "token_security", lambda t, c: _sec(
        lp={"n_holders_seen": 0, "n_locked": 0, "all_locked": None}))
    r = gp.rug_risk("0xt", "bsc")
    assert r["lp_all_locked"] is None
    assert any("锁仓状态未知" in u for u in r["unknowns"])
    assert not any("LP 未锁定" in f for f in r["facts"])


def test_num_treats_blank_as_unknown():
    assert gp._num("") is None and gp._num(None) is None
    assert gp._num("0") == 0 and gp._num("1") == 1
