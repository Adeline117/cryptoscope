"""Smart-money convergence built on realized PnL. The tests pin the two disciplines
that keep it from becoming the old false positives: consistency (not one lucky win)
and independence (mules of one actor are not convergence).
"""

import src.onchain.smart_money as sm


def _summary(trades, realized):
    return {"total_count_of_trades": str(trades), "total_realized_profit_usd": str(realized)}


def _per(n_win, n_loss):
    return {"result": [{"realized_profit_usd": "100"} for _ in range(n_win)]
                      + [{"realized_profit_usd": "-50"} for _ in range(n_loss)]}


def _wire_moralis(monkeypatch, summary, per):
    import src.onchain.moralis_client as mc
    monkeypatch.setattr(mc, "usable", lambda: True)

    def get(path):
        return summary if "summary" in path else per
    monkeypatch.setattr(mc, "get", get)


def test_consistent_winner_is_skilled(monkeypatch):
    _wire_moralis(monkeypatch, _summary(40, 25_000), _per(9, 3))   # 12 tokens, 75%
    r = sm.wallet_skill("0xw", "bsc")
    assert r["skilled"] is True and r["win_rate"] == 0.75


def test_one_lucky_win_is_not_skilled(monkeypatch):
    """Huge PnL but only 3 trades on 1 token = variance, not skill."""
    _wire_moralis(monkeypatch, _summary(3, 500_000), _per(1, 0))
    assert sm.wallet_skill("0xw", "bsc")["skilled"] is False


def test_low_win_rate_is_not_skilled(monkeypatch):
    _wire_moralis(monkeypatch, _summary(50, 2_000), _per(4, 12))   # 25% win rate
    assert sm.wallet_skill("0xw", "bsc")["skilled"] is False


def test_fetch_failure_is_unknown_not_unskilled(monkeypatch):
    import src.onchain.moralis_client as mc
    monkeypatch.setattr(mc, "usable", lambda: True)
    monkeypatch.setattr(mc, "get", lambda path: None)
    r = sm.wallet_skill("0xw", "bsc")
    assert r["available"] is False and r["skilled"] is False


def test_mules_collapse_to_one_entity(monkeypatch):
    """Five wallets from one funder are ONE actor — not five converging smart wallets."""
    monkeypatch.setattr("src.onchain.funder_graph.get_funders",
                        lambda ws, c, **k: {w: "0xfunder" for w in ws})
    groups = sm._collapse_to_entities(["0xa", "0xb", "0xc"], "bsc")
    assert len(groups) == 1 and set(groups[0]) == {"0xa", "0xb", "0xc"}


def test_unresolved_funder_stays_independent(monkeypatch):
    monkeypatch.setattr("src.onchain.funder_graph.get_funders", lambda ws, c, **k: {})
    groups = sm._collapse_to_entities(["0xa", "0xb"], "bsc")
    assert len(groups) == 2, "no funder data → each wallet its own entity, never merged"


def test_convergence_needs_three_independent_skilled(monkeypatch):
    monkeypatch.setattr(sm, "_recent_buyers", lambda t, c, **k: ["0x1", "0x2", "0x3"])
    monkeypatch.setattr(sm, "_collapse_to_entities",
                        lambda ws, c: [[w] for w in ws])   # all independent
    monkeypatch.setattr(sm, "wallet_skill",
                        lambda w, c: {"skilled": True, "reason": "x", "trades": 30})
    r = sm.convergence("0xt", "bsc")
    assert r["verdict"] == "convergence" and r["skilled_entities"] == 3


def test_one_skilled_is_only_some_not_convergence(monkeypatch):
    monkeypatch.setattr(sm, "_recent_buyers", lambda t, c, **k: ["0x1", "0x2"])
    monkeypatch.setattr(sm, "_collapse_to_entities", lambda ws, c: [[w] for w in ws])
    skills = iter([{"skilled": True, "reason": "x"}, {"skilled": False}])
    monkeypatch.setattr(sm, "wallet_skill", lambda w, c: next(skills))
    assert sm.convergence("0xt", "bsc")["verdict"] == "some"


def test_no_buyers_is_unknown(monkeypatch):
    monkeypatch.setattr(sm, "_recent_buyers", lambda t, c, **k: [])
    assert sm.convergence("0xt", "bsc")["verdict"] == "unknown"


def test_bot_is_not_skilled(monkeypatch):
    """The first live run fired on a 152,284-trade MEV bot. A bot is not smart money."""
    _wire_moralis(monkeypatch, _summary(152_284, 57_000), _per(9, 3))
    r = sm.wallet_skill("0xbot", "bsc")
    assert r["skilled"] is False and r["is_bot"] is True


def test_garbage_pnl_is_rejected(monkeypatch):
    """A $2.4-octillion realized figure is token-decimal overflow, not profit."""
    _wire_moralis(monkeypatch, _summary(500, 2.4e27), _per(9, 1))
    assert sm.wallet_skill("0xw", "bsc")["skilled"] is False
