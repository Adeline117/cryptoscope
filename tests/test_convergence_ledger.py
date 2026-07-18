"""The smart-money convergence offense measures its own forward edge, honestly."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from src.pipeline import convergence_ledger as cl


T0 = datetime(2026, 7, 17, 12, 0, tzinfo=timezone.utc).timestamp()
HOUR = 3600


@pytest.fixture
def conn(monkeypatch, tmp_path):
    monkeypatch.setattr(cl, "DB", tmp_path / "conv.db")
    c = cl._conn()
    yield c
    c.close()


def _prices(mapping):
    """price_fn returning a fixed price per token; None for unknown tokens."""
    return lambda token, chain: (
        {"price_usd": mapping[token], "fdv_usd": None, "liquidity_usd": None}
        if token in mapping else None
    )


def _buy(token, buyers, *, symbol="T", chain="Base", usd=5000):
    return {"token": token, "symbol": symbol, "chain": chain,
            "n_buyers": len(buyers), "buyers": buyers, "usd_bought": usd,
            "mins_ago": 3.0}


def test_records_only_three_plus_buyers_with_frozen_entry_price(conn):
    buys = [
        _buy("0xAAA", ["w1", "w2", "w3"]),   # convergence
        _buy("0xBBB", ["w1", "w2"]),          # only 2 — too cheap to trust
        {"token": "0xCCC", "chain": "Base", "n_buyers": 4, "buyers": ["a", "b", "c", "d"]},
    ]
    got = cl.record(buys, price_fn=_prices({"0xAAA": 1.0, "0xCCC": 2.0}),
                    now=T0, conn=conn)
    assert got["inserted"] == 2 and got["skipped_small"] == 1

    rows = dict(conn.execute("SELECT token, entry_price_usd FROM events").fetchall())
    assert rows == {"0xAAA": 1.0, "0xCCC": 2.0}
    # Three horizons scheduled per event, all still unresolved.
    assert conn.execute("SELECT COUNT(*) FROM outcomes").fetchone()[0] == 6


def test_same_token_same_day_is_frozen_once(conn):
    buy = [_buy("0xAAA", ["w1", "w2", "w3"])]
    cl.record(buy, price_fn=_prices({"0xAAA": 1.0}), now=T0, conn=conn)
    # A later poll the same day sees the wave again — must not double-record or
    # re-price (the entry stays frozen at first detection).
    again = cl.record(buy, price_fn=_prices({"0xAAA": 9.9}), now=T0 + 900, conn=conn)
    assert again["inserted"] == 0 and again["skipped_existing"] == 1
    assert conn.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 1
    assert conn.execute("SELECT entry_price_usd FROM events").fetchone()[0] == 1.0


def test_unknown_chain_or_missing_price_fails_soft(conn):
    buys = [
        _buy("0xAAA", ["w1", "w2", "w3"], chain="Fantom"),   # unmapped chain
        _buy("0xBBB", ["w1", "w2", "w3"]),                    # priced None
    ]
    got = cl.record(buys, price_fn=_prices({}), now=T0, conn=conn)
    assert got["skipped_chain"] == 1 and got["inserted"] == 1
    token, price, src = conn.execute(
        "SELECT token, entry_price_usd, price_source FROM events").fetchone()
    # An event with no entry price is still logged (so we see the miss), but its
    # forward return can never resolve — it just won't count in the summary.
    assert token == "0xBBB" and price is None and src == "unavailable"


def test_co_occurrence_flags_a_repeating_wallet_farm(conn):
    farm = ["f1", "f2", "f3", "f4"]
    # The same farm converges on three different tokens across days.
    for i, token in enumerate(("0xF1", "0xF2", "0xF3")):
        cl.record([_buy(token, farm)], price_fn=_prices({token: 1.0}),
                  now=T0 + i * 86_400, conn=conn)
    # A genuinely independent set on a fresh token scores low.
    cl.record([_buy("0xIND", ["z1", "z2", "z3"])], price_fn=_prices({"0xIND": 1.0}),
              now=T0 + 4 * 86_400, conn=conn)

    scores = dict(conn.execute("SELECT token, co_occurrence FROM events").fetchall())
    assert scores["0xF2"] >= cl.FARM_LIKE_THRESHOLD   # identical farm set → high overlap
    assert scores["0xIND"] < cl.FARM_LIKE_THRESHOLD   # disjoint wallets → low


def test_resolve_computes_forward_return_only_when_due(conn):
    cl.record([_buy("0xAAA", ["w1", "w2", "w3"])],
              price_fn=_prices({"0xAAA": 1.0}), now=T0, conn=conn)

    # Before 1h nothing is due.
    assert cl.resolve_due(price_fn=_prices({"0xAAA": 1.5}),
                          now=T0 + 1800, conn=conn)["resolved"] == 0

    # At +1h the token doubled from entry 1.0 → +50%.
    got = cl.resolve_due(price_fn=_prices({"0xAAA": 1.5}), now=T0 + HOUR, conn=conn)
    assert got["resolved"] == 1 and got["priced"] == 1
    ret = conn.execute(
        "SELECT return_pct FROM outcomes WHERE horizon='1h'").fetchone()[0]
    assert ret == pytest.approx(50.0)
    # Idempotent: a second pass does not re-resolve a filled horizon.
    assert cl.resolve_due(price_fn=_prices({"0xAAA": 9.0}),
                          now=T0 + HOUR, conn=conn)["resolved"] == 0


def test_summary_splits_diverse_from_farm_like(conn):
    farm = ["f1", "f2", "f3"]
    # co_occurrence is retrospective: the farm's FIRST appearance has nothing prior
    # to match, so it scores 0. Seed one prior farm event (left unpriced, so it is
    # excluded from the measured summary) so the later farm events are flagged.
    cl.record([_buy("0xF0", farm)], price_fn=_prices({}), now=T0, conn=conn)
    for i, token in enumerate(("0xF1", "0xF2"), start=1):   # farm-like, both dump
        cl.record([_buy(token, farm)], price_fn=_prices({token: 1.0}),
                  now=T0 + i * 86_400, conn=conn)
    cl.record([_buy("0xIND", ["z1", "z2", "z3"])],           # diverse, pumps
              price_fn=_prices({"0xIND": 1.0}), now=T0 + 5 * 86_400, conn=conn)

    at = T0 + 5 * 86_400 + HOUR
    cl.resolve_due(price_fn=_prices({"0xF1": 0.5, "0xF2": 0.6, "0xIND": 2.0}),
                   now=at, conn=conn)

    s = cl.summary(conn=conn)
    assert s["n_events"] == 4          # F0 counted as an event, just never priced
    h = s["horizons"]["1h"]
    assert h["diverse"]["n"] == 1 and h["diverse"]["hit_rate"] == 1.0    # only 0xIND
    assert h["farm_like"]["n"] == 2 and h["farm_like"]["hit_rate"] == 0.0
    assert h["all"]["n"] == 3          # F0 unpriced → not in any measured bucket
    # 7d horizon not due yet → no resolved returns.
    assert s["horizons"]["7d"]["all"]["n"] == 0
