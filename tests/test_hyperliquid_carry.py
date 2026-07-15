from datetime import datetime, timezone

import pytest

from src.onchain import hyperliquid as hl


def _row(*, interval_h=8, age_ms=0, **overrides):
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    row = {
        "fundingRate": "0.0001",
        "fundingTime": str(now_ms + 60_000),
        "nextFundingTime": str(now_ms + 60_000 + interval_h * 3_600_000),
        "ts": str(now_ms - age_ms),
    }
    row.update(overrides)
    return row


def test_okx_funding_annualizes_actual_contract_interval():
    now_ms = 1_000_000_000
    eight_hour = {"fundingRate": "0.0001", "fundingTime": str(now_ms),
                  "nextFundingTime": str(now_ms + 8 * 3_600_000), "ts": str(now_ms)}
    four_hour = {**eight_hour, "nextFundingTime": str(now_ms + 4 * 3_600_000)}

    assert hl._okx_funding_ann(eight_hour, now_ms=now_ms) == pytest.approx(10.95)
    assert hl._okx_funding_ann(four_hour, now_ms=now_ms) == pytest.approx(21.9)


@pytest.mark.parametrize(
    "row",
    [
        _row(nextFundingTime=""),
        _row(interval_h=0),
        _row(interval_h=25),
        _row(age_ms=hl.OKX_FUNDING_MAX_AGE_MS + 1),
        _row(fundingRate="nan"),
        _row(fundingRate="inf"),
    ],
)
def test_okx_funding_rejects_missing_invalid_or_stale_period(row):
    assert hl._okx_funding_ann(row) is None


def test_okx_funding_map_omits_unverifiable_interval():
    valid = _row(interval_h=4)
    invalid = _row(nextFundingTime="")

    got = hl.okx_funding_map(
        ["GOOD", "BAD"],
        fetch=lambda url: {"data": [valid if "GOOD" in url else invalid]},
    )

    assert got["GOOD"] == pytest.approx(21.9)
    assert "BAD" not in got


def test_okx_funding_map_prefers_exact_then_multiplier_alias():
    valid = _row(interval_h=4)
    calls = []

    def fetch(url):
        calls.append(url)
        return {"data": [valid]} if "PEPE-USDT" in url and "KPEPE" not in url else {"data": []}

    got = hl.okx_funding_map(["kPEPE"], fetch=fetch)
    assert got["kPEPE"] == pytest.approx(21.9)
    assert "KPEPE-USDT-SWAP" in calls[0]
    assert "PEPE-USDT-SWAP" in calls[1]


def _ctx(name: str, *, funding_ann: float, oi_usd: float = 2_000_000) -> dict:
    return {
        "name": name,
        "markPx": 1.0,
        "oi_usd": oi_usd,
        "funding_ann": funding_ann,
        "vol24": 1_000_000,
        "price_chg_24h": 0.0,
    }


def _open_status(scan: dict, symbol: str) -> dict:
    return next(row for row in scan["open_status"] if row["symbol"] == symbol)


def _stub_carry_history(monkeypatch, history=None) -> None:
    monkeypatch.setattr(hl, "_funding_persistence", lambda: history or {})
    monkeypatch.setattr(hl, "_hl_spot_tokens", lambda: set())
    monkeypatch.setattr(hl, "xdiff_stats", lambda: {})
    monkeypatch.setattr(hl, "_store_xdiff", lambda _diffs: None)


def test_scan_carry_prioritizes_open_symbol_and_uses_current_pair(monkeypatch):
    """An open episode must remain observable after it falls below every entry gate."""
    rows = [
        _ctx("OPEN", funding_ann=1.0, oi_usd=10),
        _ctx("ENTRY", funding_ann=20.0),
    ]
    requested = []

    def funding_map(coins, cap=45, fetch=None):
        requested.extend(coins[:cap])
        return {"OPEN": 4.0, "ENTRY": 0.0}

    monkeypatch.setattr(hl, "okx_funding_map", funding_map)
    # If the observation accidentally uses persistence instead of the current HL row,
    # OPEN's edge would be +96 rather than the correct current -3.
    _stub_carry_history(monkeypatch, {
        "OPEN": {"mean_ann": 100.0, "pos_frac": 1.0, "n": 10},
    })

    scan = hl.scan_carry(rows, priority_symbols=["OPEN"])

    assert requested[:2] == ["OPEN", "ENTRY"]
    assert [row["symbol"] for row in scan["signals"]] == ["ENTRY"]
    assert len(scan["open_observations"]) == 1
    observation = scan["open_observations"][0]
    assert observation["symbol"] == "OPEN" and observation["cross"] is True
    assert observation["hl_ann"] == 1.0
    assert observation["okx_ann"] == 4.0
    assert observation["edge_ann"] == pytest.approx(-3.0)
    assert observation["observed_at"]
    assert _open_status(scan, "OPEN")["status"] == "observed"


def test_carry_signals_explicit_empty_okx_rates_never_refetches(monkeypatch):
    _stub_carry_history(monkeypatch)
    monkeypatch.setattr(
        hl,
        "okx_funding_map",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("explicit OKX result triggered a second request")
        ),
    )

    assert hl.carry_signals(
        [_ctx("ALT", funding_ann=20.0)], okx_rates={}
    ) == []


def test_scan_carry_reports_source_symbol_okx_and_cap_states(monkeypatch):
    monkeypatch.setattr(
        hl, "carry_signals", lambda _rows, *, okx_rates=None: []
    )

    no_hl = hl.scan_carry([], priority_symbols=["NO_HL_SOURCE"])
    assert _open_status(no_hl, "NO_HL_SOURCE")["status"] == "hl_source_unavailable"
    assert no_hl["open_observations"] == []

    rows = [_ctx("PRESENT", funding_ann=1.0, oi_usd=10)]
    monkeypatch.setattr(hl, "okx_funding_map", lambda *_args, **_kwargs: {})
    missing_symbol = hl.scan_carry(rows, priority_symbols=["ABSENT"])
    assert _open_status(missing_symbol, "ABSENT")["status"] == "hl_symbol_unavailable"

    missing_okx = hl.scan_carry(rows, priority_symbols=["PRESENT"])
    assert _open_status(missing_okx, "PRESENT")["status"] == "okx_rate_unavailable"
    assert missing_okx["open_observations"] == []

    symbols = [f"OPEN{i:02d}" for i in range(46)]
    cap_rows = [_ctx(symbol, funding_ann=1.0, oi_usd=10) for symbol in symbols]

    def capped_rates(coins, cap=45, fetch=None):
        return {symbol: 0.0 for symbol in coins[:cap]}

    monkeypatch.setattr(hl, "okx_funding_map", capped_rates)
    capped = hl.scan_carry(cap_rows, priority_symbols=symbols)
    assert _open_status(capped, symbols[0])["status"] == "observed"
    assert _open_status(capped, symbols[-1])["status"] == "okx_request_cap"
    assert capped["source_health"]["open_requested"] == 46
    assert capped["source_health"]["open_observed"] == 45
