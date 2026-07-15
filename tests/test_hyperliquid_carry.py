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
        fetch=lambda url: {"code": "0", "data": [valid if "GOOD" in url else invalid]},
    )

    assert got["GOOD"] == pytest.approx(21.9)
    assert "BAD" not in got


def test_okx_funding_map_prefers_exact_then_multiplier_alias():
    valid = _row(interval_h=4)
    calls = []

    def fetch(url):
        calls.append(url)
        return ({"code": "0", "data": [valid]} if "PEPE-USDT" in url
                and "KPEPE" not in url else {"code": "0", "data": []})

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

    def funding_scan(coins, cap=45, fetch=None):
        requested.extend(coins[:cap])
        return {"rates": {"OPEN": 4.0, "ENTRY": 0.0},
                "status_by_symbol": {"OPEN": "observed", "ENTRY": "observed"},
                "summary": {"state": "ok", "requested": 2, "observed": 2,
                            "unsupported": 0, "request_failed": 0, "rate_stale": 0,
                            "rate_invalid": 0, "request_cap": 0}}

    monkeypatch.setattr(hl, "okx_funding_scan", funding_scan)
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
    def unsupported(coins, cap=45, fetch=None):
        statuses = {symbol: "unsupported" for symbol in coins}
        return {"rates": {}, "status_by_symbol": statuses,
                "summary": {"state": "ok", "requested": len(coins), "observed": 0,
                            "unsupported": len(coins), "request_failed": 0,
                            "rate_stale": 0, "rate_invalid": 0, "request_cap": 0}}

    monkeypatch.setattr(hl, "okx_funding_scan", unsupported)
    missing_symbol = hl.scan_carry(rows, priority_symbols=["ABSENT"])
    assert _open_status(missing_symbol, "ABSENT")["status"] == "hl_symbol_unavailable"

    missing_okx = hl.scan_carry(rows, priority_symbols=["PRESENT"])
    assert _open_status(missing_okx, "PRESENT")["status"] == "okx_unsupported"
    assert missing_okx["open_observations"] == []

    symbols = [f"OPEN{i:02d}" for i in range(46)]
    cap_rows = [_ctx(symbol, funding_ann=1.0, oi_usd=10) for symbol in symbols]

    def capped_rates(coins, cap=45, fetch=None):
        limited = coins[:cap]
        return {"rates": {symbol: 0.0 for symbol in limited},
                "status_by_symbol": {
                    symbol: ("observed" if symbol in limited else "request_cap")
                    for symbol in coins},
                "summary": {"state": "partial", "requested": len(coins),
                            "observed": len(limited), "unsupported": 0,
                            "request_failed": 0, "rate_stale": 0,
                            "rate_invalid": 0, "request_cap": len(coins) - len(limited)}}

    monkeypatch.setattr(hl, "okx_funding_scan", capped_rates)
    capped = hl.scan_carry(cap_rows, priority_symbols=symbols)
    assert _open_status(capped, symbols[0])["status"] == "observed"
    assert _open_status(capped, symbols[-1])["status"] == "okx_request_cap"
    assert capped["source_health"]["open_requested"] == 46
    assert capped["source_health"]["open_observed"] == 45


def test_okx_funding_scan_classifies_every_non_observation():
    valid = _row(interval_h=4)
    stale = _row(interval_h=4, age_ms=hl.OKX_FUNDING_MAX_AGE_MS + 1)
    invalid = _row(nextFundingTime="")

    def fetch(url):
        symbol = url.split("instId=")[1].split("-")[0]
        if symbol == "FAILED":
            raise OSError("offline")
        rows = {"FRESH": [valid], "STALE": [stale], "INVALID": [invalid],
                "UNSUPPORTED": []}[symbol]
        return {"code": "0", "data": rows}

    scan = hl.okx_funding_scan(
        ["FRESH", "FAILED", "STALE", "INVALID", "UNSUPPORTED", "CAPPED"],
        cap=5, fetch=fetch,
    )
    assert scan["status_by_symbol"] == {
        "FRESH": "observed", "FAILED": "request_failed", "STALE": "rate_stale",
        "INVALID": "rate_invalid", "UNSUPPORTED": "unsupported",
        "CAPPED": "request_cap",
    }
    assert scan["rates"]["FRESH"] == pytest.approx(21.9)
    assert scan["summary"]["state"] == "partial"


def test_scan_carry_never_turns_okx_failure_into_single_venue_entry(monkeypatch):
    rows = [_ctx("BTC", funding_ann=20.0)]
    seen = {}

    def failed_scan(coins, cap=45, fetch=None):
        return {"rates": {}, "status_by_symbol": {"BTC": "request_failed"},
                "summary": {"state": "unavailable", "requested": 1,
                            "observed": 0, "unsupported": 0, "request_failed": 1,
                            "rate_stale": 0, "rate_invalid": 0, "request_cap": 0}}

    def capture(signal_rows, *, okx_rates=None):
        seen["rows"] = signal_rows
        seen["rates"] = okx_rates
        return []

    monkeypatch.setattr(hl, "okx_funding_scan", failed_scan)
    monkeypatch.setattr(hl, "carry_signals", capture)

    scan = hl.scan_carry(rows)

    assert seen == {"rows": [], "rates": {}}
    assert scan["signals"] == []
    assert scan["source_health"]["state"] == "unavailable"


def test_hyperliquid_typed_fetch_marks_partial_rows():
    partial = hl.fetch_ctxs_result(fetch=lambda: [
        {"universe": [{"name": "BTC"}, {"name": "BROKEN"}]},
        [{"markPx": "100", "openInterest": "2", "funding": "0.0001",
          "dayNtlVlm": "1000", "prevDayPx": "99"}],
    ])

    assert [row["name"] for row in partial["rows"]] == ["BTC"]
    assert partial["health"]["state"] == "partial"
    assert partial["health"]["invalid_rows"] == 1


def test_hyperliquid_typed_fetch_separates_failure_from_empty_market():
    failed = hl.fetch_ctxs_result(fetch=lambda: (_ for _ in ()).throw(OSError("offline")))
    malformed = hl.fetch_ctxs_result(fetch=lambda: {"not": "a snapshot"})
    good = hl.fetch_ctxs_result(fetch=lambda: [
        {"universe": [{"name": "BTC"}]},
        [{"markPx": "100", "openInterest": "2", "funding": "0.0001",
          "dayNtlVlm": "1000", "prevDayPx": "99"}],
    ])
    assert failed["rows"] == [] and failed["health"]["error_kind"] == "request_failed"
    assert malformed["rows"] == [] and malformed["health"]["error_kind"] == "malformed_response"
    assert good["health"]["state"] == "ok" and good["health"]["rows"] == 1
    assert good["rows"][0]["name"] == "BTC"
