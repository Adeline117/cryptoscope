import sqlite3
from datetime import datetime, timedelta, timezone
from threading import Event, Lock
from time import monotonic, sleep

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


def _bulk_row(symbol: str, **overrides):
    row = _row(**overrides)
    row.update({"instId": f"{symbol}-USDT-SWAP", "instType": "SWAP"})
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
    valid = _bulk_row("GOOD", interval_h=4)
    invalid = _bulk_row("BAD", nextFundingTime="")

    got = hl.okx_funding_map(
        ["GOOD", "BAD"],
        fetch=lambda _url: {"code": "0", "data": [valid, invalid]},
    )

    assert got["GOOD"] == pytest.approx(21.9)
    assert "BAD" not in got


def test_okx_funding_map_prefers_exact_then_multiplier_alias():
    calls = []

    def fetch(url):
        calls.append(url)
        return {"code": "0", "data": [_bulk_row("PEPE", interval_h=4)]}

    got = hl.okx_funding_map(["kPEPE"], fetch=fetch)
    assert got["kPEPE"] == pytest.approx(21.9)
    assert calls == [hl.OKX_FUNDING_BULK_URL]


def test_okx_uppercase_k_ticker_never_drops_identity_prefix():
    scan = hl.okx_funding_scan(
        ["KAS"],
        fetch=lambda _url: {"code": "0", "data": [
            _bulk_row("AS"), _bulk_row("ETH"),
        ]},
    )

    assert hl.okx_symbol_candidates("KAS") == ("KAS",)
    assert scan["rates"] == {}
    assert scan["status_by_symbol"] == {"KAS": "unsupported"}


def test_okx_exact_stale_contract_never_falls_through_to_fresh_alias():
    scan = hl.okx_funding_scan(
        ["kPEPE"],
        fetch=lambda _url: {"code": "0", "data": [
            _bulk_row(
                "KPEPE", age_ms=hl.OKX_FUNDING_MAX_AGE_MS + 1,
            ),
            _bulk_row("PEPE"),
        ]},
    )

    assert scan["rates"] == {}
    assert scan["status_by_symbol"] == {"kPEPE": "rate_stale"}


def test_okx_funding_map_uses_only_verified_migration_aliases():
    calls = []

    def fetch(url):
        calls.append(url)
        return {"code": "0", "data": [_bulk_row("POL", interval_h=4)]}

    got = hl.okx_funding_map(["MATIC"], fetch=fetch)

    assert got["MATIC"] == pytest.approx(21.9)
    assert calls == [hl.OKX_FUNDING_BULK_URL]


def test_okx_nonzero_error_other_than_missing_instrument_fails_closed():
    scan = hl.okx_funding_scan(
        ["BTC"], fetch=lambda _url: {"code": "50011", "data": [],
                                    "msg": "rate limit"},
    )

    assert scan["rates"] == {}
    assert scan["status_by_symbol"] == {"BTC": "request_failed"}
    assert scan["summary"]["state"] == "unavailable"


@pytest.mark.parametrize("data", [[], None])
def test_okx_bulk_empty_or_malformed_snapshot_never_claims_unsupported(data):
    scan = hl.okx_funding_scan(
        ["BTC", "MISSING"],
        fetch=lambda _url: {"code": "0", "data": data},
    )

    assert scan["rates"] == {}
    assert scan["status_by_symbol"] == {
        "BTC": "request_failed", "MISSING": "request_failed",
    }
    assert scan["summary"]["state"] == "unavailable"
    assert scan["summary"]["source_error_kind"] == "request_failed"


def test_okx_bulk_invalid_identity_or_duplicate_is_not_unsupported():
    malformed = hl.okx_funding_scan(
        ["MISSING"],
        fetch=lambda _url: {"code": "0", "data": [
            _bulk_row("ETH"),
            {**_bulk_row("SPACE"), "instId": " SPACE-USDT-SWAP "},
            {**_bulk_row("BADTYPE"), "instType": "OPTION"},
        ]},
    )
    duplicate = hl.okx_funding_scan(
        ["BTC"],
        fetch=lambda _url: {"code": "0", "data": [
            _bulk_row("BTC"), _bulk_row("BTC"), _bulk_row("ETH"),
        ]},
    )

    assert malformed["status_by_symbol"] == {"MISSING": "rate_invalid"}
    assert malformed["summary"]["bulk_invalid_rows"] == 2
    assert malformed["summary"]["state"] == "unavailable"
    assert duplicate["status_by_symbol"] == {"BTC": "rate_invalid"}
    assert duplicate["summary"]["bulk_invalid_rows"] == 1
    assert duplicate["summary"]["state"] == "unavailable"


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
    assert observation["paired_funding_diff_ann_pct"] == pytest.approx(-3.0)
    assert {"edge_ann", "score_edge_ann", "observed_edge_ann"}.isdisjoint(
        observation
    )
    assert observation["observed_at"]
    assert observation["observation_version"] == 1
    assert [row["symbol"] for row in scan["entry_observations"]] == ["ENTRY"]
    assert scan["entry_observations"][0][
        "current_partial_model_proxy_ann_pct"] == pytest.approx(
            hl._carry_partial_model_proxy_ann(20.0))
    assert [row["symbol"] for row in scan["paper_observations"]] == ["OPEN", "ENTRY"]
    assert scan["source_health"]["entry_observed"] == 1
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


def test_carry_proxy_uses_fixed_hold_assumption_not_sparse_coverage(monkeypatch):
    monkeypatch.setattr(hl, "_funding_persistence", lambda: {
        "BTC": {"mean_ann": 30.0, "pos_frac": 1.0, "n": 10},
    })
    monkeypatch.setattr(hl, "_hl_spot_tokens", lambda: set())
    monkeypatch.setattr(hl, "_store_xdiff", lambda _diffs: None)
    coverage = {
        "BTC": {"positive_fraction": 5 / 6, "mean_ann": 20.0,
                "point_count": 6, "coverage_span_h": 48.0},
    }
    monkeypatch.setattr(hl, "xdiff_stats", lambda: coverage)

    first = hl.carry_signals(
        [_ctx("BTC", funding_ann=30.0)], okx_rates={"BTC": 0.0},
    )[0]
    coverage["BTC"] = {**coverage["BTC"], "coverage_span_h": 720.0}
    second = hl.carry_signals(
        [_ctx("BTC", funding_ann=30.0)], okx_rates={"BTC": 0.0},
    )[0]

    expected = hl._carry_partial_model_proxy_ann(30.0)
    assert first["gross_funding_diff_ann_pct"] == pytest.approx(30.0)
    assert first["current_paired_funding_diff_ann_pct"] == pytest.approx(30.0)
    assert first["ranking_metric"] == "gross_funding_diff_ann_pct"
    assert {
        "cross_diff", "edge_ann", "score_edge_ann", "observed_edge_ann",
    }.isdisjoint(first)
    assert first["partial_model_proxy_ann_pct"] == pytest.approx(expected, abs=0.1)
    assert second["partial_model_proxy_ann_pct"] == first["partial_model_proxy_ann_pct"]
    assert first["model_hold_days_assumption"] == 14
    assert first["hold_period_verified"] is False
    assert first["coverage_span_h"] == 48.0
    assert first["coverage_point_count"] == 6
    assert first["coverage_positive_fraction"] == pytest.approx(0.83)
    assert first["all_in_net_ann_pct"] is None
    assert first["cost_completeness"] == "partial"
    assert first["is_realized"] is False
    assert first["candidate_scope"] == "cross_venue_two_perp"
    assert first["paper_measurement_eligible"] is True
    assert "net_ann" not in first
    assert "hold_days" not in first and "hold_measured" not in first


def test_single_venue_candidate_is_separate_from_cross_venue_paper_scope(monkeypatch):
    monkeypatch.setattr(hl, "_funding_persistence", lambda: {})
    monkeypatch.setattr(hl, "_hl_spot_tokens", lambda: set())
    monkeypatch.setattr(hl, "xdiff_stats", lambda: {})
    monkeypatch.setattr(hl, "_store_xdiff", lambda _diffs: None)

    candidate = hl.carry_signals(
        [_ctx("BTC", funding_ann=30.0)], okx_rates={},
    )[0]

    assert candidate["cross"] is False
    assert candidate["gross_funding_diff_ann_pct"] == pytest.approx(30.0)
    assert candidate["current_paired_funding_diff_ann_pct"] is None
    assert candidate["candidate_scope"] == "single_venue_spot_perp"
    assert candidate["paper_measurement_eligible"] is False


def test_scan_carry_reports_source_symbol_okx_and_cap_states(monkeypatch):
    monkeypatch.setattr(hl, "OKX_FUNDING_REQUEST_CAP", 45)
    monkeypatch.setattr(
        hl, "carry_signals", lambda _rows, *, okx_rates=None: []
    )

    no_hl = hl.scan_carry([], priority_symbols=["NO_HL_SOURCE"])
    assert _open_status(no_hl, "NO_HL_SOURCE")["status"] == "hl_source_unavailable"
    assert no_hl["open_observations"] == []
    assert no_hl["entry_observations"] == []
    assert no_hl["paper_observations"] == []

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


def test_scan_carry_cap_prioritizes_open_then_current_right_tail(monkeypatch):
    """HL universe order must not push a late high-funding pair behind the OKX cap."""
    monkeypatch.setattr(hl, "OKX_FUNDING_REQUEST_CAP", 45)
    lows = [_ctx(f"LOW{i:02d}", funding_ann=10.0, oi_usd=2_000_000 + i)
            for i in range(50)]
    rows = [
        _ctx("OPEN", funding_ann=1.0, oi_usd=10),
        *lows,
        _ctx("TAIL_LOW_OI", funding_ann=40.0, oi_usd=2_000_000),
        _ctx("TAIL_HIGH_OI", funding_ann=40.0, oi_usd=3_000_000),
        _ctx("HYPE", funding_ann=80.0, oi_usd=100_000_000),
    ]
    requested = []

    def capped_rates(coins, cap=45, fetch=None):
        requested.extend(coins)
        limited = coins[:cap]
        return {
            "rates": {symbol: 0.0 for symbol in limited},
            "status_by_symbol": {
                symbol: ("observed" if symbol in limited else "request_cap")
                for symbol in coins
            },
            "summary": {
                "state": "partial", "requested": len(coins),
                "observed": len(limited), "unsupported": 0,
                "request_failed": 0, "request_timeout": 0,
                "rate_stale": 0, "rate_invalid": 0,
                "request_cap": len(coins) - len(limited),
            },
        }

    monkeypatch.setattr(hl, "okx_funding_scan", capped_rates)
    monkeypatch.setattr(hl, "carry_signals", lambda *_args, **_kwargs: [])

    scan = hl.scan_carry(rows, priority_symbols=["OPEN"])

    assert requested[:4] == ["OPEN", "HYPE", "TAIL_HIGH_OI", "TAIL_LOW_OI"]
    assert "HYPE" in requested[:hl.OKX_FUNDING_REQUEST_CAP]
    assert requested.index("HYPE") < requested.index("LOW00")
    assert _open_status(scan, "OPEN")["status"] == "observed"
    assert scan["source_health"]["entry_deferred_by_cap"] == 9
    assert scan["source_health"]["entry_priority_method"] == \
        hl.CARRY_ENTRY_PRIORITY_METHOD


def test_okx_funding_scan_classifies_every_non_observation():
    rows = [
        _bulk_row("FRESH", interval_h=4),
        _bulk_row(
            "STALE", interval_h=4,
            age_ms=hl.OKX_FUNDING_MAX_AGE_MS + 1,
        ),
        _bulk_row("INVALID", nextFundingTime=""),
    ]

    scan = hl.okx_funding_scan(
        ["FRESH", "STALE", "INVALID", "UNSUPPORTED", "CAPPED"],
        cap=4, fetch=lambda _url: {"code": "0", "data": rows},
    )
    assert scan["status_by_symbol"] == {
        "FRESH": "observed", "STALE": "rate_stale",
        "INVALID": "rate_invalid", "UNSUPPORTED": "unsupported",
        "CAPPED": "request_cap",
    }
    assert scan["rates"]["FRESH"] == pytest.approx(21.9)
    assert scan["summary"]["state"] == "partial"


def test_okx_bulk_scan_removes_legacy_45_cap_with_one_order_stable_request():
    symbols = [f"S{i:02d}" for i in range(80)]
    lock = Lock()
    active = 0
    peak = 0
    calls = []

    def fetch(url):
        nonlocal active, peak
        calls.append(url)
        with lock:
            active += 1
            peak = max(peak, active)
        try:
            sleep(0.02)
            return {"code": "0", "data": [
                _bulk_row(
                    symbol,
                    fundingRate=str(0.0001 + index / 1_000_000),
                )
                for index, symbol in reversed(list(enumerate(symbols)))
            ]}
        finally:
            with lock:
                active -= 1

    scan = hl.okx_funding_scan(
        symbols, fetch=fetch, max_workers=8, scan_timeout_s=2,
    )

    assert peak == 1
    assert calls == [hl.OKX_FUNDING_BULK_URL]
    assert list(scan["status_by_symbol"]) == symbols
    assert list(scan["rates"]) == symbols
    assert all(status == "observed" for status in scan["status_by_symbol"].values())
    assert scan["rates"]["S00"] < scan["rates"]["S79"]
    assert scan["summary"]["max_workers"] == 1
    assert scan["summary"]["upstream_requests"] == 1
    assert scan["summary"]["transport_mode"] == "bulk_any"
    assert scan["summary"]["bulk_usdt_swap_rows"] == len(symbols)
    assert scan["summary"]["request_cap"] == 0
    assert scan["summary"]["request_timeout"] == 0


def test_okx_bulk_scan_closes_every_symbol_without_cap_or_silent_gap():
    symbols = [f"C{i:02d}" for i in range(77)]
    supported = symbols[:67]
    scan = hl.okx_funding_scan(
        symbols,
        fetch=lambda _url: {"code": "0", "data": [
            _bulk_row(symbol) for symbol in supported
        ]},
    )

    assert len(scan["status_by_symbol"]) == len(symbols)
    assert scan["summary"]["requested"] == 77
    assert scan["summary"]["observed"] == 67
    assert scan["summary"]["unsupported"] == 10
    assert scan["summary"]["request_cap"] == 0
    assert scan["summary"]["upstream_requests"] == 1
    assert scan["summary"]["state"] == "ok"


def test_okx_funding_scan_whole_round_timeout_is_fail_closed():
    gate = Event()
    finished = Event()
    symbols = ["WAIT0", "WAIT1", "WAIT2", "WAIT3"]

    def fetch(_url):
        try:
            gate.wait(timeout=2)
            return {"code": "0", "data": [_bulk_row("WAIT0")]}
        finally:
            finished.set()

    started = monotonic()
    try:
        scan = hl.okx_funding_scan(
            symbols, fetch=fetch, max_workers=2, scan_timeout_s=0.05,
        )
    finally:
        gate.set()
        assert finished.wait(timeout=0.5)
    elapsed = monotonic() - started

    assert elapsed < 0.5
    assert scan["rates"] == {}
    assert scan["status_by_symbol"] == {
        symbol: "request_timeout" for symbol in symbols
    }
    assert scan["summary"]["request_timeout"] == len(symbols)
    assert scan["summary"]["state"] == "unavailable"
    assert scan["summary"]["duration_ms"] < 500


def test_okx_bulk_timeout_is_single_flight_across_scheduler_ticks():
    gate = Event()
    entered = Event()
    finished = Event()
    lock = Lock()
    calls = active = peak = 0

    def fetch(_url):
        nonlocal calls, active, peak
        with lock:
            calls += 1
            active += 1
            peak = max(peak, active)
        entered.set()
        try:
            gate.wait(timeout=2)
            return {"code": "0", "data": [_bulk_row("WAIT")]}
        finally:
            with lock:
                active -= 1
            finished.set()

    try:
        first = hl.okx_funding_scan(
            ["WAIT"], fetch=fetch, scan_timeout_s=0.02,
        )
        assert entered.wait(timeout=0.5)
        second = hl.okx_funding_scan(
            ["WAIT"], fetch=fetch, scan_timeout_s=0.02,
        )
    finally:
        gate.set()
        assert finished.wait(timeout=0.5)

    assert calls == 1 and peak == 1 and active == 0
    assert first["status_by_symbol"] == {"WAIT": "request_timeout"}
    assert second["status_by_symbol"] == {"WAIT": "request_timeout"}
    assert first["summary"]["upstream_requests"] == 1
    assert second["summary"]["upstream_requests"] == 0
    assert "still in flight" in second["summary"]["source_error"]


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


def test_carry_scorecard_is_explicitly_a_quote_proxy_not_realized_pnl(
        monkeypatch, tmp_path):
    db = tmp_path / "scorecard.db"
    c = sqlite3.connect(db)
    c.execute("CREATE TABLE snaps(coin TEXT, ts TEXT, funding_ann REAL)")
    now = datetime.now(timezone.utc)
    c.executemany(
        "INSERT INTO snaps VALUES (?,?,?)",
        [("BTC", (now - timedelta(hours=30 - i * 1.5)).isoformat(), 10 + i / 10)
         for i in range(20)],
    )
    c.commit()
    c.close()
    monkeypatch.setattr(hl, "_conn", lambda: sqlite3.connect(db))

    scorecard = hl.carry_scorecard()

    assert scorecard["available"] is True
    assert scorecard["measure_kind"] == "hl_funding_quote_snapshot_proxy"
    assert scorecard["is_realized_pnl"] is False
    assert scorecard["includes_okx_leg"] is False
    assert scorecard["includes_funding_settlements"] is False
    assert scorecard["includes_basis_pnl"] is False
    assert scorecard["includes_costs"] is False
    assert scorecard["quoted_hl_funding_rate_proxy_ann"] == pytest.approx(10.9)
    assert "realized_ann" not in scorecard
