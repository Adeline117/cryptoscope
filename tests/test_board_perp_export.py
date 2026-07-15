"""Perp export wiring contracts for carry lifecycle observations."""
from __future__ import annotations


def test_render_perps_sends_open_observations_to_paper_tracker(monkeypatch):
    from src.onchain import hyperliquid
    from src.pipeline import board_export, carry_paper, cascade_radar

    rows = [{
        "name": "OPEN", "markPx": 1.0, "oi_usd": 10,
        "funding_ann": 1.0, "vol24": 1_000, "price_chg_24h": 0.0,
    }]
    signals = [{
        "symbol": "ENTRY", "cross": True, "edge_ann": 20.0,
        "partial_model_proxy_ann_pct": 10.0,
    }]
    observations = [{
        "symbol": "OPEN", "cross": True, "hl_ann": 1.0, "okx_ann": 4.0,
        "edge_ann": -3.0, "observed_at": "2026-07-15T00:00:00+00:00",
    }]
    seen = {}

    hl_health = {
        "state": "ok", "rows": 1,
        "attempted_at": "2026-07-15T00:00:00+00:00",
    }
    monkeypatch.setattr(
        hyperliquid,
        "fetch_ctxs_result",
        lambda: {"rows": rows, "health": hl_health},
    )
    monkeypatch.setattr(hyperliquid, "_store_and_diff", lambda _rows: None)
    monkeypatch.setattr(hyperliquid, "perp_signals", lambda _rows: [])
    monkeypatch.setattr(hyperliquid, "carry_scorecard", lambda: {"available": False})
    # Keeps this test diagnostic against the old wiring: the legacy implementation
    # still imports carry_signals and would pass these signals to paper_run directly.
    monkeypatch.setattr(hyperliquid, "carry_signals", lambda _rows: signals)

    def scan_carry(scan_rows, *, priority_symbols, hl_health):
        seen["scan_rows"] = scan_rows
        seen["priority_symbols"] = priority_symbols
        seen["hl_health"] = hl_health
        return {
            "signals": signals,
            "open_observations": observations,
            "open_status": [{"symbol": "OPEN", "status": "observed"}],
            "source_health": {
                "schema_version": 1,
                "state": "ok",
                "scan_at": "2026-07-15T00:00:00+00:00",
                "hl": hl_health,
                "okx": {"state": "ok", "requested": 2, "observed": 2},
                "open_requested": 1,
                "open_observed": 1,
                "entry_deferred_by_cap": 0,
            },
        }

    monkeypatch.setattr(hyperliquid, "scan_carry", scan_carry, raising=False)
    monkeypatch.setattr(carry_paper, "open_symbols", lambda: ["OPEN"], raising=False)

    def paper_run(carries, *, observations=None):
        seen["paper_carries"] = carries
        seen["paper_observations"] = observations
        return {"n_open": 1, "n_closed": 0}

    monkeypatch.setattr(carry_paper, "run", paper_run)
    monkeypatch.setattr(cascade_radar, "record_signals", lambda _signals: 0)
    monkeypatch.setattr(cascade_radar, "view", lambda: {"events": []})

    payload = board_export.render_perps()

    assert "scan_error" not in payload
    assert seen["scan_rows"] is rows
    assert seen["priority_symbols"] == ["OPEN"]
    assert seen["hl_health"] is hl_health
    assert seen["paper_carries"] is signals
    assert seen["paper_observations"] is observations
    assert payload["carry"] is signals
    assert payload["carry_paper"] == {"n_open": 1, "n_closed": 0}
    assert payload["carry_source_health"]["state"] == "ok"
    assert payload["carry_source_health"]["paper"] == {"state": "ok"}


def test_render_perps_exposes_paper_failure_without_hiding_market_data(monkeypatch):
    from src.onchain import hyperliquid
    from src.pipeline import board_export, carry_paper, cascade_radar

    rows = [{
        "name": "BTC", "markPx": 1.0, "oi_usd": 10,
        "funding_ann": 1.0, "vol24": 1_000, "price_chg_24h": 0.0,
    }]
    monkeypatch.setattr(hyperliquid, "fetch_ctxs_result", lambda: {
        "rows": rows,
        "health": {"state": "ok", "rows": 1,
                   "attempted_at": "2026-07-15T00:00:00+00:00"},
    })
    monkeypatch.setattr(hyperliquid, "_store_and_diff", lambda _rows: None)
    monkeypatch.setattr(hyperliquid, "perp_signals", lambda _rows: [])
    monkeypatch.setattr(hyperliquid, "carry_scorecard", lambda: {"available": False})
    monkeypatch.setattr(hyperliquid, "scan_carry", lambda *_args, **_kwargs: {
        "signals": [], "open_observations": [], "open_status": [],
        "source_health": {
            "schema_version": 1, "state": "ok",
            "scan_at": "2026-07-15T00:00:00+00:00",
            "hl": {"state": "ok", "rows": 1},
            "okx": {"state": "not_needed", "requested": 0, "observed": 0},
            "open_requested": 0, "open_observed": 0,
            "entry_deferred_by_cap": 0,
        },
    })
    monkeypatch.setattr(carry_paper, "open_symbols", lambda: [])
    monkeypatch.setattr(
        carry_paper,
        "run",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("db locked")),
    )
    monkeypatch.setattr(cascade_radar, "record_signals", lambda _signals: 0)
    monkeypatch.setattr(cascade_radar, "view", lambda: {"events": []})

    payload = board_export.render_perps()

    assert "scan_error" not in payload
    assert payload["carry"] == []
    assert payload["carry_source_health"]["state"] == "partial"
    assert payload["carry_source_health"]["paper"]["state"] == "error"
    assert payload["carry_source_health"]["paper"]["error_kind"] == "tracker_failed"
