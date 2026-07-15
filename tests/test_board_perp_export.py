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
        "symbol": "ENTRY", "cross": True, "edge_ann": 20.0, "net_ann": 10.0,
    }]
    observations = [{
        "symbol": "OPEN", "cross": True, "hl_ann": 1.0, "okx_ann": 4.0,
        "edge_ann": -3.0, "observed_at": "2026-07-15T00:00:00+00:00",
    }]
    seen = {}

    monkeypatch.setattr(hyperliquid, "_fetch_ctxs", lambda: rows)
    monkeypatch.setattr(hyperliquid, "_store_and_diff", lambda _rows: None)
    monkeypatch.setattr(hyperliquid, "perp_signals", lambda _rows: [])
    monkeypatch.setattr(hyperliquid, "carry_scorecard", lambda: {"available": False})
    # Keeps this test diagnostic against the old wiring: the legacy implementation
    # still imports carry_signals and would pass these signals to paper_run directly.
    monkeypatch.setattr(hyperliquid, "carry_signals", lambda _rows: signals)

    def scan_carry(scan_rows, *, priority_symbols):
        seen["scan_rows"] = scan_rows
        seen["priority_symbols"] = priority_symbols
        return {
            "signals": signals,
            "open_observations": observations,
            "open_status": [{"symbol": "OPEN", "status": "observed"}],
            "source_health": {
                "scan_at": "2026-07-15T00:00:00+00:00",
                "open_requested": 1,
                "open_observed": 1,
                "okx_requested": 2,
                "okx_observed": 2,
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
    assert seen["paper_carries"] is signals
    assert seen["paper_observations"] is observations
    assert payload["carry"] is signals
    assert payload["carry_paper"] == {"n_open": 1, "n_closed": 0}
