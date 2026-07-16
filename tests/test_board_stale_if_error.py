"""Total lane-export failures must retain the last publicly verifiable view."""
from __future__ import annotations

import pytest


def _files(path):
    return {item.name: item.read_bytes() for item in path.glob("*.json")}


@pytest.mark.asyncio
async def test_structure_render_failure_preserves_last_good_view(tmp_path, monkeypatch):
    from src.pipeline import board_export, scheduler, structure_radar

    monkeypatch.setattr(board_export, "EXPORT_DIR", tmp_path)
    old = board_export._envelope({
        "events": [], "product_metadata_at": None,
        "product_metadata_time_semantics": (
            "current_inventory_metadata_not_event_time_evidence"
        ),
    }, view="structure")
    board_export.write_views(structure=old)
    before = _files(tmp_path)
    monkeypatch.setattr(structure_radar, "scan", lambda: {
        "scanned": 1, "inserted": 0, "events": [],
    })
    monkeypatch.setattr(
        structure_radar,
        "view",
        lambda: (_ for _ in ()).throw(RuntimeError("structure ledger unavailable")),
    )
    pushed = []
    monkeypatch.setattr(
        board_export,
        "push_to_blob",
        lambda paths: pushed.extend(paths) or len(paths),
    )

    await scheduler._run_structure_radar()

    assert _files(tmp_path) == before
    assert pushed == []


@pytest.mark.asyncio
async def test_airdrop_render_failure_preserves_last_good_view(tmp_path, monkeypatch):
    from src.pipeline import airdrop_radar, board_export, scheduler

    monkeypatch.setattr(board_export, "EXPORT_DIR", tmp_path)
    old = board_export._envelope({"events": []}, view="airdrop")
    board_export.write_views(airdrop=old)
    before = _files(tmp_path)
    monkeypatch.setattr(
        board_export,
        "render_structure",
        lambda: board_export._envelope({
            "events": [], "product_metadata_at": None,
            "product_metadata_time_semantics": (
                "current_inventory_metadata_not_event_time_evidence"
            ),
        }, view="structure"),
    )
    monkeypatch.setattr(board_export, "render_stats", lambda _opportunities: None)
    monkeypatch.setattr(
        airdrop_radar,
        "sync",
        lambda: (_ for _ in ()).throw(RuntimeError("airdrop ledger unavailable")),
    )
    pushed = []
    monkeypatch.setattr(
        board_export,
        "push_to_blob",
        lambda paths: pushed.extend(paths) or len(paths),
    )

    await scheduler._run_board_export()

    assert _files(tmp_path) == before
    assert pushed == []


@pytest.mark.asyncio
async def test_perps_render_failure_preserves_last_good_view(tmp_path, monkeypatch):
    from src.onchain import hyperliquid
    from src.pipeline import board_export, scheduler

    monkeypatch.setattr(board_export, "EXPORT_DIR", tmp_path)
    old = board_export._envelope(
        {"perps": [{"symbol": "OLD"}], "carry": [], "cascade_events": []},
        view="perps",
    )
    board_export.write_views(perps=old)
    before = _files(tmp_path)
    monkeypatch.setattr(
        hyperliquid,
        "fetch_ctxs_result",
        lambda: (_ for _ in ()).throw(RuntimeError("Hyperliquid unavailable")),
    )
    pushed = []
    monkeypatch.setattr(
        board_export,
        "push_to_blob",
        lambda paths: pushed.extend(paths) or len(paths),
    )

    await scheduler._run_perps_export()

    assert _files(tmp_path) == before
    assert pushed == []
