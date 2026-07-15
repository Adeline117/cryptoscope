"""Each board view advertises its real scheduler cadence and stale deadline."""
from __future__ import annotations

from datetime import datetime

import pytest


def test_fast_and_slow_views_have_independent_freshness_policies():
    from src.pipeline.board_export import _envelope

    launch = _envelope({}, view="launch")
    operators = _envelope({}, view="operators")
    perps = _envelope({}, view="perps")
    for payload, cadence, grace in ((launch, 3, 3), (perps, 5, 5),
                                    (operators, 60, 30)):
        generated = datetime.fromisoformat(payload["generated_at"])
        expected = datetime.fromisoformat(payload["next_expected_at"])
        stale = datetime.fromisoformat(payload["stale_after_at"])
        assert payload["refresh_cadence_min"] == cadence
        assert payload["freshness_grace_min"] == grace
        assert (expected - generated).total_seconds() == cadence * 60
        assert (stale - expected).total_seconds() == grace * 60


def test_unknown_view_cannot_inherit_a_generous_default_sla():
    from src.pipeline.board_export import _envelope

    with pytest.raises(ValueError, match="unknown board view"):
        _envelope({}, view="new_lane_without_policy")


def test_regular_export_can_skip_both_slow_scanners(monkeypatch):
    from src.pipeline import board_export

    for name in ("render_perps", "render_launch", "render_structure", "render_airdrop"):
        monkeypatch.setattr(board_export, name, lambda: {})
    monkeypatch.setattr(
        board_export, "render_operators",
        lambda: (_ for _ in ()).throw(AssertionError("operator scanner was called")),
    )
    monkeypatch.setattr(
        board_export, "render_opportunities",
        lambda: (_ for _ in ()).throw(AssertionError("opportunity scanner was called")),
    )
    monkeypatch.setattr(board_export, "render_stats",
                        lambda opportunities: {"saw": opportunities})
    monkeypatch.setattr(board_export, "write_views", lambda **views: [])

    result = board_export.run(push=False, include_operators=False,
                              include_opportunities=False)
    assert result["views_written"] == 0


def test_partial_exports_merge_manifest_instead_of_erasing_other_views(tmp_path, monkeypatch):
    import json

    from src.pipeline import board_export

    monkeypatch.setattr(board_export, "EXPORT_DIR", tmp_path)
    launch = board_export._envelope({}, view="launch")
    perps = board_export._envelope({}, view="perps")
    board_export.write_views(launch=launch)
    board_export.write_views(perps=perps)

    meta = json.loads((tmp_path / "meta.json").read_text())
    assert meta["views"] == ["launch", "perps"]
    assert meta["view_status"]["launch"]["refresh_cadence_min"] == 3
    assert meta["view_status"]["perps"]["refresh_cadence_min"] == 5
    assert not list(tmp_path.glob("*.tmp"))
