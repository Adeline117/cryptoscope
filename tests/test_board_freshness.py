"""Each board view advertises its real scheduler cadence and stale deadline."""
from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest


def test_fast_and_slow_views_have_independent_freshness_policies():
    from src.pipeline.board_export import _envelope

    launch = _envelope({}, view="launch")
    operators = _envelope({}, view="operators")
    perps = _envelope({}, view="perps")
    for payload, cadence, grace in ((launch, 0.5, 0.5), (perps, 5, 5),
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


def test_regular_export_can_skip_separately_scheduled_scanners(monkeypatch):
    from src.pipeline import board_export

    for name in ("render_launch", "render_structure", "render_airdrop"):
        monkeypatch.setattr(board_export, name, lambda: {})
    monkeypatch.setattr(
        board_export, "render_perps",
        lambda: (_ for _ in ()).throw(AssertionError("perps renderer was called")),
    )
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
                              include_opportunities=False, include_perps=False)
    assert result["views_written"] == 0


def test_board_does_not_claim_one_refresh_cadence_for_every_lane():
    board = (Path(__file__).parents[1] / "board" / "public" / "index.html").read_text()

    assert "Launch 报价最多每 30 秒采集/每 10 秒读取" in board
    assert "其他赛道 2–60 分钟采集/每 60 秒读取" in board
    assert "每 15 分钟自动刷新" not in board


def test_overview_names_stale_inputs_and_mobile_shows_every_tab():
    board = (Path(__file__).parents[1] / "board" / "public" / "index.html").read_text()

    assert '["旧版线索",data.opp]' in board
    assert '["证据",data.stats]' in board
    assert "条陈旧:" in board
    assert "最新 ${newest.name}" in board
    assert "最老 ${oldest.name}" in board
    assert "视图按时" in board
    assert "源新鲜" not in board
    assert "上游市场源的覆盖与失败" in board
    assert "grid-template-columns:repeat(3,minmax(0,1fr))" in board
    assert ".grp{display:contents}" in board


def test_launch_delivery_cache_and_poll_fit_inside_quote_ttl():
    import json

    from src.pipeline.launch_execution import QUOTE_TTL_SECONDS

    root = Path(__file__).parents[1]
    board = (root / "board" / "public" / "index.html").read_text()
    config = json.loads((root / "board" / "vercel.json").read_text())
    launch_headers = next(row["headers"] for row in config["headers"]
                          if row["source"] == "/data/launch.json")
    values = {row["key"]: row["value"] for row in launch_headers}
    assert values["Vercel-CDN-Cache-Control"] == "max-age=5"
    assert 'setInterval(loadLaunch,1e4)' in board
    assert 5 + 10 < QUOTE_TTL_SECONDS


def test_partial_exports_merge_manifest_instead_of_erasing_other_views(tmp_path, monkeypatch):
    import json

    from src.pipeline import board_export

    monkeypatch.setattr(board_export, "EXPORT_DIR", tmp_path)
    launch = board_export._envelope({}, view="launch")
    perps = board_export._envelope({}, view="perps")
    board_export.write_views(launch=launch)
    board_export.write_views(perps=perps)
    original_perps = (tmp_path / "perps.json").read_bytes()
    original_perps_status = json.loads((tmp_path / "meta.json").read_text())["view_status"][
        "perps"
    ]
    board_export.write_views(launch=board_export._envelope({}, view="launch"), perps=None)

    meta = json.loads((tmp_path / "meta.json").read_text())
    assert meta["views"] == ["launch", "perps"]
    assert meta["view_status"]["launch"]["refresh_cadence_min"] == 0.5
    assert meta["view_status"]["perps"]["refresh_cadence_min"] == 5
    assert meta["view_status"]["perps"] == original_perps_status
    assert (tmp_path / "perps.json").read_bytes() == original_perps
    assert not list(tmp_path.glob("*.tmp"))
