"""Public board writes fail closed before replacing known-good JSON."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest


def _launch_event(**overrides):
    event = {
        "id": "launch-1", "lane": "launch", "action_level": "A1_WATCH",
        "actionable_now": False, "auto_execution_allowed": False,
        "effective_decision": "WATCH",
    }
    event.update(overrides)
    return event


def _view(board_export, view, body):
    return board_export._envelope(body, view=view)


def test_wrong_view_name_rejects_write_and_preserves_existing_files(tmp_path, monkeypatch):
    from src.pipeline import board_export

    monkeypatch.setattr(board_export, "EXPORT_DIR", tmp_path)
    good = _view(board_export, "launch", {"events": [_launch_event()]})
    board_export.write_views(launch=good)
    before_launch = (tmp_path / "launch.json").read_bytes()
    before_meta = (tmp_path / "meta.json").read_bytes()
    wrong = {**good, "view": "structure"}

    with pytest.raises(ValueError, match="view name mismatch"):
        board_export.write_views(launch=wrong)

    assert (tmp_path / "launch.json").read_bytes() == before_launch
    assert (tmp_path / "meta.json").read_bytes() == before_meta
    assert not list(tmp_path.glob("*.tmp"))


def test_batch_preflight_rejects_nan_without_partial_update(tmp_path, monkeypatch):
    from src.pipeline import board_export

    monkeypatch.setattr(board_export, "EXPORT_DIR", tmp_path)
    old_launch = _view(board_export, "launch", {"events": [_launch_event()]})
    old_structure = _view(board_export, "structure", {"events": []})
    board_export.write_views(launch=old_launch, structure=old_structure)
    before = {path.name: path.read_bytes() for path in tmp_path.glob("*.json")}

    new_launch = _view(board_export, "launch", {
        "events": [_launch_event(symbol="NEW")],
    })
    bad_structure = _view(board_export, "structure", {
        "events": [], "coverage_ratio": float("nan"),
    })
    with pytest.raises(ValueError, match="Out of range float values"):
        board_export.write_views(launch=new_launch, structure=bad_structure)

    assert {path.name: path.read_bytes() for path in tmp_path.glob("*.json")} == before
    assert not list(tmp_path.glob("*.tmp"))


@pytest.mark.parametrize("changes", [
    {"actionable_now": False},
    {"auto_execution_allowed": True},
    {"effective_decision": "WATCH"},
])
def test_false_a3_cannot_cross_public_boundary(tmp_path, monkeypatch, changes):
    from src.pipeline import board_export

    monkeypatch.setattr(board_export, "EXPORT_DIR", tmp_path)
    now = datetime.now(timezone.utc)
    assessment = {
        "expires_at": (now + timedelta(minutes=1)).isoformat(),
    }
    event = _launch_event(
        action_level="A3_MANUAL_PROBE", actionable_now=True,
        effective_decision="SMALL_PROBE", current_assessment=assessment,
    )
    event.update(changes)

    with pytest.raises(ValueError):
        board_export.write_views(
            launch=_view(board_export, "launch", {"events": [event]})
        )
    assert not (tmp_path / "launch.json").exists()


def test_current_consistent_a3_can_cross_public_boundary(tmp_path, monkeypatch):
    from src.pipeline import board_export

    monkeypatch.setattr(board_export, "EXPORT_DIR", tmp_path)
    event = _launch_event(
        action_level="A3_MANUAL_PROBE", actionable_now=True,
        effective_decision="SMALL_PROBE",
        current_assessment={
            "expires_at": (datetime.now(timezone.utc) + timedelta(minutes=1)).isoformat(),
        },
    )

    board_export.write_views(
        launch=_view(board_export, "launch", {"events": [event]})
    )

    assert (tmp_path / "launch.json").exists()


def test_fail_closed_launch_and_carry_views_remain_serializable(tmp_path, monkeypatch):
    from src.pipeline import board_export

    monkeypatch.setattr(board_export, "EXPORT_DIR", tmp_path)
    launch = _view(board_export, "launch", {"events": [_launch_event()]})
    perps = _view(board_export, "perps", {
        "perps": [], "carry": [], "cascade_events": [{
            "id": "cascade-1", "lane": "cascade", "actionable_now": False,
            "effective_decision": "WATCH", "auto_execution_allowed": False,
        }],
    })

    paths = board_export.write_views(launch=launch, perps=perps)

    assert {path.name for path in paths} == {"launch.json", "perps.json", "meta.json"}
    assert json.loads((tmp_path / "launch.json").read_text())["events"][0][
        "action_level"
    ] == "A1_WATCH"


def test_full_export_render_failure_never_replaces_last_good_launch(tmp_path, monkeypatch):
    from src.pipeline import board_export

    monkeypatch.setattr(board_export, "EXPORT_DIR", tmp_path)
    old = _view(board_export, "launch", {"events": [_launch_event()]})
    board_export.write_views(launch=old)
    before = {path.name: path.read_bytes() for path in tmp_path.glob("*.json")}
    monkeypatch.setattr(board_export, "render_launch", lambda: (_ for _ in ()).throw(
        RuntimeError("launch ledger unavailable")))
    pushed = []
    monkeypatch.setattr(board_export, "push_to_blob",
                        lambda paths: pushed.extend(paths) or len(paths))

    with pytest.raises(RuntimeError, match="ledger unavailable"):
        board_export.run(push=True)

    assert {path.name: path.read_bytes() for path in tmp_path.glob("*.json")} == before
    assert pushed == []
