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
