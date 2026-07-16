"""Public safety claims must fail closed independently of Launch edge versions."""
from datetime import datetime, timedelta, timezone

import pytest


def _envelope(event: dict) -> dict:
    from tests.test_board_data_contract import _launch_body

    now = datetime.now(timezone.utc)
    return {
        "schema_version": 1,
        "view": "launch",
        "generated_at": now.isoformat(),
        "refresh_cadence_min": 1.0,
        "freshness_grace_min": 1.0,
        "next_expected_at": (now + timedelta(minutes=1)).isoformat(),
        "stale_after_at": (now + timedelta(minutes=2)).isoformat(),
        **_launch_body([event]),
    }


@pytest.mark.parametrize("value", [True, 1, "true", None])
def test_public_event_requires_literal_false_auto_execution(value):
    from src.contract.board_view import BoardViewContractError, validate_board_view

    payload = _envelope({
        "id": "launch-1", "lane": "launch", "action_level": "A1_WATCH",
        "actionable_now": False, "effective_decision": "WATCH",
        "auto_execution_allowed": value,
    })

    with pytest.raises(BoardViewContractError, match="exactly false"):
        validate_board_view("launch", payload, cadence_min=1, grace_min=1)


def test_public_board_rejects_unverified_a4_claim():
    from src.contract.board_view import BoardViewContractError, validate_board_view

    payload = _envelope({
        "id": "launch-1", "lane": "launch",
        "action_level": "A4_REAL_FILL_VALIDATED", "actionable_now": False,
        "effective_decision": "WATCH", "auto_execution_allowed": False,
    })

    with pytest.raises(BoardViewContractError, match="real-fill verifier"):
        validate_board_view("launch", payload, cadence_min=1, grace_min=1)


def test_known_untradeable_event_is_blocked_before_protocol_gate(monkeypatch):
    from src.pipeline import opportunity_ledger as ledger

    monkeypatch.setattr(
        "src.pipeline.edge_validation.is_protocol_event", lambda _row: False
    )
    item = {"decision": "WATCH"}
    assessment = {"security_state": "pass", "route_state": "untradeable"}

    action = ledger._launch_action(
        item, assessment, evidence_gate=None, now=datetime.now(timezone.utc)
    )

    assert action["action_level"] == "A0_BLOCKED"
    assert action["action_reason_codes"] == ["security_or_reverse_route_block"]


def test_generic_real_fill_cannot_self_attest_a4():
    from src.pipeline import opportunity_ledger as ledger

    item = {"decision": "WATCH"}
    assessment = {
        "kind": "real_fill", "is_real_fill": True,
        "security_state": "pass", "route_state": "quoted",
        "roundtrip_validated": True,
    }

    action = ledger._launch_action(
        item, assessment, evidence_gate=None, now=datetime.now(timezone.utc)
    )

    assert action["action_level"] != "A4_REAL_FILL_VALIDATED"
    assert action["actionable_now"] is False
