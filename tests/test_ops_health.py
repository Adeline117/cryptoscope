"""Operator health must surface Launch source/admission failures."""
from __future__ import annotations


def test_launch_protocol_health_uses_effective_current_readiness(monkeypatch):
    from src.contract.launch_protocol import (
        COHORT_VERSION, PROTOCOL_ID, PROTOCOL_START_AT,
    )
    from src.ops import health
    from src.pipeline import launch_protocol_gate, solana_launch_reconcile

    monkeypatch.setattr(
        solana_launch_reconcile, "source_readiness",
        lambda: {
            "state": "blocked", "ready": False, "observed_epochs": 10,
            "required_clean_epochs": 1_440,
            "reason_codes": ["clean_epoch_burn_in_incomplete"],
        },
    )
    monkeypatch.setattr(
        launch_protocol_gate, "read",
        lambda **_kwargs: {
            "protocol_id": PROTOCOL_ID, "cohort_version": COHORT_VERSION,
            "protocol_start_at": PROTOCOL_START_AT,
            "state": "open", "enrollment_open": True, "reason_codes": [],
            "auto_execution_allowed": False,
        },
    )

    got = health._launch_protocol_health()

    assert got["protocol_admission"]["state"] == "open"
    assert got["effective"]["persistent_admission_state"] == "open"
    assert got["effective"]["enrollment_state"] == "blocked"
    assert got["effective"]["enrollment_open"] is False
    assert "clean_epoch_burn_in_incomplete" in got["effective"]["reason_codes"]


def test_launch_protocol_health_fails_closed_when_both_reads_raise(monkeypatch):
    from src.ops import health
    from src.pipeline import launch_protocol_gate, solana_launch_reconcile

    monkeypatch.setattr(
        solana_launch_reconcile, "source_readiness",
        lambda: (_ for _ in ()).throw(OSError("source DB locked")),
    )
    monkeypatch.setattr(
        launch_protocol_gate, "read",
        lambda **_kwargs: (_ for _ in ()).throw(OSError("gate DB locked")),
    )

    got = health._launch_protocol_health()

    assert got["effective"]["enrollment_open"] is False
    assert "source_readiness_unavailable" in got["effective"]["reason_codes"]
    assert "protocol_admission_unavailable" in got["effective"]["reason_codes"]
