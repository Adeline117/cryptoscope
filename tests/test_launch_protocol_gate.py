"""A Launch cohort cannot resume after selective source downtime."""
from datetime import datetime, timedelta, timezone


def _ready(**overrides):
    return {"state": "ready", "ready": True, "reason_codes": [], **overrides}


def _blocked(*reasons):
    return {"state": "blocked", "ready": False,
            "reason_codes": list(reasons) or ["source_blocked"]}


def _admit(gate, *, now, start, readiness, **kwargs):
    return gate.admit(
        protocol_id="launch-test-v1", cohort_version=7,
        start_at=start.isoformat(), now=now,
        readiness_probe=lambda **_ignored: readiness,
        **kwargs,
    )


def test_protocol_must_arm_before_boundary_and_open_near_it(tmp_path, monkeypatch):
    from src.pipeline import launch_protocol_gate as gate
    from src.pipeline import solana_launch_stream as stream

    monkeypatch.setattr(stream, "DB", tmp_path / "launches.db")
    start = datetime(2026, 8, 3, tzinfo=timezone.utc)

    scheduled = _admit(
        gate, now=start - timedelta(minutes=10), start=start,
        readiness=_blocked("burn_in_incomplete"),
    )
    armed = _admit(
        gate, now=start - timedelta(minutes=5), start=start, readiness=_ready(),
    )
    opened = _admit(
        gate, now=start + timedelta(seconds=5), start=start, readiness=_ready(),
    )

    assert scheduled["state"] == "scheduled" and not scheduled["enrollment_open"]
    assert armed["state"] == "armed" and not armed["enrollment_open"]
    assert opened["state"] == "open" and opened["enrollment_open"]
    assert opened["auto_execution_allowed"] is False


def test_prestart_readiness_loss_returns_to_scheduled_and_can_rearm(
        tmp_path, monkeypatch):
    from src.pipeline import launch_protocol_gate as gate
    from src.pipeline import solana_launch_stream as stream

    monkeypatch.setattr(stream, "DB", tmp_path / "launches.db")
    start = datetime(2026, 8, 3, tzinfo=timezone.utc)

    armed = _admit(
        gate, now=start - timedelta(minutes=5), start=start, readiness=_ready(),
    )
    reset = _admit(
        gate, now=start - timedelta(minutes=4), start=start,
        readiness=_blocked("archive_provider_unavailable"),
    )
    rearmed = _admit(
        gate, now=start - timedelta(minutes=3), start=start, readiness=_ready(),
    )
    opened = _admit(
        gate, now=start, start=start, readiness=_ready(),
    )

    assert armed["state"] == "armed" and armed["armed_at"] is not None
    assert reset["state"] == "scheduled" and reset["armed_at"] is None
    assert rearmed["state"] == "armed" and rearmed["armed_at"] is not None
    assert opened["state"] == "open" and opened["enrollment_open"] is True


def test_unarmed_or_late_protocol_is_permanently_breached(tmp_path, monkeypatch):
    from src.pipeline import launch_protocol_gate as gate
    from src.pipeline import solana_launch_stream as stream

    monkeypatch.setattr(stream, "DB", tmp_path / "launches.db")
    start = datetime(2026, 8, 3, tzinfo=timezone.utc)
    unarmed = _admit(
        gate, now=start, start=start, readiness=_ready(),
    )
    assert unarmed["state"] == "breached"
    assert "protocol_not_armed_and_ready_at_boundary" in unarmed["reason_codes"]

    monkeypatch.setattr(stream, "DB", tmp_path / "late.db")
    _admit(gate, now=start - timedelta(minutes=1), start=start, readiness=_ready())
    late = _admit(
        gate, now=start + timedelta(seconds=181), start=start, readiness=_ready(),
    )
    assert late["state"] == "breached"
    assert "protocol_activation_late" in late["reason_codes"]


def test_open_protocol_latches_first_source_breach_and_never_reopens(
        tmp_path, monkeypatch):
    import sqlite3
    import pytest

    from src.pipeline import launch_protocol_gate as gate
    from src.pipeline import solana_launch_stream as stream

    monkeypatch.setattr(stream, "DB", tmp_path / "launches.db")
    start = datetime(2026, 8, 3, tzinfo=timezone.utc)
    _admit(gate, now=start - timedelta(minutes=1), start=start, readiness=_ready())
    _admit(gate, now=start, start=start, readiness=_ready())

    breached = _admit(
        gate, now=start + timedelta(minutes=1), start=start,
        readiness=_blocked("reconciliation_epoch_breached"),
    )
    recovered = _admit(
        gate, now=start + timedelta(minutes=2), start=start, readiness=_ready(),
    )

    assert breached["state"] == recovered["state"] == "breached"
    assert recovered["reason_codes"] == breached["reason_codes"]
    assert "source_readiness_breached_after_open" in breached["reason_codes"]
    connection = stream._conn()
    try:
        count = connection.execute(
            "SELECT COUNT(*) FROM launch_protocol_admission_observations"
        ).fetchone()[0]
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            connection.execute(
                "UPDATE launch_protocol_admission_observations SET ready=1"
            )
    finally:
        connection.close()
    assert count == 4
