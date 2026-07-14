"""Resource-safety contracts for the long-running scheduler."""


def test_scheduler_worker_budget_is_bounded(monkeypatch):
    from src.pipeline.scheduler import _scheduler_worker_count

    monkeypatch.delenv("SCHEDULER_MAX_WORKERS", raising=False)
    assert _scheduler_worker_count() == 8

    monkeypatch.setenv("SCHEDULER_MAX_WORKERS", "999")
    assert _scheduler_worker_count() == 12

    monkeypatch.setenv("SCHEDULER_MAX_WORKERS", "0")
    assert _scheduler_worker_count() == 2

    monkeypatch.setenv("SCHEDULER_MAX_WORKERS", "not-a-number")
    assert _scheduler_worker_count() == 8
