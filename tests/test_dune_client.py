"""Dune failures must never masquerade as valid empty datasets."""
from __future__ import annotations

import time

import pytest

from src.onchain import dune_client as dune


def _ok(payload: dict) -> dune.DuneRequestResult:
    return {"ok": True, "payload": payload, "error_kind": None,
            "http_status": None, "retry_after_seconds": None, "detail": None}


def _failure(kind: str, status: int | None = None,
             retry_after: int | None = None) -> dune.DuneRequestResult:
    return {"ok": False, "payload": None, "error_kind": kind,
            "http_status": status, "retry_after_seconds": retry_after,
            "detail": "test failure"}


@pytest.fixture(autouse=True)
def _configured_without_cooldown(monkeypatch):
    monkeypatch.setenv("DUNE_API_KEY", "test-key")
    monkeypatch.setattr(dune, "CREDITS_EXHAUSTED", False)
    monkeypatch.setattr(dune, "_CREDITS_EXHAUSTED_UNTIL", 0.0)


def test_successful_zero_rows_is_not_a_failure(monkeypatch):
    calls = []
    monkeypatch.setattr(dune, "_cached_query_id", lambda _sql: 17)

    def request(method, path, body=None, timeout=25):
        calls.append((method, path))
        if path == "/query/17/execute":
            return _ok({"execution_id": "exec-empty"})
        return _ok({"state": "QUERY_STATE_COMPLETED", "result": {"rows": []}})

    monkeypatch.setattr(dune, "_request", request)

    result = dune.run_sql_result("select nothing", poll_s=0)

    assert result["state"] == "ok"
    assert result["rows"] == []
    assert result["error_kind"] is None
    assert calls == [("POST", "/query/17/execute"),
                     ("GET", "/execution/exec-empty/status"),
                     ("GET", "/execution/exec-empty/results")]


@pytest.mark.parametrize(
    ("kind", "status", "expected_state"),
    [
        ("credits_exhausted", 402, "deferred"),
        ("auth_failed", 401, "failed"),
        ("auth_failed", 403, "failed"),
        ("rate_limited", 429, "deferred"),
        ("transport_error", None, "deferred"),
    ],
)
def test_cached_execute_failures_never_recreate_query(
        monkeypatch, kind, status, expected_state):
    calls = []
    monkeypatch.setattr(dune, "_cached_query_id", lambda _sql: 17)

    def request(method, path, body=None, timeout=25):
        calls.append((method, path))
        return _failure(kind, status, 120 if status in (402, 429) else None)

    monkeypatch.setattr(dune, "_request", request)

    result = dune.run_sql_result("select guarded")

    assert result["state"] == expected_state
    assert result["error_kind"] == kind
    assert result["http_status"] == status
    assert calls == [("POST", "/query/17/execute")]


def test_only_explicit_404_recreates_a_cached_query_once(monkeypatch):
    calls = []
    stored = []
    monkeypatch.setattr(dune, "_cached_query_id", lambda _sql: 17)
    monkeypatch.setattr(dune, "_store_query_id", lambda sql, qid: stored.append((sql, qid)))

    def request(method, path, body=None, timeout=25):
        calls.append((method, path))
        if path == "/query/17/execute":
            return _failure("not_found", 404)
        if path == "/query":
            return _ok({"query_id": 18})
        if path == "/query/18/execute":
            return _ok({"execution_id": "exec-new"})
        return _ok({
            "state": "QUERY_STATE_COMPLETED",
            "result": {"rows": [{"answer": 42}]},
        })

    monkeypatch.setattr(dune, "_request", request)

    result = dune.run_sql_result("select 42", poll_s=0)

    assert result["state"] == "ok"
    assert result["rows"] == [{"answer": 42}]
    assert result["query_id"] == 18
    assert stored == [("select 42", 18)]
    assert calls == [
        ("POST", "/query/17/execute"),
        ("POST", "/query"),
        ("POST", "/query/18/execute"),
        ("GET", "/execution/exec-new/status"),
        ("GET", "/execution/exec-new/results"),
    ]


def test_replacement_404_never_triggers_a_third_query(monkeypatch):
    calls = []
    monkeypatch.setattr(dune, "_cached_query_id", lambda _sql: 17)
    monkeypatch.setattr(dune, "_store_query_id", lambda *_args: None)

    def request(method, path, body=None, timeout=25):
        calls.append((method, path))
        if path == "/query":
            return _ok({"query_id": 18})
        return _failure("not_found", 404)

    monkeypatch.setattr(dune, "_request", request)

    result = dune.run_sql_result("select deleted twice")

    assert result["state"] == "failed"
    assert result["error_kind"] == "not_found"
    assert calls == [
        ("POST", "/query/17/execute"),
        ("POST", "/query"),
        ("POST", "/query/18/execute"),
    ]


def test_cache_miss_creates_stores_and_executes_query(monkeypatch):
    calls = []
    stored = []
    monkeypatch.setattr(dune, "_cached_query_id", lambda _sql: None)
    monkeypatch.setattr(dune, "_store_query_id", lambda sql, qid: stored.append((sql, qid)))

    def request(method, path, body=None, timeout=25):
        calls.append((method, path))
        if path == "/query":
            return _ok({"query_id": 29})
        if method == "POST":
            return _ok({"execution_id": "exec-created"})
        return _ok({
            "state": "QUERY_STATE_COMPLETED",
            "result": {"rows": [{"created": True}]},
        })

    monkeypatch.setattr(dune, "_request", request)

    result = dune.run_sql_result("select created", poll_s=0)

    assert result["state"] == "ok"
    assert result["rows"] == [{"created": True}]
    assert stored == [("select created", 29)]
    assert calls == [
        ("POST", "/query"),
        ("POST", "/query/29/execute"),
        ("GET", "/execution/exec-created/status"),
        ("GET", "/execution/exec-created/results"),
    ]


def test_cache_miss_create_failure_never_attempts_execute(monkeypatch):
    calls = []
    monkeypatch.setattr(dune, "_cached_query_id", lambda _sql: None)

    def request(method, path, body=None, timeout=25):
        calls.append((method, path))
        return _failure("billing_or_plan_required", 402, 3600)

    monkeypatch.setattr(dune, "_request", request)

    result = dune.run_sql_result("select cannot_create")

    assert result["state"] == "deferred"
    assert result["error_kind"] == "billing_or_plan_required"
    assert calls == [("POST", "/query")]


def test_missing_execution_id_does_not_recreate_query(monkeypatch):
    calls = []
    monkeypatch.setattr(dune, "_cached_query_id", lambda _sql: 17)

    def request(method, path, body=None, timeout=25):
        calls.append((method, path))
        return _ok({})

    monkeypatch.setattr(dune, "_request", request)

    result = dune.run_sql_result("select malformed")

    assert result["state"] == "failed"
    assert result["error_kind"] == "invalid_response"
    assert calls == [("POST", "/query/17/execute")]


def test_explicit_query_is_updated_before_execution(monkeypatch):
    calls = []

    def request(method, path, body=None, timeout=25):
        calls.append((method, path, body))
        if method == "PATCH":
            return _ok({"query_id": 17})
        if method == "POST":
            return _ok({"execution_id": "exec-explicit"})
        return _ok({
            "state": "QUERY_STATE_COMPLETED",
            "result": {"rows": [{"verified": True}]},
        })

    monkeypatch.setattr(dune, "_request", request)

    result = dune.run_sql_result("select current", query_id=17, poll_s=0)

    assert result["state"] == "ok"
    assert result["rows"] == [{"verified": True}]
    assert calls == [
        ("PATCH", "/query/17", {"query_sql": "select current"}),
        ("POST", "/query/17/execute", None),
        ("GET", "/execution/exec-explicit/status", None),
        ("GET", "/execution/exec-explicit/results", None),
    ]


@pytest.mark.parametrize(
    ("kind", "status"),
    [("auth_failed", 403), ("not_found", 404),
     ("billing_or_plan_required", 402)],
)
def test_explicit_query_update_failure_never_executes_stale_sql(
        monkeypatch, kind, status):
    calls = []

    def request(method, path, body=None, timeout=25):
        calls.append((method, path))
        return _failure(kind, status, 60 if status == 402 else None)

    monkeypatch.setattr(dune, "_request", request)

    result = dune.run_sql_result("select changed", query_id=17)

    assert result["error_kind"] == kind
    assert result["state"] == ("deferred" if status == 402 else "failed")
    assert calls == [("PATCH", "/query/17")]


def test_active_402_cooldown_makes_no_http_request(monkeypatch):
    monkeypatch.setattr(dune, "_CREDITS_EXHAUSTED_UNTIL", time.monotonic() + 120)
    monkeypatch.setattr(
        dune, "_request",
        lambda *_args, **_kwargs: pytest.fail("cooldown must prevent the request"),
    )

    result = dune.run_sql_result("select deferred")

    assert result["state"] == "deferred"
    assert result["error_kind"] == "credits_cooldown"
    assert result["http_status"] == 402
    assert 1 <= result["retry_after_seconds"] <= 120
    assert result["retry_at"].endswith("Z")


@pytest.mark.parametrize(
    ("query_state", "error_kind"),
    [
        ("QUERY_STATE_FAILED", "query_failed"),
        ("QUERY_STATE_CANCELED", "query_canceled"),
        ("QUERY_STATE_EXPIRED", "query_expired"),
        ("QUERY_STATE_COMPLETED_PARTIAL", "partial_result"),
    ],
)
def test_terminal_execution_states_fail_closed(monkeypatch, query_state, error_kind):
    monkeypatch.setattr(dune, "_cached_query_id", lambda _sql: 17)

    def request(method, path, body=None, timeout=25):
        if method == "POST":
            return _ok({"execution_id": "exec-terminal"})
        return _ok({"state": query_state, "error": "engine stopped"})

    monkeypatch.setattr(dune, "_request", request)

    result = dune.run_sql_result("select terminal", poll_s=0)

    assert result["state"] == "failed"
    assert result["error_kind"] == error_kind
    assert result["rows"] == []


def test_poll_timeout_is_deferred_not_empty_success(monkeypatch):
    monkeypatch.setattr(dune, "_cached_query_id", lambda _sql: 17)

    def request(method, path, body=None, timeout=25):
        if method == "POST":
            return _ok({"execution_id": "exec-slow"})
        return _ok({"state": "QUERY_STATE_EXECUTING"})

    monkeypatch.setattr(dune, "_request", request)

    result = dune.run_sql_result("select slow", poll_s=0, max_polls=2)

    assert result["state"] == "deferred"
    assert result["error_kind"] == "poll_timeout"


def test_free_status_is_polled_before_one_result_export(monkeypatch):
    calls = []
    status_states = iter(["QUERY_STATE_PENDING", "QUERY_STATE_EXECUTING",
                          "QUERY_STATE_COMPLETED"])
    monkeypatch.setattr(dune, "_cached_query_id", lambda _sql: 17)

    def request(method, path, body=None, timeout=25):
        calls.append((method, path))
        if method == "POST":
            return _ok({"execution_id": "exec-polled"})
        if path.endswith("/status"):
            return _ok({"state": next(status_states)})
        return _ok({
            "state": "QUERY_STATE_COMPLETED",
            "result": {"rows": [{"once": True}]},
        })

    monkeypatch.setattr(dune, "_request", request)

    result = dune.run_sql_result("select polled", poll_s=0, max_polls=3)

    assert result["state"] == "ok"
    assert [path for method, path in calls if path.endswith("/status")] == [
        "/execution/exec-polled/status",
        "/execution/exec-polled/status",
        "/execution/exec-polled/status",
    ]
    assert [path for method, path in calls if path.endswith("/results")] == [
        "/execution/exec-polled/results",
    ]


def test_expired_cooldown_recovers_and_clears_flag(monkeypatch):
    monkeypatch.setattr(dune, "CREDITS_EXHAUSTED", True)
    monkeypatch.setattr(dune, "_CREDITS_EXHAUSTED_UNTIL", time.monotonic() - 1)
    monkeypatch.setattr(dune, "_cached_query_id", lambda _sql: 17)

    def request(method, path, body=None, timeout=25):
        if method == "POST":
            return _ok({"execution_id": "exec-recovered"})
        return _ok({"state": "QUERY_STATE_COMPLETED", "result": {"rows": []}})

    monkeypatch.setattr(dune, "_request", request)

    result = dune.run_sql_result("select recovered", poll_s=0)

    assert result["state"] == "ok"
    assert dune.CREDITS_EXHAUSTED is False


def test_legacy_run_sql_wrapper_returns_rows_only_on_success(monkeypatch):
    monkeypatch.setattr(
        dune, "run_sql_result", lambda *_args, **_kwargs: dune._sql_result(
            "ok", rows=[{"answer": 42}]))
    assert dune.run_sql("select 42") == [{"answer": 42}]

    monkeypatch.setattr(
        dune, "run_sql_result", lambda *_args, **_kwargs: dune._sql_result(
            "deferred", error_kind="rate_limited", http_status=429))
    assert dune.run_sql("select delayed") == []


def test_label_verification_does_not_probe_again_after_402(monkeypatch):
    from src.onchain import cex_addresses, label_verify

    address = "0x0000000000000000000000000000000000000001"
    calls = []
    monkeypatch.setattr(
        label_verify, "gather_trusted_addresses", lambda: {address: "test operator"})
    monkeypatch.setattr(cex_addresses, "evm_exchanges", lambda: {})
    monkeypatch.setattr(dune, "available", lambda: True)

    def run_sql_result(sql, **_kwargs):
        calls.append(sql)
        return dune._sql_result(
            "deferred", error_kind="credits_exhausted", http_status=402,
            retry_after_seconds=3600)

    monkeypatch.setattr(dune, "run_sql_result", run_sql_result)

    result = label_verify.sweep()

    assert result["complete"] is False
    assert result["dune_state"] == "deferred"
    assert result["dune_error_kind"] == "credits_exhausted"
    assert len(calls) == 1
    assert "select 1 as ok" not in calls


def test_label_verification_accepts_a_verified_empty_result(monkeypatch):
    from src.onchain import cex_addresses, label_verify

    address = "0x0000000000000000000000000000000000000001"
    calls = []
    monkeypatch.setattr(
        label_verify, "gather_trusted_addresses", lambda: {address: "test operator"})
    monkeypatch.setattr(cex_addresses, "evm_exchanges", lambda: {})
    monkeypatch.setattr(dune, "available", lambda: True)

    def run_sql_result(sql, **_kwargs):
        calls.append(sql)
        return dune._sql_result("ok", rows=[])

    monkeypatch.setattr(dune, "run_sql_result", run_sql_result)

    result = label_verify.sweep()

    assert result["complete"] is True
    assert result["hits"] == []
    assert result["dune_state"] == "ok"
    assert result["dune_error_kind"] is None
    assert len(calls) == 1


def test_label_verification_without_addresses_has_a_stable_schema(monkeypatch):
    from src.onchain import label_verify

    monkeypatch.setattr(label_verify, "gather_trusted_addresses", lambda: {})

    result = label_verify.sweep()

    assert result == {
        "complete": True,
        "checked": 0,
        "hits": [],
        "dune_state": "not_needed",
        "dune_error_kind": None,
    }
