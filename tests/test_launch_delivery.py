"""Immutable public readback is the only authority that can promote Launch A3."""
from __future__ import annotations

import json
import sqlite3
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

import pytest


class _Response:
    def __init__(self, body: bytes, *, status: int, url: str,
                 content_type: str = "application/json"):
        self._body = body
        self.status = status
        self._url = url
        self.headers = {"Content-Type": content_type}

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self, _limit: int = -1) -> bytes:
        return self._body

    def geturl(self) -> str:
        return self._url


class _Blob:
    def __init__(self, *, corrupt_readback: bool = False,
                 fail_readback: bool = False, redirect_host: str | None = None):
        self.objects: dict[str, bytes] = {}
        self.requests = []
        self.corrupt_readback = corrupt_readback
        self.fail_readback = fail_readback
        self.redirect_host = redirect_host

    def __call__(self, request, *, timeout):
        self.requests.append((request, timeout))
        if request.get_method() == "PUT":
            path = urlparse(request.full_url).path.lstrip("/")
            assert request.headers.get("X-allow-overwrite") is None
            assert path not in self.objects
            self.objects[path] = request.data
            public_url = f"https://test.public.blob.vercel-storage.com/{path}"
            return _Response(
                json.dumps({"url": public_url}).encode(),
                status=200, url=request.full_url,
            )
        path = urlparse(request.full_url).path.lstrip("/")
        if self.fail_readback:
            return _Response(b"unavailable", status=503, url=request.full_url,
                             content_type="text/plain")
        body = self.objects[path]
        if self.corrupt_readback:
            body += b" "
        final_url = request.full_url
        if self.redirect_host:
            final_url = f"https://{self.redirect_host}/{path}"
        return _Response(body, status=200, url=final_url)


def _promotable_assessment(at: datetime) -> dict:
    from src.pipeline.execution_cost import route_contract
    from tests.test_execution_assessments import _assessment

    item = _assessment(
        at,
        quote_source="Jupiter Swap v2 order",
        quote_mode="keyed_v2",
        cost_contract=route_contract(
            notional_usd=25, route_loss_pct=2.0, network_fee_pct=0.02,
            method="complete_delivery_test",
        ),
        # An assessor may lie about this field; the ledger projection must ignore it.
        delivery_sla_state="pass",
    )
    item["execution_probe"] = {
        **item["execution_probe"],
        "source": "Jupiter Swap v2 order",
        "api_mode": "keyed_v2",
        "promotion_eligible": True,
        "quote_contract_verified": True,
        "network_fees_included": True,
        "provider_contract": {
            "version": 1, "provider": "jupiter", "api_version": "v2",
            "operation": "order", "endpoint": "https://api.jup.ag/swap/v2/order",
            "auth_mode": "x_api_key", "slippage_bps": 100,
            "swap_mode": "ExactIn", "read_only": True,
            "taker_supplied": False, "transaction_built": False,
        },
    }
    return item


def _a2_launch(tmp_path, monkeypatch):
    from src.pipeline import edge_validation, opportunity_ledger, opportunity_outcomes
    from tests.test_execution_assessments import _passing_edge_gate, _setup

    ledger, ident = _setup(tmp_path, monkeypatch, decision="SMALL_PROBE")
    now = datetime.now(timezone.utc).replace(microsecond=0)
    monkeypatch.setattr(
        edge_validation, "PROTOCOL_START_AT", (now - timedelta(hours=1)).isoformat(),
    )
    monkeypatch.setattr(
        edge_validation, "_candidate_source_proof",
        lambda _row, snapshot: dict(snapshot["reconciliation_proof"]),
    )
    monkeypatch.setattr(
        edge_validation, "_protocol_admission_state",
        lambda: {"state": "open", "enrollment_open": True, "reason_codes": []},
    )
    opportunity_ledger.append_execution_assessment(
        ident, _promotable_assessment(now),
    )
    monkeypatch.setattr(
        opportunity_outcomes, "actionability_gate",
        lambda lane: _passing_edge_gate(lane),
    )
    row = opportunity_ledger.active("launch", now=now)[0]
    assert row["action_level"] == "A2_PAPER_READY"
    assert row["current_assessment"]["delivery_sla_state"] == "unverified"
    assert "delivery_readback" not in row["current_assessment"]
    launch = {
        "schema_version": 1,
        "view": "launch",
        "generated_at": (now + timedelta(seconds=1)).isoformat(),
        "events": [row],
    }
    return opportunity_ledger, ident, now, launch


def test_exact_immutable_snapshot_promotes_only_manual_a3(tmp_path, monkeypatch):
    from src.pipeline.launch_delivery import publish_and_verify_launch_snapshots

    ledger, ident, now, launch = _a2_launch(tmp_path, monkeypatch)
    blob = _Blob()
    result = publish_and_verify_launch_snapshots(
        launch, token="test-token", opener=blob,
        clock=lambda: now + timedelta(seconds=2),
        nonce_factory=lambda: "nonce00000000001",
    )

    assert result == {
        "eligible": 1, "attempted": 1, "uploaded": 1, "read_back": 1,
        "inserted": 1, "errors": 0, "deferred": 0, "state": "ok",
    }
    promoted = ledger.active("launch", now=now + timedelta(seconds=2))[0]
    assert promoted["action_level"] == "A3_MANUAL_PROBE"
    assert promoted["actionable_now"] is True
    assert promoted["auto_execution_allowed"] is False
    assert promoted["current_assessment"]["auto_execution_allowed"] is False
    proof = promoted["current_assessment"]["delivery_readback"]
    assert proof["state"] == "pass"
    assert proof["assessment_id"] == promoted["current_assessment"]["assessment_id"]
    assert proof["opportunity_id"] == ident
    assert proof["snapshot_path"] in blob.objects
    assert ledger.launch_delivery_readback_matches(
        ident, promoted["current_assessment"],
    )


def test_verified_snapshot_crosses_real_public_board_contract(tmp_path, monkeypatch):
    from src.pipeline import board_export
    from src.pipeline.launch_delivery import publish_and_verify_launch_snapshots
    from tests.test_board_data_contract import (
        _enable_started_protocol, _open_launch_body,
    )

    _enable_started_protocol(monkeypatch)
    ledger, _ident, now, launch = _a2_launch(tmp_path, monkeypatch)
    result = publish_and_verify_launch_snapshots(
        launch, token="test-token", opener=_Blob(),
        clock=lambda: now + timedelta(seconds=2),
        nonce_factory=lambda: "nonce00000000010",
    )
    assert result["inserted"] == 1
    promoted = ledger.active("launch", now=now + timedelta(seconds=2))[0]
    monkeypatch.setattr(board_export, "EXPORT_DIR", tmp_path / "board")

    paths = board_export.write_views(
        launch=board_export._envelope(
            _open_launch_body([promoted]), view="launch",
        ),
    )

    assert {path.name for path in paths} == {"launch.json", "meta.json"}
    public = json.loads((tmp_path / "board" / "launch.json").read_text())
    assessment = public["events"][0]["current_assessment"]
    assert assessment["delivery_sla_state"] == "pass"
    assert assessment["delivery_readback"]["public_url"].startswith("https://")
    assert public["events"][0]["action_level"] == "A3_MANUAL_PROBE"
    assert public["events"][0]["auto_execution_allowed"] is False


def test_portable_proof_copy_without_sql_authority_is_rejected(
        tmp_path, monkeypatch):
    from src.pipeline import board_export
    from src.pipeline.launch_delivery import publish_and_verify_launch_snapshots
    from tests.test_board_data_contract import (
        _enable_started_protocol, _open_launch_body,
    )

    _enable_started_protocol(monkeypatch)
    ledger, _ident, now, launch = _a2_launch(tmp_path, monkeypatch)
    result = publish_and_verify_launch_snapshots(
        launch, token="test-token", opener=_Blob(),
        clock=lambda: now + timedelta(seconds=2),
        nonce_factory=lambda: "nonce00000000011",
    )
    assert result["inserted"] == 1
    copied = ledger.active("launch", now=now + timedelta(seconds=2))[0]
    # The payload is internally self-consistent, but its append-only proof authority
    # is deliberately absent from this independent ledger.
    monkeypatch.setattr(ledger, "DB", tmp_path / "empty-ledger.db")
    monkeypatch.setattr(board_export, "EXPORT_DIR", tmp_path / "copied-board")

    with pytest.raises(ValueError, match="absent from the append-only ledger"):
        board_export.write_views(
            launch=board_export._envelope(
                _open_launch_body([copied]), view="launch",
            ),
        )

    assert not (tmp_path / "copied-board" / "launch.json").exists()


@pytest.mark.parametrize("mode", ["corrupt", "failed"])
def test_failed_public_readback_never_appends_delivery_proof(
        tmp_path, monkeypatch, mode):
    from src.pipeline.launch_delivery import publish_and_verify_launch_snapshots

    ledger, ident, now, launch = _a2_launch(tmp_path, monkeypatch)
    blob = _Blob(corrupt_readback=mode == "corrupt", fail_readback=mode == "failed")

    result = publish_and_verify_launch_snapshots(
        launch, token="test-token", opener=blob,
        clock=lambda: now + timedelta(seconds=2),
        nonce_factory=lambda: "nonce00000000002",
    )

    assert result["inserted"] == 0
    assert result["errors"] == 1
    row = ledger.active("launch", now=now + timedelta(seconds=2))[0]
    assert row["action_level"] == "A2_PAPER_READY"
    assert row["actionable_now"] is False
    assert ledger.launch_delivery_readback(
        row["current_assessment"]["assessment_id"]
    ) is None


def test_public_snapshot_url_rejects_ssrf_and_credentials():
    from src.pipeline.launch_delivery import _validate_public_snapshot_url

    path = "launch-snapshots/v1/assessment-hash-snapshot-nonce0000.json"
    bad_urls = [
        f"https://evil.example/{path}",
        f"https://test.public.blob.vercel-storage.com.evil.example/{path}",
        f"https://user:secret@test.public.blob.vercel-storage.com/{path}",
        f"http://test.public.blob.vercel-storage.com/{path}",
    ]
    for url in bad_urls:
        with pytest.raises(ValueError, match="invalid public snapshot URL"):
            _validate_public_snapshot_url(url, snapshot_path=path)


def test_public_snapshot_redirect_never_creates_proof(tmp_path, monkeypatch):
    from src.pipeline.launch_delivery import publish_and_verify_launch_snapshots

    ledger, _ident, now, launch = _a2_launch(tmp_path, monkeypatch)
    result = publish_and_verify_launch_snapshots(
        launch, token="test-token",
        opener=_Blob(redirect_host="other.public.blob.vercel-storage.com"),
        clock=lambda: now + timedelta(seconds=2),
        nonce_factory=lambda: "nonce00000000007",
    )
    assert result["inserted"] == 0
    assert result["errors"] == 1
    assessment_id = launch["events"][0]["current_assessment"]["assessment_id"]
    assert ledger.launch_delivery_readback(assessment_id) is None


def test_future_readback_clock_never_creates_proof(tmp_path, monkeypatch):
    from src.pipeline.launch_delivery import publish_and_verify_launch_snapshots

    ledger, _ident, now, launch = _a2_launch(tmp_path, monkeypatch)
    result = publish_and_verify_launch_snapshots(
        launch, token="test-token", opener=_Blob(),
        # Still inside the portable 15-second SLA and quote TTL, but too far ahead
        # of the independent ledger wall clock to be trustworthy.
        clock=lambda: now + timedelta(seconds=10),
        nonce_factory=lambda: "nonce00000000013",
    )
    assert result["inserted"] == 0
    assert result["errors"] == 1
    assessment_id = launch["events"][0]["current_assessment"]["assessment_id"]
    assert ledger.launch_delivery_readback(assessment_id) is None


@pytest.mark.parametrize("gate", ["expired", "security_unknown"])
def test_non_delivery_gate_failure_never_uploads_snapshot(
        tmp_path, monkeypatch, gate):
    from src.pipeline.launch_delivery import publish_and_verify_launch_snapshots

    _ledger, _ident, now, launch = _a2_launch(tmp_path, monkeypatch)
    assessment = launch["events"][0]["current_assessment"]
    if gate == "expired":
        assessment["quote_expires_at"] = assessment["expires_at"] = now.isoformat()
    else:
        assessment["security_state"] = "unknown"
        assessment["security_gate"]["state"] = "unknown"
    blob = _Blob()

    result = publish_and_verify_launch_snapshots(
        launch, token="test-token", opener=blob,
        clock=lambda: now + timedelta(seconds=2),
        nonce_factory=lambda: "nonce00000000009",
    )

    assert result["eligible"] == 0
    assert result["attempted"] == 0
    assert result["uploaded"] == 0
    assert blob.requests == []


def test_candidates_are_freshest_first_and_hard_capped(tmp_path, monkeypatch):
    from src.pipeline import launch_delivery

    _ledger, _ident, now, launch = _a2_launch(tmp_path, monkeypatch)
    template = launch["events"][0]
    rows = []
    for index in range(3):
        row = deepcopy(template)
        row["id"] = f"opportunity-sort-{index}"
        assessment = row["current_assessment"]
        assessment["assessment_id"] = f"assessment-sort-{index:02d}"
        assessment["opportunity_id"] = row["id"]
        assessment["assessed_at"] = (now + timedelta(seconds=index)).isoformat()
        rows.append(row)
    launch["events"] = rows
    ordered = launch_delivery._candidate_rows(
        launch, generated_at=now + timedelta(seconds=3),
    )
    assert [item[1]["assessment_id"] for item in ordered] == [
        "assessment-sort-02", "assessment-sort-01", "assessment-sort-00",
    ]

    fake_candidates = [
        (f"opportunity-{index}", {"assessment_id": f"assessment-{index}"})
        for index in range(8)
    ]
    monkeypatch.setattr(
        launch_delivery, "_candidate_rows", lambda *_args, **_kwargs: fake_candidates,
    )
    result = launch_delivery.publish_and_verify_launch_snapshots(
        launch, token="test-token", opener=_Blob(), max_candidates=999,
        clock=lambda: now + timedelta(seconds=4),
    )
    assert result["eligible"] == 8
    assert result["attempted"] == 5
    assert result["deferred"] == 3


@pytest.mark.parametrize("bad_tail", [
    "wrong-assessment-prefix.json",
    "../../proof.json",
    "%2e%2e%2fproof.json",
    "0000000000000000-0000000000000000-nonce0000.json",
])
def test_portable_proof_rejects_unbound_or_traversal_snapshot_path(
        tmp_path, monkeypatch, bad_tail):
    from src.contract.launch_probe import launch_delivery_readback_failures
    from src.pipeline.launch_delivery import publish_and_verify_launch_snapshots

    ledger, ident, now, launch = _a2_launch(tmp_path, monkeypatch)
    result = publish_and_verify_launch_snapshots(
        launch, token="test-token", opener=_Blob(),
        clock=lambda: now + timedelta(seconds=2),
        nonce_factory=lambda: "nonce00000000008",
    )
    assert result["inserted"] == 1
    promoted = ledger.active("launch", now=now + timedelta(seconds=2))[0]
    assessment = promoted["current_assessment"]
    proof = dict(assessment["delivery_readback"])
    bad_path = f"launch-snapshots/v1/{bad_tail}"
    proof.update({
        "snapshot_path": bad_path,
        "public_url": f"https://test.public.blob.vercel-storage.com/{bad_path}",
    })
    tampered = {**assessment, "delivery_readback": proof}

    failures = launch_delivery_readback_failures({"id": ident}, tampered)

    assert "delivery_readback_path_binding_invalid" in failures


def test_crash_retry_uses_new_nonce_and_never_reuses_unknown_object(
        tmp_path, monkeypatch):
    from src.pipeline.launch_delivery import publish_and_verify_launch_snapshots

    ledger, _ident, now, launch = _a2_launch(tmp_path, monkeypatch)
    failed_blob = _Blob(fail_readback=True)
    first = publish_and_verify_launch_snapshots(
        launch, token="test-token", opener=failed_blob,
        clock=lambda: now + timedelta(seconds=2),
        nonce_factory=lambda: "nonce00000000003",
    )
    assert first["uploaded"] == 1 and first["inserted"] == 0
    orphan_path = next(iter(failed_blob.objects))

    healthy_blob = _Blob()
    healthy_blob.objects.update(failed_blob.objects)
    second = publish_and_verify_launch_snapshots(
        launch, token="test-token", opener=healthy_blob,
        clock=lambda: now + timedelta(seconds=3),
        nonce_factory=lambda: "nonce00000000004",
    )
    assert second["inserted"] == 1
    proof = ledger.launch_delivery_readback(
        launch["events"][0]["current_assessment"]["assessment_id"]
    )["payload"]
    assert proof["snapshot_path"] != orphan_path
    assert proof["snapshot_path"].endswith("nonce00000000004.json")


def test_delivery_ledger_rejects_conflict_update_and_delete(tmp_path, monkeypatch):
    from src.pipeline.launch_delivery import publish_and_verify_launch_snapshots

    ledger, _ident, now, launch = _a2_launch(tmp_path, monkeypatch)
    blob = _Blob()
    result = publish_and_verify_launch_snapshots(
        launch, token="test-token", opener=blob,
        clock=lambda: now + timedelta(seconds=2),
        nonce_factory=lambda: "nonce00000000005",
    )
    assert result["inserted"] == 1
    assessment = launch["events"][0]["current_assessment"]
    stored = ledger.launch_delivery_readback(assessment["assessment_id"])
    conflicting_path = stored["payload"]["snapshot_path"].replace(
        "nonce00000000005", "nonce00000000006",
    )
    conflict = {
        **stored["payload"],
        "snapshot_path": conflicting_path,
        "public_url": (
            "https://test.public.blob.vercel-storage.com/" + conflicting_path
        ),
    }
    snapshot_body = next(iter(blob.objects.values()))
    with pytest.raises(ValueError, match="conflicting"):
        ledger._append_launch_delivery_readback(
            conflict, assessment, snapshot_body,
        )

    c = ledger._conn()
    try:
        assert c.execute("PRAGMA recursive_triggers").fetchone()[0] == 1
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            c.execute(
                "UPDATE launch_delivery_readbacks SET verifier_version='fake' WHERE 1=1"
            )
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            c.execute("DELETE FROM launch_delivery_readbacks")
        columns = (
            "readback_id,assessment_id,opportunity_id,verifier_version,public_url,"
            "fetched_at,launch_generated_at,delivery_latency_ms,public_snapshot_sha256,"
            "public_assessment_sha256,ledger_assessment_sha256,"
            "public_assessment_payload,payload,created_at"
        )
        stored_row = c.execute(
            f"SELECT {columns} FROM launch_delivery_readbacks"
        ).fetchone()
        placeholders = ",".join("?" for _ in stored_row)
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            c.execute(
                f"INSERT OR REPLACE INTO launch_delivery_readbacks({columns}) "
                f"VALUES ({placeholders})", stored_row,
            )
    finally:
        c.close()


def test_redundant_column_corruption_fails_closed(tmp_path, monkeypatch):
    from src.pipeline.launch_delivery import publish_and_verify_launch_snapshots

    ledger, _ident, now, launch = _a2_launch(tmp_path, monkeypatch)
    result = publish_and_verify_launch_snapshots(
        launch, token="test-token", opener=_Blob(),
        clock=lambda: now + timedelta(seconds=2),
        nonce_factory=lambda: "nonce00000000012",
    )
    assert result["inserted"] == 1
    assessment_id = launch["events"][0]["current_assessment"]["assessment_id"]
    c = ledger._conn()
    try:
        c.execute("DROP TRIGGER launch_delivery_readbacks_no_update")
        c.execute(
            "UPDATE launch_delivery_readbacks SET public_url=? WHERE assessment_id=?",
            ("https://evil.example/forged.json", assessment_id),
        )
        c.commit()
    finally:
        c.close()

    assert ledger.launch_delivery_readback(assessment_id) is None
    row = ledger.active("launch", now=now + timedelta(seconds=2))[0]
    assert row["action_level"] == "A2_PAPER_READY"
    assert row["actionable_now"] is False
