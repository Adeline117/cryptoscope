"""Browser readback of immutable Launch delivery evidence stays fail closed."""
from __future__ import annotations

import base64
import hashlib
import json
import subprocess
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse

import pytest


ROOT = Path(__file__).parents[1]
BOARD = ROOT / "board" / "public" / "index.html"
DELIVERY = ROOT / "board" / "public" / "launch-delivery.js"
JOIN = ROOT / "board" / "public" / "protocol-join.js"
CHARTS = ROOT / "board" / "public" / "vendor" / "lightweight-charts-5.2.0.js"


def _healthy_runtime() -> dict:
    return {
        "version": 1, "state": "healthy", "blocks_actionability": False,
        "auto_execution_allowed": False, "storage_pressure": "ok",
        "reason_codes": [],
        "streams": {
            "solana": {
                "state": "healthy", "live": 1, "configured": 1,
                "maintenance": "healthy",
            },
            "evm": {"state": "healthy", "live": 2, "configured": 2},
        },
        "hyperliquid_raw_trade_retention": "retained",
    }


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, allow_nan=False, sort_keys=True,
        separators=(",", ":"),
    ).encode()


def _fixture() -> tuple[dict, dict, bytes]:
    from src.contract.launch_probe import launch_delivery_subject

    assessed = datetime.now(timezone.utc).replace(microsecond=123456)
    generated = assessed + timedelta(seconds=1)
    fetched = generated + timedelta(seconds=1)
    expires = assessed + timedelta(minutes=5)
    assessment = {
        "assessment_id": "assessment-browser-1",
        "opportunity_id": "launch-browser-1",
        "kind": "read_only_quote",
        "assessed_at": assessed.isoformat(),
        "expires_at": expires.isoformat(),
        "delivery_sla_state": "pass",
        "action_reason_codes": ["quote_valid"],
        "auto_execution_allowed": False,
        "is_real_fill": False,
        # Floats deliberately prove that the browser hashes the canonical raw
        # assessment bytes instead of reserialising Python's 1.0 as JavaScript 1.
        "notional_usd": 25.0,
        "nested_evidence": {"price": 1.0, "label": "右尾机会验证"},
    }
    subject = launch_delivery_subject(assessment)
    assessment_hash = hashlib.sha256(_canonical_bytes(subject)).hexdigest()
    ledger_hash = "b" * 64
    envelope = {
        "schema_version": 1,
        "kind": "cryptoscope_launch_assessment_snapshot",
        "verifier_version": "public_launch_readback_v1",
        "opportunity_id": "launch-browser-1",
        "assessment_id": "assessment-browser-1",
        "launch_generated_at": generated.isoformat(),
        "public_assessment_sha256": assessment_hash,
        "ledger_assessment_sha256": ledger_hash,
        "auto_execution_allowed": False,
        "assessment": subject,
    }
    body = _canonical_bytes(envelope)
    snapshot_hash = hashlib.sha256(body).hexdigest()
    snapshot_path = (
        "launch-snapshots/v1/assessment-browser-1-"
        f"{assessment_hash[:16]}-{snapshot_hash[:16]}-browsernonce1234.json"
    )
    public_url = f"https://test.public.blob.vercel-storage.com/{snapshot_path}"
    assessment["delivery_readback"] = {
        "version": 1,
        "verifier_version": "public_launch_readback_v1",
        "state": "pass",
        "assessment_id": "assessment-browser-1",
        "opportunity_id": "launch-browser-1",
        "public_url": public_url,
        "snapshot_path": snapshot_path,
        "fetched_at": fetched.isoformat(),
        "launch_generated_at": generated.isoformat(),
        "delivery_latency_ms": 1000.0,
        "public_snapshot_sha256": snapshot_hash,
        "public_assessment_sha256": assessment_hash,
        "ledger_assessment_sha256": ledger_hash,
    }
    row = {
        "id": "launch-browser-1",
        "chain": "solana",
        "token": "MintBrowser111",
        "symbol": "BROWSER",
        "action_level": "A3_MANUAL_PROBE",
        "actionable_now": True,
        "auto_execution_allowed": False,
        "current_assessment": assessment,
    }
    launch = {
        "schema_version": 1,
        "generated_at": (generated + timedelta(seconds=2)).isoformat(),
        "events": [row],
    }
    return row, launch, body


def _duplicate_key_attack_fixture() -> tuple[dict, dict, bytes]:
    """Build the differential-parse body that used to bind two assessments."""
    from src.contract.launch_probe import launch_delivery_subject

    row, launch, _body = _fixture()
    proof = row["current_assessment"]["delivery_readback"]
    current = launch_delivery_subject(row["current_assessment"])
    first = deepcopy(current)
    first["nested_evidence"]["label"] = "NOT THE CURRENT ASSESSMENT"
    first_hash = hashlib.sha256(_canonical_bytes(first)).hexdigest()
    rest = {
        "assessment_id": proof["assessment_id"],
        "auto_execution_allowed": False,
        "kind": "cryptoscope_launch_assessment_snapshot",
        "launch_generated_at": proof["launch_generated_at"],
        "ledger_assessment_sha256": proof["ledger_assessment_sha256"],
        "opportunity_id": proof["opportunity_id"],
        "public_assessment_sha256": first_hash,
        "schema_version": 1,
        "verifier_version": "public_launch_readback_v1",
    }
    # JSON.parse keeps the second key; the raw-byte scanner historically hashed
    # the first. Both are valid JSON values, but their coexistence must be fatal.
    body = (
        b'{"assessment":' + _canonical_bytes(first)
        + b',"assessment":' + _canonical_bytes(current)
        + b"," + _canonical_bytes(rest)[1:]
    )
    snapshot_hash = hashlib.sha256(body).hexdigest()
    snapshot_path = (
        "launch-snapshots/v1/assessment-browser-1-"
        f"{first_hash[:16]}-{snapshot_hash[:16]}-duplicatekey1234.json"
    )
    proof.update({
        "public_assessment_sha256": first_hash,
        "public_snapshot_sha256": snapshot_hash,
        "snapshot_path": snapshot_path,
        "public_url": f"https://test.public.blob.vercel-storage.com/{snapshot_path}",
    })
    return row, launch, body


def _node(script: str, payload: dict) -> dict:
    result = subprocess.run(
        ["node", "-e", script, str(DELIVERY), json.dumps(payload)],
        check=True, capture_output=True, text=True,
    )
    return json.loads(result.stdout)


def test_pure_browser_guard_binds_python_canonical_bytes_and_fails_closed():
    row, launch, body = _fixture()
    script = r"""
const guard=require(process.argv[1]);
const {webcrypto}=require("node:crypto");
const input=JSON.parse(process.argv[2]);
const bytes=Uint8Array.from(Buffer.from(input.body,"base64"));
const response=(content, changes={})=>({
  status:200, redirected:false, url:input.row.current_assessment.delivery_readback.public_url,
  headers:{get:name=>name.toLowerCase()==="content-type"?"application/json":name.toLowerCase()==="content-length"?String(content.byteLength):null},
  body:{getReader(){let sent=false;return{read:async()=>sent?{done:true}:{done:false,value:(sent=true,content)},cancel:async()=>{},releaseLock(){}}}},
  ...changes,
});
(async()=>{
  const descriptor=guard.inspect(input.row,input.launch);
  const exact=await guard.verifySnapshotBytes(descriptor,bytes,webcrypto);
  const fetched=await guard.verify(input.row,input.launch,{cryptoImpl:webcrypto,fetchImpl:async()=>response(bytes)});
  const changed=structuredClone(input.row);
  changed.current_assessment.nested_evidence.price=2;
  const rebound=await guard.verifySnapshotBytes(guard.inspect(changed,input.launch),bytes,webcrypto);
  const badBytes=bytes.slice();badBytes[badBytes.length-2]^=1;
  const tampered=await guard.verifySnapshotBytes(descriptor,badBytes,webcrypto);
  const redirected=await guard.verify(input.row,input.launch,{cryptoImpl:webcrypto,fetchImpl:async()=>response(bytes,{redirected:true})});
  const evil=structuredClone(input.row);
  evil.current_assessment.delivery_readback.public_url=evil.current_assessment.delivery_readback.public_url.replace(".com/",".com.evil.example/");
  const noZone=structuredClone(input.row);
  noZone.current_assessment.delivery_readback.fetched_at=noZone.current_assessment.delivery_readback.fetched_at.replace("+00:00","");
  const unsafePolicy=structuredClone(input.row);unsafePolicy.auto_execution_allowed=true;
  const changedProof=structuredClone(input.row);
  changedProof.current_assessment.delivery_readback.ledger_assessment_sha256="c".repeat(64);
  const changedDescriptor=guard.inspect(changedProof,input.launch);
  const oversized=await guard.verify(input.row,input.launch,{cryptoImpl:webcrypto,fetchImpl:async()=>response(new Uint8Array(guard.MAX_SNAPSHOT_BYTES+1))});
  const timedOut=await guard.verify(input.row,input.launch,{cryptoImpl:webcrypto,timeoutMs:5,fetchImpl:()=>new Promise(()=>{})});
  process.stdout.write(JSON.stringify({exact,fetched,rebound,tampered,redirected,
    evil:guard.inspect(evil,input.launch),noZone:guard.inspect(noZone,input.launch),
    unsafePolicy:guard.inspect(unsafePolicy,input.launch),
    changedProofKey:changedDescriptor.key,originalKey:descriptor.key,oversized,timedOut}));
})().catch(error=>{process.stderr.write(String(error.stack||error));process.exit(1)});
"""
    got = _node(script, {
        "row": row, "launch": launch,
        "body": base64.b64encode(body).decode(),
    })

    assert got["exact"]["state"] == got["fetched"]["state"] == "pass"
    assert got["rebound"]["reason"] == "delivery_snapshot_binding_invalid"
    assert got["tampered"]["reason"] == "delivery_snapshot_hash_mismatch"
    assert got["redirected"]["reason"] == "delivery_response_invalid"
    assert got["evil"]["reason"] == "delivery_url_invalid"
    assert got["noZone"]["reason"] == "delivery_clock_invalid"
    assert got["unsafePolicy"]["reason"] == "delivery_identity_invalid"
    assert got["changedProofKey"] != got["originalKey"]
    assert got["oversized"]["reason"] == "delivery_snapshot_size_invalid"
    assert got["timedOut"]["reason"] == "delivery_fetch_timeout"


def test_browser_guard_rejects_duplicate_key_differential_parse_attack():
    row, launch, body = _duplicate_key_attack_fixture()
    script = r"""
const guard=require(process.argv[1]);
const {webcrypto}=require("node:crypto");
const input=JSON.parse(process.argv[2]);
(async()=>{
  const bytes=Uint8Array.from(Buffer.from(input.body,"base64"));
  const result=await guard.verifySnapshotBytes(guard.inspect(input.row,input.launch),bytes,webcrypto);
  process.stdout.write(JSON.stringify(result));
})().catch(error=>{process.stderr.write(String(error.stack||error));process.exit(1)});
"""
    got = _node(script, {
        "row": row, "launch": launch,
        "body": base64.b64encode(body).decode(),
    })

    assert got["state"] == "fail"
    assert got["reason"] == "delivery_assessment_hash_mismatch"


def test_board_requires_current_browser_readback_and_only_shows_relevant_proofs():
    html = BOARD.read_text()

    assert '<script src="/launch-delivery.js"></script>' in html
    assert html.index('/launch-delivery.js') < html.index('function actionLevel(')
    assert 'launchDeliveryUiState(r,launchPayload||data?.launch).state!=="pass"' in html
    assert 'ca.delivery_sla_state==="pass"&&launchDeliveryUiState' in html
    assert "queueLaunchDeliveryVerifications(data.launch)" in html
    assert "exact bytes、公开 assessment 与当前候选均已在本浏览器重新绑定" in html
    assert 'r.action_level==="A3_MANUAL_PROBE"||ca.delivery_readback?launchDeliveryProofHtml(r):""' in html
    assert "失败或超时一律保持 A1" in html


@pytest.mark.parametrize(
    ("mode", "expected_state", "expected_level"),
    [("success", "pass", "A3_MANUAL_PROBE"),
     ("tampered", "fail", "A1_WATCH"),
     ("network_failure", "fail", "A1_WATCH")],
)
def test_real_browser_keeps_a3_downgraded_until_exact_readback(
        mode: str, expected_state: str, expected_level: str):
    playwright = pytest.importorskip("playwright.sync_api")
    row, launch, body = _fixture()
    public_url = row["current_assessment"]["delivery_readback"]["public_url"]
    bad_body = body.replace(b"cryptoscope_launch", b"Cryptoscope_launch", 1)
    empty_launch = {"schema_version": 1, "generated_at": launch["generated_at"], "events": []}
    payloads = {
        "launch": empty_launch,
        "structure": {"schema_version": 1, "events": [], "source_health": []},
        "airdrop": {"schema_version": 1, "events": []},
        "watch": {"schema_version": 1, "watch": []},
        "perps": {"schema_version": 1, "perps": [], "carry": [], "cascade_events": []},
        "opportunities": {"schema_version": 1, "opportunities": []},
        "operators": {"schema_version": 1, "operators": []},
        "stats": {"schema_version": 1, "lanes": {}},
        "meta": {"schema_version": 1, "runtime_safety": _healthy_runtime()},
    }

    with playwright.sync_playwright() as driver:
        if not Path(driver.chromium.executable_path).exists():
            pytest.skip("Playwright Chromium is not installed")
        browser = driver.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1280, "height": 900})

        def route_request(route, request):
            path = urlparse(request.url).path
            if request.url == public_url:
                if mode == "network_failure":
                    route.abort("failed")
                else:
                    route.fulfill(
                        status=200, content_type="application/json",
                        body=body if mode == "success" else bad_body,
                    )
            elif path == "/":
                route.fulfill(status=200, content_type="text/html", body=BOARD.read_text())
            elif path == "/launch-delivery.js":
                route.fulfill(status=200, content_type="text/javascript", body=DELIVERY.read_text())
            elif path == "/protocol-join.js":
                route.fulfill(status=200, content_type="text/javascript", body=JOIN.read_text())
            elif path == "/vendor/lightweight-charts-5.2.0.js":
                route.fulfill(status=200, content_type="text/javascript", body=CHARTS.read_bytes())
            elif path.startswith("/data/") and path.endswith(".json"):
                route.fulfill(status=200, content_type="application/json",
                              body=json.dumps(payloads.get(Path(path).stem, {})))
            else:
                route.abort("blockedbyclient")

        page.route("**/*", route_request)
        page.goto("https://board.test/#launch", wait_until="domcontentloaded")
        page.wait_for_function("() => typeof queueLaunchDeliveryVerifications === 'function'")
        initial = page.evaluate(
            """payload => {
              launchProtocolUiState=()=>({open:true});
              launchReconciliationProofState=()=>({state:"pass"});
              launchStatsJoinState=()=>({actionBlock:false,edgeUsable:false});
              data.launch=payload;launchDeliveryCache.clear();
              queueLaunchDeliveryVerifications(payload);
              const row=payload.events[0];
              return {raw:row.action_level,effective:actionLevel(row,payload),
                state:launchDeliveryUiState(row,payload).state,
                detail:launchDeliveryProofHtml(row)};
            }""",
            launch,
        )
        assert initial["raw"] == "A3_MANUAL_PROBE"
        assert initial["effective"] == "A1_WATCH"
        assert initial["state"] == "pending"
        assert "A3 已降级" in initial["detail"]

        page.wait_for_function(
            "() => launchDeliveryUiState(data.launch.events[0],data.launch).state !== 'pending'"
        )
        settled = page.evaluate(
            """() => {const row=data.launch.events[0];return {
              state:launchDeliveryUiState(row,data.launch).state,
              effective:actionLevel(row,data.launch),detail:launchDeliveryProofHtml(row)};}"""
        )
        browser.close()

    assert settled["state"] == expected_state
    assert settled["effective"] == expected_level
    if mode == "success":
        assert "浏览器回读通过" in settled["detail"]
        assert public_url in settled["detail"]
    else:
        assert "A3 已降级" in settled["detail"]
