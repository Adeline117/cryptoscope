"""GMGN/Cloudflare failures must not become freshly published empty markets."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.onchain import gmgn
from src.pipeline import board_export


class _Response:
    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self, _limit=-1):
        return json.dumps(self.payload).encode()


def _flare(body: str, *, status: int = 200) -> dict:
    return {"status": "ok", "solution": {"status": status, "response": body}}


def _fetch_failure(kind: str) -> gmgn.GmgnFetchResult:
    return {"state": "failed", "payload": None, "error_kind": kind,
            "http_status": None, "detail": "test failure"}


def _fetch_ok(payload: dict) -> gmgn.GmgnFetchResult:
    return {"state": "ok", "payload": payload, "error_kind": None,
            "http_status": None, "detail": None}


def _rank(state="ok", *, chain="sol", rows=None, error_kind=None,
          risk_incomplete=0):
    rows = rows or []
    return {"state": state, "rows": rows, "error_kind": error_kind,
            "chain": chain, "received": len(rows), "accepted": len(rows),
            "dropped": 0, "risk_incomplete": risk_incomplete}


def test_challenge_html_with_embedded_json_is_not_extracted(monkeypatch):
    html = '<html><script>{"code":0,"data":{"rank":[]}}</script></html>'
    monkeypatch.setattr(
        gmgn.urllib.request, "urlopen",
        lambda _request, timeout: _Response(_flare(html)),
    )

    result = gmgn._fs_get_result("https://gmgn.ai/rank", timeout=5)

    assert result["state"] == "failed"
    assert result["error_kind"] == "upstream_non_json"
    assert gmgn._fs_get("https://gmgn.ai/rank", timeout=5) is None


def test_exact_chromium_json_viewer_wrapper_is_accepted(monkeypatch):
    payload = {"code": 0, "data": {"rank": [{"name": "A & B"}]}}
    viewer = (gmgn._JSON_VIEWER_PREFIX
              + json.dumps(payload).replace("&", "&amp;")
              + gmgn._JSON_VIEWER_SUFFIX)
    monkeypatch.setattr(
        gmgn.urllib.request, "urlopen",
        lambda _request, timeout: _Response(_flare(viewer)),
    )

    result = gmgn._fs_get_result("https://gmgn.ai/rank", timeout=5)

    assert result["state"] == "ok"
    assert result["payload"] == payload


@pytest.mark.parametrize(
    ("payload", "error_kind"),
    [
        ({"code": 403, "message": "blocked", "data": {}}, "gmgn_api_error"),
        ({"message": "missing code", "data": {}}, "gmgn_api_error"),
    ],
)
def test_gmgn_api_error_is_not_a_success(monkeypatch, payload, error_kind):
    monkeypatch.setattr(
        gmgn.urllib.request, "urlopen",
        lambda _request, timeout: _Response(_flare(json.dumps(payload))),
    )

    result = gmgn._fs_get_result("https://gmgn.ai/rank", timeout=5)

    assert result["state"] == "failed"
    assert result["error_kind"] == error_kind


def test_cloudflare_solution_status_is_preserved(monkeypatch):
    monkeypatch.setattr(
        gmgn.urllib.request, "urlopen",
        lambda _request, timeout: _Response(_flare("forbidden", status=403)),
    )

    result = gmgn._fs_get_result("https://gmgn.ai/rank", timeout=5)

    assert result["state"] == "failed"
    assert result["error_kind"] == "challenge_or_blocked"
    assert result["http_status"] == 403


@pytest.mark.parametrize(
    ("payload", "error_kind"),
    [
        ({"code": 0, "data": {}}, "missing_rank"),
        ({"code": 0, "data": {"rank": {}}}, "invalid_rank_schema"),
    ],
)
def test_missing_or_wrong_rank_schema_is_failed(monkeypatch, payload, error_kind):
    monkeypatch.setattr(gmgn, "_fs_get_result", lambda _url: _fetch_ok(payload))

    result = gmgn.smart_money_rank_result("sol")

    assert result["state"] == "failed"
    assert result["rows"] == []
    assert result["error_kind"] == error_kind


def test_explicit_empty_rank_is_suspicious_not_ok(monkeypatch):
    monkeypatch.setattr(
        gmgn, "_fs_get_result",
        lambda _url: _fetch_ok({"code": 0, "data": {"rank": []}}),
    )

    result = gmgn.smart_money_rank_result("sol")

    assert result["state"] == "partial"
    assert result["error_kind"] == "suspicious_empty_rank"


def test_all_malformed_rank_rows_fail_closed(monkeypatch):
    monkeypatch.setattr(
        gmgn, "_fs_get_result",
        lambda _url: _fetch_ok({
            "code": 0,
            "data": {"rank": [{"symbol": "NO_ADDRESS"}, "not-an-object"]},
        }),
    )

    result = gmgn.smart_money_rank_result("sol")

    assert result["state"] == "failed"
    assert result["error_kind"] == "all_rank_rows_malformed"
    assert result["dropped"] == 2


@pytest.mark.parametrize(
    "row",
    [
        {"address": "token-without-signal", "is_honeypot": 0},
        {"address": "token-without-safety", "smart_degen_count": 3},
        {"address": "token-nan", "smart_degen_count": "nan", "is_honeypot": 0},
    ],
)
def test_core_signal_and_safety_fields_are_required(monkeypatch, row):
    monkeypatch.setattr(
        gmgn, "_fs_get_result",
        lambda _url: _fetch_ok({"code": 0, "data": {"rank": [row]}}),
    )

    result = gmgn.smart_money_rank_result("sol")

    assert result["state"] == "failed"
    assert result["error_kind"] == "all_rank_rows_malformed"


def test_missing_reverse_risk_schema_can_never_render_green(monkeypatch):
    monkeypatch.setattr(
        gmgn, "_fs_get_result",
        lambda _url: _fetch_ok({
            "code": 0,
            "data": {"rank": [{
                "address": "token-minimal",
                "smart_degen_count": 3,
                "is_honeypot": 0,
            }]},
        }),
    )

    result = gmgn.smart_money_rank_result("sol")
    row = result["rows"][0]

    assert result["state"] == "partial"
    assert result["error_kind"] == "incomplete_risk_fields"
    assert result["risk_incomplete"] == 1
    assert row["risk_fields_complete"] is False
    assert gmgn.exit_liquidity_risk(row)["level"] == "unknown"
    assert gmgn._manipulation(row)["level"] == "unknown"
    assert gmgn._rug_from_gmgn(row)["level"] == "unchecked"


def test_complete_reverse_risk_schema_is_normalized(monkeypatch):
    raw = {
        "address": "token-complete", "smart_degen_count": "3",
        "is_honeypot": 0, "sniper_count": "4", "bot_degen_count": 5,
        "bundler_rate": "0.1", "entrapment_ratio": "0.25",
        "dev_team_hold_rate": "0.01", "top70_sniper_hold_rate": "0.02",
        "sell_tax": "",
    }
    monkeypatch.setattr(
        gmgn, "_fs_get_result",
        lambda _url: _fetch_ok({"code": 0, "data": {"rank": [raw]}}),
    )

    result = gmgn.smart_money_rank_result("sol")
    row = result["rows"][0]

    assert result["state"] == "ok"
    assert row["risk_fields_complete"] is True
    assert row["smart_money"] == 3
    assert row["snipers"] == 4
    assert row["entrapment_ratio"] == 0.25


def test_missing_dev_hold_rate_keeps_rug_status_unchecked(monkeypatch):
    raw = {
        "address": "token-missing-dev", "smart_degen_count": 3,
        "is_honeypot": 0, "sniper_count": 4, "bot_degen_count": 5,
        "bundler_rate": 0.1, "entrapment_ratio": 0.02,
        "top70_sniper_hold_rate": 0.02, "sell_tax": "",
    }
    monkeypatch.setattr(
        gmgn, "_fs_get_result",
        lambda _url: _fetch_ok({"code": 0, "data": {"rank": [raw]}}),
    )

    result = gmgn.smart_money_rank_result("sol")
    row = result["rows"][0]

    assert result["state"] == "partial"
    assert result["error_kind"] == "incomplete_risk_fields"
    assert row["risk_fields_complete"] is False
    assert gmgn._rug_from_gmgn(row)["level"] == "unchecked"


def test_normalized_reverse_tells_still_raise_exit_and_manipulation_risk():
    normalized = {
        "smart_money": 2,
        "bots": 60,
        "snipers": 45,
        "sniper_hold_rate": 0.16,
        "bundler_rate": 0,
        "entrapment_ratio": 0.25,
        "risk_fields_complete": True,
    }

    exit_risk = gmgn.exit_liquidity_risk(normalized)
    manipulation = gmgn._manipulation(normalized)

    assert exit_risk["level"] == "high"
    assert any("诱捕盘" in reason for reason in exit_risk["reasons"])
    assert any("狙击者持仓" in reason for reason in exit_risk["reasons"])
    assert manipulation["level"] == "moderate"
    assert any("诱捕率" in reason for reason in manipulation["reasons"])
    assert any("机器人" in reason for reason in manipulation["reasons"])


def test_fallback_rows_get_unknown_risk_instead_of_green_defaults():
    rows = [{"token": "fallback", "smart_actors": 2, "age_days": 0.25,
             "rug": {"level": "unchecked", "facts": []}}]

    normalized = board_export._normalize_legacy_opportunity_rows(
        rows, "self_hosted")[0]

    assert normalized["smart_money"] == 2
    assert normalized["age_hours"] == 6
    assert normalized["confirmed_fresh"] is True
    assert normalized["exit_risk"]["level"] == "unknown"
    assert normalized["manipulation"]["level"] == "unknown"


def test_cross_chain_result_exposes_partial_coverage(monkeypatch):
    valid = {"address": "token-sol", "symbol": "SOLTEST", "smart_money": 3,
             "age_hours": 1.0, "is_honeypot": 0}
    monkeypatch.setattr(gmgn, "usable", lambda: True)

    def ranked(chain, **_kwargs):
        if chain == "sol":
            return _rank(chain=chain, rows=[valid])
        return _rank("failed", chain=chain, error_kind="challenge_or_blocked")

    monkeypatch.setattr(gmgn, "smart_money_rank_result", ranked)

    result = gmgn.opportunities_result(chains=("sol", "bsc"))

    assert result["source_health"]["state"] == "partial"
    assert result["source_health"]["successful_chains"] == 1
    assert result["source_health"]["failed_chains"] == 1
    assert [row["address"] for row in result["opportunities"]] == ["token-sol"]


def test_valid_rank_filtered_to_zero_is_a_real_empty_result(monkeypatch):
    below_threshold = {
        "address": "token-sol", "symbol": "QUIET", "smart_money": 1,
        "age_hours": 1.0, "is_honeypot": 0,
    }
    monkeypatch.setattr(gmgn, "usable", lambda: True)
    monkeypatch.setattr(
        gmgn, "smart_money_rank_result",
        lambda chain, **_kwargs: _rank(chain=chain, rows=[below_threshold]),
    )

    result = gmgn.opportunities_result(chains=("sol",), min_smart=2)

    assert result["source_health"]["state"] == "ok"
    assert result["source_health"]["rank_rows"] == 1
    assert result["opportunities"] == []
    assert gmgn.opportunities(chains=("sol",), min_smart=2) == []


def test_all_chain_failures_make_compatibility_view_unavailable(monkeypatch):
    monkeypatch.setattr(gmgn, "usable", lambda: True)
    monkeypatch.setattr(
        gmgn, "smart_money_rank_result",
        lambda chain, **_kwargs: _rank(
            "failed", chain=chain, error_kind="upstream_non_json"),
    )

    assert gmgn.opportunities_result(chains=("sol",))["source_health"]["state"] == "failed"
    assert gmgn.opportunities(chains=("sol",)) is None


def test_board_publishes_partial_health_with_verified_rows(monkeypatch):
    health = {"state": "partial", "error_kind": "chain_or_row_gap",
              "requested_chains": 4, "successful_chains": 3,
              "failed_chains": 1, "chains": []}
    row = {"address": "token-sol", "smart_money": 3, "age_hours": 1,
           "liquidity": 100_000}
    monkeypatch.setattr(
        gmgn, "opportunities_result",
        lambda **_kwargs: {"opportunities": [row], "source_health": health},
    )

    payload = board_export.render_opportunities()

    assert payload["opportunities"][0]["token"] == "token-sol"
    assert payload["source_health"] == health
    assert payload["scan_error"] == "chain_or_row_gap"


@pytest.mark.parametrize("state", ["failed", "partial"])
def test_board_uses_fallback_instead_of_publishing_untrusted_empty(
        monkeypatch, state):
    health = {"state": state, "error_kind": "all_chains_failed",
              "requested_chains": 4, "successful_chains": 0,
              "failed_chains": 4, "chains": []}
    monkeypatch.setattr(
        gmgn, "opportunities_result",
        lambda **_kwargs: {"opportunities": [], "source_health": health},
    )
    monkeypatch.setattr(
        board_export, "_cielo_smart_buys",
        lambda: [{"token": "fallback", "symbol": "SAFE_FALLBACK"}],
    )

    payload = board_export.render_opportunities()

    assert payload["source"] == "Cielo 策展聪明钱名单"
    assert payload["opportunities"][0]["token"] == "fallback"
    assert payload["opportunities"][0]["smart_money"] == 0
    assert payload["opportunities"][0]["exit_risk"]["level"] == "unknown"
    assert payload["opportunities"][0]["manipulation"]["level"] == "unknown"
    assert payload["upstream_source_health"]["gmgn"] == health


def test_board_raises_when_gmgn_and_every_fallback_fail(monkeypatch):
    health = {"state": "failed", "error_kind": "all_chains_failed",
              "requested_chains": 4, "successful_chains": 0,
              "failed_chains": 4, "chains": []}
    monkeypatch.setattr(
        gmgn, "opportunities_result",
        lambda **_kwargs: {"opportunities": [], "source_health": health},
    )
    monkeypatch.setattr(board_export, "_cielo_smart_buys", lambda: None)
    monkeypatch.setattr(
        "src.pipeline.yaobi_finder._gather_young",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("fallback down")),
    )

    with pytest.raises(RuntimeError, match="all opportunity sources failed"):
        board_export.render_opportunities()


def test_board_preserves_last_good_when_legacy_fallback_collapses_to_empty(monkeypatch):
    health = {"state": "failed", "error_kind": "all_chains_failed",
              "requested_chains": 4, "successful_chains": 0,
              "failed_chains": 4, "chains": []}
    monkeypatch.setattr(
        gmgn, "opportunities_result",
        lambda **_kwargs: {"opportunities": [], "source_health": health},
    )
    monkeypatch.setattr(board_export, "_cielo_smart_buys", lambda: [])
    monkeypatch.setattr("src.pipeline.yaobi_finder._gather_young", lambda *_a, **_k: [])

    with pytest.raises(RuntimeError, match="unverified empty"):
        board_export.render_opportunities()


def test_board_preserves_last_good_when_all_convergence_reads_are_unknown(monkeypatch):
    health = {"state": "failed", "error_kind": "all_chains_failed",
              "requested_chains": 4, "successful_chains": 0,
              "failed_chains": 4, "chains": []}
    monkeypatch.setattr(
        gmgn, "opportunities_result",
        lambda **_kwargs: {"opportunities": [], "source_health": health},
    )
    monkeypatch.setattr(board_export, "_cielo_smart_buys", lambda: None)
    monkeypatch.setattr(
        "src.pipeline.yaobi_finder._gather_young",
        lambda *_a, **_k: [{"address": "candidate", "chain": "bsc"}],
    )
    monkeypatch.setattr(
        "src.onchain.smart_money.convergence",
        lambda *_a, **_k: {"available": False, "verdict": "unknown"},
    )

    with pytest.raises(RuntimeError, match="convergence source unavailable"):
        board_export.render_opportunities()


def test_partial_fallback_coverage_is_not_reported_as_complete(monkeypatch):
    health = {"state": "failed", "error_kind": "all_chains_failed",
              "requested_chains": 4, "successful_chains": 0,
              "failed_chains": 4, "chains": []}
    monkeypatch.setattr(
        gmgn, "opportunities_result",
        lambda **_kwargs: {"opportunities": [], "source_health": health},
    )
    monkeypatch.setattr(board_export, "_cielo_smart_buys", lambda: None)
    monkeypatch.setattr(
        "src.pipeline.yaobi_finder._gather_young",
        lambda *_a, **_k: [
            {"address": "unknown", "chain": "bsc"},
            {"address": "verified-none", "chain": "bsc"},
        ],
    )
    monkeypatch.setattr(
        "src.onchain.smart_money.convergence",
        lambda token, *_a, **_k: (
            {"available": False, "verdict": "unknown"}
            if token == "unknown"
            else {"available": True, "skilled_entities": 0, "verdict": "none"}),
    )

    payload = board_export.render_opportunities()

    assert payload["opportunities"] == []
    assert payload["fallback_source_health"]["state"] == "partial"
    assert payload["fallback_source_health"]["observed"] == 1
    assert payload["fallback_source_health"]["failed"] == 1
    assert payload["scan_error"] == "fallback_convergence_partial"


def test_board_discloses_gmgn_source_health_contract():
    html = (Path(__file__).parents[1] / "board" / "public" / "index.html").read_text()

    assert "GMGN 源:" in html
    assert "空白不代表无机会" in html
    assert "GMGN 空结果未被当真" in html
    assert "接盘未知" in html
    assert '["moderate","severe"].includes(r.manipulation?.level)' in html
