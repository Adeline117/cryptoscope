from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from src.onchain import gmgn, smart_wallets
from src.pipeline import board_export


def test_flare_request_deadline_stays_inside_http_timeout(monkeypatch):
    captured = {}

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self):
            return json.dumps({
                "solution": {"status": 200, "response": '{"data": {}}'},
            }).encode()

    def fake_urlopen(request, timeout):
        captured["payload"] = json.loads(request.data)
        captured["timeout"] = timeout
        return Response()

    monkeypatch.setattr(gmgn.urllib.request, "urlopen", fake_urlopen)

    assert gmgn._fs_get("https://gmgn.ai/example", timeout=15) == {"data": {}}
    assert captured["timeout"] == 15
    assert captured["payload"]["maxTimeout"] == 14_000


def test_wallet_sweep_rotates_bounded_batch_and_reuses_fresh_cache(monkeypatch, tmp_path):
    captured = {"calls": []}
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0).timestamp()

    monkeypatch.setattr(smart_wallets, "DB", tmp_path / "wallets.db")
    monkeypatch.setattr(smart_wallets, "usable", lambda: True)
    monkeypatch.setattr(
        smart_wallets,
        "watchlist",
        lambda chain: [{"wallet": f"{chain}-{i}"} for i in range(4)],
    )
    def fake_recent(wallet, chain, window_min, request_timeout_s):
        captured["calls"].append((wallet, chain, window_min, request_timeout_s))
        return [{
            "token": "token-1",
            "symbol": "ONE",
            "cost_usd": 25,
            "ts": now,
        }]

    monkeypatch.setattr(smart_wallets, "recent_buys", fake_recent)

    first = smart_wallets.fresh_smart_buys_result(
        chain_codes=("sol", "bsc"), window_min=45, now_ts=now)

    assert len(captured["calls"]) == 3
    assert {call[3] for call in captured["calls"]} == {
        smart_wallets.SMART_WALLET_HTTP_TIMEOUT_S,
    }
    assert first["source_health"]["configured_wallets"] == 8
    assert first["source_health"]["requested"] == 3
    assert first["source_health"]["fresh_cached_wallets"] == 3
    assert first["source_health"]["state"] == "partial"

    now += 15 * 60
    second = smart_wallets.fresh_smart_buys_result(
        chain_codes=("sol", "bsc"), window_min=45, now_ts=now)
    assert second["source_health"]["fresh_cached_wallets"] == 6

    now += 15 * 60
    third = smart_wallets.fresh_smart_buys_result(
        chain_codes=("sol", "bsc"), window_min=45, now_ts=now)
    assert third["source_health"]["fresh_cached_wallets"] == 8
    assert third["source_health"]["state"] == "ok"
    assert {(row["chain"], row["n_buyers"]) for row in third["buys"]} == {
        ("solana", 4),
        ("bsc", 4),
    }


def test_watch_export_exposes_failed_source_health(monkeypatch):
    health = {
        "state": "failed",
        "error_kind": "source_unavailable",
        "configured_wallets": 25,
        "requested": 9,
        "observed": 0,
        "request_failed": 9,
        "fresh_cached_wallets": 0,
    }
    monkeypatch.setattr(
        smart_wallets,
        "fresh_smart_buys_result",
        lambda **_kwargs: {"buys": [], "source_health": health},
    )

    payload = board_export.render_watch()

    assert payload["source_health"] == health
    assert payload["scan_error"] == "source_unavailable"
    assert payload["watched_wallets"] == 25


def test_board_discloses_wallet_rotation_and_source_failure_semantics():
    html = (Path(__file__).parents[1] / "board" / "public" / "index.html").read_text()

    assert "45分钟新鲜缓存" in html
    assert "源不可用，空列表不代表没有买入" in html
    assert "源失败或缓存覆盖不足绝不解释成没有活动" in html
