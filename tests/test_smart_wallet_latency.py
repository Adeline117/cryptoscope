from __future__ import annotations

import json
from datetime import datetime, timezone

from src.onchain import gmgn, smart_wallets


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


def test_wallet_sweep_uses_bounded_pool_and_per_request_timeout(monkeypatch):
    captured = {"workers": None, "calls": []}
    now = datetime.now(timezone.utc).timestamp()

    class ImmediateExecutor:
        def __init__(self, *, max_workers, thread_name_prefix):
            captured["workers"] = max_workers
            captured["thread_name_prefix"] = thread_name_prefix

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def map(self, fn, jobs):
            return [fn(job) for job in jobs]

    monkeypatch.setattr(smart_wallets, "ThreadPoolExecutor", ImmediateExecutor)
    monkeypatch.setattr(smart_wallets, "usable", lambda: True)
    monkeypatch.setattr(
        smart_wallets,
        "watchlist",
        lambda chain: [{"wallet": f"{chain}-a"}, {"wallet": f"{chain}-b"}],
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

    result = smart_wallets.fresh_smart_buys(chain_codes=("sol", "bsc"), window_min=45)

    assert captured["workers"] == smart_wallets.SMART_WALLET_WORKERS == 4
    assert captured["thread_name_prefix"] == "smart-wallet"
    assert len(captured["calls"]) == 4
    assert {call[3] for call in captured["calls"]} == {
        smart_wallets.SMART_WALLET_HTTP_TIMEOUT_S,
    }
    assert {(row["chain"], row["n_buyers"]) for row in result} == {
        ("solana", 2),
        ("bsc", 2),
    }
