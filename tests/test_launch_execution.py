"""Launch candidates fail closed unless safety and round-trip routing are verified."""
from __future__ import annotations

from datetime import datetime, timezone


def _event(**overrides):
    event = {"lane": "launch", "chain": "solana", "token": "Mint", "symbol": "T",
             "decision": "SMALL_PROBE", "max_notional_usd": 60.0,
             "roundtrip_cost_pct_est": 1.8, "reasons": []}
    event.update(overrides)
    return event


def _solana_row(**overrides):
    row = {
        "mintable": {"status": "0"}, "freezable": {"status": "0"},
        "balance_mutable_authority": {"status": "0"}, "closable": {"status": "0"},
        "non_transferable": "0", "transfer_hook": [], "transfer_fee": {},
        "transfer_hook_upgradable": {"status": "0"},
        "transfer_fee_upgradable": {"status": "0"},
        "default_account_state_upgradable": {"status": "0"},
    }
    row.update(overrides)
    return row


def test_solana_security_requires_complete_clean_fields():
    from src.pipeline import launch_execution as le

    def clean(url, params, headers):
        return {"code": 1, "result": {"Mint": _solana_row()}}

    assert le.security_probe(_event(), fetch=clean)["state"] == "pass"

    def incomplete(url, params, headers):
        row = _solana_row()
        row.pop("freezable")
        return {"code": 1, "result": {"Mint": row}}

    got = le.security_probe(_event(), fetch=incomplete)
    assert got["state"] == "unknown" and "freezable" in got["unknown_fields"]


def test_solana_mint_or_freeze_authority_is_avoid():
    from src.pipeline import launch_execution as le

    def risky(url, params, headers):
        return {"code": 1, "result": {"Mint": _solana_row(
            mintable={"status": "1"}, freezable={"status": "1"})}}

    got = le.security_probe(_event(), fetch=risky)
    assert got["state"] == "avoid"
    assert set(got["hard_flags"]) == {"mintable", "freezable"}


def test_gate_downgrades_unknown_and_blocks_known_untradeable():
    from src.pipeline.launch_execution import gate

    watch = gate(_event(), {"state": "pass"}, {"state": "unknown"})
    assert watch["decision"] == "WATCH"
    avoid = gate(_event(), {"state": "pass"}, {"state": "untradeable"})
    assert avoid["decision"] == "AVOID"
    risky = gate(_event(), {"state": "avoid"}, {"state": "skipped"})
    assert risky["decision"] == "AVOID"


def test_jupiter_roundtrip_quote_replaces_modeled_cost():
    from src.pipeline import launch_execution as le

    requests = []

    def quotes(url, params, headers):
        requests.append((url, params, headers))
        if params["inputMint"] == le.JUPITER_USDC:
            return {"outAmount": "1000000000", "priceImpact": "-0.004",
                    "routePlan": [{"swapInfo": {"label": "Raydium"}}]}
        return {"outAmount": "58800000", "priceImpact": "-0.006",
                "routePlan": [{"swapInfo": {"label": "Meteora"}}]}

    route = le._jupiter_route(_event(), "key", quotes)
    assert route["state"] == "quoted"
    assert route["roundtrip_loss_pct"] == 2.0
    assert route["buy_price_impact_pct"] == 0.4
    assert route["sell_price_impact_pct"] == 0.6
    assert all(url == le.JUPITER_ORDER for url, _, _ in requests)
    assert all("restrictIntermediateTokens" not in params and
               "instructionVersion" not in params for _, params, _ in requests)
    assert all(headers == {"x-api-key": "key"} for _, _, headers in requests)
    event = le.gate(_event(), {"state": "pass"}, route)
    assert event["decision"] == "SMALL_PROBE"
    assert event["roundtrip_cost_pct_est"] == 2.0
    assert event["cost_model"].startswith("live_read_only_roundtrip_quote")
    assert event["execution_probe"]["is_real_fill"] is False
    assert event["quote_at"] == route["checked_at"]
    assert event["expires_at"] > event["quote_at"]
    assert event["executable_at"] is None


def test_jupiter_quote_uses_slippage_threshold_not_optimistic_output():
    from src.pipeline import launch_execution as le

    def quotes(url, params, headers):
        if params["inputMint"] == le.JUPITER_USDC:
            return {"outAmount": "1000000000", "otherAmountThreshold": "990000000",
                    "routePlan": [{"swapInfo": {"label": "Buy"}}]}
        return {"outAmount": "60000000", "otherAmountThreshold": "58800000",
                "routePlan": [{"swapInfo": {"label": "Sell"}}]}

    got = le._jupiter_route(_event(), "key", quotes)

    assert got["roundtrip_loss_pct"] == 2.0
    assert got["roundtrip_back_usd"] == 58.8


def test_nominal_quoted_route_without_a_valid_quote_clock_is_watch_only():
    from src.pipeline.launch_execution import gate

    event = gate(
        _event(),
        {"state": "pass"},
        {"state": "quoted", "roundtrip_loss_pct": 1.0,
         "checked_at": "not-a-clock", "is_real_fill": False},
    )

    assert event["decision"] == "WATCH"
    assert event["quote_at"] is None and event["expires_at"] is None
    assert event["executable_at"] is None


def test_jupiter_excessive_roundtrip_loss_is_untradeable():
    from src.pipeline import launch_execution as le

    def quotes(url, params, headers):
        out = "1000000000" if params["inputMint"] == le.JUPITER_USDC else "48000000"
        return {"outAmount": out, "routePlan": [{"swapInfo": {"label": "AMM"}}]}

    got = le._jupiter_route(_event(), "key", quotes)
    assert got["state"] == "untradeable"
    assert got["roundtrip_loss_pct"] == 20.0


def test_missing_router_key_uses_labelled_keyless_quote_fallback(monkeypatch):
    from src.pipeline import launch_execution as le

    monkeypatch.delenv("JUPITER_API_KEY", raising=False)
    calls = []

    def quotes(url, params, headers):
        calls.append((url, headers))
        if params["inputMint"] == le.JUPITER_USDC:
            return {"outAmount": "1000000000",
                    "routePlan": [{"swapInfo": {"label": "Buy"}}]}
        return {"outAmount": "58800000",
                "routePlan": [{"swapInfo": {"label": "Sell"}}]}

    got = le.route_probe(_event(), fetch=quotes)

    assert got["state"] == "quoted"
    assert got["api_mode"] == "keyless_lite_fallback"
    assert "keyless fallback" in got["source"]
    assert calls == [(le.JUPITER_LITE_QUOTE, None),
                     (le.JUPITER_LITE_QUOTE, None)]


def test_jupiter_transport_failure_is_unknown_not_a_fake_no_route():
    from src.pipeline import launch_execution as le

    def down(*_args, **_kwargs):
        raise TimeoutError("gateway timeout")

    got = le._jupiter_route(_event(), "key", down)

    assert got["state"] == "unknown"
    assert "quote unavailable" in got["reason"]


def test_scan_assessment_failure_persists_watch_not_probe(tmp_path, monkeypatch):
    from src.pipeline import launch_radar as lr
    from src.pipeline import opportunity_ledger as ol

    monkeypatch.setattr(ol, "DB", tmp_path / "ledger.db")
    now = datetime(2026, 7, 14, 12, tzinfo=timezone.utc)
    profile = {"chainId": "solana", "tokenAddress": "Mint"}
    pair = {"chainId": "solana", "pairAddress": "pool", "priceUsd": "0.001",
            "pairCreatedAt": int(now.timestamp() * 1000), "fdv": 100_000,
            "liquidity": {"usd": 20_000}, "volume": {"m5": 1_000},
            "txns": {"m5": {"buys": 10, "sells": 2}},
            "baseToken": {"address": "Mint", "symbol": "T"}}

    def fetch(url):
        return [profile] if url == lr.PROFILES_URL else [pair]

    got = lr.scan(fetch=fetch, now=now, assessor=lambda event: (_ for _ in ()).throw(
        RuntimeError("security down")))
    assert got["assessed"] == 1
    row = ol.active("launch")[0]
    assert row["decision"] == "WATCH"
    assert row["security_gate"]["state"] == "unknown"
