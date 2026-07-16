"""Launch candidates fail closed unless safety and round-trip routing are verified."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest


def _event(**overrides):
    event = {"lane": "launch", "chain": "solana", "token": "Mint", "symbol": "T",
             "decision": "SMALL_PROBE", "max_notional_usd": 60.0,
             "roundtrip_cost_pct_est": 1.8, "reasons": []}
    event.update(overrides)
    return event


def _v2_quote(input_mint, output_mint, amount, threshold, *, impact, label):
    return {
        "inputMint": input_mint, "outputMint": output_mint,
        "inAmount": str(amount), "outAmount": str(threshold),
        "otherAmountThreshold": str(threshold), "swapMode": "ExactIn",
        "slippageBps": 100, "priceImpact": impact, "transaction": None,
        "routePlan": [{"swapInfo": {
            "label": label, "inputMint": input_mint,
            "outputMint": output_mint, "inAmount": str(amount),
            "outAmount": str(threshold),
        }}],
    }


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


def _local_authority(**overrides):
    row = {
        "state": "pass", "source": "Solana finalized getAccountInfo",
        "checked_at": "2026-07-15T12:00:00+00:00", "slot": 123,
        "owner": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
        "raw_hash": "a" * 64, "mint_authority": None, "freeze_authority": None,
        "hard_flags": [], "cautions": [],
    }
    row.update(overrides)
    return row


def _evm_risk(**overrides):
    row = {
        "available": True, "checked_at": "2026-07-15T12:00:00+00:00",
        "is_open_source": 1, "owner_renounced": True, "lp_all_locked": True,
        "facts": [], "unknowns": [],
        "flags": {
            "is_honeypot": 0, "is_mintable": 0, "transfer_pausable": 0,
            "owner_change_balance": 0, "hidden_owner": 0,
            "can_take_back_ownership": 0, "is_blacklisted": 0,
            "trading_cooldown": 0, "cannot_sell_all": 0, "is_proxy": 0,
            "buy_tax": 0.0, "sell_tax": 0.0,
        },
    }
    for key, value in overrides.items():
        if key == "flags":
            row["flags"] = {**row["flags"], **value}
        else:
            row[key] = value
    return row


def test_solana_security_requires_complete_clean_fields():
    from src.pipeline import launch_execution as le

    def clean(url, params, headers):
        return {"code": 1, "result": {"Mint": _solana_row()}}

    assert le.security_probe(
        _event(), fetch=clean, authority_probe=lambda _mint: _local_authority()
    )["state"] == "pass"

    def incomplete(url, params, headers):
        row = _solana_row()
        row.pop("freezable")
        return {"code": 1, "result": {"Mint": row}}

    got = le.security_probe(
        _event(), fetch=incomplete, authority_probe=lambda _mint: _local_authority())
    assert got["state"] == "unknown" and "freezable" in got["unknown_fields"]


def test_solana_mint_or_freeze_authority_is_avoid():
    from src.pipeline import launch_execution as le

    def risky(url, params, headers):
        return {"code": 1, "result": {"Mint": _solana_row(
            mintable={"status": "1"}, freezable={"status": "1"})}}

    got = le.security_probe(
        _event(), fetch=risky, authority_probe=lambda _mint: _local_authority())
    assert got["state"] == "avoid"
    assert set(got["hard_flags"]) == {"mintable", "freezable"}


@pytest.mark.parametrize("local_state", ["caution", "unknown"])
def test_solana_local_non_pass_evidence_blocks_router(local_state):
    from src.pipeline import launch_execution as le

    calls = []

    def fetch(url, params, headers):
        calls.append(url)
        return {"code": 1, "result": {"Mint": _solana_row()}}

    local = _local_authority(
        state=local_state,
        reason="local authority evidence is not clean",
        cautions=["token_2022_extensions_not_fully_parsed"]
        if local_state == "caution" else [],
    )
    got = le.assess(_event(), fetch=fetch,
                    authority_probe=lambda _mint: local)

    assert got["security_gate"]["state"] == local_state
    assert got["decision"] == "WATCH"
    assert got["execution_probe"]["state"] == "skipped"
    assert calls == ["https://api.gopluslabs.io/api/v1/solana/token_security"]


def test_solana_local_avoid_outranks_unknown_provider():
    from src.pipeline import launch_execution as le

    def unindexed(url, params, headers):
        return {"code": 1, "result": {}}

    got = le.security_probe(
        _event(), fetch=unindexed,
        authority_probe=lambda _mint: _local_authority(
            state="avoid", hard_flags=["mint_authority"],
            mint_authority="Authority", reason="live mint authority"),
    )

    assert got["state"] == "avoid"
    assert got["providers"]["goplus"]["state"] == "unknown"
    assert got["providers"]["solana_rpc"]["state"] == "avoid"
    assert got["hard_flags"] == ["mint_authority"]


def test_solana_combined_pass_requires_both_evidence_clocks():
    from src.pipeline import launch_execution as le

    def clean(url, params, headers):
        return {"code": 1, "result": {"Mint": _solana_row()}}

    local = _local_authority()
    local.pop("checked_at")
    got = le.security_probe(
        _event(), fetch=clean, authority_probe=lambda _mint: local)

    assert got["state"] == "unknown"
    assert got["checked_at"] is None
    assert "clock" in got["reason"]


def test_evm_complete_clean_evidence_passes(monkeypatch):
    from src.pipeline import launch_execution as le

    monkeypatch.setattr("src.onchain.goplus_client.rug_risk",
                        lambda *_args: _evm_risk())

    got = le.security_probe(_event(chain="base", token="0xabc"))

    assert got["state"] == "pass"


def test_evm_missing_critical_flag_is_unknown_and_skips_route(monkeypatch):
    from src.pipeline import launch_execution as le

    risk = _evm_risk()
    risk["flags"].pop("is_honeypot")
    monkeypatch.setattr("src.onchain.goplus_client.rug_risk",
                        lambda *_args: risk)
    route_calls = []

    got = le.assess(
        _event(chain="base", token="0xabc"),
        fetch=lambda *_args, **_kwargs: route_calls.append(True),
    )

    assert got["decision"] == "WATCH"
    assert got["security_gate"]["state"] == "unknown"
    assert "is_honeypot" in got["security_gate"]["unknown_fields"]
    assert got["execution_probe"]["state"] == "skipped"
    assert route_calls == []


@pytest.mark.parametrize("tax", ["buy_tax", "sell_tax"])
def test_evm_missing_tax_is_unknown(monkeypatch, tax):
    from src.pipeline import launch_execution as le

    risk = _evm_risk()
    risk["flags"].pop(tax)
    monkeypatch.setattr("src.onchain.goplus_client.rug_risk",
                        lambda *_args: risk)

    got = le.security_probe(_event(chain="base", token="0xabc"))

    assert got["state"] == "unknown"
    assert tax in got["unknown_fields"]


def test_evm_trading_cooldown_is_hard_block(monkeypatch):
    from src.pipeline import launch_execution as le

    monkeypatch.setattr("src.onchain.goplus_client.rug_risk",
                        lambda *_args: _evm_risk(flags={"trading_cooldown": 1}))

    got = le.security_probe(_event(chain="base", token="0xabc"))

    assert got["state"] == "avoid"
    assert "trading_cooldown" in got["hard_flags"]


def test_evm_existing_proxy_and_lp_cautions_remain_non_passing(monkeypatch):
    from src.pipeline import launch_execution as le

    monkeypatch.setattr("src.onchain.goplus_client.rug_risk", lambda *_args: _evm_risk(
        flags={"is_proxy": 1}, lp_all_locked=False))

    got = le.security_probe(_event(chain="base", token="0xabc"))

    assert got["state"] == "caution"
    assert set(got["cautions"]) == {"upgradeable_proxy", "lp_lock_not_verified"}


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
        if url == le.JUPITER_PRICE:
            return {"Mint": {"decimals": 6, "usdPrice": 0.06, "blockId": 123}}
        if params["inputMint"] == le.JUPITER_USDC:
            return _v2_quote(
                le.JUPITER_USDC, "Mint", params["amount"], 1_000_000_000,
                impact="-0.004", label="Raydium",
            )
        return _v2_quote(
            "Mint", le.JUPITER_USDC, params["amount"], 58_800_000,
            impact="-0.006", label="Meteora",
        )

    route = le._jupiter_route(_event(), "key", quotes)
    assert route["state"] == "quoted"
    assert route["roundtrip_loss_pct"] == 2.0
    assert route["buy_price_impact_pct"] == 0.4
    assert route["sell_price_impact_pct"] == 0.6
    assert route["promotion_eligible"] is True
    assert route["quote_contract_verified"] is True
    assert route["provider_contract"]["endpoint"] == le.JUPITER_ORDER
    assert [url for url, _, _ in requests] == [
        le.JUPITER_ORDER, le.JUPITER_PRICE, le.JUPITER_ORDER]
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
    assert route["entry_reference_price"] == 0.06
    assert route["invalidation_reference_price"] == 0.042
    assert route["token_decimals"] == 6


def test_jupiter_quote_uses_slippage_threshold_not_optimistic_output():
    from src.pipeline import launch_execution as le

    def quotes(url, params, headers):
        if url == le.JUPITER_PRICE:
            return {"Mint": {"decimals": 6}}
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
        if url == le.JUPITER_PRICE:
            return {"Mint": {"decimals": 6}}
        out = "1000000000" if params["inputMint"] == le.JUPITER_USDC else "48000000"
        return {"outAmount": out, "routePlan": [{"swapInfo": {"label": "AMM"}}]}

    got = le._jupiter_route(_event(), "key", quotes)
    assert got["state"] == "untradeable"
    assert got["roundtrip_loss_pct"] == 20.0


def test_missing_router_key_uses_v2_keyless_diagnostic_without_promotion(monkeypatch):
    from src.pipeline import launch_execution as le

    monkeypatch.delenv("JUPITER_API_KEY", raising=False)
    calls = []

    def quotes(url, params, headers):
        calls.append((url, headers))
        if url == le.JUPITER_PRICE:
            return {"Mint": {"decimals": 6}}
        if params["inputMint"] == le.JUPITER_USDC:
            return {"outAmount": "1000000000",
                    "routePlan": [{"swapInfo": {"label": "Buy"}}]}
        return {"outAmount": "58800000",
                "routePlan": [{"swapInfo": {"label": "Sell"}}]}

    got = le.route_probe(_event(), fetch=quotes)

    assert got["state"] == "quoted"
    assert got["api_mode"] == "keyless_v2_diagnostic"
    assert got["promotion_eligible"] is False
    assert "keyless diagnostic" in got["source"]
    assert calls == [(le.JUPITER_ORDER, None),
                     (le.JUPITER_PRICE, None),
                     (le.JUPITER_ORDER, None)]


def test_missing_token_decimals_keeps_route_but_blocks_standardized_entry_price():
    from src.pipeline import launch_execution as le

    def quotes(url, params, headers):
        if url == le.JUPITER_PRICE:
            return {}
        if params["inputMint"] == le.JUPITER_USDC:
            return {"outAmount": "1000000000",
                    "routePlan": [{"swapInfo": {"label": "Buy"}}]}
        return {"outAmount": "58800000",
                "routePlan": [{"swapInfo": {"label": "Sell"}}]}

    got = le._jupiter_route(_event(), "key", quotes)
    assert got["state"] == "quoted"
    assert got["entry_reference_price"] is None
    assert "decimals unavailable" in got["price_reference_reason"]


def test_jupiter_transport_failure_is_unknown_not_a_fake_no_route():
    from src.pipeline import launch_execution as le

    def down(*_args, **_kwargs):
        raise TimeoutError("gateway timeout")

    got = le._jupiter_route(_event(), "key", down)

    assert got["state"] == "unknown"
    assert "quote unavailable" in got["reason"]


def test_scan_assessment_failure_appends_unknown_without_relabeling_discovery(tmp_path, monkeypatch):
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

    got = lr.scan(fetch=fetch, now=now, max_primary=0, max_evm=0,
                  evidence_clock=lambda: now,
                  assessor=lambda event: (_ for _ in ()).throw(
                      RuntimeError("security down")))
    assert got["assessed"] == 1
    row = ol.active("launch", now=now)[0]
    assert row["decision"] == "SMALL_PROBE"
    assert row["action_level"] == "A1_WATCH"
    assert row["current_assessment"]["security_state"] == "unknown"


def test_current_route_quote_cannot_rewrite_discovery_price_or_cost(tmp_path, monkeypatch):
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

    def assessor(event):
        event["entry_price"] = 99
        event["roundtrip_cost_pct_est"] = 2.5
        event["security_gate"] = {"state": "pass", "checked_at": now.isoformat()}
        event["execution_probe"] = {
            "state": "quoted", "source": "test router", "api_mode": "test",
            "roundtrip_loss_pct": 2.5, "roundtrip_back_usd": 58.5,
            "checked_at": now.isoformat(), "is_real_fill": False,
        }
        event["quote_at"] = now.isoformat()
        event["expires_at"] = (now + timedelta(seconds=60)).isoformat()
        return event

    lr.scan(
        fetch=fetch, now=now, max_primary=0, max_evm=0, assessor=assessor,
        evidence_clock=lambda: now,
    )
    discovery = ol.outcome_rows()[0]
    latest = ol.latest_execution_assessment(discovery["id"])
    assert discovery["entry_price"] == 0.001
    from src.pipeline.edge_validation import LAUNCH_COST_METHOD
    assert discovery["cost_contract"]["method"] == LAUNCH_COST_METHOD
    assert discovery["cost_contract"]["completeness"] == "complete"
    assert discovery["cost_contract"]["network_fee_ceiling_usd"] == 2.0
    assert latest["cost_contract"]["known_total_pct"] == 2.5
    assert latest["entry_reference_price"] is None
