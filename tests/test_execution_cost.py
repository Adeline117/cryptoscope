"""Cost contracts must expose unknowns instead of inventing an all-in number."""
import pytest


def test_discovery_contract_is_comparable_but_explicitly_partial():
    from src.pipeline.execution_cost import discovery_contract

    contract = discovery_contract(notional_usd=60, modeled_roundtrip_pct=1.25,
                                  method="constant_product_v1")
    assert contract["known_total_pct"] == 1.25
    assert contract["all_in_total_pct"] is None
    assert contract["completeness"] == "partial"
    assert contract["components"][1] == {
        "name": "network_fee", "pct": None, "status": "unknown"}


def test_route_contract_does_not_double_count_dex_fee_inside_route_loss():
    from src.pipeline.execution_cost import route_contract

    contract = route_contract(notional_usd=25, route_loss_pct=1.98,
                              method="jupiter_worst_threshold_roundtrip")
    assert contract["known_total_pct"] == 1.98
    assert contract["components"][0]["includes"] == [
        "price_impact", "dex_fee", "configured_slippage"]
    assert contract["all_in_total_pct"] is None


def test_complete_route_contract_requires_real_numeric_network_cost():
    from src.pipeline.execution_cost import route_contract

    contract = route_contract(notional_usd=25, route_loss_pct=1.98,
                              network_fee_pct=0.02, method="measured")
    assert contract["completeness"] == "complete"
    assert contract["all_in_total_pct"] == 2.0


def test_cost_contract_rejects_mismatched_totals_and_filled_unknowns():
    from src.pipeline.execution_cost import validate

    base = {"version": 1, "purpose": "current_action", "basis": "round_trip",
            "currency": "USD", "notional_usd": 25, "method": "test",
            "components": [{"name": "route", "pct": 1.0, "status": "included"}],
            "known_total_pct": 2.0, "all_in_total_pct": 1.0,
            "completeness": "complete", "is_real_fill": False}
    with pytest.raises(ValueError, match="known_total_pct"):
        validate(base)
    with pytest.raises(ValueError, match="null pct"):
        validate({**base, "known_total_pct": 1.0, "all_in_total_pct": None,
                  "completeness": "partial",
                  "components": [{"name": "network", "pct": 0.0,
                                  "status": "unknown"}]})
