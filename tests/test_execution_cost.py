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


def test_unknown_route_contract_invents_no_zero_cost_fill():
    from src.pipeline.execution_cost import unknown_route_contract

    contract = unknown_route_contract(notional_usd=25, method="route_unavailable")
    assert contract["known_total_pct"] == 0
    assert contract["all_in_total_pct"] is None
    assert all(component["status"] == "unknown" for component in contract["components"])


def test_carry_paper_contract_never_upgrades_book_quotes_to_all_in_cost():
    from src.pipeline.execution_cost import carry_paper_contract

    opened = carry_paper_contract(
        notional_usd_per_leg=10_000,
        entry_book_impact_pct=0.05,
        exit_book_impact_pct=None,
        modeled_fee_pct=0.19,
    )
    assert opened["purpose"] == "paper_measurement"
    assert opened["notional_basis"] == "per_leg"
    assert opened["measurement_gross_notional_usd"] == 20_000
    assert opened["known_total_pct"] == 0.05
    assert opened["modeled_proxy_total_pct"] is None
    assert opened["book_quote_cost_complete"] is False
    assert opened["completeness"] == "partial"
    assert opened["all_in_total_pct"] is None
    assert opened["is_real_fill"] is False

    closed = carry_paper_contract(
        notional_usd_per_leg=10_000,
        entry_book_impact_pct=0.05,
        exit_book_impact_pct=0.08,
        modeled_fee_pct=0.19,
    )
    components = {item["name"]: item for item in closed["components"]}
    assert closed["known_total_pct"] == 0.13
    assert closed["modeled_proxy_total_pct"] == 0.32
    assert closed["book_quote_cost_complete"] is True
    assert closed["completeness"] == "partial"
    assert closed["all_in_total_pct"] is None
    assert components["venue_fee_tier"]["status"] == "unknown"
    assert components["venue_fee_tier"]["modeled_proxy_pct"] == 0.19
    assert components["cross_venue_basis"]["status"] == "unknown"
    assert components["collateral_opportunity_or_borrow"]["status"] == "unknown"
    assert components["margin_transfer_and_rebalance"]["status"] == "unknown"
