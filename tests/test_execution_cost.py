"""Cost contracts must expose unknowns instead of inventing an all-in number."""
from copy import deepcopy

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


def test_solana_launch_full_paper_contract_includes_preregistered_fee_ceiling():
    from src.pipeline.execution_cost import (
        SOLANA_LAUNCH_ROUNDTRIP_NETWORK_FEE_CEILING_USD,
        solana_launch_full_paper_contract,
    )

    contract = solana_launch_full_paper_contract(
        notional_usd=25,
        modeled_route_roundtrip_pct=1.2,
        method="frozen_route_model_v1",
    )
    components = {item["name"]: item for item in contract["components"]}

    assert contract["purpose"] == "discovery_outcome"
    assert contract["measurement_kind"] == "paper_model"
    assert contract["cost_policy"] == "preregistered_full_paper_ceiling"
    assert SOLANA_LAUNCH_ROUNDTRIP_NETWORK_FEE_CEILING_USD == 2.0
    assert contract["network_fee_ceiling_usd"] == (
        SOLANA_LAUNCH_ROUNDTRIP_NETWORK_FEE_CEILING_USD
    )
    assert components["modeled_route_dex_and_impact"]["pct"] == 1.2
    assert components["network_fee"]["policy"] == "preregistered_ceiling"
    assert components["network_fee"]["ceiling_usd"] == 2.0
    assert components["network_fee"]["pct"] == 8.0
    assert contract["known_total_pct"] == 9.2
    assert contract["all_in_total_pct"] == 9.2
    assert contract["completeness"] == "complete"
    assert contract["is_real_fill"] is False


def test_solana_launch_full_paper_contract_supports_explicit_fee_ceiling():
    from src.pipeline.execution_cost import solana_launch_full_paper_contract

    contract = solana_launch_full_paper_contract(
        notional_usd=50,
        modeled_route_roundtrip_pct=0.4,
        method="frozen_route_model_v1",
        network_fee_ceiling_usd=0.75,
    )

    assert contract["components"][1]["pct"] == 1.5
    assert contract["known_total_pct"] == 1.9
    assert contract["all_in_total_pct"] == 1.9


@pytest.mark.parametrize("field,value", [
    ("notional_usd", True),
    ("notional_usd", float("nan")),
    ("notional_usd", float("inf")),
    ("modeled_route_roundtrip_pct", True),
    ("modeled_route_roundtrip_pct", -0.01),
    ("network_fee_ceiling_usd", True),
    ("network_fee_ceiling_usd", float("-inf")),
    ("network_fee_ceiling_usd", -0.01),
])
def test_solana_launch_full_paper_contract_rejects_unsafe_numbers(field, value):
    from src.pipeline.execution_cost import solana_launch_full_paper_contract

    values = {
        "notional_usd": 25,
        "modeled_route_roundtrip_pct": 1.2,
        "network_fee_ceiling_usd": 2.0,
        "method": "frozen_route_model_v1",
    }
    values[field] = value
    with pytest.raises(ValueError, match="finite non-negative|positive notional"):
        solana_launch_full_paper_contract(**values)


def test_full_paper_policy_rejects_fee_and_semantic_field_drift():
    from src.pipeline.execution_cost import (
        solana_launch_full_paper_contract,
        validate,
    )

    contract = solana_launch_full_paper_contract(
        notional_usd=25,
        modeled_route_roundtrip_pct=1.2,
        method="frozen_route_model_v1",
    )
    wrong_ceiling = deepcopy(contract)
    wrong_ceiling["components"][1]["ceiling_usd"] = 1.0
    with pytest.raises(ValueError, match="ceiling fields disagree"):
        validate(wrong_ceiling)

    wrong_pct = deepcopy(contract)
    wrong_pct["components"][1]["pct"] = 7.0
    wrong_pct["known_total_pct"] = wrong_pct["all_in_total_pct"] = 8.2
    with pytest.raises(ValueError, match="USD and pct disagree"):
        validate(wrong_pct)

    wrong_kind = {**contract, "measurement_kind": "real_fill"}
    with pytest.raises(ValueError, match="semantics disagree"):
        validate(wrong_kind)

    missing_method = {**contract, "method": " "}
    with pytest.raises(ValueError, match="requires a method"):
        validate(missing_method)

    wrong_component_policy = deepcopy(contract)
    wrong_component_policy["components"][1]["policy"] = "measured"
    with pytest.raises(ValueError, match="component policy disagrees"):
        validate(wrong_component_policy)


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


@pytest.mark.parametrize("path,value", [
    ("notional", True),
    ("notional", float("nan")),
    ("component", float("inf")),
    ("component", True),
    ("known_total", float("-inf")),
    ("all_in_total", True),
    ("extension_amount", -1),
    ("extension_pct", float("nan")),
])
def test_validate_rejects_nonfinite_boolean_and_negative_cost_fields(path, value):
    from src.pipeline.execution_cost import validate

    contract = {
        "version": 1, "purpose": "current_action", "basis": "round_trip",
        "currency": "USD", "notional_usd": 25, "method": "test",
        "components": [{"name": "route", "pct": 1.0, "status": "included"}],
        "known_total_pct": 1.0, "all_in_total_pct": 1.0,
        "completeness": "complete", "is_real_fill": False,
    }
    if path == "notional":
        contract["notional_usd"] = value
    elif path == "component":
        contract["components"][0]["pct"] = value
    elif path == "known_total":
        contract["known_total_pct"] = value
    elif path == "all_in_total":
        contract["all_in_total_pct"] = value
    elif path == "extension_amount":
        contract["components"][0]["assumption_usd"] = value
    else:
        contract["components"][0]["modeled_proxy_pct"] = value

    with pytest.raises(ValueError, match="finite non-negative|requires"):
        validate(contract)


def test_validate_rejects_non_boolean_fill_claim():
    from src.pipeline.execution_cost import validate

    contract = {
        "version": 1, "purpose": "current_action", "basis": "round_trip",
        "currency": "USD", "notional_usd": 25, "method": "test",
        "components": [{"name": "route", "pct": 1.0, "status": "included"}],
        "known_total_pct": 1.0, "all_in_total_pct": 1.0,
        "completeness": "complete", "is_real_fill": "false",
    }
    with pytest.raises(ValueError, match="is_real_fill must be boolean"):
        validate(contract)


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
