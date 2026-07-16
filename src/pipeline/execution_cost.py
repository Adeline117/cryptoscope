"""Versioned, auditable cost contracts shared by discovery and live assessments."""
from __future__ import annotations

from math import isclose, isfinite


CONTRACT_VERSION = 1
PURPOSES = {"discovery_outcome", "current_action", "paper_measurement"}
STATUSES = {"included", "unknown", "excluded", "not_applicable"}
SOLANA_LAUNCH_ROUNDTRIP_NETWORK_FEE_CEILING_USD = 2.0


def _finite_nonnegative(value: object, *, field: str) -> float:
    """Parse a cost number without accepting booleans, NaN or infinity."""
    if isinstance(value, bool):
        raise ValueError(f"{field} must be a finite non-negative number")
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{field} must be a finite non-negative number") from exc
    if not isfinite(number) or number < 0:
        raise ValueError(f"{field} must be a finite non-negative number")
    return number


def _validate_cost_numbers(value: object, *, path: str = "cost_contract") -> None:
    """Fail closed on every monetary/percentage field, including extensions."""
    if isinstance(value, dict):
        for key, nested in value.items():
            field = f"{path}.{key}"
            if key == "pct" or key.endswith("_pct") or key.endswith("_usd"):
                if nested is not None:
                    _finite_nonnegative(nested, field=field)
            else:
                _validate_cost_numbers(nested, path=field)
    elif isinstance(value, list):
        for index, nested in enumerate(value):
            _validate_cost_numbers(nested, path=f"{path}[{index}]")


def validate(contract: dict) -> dict:
    """Validate and return a normalized cost contract without filling unknowns."""
    if not isinstance(contract, dict) or contract.get("version") != CONTRACT_VERSION:
        raise ValueError("unsupported cost contract version")
    _validate_cost_numbers(contract)
    if contract.get("purpose") not in PURPOSES:
        raise ValueError("invalid cost contract purpose")
    if contract.get("basis") != "round_trip" or contract.get("currency") != "USD":
        raise ValueError("cost contract must use round-trip USD basis")
    try:
        notional = _finite_nonnegative(
            contract["notional_usd"], field="cost_contract.notional_usd"
        )
    except KeyError as exc:
        raise ValueError("cost contract requires positive notional_usd") from exc
    if notional == 0:
        raise ValueError("cost contract requires positive notional_usd")
    components = contract.get("components")
    if not isinstance(components, list) or not components:
        raise ValueError("cost contract requires components")
    known = 0.0
    incomplete = False
    names = set()
    normalized_components = []
    for component in components:
        if not isinstance(component, dict) or not component.get("name"):
            raise ValueError("cost component requires a name")
        name, status = str(component["name"]), component.get("status")
        if name in names:
            raise ValueError(f"duplicate cost component: {name}")
        names.add(name)
        if status not in STATUSES:
            raise ValueError(f"invalid cost component status: {status}")
        value = component.get("pct")
        if status == "included":
            try:
                value = _finite_nonnegative(
                    value, field=f"cost component {name} pct"
                )
            except ValueError as exc:
                raise ValueError(f"included cost component {name} needs pct") from exc
            known += value
        elif value is not None:
            raise ValueError(f"non-included cost component {name} must have null pct")
        if status in {"unknown", "excluded"}:
            incomplete = True
        normalized_components.append({**component, "name": name, "pct": value})
    supplied_known = contract.get("known_total_pct")
    try:
        supplied_known = _finite_nonnegative(
            supplied_known, field="cost_contract.known_total_pct"
        )
    except ValueError as exc:
        raise ValueError("cost contract requires known_total_pct") from exc
    if not isclose(supplied_known, known, rel_tol=0.0, abs_tol=1e-6):
        raise ValueError("known_total_pct does not equal included components")
    expected_completeness = "partial" if incomplete else "complete"
    if contract.get("completeness") != expected_completeness:
        raise ValueError("cost completeness disagrees with components")
    all_in = contract.get("all_in_total_pct")
    if incomplete and all_in is not None:
        raise ValueError("partial cost contract cannot claim all-in total")
    if not incomplete:
        try:
            all_in = _finite_nonnegative(
                all_in, field="cost_contract.all_in_total_pct"
            )
        except ValueError as exc:
            raise ValueError("complete cost contract requires all-in total") from exc
        if not isclose(all_in, known, rel_tol=0.0, abs_tol=1e-6):
            raise ValueError("all_in_total_pct does not equal included components")
    if "is_real_fill" in contract and not isinstance(contract["is_real_fill"], bool):
        raise ValueError("is_real_fill must be boolean")
    is_real_fill = contract.get("is_real_fill", False)

    if contract.get("cost_policy") == "preregistered_full_paper_ceiling":
        expected_names = {
            "modeled_route_dex_and_impact",
            "network_fee",
        }
        indexed = {component["name"]: component for component in normalized_components}
        if set(indexed) != expected_names:
            raise ValueError("full paper ceiling components disagree with policy")
        if (contract.get("purpose") != "discovery_outcome"
                or contract.get("measurement_kind") != "paper_model"
                or expected_completeness != "complete"
                or is_real_fill is not False):
            raise ValueError("full paper ceiling contract semantics disagree with policy")
        if (not isinstance(contract.get("method"), str)
                or not contract["method"].strip()):
            raise ValueError("full paper ceiling contract requires a method")
        if indexed["network_fee"].get("policy") != "preregistered_ceiling":
            raise ValueError("network fee component policy disagrees")
        ceiling = _finite_nonnegative(
            contract.get("network_fee_ceiling_usd"),
            field="cost_contract.network_fee_ceiling_usd",
        )
        component_ceiling = _finite_nonnegative(
            indexed["network_fee"].get("ceiling_usd"),
            field="network_fee.ceiling_usd",
        )
        if not isclose(ceiling, component_ceiling, rel_tol=0.0, abs_tol=1e-9):
            raise ValueError("network fee ceiling fields disagree")
        expected_fee_pct = ceiling / notional * 100.0
        if not isclose(
                indexed["network_fee"]["pct"],
                expected_fee_pct, rel_tol=0.0, abs_tol=1e-6):
            raise ValueError("network fee ceiling USD and pct disagree")
    return {**contract, "notional_usd": notional,
            "components": normalized_components,
            "known_total_pct": round(known, 6),
            "all_in_total_pct": round(all_in, 6) if all_in is not None else None,
            "completeness": expected_completeness,
            "is_real_fill": is_real_fill}


def discovery_contract(*, notional_usd: float, modeled_roundtrip_pct: float,
                       method: str) -> dict:
    """Freeze one comparable paper-cost method for both probe and control rows."""
    return validate({
        "version": CONTRACT_VERSION, "purpose": "discovery_outcome",
        "basis": "round_trip", "currency": "USD", "notional_usd": notional_usd,
        "method": method,
        "components": [
            {"name": "modeled_route_dex_and_impact", "pct": modeled_roundtrip_pct,
             "status": "included"},
            {"name": "network_fee", "pct": None, "status": "unknown"},
        ],
        "known_total_pct": modeled_roundtrip_pct, "all_in_total_pct": None,
        "completeness": "partial", "is_real_fill": False,
    })


def solana_launch_full_paper_contract(
        *, notional_usd: float, modeled_route_roundtrip_pct: float, method: str,
        network_fee_ceiling_usd: float = (
            SOLANA_LAUNCH_ROUNDTRIP_NETWORK_FEE_CEILING_USD)) -> dict:
    """Freeze a conservative, complete paper-cost policy for Solana Launch.

    The $2 default is a deliberately conservative, pre-registered scenario
    ceiling. It is not a Solana fee fact, quote, or measured fill. Converting
    the scenario to a percentage at the frozen notional makes the route and fee
    components additive while keeping their assumptions auditable.
    """
    notional = _finite_nonnegative(notional_usd, field="notional_usd")
    if notional == 0:
        raise ValueError("cost contract requires positive notional_usd")
    route_pct = _finite_nonnegative(
        modeled_route_roundtrip_pct, field="modeled_route_roundtrip_pct"
    )
    ceiling_usd = _finite_nonnegative(
        network_fee_ceiling_usd, field="network_fee_ceiling_usd"
    )
    network_fee_pct = round(ceiling_usd / notional * 100.0, 6)
    total_pct = round(route_pct + network_fee_pct, 6)
    return validate({
        "version": CONTRACT_VERSION, "purpose": "discovery_outcome",
        "basis": "round_trip", "currency": "USD", "notional_usd": notional,
        "method": method, "measurement_kind": "paper_model",
        "cost_policy": "preregistered_full_paper_ceiling",
        "network_fee_ceiling_usd": ceiling_usd,
        "components": [
            {"name": "modeled_route_dex_and_impact", "pct": route_pct,
             "status": "included", "measurement_kind": "paper_model"},
            {"name": "network_fee", "pct": network_fee_pct,
             "status": "included", "ceiling_usd": ceiling_usd,
             "policy": "preregistered_ceiling",
             "measurement_kind": "paper_model", "is_ceiling": True},
        ],
        "known_total_pct": total_pct, "all_in_total_pct": total_pct,
        "completeness": "complete", "is_real_fill": False,
    })


def route_contract(*, notional_usd: float, route_loss_pct: float,
                   method: str, network_fee_pct: float | None = None) -> dict:
    """Describe a current round-trip route without double-counting DEX fees."""
    components = [{"name": "route_loss", "pct": route_loss_pct,
                   "status": "included",
                   "includes": ["price_impact", "dex_fee", "configured_slippage"]}]
    if network_fee_pct is None:
        components.append({"name": "network_fee", "pct": None, "status": "unknown"})
        all_in = None
        completeness = "partial"
    else:
        components.append({"name": "network_fee", "pct": network_fee_pct,
                           "status": "included"})
        all_in = float(route_loss_pct) + float(network_fee_pct)
        completeness = "complete"
    return validate({
        "version": CONTRACT_VERSION, "purpose": "current_action",
        "basis": "round_trip", "currency": "USD", "notional_usd": notional_usd,
        "method": method, "components": components,
        "known_total_pct": float(route_loss_pct) + float(network_fee_pct or 0),
        "all_in_total_pct": all_in, "completeness": completeness,
        "is_real_fill": False,
    })


def unknown_route_contract(*, notional_usd: float, method: str) -> dict:
    """Represent a failed/unknown current quote with zero invented cost."""
    return validate({
        "version": CONTRACT_VERSION, "purpose": "current_action",
        "basis": "round_trip", "currency": "USD", "notional_usd": notional_usd,
        "method": method,
        "components": [
            {"name": "route_loss", "pct": None, "status": "unknown"},
            {"name": "network_fee", "pct": None, "status": "unknown"},
        ],
        "known_total_pct": 0, "all_in_total_pct": None,
        "completeness": "partial", "is_real_fill": False,
    })


def carry_paper_contract(*, notional_usd_per_leg: float,
                         entry_book_impact_pct: float | None,
                         exit_book_impact_pct: float | None,
                         modeled_fee_pct: float,
                         method: str = "cross_perp_paper_quote_proxy_v1") -> dict:
    """Describe what a Carry paper episode measures and, critically, omits.

    Book impact can be observed from read-only quotes. The account's actual fee tier,
    cross-venue basis PnL, collateral cost and rebalancing remain unknown, so this
    contract is always partial and can never emit an all-in total or realized fill.
    ``modeled_fee_pct`` is exposed only as a proxy assumption; it is not known cost.
    """
    try:
        fee_proxy = _finite_nonnegative(
            modeled_fee_pct, field="carry modeled fee proxy"
        )
    except ValueError as exc:
        raise ValueError("carry modeled fee proxy must be numeric") from exc

    components = []
    known_total = 0.0
    for name, value, phase in (
        ("entry_book_impact", entry_book_impact_pct, "entry"),
        ("exit_book_impact", exit_book_impact_pct, "exit"),
    ):
        if value is None:
            components.append({"name": name, "pct": None, "status": "unknown",
                               "source": "read_only_orderbook", "phase": phase})
        else:
            try:
                measured = _finite_nonnegative(
                    value, field=f"carry {name}"
                )
            except ValueError as exc:
                raise ValueError("carry book impact must be finite and non-negative") from exc
            known_total += measured
            components.append({"name": name, "pct": measured, "status": "included",
                               "source": "read_only_orderbook", "phase": phase,
                               "is_real_fill": False})
    components.extend([
        {"name": "venue_fee_tier", "pct": None, "status": "unknown",
         "modeled_proxy_pct": fee_proxy,
         "reason": "account and maker-taker tier not verified"},
        {"name": "cross_venue_basis", "pct": None, "status": "unknown",
         "reason": "leg prices and equal-base-quantity basis PnL not persisted"},
        {"name": "collateral_opportunity_or_borrow", "pct": None,
         "status": "unknown", "reason": "deployed collateral and borrow cost unknown"},
        {"name": "margin_transfer_and_rebalance", "pct": None,
         "status": "unknown", "reason": "transfer and rebalance path not measured"},
    ])
    book_complete = entry_book_impact_pct is not None and exit_book_impact_pct is not None
    modeled_proxy_total = known_total + fee_proxy if book_complete else None
    return validate({
        "version": CONTRACT_VERSION, "purpose": "paper_measurement",
        "basis": "round_trip", "currency": "USD",
        "notional_usd": notional_usd_per_leg, "notional_basis": "per_leg",
        "measurement_gross_notional_usd": float(notional_usd_per_leg) * 2,
        "method": method, "components": components,
        "known_total_pct": known_total, "all_in_total_pct": None,
        "modeled_proxy_total_pct": (round(modeled_proxy_total, 6)
                                    if modeled_proxy_total is not None else None),
        "book_quote_cost_complete": book_complete,
        "completeness": "partial", "is_real_fill": False,
    })
