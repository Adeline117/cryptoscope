"""Versioned, auditable cost contracts shared by discovery and live assessments."""
from __future__ import annotations

from math import isclose


CONTRACT_VERSION = 1
PURPOSES = {"discovery_outcome", "current_action"}
STATUSES = {"included", "unknown", "excluded", "not_applicable"}


def validate(contract: dict) -> dict:
    """Validate and return a normalized cost contract without filling unknowns."""
    if not isinstance(contract, dict) or contract.get("version") != CONTRACT_VERSION:
        raise ValueError("unsupported cost contract version")
    if contract.get("purpose") not in PURPOSES:
        raise ValueError("invalid cost contract purpose")
    if contract.get("basis") != "round_trip" or contract.get("currency") != "USD":
        raise ValueError("cost contract must use round-trip USD basis")
    try:
        notional = float(contract["notional_usd"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("cost contract requires positive notional_usd") from exc
    if notional <= 0:
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
                value = float(value)
            except (TypeError, ValueError) as exc:
                raise ValueError(f"included cost component {name} needs pct") from exc
            if value < 0:
                raise ValueError("cost percentages cannot be negative")
            known += value
        elif value is not None:
            raise ValueError(f"non-included cost component {name} must have null pct")
        if status in {"unknown", "excluded"}:
            incomplete = True
        normalized_components.append({**component, "name": name, "pct": value})
    supplied_known = contract.get("known_total_pct")
    try:
        supplied_known = float(supplied_known)
    except (TypeError, ValueError) as exc:
        raise ValueError("cost contract requires known_total_pct") from exc
    if not isclose(supplied_known, known, abs_tol=1e-6):
        raise ValueError("known_total_pct does not equal included components")
    expected_completeness = "partial" if incomplete else "complete"
    if contract.get("completeness") != expected_completeness:
        raise ValueError("cost completeness disagrees with components")
    all_in = contract.get("all_in_total_pct")
    if incomplete and all_in is not None:
        raise ValueError("partial cost contract cannot claim all-in total")
    if not incomplete:
        try:
            all_in = float(all_in)
        except (TypeError, ValueError) as exc:
            raise ValueError("complete cost contract requires all-in total") from exc
        if not isclose(all_in, known, abs_tol=1e-6):
            raise ValueError("all_in_total_pct does not equal included components")
    return {**contract, "notional_usd": notional,
            "components": normalized_components,
            "known_total_pct": round(known, 6),
            "all_in_total_pct": round(all_in, 6) if all_in is not None else None,
            "completeness": expected_completeness,
            "is_real_fill": bool(contract.get("is_real_fill", False))}


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
