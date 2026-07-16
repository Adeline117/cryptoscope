"""Non-Launch board lanes cannot publish malformed or self-promoted actions."""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone

import pytest


def _clock() -> str:
    return datetime.now(timezone.utc).isoformat()


def _structure_event() -> dict:
    now = _clock()
    return {
        "id": "structure-1", "lane": "structure", "chain": "cex",
        "token": "ABC", "symbol": "ABC", "source": "okx",
        "event_at": now, "detected_at": now, "decision_at": now,
        "effective_decision": "WATCH", "actionable_now": False,
        "auto_execution_allowed": False,
    }


def _airdrop_event() -> dict:
    now = _clock()
    return {
        "id": "airdrop-1", "lane": "airdrop", "chain": "starknet",
        "token": "campaign-1", "symbol": "Campaign", "state": "research",
        "source": "official campaign watchlist", "event_at": None,
        "detected_at": now, "decision_at": now, "expires_at": None,
        "deadline": None, "effective_decision": "WATCH",
        "actionable_now": False, "auto_execution_allowed": False,
        "trust_root": "starknet.io",
        "official_url": "https://campaign.starknet.io/",
        "source_evidence_url": "https://campaign.starknet.io/terms",
        "source_state": "source_unverified", "official_state": "source_unverified",
        "source_verification": {
            "trust_root": "starknet.io", "checked_at": now,
            "official_page_verified": False, "evidence_page_verified": False,
        },
        "evidence_state": "unknown", "wallet_count": 0,
    }


def _cascade_event() -> dict:
    now = _clock()
    return {
        "id": "cascade-1", "lane": "cascade", "chain": "hyperliquid",
        "token": "BTC", "symbol": "BTC", "source": "Hyperliquid",
        "event_at": now, "detected_at": now, "decision_at": now,
        "direction": "down", "side": "SHORT",
        "effective_decision": "WATCH", "actionable_now": False,
        "auto_execution_allowed": False,
    }


def _payload(view: str, body: dict) -> dict:
    from src.pipeline.board_export import _envelope

    return _envelope(body, view=view)


def _validate(view: str, body: dict) -> dict:
    from src.contract.board_view import validate_board_view
    from src.pipeline.board_export import VIEW_FRESHNESS

    cadence, grace = VIEW_FRESHNESS[view]
    return validate_board_view(
        view, _payload(view, body), cadence_min=cadence, grace_min=grace,
    )


def _valid_body(view: str) -> dict:
    if view == "structure":
        return {"events": [_structure_event()]}
    if view == "airdrop":
        return {"events": [_airdrop_event()]}
    return {
        "perps": [{"symbol": "BTC"}],
        "carry": [{"symbol": "ETH"}],
        "cascade_events": [_cascade_event()],
    }


@pytest.mark.parametrize("view", ["structure", "airdrop", "perps"])
def test_current_nonlaunch_shapes_cross_the_contract(view):
    payload = _validate(view, _valid_body(view))

    assert payload["view"] == view


@pytest.mark.parametrize(("view", "field"), [
    ("structure", "events"),
    ("airdrop", "events"),
    ("perps", "perps"),
    ("perps", "carry"),
    ("perps", "cascade_events"),
])
def test_every_nonlaunch_public_collection_is_an_explicit_array(view, field):
    from src.contract.board_view import BoardViewContractError

    body = _valid_body(view)
    body[field] = {"not": "an array"}

    with pytest.raises(BoardViewContractError, match=rf"{field} must be a list"):
        _validate(view, body)


@pytest.mark.parametrize("field", ["perps", "carry", "cascade_events"])
def test_perps_cannot_omit_a_market_or_event_collection(field):
    from src.contract.board_view import BoardViewContractError

    body = _valid_body("perps")
    del body[field]

    with pytest.raises(BoardViewContractError, match=field):
        _validate("perps", body)


@pytest.mark.parametrize(("field", "value"), [
    ("id", 7), ("chain", ""), ("token", None), ("symbol", []), ("source", " "),
    ("detected_at", None), ("decision_at", "2026-07-16T00:00:00"),
    ("event_at", "not-a-clock"),
])
def test_nonlaunch_event_requires_bound_identity_and_aware_clocks(field, value):
    from src.contract.board_view import BoardViewContractError

    body = _valid_body("structure")
    body["events"][0][field] = value

    with pytest.raises(BoardViewContractError, match=field):
        _validate("structure", body)


def test_structure_event_at_may_describe_a_future_public_schedule():
    body = _valid_body("structure")
    scheduled = datetime.now(timezone.utc) + timedelta(days=1)
    body["events"][0]["event_at"] = scheduled.isoformat()

    payload = _validate("structure", body)

    assert payload["events"][0]["event_at"] == scheduled.isoformat()


@pytest.mark.parametrize("view", ["structure", "airdrop", "perps"])
@pytest.mark.parametrize("value", [None, True, 0, "false"])
def test_nonlaunch_event_requires_literal_false_auto_execution(view, value):
    from src.contract.board_view import BoardViewContractError

    body = _valid_body(view)
    key = "cascade_events" if view == "perps" else "events"
    event = body[key][0]
    if value is None:
        event.pop("auto_execution_allowed")
    else:
        event["auto_execution_allowed"] = value

    with pytest.raises(BoardViewContractError, match="auto_execution_allowed"):
        _validate(view, body)


@pytest.mark.parametrize("nested", ["current_assessment", "evidence_gate"])
def test_nested_nonlaunch_claim_cannot_opt_into_automation(nested):
    from src.contract.board_view import BoardViewContractError

    body = _valid_body("perps")
    body["cascade_events"][0][nested] = {"auto_execution_allowed": True}

    with pytest.raises(BoardViewContractError, match=rf"{nested}.*auto_execution_allowed"):
        _validate("perps", body)


def test_structure_cannot_leak_an_action_through_effective_decision():
    from src.contract.board_view import BoardViewContractError

    body = _valid_body("structure")
    body["events"][0]["effective_decision"] = "CLAIM_CHECK"

    with pytest.raises(BoardViewContractError, match="effective_decision"):
        _validate("structure", body)


def test_unverified_airdrop_cannot_self_promote_to_claim_check():
    from src.contract.board_view import BoardViewContractError

    body = _valid_body("airdrop")
    event = body["events"][0]
    expires = datetime.now(timezone.utc) + timedelta(hours=1)
    event.update({
        "state": "claimable", "evidence_state": "recorded", "wallet_count": 1,
        "deadline": expires.isoformat(), "expires_at": expires.isoformat(),
        "effective_decision": "CLAIM_CHECK", "actionable_now": True,
    })

    with pytest.raises(BoardViewContractError, match="verified claim-check basis"):
        _validate("airdrop", body)


def test_verified_owned_wallet_airdrop_can_publish_manual_claim_check():
    body = _valid_body("airdrop")
    event = body["events"][0]
    expires = datetime.now(timezone.utc) + timedelta(hours=1)
    event.update({
        "state": "claimable", "source_state": "source_verified",
        "official_state": "source_verified", "evidence_state": "recorded",
        "wallet_count": 1, "deadline": expires.isoformat(),
        "expires_at": expires.isoformat(), "effective_decision": "CLAIM_CHECK",
        "actionable_now": True,
    })
    event["source_verification"].update({
        "official_page_verified": True, "evidence_page_verified": True,
    })

    payload = _validate("airdrop", body)

    assert payload["events"][0]["effective_decision"] == "CLAIM_CHECK"
    assert payload["events"][0]["auto_execution_allowed"] is False


def test_cascade_cannot_self_promote_without_evidence_and_bound_quote():
    from src.contract.board_view import BoardViewContractError

    body = _valid_body("perps")
    event = body["cascade_events"][0]
    event.update({
        "effective_decision": "SMALL_PROBE", "actionable_now": True,
        "quote_at": event["detected_at"],
        "expires_at": (datetime.now(timezone.utc) + timedelta(seconds=30)).isoformat(),
    })

    with pytest.raises(BoardViewContractError, match="evidence gate"):
        _validate("perps", body)


def test_bound_read_only_cascade_quote_can_publish_manual_probe():
    body = _valid_body("perps")
    event = body["cascade_events"][0]
    event.update({
        "effective_decision": "SMALL_PROBE", "actionable_now": True,
        "quote_at": event["detected_at"],
        "expires_at": (datetime.now(timezone.utc) + timedelta(seconds=30)).isoformat(),
        "evidence_gate": {"state": "pass", "auto_execution_allowed": False},
        "execution_probe": {
            "state": "quoted", "side": "SHORT", "quote_at": event["detected_at"],
            "read_only": True, "is_real_fill": False,
        },
    })

    payload = _validate("perps", body)

    assert payload["cascade_events"][0]["actionable_now"] is True
    assert payload["cascade_events"][0]["auto_execution_allowed"] is False


@pytest.mark.parametrize("mutation", [
    {"actionable_now": True},
    {"auto_execution_allowed": True},
    {"effective_decision": "SMALL_PROBE"},
    {"decision": "CLAIM_CHECK"},
])
def test_raw_carry_observation_cannot_smuggle_execution_semantics(mutation):
    from src.contract.board_view import BoardViewContractError

    body = _valid_body("perps")
    body["carry"][0].update(mutation)

    with pytest.raises(BoardViewContractError):
        _validate("perps", body)


@pytest.mark.parametrize("row", [None, "BTC", {}, {"symbol": ""}])
def test_raw_perp_and_carry_rows_require_object_identity(row):
    from src.contract.board_view import BoardViewContractError

    body = _valid_body("perps")
    body["carry"] = [deepcopy(row)]

    with pytest.raises(BoardViewContractError, match=r"carry\[0\]"):
        _validate("perps", body)
