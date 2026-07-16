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
        "inventory_detected_at": now, "scheduled_open_at": None,
        "time_semantics": "inventory_detection_not_listing_open",
        "event_type": "instrument_inventory_addition",
        "evidence_state": "instrument_inventory_delta_only",
        "listing_verification": {
            "state": "unverified",
            "reason_code": "official_announcement_and_open_time_not_verified",
        },
        "markets": ["ABC-USDT"],
        "instrument_class": "unclassified_spot",
        "instrument_classes": ["unclassified_spot"],
        "instrument_classification": {
            "state": "unclassified_metadata_unavailable",
            "metadata_observed_at": None,
            "time_semantics": "current_inventory_metadata_not_event_time_evidence",
            "event_time_evidence": False,
        },
        "products": [{
            "market": "ABC-USDT",
            "metadata": {
                "version": 1, "source": "okx", "instrument_id": "ABC-USDT",
                "market_type": "spot", "source_fields": {},
            },
            "classification": {
                "category": "unclassified_spot",
                "basis": "current_inventory_metadata_unavailable",
            },
        }],
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
        return {
            "events": [_structure_event()],
            "product_metadata_at": None,
            "product_metadata_time_semantics": (
                "current_inventory_metadata_not_event_time_evidence"
            ),
        }
    if view == "airdrop":
        return {"events": [_airdrop_event()]}
    return {
        "perps": [{"symbol": "BTC"}],
        "carry": [{"symbol": "ETH"}],
        "cascade_events": [_cascade_event()],
    }


def _with_current_structure_metadata(
    body: dict, *, source: str = "okx", source_fields: dict | None = None,
) -> tuple[dict, dict]:
    observed_at = _clock()
    body["product_metadata_at"] = observed_at
    row = body["events"][0]
    row["source"] = source
    row["instrument_classification"] = {
        "state": "current_metadata_observed",
        "metadata_observed_at": observed_at,
        "time_semantics": "current_inventory_metadata_not_event_time_evidence",
        "event_time_evidence": False,
    }
    product = row["products"][0]
    product["metadata"]["source"] = source
    product["metadata"]["source_fields"] = source_fields or {}
    product["classification"] = {
        "category": "unclassified_spot",
        "basis": "no_explicit_product_taxonomy",
    }
    return row, product


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


def test_inventory_detection_cannot_be_relabelled_as_a_future_open_time():
    from src.contract.board_view import BoardViewContractError

    body = _valid_body("structure")
    scheduled = datetime.now(timezone.utc) + timedelta(days=1)
    body["events"][0]["event_at"] = scheduled.isoformat()

    with pytest.raises(BoardViewContractError, match="inventory detection time"):
        _validate("structure", body)


def test_self_reported_official_announcement_cannot_publish_verified_listing():
    from src.contract.board_view import BoardViewContractError

    body = _valid_body("structure")
    row = body["events"][0]
    detected = datetime.fromisoformat(row["detected_at"])
    scheduled = detected + timedelta(days=1)
    evidence = {
        "version": 1,
        "kind": "official_exchange_listing_announcement",
        "exchange": "okx",
        "url": "https://www.okx.com/help/listing-abc",
        "content_sha256": "a" * 64,
        "published_at": (detected - timedelta(minutes=1)).isoformat(),
        "retrieved_at": detected.isoformat(),
        "scheduled_open_at": scheduled.isoformat(),
        "scheduled_open_text": "Trading opens at 12:00 UTC",
        "markets": ["ABC-USDT"],
    }
    row.update({
        "event_type": "verified_listing",
        "event_at": scheduled.isoformat(),
        "scheduled_open_at": scheduled.isoformat(),
        "time_semantics": "official_scheduled_open",
        "evidence_state": "official_announcement_and_open_time_verified",
        "listing_verification": {
            "state": "verified",
            "basis": "official_announcement_and_bound_open_time",
            "evidence": evidence,
        },
    })

    with pytest.raises(BoardViewContractError, match="event_type is invalid"):
        _validate("structure", body)


def test_legacy_inventory_row_is_allowed_but_cannot_claim_verified_listing():
    body = _valid_body("structure")
    row = body["events"][0]
    row.update({
        "event_type": "legacy_inventory_delta",
        "recorded_event_type": "new_listing",
        "listing_verification": {
            "state": "unverified",
            "reason_code": "legacy_inventory_rows_have_no_announcement_evidence",
        },
    })

    payload = _validate("structure", body)

    assert payload["events"][0]["event_type"] == "legacy_inventory_delta"
    assert payload["events"][0]["listing_verification"]["state"] == "unverified"


def test_inventory_metadata_schedule_cannot_promote_public_listing_semantics():
    body = _valid_body("structure")
    row = body["events"][0]
    observed_at = _clock()
    body["product_metadata_at"] = observed_at
    row["instrument_classification"] = {
        "state": "current_metadata_observed",
        "metadata_observed_at": observed_at,
        "time_semantics": "current_inventory_metadata_not_event_time_evidence",
        "event_time_evidence": False,
    }
    row["products"][0]["metadata"]["source_fields"] = {
        "contTdSwTime": "1784185200000",
    }
    row["products"][0]["classification"] = {
        "category": "unclassified_spot",
        "basis": "no_explicit_product_taxonomy",
    }
    row["products"][0]["source_reported_schedule"] = {
        "reported_open_at": "2026-07-16T07:00:00+00:00",
        "source_field": "contTdSwTime",
        "basis": "instrument_metadata_only",
        "official_announcement_verified": False,
        "metadata_observed_at": observed_at,
        "time_semantics": "current_inventory_metadata_not_event_time_evidence",
    }

    payload = _validate("structure", body)

    assert payload["events"][0]["event_type"] == "instrument_inventory_addition"
    assert payload["events"][0]["scheduled_open_at"] is None


@pytest.mark.parametrize(("source", "source_field"), [
    ("okx", "listTime"),
    ("bybit", "launchTime"),
])
def test_structure_schedule_accepts_only_other_collected_source_pairs(
    source, source_field,
):
    body = _valid_body("structure")
    raw_millis = "1784185200000"
    row, product = _with_current_structure_metadata(
        body, source=source, source_fields={source_field: raw_millis},
    )
    product["source_reported_schedule"] = {
        "reported_open_at": "2026-07-16T07:00:00+00:00",
        "source_field": source_field,
        "basis": "instrument_metadata_only",
        "official_announcement_verified": False,
        "metadata_observed_at": row["instrument_classification"]["metadata_observed_at"],
        "time_semantics": "current_inventory_metadata_not_event_time_evidence",
    }

    assert _validate("structure", body)["events"][0]["source"] == source


@pytest.mark.parametrize(("source", "source_field", "source_fields", "offset", "error"), [
    ("okx", "bogusOpenTime", {"bogusOpenTime": "1784185200000"}, 0, "not allowed"),
    ("okx", "contTdSwTime", {}, 0, "absent from source_fields"),
    ("okx", "contTdSwTime", {"contTdSwTime": "not-an-epoch"}, 0, "epoch-ms"),
    ("okx", "contTdSwTime", {"contTdSwTime": True}, 0, "epoch-ms"),
    ("okx", "contTdSwTime", {"contTdSwTime": 1784185200000.0}, 0, "epoch-ms"),
    ("okx", "contTdSwTime", {"contTdSwTime": "0"}, 0, "epoch-ms"),
    ("okx", "contTdSwTime", {"contTdSwTime": "1784185200000"}, 1, "contradicts"),
    ("coinbase", "contTdSwTime", {"contTdSwTime": "1784185200000"}, 0, "not allowed"),
])
def test_structure_schedule_must_be_exactly_bound_to_an_allowed_source_epoch(
    source, source_field, source_fields, offset, error,
):
    from src.contract.board_view import BoardViewContractError

    body = _valid_body("structure")
    row, product = _with_current_structure_metadata(
        body, source=source, source_fields=source_fields,
    )
    product["source_reported_schedule"] = {
        "reported_open_at": (
            datetime(1970, 1, 1, tzinfo=timezone.utc)
            + timedelta(milliseconds=1784185200000, seconds=offset)
        ).isoformat(),
        "source_field": source_field,
        "basis": "instrument_metadata_only",
        "official_announcement_verified": False,
        "metadata_observed_at": row["instrument_classification"]["metadata_observed_at"],
        "time_semantics": "current_inventory_metadata_not_event_time_evidence",
    }

    with pytest.raises(BoardViewContractError, match=error):
        _validate("structure", body)


def test_unavailable_structure_metadata_cannot_publish_a_schedule():
    from src.contract.board_view import BoardViewContractError

    body = _valid_body("structure")
    product = body["events"][0]["products"][0]
    product["metadata"]["source_fields"] = {"contTdSwTime": "1784185200000"}
    product["source_reported_schedule"] = {
        "reported_open_at": "2026-07-16T07:00:00+00:00",
        "source_field": "contTdSwTime",
        "basis": "instrument_metadata_only",
        "official_announcement_verified": False,
    }

    with pytest.raises(BoardViewContractError, match="cannot publish a schedule"):
        _validate("structure", body)


def test_unavailable_structure_metadata_cannot_hide_source_fields():
    from src.contract.board_view import BoardViewContractError

    body = _valid_body("structure")
    body["events"][0]["products"][0]["metadata"]["source_fields"] = {
        "instCategory": "3",
    }

    with pytest.raises(BoardViewContractError, match="cannot retain source_fields"):
        _validate("structure", body)


def test_current_structure_taxonomy_is_bound_to_sidecar_clock_not_event_clock():
    body = _valid_body("structure")
    row = body["events"][0]
    observed_at = _clock()
    body["product_metadata_at"] = observed_at
    row["instrument_class"] = "tokenized_equity_or_etf"
    row["instrument_classes"] = ["tokenized_equity_or_etf"]
    row["instrument_classification"] = {
        "state": "current_metadata_observed",
        "metadata_observed_at": observed_at,
        "time_semantics": "current_inventory_metadata_not_event_time_evidence",
        "event_time_evidence": False,
    }
    row["products"][0]["metadata"]["source_fields"] = {"instCategory": "3"}
    row["products"][0]["classification"] = {
        "category": "tokenized_equity_or_etf",
        "basis": "official_instrument_metadata",
        "source_field": "instCategory",
        "source_value": "3",
    }

    payload = _validate("structure", body)

    assert payload["events"][0]["event_at"] == row["detected_at"]
    assert payload["events"][0]["scheduled_open_at"] is None


def test_coinbase_product_type_is_an_allowed_official_taxonomy_field():
    body = _valid_body("structure")
    row, product = _with_current_structure_metadata(
        body, source="coinbase", source_fields={"product_type": "Stock"},
    )
    row["instrument_class"] = "tokenized_equity"
    row["instrument_classes"] = ["tokenized_equity"]
    product["classification"] = {
        "category": "tokenized_equity",
        "basis": "official_instrument_metadata",
        "source_field": "product_type",
        "source_value": "stock",
    }

    payload = _validate("structure", body)

    assert payload["events"][0]["instrument_class"] == "tokenized_equity"


@pytest.mark.parametrize(("source", "source_field"), [
    ("okx", "product_type"),
    ("coinbase", "asset_class"),
    ("coinbase", "bogusTaxonomy"),
    ("bybit", "product_type"),
])
def test_official_structure_taxonomy_rejects_uncollected_source_field_pairs(
    source, source_field,
):
    from src.contract.board_view import BoardViewContractError

    body = _valid_body("structure")
    row, product = _with_current_structure_metadata(
        body, source=source, source_fields={source_field: "stock"},
    )
    row["instrument_class"] = "tokenized_equity"
    row["instrument_classes"] = ["tokenized_equity"]
    product["classification"] = {
        "category": "tokenized_equity",
        "basis": "official_instrument_metadata",
        "source_field": source_field,
        "source_value": "stock",
    }

    with pytest.raises(BoardViewContractError, match="source taxonomy field is not allowed"):
        _validate("structure", body)


def test_current_structure_taxonomy_requires_bound_top_level_sidecar_clock():
    from src.contract.board_view import BoardViewContractError

    body = _valid_body("structure")
    row = body["events"][0]
    row["instrument_classification"] = {
        "state": "current_metadata_observed",
        "metadata_observed_at": _clock(),
        "time_semantics": "current_inventory_metadata_not_event_time_evidence",
        "event_time_evidence": False,
    }
    row["products"][0]["classification"] = {
        "category": "unclassified_spot",
        "basis": "no_explicit_product_taxonomy",
    }

    with pytest.raises(BoardViewContractError, match="product_metadata_at cannot be null"):
        _validate("structure", body)


def test_current_structure_metadata_rejects_a_stale_sidecar_clock():
    from src.contract.board_view import BoardViewContractError

    body = _valid_body("structure")
    row, _ = _with_current_structure_metadata(body)
    stale = (datetime.now(timezone.utc) - timedelta(minutes=6)).isoformat()
    body["product_metadata_at"] = stale
    row["instrument_classification"]["metadata_observed_at"] = stale

    with pytest.raises(BoardViewContractError, match="too old for current metadata"):
        _validate("structure", body)


def test_structure_contract_rejects_event_time_claim_for_current_metadata():
    from src.contract.board_view import BoardViewContractError

    body = _valid_body("structure")
    body["events"][0]["instrument_classification"]["event_time_evidence"] = True

    with pytest.raises(BoardViewContractError, match="cannot claim event-time evidence"):
        _validate("structure", body)


def test_structure_contract_requires_top_level_metadata_time_semantics():
    from src.contract.board_view import BoardViewContractError

    body = _valid_body("structure")
    body["product_metadata_time_semantics"] = "event_time_metadata"

    with pytest.raises(BoardViewContractError, match="current-not-event-time"):
        _validate("structure", body)


def test_structure_product_taxonomy_must_be_bound_to_source_metadata():
    from src.contract.board_view import BoardViewContractError

    body = _valid_body("structure")
    body["events"][0]["instrument_class"] = "tokenized_equity"

    with pytest.raises(BoardViewContractError, match="instrument_class contradicts"):
        _validate("structure", body)


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
