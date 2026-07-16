"""Public-structure lane: instrument inventory first, never rumors.

An exchange instrument delta changes access and liquidity, but it does not prove a
listing announcement or the announced trading-open clock.  Keep those evidence
levels separate so post-event measurement cannot acquire hindsight precision.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

from src.config import DATA_DIR
from src.pipeline.opportunity_ledger import active, record


SOURCE_HEALTH_FILE = DATA_DIR / "structure_source_health.json"
PRODUCT_METADATA_FILE = DATA_DIR / "structure_product_metadata.json"
PRODUCT_METADATA_TIME_SEMANTICS = (
    "current_inventory_metadata_not_event_time_evidence"
)
MAX_PRODUCT_METADATA_BYTES = 8 * 1024 * 1024
PRODUCT_METADATA_MAX_AGE_SECONDS = 300

_KNOWN_QUOTES = ("FDUSD", "USDT", "USDC", "TUSD", "BUSD", "DAI", "USD",
                 "EUR", "TRY", "BTC", "ETH", "BNB")
VIEW_LEDGER_LIMIT = 500

_INSTRUMENT_CLASSES = {
    "crypto_asset", "tokenized_equity", "tokenized_etf",
    "tokenized_equity_or_etf", "tokenized_commodity", "tokenized_forex",
    "tokenized_bond", "unclassified_spot", "mixed",
}

def _base_asset(symbol: str) -> str:
    """Normalize a spot market into its base asset for one listing event."""
    symbol = str(symbol).upper()
    if "-" in symbol:
        return symbol.split("-", 1)[0]
    for quote in _KNOWN_QUOTES:
        if symbol.endswith(quote) and len(symbol) > len(quote):
            return symbol[:-len(quote)]
    return symbol


def _epoch_millis_utc(value: object) -> str | None:
    """Normalize an exchange millisecond field without treating it as verified."""
    try:
        if isinstance(value, bool) or not str(value).strip():
            return None
        millis = int(str(value))
        if millis <= 0:
            return None
        return datetime.fromtimestamp(millis / 1000, timezone.utc).isoformat()
    except (TypeError, ValueError, OSError, OverflowError):
        return None


def _instrument_classification(metadata: object) -> dict:
    """Classify only source-declared product taxonomy; unknown stays unknown."""
    if not isinstance(metadata, dict):
        return {
            "category": "unclassified_spot",
            "basis": "no_explicit_product_taxonomy",
        }
    source = str(metadata.get("source") or "").lower()
    fields = metadata.get("source_fields")
    fields = fields if isinstance(fields, dict) else {}

    # OKX documents instCategory as the base asset taxonomy. Category 3 includes
    # stocks and ETFs but the inventory row does not identify which of the two, so
    # the honest class is the combined category rather than a ticker-name guess.
    if source == "okx":
        category = str(fields.get("instCategory") or "")
        mapped = {
            "1": "crypto_asset",
            "3": "tokenized_equity_or_etf",
            "4": "tokenized_commodity",
            "5": "tokenized_forex",
            "6": "tokenized_bond",
        }.get(category)
        if mapped:
            return {
                "category": mapped,
                "basis": "official_instrument_metadata",
                "source_field": "instCategory",
                "source_value": category,
            }

    explicit: list[tuple[str, str]] = []
    for field in ("product_type", "asset_class", "instrument_class", "security_type"):
        value = fields.get(field)
        if isinstance(value, str) and value.strip():
            explicit.append((field, value.strip().lower()))
    for field, value in explicit:
        if "etf" in value or "exchange traded fund" in value:
            category = "tokenized_etf"
        elif any(word in value for word in ("equity", "stock", "share")):
            category = "tokenized_equity"
        elif "commodit" in value:
            category = "tokenized_commodity"
        elif value in {"fx", "forex", "foreign_exchange"}:
            category = "tokenized_forex"
        elif any(word in value for word in ("bond", "fixed_income")):
            category = "tokenized_bond"
        elif value in {"crypto", "cryptocurrency", "digital_asset"}:
            category = "crypto_asset"
        else:
            continue
        return {
            "category": category,
            "basis": "official_instrument_metadata",
            "source_field": field,
            "source_value": value,
        }
    return {
        "category": "unclassified_spot",
        "basis": "no_explicit_product_taxonomy",
    }


def _source_reported_schedule(metadata: object) -> dict | None:
    """Expose source timing fields while explicitly withholding listing verification."""
    if not isinstance(metadata, dict):
        return None
    source = str(metadata.get("source") or "").lower()
    fields = metadata.get("source_fields")
    fields = fields if isinstance(fields, dict) else {}
    candidates: list[tuple[str, object]] = []
    if source == "okx":
        # For pre-open/call-auction products contTdSwTime is continuous trading;
        # otherwise listTime may be the start.  Both remain inventory metadata here.
        candidates = [
            ("contTdSwTime", fields.get("contTdSwTime")),
            ("listTime", fields.get("listTime")),
        ]
    elif source == "bybit":
        candidates = [("launchTime", fields.get("launchTime"))]
    for field, raw in candidates:
        normalized = _epoch_millis_utc(raw)
        if normalized:
            return {
                "reported_open_at": normalized,
                "source_field": field,
                "basis": "instrument_metadata_only",
                "official_announcement_verified": False,
            }
    return None


def _save_source_health(sources: list[dict]) -> None:
    """Atomically preserve the last per-source evidence for read-only exports."""
    SOURCE_HEALTH_FILE.parent.mkdir(parents=True, exist_ok=True)
    payload = {"updated_at": datetime.now(timezone.utc).isoformat(), "sources": sources}
    tmp = SOURCE_HEALTH_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
    tmp.replace(SOURCE_HEALTH_FILE)


def _load_source_health() -> dict:
    try:
        payload = json.loads(SOURCE_HEALTH_FILE.read_text())
        if isinstance(payload.get("sources"), list):
            return payload
    except (OSError, json.JSONDecodeError, AttributeError):
        pass
    return {"updated_at": None, "sources": []}


def _aware_utc_iso(value: object) -> str | None:
    """Normalize a timezone-aware clock, rejecting ambiguous sidecar evidence."""
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        clock = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if clock.tzinfo is None:
        return None
    return clock.astimezone(timezone.utc).isoformat()


def _now_utc() -> datetime:
    """Injectable wall clock for current-metadata freshness decisions."""
    return datetime.now(timezone.utc)


def _ledger_market_keys(rows: list[dict]) -> set[tuple[str, str]]:
    """Return only source/market identities already represented in the ledger."""
    keys: set[tuple[str, str]] = set()
    for row in rows:
        source = str(row.get("source") or "").strip().lower()
        if not source:
            continue
        markets = row.get("markets")
        if isinstance(markets, list):
            candidates = markets
        elif row.get("event_type") == "new_listing":
            # The oldest immutable rows were one pair per event and had no markets
            # array. Their symbol is still a source-bound market identity.
            candidates = [row.get("symbol")]
        else:
            candidates = []
        for market in candidates:
            if isinstance(market, str) and market.strip():
                keys.add((source, market.strip()))
    return keys


def _valid_current_metadata(source: str, market: str, value: object) -> bool:
    return bool(
        isinstance(value, dict)
        and value.get("version") == 1
        and value.get("source") == source
        and value.get("instrument_id") == market
        and value.get("market_type") == "spot"
        and isinstance(value.get("source_fields"), dict)
    )


def _save_product_metadata_catalog(catalog: object, rows: list[dict]) -> dict:
    """Atomically retain only current metadata for ledger-bound markets.

    This sidecar is deliberately separate from the append-only opportunity rows.
    A later inventory observation can improve the *current* product classification,
    but can never rewrite what was known when the event was first detected.
    """
    relevant = _ledger_market_keys(rows)
    source_catalog = catalog if isinstance(catalog, dict) else {}
    products: list[dict] = []
    for source, market in sorted(relevant):
        source_entry = source_catalog.get(source)
        if not isinstance(source_entry, dict):
            continue
        observed_at = _aware_utc_iso(source_entry.get("observed_at"))
        metadata_by_market = source_entry.get("products")
        metadata = (metadata_by_market.get(market)
                    if isinstance(metadata_by_market, dict) else None)
        if observed_at is None or not _valid_current_metadata(source, market, metadata):
            continue
        products.append({
            "source": source,
            "market": market,
            "observed_at": observed_at,
            "metadata": metadata,
        })

    payload = {
        "version": 1,
        "updated_at": _now_utc().isoformat(),
        "time_semantics": PRODUCT_METADATA_TIME_SEMANTICS,
        "products": products,
    }
    # Strict serialization rejects non-finite or non-JSON upstream values before
    # the atomic replace. The prior complete sidecar remains intact on failure.
    encoded = json.dumps(
        payload, ensure_ascii=False, allow_nan=False, separators=(",", ":"),
    )
    if len(encoded.encode("utf-8")) > MAX_PRODUCT_METADATA_BYTES:
        raise ValueError("structure product metadata sidecar exceeds 8 MiB")
    PRODUCT_METADATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = PRODUCT_METADATA_FILE.with_suffix(PRODUCT_METADATA_FILE.suffix + ".tmp")
    tmp.write_text(encoded)
    tmp.replace(PRODUCT_METADATA_FILE)
    return payload


def _load_product_metadata_catalog() -> dict:
    """Load a strict current-metadata sidecar; any damage fails closed."""
    empty = {"updated_at": None, "products": {}}

    def reject_nonfinite(value: str) -> None:
        raise ValueError(f"non-finite JSON constant: {value}")

    try:
        if PRODUCT_METADATA_FILE.stat().st_size > MAX_PRODUCT_METADATA_BYTES:
            return empty
        payload = json.loads(
            PRODUCT_METADATA_FILE.read_text(), parse_constant=reject_nonfinite,
        )
    except (OSError, UnicodeError, ValueError, RecursionError):
        return empty
    if (not isinstance(payload, dict) or payload.get("version") != 1
            or payload.get("time_semantics") != PRODUCT_METADATA_TIME_SEMANTICS
            or _aware_utc_iso(payload.get("updated_at")) is None
            or not isinstance(payload.get("products"), list)):
        return empty

    products: dict[tuple[str, str], dict] = {}
    for item in payload["products"]:
        if not isinstance(item, dict):
            return empty
        source, market = item.get("source"), item.get("market")
        observed_at = _aware_utc_iso(item.get("observed_at"))
        metadata = item.get("metadata")
        if (not isinstance(source, str) or source != source.strip().lower()
                or not source or not isinstance(market, str) or not market.strip()
                or market != market.strip() or observed_at is None
                or not _valid_current_metadata(source, market, metadata)):
            return empty
        key = (source, market)
        if key in products:
            return empty
        products[key] = {"observed_at": observed_at, "metadata": metadata}
    return {
        "updated_at": _aware_utc_iso(payload["updated_at"]),
        "products": products,
    }


def _fresh_product_metadata(catalog: dict) -> dict[tuple[str, str], dict]:
    """Expose products only while the sidecar is current and not future-dated."""
    updated_at = _aware_utc_iso(catalog.get("updated_at"))
    products = catalog.get("products")
    if updated_at is None or not isinstance(products, dict):
        return {}
    updated_clock = datetime.fromisoformat(updated_at)
    now = _now_utc()
    if now.tzinfo is None:
        return {}
    age_seconds = (
        now.astimezone(timezone.utc) - updated_clock.astimezone(timezone.utc)
    ).total_seconds()
    if age_seconds < 0 or age_seconds > PRODUCT_METADATA_MAX_AGE_SECONDS:
        return {}
    return products


def record_listings(listings: list[dict]) -> int:
    grouped: dict[tuple[str, str, str], dict] = {}
    for listing in listings:
        exchange, symbol = listing.get("exchange"), listing.get("symbol")
        event_at = listing.get("detected_at")
        if not exchange or not symbol or not event_at:
            continue
        base = _base_asset(symbol)
        key = (str(exchange), base, str(event_at))
        group = grouped.setdefault(
            key, {**listing, "base_asset": base, "markets": [], "products": []},
        )
        group["markets"].append(str(symbol))
        metadata = listing.get("product_metadata")
        if not isinstance(metadata, dict):
            metadata = {
                "version": 1, "source": str(exchange),
                "instrument_id": str(symbol), "market_type": "spot",
                "source_fields": {},
            }
        product = {
            "market": str(symbol),
            "metadata": metadata,
            "classification": _instrument_classification(metadata),
        }
        schedule = _source_reported_schedule(metadata)
        if schedule:
            product["source_reported_schedule"] = schedule
        group["products"].append(product)

    inserted = 0
    for (exchange, base, event_at), listing in grouped.items():
        markets = sorted(set(listing["markets"]))
        detected_at = str(listing.get("detected_at"))
        products = sorted(listing["products"], key=lambda item: item["market"])
        classes = sorted({item["classification"]["category"] for item in products})
        instrument_class = classes[0] if len(classes) == 1 else "mixed"
        listing_verification = {
            "state": "unverified",
            "reason_code": "independent_announcement_artifact_verifier_unavailable",
        }
        event = {
            "lane": "structure", "chain": "cex", "token": base,
            "event_key": f"instrument-inventory:{exchange}:{base}:{detected_at}",
            "symbol": base, "source": exchange,
            "detected_at": detected_at, "decision_at": detected_at,
            "inventory_detected_at": detected_at,
            "event_at": detected_at,
            "scheduled_open_at": None,
            "time_semantics": "inventory_detection_not_listing_open",
            "state": "inventory_observed",
            "decision": "WATCH", "event_type": "instrument_inventory_addition",
            "evidence_state": "instrument_inventory_delta_only",
            "listing_verification": listing_verification,
            "instrument_class": instrument_class,
            "instrument_classes": classes,
            "products": products,
            "message": (
                f"{exchange.upper()} instruments 库存新增 {base}: "
                f"{', '.join(markets)}"
            ),
            "markets": markets,
            "reasons": [
                "公开交易所 instruments 库存差分",
                "检测时间不是上币或连续交易开放时间",
                "没有不可变公告 artifact 与独立回读验证器，禁止升级为已核验上币",
            ],
        }
        _, new = record(event, refresh_existing=False)
        inserted += int(new)
    return inserted


def _current_classification_projection(
    row: dict,
    markets: list[str],
    catalog: dict[tuple[str, str], dict],
) -> dict:
    """Project current taxonomy without borrowing it as event-time knowledge."""
    source = str(row.get("source") or "unknown")
    entries = [catalog.get((source.lower(), market)) for market in markets]
    observed = {
        entry.get("observed_at") for entry in entries if isinstance(entry, dict)
    }
    complete = (
        bool(markets)
        and all(isinstance(entry, dict) for entry in entries)
        and len(observed) == 1
        and None not in observed
    )
    if not complete:
        products = [{
            "market": market,
            "metadata": {
                "version": 1, "source": source, "instrument_id": market,
                "market_type": "spot", "source_fields": {},
            },
            "classification": {
                "category": "unclassified_spot",
                "basis": "current_inventory_metadata_unavailable",
            },
        } for market in markets]
        return {
            "instrument_class": "unclassified_spot",
            "instrument_classes": ["unclassified_spot"],
            "products": products,
            "instrument_classification": {
                "state": "unclassified_metadata_unavailable",
                "metadata_observed_at": None,
                "time_semantics": PRODUCT_METADATA_TIME_SEMANTICS,
                "event_time_evidence": False,
            },
        }

    products = []
    for market, entry in zip(markets, entries):
        metadata = entry["metadata"]
        product = {
            "market": market,
            "metadata": metadata,
            "classification": _instrument_classification(metadata),
        }
        schedule = _source_reported_schedule(metadata)
        if schedule:
            schedule.update({
                "metadata_observed_at": entry["observed_at"],
                "time_semantics": PRODUCT_METADATA_TIME_SEMANTICS,
            })
            product["source_reported_schedule"] = schedule
        products.append(product)
    classes = sorted({product["classification"]["category"] for product in products})
    return {
        "instrument_class": classes[0] if len(classes) == 1 else "mixed",
        "instrument_classes": classes,
        "products": products,
        "instrument_classification": {
            "state": "current_metadata_observed",
            "metadata_observed_at": next(iter(observed)),
            "time_semantics": PRODUCT_METADATA_TIME_SEMANTICS,
            "event_time_evidence": False,
        },
    }


def _inventory_projection(
    row: dict,
    *,
    recorded_new_listing: bool = False,
    catalog: dict[tuple[str, str], dict] | None = None,
) -> dict:
    """Project an inventory-only row without rewriting its append-only ledger row."""
    detected_at = str(row.get("detected_at") or row.get("event_at") or "")
    markets = [str(market) for market in row.get("markets", [])] \
        if isinstance(row.get("markets"), list) else []
    current = _current_classification_projection(row, markets, catalog or {})
    reasons = list(row.get("reasons") or [])
    if recorded_new_listing:
        reasons.extend([
            "旧账本记录使用 new_listing 标签；公开读模型已降级为库存新增",
            "没有绑定官方公告与公告开盘时间，不得称为已核验上币",
        ])
    return {
        **row,
        "event_type": "instrument_inventory_addition",
        "recorded_event_type": "new_listing" if recorded_new_listing
        else row.get("recorded_event_type"),
        "inventory_detected_at": detected_at,
        "recorded_event_at": row.get("event_at") if recorded_new_listing else None,
        "event_at": detected_at,
        "scheduled_open_at": None,
        "time_semantics": "inventory_detection_not_listing_open",
        "state": "inventory_observed",
        "decision": "WATCH",
        "recorded_decision": "WATCH",
        "effective_decision": "WATCH",
        "actionable_now": False,
        "auto_execution_allowed": False,
        "evidence_state": "instrument_inventory_delta_only",
        "listing_verification": {
            "state": "unverified",
            "reason_code": "official_announcement_and_open_time_not_verified",
        },
        **current,
        "reasons": reasons,
    }


def _view_events(
    rows: list[dict],
    catalog: dict[tuple[str, str], dict] | None = None,
) -> tuple[list[dict], dict]:
    """Collapse old pair rows and quarantine every unsupported listing label.

    The ledger remains untouched.  Pre-upgrade ``new_listing`` rows either collapse
    by asset/batch or become an inventory-only public projection; neither is allowed
    to inherit announcement/open-time evidence that was never recorded.
    """
    canonical: list[dict] = []
    legacy_groups: dict[tuple[str, str, str], list[dict]] = {}
    downgraded_new_listing_labels = 0
    for row in rows:
        markets = row.get("markets")
        event_type = row.get("event_type")
        if event_type == "new_listing" and isinstance(markets, list) and markets:
            canonical.append(_inventory_projection(
                row, recorded_new_listing=True, catalog=catalog,
            ))
            downgraded_new_listing_labels += 1
            continue
        if event_type != "new_listing":
            projected = (_inventory_projection(row, catalog=catalog)
                         if event_type == "instrument_inventory_addition"
                         else {**row, "auto_execution_allowed": False})
            canonical.append(projected)
            continue
        source = str(row.get("source") or "unknown")
        base = _base_asset(str(row.get("symbol") or row.get("token") or "?"))
        batch_at = str(row.get("event_at") or row.get("detected_at") or "unknown")
        legacy_groups.setdefault((source, base, batch_at), []).append(row)

    legacy: list[dict] = []
    for (source, base, batch_at), group in legacy_groups.items():
        markets = sorted({str(row.get("symbol") or "") for row in group
                          if row.get("symbol")})
        first = min(group, key=lambda row: str(row.get("detected_at") or ""))
        detected_at = str(first.get("detected_at") or batch_at)
        legacy_row = {
            **first,
            "id": f"legacy-inventory:{source}:{base}:{batch_at}",
            "token": base,
            "symbol": base,
            "markets": markets,
            "event_type": "legacy_inventory_delta",
            "recorded_event_type": "new_listing",
            "state": "legacy_observation",
            "decision": "WATCH",
            "recorded_decision": "WATCH",
            "effective_decision": "WATCH",
            "actionable_now": False,
            "auto_execution_allowed": False,
            "evidence_state": "inventory_delta_only",
            "inventory_detected_at": detected_at,
            "recorded_event_at": first.get("event_at"),
            "event_at": detected_at,
            "scheduled_open_at": None,
            "time_semantics": "inventory_detection_not_listing_open",
            "listing_verification": {
                "state": "unverified",
                "reason_code": "legacy_inventory_rows_have_no_announcement_evidence",
            },
            "legacy_row_count": len(group),
            "ledger_event_ids": [row.get("id") for row in group if row.get("id")],
            "message": (f"{source.upper()} 旧版 instrument inventory delta: "
                        f"{', '.join(markets)}"),
            "reasons": [
                "旧版按交易对写入，已按基础资产折叠",
                "仅证明公开 instruments 库存差分，未独立核验为官方上币公告",
                "只观察，不进入方向优势判决",
            ],
        }
        legacy.append({
            **legacy_row,
            **_current_classification_projection(legacy_row, markets, catalog or {}),
        })

    events = sorted(canonical + legacy,
                    key=lambda row: str(row.get("event_at") or row.get("detected_at") or ""),
                    reverse=True)
    legacy_rows = sum(len(group) for group in legacy_groups.values())
    return events, {
        "canonical_events": len(canonical),
        "instrument_inventory_additions": sum(
            row.get("event_type") == "instrument_inventory_addition"
            for row in canonical
        ),
        "verified_listings": sum(
            row.get("event_type") == "verified_listing" for row in canonical
        ),
        "legacy_inventory_deltas": len(legacy),
        "recorded_new_listing_labels_downgraded": downgraded_new_listing_labels,
        "legacy_rows": legacy_rows,
        "legacy_rows_collapsed": legacy_rows - len(legacy),
        "raw_open_rows": len(rows),
    }


def scan() -> dict:
    from src.collectors.listing_detector import check_all_exchanges_with_status
    result = check_all_exchanges_with_status()
    sources = result["sources"]
    _save_source_health(sources)
    reachable = sum(source.get("status") == "ok" for source in sources)
    inserted = record_listings(result["alerts"])
    rows = active("structure", limit=VIEW_LEDGER_LIMIT)
    _save_product_metadata_catalog(result.get("product_metadata_catalog"), rows)
    catalog = _load_product_metadata_catalog()
    events, summary = _view_events(rows, _fresh_product_metadata(catalog))
    return {"scanned": reachable, "configured_sources": len(sources),
            "source_health": sources, "source_health_at": datetime.now(timezone.utc).isoformat(),
            "inserted": inserted, "events": events, **summary,
            "product_metadata_at": catalog["updated_at"],
            "product_metadata_time_semantics": PRODUCT_METADATA_TIME_SEMANTICS,
            "source": "Public exchange instrument inventory with per-source health"}


def view() -> dict:
    health = _load_source_health()
    sources = health["sources"]
    catalog = _load_product_metadata_catalog()
    events, summary = _view_events(
        active("structure", limit=VIEW_LEDGER_LIMIT),
        _fresh_product_metadata(catalog),
    )
    return {"events": events, **summary, "source_health": sources,
            "source_health_at": health["updated_at"],
            "product_metadata_at": catalog["updated_at"],
            "product_metadata_time_semantics": PRODUCT_METADATA_TIME_SEMANTICS,
            "scanned": sum(source.get("status") == "ok" for source in sources),
            "configured_sources": len(sources),
            "source": "Public structure inventory ledger with per-source health"}
