"""Public structure events are observable facts, not directional trade calls."""
from datetime import datetime, timezone


def _listing(ts="2026-07-13T12:00:00+00:00"):
    return {"exchange": "okx", "symbol": "ABC-USDT", "detected_at": ts,
            "event_type": "instrument_inventory_addition",
            "message": "[Instrument inventory] OKX 新增可交易产品: ABC-USDT"}


def test_listing_is_recorded_once_as_watch_only(tmp_path, monkeypatch):
    import src.pipeline.opportunity_ledger as ol
    import src.pipeline.structure_radar as sr
    monkeypatch.setattr(ol, "DB", tmp_path / "ledger.db")
    assert sr.record_listings([_listing()]) == 1
    assert sr.record_listings([_listing()]) == 0
    event = ol.active("structure")[0]
    assert event["decision"] == "WATCH"
    assert event["auto_execution_allowed"] is False
    assert event["event_type"] == "instrument_inventory_addition"
    assert event["source"] == "okx"
    assert event["symbol"] == "ABC"
    assert event["markets"] == ["ABC-USDT"]
    assert event["inventory_detected_at"] == _listing()["detected_at"]
    assert event["scheduled_open_at"] is None
    assert event["time_semantics"] == "inventory_detection_not_listing_open"
    assert event["listing_verification"]["state"] == "unverified"
    assert event["instrument_class"] == "unclassified_spot"


def test_structure_scan_uses_public_detector_and_returns_ledger(tmp_path, monkeypatch):
    import src.collectors.listing_detector as ld
    import src.pipeline.opportunity_ledger as ol
    import src.pipeline.structure_radar as sr
    monkeypatch.setattr(ol, "DB", tmp_path / "ledger.db")
    monkeypatch.setattr(sr, "SOURCE_HEALTH_FILE", tmp_path / "source_health.json")
    monkeypatch.setattr(ld, "check_all_exchanges_with_status", lambda: {
        "alerts": [_listing()],
        "sources": [
            {"exchange": "binance", "status": "failed", "error": "451",
             "checked_at": "2026-07-13T12:00:00+00:00", "symbol_count": None},
            {"exchange": "okx", "status": "ok", "checked_at": "2026-07-13T12:00:00+00:00",
             "symbol_count": 300, "baseline_ready": True, "new_count": 1},
            {"exchange": "bybit", "status": "failed", "error": "403",
             "checked_at": "2026-07-13T12:00:00+00:00", "symbol_count": None},
        ],
    })
    got = sr.scan()
    assert got["scanned"] == 1 and got["configured_sources"] == 3
    assert got["inserted"] == 1
    assert got["events"][0]["symbol"] == "ABC"
    view = sr.view()
    assert view["scanned"] == 1 and view["configured_sources"] == 3
    assert [s["status"] for s in view["source_health"]] == ["failed", "ok", "failed"]


def test_listing_fetch_failure_is_not_reported_as_empty_success(monkeypatch):
    import httpx
    import src.collectors.listing_detector as ld

    def blocked(*args, **kwargs):
        request = httpx.Request("GET", "https://api.binance.com/api/v3/exchangeInfo")
        response = httpx.Response(451, request=request)
        raise httpx.HTTPStatusError("451 geo blocked", request=request, response=response)

    monkeypatch.setattr(ld.httpx, "get", blocked)
    result = ld.check_exchange_result("binance")
    assert result["status"] == "failed"
    assert result["symbol_count"] is None
    assert result["alerts"] == []
    assert "451" in result["error"]


def test_listing_detector_uses_only_configured_official_fallback(tmp_path, monkeypatch):
    import httpx
    import src.collectors.listing_detector as ld

    monkeypatch.setattr(ld, "SNAPSHOT_DIR", tmp_path)
    monkeypatch.setitem(ld.EXCHANGES, "binance", {
        "urls": ["https://official-primary.invalid", "https://official-mirror.invalid"],
        "parser": "_parse_binance",
    })
    calls = []

    class Response:
        def __init__(self, url):
            self.url = url

        def raise_for_status(self):
            if "primary" in self.url:
                request = httpx.Request("GET", self.url)
                response = httpx.Response(451, request=request)
                raise httpx.HTTPStatusError("451", request=request, response=response)

        def json(self):
            return {"symbols": [{"symbol": "ABCUSDT", "status": "TRADING"}]}

    def fetch(url, timeout):
        calls.append(url)
        return Response(url)

    monkeypatch.setattr(ld.httpx, "get", fetch)
    result = ld.check_exchange_result("binance")

    assert calls == ["https://official-primary.invalid", "https://official-mirror.invalid"]
    assert result["status"] == "ok"
    assert result["endpoint"] == "https://official-mirror.invalid"
    assert result["attempted_endpoints"] == 2
    assert result["symbol_count"] == 1


def test_coinbase_parser_keeps_only_online_tradable_products():
    from src.collectors.listing_detector import _parse_coinbase

    assert _parse_coinbase([
        {"id": "BTC-USD", "status": "online", "trading_disabled": False},
        {"id": "NEW-USD", "status": "online"},
        {"id": "PAUSED-USD", "status": "online", "trading_disabled": True},
        {"id": "OLD-USD", "status": "delisted", "trading_disabled": False},
        {"status": "online"},
    ]) == {"BTC-USD", "NEW-USD"}


def test_coinbase_is_an_official_fail_closed_listing_source(tmp_path, monkeypatch):
    import src.collectors.listing_detector as ld

    monkeypatch.setattr(ld, "SNAPSHOT_DIR", tmp_path)

    class Response:
        def raise_for_status(self):
            return None

        def json(self):
            return [{"id": "BTC-USD", "status": "online",
                     "trading_disabled": False}]

    monkeypatch.setattr(ld.httpx, "get", lambda *_args, **_kwargs: Response())
    result = ld.check_exchange_result("coinbase")

    assert result["status"] == "ok"
    assert result["endpoint"] == "https://api.exchange.coinbase.com/products"
    assert result["symbol_count"] == 1
    assert result["baseline_ready"] is False
    assert result["alerts"] == []


def test_inventory_delta_retains_okx_taxonomy_and_unverified_schedule(
        tmp_path, monkeypatch):
    import src.collectors.listing_detector as ld

    monkeypatch.setattr(ld, "SNAPSHOT_DIR", tmp_path)
    ld._save_snapshot("okx", {"BTC-USDT"})

    class Response:
        def raise_for_status(self):
            return None

        def json(self):
            return {"data": [
                {"instId": "BTC-USDT", "instType": "SPOT", "state": "live",
                 "baseCcy": "BTC", "quoteCcy": "USDT", "instCategory": "1"},
                {"instId": "XAAPL-USDT", "instType": "SPOT", "state": "live",
                 "baseCcy": "XAAPL", "quoteCcy": "USDT", "instCategory": "3",
                 "listTime": "1784181600000", "contTdSwTime": "1784185200000",
                 "openType": "pre_quote"},
            ]}

    monkeypatch.setattr(ld.httpx, "get", lambda *_args, **_kwargs: Response())
    result = ld.check_exchange_result("okx")

    assert result["status"] == "ok" and result["new_count"] == 1
    alert = result["alerts"][0]
    assert alert["event_type"] == "instrument_inventory_addition"
    assert alert["listing_verification"] == {
        "state": "unverified", "reason_code": "official_announcement_not_collected",
    }
    metadata = alert["product_metadata"]
    assert metadata["base_asset"] == "XAAPL"
    assert metadata["quote_asset"] == "USDT"
    assert metadata["source_fields"]["instCategory"] == "3"
    assert metadata["source_fields"]["contTdSwTime"] == "1784185200000"


def test_okx_stock_taxonomy_is_conservatively_stock_or_etf(tmp_path, monkeypatch):
    import src.pipeline.opportunity_ledger as ol
    import src.pipeline.structure_radar as sr

    monkeypatch.setattr(ol, "DB", tmp_path / "ledger.db")
    row = {
        **_listing(), "symbol": "XAAPL-USDT",
        "product_metadata": {
            "version": 1, "source": "okx", "instrument_id": "XAAPL-USDT",
            "market_type": "spot", "base_asset": "XAAPL", "quote_asset": "USDT",
            "source_fields": {
                "instCategory": "3", "contTdSwTime": "1784185200000",
            },
        },
    }

    assert sr.record_listings([row]) == 1
    event = ol.active("structure")[0]
    assert event["instrument_class"] == "tokenized_equity_or_etf"
    assert event["products"][0]["classification"] == {
        "category": "tokenized_equity_or_etf",
        "basis": "official_instrument_metadata",
        "source_field": "instCategory",
        "source_value": "3",
    }
    schedule = event["products"][0]["source_reported_schedule"]
    assert schedule["basis"] == "instrument_metadata_only"
    assert schedule["official_announcement_verified"] is False
    assert event["scheduled_open_at"] is None


def test_untrusted_or_incomplete_announcement_never_promotes_inventory(
        tmp_path, monkeypatch):
    import src.pipeline.opportunity_ledger as ol
    import src.pipeline.structure_radar as sr

    monkeypatch.setattr(ol, "DB", tmp_path / "ledger.db")
    row = {
        **_listing(),
        "official_announcement_evidence": {
            "version": 1, "kind": "official_exchange_listing_announcement",
            "exchange": "okx", "url": "https://okx.example/listing/abc",
            "content_sha256": "0" * 64,
            "published_at": "2026-07-13T11:00:00+00:00",
            "retrieved_at": "2026-07-13T11:30:00+00:00",
            "scheduled_open_at": "2026-07-14T12:00:00+00:00",
            "scheduled_open_text": "Trading opens 12:00 UTC", "markets": ["ABC-USDT"],
        },
    }

    assert sr.record_listings([row]) == 1
    event = ol.active("structure")[0]
    assert event["event_type"] == "instrument_inventory_addition"
    assert event["listing_verification"]["state"] == "unverified"
    assert event["scheduled_open_at"] is None


def test_self_reported_official_url_and_hash_cannot_label_verified_listing(
        tmp_path, monkeypatch):
    import src.pipeline.opportunity_ledger as ol
    import src.pipeline.structure_radar as sr

    monkeypatch.setattr(ol, "DB", tmp_path / "ledger.db")
    row = {
        **_listing(),
        "official_announcement_evidence": {
            "version": 1, "kind": "official_exchange_listing_announcement",
            "exchange": "okx", "url": "https://www.okx.com/help/listing-abc",
            "content_sha256": "a" * 64,
            "published_at": "2026-07-13T11:00:00+00:00",
            "retrieved_at": "2026-07-13T11:30:00+00:00",
            "scheduled_open_at": "2026-07-14T12:00:00+00:00",
            "scheduled_open_text": "Trading opens at 12:00 UTC",
            "markets": ["ABC-USDT"],
        },
    }

    assert sr.record_listings([row]) == 1
    event = ol.active("structure")[0]
    assert event["event_type"] == "instrument_inventory_addition"
    assert event["listing_verification"] == {
        "state": "unverified",
        "reason_code": "independent_announcement_artifact_verifier_unavailable",
    }
    assert event["scheduled_open_at"] is None
    assert event["event_at"] == event["detected_at"]
    assert event["decision"] == "WATCH"
    assert event["auto_execution_allowed"] is False


def test_truncated_inventory_never_replaces_healthy_snapshot(tmp_path, monkeypatch):
    import json
    import src.collectors.listing_detector as ld

    monkeypatch.setattr(ld, "SNAPSHOT_DIR", tmp_path)
    original = {f"COIN{i}-USD" for i in range(200)}
    ld._save_snapshot("coinbase", original)

    class Response:
        def raise_for_status(self):
            return None

        def json(self):
            return [{"id": f"COIN{i}-USD", "status": "online"} for i in range(20)]

    monkeypatch.setattr(ld.httpx, "get", lambda *_args, **_kwargs: Response())
    result = ld.check_exchange_result("coinbase")

    assert result["status"] == "failed"
    assert "inventory truncated" in result["error"]
    assert set(json.loads((tmp_path / "coinbase_symbols.json").read_text())) == original


def test_snapshot_write_failure_isolated_from_other_sources(monkeypatch):
    import src.collectors.listing_detector as ld

    def check(exchange, timeout):
        if exchange == "binance":
            raise OSError("disk full")
        return {"exchange": exchange, "status": "ok", "alerts": [],
                "symbol_count": 1, "baseline_ready": True, "new_count": 0}

    monkeypatch.setattr(ld, "check_exchange_result", check)
    result = ld.check_all_exchanges_with_status()

    assert result["sources"][0]["status"] == "failed"
    assert "disk full" in result["sources"][0]["error"]
    assert all(source["status"] == "ok" for source in result["sources"][1:])


def test_same_asset_multi_quote_listing_is_one_structure_event(tmp_path, monkeypatch):
    import src.pipeline.opportunity_ledger as ol
    import src.pipeline.structure_radar as sr

    monkeypatch.setattr(ol, "DB", tmp_path / "ledger.db")
    ts = "2026-07-15T08:00:00+00:00"
    rows = [_listing(ts), {**_listing(ts), "symbol": "ABC-USDC"},
            {**_listing(ts), "symbol": "ABC-EUR"}]

    assert sr.record_listings(rows) == 1
    event = ol.active("structure")[0]
    assert event["symbol"] == "ABC"
    assert event["markets"] == ["ABC-EUR", "ABC-USDC", "ABC-USDT"]


def test_legacy_pair_rows_are_collapsed_and_never_labeled_as_new_listings():
    import src.pipeline.structure_radar as sr

    ts = "2026-07-14T00:02:00+00:00"
    legacy = [
        {"id": "old-1", "source": "okx", "symbol": "ABC-USDT",
         "event_at": ts, "detected_at": ts, "event_type": "new_listing"},
        {"id": "old-2", "source": "okx", "symbol": "ABC-USDC",
         "event_at": ts, "detected_at": ts, "event_type": "new_listing"},
        {"id": "old-3", "source": "okx", "symbol": "DEF-USDT",
         "event_at": ts, "detected_at": ts, "event_type": "new_listing"},
    ]
    canonical = {
        "id": "new-1", "source": "coinbase", "symbol": "XYZ",
        "event_at": "2026-07-15T00:00:00+00:00",
        "detected_at": "2026-07-15T00:00:00+00:00",
        "event_type": "new_listing", "markets": ["XYZ-USD"],
    }

    events, summary = sr._view_events([canonical, *legacy])

    assert summary == {
        "canonical_events": 1,
        "instrument_inventory_additions": 1,
        "verified_listings": 0,
        "legacy_inventory_deltas": 2,
        "recorded_new_listing_labels_downgraded": 1,
        "legacy_rows": 3,
        "legacy_rows_collapsed": 1,
        "raw_open_rows": 4,
    }
    abc = next(row for row in events if row["symbol"] == "ABC")
    assert abc["event_type"] == "legacy_inventory_delta"
    assert abc["evidence_state"] == "inventory_delta_only"
    assert abc["markets"] == ["ABC-USDC", "ABC-USDT"]
    assert abc["ledger_event_ids"] == ["old-1", "old-2"]
    assert abc["actionable_now"] is False
    assert abc["auto_execution_allowed"] is False
    assert any("未独立核验为官方上币公告" in reason for reason in abc["reasons"])

    xyz = next(row for row in events if row["symbol"] == "XYZ")
    assert xyz["event_type"] == "instrument_inventory_addition"
    assert xyz["recorded_event_type"] == "new_listing"
    assert xyz["listing_verification"]["state"] == "unverified"
    assert xyz["scheduled_open_at"] is None
    assert any("公开读模型已降级" in reason for reason in xyz["reasons"])


def test_listing_source_failure_is_warned_hourly_without_hiding_health(monkeypatch):
    import src.collectors.listing_detector as ld

    calls = []

    class Logger:
        def warning(self, event, **fields):
            calls.append(("warning", event, fields))

        def debug(self, event, **fields):
            calls.append(("debug", event, fields))

    monkeypatch.setattr(ld, "logger", Logger())
    ld._LAST_FAILURE_LOG.clear()
    ld._log_fetch_failure("bybit", "403", now=100)
    ld._log_fetch_failure("bybit", "403", now=200)
    ld._log_fetch_failure("bybit", "403", now=100 + ld.FAILURE_LOG_INTERVAL_SECONDS)

    assert [level for level, _, _ in calls] == ["warning", "debug", "warning"]
    assert calls[0][2] == {"exchange": "bybit", "error": "403"}


def test_scheduler_registers_structure_radar_job():
    from src.pipeline.scheduler import create_scheduler
    scheduler = create_scheduler()
    # The scheduler is intentionally not started in this construction test; shutting
    # down a stopped AsyncIOScheduler raises. Job registration is the contract here.
    assert any(j.id == "structure_radar" for j in scheduler.get_jobs())
