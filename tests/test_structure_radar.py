"""Public structure events are observable facts, not directional trade calls."""
from datetime import datetime, timezone


def _listing(ts="2026-07-13T12:00:00+00:00"):
    return {"exchange": "okx", "symbol": "ABC-USDT", "detected_at": ts,
            "message": "[新上币] OKX 新增交易对: ABC-USDT"}


def test_listing_is_recorded_once_as_watch_only(tmp_path, monkeypatch):
    import src.pipeline.opportunity_ledger as ol
    import src.pipeline.structure_radar as sr
    monkeypatch.setattr(ol, "DB", tmp_path / "ledger.db")
    assert sr.record_listings([_listing()]) == 1
    assert sr.record_listings([_listing()]) == 0
    event = ol.active("structure")[0]
    assert event["decision"] == "WATCH"
    assert event["event_type"] == "new_listing"
    assert event["source"] == "okx"


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
    assert got["events"][0]["symbol"] == "ABC-USDT"
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
