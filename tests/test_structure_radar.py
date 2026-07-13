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
    monkeypatch.setattr(ld, "check_all_exchanges", lambda: [_listing()])
    got = sr.scan()
    assert got["scanned"] == 3 and got["inserted"] == 1
    assert got["events"][0]["symbol"] == "ABC-USDT"


def test_scheduler_registers_structure_radar_job():
    from src.pipeline.scheduler import create_scheduler
    scheduler = create_scheduler()
    # The scheduler is intentionally not started in this construction test; shutting
    # down a stopped AsyncIOScheduler raises. Job registration is the contract here.
    assert any(j.id == "structure_radar" for j in scheduler.get_jobs())
