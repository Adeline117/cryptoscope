"""Solana new-pool coverage across all venues, parsed and deduped honestly."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from src.pipeline import solana_new_pools as snp


NOW = datetime(2026, 7, 18, 8, 0, tzinfo=timezone.utc)


def _raw(pool, base, symbol, dex, *, quote="So11111111111111111111111111111111111111112",
         created="2026-07-18T07:59:00Z", fdv="2700", liq="2100", vol="2666"):
    return {
        "id": f"solana_{pool}",
        "attributes": {"name": f"{symbol} / SOL", "pool_created_at": created,
                       "address": pool, "fdv_usd": fdv, "reserve_in_usd": liq,
                       "volume_usd": {"m5": vol}},
        "relationships": {
            "base_token": {"data": {"id": f"solana_{base}"}},
            "quote_token": {"data": {"id": f"solana_{quote}"}},
            "dex": {"data": {"id": dex}},
        },
    }


def test_parse_extracts_new_token_symbol_and_dex():
    p = snp.parse_pool(_raw("POOL1", "MINTabc", "PESO", "raydium"))
    assert p["token"] == "MINTabc" and p["symbol"] == "PESO" and p["dex"] == "raydium"
    assert p["liquidity_usd"] == 2100.0 and p["fdv_usd"] == 2700.0


def test_parse_uses_the_non_quote_side_as_the_new_token():
    # base side is WSOL (a quote) → the real new token is the quote_token slot.
    p = snp.parse_pool(_raw("POOL2", "So11111111111111111111111111111111111111112",
                            "X", "meteora-damm-v2", quote="REALmint"))
    assert p["token"] == "REALmint"


def test_parse_rejects_all_quote_or_malformed():
    assert snp.parse_pool({"attributes": {}, "relationships": {}}) is None
    both_quotes = _raw("P", "So11111111111111111111111111111111111111112", "X",
                       "raydium", quote="EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v")
    assert snp.parse_pool(both_quotes) is None
    assert snp.parse_pool("not-a-dict") is None


def test_record_dedups_by_token_and_prunes_old(monkeypatch, tmp_path):
    monkeypatch.setattr(snp, "DB", tmp_path / "snp.db")
    conn = snp._conn()
    r = snp.record([_raw("P1", "M1", "A", "raydium"),
                    _raw("P2", "M2", "B", "meteora-dbc")], now=NOW, conn=conn)
    assert r["inserted"] == 2

    # Same token again (a later poll) → not re-inserted; first detection frozen.
    again = snp.record([_raw("P1b", "M1", "A", "raydium")], now=NOW + timedelta(minutes=5), conn=conn)
    assert again["inserted"] == 0 and again["total"] == 2

    # A poll far in the future prunes the now-stale rows.
    pruned = snp.record([_raw("P3", "M3", "C", "raydium")],
                        now=NOW + timedelta(hours=snp.RETAIN_HOURS + 1), conn=conn)
    assert pruned["total"] == 1   # only the fresh M3 survives
    conn.close()


def test_recent_returns_launch_shaped_solana_events(monkeypatch, tmp_path):
    monkeypatch.setattr(snp, "DB", tmp_path / "snp.db")
    conn = snp._conn()
    snp.record([_raw("P1", "M1", "RAY", "raydium")], now=NOW, conn=conn)
    events = snp.recent(minutes=120, now=NOW + timedelta(minutes=10), conn=conn)
    assert len(events) == 1
    e = events[0]
    assert e["token"] == "M1" and e["chain"] == "solana" and e["symbol"] == "RAY"
    assert e["dex"] == "raydium" and "detected_at" in e
    # Beyond the window → excluded.
    assert snp.recent(minutes=5, now=NOW + timedelta(hours=1), conn=conn) == []
    conn.close()
