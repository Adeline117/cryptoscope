"""Airdrop workbench only exposes explicit official campaigns and owned-wallet facts."""
from datetime import datetime, timezone


def _campaign(**overrides):
    c = {"id": "proto-s1", "project": "Protocol", "chain": "ethereum",
         "official_url": "https://protocol.example/claim", "status": "claimable",
         "deadline": "2026-12-31T00:00:00+00:00", "estimated_cost_usd": 12,
         "wallets": ["0xowned"], "tasks": [{"name": "Swap", "evidence": "0xtx"}]}
    c.update(overrides)
    return c


def test_normalize_requires_an_official_https_url():
    from src.pipeline.airdrop_radar import normalize
    assert normalize(_campaign(official_url="http://protocol.example")) is None
    assert normalize(_campaign(official_url="")) is None


def test_claimable_campaign_needs_owned_wallet_and_evidence():
    from src.pipeline.airdrop_radar import normalize
    now = datetime(2026, 7, 13, tzinfo=timezone.utc)
    got = normalize(_campaign(), now=now)
    assert got["decision"] == "CLAIM_CHECK"
    assert got["evidence_state"] == "recorded"
    unknown = normalize(_campaign(wallets=[], tasks=[{"name": "Swap"}]), now=now)
    assert unknown["decision"] == "WATCH"
    assert unknown["evidence_state"] == "unknown"


def test_sync_records_campaign_without_private_data(tmp_path, monkeypatch):
    import src.pipeline.airdrop_radar as ar
    import src.pipeline.opportunity_ledger as ol
    monkeypatch.setattr(ol, "DB", tmp_path / "ledger.db")
    config = tmp_path / "airdrop.yaml"
    config.write_text("campaigns:\n  - id: proto-s1\n    project: Protocol\n    chain: ethereum\n    official_url: https://protocol.example/claim\n    status: active\n")
    got = ar.sync(config, now=datetime(2026, 7, 13, tzinfo=timezone.utc))
    assert got["configured"] == 1 and got["inserted"] == 1
    event = ol.active("airdrop")[0]
    assert event["wallet_count"] == 0 and event["evidence_state"] == "unknown"
