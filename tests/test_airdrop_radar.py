"""Airdrop workbench only exposes explicit official campaigns and owned-wallet facts."""
from datetime import datetime, timezone


def _campaign(**overrides):
    c = {"id": "proto-s1", "project": "Protocol", "chain": "ethereum",
         "official_url": "https://protocol.example/claim", "status": "claimable",
         "official_domains": ["protocol.example"],
         "deadline": "2026-12-31T00:00:00+00:00", "estimated_cost_usd": 12,
         "wallets": ["0xowned"], "tasks": [{"name": "Swap", "evidence": "0xtx"}]}
    c.update(overrides)
    return c


def test_normalize_requires_an_official_https_url():
    from src.pipeline.airdrop_radar import normalize
    assert normalize(_campaign(official_url="http://protocol.example")) is None
    assert normalize(_campaign(official_url="")) is None
    assert normalize(_campaign(official_url="https://protocol.example.evil.test/claim")) is None
    assert normalize(_campaign(official_url="https://protocol.example@evil.test/claim")) is None
    assert normalize(_campaign(official_domains=[])) is None
    assert normalize(_campaign(official_url="https://claim.protocol.example/")) is not None


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
    config.write_text("campaigns:\n  - id: proto-s1\n    project: Protocol\n    chain: ethereum\n    official_url: https://protocol.example/claim\n    official_domains: [protocol.example]\n    status: active\n")
    got = ar.sync(config, now=datetime(2026, 7, 13, tzinfo=timezone.utc))
    assert got["configured"] == 1 and got["inserted"] == 1
    event = ol.active("airdrop")[0]
    assert event["wallet_count"] == 0 and event["evidence_state"] == "unknown"


def test_claim_requires_complete_public_evidence_and_settles_ledger(tmp_path, monkeypatch):
    import src.pipeline.airdrop_radar as ar
    import src.pipeline.opportunity_ledger as ol

    monkeypatch.setattr(ol, "DB", tmp_path / "ledger.db")
    config = tmp_path / "airdrop.yaml"
    tx_hash = "0x" + "a" * 64
    config.write_text(f"""campaigns:
  - id: proto-s1
    project: Protocol
    chain: ethereum
    official_url: https://protocol.example/claim
    official_domains: [protocol.example]
    status: claimed
    claim:
      claimed_at: 2026-07-13T12:00:00Z
      tx_url: https://etherscan.io/tx/{tx_hash}
      reward_usd: 125
      actual_cost_usd: 5
""")
    def verified(_url, _chain):
        return {"source": "ethereum_mainnet_rpc", "tx_id": tx_hash,
                "block_number": 123, "confirmed_at": "2026-07-13T12:01:00+00:00",
                "onchain_success": True}

    got = ar.sync(config, now=datetime(2026, 7, 14, tzinfo=timezone.utc),
                  claim_verifier=verified)
    assert got["inserted"] == 1
    row = ol.outcome_rows()[0]
    assert row["decision"] == "CLAIMED" and row["outcome_state"] == "resolved"
    assert row["outcome"]["net_reward_usd"] == 120
    assert row["outcome"]["cost_is_actual"] is True
    assert row["outcome"]["claimed_at"] == "2026-07-13T12:01:00+00:00"
    assert row["outcome"]["transaction_verification"]["block_number"] == 123
    assert "wallets" not in row["payload"]


def test_claimed_status_rejects_incomplete_or_ambiguous_claim():
    from src.pipeline.airdrop_radar import normalize

    now = datetime(2026, 7, 14, tzinfo=timezone.utc)
    assert normalize(_campaign(status="claimed", claim={"reward_usd": 100}), now=now) is None
    assert normalize(_campaign(
        status="claimed", claim={"claimed_at": "2026-07-13T12:00:00",
                                  "tx_url": "https://etherscan.io/tx/0xabc",
                                  "reward_usd": 100, "actual_cost_usd": 5}), now=now) is None


def test_claim_transaction_url_must_match_chain_explorer_and_hash_shape():
    from src.pipeline.airdrop_radar import normalize

    now = datetime(2026, 7, 14, tzinfo=timezone.utc)
    claim = {"claimed_at": "2026-07-13T12:00:00Z", "reward_usd": 100,
             "actual_cost_usd": 5, "tx_url": "https://evil.test/tx/" + "0x" + "a" * 64}
    assert normalize(_campaign(status="claimed", claim=claim), now=now) is None
    claim["tx_url"] = "https://etherscan.io/tx/0xabc"
    assert normalize(_campaign(status="claimed", claim=claim), now=now) is None
    claim["tx_url"] = "https://basescan.org/tx/" + "0x" + "a" * 64
    assert normalize(_campaign(status="claimed", claim=claim), now=now) is None


def test_claimed_status_rejects_nonexistent_or_failed_onchain_transaction():
    from src.pipeline.airdrop_radar import normalize

    tx_hash = "0x" + "a" * 64
    claim = {"claimed_at": "2026-07-13T12:00:00Z", "reward_usd": 100,
             "actual_cost_usd": 5, "tx_url": "https://etherscan.io/tx/" + tx_hash}
    campaign = _campaign(status="claimed", claim=claim)

    assert normalize(campaign, claim_verifier=lambda _url, _chain: None) is None
    assert normalize(campaign, claim_verifier=lambda _url, _chain: {
        "onchain_success": False,
    }) is None


def test_evm_transaction_verification_requires_success_and_chain_block_time(monkeypatch):
    from src.pipeline import airdrop_radar as ar

    tx_hash = "0x" + "a" * 64
    tx_url = "https://etherscan.io/tx/" + tx_hash
    calls = []

    def rpc(_url, method, params):
        calls.append((method, params))
        if method == "eth_getTransactionReceipt":
            return {"result": {"transactionHash": tx_hash, "status": "0x1",
                               "blockNumber": "0x7b"}}
        return {"result": {"timestamp": "0x668ff1c0"}}

    got = ar._verify_transaction(tx_url, "ethereum", fetch=rpc)
    assert got["onchain_success"] is True and got["block_number"] == 123
    assert [call[0] for call in calls] == ["eth_getTransactionReceipt", "eth_getBlockByNumber"]

    def failed(_url, _method, _params):
        return {"result": {"transactionHash": tx_hash, "status": "0x0",
                           "blockNumber": "0x7b"}}

    assert ar._verify_transaction(tx_url, "ethereum", fetch=failed) is None
