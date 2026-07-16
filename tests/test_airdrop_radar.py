"""Airdrop workbench only exposes explicit official campaigns and owned-wallet facts."""
from datetime import datetime, timezone


def _campaign(**overrides):
    c = {"id": "starknet-s1", "project": "Starknet", "chain": "ethereum",
         "official_url": "https://www.starknet.io/claim", "status": "claimable",
         "source_evidence_url": "https://www.starknet.io/blog/season-1",
         "official_markers": ["Starknet", "claim"],
         "source_evidence_markers": ["Starknet", "Season 1"],
         "deadline": "2026-12-31T00:00:00+00:00", "estimated_cost_usd": 12,
         "wallets": ["0xowned"], "tasks": [{"name": "Swap", "evidence": "0xtx"}]}
    c.update(overrides)
    return c


def _verified_source(_url, markers):
    return bool(markers)


def test_normalize_requires_two_https_urls_under_a_code_trust_root():
    from src.pipeline.airdrop_radar import normalize

    assert normalize(_campaign(official_url="http://starknet.io"),
                     source_verifier=_verified_source) is None
    assert normalize(_campaign(official_url=""), source_verifier=_verified_source) is None
    assert normalize(_campaign(source_evidence_url=""),
                     source_verifier=_verified_source) is None
    assert normalize(_campaign(
        source_evidence_url="https://www.starknet.io/claim"
    ), source_verifier=_verified_source) is None
    assert normalize(_campaign(official_url="https://starknet.io.evil.test/claim"),
                     source_verifier=_verified_source) is None
    assert normalize(_campaign(official_url="https://starknet.io@evil.test/claim"),
                     source_verifier=_verified_source) is None
    assert normalize(_campaign(source_evidence_url="https://evil.test/evidence"),
                     source_verifier=_verified_source) is None
    assert normalize(_campaign(trust_root="evil.test"),
                     source_verifier=_verified_source) is None
    # A campaign-controlled domain list cannot authorize its own unreviewed host.
    assert normalize(_campaign(
        official_url="https://protocol.example/claim",
        source_evidence_url="https://protocol.example/evidence",
        official_domains=["protocol.example"],
    ), source_verifier=_verified_source) is None
    accepted = normalize(_campaign(
        official_url="https://claim.starknet.io/",
        official_domains=["evil.test"],
    ), source_verifier=_verified_source)
    assert accepted["trust_root"] == "starknet.io"


def test_source_pages_and_markers_fail_closed_without_rejecting_watch():
    from src.pipeline.airdrop_radar import normalize

    calls = []

    def verified(url, markers):
        calls.append((url, markers))
        return True

    got = normalize(_campaign(), source_verifier=verified)
    assert got["decision"] == "CLAIM_CHECK"
    assert got["official_state"] == got["source_state"] == "source_verified"
    assert got["source_verification"]["checked_at"]
    assert [call[0] for call in calls] == [
        "https://www.starknet.io/claim",
        "https://www.starknet.io/blog/season-1",
    ]

    unverified = normalize(_campaign(), source_verifier=lambda _url, _markers: False)
    assert unverified["decision"] == "WATCH"
    assert unverified["source_state"] == "source_unverified"
    assert unverified["source_verification"]["official_page_verified"] is False

    def offline(_url, _markers):
        raise OSError("offline")

    assert normalize(_campaign(), source_verifier=offline)["decision"] == "WATCH"
    missing_markers = normalize(
        _campaign(official_markers=[]), source_verifier=verified
    )
    assert missing_markers["decision"] == "WATCH"
    assert missing_markers["source_state"] == "source_unverified"


def test_claimable_campaign_needs_owned_wallet_and_evidence():
    from src.pipeline.airdrop_radar import normalize
    now = datetime(2026, 7, 13, tzinfo=timezone.utc)
    got = normalize(_campaign(), now=now, source_verifier=_verified_source)
    assert got["decision"] == "CLAIM_CHECK"
    assert got["evidence_state"] == "recorded"
    unknown = normalize(_campaign(wallets=[], tasks=[{"name": "Swap"}]), now=now,
                        source_verifier=_verified_source)
    assert unknown["decision"] == "WATCH"
    assert unknown["evidence_state"] == "unknown"
    missing_task_evidence = normalize(
        _campaign(tasks=[{"name": "Swap"}]), now=now,
        source_verifier=_verified_source,
    )
    assert missing_task_evidence["decision"] == "WATCH"


def test_estimated_cost_is_unknown_when_missing_and_rejects_invalid_values():
    from src.pipeline.airdrop_radar import normalize

    missing = _campaign()
    del missing["estimated_cost_usd"]
    assert normalize(missing, source_verifier=_verified_source)["estimated_cost_usd"] is None
    assert normalize(_campaign(estimated_cost_usd=None),
                     source_verifier=_verified_source)["estimated_cost_usd"] is None
    assert normalize(_campaign(estimated_cost_usd="0"),
                     source_verifier=_verified_source)["estimated_cost_usd"] == 0
    for invalid in ("unknown", -1, True, float("nan"), float("inf")):
        assert normalize(_campaign(estimated_cost_usd=invalid),
                         source_verifier=_verified_source) is None


def test_capital_requirement_and_campaign_risks_are_distinct_from_cost():
    from src.pipeline.airdrop_radar import normalize

    got = normalize(_campaign(
        estimated_cost_usd=None, capital_required_usd=70, kyc_required=True,
        risk_notes=["Reward not guaranteed", "Sybil prohibited", ""],
    ), source_verifier=_verified_source)

    assert got["estimated_cost_usd"] is None
    assert got["capital_required_usd"] == 70
    assert got["kyc_required"] is True
    assert got["risk_notes"] == ["Reward not guaranteed", "Sybil prohibited"]
    for invalid in (-1, True, float("nan")):
        assert normalize(_campaign(capital_required_usd=invalid),
                         source_verifier=_verified_source) is None


def test_sync_reports_verified_unverified_and_rejected_coverage(tmp_path, monkeypatch):
    import src.pipeline.airdrop_radar as ar
    import src.pipeline.opportunity_ledger as ol
    monkeypatch.setattr(ol, "DB", tmp_path / "ledger.db")
    config = tmp_path / "airdrop.yaml"
    config.write_text("""campaigns:
  - id: starknet-verified
    project: Starknet Verified
    chain: ethereum
    official_url: https://starknet.io/claim-ok
    source_evidence_url: https://starknet.io/evidence-ok
    official_markers: [Starknet, claim]
    source_evidence_markers: [Starknet, evidence]
    official_domains: [evil.test]
    status: active
  - id: starknet-unverified
    project: Starknet Unverified
    chain: ethereum
    official_url: https://starknet.io/claim-fail
    source_evidence_url: https://starknet.io/evidence-fail
    source_markers: [Starknet]
    status: claimable
    wallets: [owned]
  - id: rejected-domain
    project: Rejected
    chain: ethereum
    official_url: https://evil.test/claim
    source_evidence_url: https://evil.test/evidence
    source_markers: [Rejected]
    status: active
""")

    def verify(url, _markers):
        return url.endswith("-ok")

    got = ar.sync(config, now=datetime(2026, 7, 13, tzinfo=timezone.utc),
                  source_verifier=verify)
    assert {key: got[key] for key in (
        "configured", "accepted", "source_verified", "source_unverified",
        "rejected", "inserted",
    )} == {
        "configured": 3, "accepted": 2, "source_verified": 1,
        "source_unverified": 1, "rejected": 1, "inserted": 2,
    }
    events = {event["token"]: event for event in ol.active("airdrop")}
    assert events["starknet-verified"]["estimated_cost_usd"] is None
    assert events["starknet-verified"]["source_state"] == "source_verified"
    assert events["starknet-unverified"]["decision"] == "WATCH"
    assert events["starknet-unverified"]["source_state"] == "source_unverified"
    assert all("wallets" not in event for event in events.values())


def test_claim_requires_complete_public_evidence_and_settles_ledger(tmp_path, monkeypatch):
    import src.pipeline.airdrop_radar as ar
    import src.pipeline.opportunity_ledger as ol

    monkeypatch.setattr(ol, "DB", tmp_path / "ledger.db")
    config = tmp_path / "airdrop.yaml"
    tx_hash = "0x" + "a" * 64
    config.write_text(f"""campaigns:
  - id: starknet-s1
    project: Starknet
    chain: ethereum
    official_url: https://starknet.io/claim
    source_evidence_url: https://starknet.io/blog/season-1
    source_markers: [Starknet]
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
                  claim_verifier=verified, source_verifier=_verified_source)
    assert got["inserted"] == 1
    assert got["accepted"] == got["source_verified"] == 1
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
    assert normalize(_campaign(status="claimed", claim={"reward_usd": 100}), now=now,
                     source_verifier=_verified_source) is None
    assert normalize(_campaign(
        status="claimed", claim={"claimed_at": "2026-07-13T12:00:00",
                                  "tx_url": "https://etherscan.io/tx/0xabc",
                                  "reward_usd": 100, "actual_cost_usd": 5}), now=now,
                     source_verifier=_verified_source) is None


def test_claim_transaction_url_must_match_chain_explorer_and_hash_shape():
    from src.pipeline.airdrop_radar import normalize

    now = datetime(2026, 7, 14, tzinfo=timezone.utc)
    claim = {"claimed_at": "2026-07-13T12:00:00Z", "reward_usd": 100,
             "actual_cost_usd": 5, "tx_url": "https://evil.test/tx/" + "0x" + "a" * 64}
    assert normalize(_campaign(status="claimed", claim=claim), now=now,
                     source_verifier=_verified_source) is None
    claim["tx_url"] = "https://etherscan.io/tx/0xabc"
    assert normalize(_campaign(status="claimed", claim=claim), now=now,
                     source_verifier=_verified_source) is None
    claim["tx_url"] = "https://basescan.org/tx/" + "0x" + "a" * 64
    assert normalize(_campaign(status="claimed", claim=claim), now=now,
                     source_verifier=_verified_source) is None


def test_starknet_claim_url_requires_an_exact_mainnet_explorer_and_canonical_felt():
    from src.pipeline.airdrop_radar import _transaction_url

    tx_hash = "0x" + "a" * 63
    assert _transaction_url(
        f"https://voyager.online/tx/{tx_hash}", "starknet"
    ) == f"https://voyager.online/tx/{tx_hash}"
    assert _transaction_url(
        f"https://starkscan.co/tx/{tx_hash}", "starknet"
    ) == f"https://starkscan.co/tx/{tx_hash}"
    assert _transaction_url("https://voyager.online/tx/0x0", "starknet")

    assert _transaction_url(
        f"https://sepolia.voyager.online/tx/{tx_hash}", "starknet"
    ) is None
    assert _transaction_url(
        f"https://voyager.online.evil.test/tx/{tx_hash}", "starknet"
    ) is None
    assert _transaction_url(
        "https://voyager.online/tx/0x0" + "a" * 62, "starknet"
    ) is None
    assert _transaction_url(
        "https://voyager.online/tx/0x" + "a" * 64, "starknet"
    ) is None


def test_claimed_status_rejects_nonexistent_or_failed_onchain_transaction():
    from src.pipeline.airdrop_radar import normalize

    tx_hash = "0x" + "a" * 64
    claim = {"claimed_at": "2026-07-13T12:00:00Z", "reward_usd": 100,
             "actual_cost_usd": 5, "tx_url": "https://etherscan.io/tx/" + tx_hash}
    campaign = _campaign(status="claimed", claim=claim)

    assert normalize(campaign, claim_verifier=lambda _url, _chain: None,
                     source_verifier=_verified_source) is None
    assert normalize(campaign, claim_verifier=lambda _url, _chain: {
        "onchain_success": False,
    }, source_verifier=_verified_source) is None


def test_verified_claim_remains_auditable_but_not_actionable_when_source_is_offline():
    from src.pipeline.airdrop_radar import normalize

    tx_hash = "0x" + "a" * 64
    claim = {"claimed_at": "2026-07-13T12:00:00Z", "reward_usd": 100,
             "actual_cost_usd": 5, "tx_url": "https://etherscan.io/tx/" + tx_hash}

    def verified_transaction(_url, _chain):
        return {"source": "ethereum_mainnet_rpc", "tx_id": tx_hash,
                "block_number": 123, "confirmed_at": "2026-07-13T12:01:00+00:00",
                "onchain_success": True}

    event = normalize(
        _campaign(status="claimed", claim=claim),
        claim_verifier=verified_transaction,
        source_verifier=lambda _url, _markers: False,
    )
    assert event["claim_outcome"]["transaction_verification"]["onchain_success"] is True
    assert event["decision"] == "WATCH"
    assert event["source_state"] == "source_unverified"


def test_default_source_verifier_reads_content_and_rejects_bad_redirect(monkeypatch):
    from src.pipeline import airdrop_radar as ar

    state = {"body": b"Starknet Season 1 claim", "final": "https://starknet.io/claim"}

    class Response:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def geturl(self):
            return state["final"]

        def read(self, _limit):
            return state["body"]

    monkeypatch.setattr(ar.urllib.request, "urlopen", lambda *_args, **_kwargs: Response())
    assert ar._verify_source_page(
        "https://starknet.io/claim", ["Starknet", "Season 1"]
    ) is True
    assert ar._verify_source_page(
        "https://starknet.io/claim", ["missing marker"]
    ) is False
    state["final"] = "https://evil.test/claim"
    assert ar._verify_source_page("https://starknet.io/claim", ["Starknet"]) is False


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


def test_starknet_transaction_verification_cross_checks_mainnet_receipt_and_block():
    from src.pipeline import airdrop_radar as ar

    tx_hash = "0x" + "a" * 63
    block_hash = "0x" + "b" * 63
    tx_url = "https://voyager.online/tx/" + tx_hash
    calls = []

    def rpc(_url, method, params):
        calls.append((method, params))
        if method == "starknet_chainId":
            return {"result": ar.STARKNET_MAINNET_CHAIN_ID}
        if method == "starknet_getTransactionReceipt":
            return {"result": {
                "transaction_hash": tx_hash,
                "execution_status": "SUCCEEDED",
                "finality_status": "ACCEPTED_ON_L2",
                "block_hash": block_hash,
                "block_number": 123,
            }}
        return {"result": {
            "status": "ACCEPTED_ON_L1",
            "block_hash": block_hash,
            "block_number": 123,
            "timestamp": 1720717760,
            "transactions": ["0x1", tx_hash],
        }}

    got = ar._verify_transaction(tx_url, "starknet", fetch=rpc)

    assert got == {
        "source": "starknet_mainnet_rpc",
        "tx_id": tx_hash,
        "chain_id": ar.STARKNET_MAINNET_CHAIN_ID,
        "block_hash": block_hash,
        "block_number": 123,
        "finality_status": "ACCEPTED_ON_L1",
        "execution_status": "SUCCEEDED",
        "confirmed_at": "2024-07-11T17:09:20+00:00",
        "onchain_success": True,
        "verification_scope": "transaction_execution_only",
        "campaign_semantics_verified": False,
    }
    assert calls == [
        ("starknet_chainId", []),
        ("starknet_getTransactionReceipt", {"transaction_hash": tx_hash}),
        ("starknet_getBlockWithTxHashes", [{"block_hash": block_hash}]),
    ]


def test_starknet_transaction_verification_rejects_wrong_chain_or_unsettled_execution():
    from src.pipeline import airdrop_radar as ar

    tx_hash = "0x" + "a" * 63
    block_hash = "0x" + "b" * 63
    tx_url = "https://starkscan.co/tx/" + tx_hash

    def verify(*, chain_id=ar.STARKNET_MAINNET_CHAIN_ID,
               execution="SUCCEEDED", finality="ACCEPTED_ON_L2"):
        def rpc(_url, method, _params):
            if method == "starknet_chainId":
                return {"result": chain_id}
            if method == "starknet_getTransactionReceipt":
                return {"result": {
                    "transaction_hash": tx_hash,
                    "execution_status": execution,
                    "finality_status": finality,
                    "block_hash": block_hash,
                    "block_number": 123,
                }}
            return {"result": {
                "status": "ACCEPTED_ON_L2", "block_hash": block_hash,
                "block_number": 123, "timestamp": 1720717760,
                "transactions": [tx_hash],
            }}
        return ar._verify_transaction(tx_url, "starknet", fetch=rpc)

    assert verify(chain_id="0x534e5f5345504f4c4941") is None
    assert verify(execution="REVERTED") is None
    assert verify(finality="PRE_CONFIRMED") is None
    assert verify(finality="UNKNOWN") is None


def test_starknet_transaction_verification_rejects_inconsistent_block_evidence():
    from src.pipeline import airdrop_radar as ar

    tx_hash = "0x" + "a" * 63
    other_tx = "0x" + "c" * 63
    block_hash = "0x" + "b" * 63
    tx_url = "https://voyager.online/tx/" + tx_hash

    def verify(receipt_changes=None, block_changes=None):
        receipt = {
            "transaction_hash": tx_hash, "execution_status": "SUCCEEDED",
            "finality_status": "ACCEPTED_ON_L1", "block_hash": block_hash,
            "block_number": 123,
        }
        block = {
            "status": "ACCEPTED_ON_L1", "block_hash": block_hash,
            "block_number": 123, "timestamp": 1720717760,
            "transactions": [tx_hash],
        }
        receipt.update(receipt_changes or {})
        block.update(block_changes or {})

        def rpc(_url, method, _params):
            if method == "starknet_chainId":
                return {"result": ar.STARKNET_MAINNET_CHAIN_ID}
            if method == "starknet_getTransactionReceipt":
                return {"result": receipt}
            return {"result": block}
        return ar._verify_transaction(tx_url, "starknet", fetch=rpc)

    assert verify(receipt_changes={"transaction_hash": other_tx}) is None
    assert verify(receipt_changes={"block_hash": "0x0"}) is None
    assert verify(block_changes={"block_hash": "0x" + "d" * 63}) is None
    assert verify(block_changes={"block_number": 124}) is None
    assert verify(block_changes={"block_number": 123.0}) is None
    assert verify(block_changes={"transactions": [other_tx]}) is None
    assert verify(block_changes={"status": "ACCEPTED_ON_L2"}) is None
    assert verify(block_changes={"status": "PRE_CONFIRMED"}) is None
    assert verify(block_changes={"timestamp": 0}) is None
    assert verify(block_changes={"timestamp": True}) is None

    def offline(_url, _method, _params):
        raise OSError("offline")

    assert ar._verify_transaction(tx_url, "starknet", fetch=offline) is None
