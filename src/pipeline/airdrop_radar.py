"""Airdrop workbench — official campaigns and owned-wallet evidence only.

There is no universal eligibility API. A generic scraper would turn rumours into
false rewards, and multi-account automation is intentionally out of scope. This
module therefore accepts only an explicit, auditable campaign watchlist and makes
missing wallet evidence visible as UNKNOWN.
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse
import json
import math
import os
import re
import urllib.error
import urllib.request

import yaml

from src.config import CONFIG_DIR
from src.pipeline.opportunity_ledger import active, record, save_outcome

WATCHLIST = CONFIG_DIR / "airdrop_watchlist.yaml"
VALID_STATUS = {"research", "active", "claimable", "claimed", "expired"}
# Trust roots are reviewed in code, not supplied beside the campaign URL.  A watchlist
# can select a page under one of these roots, but it cannot authorize a new domain.
TRUST_ROOTS = frozenset({"starknet.io"})
MAX_SOURCE_PAGE_BYTES = 2_000_000
EXPLORER_HOSTS = {
    "ethereum": {"etherscan.io"},
    "base": {"basescan.org"},
    "bsc": {"bscscan.com"},
    "solana": {"solscan.io"},
}
DEFAULT_RPCS = {
    "ethereum": "https://ethereum-rpc.publicnode.com",
    "base": "https://mainnet.base.org",
    "bsc": "https://bsc-dataseed.binance.org",
    "solana": "https://api.mainnet-beta.solana.com",
}


def _load(path: Path = WATCHLIST) -> list[dict]:
    try:
        raw = yaml.safe_load(path.read_text()) or {}
    except (OSError, yaml.YAMLError):
        return []
    campaigns = raw.get("campaigns", []) if isinstance(raw, dict) else []
    return campaigns if isinstance(campaigns, list) else []


def _https_url(value: object) -> tuple[str, str] | None:
    url = str(value or "")
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower().rstrip(".")
    if (parsed.scheme != "https" or not host or parsed.username is not None
            or parsed.password is not None):
        return None
    return url, host


def _host_allowed(host: str, domains: set[str]) -> bool:
    return any(host == domain or host.endswith("." + domain) for domain in domains)


def _trusted_source_urls(official_value: object, evidence_value: object) -> tuple[str, str, str] | None:
    """Return two HTTPS source URLs only when both share one code-audited trust root."""
    official = _https_url(official_value)
    evidence = _https_url(evidence_value)
    if not official or not evidence or official[0] == evidence[0]:
        return None
    for root in sorted(TRUST_ROOTS):
        if _host_allowed(official[1], {root}) and _host_allowed(evidence[1], {root}):
            return official[0], evidence[0], root
    return None


def _markers(value: object) -> tuple[str, ...]:
    if not isinstance(value, list):
        return ()
    return tuple(dict.fromkeys(
        marker.strip() for marker in value
        if isinstance(marker, str) and marker.strip()
    ))


def _campaign_markers(campaign: dict) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Support explicit per-page markers and one shared marker list."""
    shared = campaign.get("source_markers", campaign.get("markers"))
    if isinstance(shared, dict):
        shared_official = shared.get("official")
        shared_evidence = shared.get("evidence")
    else:
        shared_official = shared_evidence = shared
    official = _markers(campaign.get("official_markers", shared_official))
    evidence = _markers(campaign.get("source_evidence_markers", shared_evidence))
    return official, evidence


def _verify_source_page(url: str, markers: list[str] | tuple[str, ...]) -> bool:
    """Read one trusted page and require every configured marker in its content."""
    parsed = _https_url(url)
    roots = [root for root in TRUST_ROOTS
             if parsed and _host_allowed(parsed[1], {root})]
    clean_markers = _markers(list(markers))
    if not parsed or len(roots) != 1 or not clean_markers:
        return False
    request = urllib.request.Request(
        parsed[0], headers={"User-Agent": "CryptoScope/1.0", "Accept": "text/html,text/plain"}
    )
    try:
        with urllib.request.urlopen(request, timeout=12) as response:
            if getattr(response, "status", 200) != 200:
                return False
            final_url = response.geturl() if hasattr(response, "geturl") else parsed[0]
            final = _https_url(final_url)
            if not final or not _host_allowed(final[1], {roots[0]}):
                return False
            body = response.read(MAX_SOURCE_PAGE_BYTES + 1)
    except urllib.error.HTTPError as exc:
        exc.close()
        return False
    except Exception:
        return False
    if not isinstance(body, bytes) or len(body) > MAX_SOURCE_PAGE_BYTES:
        return False
    text = body.decode("utf-8", errors="ignore").casefold()
    return all(marker.casefold() in text for marker in clean_markers)


def _source_page_verified(url: str, markers: tuple[str, ...], verifier) -> bool:
    if not markers:
        return False
    try:
        return verifier(url, list(markers)) is True
    except Exception:
        return False


def _estimated_cost(campaign: dict) -> tuple[float | None, bool]:
    if "estimated_cost_usd" not in campaign or campaign.get("estimated_cost_usd") is None:
        return None, True
    value = campaign.get("estimated_cost_usd")
    if isinstance(value, bool):
        return None, False
    try:
        cost = float(value)
    except (TypeError, ValueError):
        return None, False
    return (cost, True) if math.isfinite(cost) and cost >= 0 else (None, False)


def _optional_nonnegative(campaign: dict, field: str) -> tuple[float | None, bool]:
    if field not in campaign or campaign.get(field) is None:
        return None, True
    value = campaign.get(field)
    if isinstance(value, bool):
        return None, False
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None, False
    return (number, True) if math.isfinite(number) and number >= 0 else (None, False)


def _transaction_url(value: object, chain: str) -> str | None:
    parsed = _https_url(value)
    allowed = EXPLORER_HOSTS.get(chain, set())
    if not parsed or parsed[1] not in allowed:
        return None
    path = urlparse(parsed[0]).path.rstrip("/")
    tx_id = path.rsplit("/", 1)[-1] if "/tx/" in path else ""
    valid = (bool(re.fullmatch(r"0x[0-9a-fA-F]{64}", tx_id))
             if chain != "solana" else bool(re.fullmatch(r"[1-9A-HJ-NP-Za-km-z]{64,88}", tx_id)))
    return parsed[0] if valid else None


def _rpc_url(chain: str) -> str | None:
    configured = os.getenv(f"RPC_{chain.upper()}", "").split(",", 1)[0].strip()
    return configured or DEFAULT_RPCS.get(chain)


def _rpc_json(url: str, method: str, params: list) -> dict:
    request = urllib.request.Request(
        url,
        data=json.dumps({"jsonrpc": "2.0", "id": 1, "method": method,
                         "params": params}).encode(),
        headers={"Content-Type": "application/json", "User-Agent": "CryptoScope/1.0"},
    )
    with urllib.request.urlopen(request, timeout=12) as response:
        data = json.loads(response.read().decode())
    return data if isinstance(data, dict) else {}


def _verify_transaction(tx_url: str, chain: str, fetch=_rpc_json) -> dict | None:
    """Require a successful mainnet transaction and derive its time from chain data."""
    rpc = _rpc_url(chain)
    tx_id = urlparse(tx_url).path.rstrip("/").rsplit("/", 1)[-1]
    if not rpc or not tx_id:
        return None
    try:
        if chain == "solana":
            response = fetch(rpc, "getTransaction", [tx_id, {
                "encoding": "json", "commitment": "confirmed",
                "maxSupportedTransactionVersion": 0,
            }])
            result = response.get("result")
            signatures = (((result or {}).get("transaction") or {}).get("signatures") or [])
            block_time = (result or {}).get("blockTime")
            if (not isinstance(result, dict) or tx_id not in signatures
                    or (result.get("meta") or {}).get("err") is not None
                    or not block_time):
                return None
            confirmed_at = datetime.fromtimestamp(int(block_time), tz=timezone.utc)
            return {"source": "solana_mainnet_rpc", "tx_id": tx_id,
                    "slot": result.get("slot"), "confirmed_at": confirmed_at.isoformat(),
                    "onchain_success": True}

        receipt_response = fetch(rpc, "eth_getTransactionReceipt", [tx_id])
        receipt = receipt_response.get("result")
        if (not isinstance(receipt, dict) or receipt.get("status") != "0x1"
                or str(receipt.get("transactionHash", "")).lower() != tx_id.lower()
                or not receipt.get("blockNumber")):
            return None
        block_number = receipt["blockNumber"]
        block_response = fetch(rpc, "eth_getBlockByNumber", [block_number, False])
        block = block_response.get("result")
        if not isinstance(block, dict) or not block.get("timestamp"):
            return None
        confirmed_at = datetime.fromtimestamp(int(block["timestamp"], 16), tz=timezone.utc)
        return {"source": f"{chain}_mainnet_rpc", "tx_id": tx_id,
                "block_number": int(block_number, 16),
                "confirmed_at": confirmed_at.isoformat(), "onchain_success": True}
    except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError):
        return None


def _timestamp(value: object) -> str | None:
    if value in (None, ""):
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        return None
    return dt.astimezone(timezone.utc).isoformat()


def _claim_outcome(campaign: dict, verifier=_verify_transaction) -> dict | None:
    """Accept a realized claim only with complete public evidence and actual cost."""
    raw = campaign.get("claim")
    if not isinstance(raw, dict):
        return None
    claimed_at = _timestamp(raw.get("claimed_at"))
    chain = str(raw.get("chain") or campaign.get("chain") or "")
    tx_url = _transaction_url(raw.get("tx_url"), chain)
    try:
        reward_usd = float(raw["reward_usd"])
        actual_cost_usd = float(raw["actual_cost_usd"])
    except (KeyError, TypeError, ValueError):
        return None
    if not claimed_at or not tx_url or reward_usd < 0 or actual_cost_usd < 0:
        return None
    verification = verifier(tx_url, chain)
    if not isinstance(verification, dict) or verification.get("onchain_success") is not True:
        return None
    return {
        "version": 2, "kind": "airdrop_claim",
        "claimed_at": verification["confirmed_at"],
        "reported_claimed_at": claimed_at,
        "tx_url": tx_url, "gross_reward_usd": reward_usd,
        "chain": chain,
        "actual_cost_usd": actual_cost_usd,
        "net_reward_usd": reward_usd - actual_cost_usd,
        "reward_is_claimed": True, "cost_is_actual": True,
        "transaction_verification": verification,
    }


def normalize(campaign: dict, now: datetime | None = None,
              claim_verifier=_verify_transaction,
              source_verifier=_verify_source_page) -> dict | None:
    """Validate a manually curated campaign without asserting eligibility."""
    if not isinstance(campaign, dict):
        return None
    now = now or datetime.now(timezone.utc)
    ident, project = str(campaign.get("id") or ""), str(campaign.get("project") or "")
    trusted_urls = _trusted_source_urls(
        campaign.get("official_url"), campaign.get("source_evidence_url")
    )
    status = campaign.get("status", "research")
    estimated_cost, cost_valid = _estimated_cost(campaign)
    capital_required, capital_valid = _optional_nonnegative(
        campaign, "capital_required_usd"
    )
    if (not ident or not project or not trusted_urls or status not in VALID_STATUS
            or not cost_valid or not capital_valid):
        return None
    url, source_evidence_url, trust_root = trusted_urls
    configured_root = campaign.get("trust_root")
    if configured_root is not None:
        configured_root = str(configured_root).strip().lower().rstrip(".")
        if configured_root != trust_root:
            return None
    deadline = campaign.get("deadline")
    if deadline:
        deadline = _timestamp(deadline)
        if not deadline:
            return None
        if datetime.fromisoformat(deadline) < now and status != "claimed":
            status = "expired"
    announced_at = _timestamp(campaign.get("announced_at"))
    if campaign.get("announced_at") and not announced_at:
        return None
    claim_outcome = _claim_outcome(campaign, verifier=claim_verifier)
    if status == "claimed" and not claim_outcome:
        return None
    wallets = [str(w) for w in campaign.get("wallets", []) if str(w).strip()]
    tasks = [t for t in campaign.get("tasks", []) if isinstance(t, dict) and t.get("name")]
    risk_notes = [str(note).strip() for note in campaign.get("risk_notes", [])
                  if isinstance(note, str) and note.strip()]
    official_markers, evidence_markers = _campaign_markers(campaign)
    official_verified = _source_page_verified(url, official_markers, source_verifier)
    evidence_verified = _source_page_verified(
        source_evidence_url, evidence_markers, source_verifier
    )
    source_verified = official_verified and evidence_verified
    source_state = "source_verified" if source_verified else "source_unverified"
    evidence_state = "recorded" if wallets and all(t.get("evidence") for t in tasks) else "unknown"
    # A claim page can be actionable; a task campaign still needs a controlled wallet
    # and explicit task evidence before it is anything more than research.
    decision = ("CLAIMED" if status == "claimed" and source_verified else
                "CLAIM_CHECK" if (status == "claimable" and evidence_state == "recorded"
                                  and source_verified)
                else "WATCH")
    return {
        "lane": "airdrop", "chain": campaign.get("chain", "multi"), "token": ident,
        "symbol": project, "source": "official campaign watchlist", "state": status,
        "decision": decision, "event_type": "airdrop_campaign", "official_url": url,
        "source_evidence_url": source_evidence_url, "trust_root": trust_root,
        "event_at": announced_at, "detected_at": now.isoformat(),
        "decision_at": now.isoformat(), "expires_at": deadline,
        "deadline": deadline, "estimated_cost_usd": estimated_cost,
        "capital_required_usd": capital_required,
        "kyc_required": campaign.get("kyc_required") is True,
        "risk_notes": risk_notes,
        "wallet_count": len(wallets), "task_count": len(tasks), "evidence_state": evidence_state,
        "official_state": source_state, "source_state": source_state,
        "source_verification": {
            "trust_root": trust_root,
            "checked_at": now.isoformat(),
            "official_page_verified": official_verified,
            "evidence_page_verified": evidence_verified,
            "official_marker_count": len(official_markers),
            "evidence_marker_count": len(evidence_markers),
        },
        "tasks": tasks, "claim_outcome": claim_outcome,
        "reasons": [f"代码信任根: {trust_root}", f"官方源验证: {source_state}",
                    f"资格证据: {evidence_state}"],
    }


def sync(path: Path = WATCHLIST, now: datetime | None = None,
         claim_verifier=_verify_transaction,
         source_verifier=_verify_source_page) -> dict:
    campaigns = _load(path)
    inserted = accepted = source_verified = source_unverified = rejected = 0
    for campaign in campaigns:
        event = normalize(campaign, now=now, claim_verifier=claim_verifier,
                          source_verifier=source_verifier)
        if not event:
            rejected += 1
            continue
        ident, new = record(event)
        if event.get("claim_outcome"):
            save_outcome(ident, event["claim_outcome"], "resolved")
        inserted += int(new)
        accepted += 1
        if event["source_state"] == "source_verified":
            source_verified += 1
        else:
            source_unverified += 1
    return {"configured": len(campaigns), "accepted": accepted,
            "source_verified": source_verified, "source_unverified": source_unverified,
            "rejected": rejected, "inserted": inserted, "events": active("airdrop"),
            "source": "official campaign watchlist"}


def view() -> dict:
    return {"events": active("airdrop"), "source": "official campaign watchlist"}
