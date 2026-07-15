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
import re

import yaml

from src.config import CONFIG_DIR
from src.pipeline.opportunity_ledger import active, record, save_outcome

WATCHLIST = CONFIG_DIR / "airdrop_watchlist.yaml"
VALID_STATUS = {"research", "active", "claimable", "claimed", "expired"}
EXPLORER_HOSTS = {
    "ethereum": {"etherscan.io"},
    "base": {"basescan.org"},
    "bsc": {"bscscan.com"},
    "solana": {"solscan.io"},
}


def _load(path: Path = WATCHLIST) -> list[dict]:
    try:
        raw = yaml.safe_load(path.read_text()) or {}
    except (OSError, yaml.YAMLError):
        return []
    return raw.get("campaigns", []) if isinstance(raw, dict) else []


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


def _official_url(value: object, domains: object) -> str | None:
    parsed = _https_url(value)
    allowed = {
        str(domain).strip().lower().rstrip(".")
        for domain in (domains if isinstance(domains, list) else [])
        if str(domain).strip()
    }
    if not parsed or not allowed or not _host_allowed(parsed[1], allowed):
        return None
    return parsed[0]


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


def _claim_outcome(campaign: dict) -> dict | None:
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
    return {
        "version": 1, "kind": "airdrop_claim", "claimed_at": claimed_at,
        "tx_url": tx_url, "gross_reward_usd": reward_usd,
        "chain": chain,
        "actual_cost_usd": actual_cost_usd,
        "net_reward_usd": reward_usd - actual_cost_usd,
        "reward_is_claimed": True, "cost_is_actual": True,
    }


def normalize(campaign: dict, now: datetime | None = None) -> dict | None:
    """Validate a manually curated campaign without asserting eligibility."""
    now = now or datetime.now(timezone.utc)
    ident, project = str(campaign.get("id") or ""), str(campaign.get("project") or "")
    url = _official_url(campaign.get("official_url"), campaign.get("official_domains"))
    status = campaign.get("status", "research")
    if not ident or not project or not url or status not in VALID_STATUS:
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
    claim_outcome = _claim_outcome(campaign)
    if status == "claimed" and not claim_outcome:
        return None
    wallets = [str(w) for w in campaign.get("wallets", []) if str(w).strip()]
    tasks = [t for t in campaign.get("tasks", []) if isinstance(t, dict) and t.get("name")]
    # A claim page can be actionable; a task campaign still needs a controlled wallet
    # and explicit task evidence before it is anything more than research.
    decision = ("CLAIMED" if status == "claimed" else
                "CLAIM_CHECK" if status == "claimable" and wallets else "WATCH")
    evidence_state = "recorded" if wallets and all(t.get("evidence") for t in tasks) else "unknown"
    return {
        "lane": "airdrop", "chain": campaign.get("chain", "multi"), "token": ident,
        "symbol": project, "source": "official campaign watchlist", "state": status,
        "decision": decision, "event_type": "airdrop_campaign", "official_url": url,
        "event_at": announced_at, "detected_at": now.isoformat(),
        "decision_at": now.isoformat(), "expires_at": deadline,
        "deadline": deadline, "estimated_cost_usd": float(campaign.get("estimated_cost_usd") or 0),
        "wallet_count": len(wallets), "task_count": len(tasks), "evidence_state": evidence_state,
        "official_state": "domain_allowlisted",
        "tasks": tasks, "claim_outcome": claim_outcome,
        "reasons": ["仅官方链接", f"资格证据: {evidence_state}"],
    }


def sync(path: Path = WATCHLIST, now: datetime | None = None) -> dict:
    campaigns = _load(path)
    inserted = 0
    for campaign in campaigns:
        event = normalize(campaign, now=now)
        if event:
            ident, new = record(event)
            if event.get("claim_outcome"):
                save_outcome(ident, event["claim_outcome"], "resolved")
            inserted += int(new)
    return {"configured": len(campaigns), "inserted": inserted, "events": active("airdrop"),
            "source": "official campaign watchlist"}


def view() -> dict:
    return {"events": active("airdrop"), "source": "official campaign watchlist"}
