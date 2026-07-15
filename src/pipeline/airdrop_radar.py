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

import yaml

from src.config import CONFIG_DIR
from src.pipeline.opportunity_ledger import active, record, save_outcome

WATCHLIST = CONFIG_DIR / "airdrop_watchlist.yaml"
VALID_STATUS = {"research", "active", "claimable", "claimed", "expired"}


def _load(path: Path = WATCHLIST) -> list[dict]:
    try:
        raw = yaml.safe_load(path.read_text()) or {}
    except (OSError, yaml.YAMLError):
        return []
    return raw.get("campaigns", []) if isinstance(raw, dict) else []


def _official_url(value: object) -> str | None:
    url = str(value or "")
    parsed = urlparse(url)
    return url if parsed.scheme == "https" and parsed.netloc else None


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
    tx_url = _official_url(raw.get("tx_url"))
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
        "actual_cost_usd": actual_cost_usd,
        "net_reward_usd": reward_usd - actual_cost_usd,
        "reward_is_claimed": True, "cost_is_actual": True,
    }


def normalize(campaign: dict, now: datetime | None = None) -> dict | None:
    """Validate a manually curated campaign without asserting eligibility."""
    now = now or datetime.now(timezone.utc)
    ident, project = str(campaign.get("id") or ""), str(campaign.get("project") or "")
    url, status = _official_url(campaign.get("official_url")), campaign.get("status", "research")
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
