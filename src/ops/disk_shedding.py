"""Fail-safe scheduler load shedding when the workspace volume is nearly full.

The policy is deliberately an allow-list.  A newly registered scheduler job must
be classified explicitly before the scheduler can start; disk pressure can never
silently turn an unknown (possibly core) job off.
"""

from __future__ import annotations

from collections.abc import Iterable

from src.ops import health


# Expensive, non-core refreshes and legacy/operator views.  Skipping these does not
# delete or rewrite their last-good state; the next scheduled tick runs normally as
# soon as the workspace is healthy again.
DISK_SHED_AT_WARN = frozenset({
    "anomaly_check",
    "operator_export",
    "harvest_wallets",
    "anomaly_screen",
    "operator_hunt",
    "operator_id_push",
    "holder_snapshots",
    "yaobi_finder",
    "cluster_coverage",
    "cex_label_refresh",
    "label_verify",
    "holder_growth_screen",
    # HLP writes one tiny slow-moving JSON and is not part of the five core lanes,
    # so it is the first to shed under any disk pressure.
    "hlp_tracker",
    # The convergence ledger is a small free paper-validation of the offense line;
    # shed it early under disk pressure.
    "convergence_ledger",
})

# 庄家/operator detection jobs — resumed 2026-07 (user opted into the paid BSC/Base
# operator signal coverage, Moralis re-enabled). Shed at CRITICAL only.
DISK_SHED_AT_CRITICAL = frozenset({
    "accumulation_detection",
    "operator_sentinel",
    "funder_watch",
})

# Five-lane collection/publication, invalidation and exit monitoring, edge/outcome
# accounting, and operational correctness are never shed for disk pressure.
DISK_PROTECTED_JOBS = frozenset({
    "exit_monitor",
    "stage2_launch_detector",
    "health_summary",
    "self_audit",
    "board_export",
    "opportunity_export",
    "smart_wallet_watch",
    "perps_export",
    "launch_radar",
    "solana_launch_reconciliation",
    "launch_quote_refresh",
    "structure_radar",
    "perp_universe_refresh",
    "perp_cex_scan",
    "perp_mobilization",
    "early_accumulation",
    "second_leg_assess",
    "resolve_outcomes",
})

DISK_CLASSIFIED_JOBS = (
    DISK_SHED_AT_WARN | DISK_SHED_AT_CRITICAL | DISK_PROTECTED_JOBS
)


def validate_disk_job_policy(job_ids: Iterable[str]) -> None:
    """Require an exact policy classification for every active scheduler job."""
    active = frozenset(job_ids)
    missing = active - DISK_CLASSIFIED_JOBS
    stale = DISK_CLASSIFIED_JOBS - active
    if missing or stale:
        raise RuntimeError(
            "disk shedding policy does not match active jobs: "
            f"missing={sorted(missing)!r} stale={sorted(stale)!r}"
        )


def disk_shedding_decision(job_id: str, snapshot: dict | None = None) -> dict:
    """Return a side-effect-free, structured run/skip decision for one job.

    Disk measurement failure is fail-open: only a positively observed WARN or
    CRITICAL state may shed an explicitly listed job.
    """
    if job_id not in DISK_CLASSIFIED_JOBS:
        raise ValueError(f"unclassified scheduler job: {job_id}")

    if snapshot is None:
        try:
            snapshot = health._disk_health()
        except Exception as exc:  # defensive if the health implementation regresses
            snapshot = {
                "state": "unknown",
                "free_gib": None,
                "free_percent": None,
                "thresholds": {},
                "error": f"{type(exc).__name__}: {exc}"[:160],
            }

    state = str(snapshot.get("state", "unknown")).lower()
    if job_id in DISK_PROTECTED_JOBS:
        policy = "protected"
        skip = False
    elif job_id in DISK_SHED_AT_WARN:
        policy = "shed_at_warn"
        skip = state in {"warn", "critical"}
    else:
        policy = "shed_at_critical"
        skip = state == "critical"

    reason = None
    if skip:
        reason = (
            "workspace_disk_critical_non_core_shed"
            if state == "critical"
            else "workspace_disk_warn_low_priority_shed"
        )
    return {
        "skip": skip,
        "job_id": job_id,
        "disk_state": state,
        "disk_policy": policy,
        "reason": reason,
        "free_gib": snapshot.get("free_gib"),
        "free_percent": snapshot.get("free_percent"),
        "thresholds": snapshot.get("thresholds") or {},
        "health_error": snapshot.get("error"),
    }
