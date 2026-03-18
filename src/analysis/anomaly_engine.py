"""Statistical anomaly detection across collected data."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

import structlog

from src.collectors.base import CollectedItem

logger = structlog.get_logger()


@dataclass
class Anomaly:
    """A detected anomaly in the data."""

    item: CollectedItem
    anomaly_type: str
    severity: str  # "critical", "high", "medium"
    description: str
    metric_name: str
    metric_value: float
    threshold: float


# Thresholds for anomaly detection
THRESHOLDS = {
    "tvl_change_1d_pct": {"critical": 20, "high": 10, "medium": 5},
    "tvl_change_7d_pct": {"critical": 30, "high": 15, "medium": 8},
    "funding_rate": {"critical": 0.1, "high": 0.05, "medium": 0.03},
    "volume_spike_multiplier": {"critical": 5, "high": 3, "medium": 2},
    "github_commit_spike": {"critical": 50, "high": 20, "medium": 10},
    "governance_vote_turnout": {"critical": 0.8, "high": 0.5, "medium": 0.3},
}


def detect_tvl_anomalies(items: list[CollectedItem]) -> list[Anomaly]:
    """Detect unusual TVL changes in DeFi protocols."""
    anomalies = []
    for item in items:
        meta = item.metadata
        if meta.get("data_type") != "protocol_tvl":
            continue

        for period, field in [("1d", "tvl_change_1d"), ("7d", "tvl_change_7d")]:
            change = meta.get(field)
            if change is None:
                continue
            try:
                pct = abs(float(change))
            except (ValueError, TypeError):
                continue

            threshold_key = f"tvl_change_{period}_pct"
            thresholds = THRESHOLDS[threshold_key]

            severity = None
            if pct >= thresholds["critical"]:
                severity = "critical"
            elif pct >= thresholds["high"]:
                severity = "high"
            elif pct >= thresholds["medium"]:
                severity = "medium"

            if severity:
                direction = "increased" if float(change) > 0 else "decreased"
                anomalies.append(
                    Anomaly(
                        item=item,
                        anomaly_type="tvl_change",
                        severity=severity,
                        description=(
                            f"{meta.get('name', 'Unknown')} TVL {direction} by {pct:.1f}% "
                            f"in {period} (${meta.get('tvl', 0):,.0f})"
                        ),
                        metric_name=f"tvl_change_{period}",
                        metric_value=pct,
                        threshold=thresholds[severity],
                    )
                )

    return anomalies


def detect_governance_anomalies(items: list[CollectedItem]) -> list[Anomaly]:
    """Detect high-stakes governance proposals."""
    anomalies = []
    for item in items:
        meta = item.metadata
        if meta.get("data_type") != "governance_proposal":
            continue

        votes = meta.get("votes", 0)
        scores = meta.get("scores", [])
        scores_total = meta.get("scores_total", 0)

        # Detect close votes
        if len(scores) >= 2 and scores_total > 0:
            top_two = sorted(scores, reverse=True)[:2]
            if top_two[1] > 0:
                ratio = top_two[0] / top_two[1]
                if ratio < 1.5:  # Close vote
                    anomalies.append(
                        Anomaly(
                            item=item,
                            anomaly_type="close_governance_vote",
                            severity="high",
                            description=(
                                f"Close vote: {item.title} — "
                                f"{votes} votes, lead ratio {ratio:.2f}"
                            ),
                            metric_name="vote_ratio",
                            metric_value=ratio,
                            threshold=1.5,
                        )
                    )

        # Detect high-turnout votes
        if votes > 1000:
            anomalies.append(
                Anomaly(
                    item=item,
                    anomaly_type="high_turnout_vote",
                    severity="medium",
                    description=f"High turnout: {item.title} — {votes} votes",
                    metric_name="vote_count",
                    metric_value=votes,
                    threshold=1000,
                )
            )

    return anomalies


def detect_volume_anomalies(items: list[CollectedItem]) -> list[Anomaly]:
    """Detect unusual volume spikes."""
    anomalies = []
    thresholds = THRESHOLDS["volume_spike_multiplier"]

    for item in items:
        meta = item.metadata
        if meta.get("data_type") not in ("exchange_volume", "dex_volume", "volume"):
            continue

        multiplier = meta.get("volume_multiplier") or meta.get("volume_spike")
        if multiplier is None:
            continue
        try:
            multiplier = float(multiplier)
        except (ValueError, TypeError):
            continue

        severity = None
        if multiplier >= thresholds["critical"]:
            severity = "critical"
        elif multiplier >= thresholds["high"]:
            severity = "high"
        elif multiplier >= thresholds["medium"]:
            severity = "medium"

        if severity:
            anomalies.append(
                Anomaly(
                    item=item,
                    anomaly_type="volume_spike",
                    severity=severity,
                    description=(
                        f"{meta.get('name', 'Unknown')} volume spiked "
                        f"{multiplier:.1f}x above average"
                    ),
                    metric_name="volume_spike_multiplier",
                    metric_value=multiplier,
                    threshold=thresholds[severity],
                )
            )

    return anomalies


def detect_funding_rate_anomalies(items: list[CollectedItem]) -> list[Anomaly]:
    """Detect extreme funding rates."""
    anomalies = []
    thresholds = THRESHOLDS["funding_rate"]

    for item in items:
        meta = item.metadata
        if "funding" not in (meta.get("data_type") or ""):
            continue

        rate = meta.get("funding_rate") or meta.get("rate")
        if rate is None:
            continue
        try:
            rate = float(rate)
        except (ValueError, TypeError):
            continue

        abs_rate = abs(rate)
        severity = None
        if abs_rate >= thresholds["critical"]:
            severity = "critical"
        elif abs_rate >= thresholds["high"]:
            severity = "high"
        elif abs_rate >= thresholds["medium"]:
            severity = "medium"

        if severity:
            direction = "positive" if rate > 0 else "negative"
            anomalies.append(
                Anomaly(
                    item=item,
                    anomaly_type="funding_rate_extreme",
                    severity=severity,
                    description=(
                        f"{meta.get('name', 'Unknown')} funding rate is "
                        f"extremely {direction} at {rate:.4f}"
                    ),
                    metric_name="funding_rate",
                    metric_value=abs_rate,
                    threshold=thresholds[severity],
                )
            )

    return anomalies


def detect_github_anomalies(items: list[CollectedItem]) -> list[Anomaly]:
    """Detect unusual spikes in GitHub commit activity."""
    anomalies = []
    thresholds = THRESHOLDS["github_commit_spike"]

    for item in items:
        meta = item.metadata
        if meta.get("data_type") != "github_commit":
            continue

        count = meta.get("commit_count") or meta.get("commits")
        if count is None:
            continue
        try:
            count = float(count)
        except (ValueError, TypeError):
            continue

        severity = None
        if count >= thresholds["critical"]:
            severity = "critical"
        elif count >= thresholds["high"]:
            severity = "high"
        elif count >= thresholds["medium"]:
            severity = "medium"

        if severity:
            anomalies.append(
                Anomaly(
                    item=item,
                    anomaly_type="github_commit_spike",
                    severity=severity,
                    description=(
                        f"{meta.get('name', 'Unknown')} had {int(count)} commits "
                        f"(spike detected)"
                    ),
                    metric_name="github_commit_count",
                    metric_value=count,
                    threshold=thresholds[severity],
                )
            )

    return anomalies


def detect_all_anomalies(items: list[CollectedItem]) -> list[Anomaly]:
    """Run all anomaly detectors and return sorted by severity."""
    anomalies = []
    anomalies.extend(detect_tvl_anomalies(items))
    anomalies.extend(detect_governance_anomalies(items))
    anomalies.extend(detect_volume_anomalies(items))
    anomalies.extend(detect_funding_rate_anomalies(items))
    anomalies.extend(detect_github_anomalies(items))

    severity_order = {"critical": 0, "high": 1, "medium": 2}
    anomalies.sort(key=lambda a: severity_order.get(a.severity, 3))

    logger.info("anomaly_scan_complete", total=len(anomalies))
    return anomalies
