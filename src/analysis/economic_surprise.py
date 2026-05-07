"""Economic surprise tracker.

Compares actual economic data releases (from FRED) against market consensus
estimates to detect surprises that may impact crypto markets.

Approach:
1. A YAML config file (config/economic_releases.yaml) stores upcoming releases
   with their consensus estimates and historical standard deviations.
2. When actual data arrives via FRED CollectedItems, we compute:
       z_score = (actual - consensus) / historical_std
3. If |z_score| > 1.5, a high-priority surprise alert is generated.

Usage:
    from src.analysis.economic_surprise import EconomicSurpriseTracker
    tracker = EconomicSurpriseTracker()
    surprises = tracker.detect_surprises(fred_items)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import structlog
import yaml

from src.collectors.base import CollectedItem
from src.config import CONFIG_DIR

logger = structlog.get_logger()

RELEASES_CONFIG = CONFIG_DIR / "economic_releases.yaml"

# Z-score thresholds
Z_SCORE_HIGH = 1.5
Z_SCORE_EXTREME = 2.5


@dataclass
class EconomicSurprise:
    """A detected economic data surprise."""

    series_id: str
    name: str
    actual: float
    consensus: float
    z_score: float
    severity: str  # "extreme", "high", "moderate"
    direction: str  # "above" or "below"
    crypto_impact: str
    item: CollectedItem

    @property
    def description(self) -> str:
        return (
            f"{self.name}: actual {self.actual:,.2f} vs consensus {self.consensus:,.2f} "
            f"(z={self.z_score:+.2f}, {self.direction} expectations) — {self.crypto_impact}"
        )


@dataclass
class ReleaseConfig:
    """Configuration for a tracked economic release."""

    fred_series: str
    name: str
    consensus: float | None
    historical_std: float
    release_date: str | None
    frequency: str
    crypto_impact: str


class EconomicSurpriseTracker:
    """Track economic data surprises by comparing actuals vs consensus.

    Loads consensus estimates and historical standard deviations from a
    YAML config file, then scores incoming FRED data against them.
    """

    def __init__(self, config_path: Path | None = None):
        self.config_path = config_path or RELEASES_CONFIG
        self.releases: dict[str, ReleaseConfig] = {}
        self.log = logger.bind(component="economic_surprise")
        self._load_config()

    def _load_config(self) -> None:
        """Load release configurations from YAML."""
        try:
            with open(self.config_path) as f:
                data = yaml.safe_load(f)

            releases_list = data.get("releases", [])
            for entry in releases_list:
                series_id = entry.get("fred_series")
                if not series_id:
                    continue
                self.releases[series_id] = ReleaseConfig(
                    fred_series=series_id,
                    name=entry.get("name", series_id),
                    consensus=entry.get("consensus"),
                    historical_std=float(entry.get("historical_std", 1.0)),
                    release_date=entry.get("release_date"),
                    frequency=entry.get("frequency", "unknown"),
                    crypto_impact=entry.get("crypto_impact", ""),
                )

            self.log.info("config_loaded", releases=len(self.releases))
        except FileNotFoundError:
            self.log.warning("config_not_found", path=str(self.config_path))
        except Exception as e:
            self.log.error("config_load_error", error=str(e))

    def reload_config(self) -> None:
        """Reload the YAML config (e.g., after updating consensus values)."""
        self.releases.clear()
        self._load_config()

    def update_consensus(self, series_id: str, consensus: float) -> None:
        """Update consensus for a series in memory and persist to YAML.

        This allows programmatic updates before a release.
        """
        if series_id in self.releases:
            self.releases[series_id].consensus = consensus
            self._persist_config()
            self.log.info("consensus_updated", series=series_id, consensus=consensus)

    def _persist_config(self) -> None:
        """Write current release configs back to YAML."""
        try:
            releases_list = []
            for rc in self.releases.values():
                releases_list.append({
                    "fred_series": rc.fred_series,
                    "name": rc.name,
                    "consensus": rc.consensus,
                    "historical_std": rc.historical_std,
                    "release_date": rc.release_date,
                    "frequency": rc.frequency,
                    "crypto_impact": rc.crypto_impact,
                })

            data = {"releases": releases_list}
            with open(self.config_path, "w") as f:
                yaml.dump(data, f, default_flow_style=False, sort_keys=False)

            self.log.info("config_persisted", path=str(self.config_path))
        except Exception as e:
            self.log.error("config_persist_error", error=str(e))

    def compute_z_score(
        self, actual: float, consensus: float, historical_std: float
    ) -> float:
        """Compute z-score = (actual - consensus) / historical_std."""
        if historical_std == 0:
            return 0.0
        return (actual - consensus) / historical_std

    def detect_surprises(
        self,
        fred_items: list[CollectedItem],
        z_threshold: float = Z_SCORE_HIGH,
    ) -> list[EconomicSurprise]:
        """Detect economic surprises from FRED collected items.

        Args:
            fred_items: List of CollectedItems from FREDCollector.
            z_threshold: Minimum |z_score| to report as a surprise.

        Returns:
            List of EconomicSurprise objects, sorted by |z_score| descending.
        """
        surprises: list[EconomicSurprise] = []

        for item in fred_items:
            meta = item.metadata
            series_id = meta.get("series_id")
            if not series_id:
                continue

            release = self.releases.get(series_id)
            if release is None:
                continue

            # Skip if no consensus is set
            if release.consensus is None:
                continue

            actual = meta.get("value")
            if actual is None:
                continue

            try:
                actual = float(actual)
            except (ValueError, TypeError):
                continue

            z_score = self.compute_z_score(
                actual, release.consensus, release.historical_std
            )

            if abs(z_score) < z_threshold:
                continue

            # Determine severity
            if abs(z_score) >= Z_SCORE_EXTREME:
                severity = "extreme"
            elif abs(z_score) >= Z_SCORE_HIGH:
                severity = "high"
            else:
                severity = "moderate"

            direction = "above" if z_score > 0 else "below"

            surprises.append(EconomicSurprise(
                series_id=series_id,
                name=release.name,
                actual=actual,
                consensus=release.consensus,
                z_score=z_score,
                severity=severity,
                direction=direction,
                crypto_impact=release.crypto_impact,
                item=item,
            ))

        # Sort by absolute z-score descending (biggest surprises first)
        surprises.sort(key=lambda s: abs(s.z_score), reverse=True)

        if surprises:
            self.log.info(
                "surprises_detected",
                count=len(surprises),
                extreme=sum(1 for s in surprises if s.severity == "extreme"),
                high=sum(1 for s in surprises if s.severity == "high"),
            )

        return surprises

    def surprises_to_items(
        self, surprises: list[EconomicSurprise]
    ) -> list[CollectedItem]:
        """Convert EconomicSurprise objects into CollectedItems for the pipeline.

        This allows surprise alerts to flow through the same pipeline as
        other collected data (dedup, scoring, distribution).
        """
        from datetime import datetime, timezone

        items: list[CollectedItem] = []
        now = datetime.now(timezone.utc)

        for surprise in surprises:
            items.append(CollectedItem(
                id=f"econ_surprise_{surprise.series_id}_{now.strftime('%Y%m%d')}",
                title=(
                    f"ECON SURPRISE [{surprise.severity.upper()}]: "
                    f"{surprise.name} z={surprise.z_score:+.2f}"
                ),
                content=surprise.description,
                url=f"https://fred.stlouisfed.org/series/{surprise.series_id}",
                published_at=now,
                metadata={
                    "data_type": "economic_surprise",
                    "category": "macro_surprise",
                    "series_id": surprise.series_id,
                    "actual": surprise.actual,
                    "consensus": surprise.consensus,
                    "z_score": surprise.z_score,
                    "severity": surprise.severity,
                    "direction": surprise.direction,
                    "crypto_impact": surprise.crypto_impact,
                    "priority": "critical" if surprise.severity == "extreme" else "high",
                },
                raw={
                    "actual": surprise.actual,
                    "consensus": surprise.consensus,
                    "z_score": surprise.z_score,
                    "historical_std": self.releases[surprise.series_id].historical_std,
                },
            ))

        return items
