"""Token vesting unlock calendar collector."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import yaml

from src.collectors.base import BaseCollector, CollectedItem, CollectionResult
from src.config import CONFIG_DIR


class TokenUnlockCollector(BaseCollector):
    """Read upcoming token unlocks from YAML calendar and generate alerts.

    Alert rules:
        - T-72h: "即将解锁" warning (upcoming unlock)
        - T-24h: "明天解锁" high priority (unlocking tomorrow)
        - Unlock > 5% of circulating supply: critical severity
    """

    source_id = "token_unlocks"
    source_name = "Token Unlock Calendar"
    source_type = "db"

    # Alert windows
    WARNING_HOURS = 72  # T-72h
    HIGH_HOURS = 24  # T-24h

    # Critical threshold for % of circulating supply
    CRITICAL_PCT_THRESHOLD = 5.0

    def __init__(
        self,
        calendar_path: Path | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.calendar_path = calendar_path or (CONFIG_DIR / "token_unlocks.yaml")

    async def _collect(self) -> CollectionResult:
        items: list[CollectedItem] = []

        unlocks = self._load_calendar()
        if not unlocks:
            self.log.warning("no_unlocks_loaded", path=str(self.calendar_path))
            return CollectionResult(
                source_id=self.source_id,
                source_name=self.source_name,
                source_type=self.source_type,
                items=items,
            )

        now = datetime.now(timezone.utc)

        for unlock in unlocks:
            token = unlock.get("token", "UNKNOWN")
            date_str = unlock.get("date", "")
            amount_usd = unlock.get("amount_usd", 0)
            unlock_type = unlock.get("type", "unknown")
            pct_circulating = unlock.get("pct_of_circulating", 0)

            # Parse unlock date
            try:
                unlock_date = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            except (ValueError, TypeError):
                self.log.warning("invalid_unlock_date", token=token, date=date_str)
                continue

            hours_until = (unlock_date - now).total_seconds() / 3600

            # Skip past unlocks (more than 24h ago) and far-future ones
            if hours_until < -24:
                continue
            if hours_until > self.WARNING_HOURS:
                continue

            # Determine severity and alert message
            severity, alert_label = self._classify_alert(hours_until, pct_circulating)

            title = (
                f"[{alert_label}] {token} — "
                f"${amount_usd / 1_000_000:.0f}M unlock ({unlock_type}), "
                f"{pct_circulating:.1f}% of circulating"
            )

            content = (
                f"Token: {token}\n"
                f"Unlock date: {date_str}\n"
                f"Amount: ${amount_usd:,.0f}\n"
                f"Type: {unlock_type}\n"
                f"% of circulating supply: {pct_circulating:.1f}%\n"
                f"Hours until unlock: {hours_until:.0f}"
            )

            # Determine priority for the scoring system
            if severity == "critical":
                priority = "high"
                auto_draft = True
            elif severity == "high":
                priority = "high"
                auto_draft = True
            else:
                priority = "medium"
                auto_draft = False

            items.append(
                CollectedItem(
                    id=f"token_unlock_{token}_{date_str}",
                    title=title,
                    content=content,
                    url=f"https://token.unlocks.app/{token.lower()}",
                    published_at=datetime.now(timezone.utc),
                    metadata={
                        "data_type": "token_unlock",
                        "token": token,
                        "unlock_date": date_str,
                        "amount_usd": amount_usd,
                        "unlock_type": unlock_type,
                        "pct_of_circulating": pct_circulating,
                        "hours_until_unlock": round(hours_until, 1),
                        "severity": severity,
                        "alert_label": alert_label,
                        "priority": priority,
                        "auto_draft": auto_draft,
                        "category": "defi_protocol",
                    },
                    raw=unlock,
                )
            )

        # Sort by time until unlock (most imminent first)
        items.sort(key=lambda i: i.metadata.get("hours_until_unlock", 999))

        self.log.info("token_unlocks_processed", alerts=len(items))

        return CollectionResult(
            source_id=self.source_id,
            source_name=self.source_name,
            source_type=self.source_type,
            items=items,
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _load_calendar(self) -> list[dict]:
        """Load the token unlock YAML calendar."""
        try:
            with open(self.calendar_path) as f:
                data = yaml.safe_load(f)
            if isinstance(data, dict):
                return data.get("unlocks", [])
            return data if isinstance(data, list) else []
        except FileNotFoundError:
            self.log.error("calendar_not_found", path=str(self.calendar_path))
            return []
        except Exception as e:
            self.log.error("calendar_load_failed", error=str(e))
            return []

    def _classify_alert(
        self, hours_until: float, pct_circulating: float
    ) -> tuple[str, str]:
        """Classify the alert severity and label.

        Returns:
            (severity, alert_label) tuple
        """
        # Check for critical supply impact first
        is_supply_critical = pct_circulating >= self.CRITICAL_PCT_THRESHOLD

        if hours_until <= 0:
            # Unlock is happening now or already happened
            if is_supply_critical:
                return "critical", "CRITICAL-解锁中"
            return "high", "正在解锁"

        if hours_until <= self.HIGH_HOURS:
            if is_supply_critical:
                return "critical", "CRITICAL-明天解锁"
            return "high", "明天解锁"

        if hours_until <= self.WARNING_HOURS:
            if is_supply_critical:
                return "critical", "CRITICAL-即将解锁"
            return "warning", "即将解锁"

        return "info", "即将解锁"
