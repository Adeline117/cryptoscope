"""GitHub Star Velocity collector — tracks star growth, detects anomalies, discovers trending repos."""

from __future__ import annotations

import asyncio
import math
import os
import statistics
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import aiosqlite
import structlog
import yaml

from src.collectors.base import BaseCollector, CollectedItem, CollectionResult
from src.config import CONFIG_DIR, DATA_DIR

logger = structlog.get_logger()

STAR_DB = DATA_DIR / "star_history.db"


class StarHistoryStore:
    """SQLite-backed storage for star/fork snapshots with sliding-window queries."""

    def __init__(self, db_path: Path = STAR_DB):
        self.db_path = db_path
        self._db: aiosqlite.Connection | None = None

    async def init(self) -> None:
        """Create tables and indexes if they don't exist."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._db = await aiosqlite.connect(str(self.db_path))
        await self._db.executescript(
            """
            CREATE TABLE IF NOT EXISTS star_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                repo TEXT NOT NULL,
                star_count INTEGER NOT NULL,
                fork_count INTEGER NOT NULL DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_snap_repo_ts
                ON star_snapshots (repo, timestamp);

            CREATE TABLE IF NOT EXISTS discovered_repos (
                repo TEXT PRIMARY KEY,
                discovered_at TEXT NOT NULL,
                source TEXT NOT NULL,
                initial_stars INTEGER NOT NULL DEFAULT 0,
                category TEXT
            );
            """
        )
        await self._db.commit()

    async def close(self) -> None:
        if self._db:
            await self._db.close()

    async def record_snapshot(
        self, repo: str, star_count: int, fork_count: int
    ) -> None:
        """Insert a star/fork count snapshot."""
        now = datetime.now(timezone.utc).isoformat()
        await self._db.execute(
            "INSERT INTO star_snapshots (timestamp, repo, star_count, fork_count) VALUES (?, ?, ?, ?)",
            (now, repo, star_count, fork_count),
        )
        await self._db.commit()

    async def get_history(
        self, repo: str, days: int = 30
    ) -> list[dict[str, Any]]:
        """Return snapshots for a repo within the last N days."""
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        async with self._db.execute(
            "SELECT timestamp, star_count, fork_count FROM star_snapshots "
            "WHERE repo = ? AND timestamp >= ? ORDER BY timestamp",
            (repo, cutoff),
        ) as cursor:
            rows = await cursor.fetchall()
        return [
            {"timestamp": r[0], "star_count": r[1], "fork_count": r[2]}
            for r in rows
        ]

    async def get_latest(self, repo: str) -> dict[str, Any] | None:
        """Return the most recent snapshot for a repo."""
        async with self._db.execute(
            "SELECT timestamp, star_count, fork_count FROM star_snapshots "
            "WHERE repo = ? ORDER BY timestamp DESC LIMIT 1",
            (repo,),
        ) as cursor:
            row = await cursor.fetchone()
        if row is None:
            return None
        return {"timestamp": row[0], "star_count": row[1], "fork_count": row[2]}

    async def get_star_delta(self, repo: str, hours: int = 24) -> int:
        """Compute star change over the last N hours."""
        now = datetime.now(timezone.utc)
        cutoff = (now - timedelta(hours=hours)).isoformat()
        async with self._db.execute(
            "SELECT star_count FROM star_snapshots "
            "WHERE repo = ? AND timestamp <= ? ORDER BY timestamp DESC LIMIT 1",
            (repo, cutoff),
        ) as cursor:
            old_row = await cursor.fetchone()
        async with self._db.execute(
            "SELECT star_count FROM star_snapshots "
            "WHERE repo = ? ORDER BY timestamp DESC LIMIT 1",
            (repo,),
        ) as cursor:
            new_row = await cursor.fetchone()
        if old_row is None or new_row is None:
            return 0
        return new_row[0] - old_row[0]

    async def record_discovered(
        self, repo: str, source: str, initial_stars: int, category: str = ""
    ) -> None:
        """Track a newly discovered repo."""
        now = datetime.now(timezone.utc).isoformat()
        await self._db.execute(
            "INSERT OR IGNORE INTO discovered_repos (repo, discovered_at, source, initial_stars, category) "
            "VALUES (?, ?, ?, ?, ?)",
            (repo, now, source, initial_stars, category),
        )
        await self._db.commit()

    async def is_known(self, repo: str) -> bool:
        """Check if a repo is already in our discovered list."""
        async with self._db.execute(
            "SELECT 1 FROM discovered_repos WHERE repo = ?", (repo,)
        ) as cursor:
            return await cursor.fetchone() is not None


def _format_top_velocity(entries: list[dict[str, Any]]) -> str:
    """Format top velocity entries for summary text."""
    parts = []
    for v in entries:
        repo = v["repo"]
        spd = v["stars_per_day"]
        parts.append(f"{repo}({spd:.0f}/d)")
    return ", ".join(parts)


def _load_tracking_config() -> dict[str, Any]:
    """Load the star tracking YAML configuration."""
    path = CONFIG_DIR / "sources" / "github_star_tracking.yaml"
    with open(path) as f:
        return yaml.safe_load(f)


class GitHubStarVelocityCollector(BaseCollector):
    """Collect and analyze GitHub star velocity for crypto repositories.

    Features:
    - Batch star/fork count fetching for 150+ watched repos
    - Trending repo scanning by language (Solidity, Rust, Move, Cairo)
    - New repo discovery via GitHub Search API topics
    - Key developer monitoring for new repo creation
    - Sliding-window velocity calculation (stars/day)
    - Z-score anomaly detection with tiered alert levels
    - Absolute threshold alerts (500 stars/24h, 2000 stars/7d)
    """

    source_id = "github_star_velocity"
    source_name = "GitHub Star Velocity Tracker"
    source_type = "api"

    BASE_URL = "https://api.github.com"

    def __init__(self, **kwargs: Any):
        super().__init__(max_concurrent=10, cache_ttl=1800, **kwargs)
        self.token: str = os.environ.get("GITHUB_TOKEN", "")
        self._store = StarHistoryStore()
        self._config: dict[str, Any] = {}

    @property
    def _headers(self) -> dict[str, str]:
        h: dict[str, str] = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        if self.token:
            h["Authorization"] = f"Bearer {self.token}"
        return h

    def _load_config(self) -> None:
        """Load and cache the tracking configuration."""
        if not self._config:
            self._config = _load_tracking_config()

    def _all_watched_repos(self) -> list[str]:
        """Flatten all category repo lists into a single list."""
        self._load_config()
        repos: list[str] = []
        watched = self._config.get("watched_repos", {})
        for _category, repo_list in watched.items():
            if isinstance(repo_list, list):
                repos.extend(repo_list)
        return [r for r in repos if r and "/" in str(r)]

    def _category_for_repo(self, repo: str) -> str:
        """Return the category a repo belongs to, or 'unknown'."""
        self._load_config()
        watched = self._config.get("watched_repos", {})
        for category, repo_list in watched.items():
            if isinstance(repo_list, list) and repo in repo_list:
                return category
        return "unknown"

    # -------------------------------------------------------------------------
    # Core collection methods
    # -------------------------------------------------------------------------

    async def collect_star_counts(self) -> list[dict[str, Any]]:
        """Batch-fetch star and fork counts for all watched repos.

        Uses the GitHub REST API ``GET /repos/{owner}/{repo}`` endpoint.
        Results are stored in SQLite for historical tracking.
        """
        repos = self._all_watched_repos()
        self.log.info("collecting_star_counts", repo_count=len(repos))

        async def _fetch_one(repo: str) -> dict[str, Any] | None:
            try:
                data = await self._fetch_json(
                    f"{self.BASE_URL}/repos/{repo}",
                    headers=self._headers,
                    use_cache=False,  # always want fresh counts
                )
                stars = data.get("stargazers_count", 0)
                forks = data.get("forks_count", 0)
                await self._store.record_snapshot(repo, stars, forks)
                return {
                    "repo": repo,
                    "stars": stars,
                    "forks": forks,
                    "description": data.get("description", ""),
                    "language": data.get("language"),
                    "topics": data.get("topics", []),
                    "pushed_at": data.get("pushed_at"),
                    "open_issues": data.get("open_issues_count", 0),
                    "subscribers": data.get("subscribers_count", 0),
                }
            except Exception as e:
                self.log.warning("star_fetch_failed", repo=repo, error=str(e))
                return None

        tasks = [_fetch_one(repo) for repo in repos]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return [r for r in results if isinstance(r, dict)]

    async def scan_trending(self) -> list[dict[str, Any]]:
        """Scan GitHub trending for crypto-relevant repos.

        Fetches the GitHub Search API sorted by stars for each tracked language,
        filtering to repos created in the last 90 days.
        """
        self._load_config()
        languages = self._config.get("discovery", {}).get("trending_languages", [])
        cutoff = (datetime.now(timezone.utc) - timedelta(days=90)).strftime("%Y-%m-%d")
        discovered: list[dict[str, Any]] = []

        for lang in languages:
            try:
                query = f"language:{lang} created:>{cutoff} stars:>50"
                data = await self._fetch_json(
                    f"{self.BASE_URL}/search/repositories",
                    headers=self._headers,
                    params={
                        "q": query,
                        "sort": "stars",
                        "order": "desc",
                        "per_page": 30,
                    },
                    use_cache=True,
                )
                for item in data.get("items", []):
                    full_name = item.get("full_name", "")
                    if not await self._store.is_known(full_name):
                        stars = item.get("stargazers_count", 0)
                        await self._store.record_discovered(
                            full_name, f"trending_{lang.lower()}", stars
                        )
                        await self._store.record_snapshot(
                            full_name, stars, item.get("forks_count", 0)
                        )
                        discovered.append({
                            "repo": full_name,
                            "stars": stars,
                            "forks": item.get("forks_count", 0),
                            "language": item.get("language"),
                            "description": item.get("description", ""),
                            "topics": item.get("topics", []),
                            "created_at": item.get("created_at"),
                            "source": f"trending_{lang.lower()}",
                        })
                # Respect GitHub Search rate limit (30 req/min)
                await asyncio.sleep(2)
            except Exception as e:
                self.log.warning("trending_scan_failed", language=lang, error=str(e))

        self.log.info("trending_scan_complete", new_repos=len(discovered))
        return discovered

    async def search_new_repos(self) -> list[dict[str, Any]]:
        """Discover new repos via GitHub Search API by topic.

        Searches configured topics and filters by minimum stars and recency.
        """
        self._load_config()
        discovery = self._config.get("discovery", {})
        topics = discovery.get("search_topics", [])
        filters = discovery.get("search_filters", {})
        min_stars = filters.get("min_stars", 50)
        created_after = filters.get("created_after", "2024-01-01")
        discovered: list[dict[str, Any]] = []

        for topic in topics:
            try:
                query = f"topic:{topic} stars:>{min_stars} created:>{created_after}"
                data = await self._fetch_json(
                    f"{self.BASE_URL}/search/repositories",
                    headers=self._headers,
                    params={
                        "q": query,
                        "sort": "stars",
                        "order": "desc",
                        "per_page": 20,
                    },
                    use_cache=True,
                )
                for item in data.get("items", []):
                    full_name = item.get("full_name", "")
                    if not await self._store.is_known(full_name):
                        stars = item.get("stargazers_count", 0)
                        await self._store.record_discovered(
                            full_name, f"topic_{topic}", stars, category=topic
                        )
                        await self._store.record_snapshot(
                            full_name, stars, item.get("forks_count", 0)
                        )
                        discovered.append({
                            "repo": full_name,
                            "stars": stars,
                            "forks": item.get("forks_count", 0),
                            "language": item.get("language"),
                            "description": item.get("description", ""),
                            "topics": item.get("topics", []),
                            "created_at": item.get("created_at"),
                            "source": f"topic_{topic}",
                        })
                # Respect rate limit
                await asyncio.sleep(2)
            except Exception as e:
                self.log.warning("topic_search_failed", topic=topic, error=str(e))

        self.log.info("topic_search_complete", new_repos=len(discovered))
        return discovered

    async def track_key_developers(self) -> list[dict[str, Any]]:
        """Check key developer accounts for new repo creation.

        Uses ``GET /users/{user}/repos?sort=created`` to find repos
        created in the last 30 days by tracked developers.
        """
        self._load_config()
        devs = self._config.get("discovery", {}).get("key_developers_to_track", [])
        cutoff = datetime.now(timezone.utc) - timedelta(days=30)
        new_repos: list[dict[str, Any]] = []

        for dev in devs:
            try:
                repos = await self._fetch_json(
                    f"{self.BASE_URL}/users/{dev}/repos",
                    headers=self._headers,
                    params={"sort": "created", "direction": "desc", "per_page": 10},
                    use_cache=True,
                )
                for repo_data in repos:
                    created_str = repo_data.get("created_at", "")
                    if not created_str:
                        continue
                    created = datetime.fromisoformat(
                        created_str.replace("Z", "+00:00")
                    )
                    if created < cutoff:
                        continue
                    full_name = repo_data.get("full_name", "")
                    if repo_data.get("fork"):
                        continue
                    if not await self._store.is_known(full_name):
                        stars = repo_data.get("stargazers_count", 0)
                        await self._store.record_discovered(
                            full_name, f"dev_{dev}", stars
                        )
                        await self._store.record_snapshot(
                            full_name, stars, repo_data.get("forks_count", 0)
                        )
                        new_repos.append({
                            "repo": full_name,
                            "developer": dev,
                            "stars": stars,
                            "forks": repo_data.get("forks_count", 0),
                            "language": repo_data.get("language"),
                            "description": repo_data.get("description", ""),
                            "created_at": created_str,
                            "source": f"dev_{dev}",
                        })
            except Exception as e:
                self.log.debug("dev_tracking_failed", developer=dev, error=str(e))

        self.log.info("dev_tracking_complete", new_repos=len(new_repos))
        return new_repos

    # -------------------------------------------------------------------------
    # Analytics
    # -------------------------------------------------------------------------

    async def calculate_velocity(
        self, repo: str, window_days: int = 7
    ) -> dict[str, Any]:
        """Compute star velocity (stars/day) with a sliding window.

        Returns velocity metrics including daily average, trend direction,
        and raw daily deltas.
        """
        history = await self._store.get_history(repo, days=window_days + 1)
        if len(history) < 2:
            return {
                "repo": repo,
                "stars_per_day": 0.0,
                "total_delta": 0,
                "window_days": window_days,
                "data_points": len(history),
                "daily_deltas": [],
                "trend": "insufficient_data",
            }

        # Compute daily deltas between consecutive snapshots
        daily_deltas: list[float] = []
        for i in range(1, len(history)):
            ts_prev = datetime.fromisoformat(history[i - 1]["timestamp"])
            ts_curr = datetime.fromisoformat(history[i]["timestamp"])
            hours_elapsed = max((ts_curr - ts_prev).total_seconds() / 3600, 0.1)
            star_delta = history[i]["star_count"] - history[i - 1]["star_count"]
            daily_rate = star_delta * (24.0 / hours_elapsed)
            daily_deltas.append(daily_rate)

        total_delta = history[-1]["star_count"] - history[0]["star_count"]
        avg_velocity = statistics.mean(daily_deltas) if daily_deltas else 0.0

        # Determine trend from second half vs first half
        mid = len(daily_deltas) // 2
        if mid > 0:
            first_half = statistics.mean(daily_deltas[:mid])
            second_half = statistics.mean(daily_deltas[mid:])
            if second_half > first_half * 1.2:
                trend = "accelerating"
            elif second_half < first_half * 0.8:
                trend = "decelerating"
            else:
                trend = "stable"
        else:
            trend = "insufficient_data"

        return {
            "repo": repo,
            "stars_per_day": round(avg_velocity, 2),
            "total_delta": total_delta,
            "window_days": window_days,
            "data_points": len(history),
            "daily_deltas": [round(d, 2) for d in daily_deltas],
            "trend": trend,
        }

    async def detect_anomalies(
        self, repo_stats: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Z-score based anomaly detection across all tracked repos.

        Alert levels:
        - HIGH: z-score > 3.0  (extremely unusual growth)
        - MEDIUM: z-score > 2.0  (notable acceleration)
        - NOTABLE: z-score > 1.5  (worth watching)

        Also triggers on absolute thresholds:
        - 500+ new stars in 24h -> auto-analyze
        - 2000+ new stars in 7d -> weekly report flag
        """
        self._load_config()
        anomaly_cfg = self._config.get("anomaly_detection", {})
        z_thresholds = anomaly_cfg.get("z_score_thresholds", {})
        z_high = z_thresholds.get("high", 3.0)
        z_medium = z_thresholds.get("medium", 2.0)
        z_notable = z_thresholds.get("notable", 1.5)
        abs_thresholds = anomaly_cfg.get("absolute_thresholds", {})
        abs_24h = abs_thresholds.get("stars_24h_auto_analyze", 500)
        abs_7d = abs_thresholds.get("stars_7d_weekly_report", 2000)
        window = anomaly_cfg.get("sliding_window_days", 30)
        min_points = anomaly_cfg.get("min_data_points", 7)

        anomalies: list[dict[str, Any]] = []

        # Collect velocities for all repos
        velocities: list[float] = []
        repo_velocity_map: dict[str, dict[str, Any]] = {}
        for stat in repo_stats:
            repo = stat["repo"]
            vel = await self.calculate_velocity(repo, window_days=window)
            repo_velocity_map[repo] = vel
            if vel["data_points"] >= min_points:
                velocities.append(vel["stars_per_day"])

        if len(velocities) < 3:
            self.log.info("insufficient_data_for_anomaly_detection", count=len(velocities))
            return anomalies

        mean_vel = statistics.mean(velocities)
        stdev_vel = statistics.stdev(velocities) if len(velocities) > 1 else 1.0
        if stdev_vel == 0:
            stdev_vel = 1.0

        for stat in repo_stats:
            repo = stat["repo"]
            vel = repo_velocity_map.get(repo)
            if vel is None or vel["data_points"] < min_points:
                continue

            z_score = (vel["stars_per_day"] - mean_vel) / stdev_vel

            # Determine alert level from z-score
            alert_level: str | None = None
            if z_score >= z_high:
                alert_level = "HIGH"
            elif z_score >= z_medium:
                alert_level = "MEDIUM"
            elif z_score >= z_notable:
                alert_level = "NOTABLE"

            # Check absolute thresholds
            stars_24h = await self._store.get_star_delta(repo, hours=24)
            stars_7d = await self._store.get_star_delta(repo, hours=168)
            absolute_trigger = False
            if stars_24h >= abs_24h:
                alert_level = alert_level or "HIGH"
                absolute_trigger = True
            if stars_7d >= abs_7d:
                alert_level = alert_level or "MEDIUM"
                absolute_trigger = True

            if alert_level is None:
                continue

            anomalies.append({
                "repo": repo,
                "alert_level": alert_level,
                "velocity_zscore": round(z_score, 3),
                "stars_per_day": vel["stars_per_day"],
                "total_delta_window": vel["total_delta"],
                "stars_24h": stars_24h,
                "stars_7d": stars_7d,
                "trend": vel["trend"],
                "absolute_trigger": absolute_trigger,
                "current_stars": stat.get("stars", 0),
                "category": self._category_for_repo(repo),
            })

        # Sort by z-score descending
        anomalies.sort(key=lambda a: a["velocity_zscore"], reverse=True)
        self.log.info(
            "anomaly_detection_complete",
            total_anomalies=len(anomalies),
            high=sum(1 for a in anomalies if a["alert_level"] == "HIGH"),
            medium=sum(1 for a in anomalies if a["alert_level"] == "MEDIUM"),
            notable=sum(1 for a in anomalies if a["alert_level"] == "NOTABLE"),
        )
        return anomalies

    async def _enrich_repo_context(self, repo: str) -> dict[str, Any]:
        """Fetch additional context for a repo: recent commits, contributors, latest release, topics."""
        context: dict[str, Any] = {}

        # Recent commits (last 5)
        try:
            commits = await self._fetch_json(
                f"{self.BASE_URL}/repos/{repo}/commits",
                headers=self._headers,
                params={"per_page": 5},
            )
            context["recent_commits"] = [
                {
                    "sha": c.get("sha", "")[:8],
                    "message": c.get("commit", {}).get("message", "").split("\n")[0][:120],
                    "author": c.get("commit", {}).get("author", {}).get("name", ""),
                    "date": c.get("commit", {}).get("author", {}).get("date", ""),
                }
                for c in commits
            ]
        except Exception:
            context["recent_commits"] = []

        # Top contributors
        try:
            contribs = await self._fetch_json(
                f"{self.BASE_URL}/repos/{repo}/contributors",
                headers=self._headers,
                params={"per_page": 10},
            )
            context["top_contributors"] = [
                {"login": c.get("login", ""), "contributions": c.get("contributions", 0)}
                for c in contribs
            ]
        except Exception:
            context["top_contributors"] = []

        # Latest release
        try:
            release = await self._fetch_json(
                f"{self.BASE_URL}/repos/{repo}/releases/latest",
                headers=self._headers,
            )
            context["latest_release"] = {
                "tag": release.get("tag_name", ""),
                "name": release.get("name", ""),
                "published_at": release.get("published_at", ""),
                "prerelease": release.get("prerelease", False),
            }
        except Exception:
            context["latest_release"] = None

        # Repo topics and language (from main repo endpoint)
        try:
            repo_data = await self._fetch_json(
                f"{self.BASE_URL}/repos/{repo}",
                headers=self._headers,
            )
            context["topics"] = repo_data.get("topics", [])
            context["language"] = repo_data.get("language")
            context["license"] = (repo_data.get("license") or {}).get("spdx_id")
            context["created_at"] = repo_data.get("created_at")
            context["default_branch"] = repo_data.get("default_branch")
        except Exception:
            pass

        return context

    # -------------------------------------------------------------------------
    # Main collection entrypoint
    # -------------------------------------------------------------------------

    async def _collect(self) -> CollectionResult:
        """Run full star velocity collection pipeline.

        Steps:
        1. Fetch current star counts for all watched repos
        2. Scan GitHub trending for new crypto repos
        3. Search for new repos by topic
        4. Track key developers for new repo creation
        5. Calculate velocity and detect anomalies
        6. Enrich anomalous repos with context
        7. Return CollectionResult with alert items
        """
        await self._store.init()
        try:
            items: list[CollectedItem] = []

            # Step 1: Collect star counts
            repo_stats = await self.collect_star_counts()

            # Steps 2-4: Discovery (run in parallel)
            trending_task = asyncio.create_task(self.scan_trending())
            search_task = asyncio.create_task(self.search_new_repos())
            dev_task = asyncio.create_task(self.track_key_developers())

            trending_repos = await trending_task
            search_repos = await search_task
            dev_repos = await dev_task

            # Add discovery items
            for disco in trending_repos + search_repos + dev_repos:
                items.append(
                    CollectedItem(
                        id=f"gh_discovered_{disco['repo']}",
                        title=f"[New Repo] {disco['repo']} ({disco.get('stars', 0)} stars)",
                        content=disco.get("description", ""),
                        url=f"https://github.com/{disco['repo']}",
                        published_at=datetime.now(timezone.utc),
                        metadata={
                            "data_type": "github_discovery",
                            "repo": disco["repo"],
                            "stars": disco.get("stars", 0),
                            "forks": disco.get("forks", 0),
                            "language": disco.get("language"),
                            "source": disco.get("source"),
                            "topics": disco.get("topics", []),
                        },
                    )
                )

            # Step 5: Anomaly detection
            anomalies = await self.detect_anomalies(repo_stats)

            # Step 6: Enrich anomalous repos with context
            for anomaly in anomalies:
                repo = anomaly["repo"]
                context = await self._enrich_repo_context(repo)

                # Find matching stat for description
                stat = next((s for s in repo_stats if s["repo"] == repo), {})
                description = stat.get("description", "")

                velocity_info = await self.calculate_velocity(repo, window_days=7)

                items.append(
                    CollectedItem(
                        id=f"gh_star_anomaly_{repo}_{anomaly['alert_level'].lower()}",
                        title=(
                            f"[Star Velocity {anomaly['alert_level']}] {repo}: "
                            f"+{anomaly['stars_24h']} stars/24h "
                            f"(z={anomaly['velocity_zscore']:.1f})"
                        ),
                        content=(
                            f"{description}\n\n"
                            f"Star velocity: {anomaly['stars_per_day']:.1f} stars/day | "
                            f"Trend: {anomaly['trend']} | "
                            f"24h: +{anomaly['stars_24h']} | 7d: +{anomaly['stars_7d']} | "
                            f"Total: {anomaly['current_stars']:,}"
                        ),
                        url=f"https://github.com/{repo}",
                        published_at=datetime.now(timezone.utc),
                        metadata={
                            "data_type": "github_star_anomaly",
                            "alert_level": anomaly["alert_level"],
                            "velocity_zscore": anomaly["velocity_zscore"],
                            "stars_per_day": anomaly["stars_per_day"],
                            "stars_24h": anomaly["stars_24h"],
                            "stars_7d": anomaly["stars_7d"],
                            "total_stars": anomaly["current_stars"],
                            "trend": anomaly["trend"],
                            "category": anomaly["category"],
                            "absolute_trigger": anomaly["absolute_trigger"],
                            "velocity_detail": velocity_info,
                            "context": context,
                        },
                        raw=anomaly,
                    )
                )

            # Step 7: Summary stats item
            if repo_stats:
                top_by_velocity: list[dict[str, Any]] = []
                for stat in repo_stats[:50]:  # top 50 by current stars for velocity check
                    vel = await self.calculate_velocity(stat["repo"], window_days=7)
                    if vel["stars_per_day"] > 0:
                        top_by_velocity.append({
                            "repo": stat["repo"],
                            "stars_per_day": vel["stars_per_day"],
                            "total_stars": stat["stars"],
                        })
                top_by_velocity.sort(key=lambda x: x["stars_per_day"], reverse=True)

                items.append(
                    CollectedItem(
                        id=f"gh_star_summary_{datetime.now(timezone.utc).strftime('%Y%m%d')}",
                        title=f"[Star Summary] {len(repo_stats)} repos tracked, {len(anomalies)} anomalies detected",
                        content=(
                            f"Repos tracked: {len(repo_stats)}\n"
                            f"Anomalies: {len(anomalies)} "
                            f"(HIGH: {sum(1 for a in anomalies if a['alert_level'] == 'HIGH')}, "
                            f"MEDIUM: {sum(1 for a in anomalies if a['alert_level'] == 'MEDIUM')}, "
                            f"NOTABLE: {sum(1 for a in anomalies if a['alert_level'] == 'NOTABLE')})\n"
                            f"New repos discovered: {len(trending_repos) + len(search_repos) + len(dev_repos)}\n"
                            f"Top velocity: {_format_top_velocity(top_by_velocity[:5])}"
                        ),
                        url="",
                        published_at=datetime.now(timezone.utc),
                        metadata={
                            "data_type": "github_star_summary",
                            "repos_tracked": len(repo_stats),
                            "anomaly_count": len(anomalies),
                            "discovered_trending": len(trending_repos),
                            "discovered_search": len(search_repos),
                            "discovered_dev": len(dev_repos),
                            "top_velocity": top_by_velocity[:10],
                        },
                    )
                )

            self.log.info(
                "star_velocity_collection_complete",
                items=len(items),
                anomalies=len(anomalies),
                discovered=len(trending_repos) + len(search_repos) + len(dev_repos),
            )
            return CollectionResult(
                source_id=self.source_id,
                source_name=self.source_name,
                source_type=self.source_type,
                items=items,
            )
        finally:
            await self._store.close()
