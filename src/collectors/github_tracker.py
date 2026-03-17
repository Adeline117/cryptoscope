"""GitHub repository monitoring: commits, releases, stars, forks."""

from __future__ import annotations

import os
from datetime import datetime, timezone

from src.collectors.base import BaseCollector, CollectedItem, CollectionResult
from src.config import load_sources


class GitHubTracker(BaseCollector):
    """Monitor GitHub repos for releases, significant commits, and trending activity."""

    source_id = "github_tracker"
    source_name = "GitHub Repository Tracker"
    source_type = "api"

    BASE_URL = "https://api.github.com"

    def __init__(self, **kwargs):
        super().__init__(cache_ttl=3600, **kwargs)
        self.token = os.environ.get("GITHUB_TOKEN", "")

    @property
    def _headers(self) -> dict:
        h = {"Accept": "application/vnd.github+json"}
        if self.token:
            h["Authorization"] = f"Bearer {self.token}"
        return h

    def _get_repos(self) -> list[dict]:
        """Load repos from source registry."""
        try:
            sources = load_sources("github_repos.yaml")
            return [s for s in sources if s.get("enabled", True)]
        except FileNotFoundError:
            self.log.warning("github_repos.yaml not found, using empty list")
            return []

    async def _collect_repo_releases(self, repo: str) -> list[CollectedItem]:
        """Fetch latest releases for a repo."""
        items = []
        try:
            releases = await self._fetch_json(
                f"{self.BASE_URL}/repos/{repo}/releases",
                headers=self._headers,
                params={"per_page": 5},
            )
            for rel in releases:
                pub_date = rel.get("published_at")
                items.append(
                    CollectedItem(
                        id=f"gh_release_{repo}_{rel.get('id', '')}",
                        title=f"[Release] {repo}: {rel.get('name') or rel.get('tag_name', '')}",
                        content=rel.get("body", "")[:2000],
                        url=rel.get("html_url", ""),
                        published_at=(
                            datetime.fromisoformat(pub_date.replace("Z", "+00:00"))
                            if pub_date
                            else None
                        ),
                        metadata={
                            "data_type": "github_release",
                            "repo": repo,
                            "tag": rel.get("tag_name"),
                            "prerelease": rel.get("prerelease", False),
                        },
                        raw=rel,
                    )
                )
        except Exception as e:
            self.log.debug("releases_error", repo=repo, error=str(e))
        return items

    async def _collect_repo_commits(self, repo: str) -> list[CollectedItem]:
        """Fetch recent commits for a repo."""
        items = []
        try:
            commits = await self._fetch_json(
                f"{self.BASE_URL}/repos/{repo}/commits",
                headers=self._headers,
                params={"per_page": 10},
            )
            for c in commits:
                commit_data = c.get("commit", {})
                author_info = commit_data.get("author", {})
                date_str = author_info.get("date")
                items.append(
                    CollectedItem(
                        id=f"gh_commit_{repo}_{c.get('sha', '')[:12]}",
                        title=f"[Commit] {repo}: {commit_data.get('message', '')[:120]}",
                        content=commit_data.get("message", ""),
                        url=c.get("html_url", ""),
                        published_at=(
                            datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                            if date_str
                            else None
                        ),
                        metadata={
                            "data_type": "github_commit",
                            "repo": repo,
                            "sha": c.get("sha"),
                            "author": author_info.get("name"),
                        },
                        raw={"sha": c.get("sha"), "message": commit_data.get("message")},
                    )
                )
        except Exception as e:
            self.log.debug("commits_error", repo=repo, error=str(e))
        return items

    async def _collect_repo_stats(self, repo: str) -> CollectedItem | None:
        """Fetch repo metadata: stars, forks, open issues."""
        try:
            data = await self._fetch_json(
                f"{self.BASE_URL}/repos/{repo}",
                headers=self._headers,
            )
            return CollectedItem(
                id=f"gh_stats_{repo}",
                title=f"[Stats] {repo}: ⭐{data.get('stargazers_count', 0)} 🍴{data.get('forks_count', 0)}",
                content=data.get("description", "") or "",
                url=data.get("html_url", ""),
                published_at=datetime.now(timezone.utc),
                metadata={
                    "data_type": "github_stats",
                    "repo": repo,
                    "stars": data.get("stargazers_count", 0),
                    "forks": data.get("forks_count", 0),
                    "open_issues": data.get("open_issues_count", 0),
                    "language": data.get("language"),
                    "pushed_at": data.get("pushed_at"),
                },
                raw={
                    "stars": data.get("stargazers_count"),
                    "forks": data.get("forks_count"),
                },
            )
        except Exception as e:
            self.log.debug("stats_error", repo=repo, error=str(e))
            return None

    async def _collect(self) -> CollectionResult:
        import asyncio

        repos = self._get_repos()
        repo_names = [r.get("repo") or r.get("url", "").replace("https://github.com/", "") for r in repos]
        repo_names = [r for r in repo_names if r and "/" in r]

        self.log.info("tracking_repos", count=len(repo_names))

        # Collect releases and recent commits in parallel
        release_tasks = [self._collect_repo_releases(repo) for repo in repo_names]
        commit_tasks = [self._collect_repo_commits(repo) for repo in repo_names]
        stats_tasks = [self._collect_repo_stats(repo) for repo in repo_names]

        all_results = await asyncio.gather(
            *release_tasks, *commit_tasks, *stats_tasks, return_exceptions=True
        )

        items = []
        for result in all_results:
            if isinstance(result, list):
                items.extend(result)
            elif isinstance(result, CollectedItem):
                items.append(result)

        return CollectionResult(
            source_id=self.source_id,
            source_name=self.source_name,
            source_type=self.source_type,
            items=items,
        )
