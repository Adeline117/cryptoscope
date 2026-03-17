"""Governance monitoring: Snapshot proposals and on-chain governance."""

from __future__ import annotations

import json
from datetime import datetime, timezone

from src.collectors.base import BaseCollector, CollectedItem, CollectionResult
from src.config import load_sources

SNAPSHOT_GRAPHQL = "https://hub.snapshot.org/graphql"

PROPOSALS_QUERY = """
query Proposals($space: String!, $first: Int!) {
  proposals(
    first: $first,
    skip: 0,
    where: { space: $space, state: "active" },
    orderBy: "created",
    orderDirection: desc
  ) {
    id
    title
    body
    choices
    start
    end
    state
    scores
    scores_total
    votes
    author
    link
    space { id name }
  }
}
"""


class GovernanceWatchCollector(BaseCollector):
    """Monitor Snapshot spaces and governance forums for active proposals."""

    source_id = "governance_watch"
    source_name = "Governance Watch"
    source_type = "api"

    def __init__(self, **kwargs):
        super().__init__(cache_ttl=3600, **kwargs)

    def _get_snapshot_spaces(self) -> list[str]:
        """Load Snapshot space IDs from source registry."""
        try:
            sources = load_sources("governance.yaml")
            return [
                s["space_id"]
                for s in sources
                if s.get("subcategory") == "snapshot"
                and s.get("enabled", True)
                and "space_id" in s
            ]
        except FileNotFoundError:
            return []

    async def _fetch_snapshot_proposals(self, space_id: str) -> list[CollectedItem]:
        """Fetch active proposals from a Snapshot space."""
        try:
            payload = json.dumps({
                "query": PROPOSALS_QUERY,
                "variables": {"space": space_id, "first": 10},
            })

            async with self._semaphore:
                async with self._session.post(
                    SNAPSHOT_GRAPHQL,
                    data=payload,
                    headers={"Content-Type": "application/json"},
                ) as resp:
                    resp.raise_for_status()
                    data = await resp.json()

            items = []
            proposals = data.get("data", {}).get("proposals", [])
            for p in proposals:
                end_ts = p.get("end", 0)
                items.append(
                    CollectedItem(
                        id=f"snapshot_{p['id']}",
                        title=f"[{p.get('space', {}).get('name', space_id)}] {p.get('title', '')}",
                        content=p.get("body", "")[:2000],
                        url=f"https://snapshot.org/#/{space_id}/proposal/{p['id']}",
                        published_at=datetime.fromtimestamp(p.get("start", 0), tz=timezone.utc),
                        metadata={
                            "data_type": "governance_proposal",
                            "platform": "snapshot",
                            "space": space_id,
                            "space_name": p.get("space", {}).get("name", ""),
                            "state": p.get("state"),
                            "choices": p.get("choices", []),
                            "scores": p.get("scores", []),
                            "scores_total": p.get("scores_total", 0),
                            "votes": p.get("votes", 0),
                            "end_time": datetime.fromtimestamp(end_ts, tz=timezone.utc).isoformat() if end_ts else None,
                            "author": p.get("author", ""),
                            "category": "governance",
                        },
                        raw=p,
                    )
                )
            return items
        except Exception as e:
            self.log.warning("snapshot_error", space=space_id, error=str(e))
            return []

    async def _collect(self) -> CollectionResult:
        import asyncio

        spaces = self._get_snapshot_spaces()
        self.log.info("checking_snapshot_spaces", count=len(spaces))

        tasks = [self._fetch_snapshot_proposals(space) for space in spaces]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        all_items = []
        for result in results:
            if isinstance(result, list):
                all_items.extend(result)

        return CollectionResult(
            source_id=self.source_id,
            source_name=self.source_name,
            source_type=self.source_type,
            items=all_items,
        )
