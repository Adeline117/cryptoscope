"""Academic paper collector: Semantic Scholar, DBLP, arXiv API."""

from __future__ import annotations

import os
from datetime import datetime, timezone

from src.collectors.base import BaseCollector, CollectedItem, CollectionResult
from src.config import load_sources


class AcademicPapersCollector(BaseCollector):
    """Collect recent academic papers on blockchain/DeFi/crypto topics."""

    source_id = "academic_papers"
    source_name = "Academic Papers"
    source_type = "api"

    SEMANTIC_SCHOLAR_URL = "https://api.semanticscholar.org/graph/v1"

    DEFAULT_QUERIES = [
        "decentralized finance DeFi",
        "blockchain consensus mechanism",
        "smart contract security vulnerability",
        "maximal extractable value MEV",
        "automated market maker AMM",
        "sybil detection blockchain",
        "cross-chain bridge security",
        "zero knowledge proof blockchain",
        "liquid staking",
        "blockchain scalability layer 2",
    ]

    def __init__(self, **kwargs):
        super().__init__(cache_ttl=21600, **kwargs)  # 6h cache
        self.api_key = os.environ.get("SEMANTIC_SCHOLAR_API_KEY", "")

    async def _search_semantic_scholar(self, query: str, limit: int = 20) -> list[CollectedItem]:
        """Search Semantic Scholar for recent papers."""
        headers = {}
        if self.api_key:
            headers["x-api-key"] = self.api_key

        try:
            data = await self._fetch_json(
                f"{self.SEMANTIC_SCHOLAR_URL}/paper/search",
                headers=headers,
                params={
                    "query": query,
                    "limit": limit,
                    "fields": "title,abstract,url,year,citationCount,authors,publicationDate,venue,externalIds",
                    "year": "2024-2026",
                },
            )
            items = []
            for paper in data.get("data", []):
                pub_date = paper.get("publicationDate")
                authors = [a.get("name", "") for a in paper.get("authors", [])[:5]]
                items.append(
                    CollectedItem(
                        id=f"ss_{paper.get('paperId', '')}",
                        title=paper.get("title", "Untitled"),
                        content=paper.get("abstract", "") or "",
                        url=paper.get("url", ""),
                        published_at=(
                            datetime.fromisoformat(pub_date) if pub_date else None
                        ),
                        metadata={
                            "data_type": "academic_paper",
                            "source": "semantic_scholar",
                            "query": query,
                            "authors": authors,
                            "venue": paper.get("venue", ""),
                            "citation_count": paper.get("citationCount", 0),
                            "year": paper.get("year"),
                            "category": "academic",
                        },
                        raw=paper,
                    )
                )
            return items
        except Exception as e:
            self.log.warning("semantic_scholar_error", query=query, error=str(e))
            return []

    async def _collect(self) -> CollectionResult:
        import asyncio

        # Load queries from config if available
        queries = self.DEFAULT_QUERIES
        try:
            sources = load_sources("academic.yaml")
            for s in sources:
                if s.get("id") == "semantic_scholar" and "queries" in s:
                    queries = s["queries"]
                    break
        except FileNotFoundError:
            pass

        tasks = [self._search_semantic_scholar(q) for q in queries]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        all_items = []
        seen_ids = set()
        for result in results:
            if isinstance(result, list):
                for item in result:
                    if item.id not in seen_ids:
                        seen_ids.add(item.id)
                        all_items.append(item)

        return CollectionResult(
            source_id=self.source_id,
            source_name=self.source_name,
            source_type=self.source_type,
            items=all_items,
        )
