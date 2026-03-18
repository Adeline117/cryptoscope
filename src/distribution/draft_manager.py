"""Track draft thread status: pending → editing → approved → posted → skipped."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import aiosqlite
import structlog

from src.config import DATA_DIR

logger = structlog.get_logger()

DB_PATH = DATA_DIR / "drafts.db"


class DraftManager:
    """SQLite-based draft lifecycle tracker."""

    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
        self._db: aiosqlite.Connection | None = None

    async def init(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._db = await aiosqlite.connect(str(self.db_path))
        await self._db.execute("""
            CREATE TABLE IF NOT EXISTS drafts (
                draft_id TEXT PRIMARY KEY,
                topic TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                priority_score REAL,
                text_en TEXT,
                text_zh TEXT,
                charts TEXT,  -- JSON array of chart filenames
                openclaw_url TEXT,
                twitter_urls TEXT,  -- JSON array of posted tweet URLs
                sources TEXT,  -- JSON array of source URLs
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)
        await self._db.commit()

    async def close(self) -> None:
        if self._db:
            await self._db.close()

    async def create_draft(
        self,
        draft_id: str,
        topic: str,
        text_en: str,
        text_zh: str,
        priority_score: float = 0,
        charts: list[str] | None = None,
        sources: list[str] | None = None,
    ) -> None:
        now = datetime.now(timezone.utc).isoformat()
        await self._db.execute(
            """INSERT INTO drafts
               (draft_id, topic, status, priority_score, text_en, text_zh, charts, sources, created_at, updated_at)
               VALUES (?, ?, 'pending', ?, ?, ?, ?, ?, ?, ?)""",
            (
                draft_id, topic, priority_score,
                text_en, text_zh,
                json.dumps(charts or []),
                json.dumps(sources or []),
                now, now,
            ),
        )
        await self._db.commit()
        logger.info("draft_created", draft_id=draft_id, topic=topic)

    async def update_status(self, draft_id: str, status: str) -> None:
        """Update draft status. Valid: pending, editing, approved, posted, skipped."""
        VALID_STATUSES = {"pending", "editing", "approved", "posted", "skipped"}
        if status not in VALID_STATUSES:
            raise ValueError(f"Invalid draft status: {status}. Must be one of {VALID_STATUSES}")
        now = datetime.now(timezone.utc).isoformat()
        await self._db.execute(
            "UPDATE drafts SET status = ?, updated_at = ? WHERE draft_id = ?",
            (status, now, draft_id),
        )
        await self._db.commit()

    async def set_openclaw_url(self, draft_id: str, url: str) -> None:
        now = datetime.now(timezone.utc).isoformat()
        await self._db.execute(
            "UPDATE drafts SET openclaw_url = ?, updated_at = ? WHERE draft_id = ?",
            (url, now, draft_id),
        )
        await self._db.commit()

    async def set_twitter_urls(self, draft_id: str, urls: list[str]) -> None:
        now = datetime.now(timezone.utc).isoformat()
        await self._db.execute(
            "UPDATE drafts SET twitter_urls = ?, updated_at = ? WHERE draft_id = ?",
            (json.dumps(urls), now, draft_id),
        )
        await self._db.commit()

    async def get_draft(self, draft_id: str) -> dict | None:
        async with self._db.execute(
            "SELECT * FROM drafts WHERE draft_id = ?", (draft_id,)
        ) as cursor:
            row = await cursor.fetchone()
            if row:
                cols = [d[0] for d in cursor.description]
                return dict(zip(cols, row))
        return None

    async def get_pending_drafts(self) -> list[dict]:
        async with self._db.execute(
            "SELECT * FROM drafts WHERE status = 'pending' ORDER BY priority_score DESC"
        ) as cursor:
            cols = [d[0] for d in cursor.description]
            return [dict(zip(cols, row)) for row in await cursor.fetchall()]

    async def get_stats(self) -> dict[str, int]:
        stats = {}
        async with self._db.execute(
            "SELECT status, COUNT(*) FROM drafts GROUP BY status"
        ) as cursor:
            for row in await cursor.fetchall():
                stats[row[0]] = row[1]
        return stats
