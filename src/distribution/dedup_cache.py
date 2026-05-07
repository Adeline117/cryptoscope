"""Deduplication cache to prevent pushing the same news item twice.

Uses SQLite for persistence. Items are identified by an MD5 hash of the first
60 characters of their title, and expire after a configurable TTL.
"""

from __future__ import annotations

import hashlib
import sqlite3
import time
from pathlib import Path

import structlog

logger = structlog.get_logger()


class DedupCache:
    """SQLite-backed dedup cache for sent Telegram items."""

    def __init__(self, db_path: str = "data/sent_items.db", ttl_hours: int = 6):
        self.db_path = Path(db_path)
        self.ttl_seconds = ttl_hours * 3600
        self._ensure_db()

    def _ensure_db(self) -> None:
        """Create the database and table if they don't exist."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS sent_items (
                    key_hash TEXT PRIMARY KEY,
                    title_preview TEXT,
                    sent_at REAL NOT NULL
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_sent_at ON sent_items (sent_at)
            """)
            conn.commit()

    def _connect(self) -> sqlite3.Connection:
        """Open a SQLite connection."""
        return sqlite3.connect(str(self.db_path))

    @staticmethod
    def _hash_title(title: str) -> str:
        """Generate MD5 hash from the first 60 chars of a title."""
        normalized = title[:60].strip().lower()
        return hashlib.md5(normalized.encode("utf-8")).hexdigest()

    def has_been_sent(self, item_title: str) -> bool:
        """Check if a similar title was sent within the TTL window.

        Args:
            item_title: The title of the item to check.

        Returns:
            True if the item was already sent recently.
        """
        key = self._hash_title(item_title)
        cutoff = time.time() - self.ttl_seconds

        with self._connect() as conn:
            row = conn.execute(
                "SELECT sent_at FROM sent_items WHERE key_hash = ? AND sent_at > ?",
                (key, cutoff),
            ).fetchone()

        if row is not None:
            logger.debug("dedup_hit", title_preview=item_title[:40])
            return True
        return False

    def mark_sent(self, item_title: str) -> None:
        """Record that this item was sent.

        Uses INSERT OR REPLACE to update the timestamp if the key already exists.

        Args:
            item_title: The title of the item that was sent.
        """
        key = self._hash_title(item_title)
        now = time.time()

        with self._connect() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO sent_items (key_hash, title_preview, sent_at) VALUES (?, ?, ?)",
                (key, item_title[:80], now),
            )
            conn.commit()

        logger.debug("dedup_marked", title_preview=item_title[:40])

    def cleanup(self) -> int:
        """Remove entries older than the TTL.

        Returns:
            Number of rows deleted.
        """
        cutoff = time.time() - self.ttl_seconds

        with self._connect() as conn:
            cursor = conn.execute(
                "DELETE FROM sent_items WHERE sent_at < ?",
                (cutoff,),
            )
            conn.commit()
            deleted = cursor.rowcount

        if deleted > 0:
            logger.info("dedup_cleanup", deleted=deleted)
        return deleted

    def stats(self) -> dict:
        """Return cache statistics."""
        cutoff = time.time() - self.ttl_seconds

        with self._connect() as conn:
            total = conn.execute("SELECT COUNT(*) FROM sent_items").fetchone()[0]
            active = conn.execute(
                "SELECT COUNT(*) FROM sent_items WHERE sent_at > ?",
                (cutoff,),
            ).fetchone()[0]
            expired = total - active

        return {
            "total_entries": total,
            "active_entries": active,
            "expired_entries": expired,
            "ttl_hours": self.ttl_seconds // 3600,
        }

    def clear(self) -> None:
        """Clear all entries (use for testing)."""
        with self._connect() as conn:
            conn.execute("DELETE FROM sent_items")
            conn.commit()
        logger.info("dedup_cache_cleared")
