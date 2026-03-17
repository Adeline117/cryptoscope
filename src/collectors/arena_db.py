"""Arena PostgreSQL database collector for trader ranking insights."""

from __future__ import annotations

import os
from datetime import datetime, timezone

from src.collectors.base import BaseCollector, CollectedItem, CollectionResult


class ArenaDBCollector(BaseCollector):
    """Query Arena's PostgreSQL database for trader ranking insights."""

    source_id = "arena_db"
    source_name = "Arena Trader Rankings"
    source_type = "db"

    # Pre-defined analysis queries
    QUERIES = {
        "top_traders_24h": """
            SELECT trader_id, username, exchange, pnl_24h, pnl_7d, pnl_30d,
                   win_rate, total_trades, roi_24h
            FROM trader_rankings
            ORDER BY pnl_24h DESC
            LIMIT 20
        """,
        "top_traders_7d": """
            SELECT trader_id, username, exchange, pnl_7d, pnl_30d,
                   win_rate, total_trades, roi_7d
            FROM trader_rankings
            ORDER BY pnl_7d DESC
            LIMIT 20
        """,
        "biggest_position_changes": """
            SELECT trader_id, username, exchange, symbol,
                   position_size, position_change_24h, direction
            FROM trader_positions
            WHERE ABS(position_change_24h) > 100000
            ORDER BY ABS(position_change_24h) DESC
            LIMIT 20
        """,
        "exchange_volume_distribution": """
            SELECT exchange, SUM(volume_24h) as total_volume,
                   COUNT(DISTINCT trader_id) as active_traders
            FROM trader_rankings
            GROUP BY exchange
            ORDER BY total_volume DESC
        """,
        "new_top_entrants": """
            SELECT trader_id, username, exchange, pnl_7d, first_ranked_at
            FROM trader_rankings
            WHERE first_ranked_at > NOW() - INTERVAL '7 days'
            AND pnl_7d > 50000
            ORDER BY pnl_7d DESC
            LIMIT 10
        """,
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.db_config = {
            "host": os.environ.get("ARENA_DB_HOST", ""),
            "port": int(os.environ.get("ARENA_DB_PORT", "5432")),
            "database": os.environ.get("ARENA_DB_NAME", "arena"),
            "user": os.environ.get("ARENA_DB_USER", ""),
            "password": os.environ.get("ARENA_DB_PASS", ""),
        }

    async def _collect(self) -> CollectionResult:
        if not self.db_config["host"] or not self.db_config["user"]:
            self.log.warning("arena_db_not_configured")
            return CollectionResult(
                source_id=self.source_id,
                source_name=self.source_name,
                source_type=self.source_type,
            )

        items = []
        try:
            import asyncpg

            conn = await asyncpg.connect(**self.db_config)
            try:
                for query_name, sql in self.QUERIES.items():
                    try:
                        rows = await conn.fetch(sql)
                        records = [dict(r) for r in rows]
                        items.append(
                            CollectedItem(
                                id=f"arena_{query_name}_{datetime.now(timezone.utc).strftime('%Y%m%d')}",
                                title=f"Arena: {query_name.replace('_', ' ').title()}",
                                content=f"{len(records)} records",
                                url="https://arenafi.org",
                                published_at=datetime.now(timezone.utc),
                                metadata={
                                    "data_type": "arena_query",
                                    "query_name": query_name,
                                    "record_count": len(records),
                                    "records": records[:20],
                                    "category": "exchange_data",
                                    "subcategory": "trading",
                                    "priority": "high",
                                },
                                raw={"query": query_name, "row_count": len(records)},
                            )
                        )
                    except Exception as e:
                        self.log.warning("arena_query_failed", query=query_name, error=str(e))
            finally:
                await conn.close()
        except ImportError:
            self.log.error("asyncpg_not_installed")
        except Exception as e:
            self.log.error("arena_db_connection_failed", error=str(e))

        return CollectionResult(
            source_id=self.source_id,
            source_name=self.source_name,
            source_type=self.source_type,
            items=items,
        )
