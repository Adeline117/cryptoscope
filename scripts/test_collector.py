#!/usr/bin/env python3
"""Test individual collectors interactively."""

import asyncio
import json
import sys
from datetime import datetime

import click
from rich.console import Console
from rich.table import Table

sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent.parent))

console = Console()

COLLECTORS = {
    "news": "src.collectors.news_feed:NewsFeedCollector",
    "defillama": "src.collectors.chain_data:DeFiLlamaCollector",
    "dune": "src.collectors.chain_data:DuneCollector",
    "etherscan": "src.collectors.chain_data:EtherscanCollector",
    "github": "src.collectors.github_tracker:GitHubTracker",
}


def _import_collector(path: str):
    module_path, class_name = path.rsplit(":", 1)
    module = __import__(module_path, fromlist=[class_name])
    return getattr(module, class_name)


@click.command()
@click.argument("collector_name", type=click.Choice(list(COLLECTORS.keys())))
@click.option("--limit", "-l", default=20, help="Max items to display")
@click.option("--json-output", "-j", is_flag=True, help="Output as JSON")
def main(collector_name: str, limit: int, json_output: bool):
    """Test a specific collector."""
    collector_cls = _import_collector(COLLECTORS[collector_name])
    collector = collector_cls()

    console.print(f"\n[bold cyan]Testing collector: {collector_name}[/bold cyan]\n")

    result = asyncio.run(collector.collect())

    if json_output:
        items = [item.model_dump(mode="json") for item in result.items[:limit]]
        print(json.dumps(items, indent=2, default=str))
        return

    console.print(
        f"[green]Collected {len(result.items)} new items[/green] "
        f"from {result.source_name}"
    )

    if not result.items:
        console.print("[yellow]No new items found.[/yellow]")
        return

    table = Table(title=f"Top {min(limit, len(result.items))} Items")
    table.add_column("#", width=4)
    table.add_column("Title", max_width=60)
    table.add_column("URL", max_width=40)
    table.add_column("Published", width=20)

    for i, item in enumerate(result.items[:limit], 1):
        pub = item.published_at.strftime("%Y-%m-%d %H:%M") if item.published_at else "—"
        url_short = item.url[:37] + "..." if len(item.url) > 40 else item.url
        table.add_row(str(i), item.title[:60], url_short, pub)

    console.print(table)


if __name__ == "__main__":
    main()
