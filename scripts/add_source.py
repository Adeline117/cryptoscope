#!/usr/bin/env python3
"""CLI tool to add a new source to the registry."""

import sys
from pathlib import Path

import click
import yaml

sys.path.insert(0, str(Path(__file__).parent.parent))

SOURCES_DIR = Path(__file__).parent.parent / "config" / "sources"

SOURCE_FILES = {
    "exchange": "exchanges.yaml",
    "onchain": "onchain.yaml",
    "defi": "defi_protocols.yaml",
    "academic": "academic.yaml",
    "github": "github_repos.yaml",
    "research": "research_firms.yaml",
    "news": "news_media.yaml",
    "newsletter": "newsletters.yaml",
    "governance": "governance.yaml",
    "twitter": "twitter_lists.yaml",
    "telegram": "telegram_channels.yaml",
    "podcast": "podcasts.yaml",
    "regulatory": "regulatory.yaml",
}


@click.command()
@click.option("--category", "-c", type=click.Choice(list(SOURCE_FILES.keys())), prompt="Category")
@click.option("--id", "source_id", prompt="Source ID (unique slug)")
@click.option("--name", "source_name", prompt="Display name")
@click.option("--type", "source_type", default="rss", prompt="Type (rss/api/scrape/db)")
@click.option("--url", prompt="URL")
@click.option("--priority", default="medium", type=click.Choice(["high", "medium", "low"]))
@click.option("--notes", default="", help="Additional notes")
def main(category, source_id, source_name, source_type, url, priority, notes):
    """Add a new source to the registry."""
    target_file = SOURCES_DIR / SOURCE_FILES[category]

    new_entry = {
        "id": source_id,
        "name": source_name,
        "type": source_type,
        "url": url,
        "collector": "news_feed" if source_type == "rss" else "chain_data",
        "frequency": "2h",
        "category": category,
        "priority": priority,
        "enabled": True,
    }
    if notes:
        new_entry["notes"] = notes

    # Load existing or start fresh
    if target_file.exists():
        existing = yaml.safe_load(target_file.read_text()) or []
        if not isinstance(existing, list):
            existing = existing.get("sources", [])
    else:
        existing = []

    # Check for duplicate
    if any(s.get("id") == source_id for s in existing):
        click.echo(f"Error: Source ID '{source_id}' already exists in {target_file.name}")
        sys.exit(1)

    existing.append(new_entry)
    target_file.write_text(yaml.dump(existing, default_flow_style=False, allow_unicode=True))
    click.echo(f"Added '{source_name}' to {target_file.name} ({len(existing)} total sources)")


if __name__ == "__main__":
    main()
