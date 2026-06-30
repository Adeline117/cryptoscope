"""Configuration loader for CryptoScope."""

import os
import re
from pathlib import Path
from typing import Any

import yaml

PROJECT_ROOT = Path(__file__).parent.parent
CONFIG_DIR = PROJECT_ROOT / "config"
DATA_DIR = PROJECT_ROOT / "data"
OUTPUT_DIR = PROJECT_ROOT / "output"


def _load_dotenv_once() -> None:
    """Load .env at config-import time so EVERY entry point (scheduler, ad-hoc
    scripts, tests) has the API keys — not just the few that called load_dotenv
    explicitly. Without this, modules that import config but run outside the main
    entry points silently lose Moralis/Covalent/Etherscan keys → BSC holder/funder
    data quietly degrades to the keyless path (the 'data black hole' we kept hitting).
    Never overrides an already-set var; tolerates a missing python-dotenv."""
    env_path = PROJECT_ROOT / ".env"
    if not env_path.exists():
        return
    try:
        from dotenv import load_dotenv
        load_dotenv(env_path, override=False)
        return
    except Exception:
        pass
    # Manual fallback (no python-dotenv installed)
    try:
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            k = k.strip()
            v = v.strip().strip('"').strip("'")
            if k and k not in os.environ:
                os.environ[k] = v
    except Exception:
        pass


_load_dotenv_once()


def _resolve_env_vars(value: str) -> str | None:
    """Replace ${VAR_NAME} with environment variable value."""
    if not isinstance(value, str):
        return value
    pattern = re.compile(r"\$\{(\w+)\}")
    match = pattern.fullmatch(value)
    if match:
        return os.environ.get(match.group(1))
    # Partial substitution
    def replacer(m: re.Match) -> str:
        return os.environ.get(m.group(1), "")
    result = pattern.sub(replacer, value)
    return result if result != value else value


def _walk_resolve(obj: Any) -> Any:
    """Recursively resolve env vars in a config dict."""
    if isinstance(obj, dict):
        return {k: _walk_resolve(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_walk_resolve(v) for v in obj]
    if isinstance(obj, str):
        return _resolve_env_vars(obj)
    return obj


def load_settings() -> dict:
    """Load and return the main settings.yaml with env vars resolved."""
    path = CONFIG_DIR / "settings.yaml"
    with open(path) as f:
        raw = yaml.safe_load(f)
    return _walk_resolve(raw)


def load_sources(source_file: str) -> list[dict]:
    """Load a source registry YAML file from config/sources/."""
    path = CONFIG_DIR / "sources" / source_file
    with open(path) as f:
        data = yaml.safe_load(f)
    return data if isinstance(data, list) else data.get("sources", [])


def load_all_sources() -> list[dict]:
    """Load all source registry files and merge into a single list."""
    sources_dir = CONFIG_DIR / "sources"
    all_sources = []
    for path in sorted(sources_dir.glob("*.yaml")):
        try:
            data = yaml.safe_load(path.read_text())
            if isinstance(data, list):
                all_sources.extend(data)
            elif isinstance(data, dict) and "sources" in data:
                all_sources.extend(data["sources"])
        except Exception:
            continue
    return all_sources
