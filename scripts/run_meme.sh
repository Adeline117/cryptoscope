#!/usr/bin/env bash
# Run the meme coin scanning pipeline (every 10 minutes via cron/scheduler)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_DIR"

# Load environment
if [ -f .env ]; then
    set -a
    source .env
    set +a
fi

export PYTHONPATH="$PROJECT_DIR:${PYTHONPATH:-}"

echo "$(date '+%Y-%m-%d %H:%M:%S') [MEME] Starting meme coin scan pipeline..."
python -m src.pipeline.meme_pipeline
echo "$(date '+%Y-%m-%d %H:%M:%S') [MEME] Pipeline complete."
