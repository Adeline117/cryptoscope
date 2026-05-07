#!/bin/bash
# CryptoScope exchange listing detector (checks every 60s)
cd /Users/adelinewen/cryptoscope-1
source .venv/bin/activate
export $(grep -v '^#' .env | grep -v '^$' | xargs)

# Check if already running
if pgrep -f "listing_monitor" > /dev/null; then
    exit 0
fi

python3 -c "
import asyncio, sys
sys.path.insert(0, '.')
from src.collectors.listing_detector import run_listing_monitor
asyncio.run(run_listing_monitor())
" >> /Users/adelinewen/cryptoscope-1/output/listing.log 2>&1
