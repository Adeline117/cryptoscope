#!/bin/bash
# CryptoScope 5-min watchdog: depeg, crash, liquidation alerts
cd /Users/adelinewen/cryptoscope-1
source .venv/bin/activate
export $(grep -v '^#' .env | grep -v '^$' | xargs)
python3 -m src.pipeline.watchdog_pipeline >> /Users/adelinewen/cryptoscope-1/output/watchdog.log 2>&1
