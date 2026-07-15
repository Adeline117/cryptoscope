# Deploying the 24/7 scheduler

The long-lived process `src.pipeline.scheduler` runs all 15 cron jobs (daily
report, 2h highlight, 30min anomaly scan, GitHub tracking, etc.) and pushes to
Telegram. It must run on an always-on host.

## macOS (launchd) — for an always-on Mac

```bash
# 1. Fill in credentials
cp .env.example .env
$EDITOR .env          # at minimum TELEGRAM_BOT_TOKEN + TG_REVIEW_CHANNEL

# 2. Install dependencies
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt

# 3. Install the LaunchAgent (edit paths inside the plist if repo moved)
cp deploy/com.cryptoscope.scheduler.plist ~/Library/LaunchAgents/
launchctl load ~/Library/LaunchAgents/com.cryptoscope.scheduler.plist

# Independent read-only Hyperliquid BBO/L2/trades/context stream
cp deploy/com.cryptoscope.hyperliquid.plist ~/Library/LaunchAgents/
launchctl load ~/Library/LaunchAgents/com.cryptoscope.hyperliquid.plist

# Independent read-only standard Solana Pump.fun launch evidence stream
cp deploy/com.cryptoscope.solana-launches.plist ~/Library/LaunchAgents/
launchctl load ~/Library/LaunchAgents/com.cryptoscope.solana-launches.plist

# Independent read-only EVM factory event stream
cp deploy/com.cryptoscope.evm-factories.plist ~/Library/LaunchAgents/
launchctl load ~/Library/LaunchAgents/com.cryptoscope.evm-factories.plist
```

Manage it:

```bash
launchctl list | grep cryptoscope          # status (PID, exit code)
launchctl unload ~/Library/LaunchAgents/com.cryptoscope.scheduler.plist   # stop
launchctl unload ~/Library/LaunchAgents/com.cryptoscope.hyperliquid.plist # stop market stream
launchctl unload ~/Library/LaunchAgents/com.cryptoscope.solana-launches.plist # stop launch stream
launchctl unload ~/Library/LaunchAgents/com.cryptoscope.evm-factories.plist # stop factory stream
tail -f logs/scheduler.err.log             # logs
tail -f logs/hyperliquid.err.log           # stream errors/reconnects
tail -f logs/solana-launches.err.log        # launch stream errors/reconnects
tail -f logs/evm-factories.err.log          # EVM factory errors/reconnects
```

`KeepAlive` restarts the process on crash; `RunAtLoad` starts it on login/boot.
The LaunchAgent grants a finite 2048-fd soft limit (4096 hard limit). The scheduler
also raises a smaller inherited soft limit to `SCHEDULER_NOFILE_SOFT` (default 2048)
and serializes its two descriptor-heavy scans.

The Hyperliquid stream is a separate read-only process so a socket reconnect cannot
stall cron jobs. It uses only public subscriptions; `HL_STREAM_COINS=BTC,ETH,SOL`
can override the default universe.

The Solana launch stream is also isolated and read-only. It defaults to standard
public `logsSubscribe`/JSON-RPC endpoints, records only raw Pump.fun creation
evidence, and never promotes an event to a trade. Set `SOLANA_STREAM_WS_URL` and
`SOLANA_STREAM_RPC_URL` to dedicated provider endpoints for sustained production
rate limits; paid Helius `transactionSubscribe` access is not required.

The EVM factory stream starts with the official PancakeSwap V2 BSC factory. It
records `PairCreated` as raw, reorg-aware evidence and uses `newHeads` plus bounded
`eth_getLogs` recovery; it never turns a new pool directly into a trade signal.

## Credentials

The scheduler calls `load_dotenv()` on startup, reading `.env` from the project
root. `.env` is gitignored — never commit real tokens.

Telegram-only is enough to push. For full detection coverage also set
`ANTHROPIC_API_KEY`, `ETHERSCAN_API_KEY`, `DUNE_API_KEY`, etc. (see `.env.example`).

## Alternatives

- **Docker** — `docker compose up -d` (`CMD` already runs the scheduler).
- **GitHub Actions** — `.github/workflows/daily.yaml` / `weekly.yaml` run the
  daily/weekly pipelines in the cloud, but NOT the 2h/30min high-frequency jobs.
  Set the same names as repo Secrets.
