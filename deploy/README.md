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
```

Manage it:

```bash
launchctl list | grep cryptoscope          # status (PID, exit code)
launchctl unload ~/Library/LaunchAgents/com.cryptoscope.scheduler.plist   # stop
tail -f logs/scheduler.err.log             # logs
```

`KeepAlive` restarts the process on crash; `RunAtLoad` starts it on login/boot.
The LaunchAgent grants a finite 2048-fd soft limit (4096 hard limit). The scheduler
also raises a smaller inherited soft limit to `SCHEDULER_NOFILE_SOFT` (default 2048)
and serializes its two descriptor-heavy scans.

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
