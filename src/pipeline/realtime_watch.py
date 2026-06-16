"""Real-time operator watcher — second-level, not 5-minute.

For the confirmed clusters (BASED/ESPORTS/EVAA), the 5-minute scheduler poll is
too slow: in a thin book the operator can dump in seconds. This is a long-lived
loop that polls the cluster's combined on-chain balance every POLL_SECONDS (~20s,
~7 BSC blocks) and fires Telegram the instant the operator's net position turns —
catching their FIRST buy/sell while price has barely moved.

Free: combined balance via archive eth_call (rotated public RPCs), price via
DexScreener. No Moralis quota. Reuses operator_sentinel's check_run() so the
trigger/direction logic and the shared state file are identical (the 5-min
scheduler sentinel stays as a backstop; shared state avoids double-firing).

Runs under launchd (com.cryptoscope.watcher) — survives closing Cursor.

    python -m src.pipeline.realtime_watch          # run the loop
    python -m src.pipeline.realtime_watch --once   # single pass (debug)
"""

from __future__ import annotations

import sys
import time

import structlog

logger = structlog.get_logger()

POLL_SECONDS = 20


def _cycle() -> int:
    """One real-time pass: check + alert. Returns alert count."""
    from src.pipeline.anomaly_screener import _run_coro
    from src.pipeline.operator_sentinel import _format, check_run

    alerts = check_run()
    if alerts:
        from src.distribution.telegram_sender import send_alert
        _run_coro(send_alert("⚡ <b>实时</b>\n" + _format(alerts)))
        logger.warning("realtime_alert", count=len(alerts),
                       symbols=[a["symbol"] for a in alerts])
    return len(alerts)


def run_loop() -> None:
    from src.pipeline.operator_sentinel import _load

    n = len(_load())
    logger.info("realtime_watch_start", targets=n, poll_seconds=POLL_SECONDS)
    while True:
        try:
            _cycle()
        except Exception as e:
            logger.error("realtime_cycle_error", error=str(e)[:120])
        time.sleep(POLL_SECONDS)


def main():
    from dotenv import load_dotenv

    from src.config import PROJECT_ROOT
    load_dotenv(PROJECT_ROOT / ".env")
    if "--once" in sys.argv:
        print(f"实时巡检一次 → {_cycle()} 条告警")
        return
    run_loop()


if __name__ == "__main__":
    main()
