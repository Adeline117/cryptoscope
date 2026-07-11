"""System health dashboard for the accumulation system.

One command to see what the 24/7 pipeline has accumulated and whether everything
is wired:

    python -m src.ops.health

Reports per-DB row counts (snapshots, tokens, signals, watchlist, funders),
which API keys are configured, and the launchd process status. Also exposes
`send_health_summary()` for a daily Telegram push.

Every query is defensive — tables may not exist yet on a fresh install.
"""

from __future__ import annotations

import os
import sqlite3
import subprocess
from datetime import datetime, timezone

from src.config import DATA_DIR


def _scalar(db: str, sql: str, params: tuple = ()) -> int:
    path = DATA_DIR / db
    if not path.exists():
        return 0
    try:
        conn = sqlite3.connect(str(path), timeout=5)
        conn.execute("PRAGMA busy_timeout=3000")
        try:
            row = conn.execute(sql, params).fetchone()
            return int(row[0]) if row and row[0] is not None else 0
        finally:
            conn.close()
    except Exception:
        return 0


def _rows(db: str, sql: str) -> list:
    path = DATA_DIR / db
    if not path.exists():
        return []
    try:
        conn = sqlite3.connect(str(path), timeout=5)
        try:
            return conn.execute(sql).fetchall()
        finally:
            conn.close()
    except Exception:
        return []


# A detector's max tolerated gap between EVALUABLE results. transfer_flow stamps
# every 5-min pass while cursors advance; 48h stale (accounting for legit RPC
# outages that age out) means it is broken, not merely quiet.
_DETECTOR_MAX_AGE_H = {"transfer_flow": 48}


def _dead_signals() -> list[dict]:
    """Registered detectors whose last EVALUABLE result is missing or too old.

    Returns [{detector, age_hours|None, reason}]. Only flags a detector when there
    are registered sentinels for it to evaluate — an empty sentinel set means the
    detector is idle-by-design, not dead."""
    import json as _json
    from datetime import datetime, timezone

    from src.config import DATA_DIR
    sf = DATA_DIR / "operator_sentinels.json"
    try:
        n_sentinels = len(_json.loads(sf.read_text())) if sf.exists() else 0
    except Exception:
        n_sentinels = 0
    if n_sentinels == 0:
        return []   # nothing to evaluate → not dead

    try:
        from src.pipeline.operator_sentinel import detector_heartbeats
        hb = detector_heartbeats()
    except Exception:
        hb = {}

    now = datetime.now(timezone.utc)
    dead = []
    for det, max_age in _DETECTOR_MAX_AGE_H.items():
        ts = hb.get(det)
        if not ts:
            dead.append({"detector": det, "age_hours": None,
                         "reason": f"从未产出可评估结果(有{n_sentinels}个哨兵在监控)"})
            continue
        try:
            age_h = (now - datetime.fromisoformat(ts)).total_seconds() / 3600
        except Exception:
            continue
        if age_h > max_age:
            dead.append({"detector": det, "age_hours": round(age_h, 1),
                         "reason": f"距上次可评估 {age_h:.0f}h > {max_age}h — 每5分钟在跑却静默无评估(游标类静默失败)"})
    return dead


def collect_stats() -> dict:
    """Gather health metrics from every DB + environment."""
    # Holder snapshots
    snap_total = _scalar("holder_snapshots.db", "SELECT COUNT(*) FROM holder_snapshots")
    snap_tokens = _scalar("holder_snapshots.db", "SELECT COUNT(DISTINCT token) FROM holder_snapshots")
    snap_latest = _rows("holder_snapshots.db", "SELECT MAX(snapshot_at) FROM holder_snapshots")
    last_snap = snap_latest[0][0] if snap_latest and snap_latest[0][0] else None

    # Stalled/frozen holder feeds — a snapshot source that silently stopped
    # updating (or serves a cached identical row) makes "latest" lie. Only flag
    # tokens we are ACTIVELY watching (operator sentinels + live watchlist), so a
    # real regression like SIREN surfaces without drowning in dormant candidates.
    stale_snaps = []
    try:
        import json as _json

        from src.config import DATA_DIR
        from src.onchain.holder_snapshot import find_stale_snapshots

        # Operator sentinels are the tokens we monitor CLOSELY — where a frozen
        # holder feed silently corrupts the analysis (the SIREN 48%-whale ghost).
        # The watchlist is excluded: it is full of dormant screener entries and
        # would just re-introduce the noise we scoped out.
        watched: list[str] = []
        sf = DATA_DIR / "operator_sentinels.json"
        if sf.exists():
            watched += [v.get("token") for v in _json.loads(sf.read_text()).values()
                        if v.get("token")]
        stale_snaps = find_stale_snapshots(tokens=watched) if watched else []
    except Exception:
        stale_snaps = []

    # Dead-signal detection: a detector that runs every pass but silently evaluates
    # nothing (the weeks-dead 庄在卖 cursor bug). Distinct from data-freshness above:
    # this is DETECTOR-evaluability, not snapshot age. Only meaningful when there ARE
    # registered sentinels for the detector to evaluate.
    dead_signals = _dead_signals()

    # Tokens with enough history to fire the signal (>=4 snapshots in window)
    ready = _scalar(
        "holder_snapshots.db",
        "SELECT COUNT(*) FROM (SELECT token FROM holder_snapshots "
        "GROUP BY token, chain HAVING COUNT(*) >= 4)",
    )

    # Signals by type
    sig_rows = _rows("signal_scorecard.db", "SELECT signal_type, COUNT(*) FROM signals GROUP BY signal_type")
    signals = {t: n for t, n in sig_rows}
    sig_pending = _scalar("signal_scorecard.db", "SELECT COUNT(*) FROM signals WHERE checked_24h = 0")

    # Watchlist + funders
    watch_active = _scalar("watchlist.db", "SELECT COUNT(*) FROM watchlist WHERE status='watching'")
    funders_cached = _scalar("funder_graph.db", "SELECT COUNT(*) FROM funders")
    funders_resolved = _scalar("funder_graph.db", "SELECT COUNT(*) FROM funders WHERE funder IS NOT NULL")

    # Screener persistence: tokens tracked + the most-recurring candidates
    screener_tracked = _scalar("screener_state.db", "SELECT COUNT(*) FROM screener_state")
    recurring = _rows(
        "screener_state.db",
        "SELECT token, chain, appearances FROM screener_state "
        "WHERE appearances >= 3 ORDER BY appearances DESC LIMIT 8",
    )
    top_recurring = [{"token": t, "chain": c, "appearances": n} for t, c, n in recurring]

    # Labels accumulated for validation/training
    from pathlib import Path
    labels_dir = Path("data/research/labels")
    label_files = [f for f in labels_dir.glob("*.json") if not f.name.startswith("_")] if labels_dir.exists() else []
    labels_pump = labels_dud = 0
    for f in label_files:
        try:
            import json as _j
            o = _j.loads(f.read_text())
            if o.get("outcome") == "pump":
                labels_pump += 1
            elif o.get("outcome") == "dud":
                labels_dud += 1
        except Exception:
            pass

    # API keys
    keys = {
        k: bool(os.environ.get(k))
        for k in ("TELEGRAM_BOT_TOKEN", "HELIUS_API_KEY", "ALCHEMY_API_KEY",
                  "ETHERSCAN_API_KEY", "DUNE_API_KEY", "ANTHROPIC_API_KEY")
    }

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "snapshots": {"total": snap_total, "tokens": snap_tokens,
                      "signal_ready_tokens": ready, "last_snapshot_at": last_snap,
                      "stale_count": len(stale_snaps),
                      "stale": [{"token": s["token"], "chain": s["chain"],
                                 "reason": s["reason"], "age_hours": s["age_hours"]}
                                for s in stale_snaps[:8]]},
        "dead_signals": dead_signals,
        "signals": {"by_type": signals, "pending_price_checks": sig_pending},
        "watchlist_active": watch_active,
        "funders": {"cached": funders_cached, "resolved": funders_resolved},
        "screener": {"tracked": screener_tracked, "top_recurring": top_recurring},
        "labels": {"pump": labels_pump, "dud": labels_dud,
                   "ready_for_calibration": labels_pump >= 5 and labels_dud >= 5},
        "api_keys": keys,
        "scheduler": _scheduler_status(),
    }


def _scheduler_status() -> dict:
    try:
        out = subprocess.run(
            ["launchctl", "list"], capture_output=True, text=True, timeout=5
        ).stdout
        for line in out.splitlines():
            if "com.cryptoscope.scheduler" in line:
                parts = line.split()
                return {"running": parts[0] != "-", "pid": parts[0], "last_exit": parts[1]}
        return {"running": False, "pid": None}
    except Exception:
        return {"running": None, "pid": None}


def format_report(stats: dict) -> str:
    """Human-readable plain-text report (for CLI / logs)."""
    s = stats
    L = []
    L.append("=" * 50)
    L.append("CryptoScope 健康看板")
    L.append("=" * 50)
    sch = s["scheduler"]
    L.append(f"调度器: {'✅ 运行中 PID ' + str(sch['pid']) if sch.get('running') else '❌ 未运行'}")
    L.append("")
    snap = s["snapshots"]
    L.append("📸 持币快照")
    L.append(f"  总快照: {snap['total']}  |  追踪 token: {snap['tokens']}")
    L.append(f"  够历史可出信号的 token (≥4快照): {snap['signal_ready_tokens']}")
    L.append(f"  最近快照: {snap['last_snapshot_at'] or '尚无'}")
    if snap.get("stale_count"):
        L.append(f"  ⚠️ 数据冻结/停更: {snap['stale_count']} 个 token (该token的持币数据不可信)")
        for st in snap.get("stale", []):
            age = f"{st['age_hours']}h" if st.get("age_hours") is not None else "?"
            L.append(f"    {st['token'][:16]}… [{st['chain']}] {st['reason']} · 距今 {age}")
    L.append("")
    L.append("🎯 信号")
    if s["signals"]["by_type"]:
        for t, n in s["signals"]["by_type"].items():
            L.append(f"  {t}: {n}")
    else:
        L.append("  尚无信号（需快照积累到 ≥4 才会触发）")
    L.append(f"  待价格回填: {s['signals']['pending_price_checks']}")
    L.append("")
    L.append(f"👁  观察名单(near-saturation): {s['watchlist_active']}")
    f = s["funders"]
    L.append(f"🔗 funder 缓存: {f['cached']} (已解析 {f['resolved']})")
    L.append("")
    scr = s.get("screener", {})
    L.append(f"🔎 筛选器追踪 token: {scr.get('tracked', 0)}")
    if scr.get("top_recurring"):
        L.append("  持续出现的候选(越多轮越可信):")
        for r in scr["top_recurring"]:
            L.append(f"    {r['token'][:16]}… [{r['chain']}] × {r['appearances']}轮")
    else:
        L.append("  (暂无持续≥3轮的候选)")
    lab = s.get("labels", {})
    L.append("")
    L.append(f"🏷  标签: 拉盘 {lab.get('pump',0)} / 横死 {lab.get('dud',0)}"
             + ("  ✅可校准权重" if lab.get("ready_for_calibration") else "  (各需≥5才能校准)"))
    L.append("")
    L.append("🔑 API keys")
    L.append("  " + "  ".join(f"{k.split('_')[0]}{'✅' if v else '❌'}" for k, v in s["api_keys"].items()))
    L.append("=" * 50)
    return "\n".join(L)


def format_telegram(stats: dict) -> str:
    s = stats
    snap = s["snapshots"]
    sigs = sum(s["signals"]["by_type"].values()) if s["signals"]["by_type"] else 0
    return (
        f"📊 <b>CryptoScope 日报 · 系统健康</b>\n"
        f"━━━━━━━━━━━━━━\n"
        f"📸 快照 {snap['total']} 条 · {snap['tokens']} 个 token\n"
        f"   可出信号(≥4快照): <b>{snap['signal_ready_tokens']}</b>\n"
        + (f"   ⚠️ 数据冻结/停更: <b>{snap['stale_count']}</b> 个 token\n" if snap.get("stale_count") else "")
        + ("".join(f"🛑 <b>信号已死</b>: {d['detector']} — {d['reason']}\n"
                   for d in s.get("dead_signals", []))
           if s.get("dead_signals") else "")
        + f"🎯 累计信号 {sigs} 条\n"
        f"👁 观察名单 {s['watchlist_active']}\n"
        f"🔎 筛选器追踪 {s.get('screener',{}).get('tracked',0)} · 持续候选 {len(s.get('screener',{}).get('top_recurring',[]))}\n"
        f"🏷 标签 拉盘{s.get('labels',{}).get('pump',0)}/横死{s.get('labels',{}).get('dud',0)}\n"
        f"调度器 {'✅' if s['scheduler'].get('running') else '❌'}\n"
        f"<i>积累中——持续多轮的候选最值得查 Arkham</i>"
    )


async def send_health_summary() -> bool:
    from src.distribution.telegram_sender import send_alert

    stats = collect_stats()
    # A dead signal is an OUTAGE, not a status line. The daily summary is calibrated
    # to read 'healthy', so a silently-dead detector must ALSO fire its own loud
    # alarm — otherwise it hides in a green report (exactly how 庄在卖 stayed dead
    # for weeks). Sent unconditionally (not gated by OPERATOR_ALERTS_MUTED): this is
    # an operational alarm, not a trade signal.
    dead = stats.get("dead_signals") or []
    if dead:
        lines = ["🛑 <b>信号已死 · DEAD SIGNAL</b>", "━━━━━━━━━━━━━━"]
        for d in dead:
            lines.append(f"• <b>{d['detector']}</b>: {d['reason']}")
        lines.append("<i>检测器每轮都在跑却没在评估任何东西 — 立刻查游标/数据源,当前该信号的沉默不可信</i>")
        try:
            await send_alert("\n".join(lines))
        except Exception:
            pass
    return await send_alert(format_telegram(stats))


if __name__ == "__main__":
    try:
        from dotenv import load_dotenv

        from src.config import PROJECT_ROOT

        load_dotenv(PROJECT_ROOT / ".env")
    except ImportError:
        pass
    print(format_report(collect_stats()))
