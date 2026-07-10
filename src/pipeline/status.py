"""One command that tells you the truth about the bet.

    python -m src.pipeline.status

Deliberately answers only the questions that have answers today:
  - Is the bet reachable?      (kill_line: shortable events, accrual rate, ETA)
  - What did the alerts do?    (report: episodes, Wilson, lift vs base rate)
  - Is the machinery alive?    (are the accrual jobs actually running?)

It never prints a hit rate that the sample cannot support, and it never converts a
data failure into a reassuring number. If something is unknown, it says unknown.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone

from src.config import DATA_DIR


_TAIL_BYTES = 8_000_000     # the log is ~200MB; a small tail is a recent-time window


def _accrual_health() -> str:
    """Are the jobs that feed the thesis actually firing?

    A dead scheduler looks exactly like a quiet market from the outside — which is why
    this check exists. But the check must not lie either: an earlier version read the
    last 400KB of a 210MB log and printed '从未运行' for a job that had in fact run 380
    times. Absent from a truncated window ≠ never happened. We now report the LAST SEEN
    time, and say plainly when the window simply doesn't reach back far enough.
    """
    import re
    lines = ["机器是否活着(积累引擎)", "-" * 66]
    log = DATA_DIR.parent / "logs" / "scheduler.out.log"
    if not log.exists():
        lines.append("  scheduler.out.log 不存在 → 无法确认调度器在跑(不等于没跑)")
        return "\n".join(lines)
    try:
        size = log.stat().st_size
        with log.open("rb") as f:
            if size > _TAIL_BYTES:
                f.seek(-_TAIL_BYTES, 2)
            tail = f.read().decode("utf-8", errors="ignore")
    except Exception as e:
        lines.append(f"  日志读取失败: {str(e)[:50]} → 未知,不做判断")
        return "\n".join(lines)

    stamps = re.findall(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", tail, re.M)
    window = f"{stamps[0]} → {stamps[-1]}" if stamps else "无法解析时间"
    lines.append(f"  (只看日志尾部 {_TAIL_BYTES//10**6}MB,覆盖 {window};"
                 f" 窗口外的运行看不见 ≠ 没运行)")

    pat = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*?%s")
    for job, label in [("perp_cex_scan_done", "perp CEX充值扫描(日)"),
                       ("perp_mobilization_done", "perp 戒备事件扫描(6h)"),
                       ("operator_sentinel_done", "哨兵(5min)"),
                       ("outcomes_resolved", "结算(1h)")]:
        hits = re.findall(pat.pattern % job, tail, re.M)
        if hits:
            lines.append(f"  ✓  {label:26} {len(hits):>4} 次,最近 {hits[-1]}")
        else:
            lines.append(f"  ?  {label:26} 本窗口内未见(可能未到执行时间,或未运行)")
    if "approval_scan_gap_skipped" in tail:
        lines.append("  ⚠️ approval_scan_gap_skipped:有区块窗口被永久跳过,事件已丢失")
    if size > 100_000_000:
        lines.append(f"  ⚠️ 日志已 {size/10**6:.0f}MB,需要轮转,否则诊断只能看到最近几分钟")
    return "\n".join(lines)


def _pending() -> str:
    db = DATA_DIR / "alert_outcomes.db"
    if not db.exists():
        return "  alert_outcomes.db 不存在"
    now = datetime.now(timezone.utc)
    c = sqlite3.connect(str(db))
    try:
        def n(sql, *a):
            return c.execute(sql, a).fetchone()[0]
        base = "FROM alerts WHERE resolved=0 AND chain!='majors'"
        pend = n(f"SELECT COUNT(*) {base}")
        d1 = (now - timedelta(hours=24)).isoformat()
        d7 = (now - timedelta(days=7)).isoformat()
        overdue = n(f"SELECT COUNT(*) {base} AND ts < ?", d1)
        zombie = n(f"SELECT COUNT(*) {base} AND ts < ?", d7)
        nopx = n(f"SELECT COUNT(*) {base} AND (price0 IS NULL OR price0 <= 0)")
    finally:
        c.close()
    out = [f"  待结算 {pend} 条(其中超24h {overdue},超7天 {zombie})"]
    if nopx:
        out.append(f"  · {nopx} 条入场价为0 → 永远无法结算,应从分母中排除,而不是当作'未命中'")
    if zombie:
        out.append(f"  · {zombie} 条超过7天仍未结算:这些标的多半已无价源(下架/池子没了)。")
        out.append("    它们不是'待测量',是'不可测量' —— 计入待办会让进度看起来永远在动。")
    if overdue and overdue == pend and pend > 5:
        out.append("  ⚠️ 全部待结算都已超时 → 检查 resolve_outcomes 是否真的在跑")
    return "\n".join(out)


GOAL = """核心目标: 赚钱。操作性约束(一夜验证得出): 系统必须先不撒谎,任何下注才
建立在链上事实上而非故事上。
  已验证可变现 · 避雷 (pretrade 开仓前体检) —— 不需要择时,今天就有价值
  前向实验 · 早期操盘吸筹 + 聪明钱收敛 (fresh 币, 已实现PnL, 带死线)
  已证伪 · 链上做空择时 (0/45)、状态分类器择时 (lift 0.43)、结构类信号 (会撒谎)"""


def main() -> None:
    from src.pipeline.evidence import kill_line, report
    from src.pipeline.operator_sentinel import alerts_muted

    print(GOAL)
    print("\n" + "=" * 66 + "\n")
    print(kill_line())
    print("\n")
    print(_accrual_health())
    print("\n结算队列")
    print("-" * 66)
    print(_pending())
    print(f"\n推送状态: {'静音(未证明有edge)' if alerts_muted() else '开启'}"
          f"  — 记录始终进行,静音不影响积累")
    print("\n")
    print(report(with_base_rate=False))
    print("\n(基准率/lift 需要联网抽样,跑 `python -m src.pipeline.evidence` 获取完整版)")


if __name__ == "__main__":
    main()
