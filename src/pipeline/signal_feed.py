"""One ranked signal feed over the few get-rich directions.

The get-rich plays in this market are few and known: 打新 (early launches),
吸筹 (quiet operator accumulation), 聪明钱 (proven wallets converging), and
派发/做空 (operators moving to exit — a short/avoid signal). The detectors that
find each already run; this module just AGGREGATES their latest output into one
ranked "today's targets" list with the evidence, and hands it to a human trader
(Telegram DM + a board JSON) who does their own timing, sizing, and stops.

Not a buy button, not autonomous execution — a ranked shortlist with the on-chain
why, so the trader looks at fewer, better charts.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

from src.config import DATA_DIR

EXPORT_DIR = DATA_DIR / "board_export"
SIGNALS_FILE = EXPORT_DIR / "signals.json"
TOP_N = 6

DIRECTIONS = ("打新", "吸筹", "聪明钱", "派发做空")


def _num(value, default=0.0):
    try:
        out = float(value)
        return out if out == out else default
    except (TypeError, ValueError):
        return default


def _candidate(direction, symbol, chain, token, score, why):
    return {"direction": direction, "symbol": str(symbol or "?"),
            "chain": str(chain or "?"), "token": str(token or ""),
            "score": round(float(score), 1), "why": why}


def _launch_candidates(launch_events, top_n, *, safety_fn=None, now=None):
    """打新: freshest launches, honeypots dropped, code-risk flagged (GoPlus, free)."""
    current = (now or datetime.now(timezone.utc)).timestamp()
    rows = []
    for e in launch_events or []:
        if not isinstance(e, dict) or not e.get("token"):
            continue
        try:
            ts = datetime.fromisoformat(
                str(e.get("detected_at")).replace("Z", "+00:00")).timestamp()
        except (TypeError, ValueError):
            continue
        rows.append((ts, e))
    rows.sort(key=lambda x: -x[0])
    out = []
    # Scan more than top_n so honeypots can be dropped without starving the list.
    for ts, e in rows[:top_n * 3]:
        if len(out) >= top_n:
            break
        mins = (current - ts) / 60
        chain = e.get("chain", "?")
        why = f"{round(mins)} 分钟前新发现 · {chain}"
        score = max(0, 100 - mins / 3)
        safe = safety_fn(e.get("token"), chain) if safety_fn else None
        if safe and safe.get("available"):
            if safe.get("honeypot"):
                continue  # never surface a token you can't sell out of
            facts = safe.get("facts") or []
            if facts:
                why += " · 🚨 " + "/".join(facts[:3])
                score *= 0.4
            else:
                why += " · ✅ 安全已检"
        else:
            why += " · 安全未检"
        out.append(_candidate("打新", e.get("symbol"), chain, e.get("token"), score, why))
    out.sort(key=lambda c: -c["score"])
    return out


def _goplus_safety(token: str, chain: str) -> dict:
    """Free keyless GoPlus code-risk facts; honeypot flagged for dropping."""
    from src.onchain import goplus_client

    r = goplus_client.rug_risk(token, chain)
    if not r.get("available"):
        return {"available": False}
    facts = r.get("facts") or []
    return {"available": True,
            "honeypot": any("蜜罐" in str(f) for f in facts),
            "facts": facts}


def _operator_candidates(operators, top_n):
    """吸筹 (buy phase) and 派发做空 (sell/distribute phase) from operator records."""
    accumulate, distribute = [], []
    for o in operators or []:
        if not isinstance(o, dict) or not o.get("token"):
            continue
        phase = str(o.get("live_phase") or "").lower()
        conf = _num(o.get("confidence"))
        ent = _num(o.get("largest_entity_pct"))
        liq = _num(o.get("liquidity_usd"))
        buys, sells = _num(o.get("buys_h24")), _num(o.get("sells_h24"))
        base = f"庄簇 {ent:.0f}% · 置信 {conf:.0f} · 流动性 ${liq:,.0f}"
        if phase == "buy" and str(o.get("acquisition")) == "bought":
            accumulate.append(_candidate(
                "吸筹", o.get("symbol"), o.get("chain"), o.get("token"),
                conf, f"{base} · 买入相 24h买/卖 {buys:.0f}/{sells:.0f}"))
        elif phase in ("sell", "distribute") or (sells > buys * 1.3 and sells > 200):
            distribute.append(_candidate(
                "派发做空", o.get("symbol"), o.get("chain"), o.get("token"),
                conf + (sells - buys) / max(buys, 1) * 10,
                f"{base} · 派发相 24h买/卖 {buys:.0f}/{sells:.0f}"))
    accumulate.sort(key=lambda c: -c["score"])
    distribute.sort(key=lambda c: -c["score"])
    return accumulate[:top_n], distribute[:top_n]


def _smart_money_candidates(smart_buys, top_n):
    """聪明钱: proven wallets converging on a fresh token (>=2, ranked by count)."""
    out = []
    for b in smart_buys or []:
        if not isinstance(b, dict) or int(b.get("n_buyers") or 0) < 2:
            continue
        n = int(b["n_buyers"])
        mins = b.get("mins_ago")
        why = f"{n} 个已证钱包买入 · {b.get('usd_bought', 0):,} 美元 · {mins} 分钟前"
        out.append(_candidate("聪明钱", b.get("symbol"), b.get("chain"),
                              b.get("token"), n * 20, why))
    out.sort(key=lambda c: -c["score"])
    return out[:top_n]


def build_feed(*, launch_events=None, operators=None, smart_buys=None,
               now=None, top_n=TOP_N, launch_safety_fn=None) -> dict:
    """Pure aggregation: the four directions' latest candidates, ranked, with evidence."""
    stamp = (now or datetime.now(timezone.utc)).astimezone(timezone.utc).isoformat()
    accumulate, distribute = _operator_candidates(operators, top_n)
    directions = {
        "打新": _launch_candidates(launch_events, top_n,
                                 safety_fn=launch_safety_fn, now=now),
        "吸筹": accumulate,
        "聪明钱": _smart_money_candidates(smart_buys, top_n),
        "派发做空": distribute,
    }
    total = sum(len(v) for v in directions.values())
    return {"schema_version": 1, "view": "signals", "generated_at": stamp,
            "n_candidates": total, "directions": directions,
            "disclaimer": "链上信号候选,不是买入指令;止损/仓位/择时自行判断。"}


def format_text(feed: dict) -> str:
    """A Telegram-friendly digest grouped by direction."""
    icons = {"打新": "🚀", "吸筹": "🟢", "聪明钱": "🐋", "派发做空": "🔴"}
    lines = [f"📡 信号台 · {feed.get('n_candidates', 0)} 个候选",
             feed.get("generated_at", "")[:16].replace("T", " ") + " UTC", ""]
    for d in DIRECTIONS:
        rows = feed.get("directions", {}).get(d) or []
        if not rows:
            continue
        lines.append(f"{icons.get(d, '·')} {d}")
        for c in rows:
            lines.append(f"  {c['symbol']} ({c['chain']}) · {c['why']}")
        lines.append("")
    lines.append(feed.get("disclaimer", ""))
    return "\n".join(lines).strip()


def _read_json(path):
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return {}


def _write_signals(feed: dict) -> None:
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    tmp = SIGNALS_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(feed, ensure_ascii=False, separators=(",", ":")))
    tmp.replace(SIGNALS_FILE)


def run(*, now=None, push_telegram=True, push_blob=True) -> dict:
    """Aggregate live sources, write signals.json, push to blob + Telegram DM."""
    launch = _read_json(EXPORT_DIR / "launch.json").get("events") or []
    operators = _read_json(EXPORT_DIR / "operators.json").get("operators") or []
    try:
        from src.onchain import smart_wallets
        smart = smart_wallets.fresh_smart_buys_result().get("buys") or []
    except Exception:
        smart = []
    feed = build_feed(launch_events=launch, operators=operators,
                      smart_buys=smart, now=now, launch_safety_fn=_goplus_safety)
    _write_signals(feed)
    pushed = False
    if push_blob:
        try:
            from src.pipeline import board_export
            board_export.push_to_blob([SIGNALS_FILE])
        except Exception:
            pass
    if push_telegram and feed["n_candidates"]:
        try:
            from src.distribution import telegram_sender
            pushed = telegram_sender.send_plain(format_text(feed))
        except Exception:
            pushed = False
    return {"n_candidates": feed["n_candidates"], "telegram_pushed": pushed}


if __name__ == "__main__":
    print(json.dumps(run(push_telegram=False), ensure_ascii=False, indent=2))
