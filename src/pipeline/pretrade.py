"""Pre-trade check — the one command to run before you buy anything.

This is the system's honest product. A full night of evidence showed it CANNOT time
entries (on-chain state doesn't predict 24h direction; the short thesis is falsified
0/45; the standing verdict is a constant that can't be early to anything). What it CAN
do, now that the data is clean, is tell you what to AVOID — and in crypto, not touching
the -80% holes is the largest reliably-capturable edge a retail account has.

So this is a DON'T-LOSE filter, never a BUY signal. It answers "is this safe to
consider" and "what would hurt me here", using only facts that can be verified on the
chain today:

  · identify_operator  — is an operator distributing / rotating loaded ammo?
  · effective_concentration (supply_verified) — real concentration, not a subset ratio
  · acquisition_mode   — a concentrated cluster that ALLOCATED is an issuer, not a 庄
  · goplus rug_risk    — mintable / pausable / fake-renounce / LP unlocked
  · market/terminal    — is the pool already dead

Every data failure is UNKNOWN, never a green light. "We couldn't check" is not "safe".

    python -m src.pipeline.pretrade <token> <chain>
"""

from __future__ import annotations

import argparse

import structlog

from src.onchain.operator_id import identify_operator

logger = structlog.get_logger()

# The three outcomes, in order of severity. UNKNOWN sits above CONSIDER on purpose:
# missing data is a reason for caution, not permission.
AVOID, CAUTION, UNKNOWN, CONSIDER = "AVOID", "CAUTION", "UNKNOWN", "CONSIDER"
_RANK = {AVOID: 0, CAUTION: 1, UNKNOWN: 2, CONSIDER: 3}

# Operator states that mean someone is already getting out / holds the trigger.
_SELLING = {"distributing", "distributing_or_churn", "exited_by_selling"}
_LOADED_THREAT = {"present_rotating_confirmed"}


def check(token: str, chain: str) -> dict:
    """Return {level, reasons:[...], facts:{...}}. Never raises."""
    reasons: list[tuple[str, str]] = []   # (level, human reason)
    facts: dict = {}

    try:
        op = identify_operator(token, chain)
    except Exception as e:
        return {"level": UNKNOWN, "reasons": [(UNKNOWN, f"判决引擎报错: {str(e)[:60]}")],
                "facts": {}}

    verdict = op.get("verdict")
    conf = op.get("confidence", 0)
    facts["operator_verdict"] = verdict
    facts["confidence"] = conf
    cur = op.get("current", {})

    # ---- operator behaviour ----
    if verdict in _SELLING:
        reasons.append((AVOID, f"操盘在派发/离场({verdict} conf{conf}):你买进的对手盘就是庄"))
    elif verdict in _LOADED_THREAT:
        reasons.append((CAUTION, f"换钱包装弹({verdict} conf{conf}):弹药在手,随时可砸"))
    elif verdict == "loaded_accumulating":
        reasons.append((CONSIDER, f"链上核实在吸筹(conf{conf}):唯一带正向含义的形态,但择时未验证"))
    elif verdict == "loaded_dormant":
        reasons.append((CAUTION, "装弹但休眠:庄在场却不动,不是买入理由"))
    elif verdict in ("loaded_live_operator", "live_operator"):
        # THE HOLE the review found: these are the engine's OWN "most dangerous live
        # setup" (a verified loaded operator holding the ammo), and loaded_live_operator
        # is the DEFAULT when 30d velocity can't be computed (common on free BSC RPC).
        # With no branch, an empty reasons list defaulted to CONSIDER → the avoidance
        # filter green-lit a loaded 庄. That is the exact opposite of its job.
        reasons.append((CAUTION, f"当前活簇装弹操盘({verdict} conf{conf}):庄在场持仓,"
                        f"随时可派发 — 不是安全,是最危险的活盘之一"))
    elif verdict == "too_young_to_judge":
        reasons.append((UNKNOWN, "代币过新,操盘生命周期无法判断"))
    elif verdict in ("unknown", "indeterminate_emptied"):
        reasons.append((UNKNOWN, f"操盘状态取数不足({verdict}):不构成安全,只是没查清"))
    elif verdict not in ("dispersed", "treasury", "treasury_only", "distributing_or_churn"):
        # Final catch-all: any UNRECOGNIZED verdict must not silently default to green.
        # A new verdict string added later can never green-light by omission.
        reasons.append((UNKNOWN, f"未知判决类型({verdict}):无法评估,不构成安全"))

    # concentration only counts when it's a share of REAL supply and BOUGHT (not issued)
    lg = cur.get("largest_entity_pct")
    if not cur.get("current_graph_available", True):
        pass  # replay / no snapshot — skip
    elif cur.get("holders_fetched", 1) == 0:
        reasons.append((UNKNOWN, "持仓快照取数失败:集中度不可判(≠分散)"))
    acq = cur.get("acquisition") or {}
    if acq.get("verdict") == "allocated":
        facts["acquisition"] = "allocated"
        reasons.append((CAUTION, "高集中但簇成员是被分配/铸造的:发行方持仓,非交易型操盘"))
    elif acq.get("verdict") == "bought" and lg and lg >= 15:
        facts["acquisition"] = "bought"
        reasons.append((CAUTION, f"协同簇从市场买入并持有 {lg:.0f}% 供应:真操盘在场"))

    # ---- contract facts (GoPlus) ----
    rr = op.get("rug_risk") or {}
    if not rr.get("available"):
        reasons.append((UNKNOWN, "合约代码风险未检查(GoPlus 无数据)— 不等于安全"))
    elif rr.get("is_open_source") == 0:
        # Closed source: GoPlus can't analyse the code, so its flags are unknowable
        # (rug_risk suppresses them from `facts`). Reading raw `flags` here would be
        # inconsistent — treat as UNCHECKED, not clean.
        reasons.append((UNKNOWN, "合约未开源:代码风险不可核实 — 不等于安全"))
        facts["rug_facts"] = rr.get("facts", [])
    else:
        facts["rug_facts"] = rr.get("facts", [])
        f = rr.get("flags", {})
        if f.get("is_honeypot") == 1:
            reasons.append((AVOID, "蜜罐:买得进卖不出"))
        if f.get("owner_change_balance") == 1:
            reasons.append((AVOID, "owner 可直接改你的余额"))
        if f.get("is_mintable") == 1 and rr.get("owner_renounced") is not True:
            reasons.append((AVOID, "可增发且所有权未真正放弃:随时稀释归零"))
        elif f.get("is_mintable") == 1:
            reasons.append((CAUTION, "可增发(owner 已弃权,风险降低但仍在)"))
        if rr.get("owner_renounced") is None and any(
                "不可信" in x for x in rr.get("facts", [])):
            reasons.append((AVOID, "放弃所有权是假的(隐藏 owner / 可收回)"))
        if f.get("transfer_pausable") == 1:
            reasons.append((AVOID, "可暂停转账:能冻结你的卖出"))
        if (f.get("sell_tax") or 0) >= 0.10:
            reasons.append((CAUTION, f"卖出税 {f['sell_tax']*100:.0f}%"))
        if rr.get("lp_all_locked") is False:
            reasons.append((CAUTION, f"LP 未锁定({rr.get('lp_locked_n')}/{rr.get('lp_holders_n')}):可撤池"))

    # ---- deployer track record (AVOIDANCE-ONLY, low coverage) ----
    # Modern tokens are mostly factory-deployed, so the "creator" is often a shared
    # bot/factory and this returns unknown for most tokens. It is wired as a red-flag-
    # only bonus: when it CAN confirm a serial rugger (the dev's prior tokens are all
    # dead pools), that is a near-certain avoid; otherwise it says nothing. It can
    # never green-light.
    try:
        from src.onchain.deployer_history import deployer_history
        dh = deployer_history(token, chain)
        if dh.get("verdict") == "serial_rugger":
            facts["deployer"] = dh
            reasons.append((AVOID, f"发币者连续跑路:{dh['reason']}"))
    except Exception:
        pass

    # ---- pool liveness ----
    mkt = cur.get("market") or {}
    if mkt.get("available"):
        facts["liquidity_usd"] = mkt.get("liquidity_usd")
        facts["volume_h24"] = mkt.get("volume_h24")
        if (mkt.get("liquidity_usd") or 0) < 15_000 and (mkt.get("volume_h24") or 0) < 2_000:
            reasons.append((CAUTION, "池子已枯、几乎无成交:进出都是巨额滑点,event 多半已发生"))

    level = min((r[0] for r in reasons), key=lambda x: _RANK[x], default=CONSIDER)
    return {"level": level, "reasons": reasons, "facts": facts,
            "token": token, "chain": chain}


def format_report(res: dict) -> str:
    icon = {AVOID: "🔴 别碰", CAUTION: "🟠 谨慎", UNKNOWN: "⚪ 查不清",
            CONSIDER: "🟢 无明显红旗"}[res["level"]]
    lines = [f"开仓前体检: {res['token'][:14]} [{res['chain']}]",
             "=" * 60, f"结论: {icon}", ""]
    order = sorted(res["reasons"], key=lambda r: _RANK[r[0]])
    tag = {AVOID: "🔴", CAUTION: "🟠", UNKNOWN: "⚪", CONSIDER: "🟢"}
    for lvl, why in order:
        lines.append(f"  {tag[lvl]} {why}")
    if not order:
        lines.append("  (没有触发任何检查项)")
    lines.append("")
    lines.append("这是【别亏钱】过滤器,不是买入信号。'无红旗' 只表示没查到雷,")
    lines.append("不表示会涨 —— 链上状态不预测短线方向(今晚已证)。仓位自负。")
    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("token")
    ap.add_argument("chain", nargs="?", default="bsc")
    args = ap.parse_args()
    print(format_report(check(args.token, args.chain)))


if __name__ == "__main__":
    main()
