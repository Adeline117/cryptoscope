"""GoPlus token-security — the CODE-RISK dimension this system never had.

Everything here is a behavioral detector's blind spot: whether the contract itself
lets the operator rug you. `is_mintable`, `transfer_pausable`, `owner_change_balance`,
and whether the LP is locked are ground-truth contract facts — verifiable against the
chain today, with no outcome labels required. That is precisely why they may be added
before the evaluation harness is finished, while a *fused risk score* may not: a score
needs weights, and un-backtested weights are how the fake 44% was born.

Keyless and free. Verified on chains 1 / 56 / 8453.

DISCIPLINE (this codebase's recurring disease is missing-read → confident conclusion):
  - fetch fails                → available=False. "UNCHECKED", never "clean".
  - is_open_source == 0        → the other flags are UNKNOWABLE, not False. A closed
                                 contract is a non-read, not a clean read.
  - every result is timestamped. A stale cache is not current safety.

What this CANNOT tell you: whether the operator intends to use the power, or when.
`lp_locked=0` means they CAN pull; it is a 戒备 (escalation) fact, not an entry signal.
"""

from __future__ import annotations

import json
import time
import urllib.request
from datetime import datetime, timezone

import structlog

logger = structlog.get_logger()

_CHAIN_ID = {"ethereum": "1", "bsc": "56", "base": "8453",
             "arbitrum": "42161", "polygon": "137", "optimism": "10"}
_API = "https://api.gopluslabs.io/api/v1/token_security"
_CACHE: dict = {}
_CACHE_TTL_S = 900          # 15 min: a lock can be released within the hour


def _num(v) -> int | None:
    """GoPlus returns "0"/"1" strings; anything else (absent, "") is UNKNOWN."""
    if v is None or v == "":
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _flt(v) -> float | None:
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def token_security(token: str, chain: str, timeout: int = 20) -> dict:
    """Raw-ish normalized security facts. Never raises.

    Returns {available, checked_at, is_open_source, flags{...}, lp{...}, holders[...]}
    with `available=False` and a `reason` when the read did not happen.
    """
    cid = _CHAIN_ID.get(chain)
    if not cid:
        return {"available": False, "reason": f"chain {chain} unsupported"}
    key = (cid, token.lower())
    now = time.time()
    hit = _CACHE.get(key)
    if hit and now - hit[0] < _CACHE_TTL_S:
        return hit[1]

    url = f"{_API}/{cid}?contract_addresses={token}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=timeout) as response:
            d = json.loads(response.read().decode())
    except Exception as e:
        logger.debug("goplus_fetch_failed", token=token, error=str(e)[:60])
        return {"available": False, "reason": f"fetch failed: {str(e)[:40]}"}

    if d.get("code") != 1:
        return {"available": False, "reason": f"api code {d.get('code')}: {d.get('message')}"}
    results = d.get("result") or {}
    row = next(iter(results.values()), None)
    if not row:
        return {"available": False, "reason": "token not indexed by GoPlus"}

    open_source = _num(row.get("is_open_source"))
    lp_holders = row.get("lp_holders") or []
    locked = sum(1 for h in lp_holders if str(h.get("is_locked")) == "1")

    out = {
        "available": True,
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "is_open_source": open_source,
        # A closed-source contract cannot be analysed: its flags are UNKNOWN, not safe.
        "flags_knowable": open_source == 1,
        "flags": {
            "is_honeypot": _num(row.get("is_honeypot")),
            "is_mintable": _num(row.get("is_mintable")),
            "transfer_pausable": _num(row.get("transfer_pausable")),
            "owner_change_balance": _num(row.get("owner_change_balance")),
            "hidden_owner": _num(row.get("hidden_owner")),
            "can_take_back_ownership": _num(row.get("can_take_back_ownership")),
            "is_blacklisted": _num(row.get("is_blacklisted")),
            "trading_cooldown": _num(row.get("trading_cooldown")),
            "cannot_sell_all": _num(row.get("cannot_sell_all")),
            "is_proxy": _num(row.get("is_proxy")),
            "buy_tax": _flt(row.get("buy_tax")),
            "sell_tax": _flt(row.get("sell_tax")),
        },
        "owner_address": (row.get("owner_address") or "").lower() or None,
        "creator_address": (row.get("creator_address") or "").lower() or None,
        "lp": {
            "holder_count": _num(row.get("lp_holder_count")),
            "n_holders_seen": len(lp_holders),
            "n_locked": locked,
            # None (not False) when there are no LP holders to judge.
            "all_locked": (locked == len(lp_holders)) if lp_holders else None,
            "holders": [{"address": (h.get("address") or "").lower(),
                         "is_locked": _num(h.get("is_locked")),
                         "percent": _flt(h.get("percent")),
                         "tag": h.get("tag") or ""} for h in lp_holders],
        },
        "holder_count": _num(row.get("holder_count")),
        "top_holders": [{"address": (h.get("address") or "").lower(),
                         "percent": _flt(h.get("percent")),
                         "is_contract": _num(h.get("is_contract")),
                         "is_locked": _num(h.get("is_locked")),
                         "tag": h.get("tag") or ""} for h in (row.get("holders") or [])],
    }
    _CACHE[key] = (now, out)
    return out


def rug_risk(token: str, chain: str) -> dict:
    """The `rug_risk` dimension attached to a verdict — FACTS, never a fused score.

    Deliberately emits no number. A weighted risk score would need weights, and
    un-backtested weights are exactly the mechanism that produced a confident-looking
    44% hit rate out of nothing. Callers render the facts; the backtest may earn the
    right to a score later.
    """
    sec = token_security(token, chain)
    if not sec.get("available"):
        return {"available": False, "reason": sec.get("reason"),
                "note": "代码风险未检查 — 不等于安全"}

    f = sec["flags"]
    facts, unknowns = [], []
    if not sec["flags_knowable"]:
        unknowns.append("合约未开源:所有代码风险标志不可核实(≠已核实安全)")
    else:
        if f["is_honeypot"] == 1:
            facts.append("蜜罐:买得进卖不出")
        if f["is_mintable"] == 1:
            facts.append("可增发(owner 能凭空铸币稀释)")
        if f["transfer_pausable"] == 1:
            facts.append("可暂停转账(能冻结你的卖出)")
        if f["owner_change_balance"] == 1:
            facts.append("owner 可直接改余额")
        if f["can_take_back_ownership"] == 1:
            facts.append("可收回 ownership(放弃是假的)")
        if f["hidden_owner"] == 1:
            facts.append("隐藏 owner")
        if (f["sell_tax"] or 0) >= 0.10:
            facts.append(f"卖出税 {f['sell_tax']*100:.0f}%")
    for k, v in f.items():
        if v is None and sec["flags_knowable"]:
            unknowns.append(f"{k} 未返回")

    lp = sec["lp"]
    if lp["all_locked"] is False:
        facts.append(f"LP 未锁定({lp['n_holders_seen'] - lp['n_locked']}/"
                     f"{lp['n_holders_seen']} 个 LP 持有人可随时撤池)")
    elif lp["all_locked"] is None:
        unknowns.append("无 LP 持有人数据,锁仓状态未知")

    # "Renounced" is a reassuring word, so it must be earned. A zero owner_address
    # means nothing if there is a HIDDEN owner or ownership can be taken back —
    # POD reports owner=0x0 while hidden_owner=1 AND owner_change_balance=1, i.e. the
    # renounce is theatre. Emit None (unknown) rather than a false all-clear.
    zero_owner = sec["owner_address"] in (
        None, "0x0000000000000000000000000000000000000000")
    if f["hidden_owner"] == 1 or f["can_take_back_ownership"] == 1:
        owner_renounced = None
        facts.append("放弃所有权不可信(存在隐藏owner/可收回) — 勿以'已放弃'安心")
    elif not sec["flags_knowable"]:
        owner_renounced = None            # closed source: can't verify the claim
    else:
        owner_renounced = zero_owner
    return {"available": True, "checked_at": sec["checked_at"],
            "facts": facts, "unknowns": unknowns,
            "owner_renounced": owner_renounced,
            "lp_all_locked": lp["all_locked"],
            "lp_locked_n": lp["n_locked"], "lp_holders_n": lp["n_holders_seen"],
            "is_open_source": sec["is_open_source"],
            "flags": f,
            "note": "事实陈述,非风险评分。'有能力撤池' ≠ '将要撤池'。"}
