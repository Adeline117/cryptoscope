"""Pre-dump MOBILIZATION signals — the operator's logistics BEFORE firing.

The dump lifecycle the sentinels currently see starts at "庄在卖" (already selling)
— by then the event is half over. But an operator must do observable logistics
first, and each step is on-chain:

  1. rotate ammo into fresh wallets   (days ahead — present_rotating covers this)
  2. GAS-FUND the ammo wallets        (hours-day ahead — a parked wallet needs
                                       native token to pay for the coming sells)
  3. APPROVE the DEX router           (minutes-hours ahead — a wallet's FIRST
                                       router sell requires an Approval tx)
  4. test-sell / deposit to CEX       (imminent — CEX充值 alarm covers this)
  5. sell                             (庄在卖 — confirmation, not prediction)

This module adds steps 2 and 3. Both are cheap keyless reads and both fail to
UNKNOWN, never to a phantom signal: an incomplete scan reports complete=False and
the caller must not advance its cursor past unscanned blocks.

Precision honesty: an Approval arms a wallet, it doesn't commit it — expect more
false alarms than 庄在卖. These are ESCALATION alerts (戒备), not entry signals.
"""

from __future__ import annotations

import structlog

logger = structlog.get_logger()

_APPROVAL_TOPIC = "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925"
_EVM = {"bsc", "ethereum", "base", "arbitrum"}
# native-balance jump that reads as "funding the dump txs", per chain (in native
# units). Below this = dust/refund noise.
_GAS_TOPUP_MIN = {"bsc": 0.01, "ethereum": 0.004, "base": 0.002, "arbitrum": 0.002}
_MAX_WINDOW = 20_000        # clamp a stale cursor so one pass never scans unbounded


def _pad_topic(addr: str) -> str:
    return "0x" + addr.lower().replace("0x", "").rjust(64, "0")


def approval_scan(token: str, chain: str, wallets: list[str],
                  from_block: int | None) -> dict:
    """Approval events on `token` where the OWNER is a watched wallet — the wallet
    is arming to sell on a DEX. One eth_getLogs call (owners OR'd in topic1).

    Returns {complete, to_block, approvals:[{owner, spender, spender_kind}]}.
    complete=False means the window wasn't scanned — the caller must NOT advance
    its cursor (a skipped window would silently swallow an arming event)."""
    if chain not in _EVM:
        return {"complete": False, "approvals": [], "to_block": None}
    try:
        from src.onchain.evm_archive import ArchiveRPC
        rpc = ArchiveRPC(chain)
        head = rpc.logs_head()
    except Exception as e:
        logger.debug("approval_scan_no_head", chain=chain, error=str(e)[:60])
        return {"complete": False, "approvals": [], "to_block": None}
    frm = head - 1_200 if from_block is None else from_block + 1
    frm = max(frm, head - _MAX_WINDOW)
    if frm > head:
        return {"complete": True, "approvals": [], "to_block": head}
    try:
        r = rpc._logs_call("eth_getLogs", [{
            "address": token,
            "fromBlock": hex(frm), "toBlock": hex(head),
            "topics": [_APPROVAL_TOPIC, [_pad_topic(w) for w in wallets[:20]]],
        }])
        res = r.get("result")
        if not isinstance(res, list):
            return {"complete": False, "approvals": [], "to_block": None}
    except Exception as e:
        logger.debug("approval_scan_failed", token=token, error=str(e)[:80])
        return {"complete": False, "approvals": [], "to_block": None}

    routers = set()
    try:
        from src.onchain.operator_id import _infra
        inf = _infra(chain)
        routers = inf["routers"] | inf["bridges"]
    except Exception:
        pass
    seen, approvals = set(), []
    for lg in res:
        tp = lg.get("topics", [])
        if len(tp) < 3:
            continue
        owner = "0x" + tp[1][-40:].lower()
        spender = "0x" + tp[2][-40:].lower()
        # zero-value approval = REVOKE, not arming
        try:
            if int(lg.get("data", "0x0"), 16) == 0:
                continue
        except (ValueError, TypeError):
            pass
        k = (owner, spender)
        if k in seen:
            continue
        seen.add(k)
        approvals.append({"owner": owner, "spender": spender,
                          "spender_kind": "router" if spender in routers else "other"})
    return {"complete": True, "approvals": approvals, "to_block": head}


def gas_topup_scan(chain: str, wallets: list[str],
                   prev_balances: dict | None) -> dict:
    """Native-balance jump on watched ammo wallets = someone is funding the coming
    transactions. First pass ARMS (records balances, no alert). A failed read keeps
    the previous stored balance so a flaky RPC can't fake a jump on recovery.

    Returns {balances:{wallet: native}, topups:[{wallet, delta}], armed:bool}."""
    if chain not in _EVM:
        return {"balances": prev_balances or {}, "topups": [], "armed": False}
    try:
        from src.onchain.evm_archive import ArchiveRPC
        rpc = ArchiveRPC(chain)
    except Exception:
        return {"balances": prev_balances or {}, "topups": [], "armed": False}
    min_jump = _GAS_TOPUP_MIN.get(chain, 0.01)
    balances: dict[str, float] = dict(prev_balances or {})
    topups = []
    first_pass = not prev_balances
    for w in wallets[:15]:
        wl = w.lower()
        try:
            res = rpc._call("eth_getBalance", [wl, "latest"]).get("result")
            if not res or res == "0x":
                continue                       # soft failure: keep stored value
            bal = int(res, 16) / 1e18
        except Exception:
            continue
        prev = balances.get(wl)
        if (not first_pass and prev is not None
                and bal - prev >= min_jump):
            topups.append({"wallet": wl, "delta": round(bal - prev, 4)})
        balances[wl] = bal
    return {"balances": balances, "topups": topups, "armed": True}
