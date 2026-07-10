"""Smart-money convergence — the offense angle built on the one thing that can't be
faked: realized profit.

A full night proved that STRUCTURE lies. Ghost balances, issuer allocations dressed as
operators, subset ratios posing as supply share — every structural tell fooled us at
least once. Realized PnL cannot be dressed up: a wallet either made money across its
trades or it didn't, and Moralis computes it from the chain.

So this flips the question. Not "is this token shaped like a 妖币" (structure), but
"are wallets that have PROVABLY made money before buying it now, and are several
INDEPENDENT ones converging" — which is what the profitable tools (Nansen/GMGN)
actually run on.

Two hard disciplines, both learned tonight:

  · CONSISTENCY, not one lucky win. A wallet needs enough closed trades across enough
    tokens before its win rate means skill rather than variance.
  · INDEPENDENCE. Five wallets funded by one source buying the same token is ONE actor
    (an operator using mules), not convergence. Funder-linked buyers collapse to one
    before anything is counted — the same same-entity logic that separated operators
    from noise all night.

Honest limits that ship WITH the signal, never buried:
  · Reflexivity — if everyone copies the same smart wallets, the edge decays.
  · Latency — by the time a smart wallet's buy is on-chain, you are behind its entry.
  · Survivorship — a profitable history can still be luck; the trade-count floor only
    reduces, never removes, that.
This produces a WATCH signal to be forward-tested against a base rate, never a buy.
"""

from __future__ import annotations

import time

import structlog

logger = structlog.get_logger()

_MCHAIN = {"bsc": "bsc", "ethereum": "eth", "base": "base", "arbitrum": "arbitrum"}

# Skill thresholds. The FIRST live run fired on MEV/arbitrage bots — one "smart money"
# wallet showed 152,284 trades, another 72,310,888 with a nonsense $2.4-octillion PnL
# (a high-frequency bot, and its realized figure corrupted by token-decimal overflow).
# A wallet you would copy into a directional position makes hundreds to a few thousand
# discretionary trades, not millions. So skill is BOUNDED on both sides, and the PnL
# must be in a believable, copyable range — a green checkmark was trusted once; the
# output was not looked at. It is now.
MIN_TRADES = 20            # below this, win rate is variance, not skill
MAX_TRADES = 3_000         # above this it's a bot arbitraging the pool, not a trader
MIN_TOKENS = 8             # distinct tokens: one hot token ≠ skill
MIN_WIN_RATE = 0.50        # profitable on a majority of positions
MIN_REALIZED_USD = 1_000   # actually made money, not rounding noise
MAX_REALIZED_USD = 50_000_000  # above this = whale/bot/garbage, not a copyable trader


def wallet_skill(address: str, chain: str) -> dict:
    """Realized-PnL skill profile for a wallet. Never raises.

    {available, trades, tokens, realized_usd, win_rate, skilled, reason}. `skilled` is
    True only on an AFFIRMATIVE read that clears every floor; a fetch failure is
    available=False (unknown), never skilled and never explicitly unskilled.
    """
    from src.onchain import moralis_client
    mch = _MCHAIN.get(chain)
    if not mch or not moralis_client.usable():
        return {"available": False, "skilled": False, "reason": "no source"}
    s = moralis_client.get(f"wallets/{address}/profitability/summary?chain={mch}")
    if not s:
        return {"available": False, "skilled": False, "reason": "summary 取数失败"}
    trades = int(s.get("total_count_of_trades") or 0)
    realized = float(s.get("total_realized_profit_usd") or 0)
    if trades == 0:
        return {"available": True, "skilled": False, "trades": 0,
                "realized_usd": realized, "reason": "无 DEX 交易记录(持仓/合约钱包)"}
    per = (moralis_client.get(f"wallets/{address}/profitability?chain={mch}") or {}).get("result") or []
    n_tok = len(per)
    wins = sum(1 for x in per if float(x.get("realized_profit_usd") or 0) > 0)
    win_rate = wins / n_tok if n_tok else 0.0
    is_bot = trades > MAX_TRADES
    is_garbage = realized > MAX_REALIZED_USD    # overflow / whale, not copyable
    skilled = (MIN_TRADES <= trades <= MAX_TRADES and n_tok >= MIN_TOKENS
               and win_rate >= MIN_WIN_RATE
               and MIN_REALIZED_USD <= realized <= MAX_REALIZED_USD)
    reason = ("跨%d币%d笔·净$%d·胜率%.0f%%" % (n_tok, trades, realized, win_rate * 100))
    if is_bot:
        reason = f"机器人({trades:,}笔)— 非可跟随交易者"
    elif is_garbage:
        reason = f"盈利数异常(${realized:.0e})— 溢出/巨鲸,剔除"
    return {"available": True, "skilled": skilled, "trades": trades, "tokens": n_tok,
            "realized_usd": round(realized), "win_rate": round(win_rate, 2),
            "is_bot": is_bot, "reason": reason}


def _recent_buyers(token: str, chain: str, limit: int = 60) -> list[str]:
    """Distinct wallets that BOUGHT the token recently (Moralis swaps)."""
    from src.onchain import moralis_client
    mch = _MCHAIN.get(chain)
    if not mch:
        return []
    d = moralis_client.get(f"erc20/{token}/swaps?chain={mch}&order=DESC&limit={limit}")
    out, seen = [], set()
    for r in ((d or {}).get("result") or []):
        if str(r.get("transactionType")).lower() != "buy":
            continue
        w = (r.get("walletAddress") or "").lower()
        if w and w not in seen:
            seen.add(w)
            out.append(w)
    return out


def _collapse_to_entities(wallets: list[str], chain: str) -> list[list[str]]:
    """Group funder-linked wallets: five mules from one funder are ONE actor, not five
    converging smart-money wallets. Each returned group is one independent entity.
    A wallet whose funder can't be resolved stays its own entity (unknown ≠ merged)."""
    try:
        from src.onchain.funder_graph import get_funders
        fmap = get_funders(wallets, chain)
    except Exception:
        fmap = {}
    by_funder: dict = {}
    singletons: list[list[str]] = []
    for w in wallets:
        f = str(fmap.get(w) or "").lower()
        if f:
            by_funder.setdefault(f, []).append(w)
        else:
            singletons.append([w])
    return list(by_funder.values()) + singletons


def convergence(token: str, chain: str, max_check: int = 20) -> dict:
    """Independent SKILLED wallets converging on this token now.

    Returns {available, buyers_checked, skilled_entities, skilled_wallets, verdict,
    detail}. `verdict` ∈ convergence | some | none | unknown. Convergence requires
    >=3 INDEPENDENT skilled entities — one skilled buyer is noise, and a cluster of
    mules is one actor no matter how many wallets.
    """
    buyers = _recent_buyers(token, chain)
    if not buyers:
        return {"available": False, "verdict": "unknown",
                "detail": "无近期买家数据(swaps 空/取数失败)"}

    entities = _collapse_to_entities(buyers, chain)
    skilled_entities = 0
    skilled_wallets: list[dict] = []
    checked = 0
    for group in entities:
        if checked >= max_check:
            break
        # score the group's largest/first wallet as its representative
        rep = group[0]
        checked += 1
        sk = wallet_skill(rep, chain)
        time.sleep(0.2)
        if sk.get("skilled"):
            skilled_entities += 1
            skilled_wallets.append({"wallet": rep, "mules": len(group), **sk})

    if skilled_entities >= 3:
        verdict = "convergence"
    elif skilled_entities >= 1:
        verdict = "some"
    else:
        verdict = "none"
    return {"available": True, "buyers_checked": checked,
            "independent_entities": len(entities),
            "skilled_entities": skilled_entities,
            "skilled_wallets": skilled_wallets, "verdict": verdict,
            "detail": (f"{checked}个独立买家实体中 {skilled_entities} 个是已实现盈利的"
                       f"聪明钱({'收敛' if verdict=='convergence' else verdict})"),
            "caveats": ["反身性:人人跟同一批聪明钱→edge衰减",
                        "延迟:你看到时已落后其入场价",
                        "幸存者偏差:盈利历史仍可能是运气,前向验证+死线才算数"]}
