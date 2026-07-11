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
MAX_TRADES_PER_TOKEN = 50  # a trader does ~3-7/token; a bot does hundreds (live-measured)
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
    # The SET of tokens traded — a behavioral fingerprint. A wallet FARM (one actor,
    # many separately-funded wallets to defeat funder clustering) shows up as several
    # wallets with near-identical token sets. convergence() collapses on this so a
    # farm can't masquerade as N independent skilled entities (the CZBULL lesson).
    token_set = frozenset((x.get("token_address") or "").lower()
                          for x in per if x.get("token_address"))
    # trades-per-token cleanly separates a discretionary trader (~3-7 trades/token,
    # live-measured) from a high-frequency bot (>100/token) — sharper than the absolute
    # MAX_TRADES, which a 2900-trades/10-tokens bot would slip past.
    per_token = trades / n_tok if n_tok else 0
    is_bot = trades > MAX_TRADES or per_token > MAX_TRADES_PER_TOKEN
    is_garbage = realized > MAX_REALIZED_USD    # overflow / whale, not copyable
    skilled = (MIN_TRADES <= trades <= MAX_TRADES and n_tok >= MIN_TOKENS
               and per_token <= MAX_TRADES_PER_TOKEN
               and win_rate >= MIN_WIN_RATE
               and MIN_REALIZED_USD <= realized <= MAX_REALIZED_USD)
    reason = ("跨%d币%d笔·净$%d·胜率%.0f%%" % (n_tok, trades, realized, win_rate * 100))
    if is_bot:
        reason = f"机器人({trades:,}笔)— 非可跟随交易者"
    elif is_garbage:
        reason = f"盈利数异常(${realized:.0e})— 溢出/巨鲸,剔除"
    return {"available": True, "skilled": skilled, "trades": trades, "tokens": n_tok,
            "realized_usd": round(realized), "win_rate": round(win_rate, 2),
            "is_bot": is_bot, "reason": reason, "token_set": token_set}


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
        # Moralis swaps does not reliably populate walletAddress — try the same key
        # fallbacks the proven reader uses, or the wallet silently drops and buyers
        # are undercounted toward a false 'unknown'.
        w = (r.get("walletAddress") or r.get("wallet_address")
             or r.get("fromAddress") or r.get("from_address") or "").lower()
        if w and w not in seen:
            seen.add(w)
            out.append(w)
    return out


def _collapse_to_entities(wallets: list[str], chain: str) -> list[list[str]]:
    """Group funder-linked wallets: five mules from one funder are ONE actor, not five
    converging smart-money wallets. Each returned group is one independent entity.
    A wallet whose funder can't be resolved stays its own entity (unknown ≠ merged).

    A CEX/disperser funder does NOT indicate one actor — it links thousands of
    unrelated retail wallets (the falsified 'family root' lesson). Collapsing on a
    CEX funder would merge genuinely independent smart-money wallets and defeat the
    whole independence check, so a CEX-funded wallet stays its own entity."""
    try:
        from src.onchain.funder_graph import get_funders
        fmap = get_funders(wallets, chain)
    except Exception:
        fmap = {}
    try:
        from src.onchain.cex_addresses import evm_exchanges
        cex = {a.lower() for a in evm_exchanges()}
    except Exception:
        cex = set()
    by_funder: dict = {}
    singletons: list[list[str]] = []
    for w in wallets:
        f = str(fmap.get(w) or "").lower()
        if f and f not in cex:          # a shared NON-CEX funder = same actor
            by_funder.setdefault(f, []).append(w)
        else:                            # no funder, or a CEX funder → independent
            singletons.append([w])
    return list(by_funder.values()) + singletons


def _collapse_by_behavior(skilled: list[dict], containment: float = 0.6) -> list[list[dict]]:
    """Group skilled wallets that trade near-identical token SETS into one actor.

    A wallet farm funds each wallet separately (often from a CEX) so the funder graph
    never links them — but the wallets run ONE strategy, so their traded-token sets
    are near-identical. Independent traders share at most the few hot tokens; a farm
    shares ~everything. CZBULL surfaced 10 'skilled entities' that were one farm:
    three sampled wallets had 61/61, 60/61, 60/61 identical token sets. So collapse on
    token-set containment: if one wallet's set is >=`containment` inside another's,
    they are the same actor. Greedy single-link clustering (farms are tight, so the
    representative-set drift of single-link is not a problem here)."""
    groups: list[dict] = []
    for sk in skilled:
        ts = sk.get("token_set") or frozenset()
        placed = False
        if ts:
            for g in groups:
                rep = g["rep"]
                inter = len(ts & rep)
                denom = min(len(ts), len(rep)) or 1
                if inter / denom >= containment:
                    g["members"].append(sk)
                    g["rep"] = rep | ts          # grow the actor's footprint
                    placed = True
                    break
        if not placed:
            groups.append({"rep": set(ts), "members": [sk]})
    return [g["members"] for g in groups]


def convergence(token: str, chain: str, max_check: int = 20) -> dict:
    """Independent SKILLED wallets converging on this token now.

    Returns {available, buyers_checked, skilled_entities, skilled_wallets, verdict,
    detail}. `verdict` ∈ convergence | some | none | unknown. Convergence requires
    >=3 INDEPENDENT skilled entities — one skilled buyer is noise, a cluster of mules
    is one actor no matter how many wallets, and a behaviorally-identical wallet FARM
    (separately funded to dodge funder clustering) is likewise ONE actor.
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
        # An entity counts as skilled if ANY of its wallets is skilled — group[0] is
        # just the first-seen wallet (DESC order), not the best, so scoring only it
        # would miss a skilled wallet grouped behind an unskilled co-funded one.
        hit = None
        for w in group[:3]:              # bounded: check up to 3 of a group
            if checked >= max_check:
                break
            checked += 1
            sk = wallet_skill(w, chain)
            time.sleep(0.2)
            if sk.get("skilled"):
                hit = {"wallet": w, "mules": len(group), **sk}
                break
        if hit:
            skilled_entities += 1
            skilled_wallets.append(hit)

    # SECOND independence pass: collapse behaviorally-identical wallets (a farm the
    # funder graph missed). The COUNT that decides convergence is farms, not wallets.
    farms = _collapse_by_behavior(skilled_wallets)
    distinct_actors = len(farms)
    farmed_out = skilled_entities - distinct_actors     # wallets revealed as one farm

    if distinct_actors >= 3:
        verdict = "convergence"
    elif distinct_actors >= 1:
        verdict = "some"
    else:
        verdict = "none"

    detail = (f"{checked}个买家实体中 {skilled_entities} 个聪明钱钱包 → 按行为去重后 "
              f"{distinct_actors} 个独立主体({'收敛' if verdict=='convergence' else verdict})")
    caveats = ["反身性:人人跟同一批聪明钱→edge衰减",
               "延迟:你看到时已落后其入场价",
               "幸存者偏差:盈利历史仍可能是运气,前向验证+死线才算数"]
    if farmed_out > 0:
        detail += f" ⚠️ 有 {farmed_out} 个钱包被识别为同一钱包农场(交易同一组币),已合并"
        caveats.insert(0, f"钱包农场:{skilled_entities}个'聪明钱'实为{distinct_actors}个主体,"
                          f"农场刷量伪装成收敛(CZBULL教训)")
    return {"available": True, "buyers_checked": checked,
            "independent_entities": len(entities),
            "skilled_wallets_n": skilled_entities,
            "skilled_entities": distinct_actors,          # behavior-deduped = the real count
            "skilled_wallets": skilled_wallets, "verdict": verdict,
            "detail": detail, "caveats": caveats}
