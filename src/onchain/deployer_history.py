"""Deployer track record — is the dev a serial rugger, or has their work survived?

The sharpest LEADING signal the research pointed to: a token's deployer usually isn't
new. The same wallet has launched tokens before, and their fate is on-chain. A dev
whose last five tokens are all dead pools is telling you what the sixth will be.

This is framed as an AVOIDANCE signal first, on purpose. The honest asymmetry:
  · serial_rugger  — the prior tokens all died. STRONG, verifiable, cheap. A near-
                     certain avoid. Nothing about it depends on predicting a pump.
  · prior_survived — some prior tokens still trade. WEAK positive: survival is not a
                     pump, and a survivor could be luck. Never a buy signal alone.

Why current-state-of-prior-tokens and not realized returns: a token that pumped-then-
died and one that rugged both read as "dead pool" today. Distinguishing them needs
per-token historical price, which is expensive and often missing for dead tokens. The
liquidity-alive-now proxy is cheap and honest about what it measures — survival, not
return — and survival is exactly what the avoid case turns on.

Fail-to-unknown throughout: no creator, no creations found, or unpriceable priors →
UNKNOWN, never "clean dev".
"""

from __future__ import annotations

import time

import structlog

logger = structlog.get_logger()

_MCHAIN = {"bsc": "bsc", "ethereum": "eth", "base": "base", "arbitrum": "arbitrum"}
_DEAD_LIQ_USD = 3_000        # a pool below this is effectively dead
_ALIVE_LIQ_USD = 20_000      # a prior token still trading meaningfully


def _creator(token: str, chain: str) -> str | None:
    try:
        from src.onchain.goplus_client import token_security
        sec = token_security(token, chain)
        if sec.get("available"):
            c = sec.get("creator_address")
            return c.lower() if c else None
    except Exception:
        pass
    return None


def _created_contracts(creator: str, chain: str, max_pages: int = 3) -> list[str]:
    """Contracts this wallet deployed (contract-creation txs). Paginated; deduped."""
    from src.onchain import moralis_client
    mch = _MCHAIN.get(chain)
    if not mch or not moralis_client.usable():
        return []
    out: list[str] = []
    cursor = None
    for pg in range(max_pages):
        if pg:
            time.sleep(0.25)
        path = f"{creator}?chain={mch}&limit=100" + (f"&cursor={cursor}" if cursor else "")
        d = moralis_client.get(path)
        if not d:
            break
        for t in (d.get("result") or []):
            ca = t.get("receipt_contract_address") or t.get("contract_address")
            if ca:
                out.append(ca.lower())
        cursor = d.get("cursor") if isinstance(d, dict) else None
        if not cursor:
            break
    # dedup, drop the token itself is handled by the caller
    seen: set[str] = set()
    return [c for c in out if not (c in seen or seen.add(c))]


def _token_liquidity(token: str, chain: str) -> float | None:
    """Deepest-pool liquidity now. None = not indexed (unknown), not zero."""
    import json
    import urllib.request
    try:
        r = urllib.request.urlopen(urllib.request.Request(
            f"https://api.dexscreener.com/tokens/v1/{chain}/{token}",
            headers={"User-Agent": "Mozilla/5.0"}), timeout=15)
        data = json.loads(r.read().decode())
    except Exception:
        return None
    pairs = data if isinstance(data, list) else (data.get("pairs") or [])
    if not pairs:
        return None                       # not indexed → unknown
    return max((float((p.get("liquidity") or {}).get("usd") or 0)) for p in pairs)


def deployer_history(token: str, chain: str, max_priors: int = 12) -> dict:
    """Assess the deployer's prior tokens. Never raises.

    Returns {available, creator, n_prior, dead, alive, unknown, verdict, reason}.
    verdict ∈ serial_rugger | prior_survived | first_timer | mixed | unknown.
    """
    creator = _creator(token, chain)
    if not creator:
        return {"available": False, "verdict": "unknown", "reason": "creator 未知"}
    if chain not in _MCHAIN:
        return {"available": False, "verdict": "unknown", "reason": f"chain {chain} 不支持"}

    created = [c for c in _created_contracts(creator, chain) if c != token.lower()]
    # Not every created contract is a tradeable token; DexScreener indexing filters
    # to the ones that ever had a market. An un-indexed creation is dropped as
    # "not a token we can judge", not counted as a death.
    dead = alive = unknown = 0
    priors_checked = 0
    for c in created[:max_priors]:
        liq = _token_liquidity(c, chain)
        time.sleep(0.3)
        if liq is None:
            unknown += 1
            continue
        priors_checked += 1
        if liq < _DEAD_LIQ_USD:
            dead += 1
        elif liq >= _ALIVE_LIQ_USD:
            alive += 1
        else:
            unknown += 1                  # limbo — don't force it either way

    if priors_checked == 0:
        # No prior token had a readable market. Could be a genuine first launch, or
        # the creator is a factory/proxy. Either way we cannot judge track record.
        verdict = "first_timer" if not created else "unknown"
        reason = ("无可判的历史发币(可能首发,或 creator 是工厂合约)")
        return {"available": True, "creator": creator, "n_prior": len(created),
                "dead": 0, "alive": 0, "unknown": unknown, "verdict": verdict,
                "reason": reason}

    if dead >= 3 and alive == 0:
        verdict = "serial_rugger"
        reason = f"该 dev 过去 {dead} 个币全部无流动性(已死)→ 连续跑路者"
    elif dead >= 2 and dead > alive * 2:
        verdict = "serial_rugger"
        reason = f"该 dev 历史 {dead} 死 / {alive} 活,死亡占压倒多数"
    elif alive >= 2 and alive >= dead:
        verdict = "prior_survived"
        reason = f"该 dev 有 {alive} 个旧币仍在交易(存活≠拉盘,弱正面)"
    else:
        verdict = "mixed"
        reason = f"历史 {alive} 活 / {dead} 死,无明显倾向"
    return {"available": True, "creator": creator, "n_prior": len(created),
            "dead": dead, "alive": alive, "unknown": unknown,
            "verdict": verdict, "reason": reason}
