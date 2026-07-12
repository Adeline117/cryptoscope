"""GMGN smart-money rank via FlareSolverr — the strong #1 source, ALL chains incl Solana.

GMGN's `orderby=smartmoney` ranks fresh tokens by how many curated smart-money wallets
are in — exactly "他们在买什么" — and the same row carries bot/sniper counts (reverse
tells), honeypot/tax/LP-lock (#5 safety), age, liquidity, mcap. One call per chain.

It sits behind Cloudflare, so we route through a local FlareSolverr container
(docker run -p 8191:8191 ghcr.io/flaresolverr/flaresolverr). If FlareSolverr is down
or Cloudflare serves a managed challenge, every function returns empty/None and the
caller falls back — never a fake result. This is a FRAGILE, ToS-grey source: treat it
as a strong bonus, not a guaranteed dependency.
"""

from __future__ import annotations

import json
import os
import re
import urllib.request
from datetime import datetime, timezone

import structlog

logger = structlog.get_logger()

FLARESOLVERR_URL = os.environ.get("FLARESOLVERR_URL", "http://localhost:8191/v1")
# GMGN chain code → our display chain name
CHAINS = {"sol": "solana", "bsc": "bsc", "base": "base", "eth": "ethereum"}


def _fs_get(url: str, timeout: int = 75) -> dict | None:
    """Fetch `url` through FlareSolverr, return the parsed JSON body, or None on any
    failure (FlareSolverr down, Cloudflare challenge unsolved, non-JSON)."""
    try:
        body = json.dumps({"cmd": "request.get", "url": url, "maxTimeout": 60000}).encode()
        req = urllib.request.Request(FLARESOLVERR_URL, data=body,
                                     headers={"Content-Type": "application/json"})
        r = json.loads(urllib.request.urlopen(req, timeout=timeout).read())
        sol = r.get("solution") or {}
        if sol.get("status") != 200:
            return None
        m = re.search(r"\{.*\}", sol.get("response", ""), re.S)
        return json.loads(m.group(0)) if m else None
    except Exception as e:
        logger.debug("flaresolverr_failed", error=str(e)[:80])
        return None


def usable() -> bool:
    """True if FlareSolverr answers — cheap liveness probe."""
    try:
        with urllib.request.urlopen(FLARESOLVERR_URL.replace("/v1", "/"), timeout=5) as r:
            return b"FlareSolverr" in r.read()
    except Exception:
        return False


def smart_money_rank(chain: str, tf: str = "1h", limit: int = 40) -> list[dict]:
    """Normalized GMGN smart-money rank for one chain code (sol/bsc/base/eth). []
    on failure. Each row = a fresh token with smart-money + reverse-tells + safety."""
    url = (f"https://gmgn.ai/defi/quotation/v1/rank/{chain}/swaps/{tf}"
           f"?orderby=smartmoney&direction=desc&filters[]=not_honeypot")
    d = _fs_get(url)
    rank = ((d or {}).get("data") or {}).get("rank") or []
    now = datetime.now(timezone.utc).timestamp()
    out = []
    for t in rank[:limit]:
        try:
            ots = t.get("open_timestamp") or 0
            age_h = (now - ots) / 3600 if ots else None
            out.append({
                "symbol": t.get("symbol"), "name": t.get("name"),
                "chain": CHAINS.get(chain, chain), "address": t.get("address"),
                "price": t.get("price"), "price_chg_1h": t.get("price_change_percent1h"),
                "liquidity": t.get("liquidity"), "mcap": t.get("market_cap"),
                "holder_count": t.get("holder_count"), "age_hours": age_h,
                # smart money (the signal)
                "smart_money": t.get("smart_degen_count") or 0,
                "renowned": t.get("renowned_count") or 0,
                # reverse tells (caution)
                "snipers": t.get("sniper_count") or 0,
                "bots": t.get("bot_degen_count") or 0,
                "bundler_rate": t.get("bundler_rate") or 0,
                "dev_hold_rate": t.get("dev_team_hold_rate") or 0,
                "sniper_hold_rate": t.get("top70_sniper_hold_rate") or 0,
                # safety (#5, native)
                "is_honeypot": t.get("is_honeypot"),
                "is_renounced": t.get("is_renounced"),
                "is_open_source": t.get("is_open_source"),
                "buy_tax": t.get("buy_tax"), "sell_tax": t.get("sell_tax"),
                "lock_percent": t.get("lock_percent"),
            })
        except Exception:
            continue
    return out


def exit_liquidity_risk(t: dict) -> dict:
    """The metric the打狗 research says should DOMINATE: how likely is the user to be
    the EXIT LIQUIDITY here? (~3% of pump.fun traders ever clear $1k; 85% of snipers
    dump in 5 min; copiers make 3% vs the 14% they copy.) Scored from the tells that
    predict 'you're the one being dumped on': bundled/entrapment launch, snipers
    already loaded, smart money already crowded (you're late), bot-dominated volume."""
    reasons, score = [], 0
    br = _num(t.get("bundler_rate"))
    er = _num(t.get("entrapment_ratio"))
    snipers = t.get("sniper_count") or 0
    smart = t.get("smart_degen_count") or 0
    bots = t.get("bot_degen_count") or 0
    sh = _num(t.get("top70_sniper_hold_rate"))
    if br >= 0.30:
        score += 2; reasons.append("捆绑发射(创建者同捆买)")
    if er >= 0.20:
        score += 2; reasons.append("诱捕盘(在钓人接盘)")
    if sh >= 0.15:
        score += 2; reasons.append(f"狙击者持仓{sh*100:.0f}%(随时砸你)")
    if snipers >= 40:
        score += 1; reasons.append(f"{snipers}个狙击已埋伏(你晚了)")
    if smart >= 40:
        score += 1; reasons.append("聪明钱已过多=扩散过半(你是后排)")
    if smart and bots / max(smart, 1) >= 25:
        score += 1; reasons.append("机器人主导成交(刷量)")
    level = "high" if score >= 3 else ("med" if score >= 1 else "low")
    return {"level": level, "score": score, "reasons": reasons[:3]}


def _manipulation(t: dict) -> dict:
    """Structure #4, the honest defensive form: is this token's activity MANIPULATED /
    bot-driven, so its smart-money/price signal is polluted? From GMGN's native fields
    — bundler_rate (creator-controlled launch-bundle buys = frontrun/sandwich kin),
    entrapment_ratio (luring buyers to dump on), and bot-vs-smart dominance. Not per-tx
    sandwich detection (those free APIs are dead) — a per-token taint flag, which is the
    'MEV pollutes my signal' defense the goal actually needs."""
    reasons = []
    br = _num(t.get("bundler_rate"))
    er = _num(t.get("entrapment_ratio"))
    bots = t.get("bot_degen_count") or 0
    smart = t.get("smart_degen_count") or 0
    if _num(t.get("sell_tax")) >= 0.05:      # 打狗研究: >5% sell tax = skip (was 10%)
        reasons.append(f"卖出税{_num(t.get('sell_tax'))*100:.0f}%")
    if br >= 0.30:
        reasons.append(f"捆绑抢跑率{br*100:.0f}%(创建者钱包同捆买入)")
    if er >= 0.20:
        reasons.append(f"诱捕率{er*100:.0f}%(在钓人接盘)")
    if smart == 0 and bots >= 50:
        reasons.append(f"纯机器人刷量({bots}bot/0聪明钱)")
    elif smart > 0 and bots / max(smart, 1) >= 25:
        reasons.append(f"机器人是聪明钱的{bots//max(smart,1)}倍(刷量为主)")
    severe = br >= 0.45 or er >= 0.35 or (smart == 0 and bots >= 200)
    level = "severe" if severe else ("moderate" if reasons else "clean")
    return {"level": level, "reasons": reasons[:3],
            "bundler_rate": round(br, 3), "entrapment_ratio": round(er, 3)}


def _num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def _rug_from_gmgn(t: dict) -> dict:
    """Turn GMGN's native safety fields into the board's avoid/caution/clean badge."""
    facts = []
    if t.get("is_honeypot") == 1:
        facts.append("蜜罐")
    try:
        if float(t.get("sell_tax") or 0) >= 0.10:
            facts.append(f"卖出税{float(t['sell_tax'])*100:.0f}%")
    except (TypeError, ValueError):
        pass
    if t.get("is_open_source") == 0:
        facts.append("未开源")
    if (t.get("dev_hold_rate") or 0) >= 0.10:
        facts.append(f"dev持仓{t['dev_hold_rate']*100:.0f}%")
    if (t.get("sniper_hold_rate") or 0) >= 0.15:
        facts.append(f"狙击者持仓{t['sniper_hold_rate']*100:.0f}%")
    hard = any(w in "".join(facts) for w in ("蜜罐",)) or (t.get("is_honeypot") == 1)
    level = "avoid" if hard else ("caution" if facts else "clean")
    return {"level": level, "facts": facts[:4]}


def opportunities(chains=("sol", "bsc", "base", "eth"), min_smart: int = 2,
                  tf: str = "5m", per_chain: int = 40,
                  max_age_hours: float = 48.0) -> list[dict] | None:
    """Cross-chain 'EARLY smart-money entries' feed. Returns None if FlareSolverr is
    unusable (caller falls back).

    EARLINESS is the whole point (a token with 167 smart wallets over 25 days is EXIT
    liquidity, not an entry). So:
      · tf='5m' — smart money active in the LAST 5 MINUTES, not a 1h/24h aggregate.
      · age filter — drop anything older than max_age_hours; the money in structure #1
        is on the diffusion curve's early slope, not the plateau.
      · rank by FRESHNESS-WEIGHTED conviction = smart_money / age (a young token with a
        few smart wallets JUST entering beats an old one with a crowd).
    Honest ceiling: you are still behind the deployer-funded snipers who buy in the
    creation block; the earliest a dashboard realistically gets you is minutes-fresh
    diffusion, not the insider entry."""
    if not usable():
        return None
    out = []
    for ch in chains:
        for t in smart_money_rank(ch, tf=tf, limit=per_chain):
            if t.get("is_honeypot") == 1:
                continue
            if (t.get("smart_money") or 0) < min_smart:
                continue
            age = t.get("age_hours")
            if age is not None and age > max_age_hours:
                continue                       # already past the early slope → skip
            t["rug"] = _rug_from_gmgn(t)
            t["manipulation"] = _manipulation(t)     # #4 taint flag
            t["exit_risk"] = exit_liquidity_risk(t)  # 打狗研究: the dominant metric
            # A huge smart-money count is a LATENESS tell, not conviction — 200 smart
            # wallets don't pile in during the early slope. So the fresh score REWARDS
            # young age and CAPS the smart-money contribution (a handful just-entering
            # on a fresh token beats a crowd on a discovered one). Unknown age is
            # treated as old (can't confirm it's early), so it can't fake freshness.
            a = max(age if age is not None else max_age_hours, 0.25)
            sm_capped = min(t["smart_money"] or 0, 20)     # >20 = already the crowd
            t["fresh_score"] = round(sm_capped / a, 2)
            t["confirmed_fresh"] = age is not None and age <= 12
            t["crowded"] = (t["smart_money"] or 0) > 40     # likely already run
            t["strength"] = "强" if (t["confirmed_fresh"] and t["smart_money"] >= 3) else "弱"
            out.append(t)
    # rank: confirmed-fresh first, then LOW exit-liquidity-risk (the research's
    # dominant metric — a fresh token you'd be dumped on is not an opportunity), then
    # freshness. High-exit-risk fresh tokens sink below clean ones.
    _er = {"low": 0, "med": 1, "high": 2}
    out.sort(key=lambda x: (not x["confirmed_fresh"], _er.get(x["exit_risk"]["level"], 1), -x["fresh_score"]))
    return out


if __name__ == "__main__":
    from dotenv import load_dotenv

    from src.config import PROJECT_ROOT
    load_dotenv(PROJECT_ROOT / ".env")
    print("flaresolverr usable:", usable())
    ops = opportunities()
    print(f"{len(ops or [])} smart-money opportunities across chains")
    for o in (ops or [])[:12]:
        print(f"  {o['symbol']:12} [{o['chain']:8}] 聪明钱{o['smart_money']:3} 知名{o['renowned']:3} "
              f"狙击{o['snipers']:3} bot{o['bots']:4} liq${(o['liquidity'] or 0)/1e3:.0f}k "
              f"{o['rug']['level']} {o.get('age_hours') and round(o['age_hours'],1)}h")
