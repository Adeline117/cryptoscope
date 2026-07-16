"""GMGN smart-money rank via FlareSolverr — high-information but fragile discovery.

GMGN's `orderby=smartmoney` ranks fresh tokens by how many curated smart-money wallets
are in — exactly "他们在买什么" — and the same row carries bot/sniper counts (reverse
tells), honeypot/tax/LP-lock (#5 safety), age, liquidity, mcap. One call per chain.

It sits behind Cloudflare, so the existing deployment routes through a local
FlareSolverr container. Fetch/rank/opportunity results are status-bearing: transport,
challenge, API, and schema failures are never represented as a valid empty market.
This is a FRAGILE, ToS-grey source: treat it as a bonus, not a dependency or truth.
"""

from __future__ import annotations

import html
import json
import math
import os
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Literal, TypedDict

import structlog

logger = structlog.get_logger()

FLARESOLVERR_URL = os.environ.get("FLARESOLVERR_URL", "http://localhost:8191/v1")
_MAX_FLARE_RESPONSE_BYTES = 8 * 1024 * 1024
_JSON_VIEWER_PREFIX = (
    '<html><head><meta name="color-scheme" content="light dark">'
    '<meta charset="utf-8"></head><body><pre>')
_JSON_VIEWER_SUFFIX = (
    '</pre><div class="json-formatter-container"></div></body></html>')
# GMGN chain code → our display chain name
CHAINS = {"sol": "solana", "bsc": "bsc", "base": "base", "eth": "ethereum"}


class GmgnFetchResult(TypedDict):
    state: Literal["ok", "failed"]
    payload: dict | None
    error_kind: str | None
    http_status: int | None
    detail: str | None


class GmgnRankResult(TypedDict):
    state: Literal["ok", "partial", "failed"]
    rows: list[dict]
    error_kind: str | None
    chain: str
    received: int
    accepted: int
    dropped: int
    risk_incomplete: int


class GmgnOpportunitiesResult(TypedDict):
    opportunities: list[dict]
    source_health: dict


def _fetch_result(state: Literal["ok", "failed"], *, payload: dict | None = None,
                  error_kind: str | None = None, http_status: int | None = None,
                  detail: str | None = None) -> GmgnFetchResult:
    return {"state": state, "payload": payload, "error_kind": error_kind,
            "http_status": http_status, "detail": detail}


def _fs_get_result(url: str, timeout: int = 75) -> GmgnFetchResult:
    """Fetch one GMGN JSON document without treating a challenge/error as emptiness."""
    try:
        # Keep the server-side browser deadline inside the HTTP client's deadline.
        # Otherwise a caller that intentionally uses a short timeout disconnects while
        # FlareSolverr keeps the abandoned browser request alive for a full minute.
        max_timeout_ms = max(1_000, min(60_000, int(max(1, timeout - 1) * 1_000)))
        body = json.dumps({"cmd": "request.get", "url": url,
                           "maxTimeout": max_timeout_ms}).encode()
        req = urllib.request.Request(FLARESOLVERR_URL, data=body,
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=timeout) as response:
            raw = response.read(_MAX_FLARE_RESPONSE_BYTES + 1)
        if len(raw) > _MAX_FLARE_RESPONSE_BYTES:
            return _fetch_result("failed", error_kind="response_too_large")
        try:
            envelope = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            return _fetch_result(
                "failed", error_kind="invalid_flaresolverr_json",
                detail=str(exc)[:160])
        if not isinstance(envelope, dict):
            return _fetch_result("failed", error_kind="invalid_flaresolverr_envelope")
        if envelope.get("status") not in (None, "ok"):
            return _fetch_result(
                "failed", error_kind="flaresolverr_error",
                detail=str(envelope.get("message") or "")[:160] or None)
        solution = envelope.get("solution")
        if not isinstance(solution, dict):
            return _fetch_result("failed", error_kind="missing_solution")
        solution_status = solution.get("status")
        if isinstance(solution_status, bool):
            solution_status = None
        try:
            solution_status = int(solution_status)
        except (TypeError, ValueError):
            solution_status = None
        if solution_status != 200:
            kind = (
                "challenge_or_blocked" if solution_status in (401, 403)
                else "rate_limited" if solution_status == 429
                else "upstream_http_error")
            return _fetch_result(
                "failed", error_kind=kind, http_status=solution_status,
                detail=f"GMGN HTTP status {solution_status!r}")
        response_text = solution.get("response")
        if not isinstance(response_text, str):
            return _fetch_result("failed", error_kind="missing_upstream_body")
        json_text = response_text
        if (response_text.startswith(_JSON_VIEWER_PREFIX)
                and response_text.endswith(_JSON_VIEWER_SUFFIX)):
            # Chromium wraps application/json in this fixed, script-free viewer.
            # Accept that exact shell only; never search arbitrary HTML for braces.
            json_text = html.unescape(response_text[
                len(_JSON_VIEWER_PREFIX):-len(_JSON_VIEWER_SUFFIX)])
        try:
            payload = json.loads(json_text)
        except json.JSONDecodeError as exc:
            # Do not regex-extract JSON embedded in a Cloudflare/challenge HTML page.
            return _fetch_result(
                "failed", error_kind="upstream_non_json", detail=str(exc)[:160])
        if not isinstance(payload, dict):
            return _fetch_result("failed", error_kind="invalid_upstream_schema")
        code = payload.get("code")
        if isinstance(code, bool) or code not in (0, "0"):
            return _fetch_result(
                "failed", error_kind="gmgn_api_error",
                detail=str(payload.get("message") or payload.get("reason") or code)[:160])
        return _fetch_result("ok", payload=payload)
    except urllib.error.HTTPError as exc:
        try:
            detail = exc.read(2048).decode(errors="replace")[:160]
        except Exception:
            detail = None
        finally:
            try:
                exc.close()
            except Exception:
                pass
        logger.debug("flaresolverr_http_error", code=exc.code)
        return _fetch_result(
            "failed", error_kind="flaresolverr_http_error",
            http_status=exc.code, detail=detail)
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        logger.debug("flaresolverr_failed", error=str(exc)[:80])
        return _fetch_result(
            "failed", error_kind="flaresolverr_transport_error",
            detail=str(exc)[:160])
    except Exception as exc:
        logger.debug("flaresolverr_failed", error=str(exc)[:80])
        return _fetch_result(
            "failed", error_kind="flaresolverr_transport_error",
            detail=str(exc)[:160])


def _fs_get(url: str, timeout: int = 75) -> dict | None:
    """Compatibility wrapper; use _fs_get_result when empty vs failure matters."""
    result = _fs_get_result(url, timeout)
    return result["payload"] if result["state"] == "ok" else None


def usable() -> bool:
    """True if FlareSolverr answers — cheap liveness probe."""
    try:
        with urllib.request.urlopen(FLARESOLVERR_URL.replace("/v1", "/"), timeout=5) as r:
            return b"FlareSolverr" in r.read()
    except urllib.error.HTTPError as exc:
        try:
            exc.close()
        except Exception:
            pass
        return False
    except Exception:
        return False


def _finite_number(value) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) and number >= 0 else None


def _finite_count(value) -> int | None:
    number = _finite_number(value)
    if number is None or not number.is_integer():
        return None
    return int(number)


def _risk_fields_complete(row: dict, chain: str) -> bool:
    counts_ok = all(
        key in row and _finite_count(row.get(key)) is not None
        for key in ("sniper_count", "bot_degen_count"))
    rates_ok = all(
        key in row and (value := _finite_number(row.get(key))) is not None
        and value <= 1
        for key in (
            "bundler_rate", "entrapment_ratio", "dev_team_hold_rate",
            "top70_sniper_hold_rate"))
    if "sell_tax" not in row:
        tax_ok = False
    elif chain == "sol" and row.get("sell_tax") in (None, ""):
        tax_ok = True  # EVM transfer tax is not applicable to Solana programs.
    else:
        tax_ok = _finite_number(row.get("sell_tax")) is not None
    return counts_ok and rates_ok and tax_ok


def smart_money_rank_result(chain: str, tf: str = "1h",
                            limit: int = 40) -> GmgnRankResult:
    """Normalized rank with explicit API/schema/row completeness."""
    url = (f"https://gmgn.ai/defi/quotation/v1/rank/{chain}/swaps/{tf}"
           f"?orderby=smartmoney&direction=desc&filters[]=not_honeypot")
    fetched = _fs_get_result(url)
    if fetched["state"] != "ok":
        return {"state": "failed", "rows": [],
                "error_kind": fetched["error_kind"], "chain": chain,
                "received": 0, "accepted": 0, "dropped": 0,
                "risk_incomplete": 0}
    data = (fetched["payload"] or {}).get("data")
    if not isinstance(data, dict) or "rank" not in data:
        return {"state": "failed", "rows": [],
                "error_kind": "missing_rank", "chain": chain,
                "received": 0, "accepted": 0, "dropped": 0,
                "risk_incomplete": 0}
    rank = data.get("rank")
    if not isinstance(rank, list):
        return {"state": "failed", "rows": [],
                "error_kind": "invalid_rank_schema", "chain": chain,
                "received": 0, "accepted": 0, "dropped": 0,
                "risk_incomplete": 0}
    if not rank:
        return {"state": "partial", "rows": [],
                "error_kind": "suspicious_empty_rank", "chain": chain,
                "received": 0, "accepted": 0, "dropped": 0,
                "risk_incomplete": 0}
    now = datetime.now(timezone.utc).timestamp()
    out = []
    dropped = 0
    risk_incomplete = 0
    for t in rank[:limit]:
        if not isinstance(t, dict) or not isinstance(t.get("address"), str) \
                or not t["address"].strip():
            dropped += 1
            continue
        raw_smart = t.get("smart_degen_count")
        raw_honeypot = t.get("is_honeypot")
        try:
            smart_count = _finite_count(raw_smart)
            if smart_count is None:
                raise ValueError("invalid smart-money count")
            if raw_honeypot not in (0, 1, "0", "1", False, True):
                raise ValueError("invalid honeypot status")
            honeypot = int(raw_honeypot)
            ots = float(t.get("open_timestamp") or 0)
            if not math.isfinite(ots) or ots < 0:
                raise ValueError("invalid open timestamp")
            age_h = (now - ots) / 3600 if ots else None
            risk_complete = _risk_fields_complete(t, chain)
            if not risk_complete:
                risk_incomplete += 1
            out.append({
                "symbol": t.get("symbol"), "name": t.get("name"),
                "chain": CHAINS.get(chain, chain), "address": t.get("address"),
                "price": t.get("price"), "price_chg_1h": t.get("price_change_percent1h"),
                "pc_1m": t.get("price_change_percent1m"), "pc_5m": t.get("price_change_percent5m"),
                "pc_1h": t.get("price_change_percent1h"),
                "liquidity": t.get("liquidity"), "mcap": t.get("market_cap"),
                "holder_count": t.get("holder_count"), "age_hours": age_h,
                # smart money (the signal)
                "smart_money": smart_count,
                "renowned": _finite_count(t.get("renowned_count")) or 0,
                # reverse tells (caution)
                "snipers": _finite_count(t.get("sniper_count")) or 0,
                "bots": _finite_count(t.get("bot_degen_count")) or 0,
                "bundler_rate": _finite_number(t.get("bundler_rate")) or 0,
                "entrapment_ratio": _finite_number(t.get("entrapment_ratio")) or 0,
                "dev_hold_rate": _finite_number(t.get("dev_team_hold_rate")) or 0,
                "sniper_hold_rate": _finite_number(t.get("top70_sniper_hold_rate")) or 0,
                "risk_fields_complete": risk_complete,
                # safety (#5, native)
                "is_honeypot": honeypot,
                "is_renounced": t.get("is_renounced"),
                "is_open_source": t.get("is_open_source"),
                "buy_tax": t.get("buy_tax"), "sell_tax": t.get("sell_tax"),
                "lock_percent": t.get("lock_percent"),
            })
        except Exception:
            dropped += 1
            continue
    state = "failed" if not out else (
        "partial" if dropped or risk_incomplete else "ok")
    error_kind = (
        "all_rank_rows_malformed" if state == "failed"
        else "malformed_and_incomplete_risk_rows" if dropped and risk_incomplete
        else "malformed_rank_rows" if dropped
        else "incomplete_risk_fields" if risk_incomplete else None)
    return {"state": state, "rows": out,
            "error_kind": error_kind,
            "chain": chain, "received": min(len(rank), limit),
            "accepted": len(out), "dropped": dropped,
            "risk_incomplete": risk_incomplete}


def smart_money_rank(chain: str, tf: str = "1h", limit: int = 40) -> list[dict]:
    """Compatibility rows; use smart_money_rank_result when emptiness matters."""
    result = smart_money_rank_result(chain, tf=tf, limit=limit)
    return result["rows"] if result["state"] != "failed" else []


def exit_liquidity_risk(t: dict) -> dict:
    """The metric the打狗 research says should DOMINATE: how likely is the user to be
    the EXIT LIQUIDITY here? (~3% of pump.fun traders ever clear $1k; 85% of snipers
    dump in 5 min; copiers make 3% vs the 14% they copy.) Scored from the tells that
    predict 'you're the one being dumped on': bundled/entrapment launch, snipers
    already loaded, smart money already crowded (you're late), bot-dominated volume."""
    reasons, score = [], 0
    br = _num(t.get("bundler_rate"))
    er = _num(t.get("entrapment_ratio"))
    snipers = t.get("snipers") if t.get("snipers") is not None else t.get("sniper_count") or 0
    smart = t.get("smart_money") if t.get("smart_money") is not None else t.get("smart_degen_count") or 0
    bots = t.get("bots") if t.get("bots") is not None else t.get("bot_degen_count") or 0
    sh = _num(t.get("sniper_hold_rate") if t.get("sniper_hold_rate") is not None
              else t.get("top70_sniper_hold_rate"))
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
    level = (
        "high" if score >= 3 else "med" if score >= 1
        else "low" if t.get("risk_fields_complete") is True else "unknown")
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
    bots = t.get("bots") if t.get("bots") is not None else t.get("bot_degen_count") or 0
    smart = t.get("smart_money") if t.get("smart_money") is not None else t.get("smart_degen_count") or 0
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
    level = (
        "severe" if severe else "moderate" if reasons
        else "clean" if t.get("risk_fields_complete") is True else "unknown")
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
        if float(t.get("sell_tax") or 0) >= 0.05:      # 打狗研究: >5% = skip
            facts.append(f"卖出税{float(t['sell_tax'])*100:.0f}%")
    except (TypeError, ValueError):
        pass
    # NOTE: is_open_source is ~always 0 on Solana (programs aren't EVM-verified-source)
    # — it floods the safety column with false 'caution'. Dropped; it's not a Tier-A tell.
    if (t.get("dev_hold_rate") or 0) >= 0.10:
        facts.append(f"dev持仓{t['dev_hold_rate']*100:.0f}%")
    if (t.get("sniper_hold_rate") or 0) >= 0.15:
        facts.append(f"狙击者持仓{t['sniper_hold_rate']*100:.0f}%")
    hard = any(w in "".join(facts) for w in ("蜜罐",)) or (t.get("is_honeypot") == 1)
    level = (
        "avoid" if hard else "caution" if facts
        else "clean" if t.get("risk_fields_complete") is True else "unchecked")
    return {"level": level, "facts": facts[:4]}


def opportunities_result(chains=("sol", "bsc", "base", "eth"), min_smart: int = 2,
                         tf: str = "5m", per_chain: int = 40,
                         max_age_hours: float = 48.0) -> GmgnOpportunitiesResult:
    """Cross-chain early smart-money feed with per-chain source completeness.

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
    chains = tuple(chains)
    if not usable():
        return {
            "opportunities": [],
            "source_health": {
                "state": "failed", "error_kind": "flaresolverr_unavailable",
                "requested_chains": len(chains), "successful_chains": 0,
                "failed_chains": len(chains), "rank_rows": 0,
                "opportunities": 0,
                "chains": [
                    {"chain": ch, "state": "failed",
                     "error_kind": "flaresolverr_unavailable",
                     "received": 0, "accepted": 0, "dropped": 0,
                     "risk_incomplete": 0}
                    for ch in chains
                ],
                "checked_at": datetime.now(timezone.utc).isoformat(),
            },
        }
    out = []
    chain_health = []
    for ch in chains:
        ranked = smart_money_rank_result(ch, tf=tf, limit=per_chain)
        chain_health.append({
            "chain": ch, "state": ranked["state"],
            "error_kind": ranked["error_kind"], "received": ranked["received"],
            "accepted": ranked["accepted"], "dropped": ranked["dropped"],
            "risk_incomplete": ranked["risk_incomplete"],
        })
        if ranked["state"] == "failed":
            continue
        for t in ranked["rows"]:
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
    successful = sum(row["state"] != "failed" for row in chain_health)
    failed = len(chain_health) - successful
    incomplete = failed or any(row["state"] == "partial" for row in chain_health)
    state = "failed" if successful == 0 else ("partial" if incomplete else "ok")
    error_kind = (
        "all_chains_failed" if state == "failed"
        else "chain_or_row_gap" if state == "partial" else None)
    return {
        "opportunities": out,
        "source_health": {
            "state": state, "error_kind": error_kind,
            "requested_chains": len(chain_health), "successful_chains": successful,
            "failed_chains": failed,
            "rank_rows": sum(row["accepted"] for row in chain_health),
            "opportunities": len(out), "chains": chain_health,
            "checked_at": datetime.now(timezone.utc).isoformat(),
        },
    }


def opportunities(chains=("sol", "bsc", "base", "eth"), min_smart: int = 2,
                  tf: str = "5m", per_chain: int = 40,
                  max_age_hours: float = 48.0) -> list[dict] | None:
    """Compatibility view; None means no chain produced a trustworthy response."""
    result = opportunities_result(
        chains=chains, min_smart=min_smart, tf=tf, per_chain=per_chain,
        max_age_hours=max_age_hours)
    if result["source_health"]["state"] == "failed":
        return None
    return result["opportunities"]


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
