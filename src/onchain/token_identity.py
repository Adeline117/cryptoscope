"""Token identity profiling — REAL project vs ANONYMOUS operator play.

WHY: The 妖币 screener scores on-chain behaviour (concentration, funding,
distribution history) and is blind to *off-chain identity*. That blindness
makes it flag issuer-controlled bluechips — tokens with a real treasury, a
website, and established socials — as "operators", because a legit team holding
a deployer/treasury wallet looks structurally identical to an anonymous 庄 from
the chain alone.

This module supplies the missing context. Using only keyless DexScreener token
metadata (the same endpoints the codebase already uses), it answers: does this
token look like a *real protocol/project* (has a website, multiple real socials,
an established/non-generic name, some age) or an *anonymous meme / pure operator
play* (no site, no socials, brand-new, generic name)?

The output is interpretable: a small additive score plus the raw evidence
(website?, socials, age, name) so the caller can downgrade an "operator" verdict
when the token is clearly a real project.

Heuristic spirit is borrowed from `token_alpha_scorer._score_social` /
`_score_narrative`: additive points, capped, with human-readable flags.

Defensive: never raises. On any failure returns profile="unknown".
"""

from __future__ import annotations

import json
import re
import urllib.request
from datetime import datetime, timezone
from typing import Any

import structlog

logger = structlog.get_logger()

# DexScreener — keyless, the endpoints already used across the codebase.
_TOKEN_PAIRS_URL = "https://api.dexscreener.com/token-pairs/v1/{chain}/{token}"
_TOKENS_URL = "https://api.dexscreener.com/latest/dex/tokens/{token}"

# Socials we count as "real project" signal (excludes the dex link itself).
_REAL_SOCIALS = {"twitter", "x", "telegram", "discord", "medium", "github", "reddit"}

# Generic / throwaway meme-name tokens that carry no project identity.
_GENERIC_NAME_RE = re.compile(
    r"^(test|token|coin|meme|pepe|doge|shib|inu|elon|moon|safe|baby|mini|"
    r"floki|wojak|cat|dog|frog|chad|based|wif|bonk)\b",
    re.IGNORECASE,
)


# --------------------------------------------------------------------------
# I/O — best-effort, never raises
# --------------------------------------------------------------------------

def _fetch(url: str, timeout: int = 10) -> Any:
    """Fetch JSON from a keyless endpoint. Returns None on any failure."""
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "CryptoScope/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    except Exception as e:  # noqa: BLE001 — best-effort, callers must not crash
        logger.debug("token_identity_fetch_failed", url=url, error=str(e))
        return None


def _pairs_for(token: str, chain: str) -> list[dict]:
    """Return DexScreener pairs for a token, trying both known endpoints."""
    data = _fetch(_TOKEN_PAIRS_URL.format(chain=chain, token=token))
    if isinstance(data, list) and data:
        return data
    if isinstance(data, dict) and data.get("pairs"):
        return data["pairs"]

    # Fallback: the multichain /latest/dex/tokens endpoint, filtered to chain.
    data = _fetch(_TOKENS_URL.format(token=token))
    if isinstance(data, dict):
        pairs = data.get("pairs") or []
        same_chain = [p for p in pairs if (p.get("chainId") or "").lower() == chain.lower()]
        return same_chain or pairs
    return []


# --------------------------------------------------------------------------
# Pure logic — extraction & profiling (no I/O, easy to reason about)
# --------------------------------------------------------------------------

def _best_pair(pairs: list[dict]) -> dict:
    """Highest-liquidity pair carries the most complete metadata."""
    if not pairs:
        return {}
    return max(pairs, key=lambda p: (p.get("liquidity", {}) or {}).get("usd", 0) or 0)


def _extract_socials(pair: dict) -> list[dict]:
    """Normalise pair.info.socials -> [{type, url}], deduped by type."""
    info = pair.get("info") or {}
    out: list[dict] = []
    seen: set[str] = set()
    for s in info.get("socials") or []:
        if not isinstance(s, dict):
            continue
        stype = (s.get("type") or "").lower().strip()
        url = s.get("url") or ""
        if stype and stype not in seen:
            seen.add(stype)
            out.append({"type": stype, "url": url})
    return out


def _extract_websites(pair: dict) -> list[str]:
    """Normalise pair.info.websites -> [url]."""
    info = pair.get("info") or {}
    out: list[str] = []
    for w in info.get("websites") or []:
        if isinstance(w, dict) and w.get("url"):
            out.append(w["url"])
        elif isinstance(w, str) and w:
            out.append(w)
    return out


def _age_days(pair: dict) -> float | None:
    """Token age in days from pairCreatedAt (ms epoch)."""
    created = pair.get("pairCreatedAt")
    if not created:
        return None
    try:
        created_dt = datetime.fromtimestamp(created / 1000, tz=timezone.utc)
        delta = datetime.now(tz=timezone.utc) - created_dt
        return round(delta.total_seconds() / 86400, 2)
    except Exception:  # noqa: BLE001
        return None


def _is_generic_name(name: str, symbol: str) -> bool:
    """A name/symbol that signals a throwaway meme rather than a project."""
    name = (name or "").strip()
    if not name:
        return True
    if _GENERIC_NAME_RE.match(name) or _GENERIC_NAME_RE.match(symbol or ""):
        return True
    return False


# A token older than this is, by definition, not a fresh anonymous meme play —
# even when DexScreener carries no off-chain metadata for it. Such a token is
# at worst "unknown", never "anon_meme". This is the guard that stops the
# screener mislabelling established issuer-controlled tokens as operators.
_ESTABLISHED_AGE_DAYS = 14.0


def _profile(
    has_website: bool,
    socials: list[dict],
    age_days: float | None,
    name: str,
    symbol: str,
) -> tuple[str, int, str]:
    """Additive identity score -> (profile, score, detail).

    Score (0-100), interpretable:
      +35 website present
      +15 per real social (twitter/telegram/discord/...), capped at 45
      +10 non-generic name  (+5 more if multi-word, e.g. "Yooldo Games")
      +20 age >= 180d  /  +12 age >= 30d  /  +5 age >= 7d  /  -10 age < 2d

    Classification:
      score >= 55                          -> real_project
      score <= 20 AND token is brand-new   -> anon_meme
      otherwise                            -> unknown

    The age gate matters: DexScreener often lacks website/socials for a real
    project's secondary-chain token (e.g. Yooldo Games on BSC). Without the
    gate such tokens score low and would be wrongly tagged anon_meme; the gate
    keeps them "unknown" instead of a false operator/meme verdict.
    """
    flags: list[str] = []
    score = 0

    if has_website:
        score += 35
        flags.append("有官网")
    else:
        flags.append("无官网")

    real_socials = [s for s in socials if s.get("type") in _REAL_SOCIALS]
    n_social = len(real_socials)
    score += min(n_social * 15, 45)
    if n_social:
        flags.append(f"{n_social}个社交媒体({', '.join(s['type'] for s in real_socials)})")
    else:
        flags.append("无社交媒体")

    generic = _is_generic_name(name, symbol)
    if not generic:
        score += 10
        # A multi-word proper name is a stronger project signal than a ticker.
        if len((name or "").split()) >= 2:
            score += 5
        flags.append(f"专有名称({name})")
    else:
        flags.append("通用/meme名称")

    is_new = age_days is None or age_days < _ESTABLISHED_AGE_DAYS
    if age_days is not None:
        if age_days >= 180:
            score += 20
            flags.append(f"长期运行 {age_days:.0f}天")
        elif age_days >= 30:
            score += 12
            flags.append(f"已建立 {age_days:.0f}天")
        elif age_days >= 7:
            score += 5
            flags.append(f"运行 {age_days:.0f}天")
        elif age_days < 2:
            score -= 10
            flags.append(f"极新 {age_days:.1f}天")
    else:
        flags.append("年龄未知")

    score = max(0, min(score, 100))

    if score >= 55:
        profile = "real_project"
    elif score <= 20 and is_new:
        profile = "anon_meme"
    else:
        profile = "unknown"

    return profile, score, "; ".join(flags)


# --------------------------------------------------------------------------
# Public API
# --------------------------------------------------------------------------

def token_identity(token: str, chain: str) -> dict[str, Any]:
    """Profile a token's off-chain identity to separate real projects from memes.

    Args:
        token: Token contract / mint address.
        chain: DexScreener chain id ("bsc", "ethereum"/"eth", "solana", "base"...).

    Returns:
        {
          "has_website": bool,
          "socials": [{"type": str, "url": str}, ...],
          "age_days": float | None,
          "name": str,
          "profile": "real_project" | "anon_meme" | "unknown",
          "score": int,        # 0-100, higher = more like a real project
          "detail": str,       # human-readable evidence
        }

    Never raises; on failure returns a "unknown" profile with empty evidence.
    """
    result: dict[str, Any] = {
        "has_website": False,
        "socials": [],
        "age_days": None,
        "name": "",
        "profile": "unknown",
        "score": 0,
        "detail": "no data",
    }

    try:
        pairs = _pairs_for(token, chain)
        if not pairs:
            logger.debug("token_identity_no_pairs", token=token, chain=chain)
            return result

        pair = _best_pair(pairs)
        base = pair.get("baseToken", {}) or {}
        name = base.get("name", "") or ""
        symbol = base.get("symbol", "") or ""

        websites = _extract_websites(pair)
        socials = _extract_socials(pair)
        age = _age_days(pair)
        has_website = bool(websites)

        profile, score, detail = _profile(has_website, socials, age, name, symbol)

        result.update(
            has_website=has_website,
            socials=socials,
            age_days=age,
            name=name,
            profile=profile,
            score=score,
            detail=detail,
        )
        logger.info(
            "token_identity",
            token=token,
            chain=chain,
            name=name,
            profile=profile,
            score=score,
        )
        return result
    except Exception as e:  # noqa: BLE001 — defensive top-level guard
        logger.warning("token_identity_failed", token=token, chain=chain, error=str(e))
        result["detail"] = f"error: {e}"
        return result
