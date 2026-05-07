"""Early-stage token alpha scorer (0-100 score with veto logic)."""

from __future__ import annotations

from typing import Any


# ---------------------------------------------------------------------------
# Grade thresholds
# ---------------------------------------------------------------------------

_GRADE_MAP = [
    (90, "S"),
    (75, "A"),
    (60, "B"),
    (40, "C"),
    (0, "D"),
]


def _grade(score: int) -> str:
    for threshold, g in _GRADE_MAP:
        if score >= threshold:
            return g
    return "D"


# ---------------------------------------------------------------------------
# Sub-scorers (each returns 0 - max_points)
# ---------------------------------------------------------------------------

def _score_liquidity(data: dict[str, Any], max_pts: int = 20) -> tuple[int, list[str]]:
    """Liquidity health: $50K+ pool = full marks."""
    flags: list[str] = []
    liquidity_usd = data.get("liquidity_usd", 0)
    if liquidity_usd >= 50_000:
        return max_pts, flags
    if liquidity_usd >= 20_000:
        return 12, flags
    if liquidity_usd >= 5_000:
        flags.append(f"流动性偏低: ${liquidity_usd:,.0f}")
        return 6, flags
    flags.append(f"流动性极低: ${liquidity_usd:,.0f}")
    return 0, flags


def _score_holder_concentration(data: dict[str, Any], max_pts: int = 20) -> tuple[int, list[str]]:
    """Holder concentration: Top10 < 40% = full marks."""
    flags: list[str] = []
    top10_pct = data.get("top10_holder_pct", 100)
    if top10_pct < 40:
        return max_pts, flags
    if top10_pct < 60:
        flags.append(f"Top10持仓集中: {top10_pct:.1f}%")
        return 10, flags
    flags.append(f"Top10持仓过于集中: {top10_pct:.1f}%")
    return 0, flags


def _score_security(data: dict[str, Any], max_pts: int = 20) -> tuple[int, list[str]]:
    """Security check (GoPlus-style flags)."""
    flags: list[str] = []
    security = data.get("security", {})

    is_honeypot = security.get("is_honeypot", False)
    can_mint = security.get("can_mint", False)
    dev_pct = security.get("dev_holding_pct", 0)
    has_proxy = security.get("has_proxy", False)
    has_blacklist = security.get("has_blacklist", False)

    # Veto conditions tracked for the caller
    if is_honeypot:
        flags.append("HONEYPOT 蜜罐合约")
    if can_mint:
        flags.append("CAN_MINT 可增发")
    if dev_pct > 20:
        flags.append(f"DEV持仓 {dev_pct:.1f}% > 20%")

    # Scoring (veto is applied by the main function, not here)
    score = max_pts
    if has_proxy:
        score -= 5
        flags.append("代理合约(可升级)")
    if has_blacklist:
        score -= 5
        flags.append("存在黑名单函数")
    if dev_pct > 10:
        score -= 5
    if dev_pct > 5:
        score -= 3

    return max(score, 0), flags


def _score_smart_money(data: dict[str, Any], max_pts: int = 20) -> tuple[int, list[str]]:
    """Smart money participation: 3+ T1 wallets = full marks."""
    flags: list[str] = []
    t1_wallets = data.get("smart_money_t1_count", 0)
    t2_wallets = data.get("smart_money_t2_count", 0)

    if t1_wallets >= 3:
        return max_pts, flags
    if t1_wallets >= 1:
        score = 8 + t1_wallets * 4 + min(t2_wallets, 3) * 2
        return min(score, max_pts), flags

    if t2_wallets >= 3:
        return 8, flags
    if t2_wallets >= 1:
        return 4, flags

    flags.append("无聪明钱参与")
    return 0, flags


def _score_social(data: dict[str, Any], max_pts: int = 10) -> tuple[int, list[str]]:
    """Social buzz score."""
    flags: list[str] = []
    mentions = data.get("social_mentions_24h", 0)
    sentiment = data.get("social_sentiment", 0.5)  # 0-1

    score = 0
    if mentions >= 100:
        score += 5
    elif mentions >= 30:
        score += 3
    elif mentions >= 10:
        score += 1

    if sentiment >= 0.7:
        score += 5
    elif sentiment >= 0.5:
        score += 3
    elif sentiment >= 0.3:
        score += 1

    return min(score, max_pts), flags


def _score_narrative(data: dict[str, Any], max_pts: int = 10) -> tuple[int, list[str]]:
    """Narrative fit score."""
    flags: list[str] = []
    hot_narratives = data.get("hot_narratives", [])  # e.g. ["AI", "RWA"]
    token_narratives = data.get("token_narratives", [])

    if not hot_narratives or not token_narratives:
        return 0, flags

    overlap = set(n.lower() for n in hot_narratives) & set(n.lower() for n in token_narratives)
    if len(overlap) >= 2:
        return max_pts, flags
    if len(overlap) == 1:
        return 6, flags
    return 0, flags


# ---------------------------------------------------------------------------
# Main scorer
# ---------------------------------------------------------------------------

_VETO_KEYS = {"is_honeypot", "can_mint"}


def score_token(data: dict[str, Any]) -> dict[str, Any]:
    """Score an early-stage token and return a structured result.

    Args:
        data: Dict with token data.  Expected keys:
            - liquidity_usd (float)
            - top10_holder_pct (float)
            - security (dict with is_honeypot, can_mint, dev_holding_pct, etc.)
            - smart_money_t1_count (int)
            - smart_money_t2_count (int)
            - social_mentions_24h (int)
            - social_sentiment (float 0-1)
            - hot_narratives (list[str])
            - token_narratives (list[str])

    Returns:
        Dict with ``alpha_score`` (0-100), ``grade`` (S/A/B/C/D),
        ``red_flags`` (list[str]), ``recommendation`` (BUY/WATCH/SKIP),
        and ``breakdown`` (per-category scores).
    """
    security = data.get("security", {})

    # Veto check: immediate score = 0
    is_vetoed = (
        security.get("is_honeypot", False)
        or security.get("can_mint", False)
        or security.get("dev_holding_pct", 0) > 20
    )

    # Run all sub-scorers to collect flags even on veto
    liq_score, liq_flags = _score_liquidity(data)
    holder_score, holder_flags = _score_holder_concentration(data)
    sec_score, sec_flags = _score_security(data)
    sm_score, sm_flags = _score_smart_money(data)
    social_score, social_flags = _score_social(data)
    narrative_score, narrative_flags = _score_narrative(data)

    all_flags = liq_flags + holder_flags + sec_flags + sm_flags + social_flags + narrative_flags

    if is_vetoed:
        return {
            "alpha_score": 0,
            "grade": "D",
            "red_flags": all_flags,
            "recommendation": "SKIP",
            "veto_reason": "一票否决: " + "; ".join(
                f for f in all_flags if any(
                    k in f for k in ("HONEYPOT", "CAN_MINT", "DEV持仓")
                )
            ),
            "breakdown": {
                "liquidity": liq_score,
                "holder_concentration": holder_score,
                "security": sec_score,
                "smart_money": sm_score,
                "social": social_score,
                "narrative": narrative_score,
            },
        }

    total_score = liq_score + holder_score + sec_score + sm_score + social_score + narrative_score
    grade = _grade(total_score)

    if total_score >= 75:
        recommendation = "BUY"
    elif total_score >= 50:
        recommendation = "WATCH"
    else:
        recommendation = "SKIP"

    return {
        "alpha_score": total_score,
        "grade": grade,
        "red_flags": all_flags,
        "recommendation": recommendation,
        "breakdown": {
            "liquidity": liq_score,
            "holder_concentration": holder_score,
            "security": sec_score,
            "smart_money": sm_score,
            "social": social_score,
            "narrative": narrative_score,
        },
    }
