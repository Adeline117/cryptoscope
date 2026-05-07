"""Rug Detector — multi-layer security check before buying any token.

Checks:
1. GoPlus: honeypot, mintable, freezable, hidden owner, proxy
2. RugCheck: risk score, LP lock, insider detection
3. DexScreener: liquidity depth, pair age, volume/liquidity ratio
4. Dev wallet: check if deployer has previous rug history (placeholder)

Returns a RiskReport with pass/fail and detailed breakdown.
"""

from __future__ import annotations

import json
import urllib.request
from dataclasses import dataclass, field
from typing import Any

import structlog

logger = structlog.get_logger()


@dataclass
class RiskReport:
    """Security assessment result."""
    token_mint: str
    chain: str
    passed: bool  # Overall: safe to trade?
    risk_score: int  # 0 (safest) to 100 (riskiest)
    checks: list[dict] = field(default_factory=list)  # [{name, passed, detail}]
    flags: list[str] = field(default_factory=list)  # Red flags
    liquidity_usd: float = 0
    holder_count: int = 0
    lp_locked_pct: float = 0


def _fetch(url: str, timeout: int = 8) -> Any:
    req = urllib.request.Request(url, headers={"User-Agent": "CryptoScope/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


def check_goplus(mint: str, chain: str = "solana") -> list[dict]:
    """GoPlus security checks."""
    checks = []
    try:
        if chain == "solana":
            url = f"https://api.gopluslabs.io/api/v1/solana/token_security?contract_addresses={mint}"
        else:
            chain_map = {"ethereum": 1, "bsc": 56, "base": 8453}
            cid = chain_map.get(chain, chain)
            url = f"https://api.gopluslabs.io/api/v1/token_security/{cid}?contract_addresses={mint.lower()}"

        data = _fetch(url)
        result = data.get("result", {})
        token = result.get(mint.lower(), result.get(mint, {}))

        if not token:
            checks.append({"name": "GoPlus", "passed": None, "detail": "未收录"})
            return checks

        # Critical checks
        is_honeypot = token.get("is_honeypot") in ("1", 1, True)
        checks.append({"name": "蜜罐检测", "passed": not is_honeypot,
                       "detail": "❌ 蜜罐!" if is_honeypot else "✅ 安全"})

        is_mintable = token.get("is_mintable") in ("1", 1, True)
        checks.append({"name": "增发权限", "passed": not is_mintable,
                       "detail": "❌ 可增发" if is_mintable else "✅ 不可增发"})

        is_proxy = token.get("is_proxy") in ("1", 1, True)
        checks.append({"name": "代理合约", "passed": not is_proxy,
                       "detail": "⚠️ 代理合约(可升级)" if is_proxy else "✅ 非代理"})

        hidden_owner = token.get("hidden_owner") in ("1", 1, True)
        checks.append({"name": "隐藏Owner", "passed": not hidden_owner,
                       "detail": "❌ 隐藏Owner" if hidden_owner else "✅ 无隐藏"})

        owner_change = token.get("owner_change_balance") in ("1", 1, True)
        checks.append({"name": "篡改余额", "passed": not owner_change,
                       "detail": "❌ Owner可改余额" if owner_change else "✅ 安全"})

        # Tax checks
        buy_tax = float(token.get("buy_tax", 0) or 0)
        sell_tax = float(token.get("sell_tax", 0) or 0)
        if buy_tax > 1:
            buy_tax = buy_tax / 100
        if sell_tax > 1:
            sell_tax = sell_tax / 100

        tax_ok = buy_tax < 0.10 and sell_tax < 0.10
        checks.append({"name": "交易税",
                       "passed": tax_ok,
                       "detail": f"买{buy_tax*100:.0f}%/卖{sell_tax*100:.0f}%" + (" ⚠️高税" if not tax_ok else " ✅")})

    except Exception as e:
        checks.append({"name": "GoPlus", "passed": None, "detail": f"检测失败: {e}"})

    return checks


def check_rugcheck(mint: str) -> tuple[list[dict], float]:
    """RugCheck security score (Solana only)."""
    checks = []
    lp_locked = 0
    try:
        data = _fetch(f"https://api.rugcheck.xyz/v1/tokens/{mint}/report/summary")

        rugged = data.get("rugged", False)
        checks.append({"name": "RugCheck已rug", "passed": not rugged,
                       "detail": "❌ 已确认RUG!" if rugged else "✅ 未rug"})

        score = data.get("score_normalised", data.get("score", 0))
        score_ok = score < 300  # Lower is safer
        checks.append({"name": "RugCheck评分",
                       "passed": score_ok,
                       "detail": f"评分 {score}" + (" ✅" if score_ok else " ⚠️高风险")})

        lp_locked = data.get("lpLockedPct", 0) or 0
        lp_ok = lp_locked >= 80
        checks.append({"name": "LP锁定",
                       "passed": lp_ok,
                       "detail": f"{lp_locked:.0f}%" + (" ✅" if lp_ok else " ⚠️未锁定")})

        # Risks
        risks = data.get("risks", [])
        for r in risks[:3]:
            level = r.get("level", "info")
            if level in ("danger", "critical"):
                checks.append({"name": r.get("name", "风险"),
                               "passed": False,
                               "detail": f"❌ {r.get('description', '')[:50]}"})

    except Exception as e:
        checks.append({"name": "RugCheck", "passed": None, "detail": f"检测失败: {e}"})

    return checks, lp_locked


def check_liquidity(mint: str) -> tuple[list[dict], float, int]:
    """Check liquidity and basic market data from DexScreener."""
    checks = []
    liq = 0
    holders = 0
    try:
        data = _fetch(f"https://api.dexscreener.com/token-pairs/v1/solana/{mint}")
        pairs = data if isinstance(data, list) else data.get("pairs", [])
        if not pairs:
            checks.append({"name": "流动性", "passed": False, "detail": "❌ 无交易对"})
            return checks, 0, 0

        best = max(pairs, key=lambda p: (p.get("liquidity", {}) or {}).get("usd", 0) or 0)
        liq = (best.get("liquidity", {}) or {}).get("usd", 0) or 0
        vol = (best.get("volume", {}) or {}).get("h24", 0) or 0
        created = best.get("pairCreatedAt", 0) or 0

        # Liquidity check
        liq_ok = liq >= 10000
        checks.append({"name": "流动性",
                       "passed": liq_ok,
                       "detail": f"${liq:,.0f}" + (" ✅" if liq_ok else " ❌ <$10K")})

        # Volume/Liquidity ratio (wash trading check)
        if liq > 0:
            vl_ratio = vol / liq
            vl_ok = vl_ratio < 50  # Not wash trading
            checks.append({"name": "量/池比",
                           "passed": vl_ok,
                           "detail": f"{vl_ratio:.1f}x" + (" ✅" if vl_ok else " ⚠️可能刷量")})

        # Age check
        if created:
            import time
            age_hours = (time.time() * 1000 - created) / (1000 * 3600)
            age_ok = age_hours > 0.5  # At least 30 min old
            checks.append({"name": "代币年龄",
                           "passed": age_ok,
                           "detail": f"{age_hours:.1f}h" + (" ✅" if age_ok else " ⚠️太新")})

    except Exception as e:
        checks.append({"name": "DexScreener", "passed": None, "detail": f"检测失败: {e}"})

    return checks, liq, holders


def full_security_check(mint: str, chain: str = "solana") -> RiskReport:
    """Run ALL security checks and return a RiskReport."""
    all_checks = []
    flags = []

    # 1. GoPlus
    gp_checks = check_goplus(mint, chain)
    all_checks.extend(gp_checks)

    # 2. RugCheck (Solana only)
    lp_locked = 0
    if chain == "solana":
        rc_checks, lp_locked = check_rugcheck(mint)
        all_checks.extend(rc_checks)

    # 3. Liquidity
    liq_checks, liquidity, holders = check_liquidity(mint)
    all_checks.extend(liq_checks)

    # Calculate risk score and pass/fail
    failed = [c for c in all_checks if c["passed"] is False]
    unknown = [c for c in all_checks if c["passed"] is None]

    # Hard fails: honeypot, rug confirmed, no liquidity
    hard_fail_names = {"蜜罐检测", "RugCheck已rug", "流动性"}
    hard_fails = [c for c in failed if c["name"] in hard_fail_names]

    for f in failed:
        flags.append(f["detail"])

    risk_score = len(failed) * 15 + len(unknown) * 5
    risk_score = min(risk_score, 100)

    passed = len(hard_fails) == 0 and risk_score < 50

    return RiskReport(
        token_mint=mint,
        chain=chain,
        passed=passed,
        risk_score=risk_score,
        checks=all_checks,
        flags=flags,
        liquidity_usd=liquidity,
        holder_count=holders,
        lp_locked_pct=lp_locked,
    )


def format_risk_report(report: RiskReport) -> str:
    """Format RiskReport as Telegram HTML."""
    status = "✅ 通过" if report.passed else "❌ 未通过"
    mint_short = f"{report.token_mint[:8]}...{report.token_mint[-6:]}"

    lines = [
        f"🛡️ <b>安全检测报告</b>",
        f"Token: <code>{mint_short}</code>",
        f"结果: <b>{status}</b> · 风险 {report.risk_score}/100",
        f"LP锁定: {report.lp_locked_pct:.0f}%",
        "",
    ]

    for c in report.checks:
        icon = "✅" if c["passed"] else "❌" if c["passed"] is False else "❓"
        lines.append(f"  {icon} {c['name']}: {c['detail']}")

    if report.flags:
        lines.append("\n⚠️ <b>风险提示:</b>")
        for f in report.flags[:5]:
            lines.append(f"  · {f}")

    return "\n".join(lines)
