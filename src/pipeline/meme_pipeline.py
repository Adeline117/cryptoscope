"""Meme coin scanning pipeline — runs every 10 minutes.

Dedicated pipeline for meme coin discovery and alerting.
Scans Pump.fun new launches + DexScreener trending, runs security checks,
scores with token_alpha_scorer, and pushes qualified tokens to Telegram.

Flow:
1. Collect from PumpFunCollector + DexScreenerTrendingCollector
2. Enrich with GoPlus security data
3. Score each token with token_alpha_scorer
4. Filter: alpha_score >= 40 OR recommendation != SKIP
5. Format and send via Telegram (individual alerts for S/A grade, digest for rest)
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import structlog

logger = structlog.get_logger()


async def run_meme_pipeline() -> dict:
    """Execute the meme coin scanning pipeline.

    Returns:
        Summary dict with counts and status.
    """
    logger.info("meme_pipeline_started")
    start = datetime.now(timezone.utc)

    # 1. Collect from meme-specific collectors
    all_tokens = await _collect_meme_tokens()
    logger.info("meme_collection_done", count=len(all_tokens))

    if not all_tokens:
        logger.info("meme_pipeline_no_tokens")
        return {"status": "empty", "tokens_scanned": 0}

    # 2. Enrich with security data and score
    scored_tokens = await _enrich_and_score(all_tokens)
    logger.info("meme_scoring_done", scored=len(scored_tokens))

    # 3. Filter — keep tokens worth alerting
    qualified = [t for t in scored_tokens if t.get("recommendation") != "SKIP"]
    qualified.sort(key=lambda t: t.get("alpha_score", 0), reverse=True)

    logger.info("meme_filter_done", qualified=len(qualified), total=len(scored_tokens))

    # 4. Send alerts
    sent_count = await _send_meme_alerts(qualified, len(all_tokens))

    elapsed = (datetime.now(timezone.utc) - start).total_seconds()
    summary = {
        "status": "sent" if sent_count > 0 else "no_qualified",
        "tokens_scanned": len(all_tokens),
        "tokens_qualified": len(qualified),
        "alerts_sent": sent_count,
        "elapsed_seconds": round(elapsed, 1),
    }
    logger.info("meme_pipeline_complete", **summary)
    return summary


async def _collect_meme_tokens() -> list[dict]:
    """Run PumpFunCollector and DexScreenerTrendingCollector, return parsed token dicts."""
    tokens: list[dict] = []

    collectors_to_try = [
        ("src.collectors.meme_scanner", "PumpFunCollector"),
        ("src.collectors.meme_scanner", "DexScreenerTrendingCollector"),
    ]

    for mod_path, cls_name in collectors_to_try:
        try:
            import importlib
            mod = importlib.import_module(mod_path)
            cls = getattr(mod, cls_name)
            collector = cls(auto_security_check=False)  # We do security checks ourselves
            await collector.setup()
            try:
                result = await collector._collect()
                for item in result.items:
                    meta = item.metadata or {}
                    # Determine platform
                    platform = ""
                    if meta.get("source") == "pumpfun":
                        platform = "pump.fun"
                    elif meta.get("dex_id"):
                        platform = meta["dex_id"]

                    # Calculate age in minutes
                    age_minutes = None
                    created = meta.get("pair_created_at")
                    if created and isinstance(created, (int, float)):
                        age_seconds = (datetime.now(timezone.utc).timestamp() * 1000 - created) / 1000
                        age_minutes = max(0, int(age_seconds / 60))

                    tokens.append({
                        "token_name": meta.get("token_name", ""),
                        "token_symbol": meta.get("token_symbol", ""),
                        "token_address": meta.get("token_address", ""),
                        "pair_address": meta.get("pair_address", ""),
                        "chain_id": meta.get("chain_id", ""),
                        "dex_id": meta.get("dex_id", ""),
                        "platform": platform,
                        "price_usd": meta.get("price_usd"),
                        "market_cap": _estimate_mcap(meta),
                        "liquidity_usd": meta.get("liquidity_usd", 0),
                        "volume_24h": meta.get("volume_24h", 0),
                        "price_change_5m": meta.get("price_change_5m"),
                        "price_change_1h": meta.get("price_change_1h"),
                        "price_change_24h": meta.get("price_change_24h"),
                        "age_minutes": age_minutes,
                        "risk_level": meta.get("risk_level", ""),
                        "flags": meta.get("flags", []),
                        "volume_liquidity_ratio": meta.get("volume_liquidity_ratio", 0),
                        "boost_amount": meta.get("boost_amount", 0),
                        "data_type": meta.get("data_type", ""),
                        "url": item.url or "",
                        "_raw_security": meta.get("security_check"),
                    })
            finally:
                await collector.teardown()
        except Exception as e:
            logger.warning("meme_collector_failed", collector=cls_name, error=str(e))

    # Deduplicate by token address
    seen: set[str] = set()
    unique: list[dict] = []
    for t in tokens:
        addr = t.get("token_address", "")
        if addr and addr not in seen:
            seen.add(addr)
            unique.append(t)

    return unique


def _estimate_mcap(meta: dict) -> float | None:
    """Estimate market cap from available data."""
    # DexScreener sometimes provides fdv directly in raw data
    price = meta.get("price_usd")
    if price:
        try:
            price_f = float(price)
            # For pump.fun tokens, total supply is typically 1B
            if meta.get("source") == "pumpfun":
                return price_f * 1_000_000_000
        except (ValueError, TypeError):
            pass
    return None


def _classify_security_result(result) -> dict:
    """Convert a GoPlus response into an evidence-aware directional gate."""
    risks = [str(value) for value in (result.risks or [])]
    lowered = " ".join(risks).lower()
    evidence_available = (
        isinstance(result.raw, dict)
        and bool(result.raw)
        and "unable to verify" not in lowered
        and "not found" not in lowered
    )
    if not evidence_available:
        state = "unknown"
    elif result.is_honeypot or result.risk_score < 50:
        state = "avoid"
    elif result.risk_score < 70:
        state = "caution"
    else:
        state = "pass"
    info = result.info if isinstance(result.info, dict) else {}
    return {
        "state": state,
        "evidence_available": evidence_available,
        "is_honeypot": bool(result.is_honeypot),
        "can_mint": "mint" in lowered,
        "has_freeze": "freeze" in lowered or "frozen" in lowered,
        "has_blacklist": "blacklist" in lowered,
        "has_proxy": "proxy" in lowered or "upgradeable" in lowered,
        "goplus_score": result.risk_score,
        "dev_holding_pct": 0,
        "holder_count": info.get("holder_count"),
        "risks": risks[:8],
    }


async def _enrich_and_score(tokens: list[dict], security_checker_factory=None) -> list[dict]:
    """Score discoveries, but make every directional recommendation fail closed."""
    security_checker = None
    try:
        if security_checker_factory is None:
            from src.collectors.contract_security import ContractSecurityChecker

            security_checker_factory = ContractSecurityChecker
        security_checker = security_checker_factory()
        await security_checker.setup()
    except Exception as e:
        logger.warning("security_checker_unavailable", error=str(e))

    from src.analysis.token_alpha_scorer import score_token

    scored: list[dict] = []

    for token in tokens:
        # Skip tokens with extremely low liquidity (< $500) — not worth checking
        if token.get("liquidity_usd", 0) < 500:
            continue

        # Run security check
        security_data = {
            "state": "unknown",
            "evidence_available": False,
            "goplus_score": None,
            "risks": ["security evidence unavailable"],
        }
        if security_checker and token.get("token_address"):
            try:
                chain_map = {"solana": "solana", "ethereum": 1, "bsc": 56, "base": 8453}
                chain_val = chain_map.get(token["chain_id"], token["chain_id"])
                result = await security_checker.check_token(str(chain_val), token["token_address"])
                if result:
                    security_data = _classify_security_result(result)
                    # Extract holder count
                    token["holder_count"] = security_data.get("holder_count")
            except Exception as e:
                logger.debug("meme_security_check_failed", token=token.get("token_symbol"), error=str(e))
                security_data["risks"] = [f"security check failed: {str(e)[:80]}"]

        token["security"] = security_data
        token["security_state"] = security_data["state"]
        token["security_qualified"] = security_data["state"] == "pass"

        # Prepare data for alpha scorer
        scorer_data = {
            "liquidity_usd": token.get("liquidity_usd", 0),
            "top10_holder_pct": token.get("top10_holder_pct", 50),  # default to moderate
            "security": {
                "is_honeypot": security_data.get("is_honeypot", False),
                "can_mint": security_data.get("can_mint", False),
                "dev_holding_pct": security_data.get("dev_holding_pct", 0),
                "has_proxy": security_data.get("has_proxy", False),
                "has_blacklist": security_data.get("has_blacklist", False),
            },
            "smart_money_t1_count": 0,
            "smart_money_t2_count": 0,
            "social_mentions_24h": 0,
            "social_sentiment": 0.5,
            "hot_narratives": ["meme"],
            "token_narratives": ["meme"],
        }

        # Try to get smart money data
        try:
            sm_data = await _check_smart_money(token)
            if sm_data:
                scorer_data["smart_money_t1_count"] = sm_data.get("t1_count", 0)
                scorer_data["smart_money_t2_count"] = sm_data.get("t2_count", 0)
                token["smart_money"] = sm_data
        except Exception:
            token["smart_money"] = {}

        # Score
        alpha_result = score_token(scorer_data)
        token["raw_alpha_score"] = alpha_result["alpha_score"]
        token["raw_alpha_grade"] = alpha_result["grade"]
        token["raw_recommendation"] = alpha_result["recommendation"]
        if token["security_qualified"]:
            token["alpha_score"] = alpha_result["alpha_score"]
            token["alpha_grade"] = alpha_result["grade"]
            token["recommendation"] = alpha_result["recommendation"]
            token["red_flags"] = alpha_result.get("red_flags", [])
        else:
            reason = (security_data.get("risks") or ["security evidence unavailable"])[0]
            token["alpha_score"] = 0
            token["alpha_grade"] = "D"
            token["recommendation"] = "SKIP"
            token["red_flags"] = [
                f"SECURITY_{security_data['state'].upper()}: {reason}",
                *alpha_result.get("red_flags", []),
            ]

        scored.append(token)

    if security_checker:
        try:
            await security_checker.teardown()
        except Exception:
            pass

    return scored


async def _check_smart_money(token: dict) -> dict | None:
    """Try to check smart money participation via SmartMoneyCollector."""
    try:
        from src.collectors.smart_money import SmartMoneyCollector
        # SmartMoneyCollector may not support per-token queries;
        # return empty for now — this can be enhanced later
        return {"t1_count": 0, "t2_count": 0}
    except ImportError:
        return None


async def _send_meme_alerts(qualified: list[dict], total_scanned: int) -> int:
    """Send meme alerts via Telegram.

    - S/A grade tokens: individual detailed alerts with inline keyboard
    - B/C grade tokens: bundled into a digest message
    """
    from src.distribution.message_templates import format_meme_alert, format_meme_digest
    from src.distribution.telegram_sender import send_meme_alert

    sent = 0

    # Individual alerts for high-quality finds (S or A grade)
    top_tokens = [t for t in qualified if t.get("alpha_grade") in ("S", "A")]
    for token in top_tokens[:3]:  # Max 3 individual alerts per cycle
        message = format_meme_alert(token)

        # Build inline keyboard
        try:
            from src.distribution.inline_keyboards import meme_action_keyboard
            keyboard = meme_action_keyboard(
                symbol=token.get("token_symbol", ""),
                chain=token.get("chain_id", ""),
                address=token.get("token_address", ""),
            )
        except Exception:
            keyboard = None

        success = await send_meme_alert(message, reply_markup=keyboard)
        if success:
            sent += 1
        await asyncio.sleep(0.5)

    # Digest for B/C grade tokens
    watchlist_tokens = [t for t in qualified if t.get("alpha_grade") in ("B", "C")]
    if watchlist_tokens:
        digest_msg = format_meme_digest(
            watchlist_tokens[:8],
            scan_stats={
                "total_scanned": total_scanned,
                "passed_filter": len(qualified),
                "chains": list({t.get("chain_id", "") for t in qualified if t.get("chain_id")}),
            },
        )
        success = await send_meme_alert(digest_msg)
        if success:
            sent += 1

    # If nothing qualified but we scanned tokens, log it
    if not qualified:
        logger.info("meme_no_qualified_tokens", total_scanned=total_scanned)

    return sent


if __name__ == "__main__":
    import json

    result = asyncio.run(run_meme_pipeline())
    print(json.dumps(result, indent=2, ensure_ascii=False))
