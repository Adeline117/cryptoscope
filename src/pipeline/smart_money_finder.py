"""Smart-money finder — track-record-based, fixing the survivorship bias.

The operator-cluster method finds who's STILL HOLDING (believers), never the smart
operator who SOLD the top (they've left the holder list). To find real smart money
you must select by REALIZED track record, not current holdings.

Approach:
  1. Seed with tokens that already pumped (the arena where smart money made money).
  2. Pull their swaps → the wallets that traded them (esp. sellers near the top).
  3. Score each wallet by Moralis realized PnL (total_realized_profit_usd + that it
     actually SELLS, not just holds). Keep proven profit-takers.
  4. Save to config/smart_money_proven.json.
  5. Forward signal: what those proven wallets are accumulating NOW = smart-money-
     backed candidates (the opposite of chasing believers).

Free via Moralis (multi-key rotated).

    python -m src.pipeline.smart_money_finder            # screen seeds → save list
    python -m src.pipeline.smart_money_finder --forward  # what the list buys now
"""

from __future__ import annotations

import json
import sys

import structlog

from src.config import CONFIG_DIR

logger = structlog.get_logger()

PROVEN_FILE = CONFIG_DIR / "smart_money_proven.json"
MIN_REALIZED_USD = 20_000     # proven realized profit floor
MIN_SELLS = 3                 # must actually take profit, not just hold
MIN_WIN_PCT = 20              # realized profit % floor
MAX_TRADES = 1500             # above this = market-maker / HFT bot, not directional
MIN_USD_PER_TRADE = 300       # MMs scalp tiny; a directional whale earns big per trade

# Seed tokens: ones that demonstrably pumped (the hunting ground for who profited).
_SEED_PUMPED = [
    ("0xF39e4b21c84e737Df08e2C3b32541d856f508E48", "bsc"),  # ESPORTS (+382%)
    ("0x997A58129890bBdA032231A52eD1ddC845fc18e1", "bsc"),  # SIREN (~23x)
]
_MCHAIN = {"bsc": "bsc", "ethereum": "eth", "base": "base"}


def _swap_wallet(s: dict) -> str | None:
    for k in ("walletAddress", "wallet_address", "fromAddress", "from_address"):
        if s.get(k):
            return str(s[k]).lower()
    return None


def token_traders(token: str, chain: str, pages: int = 4) -> set[str]:
    """Distinct wallets that traded a token (from swaps)."""
    from src.onchain import moralis_client
    mchain = _MCHAIN.get(chain, "bsc")
    wallets: set[str] = set()
    cursor = None
    for _ in range(pages):
        path = f"erc20/{token}/swaps?chain={mchain}&order=DESC&limit=100"
        if cursor:
            path += f"&cursor={cursor}"
        d = moralis_client.get(path)
        if not d:
            break
        for s in d.get("result", []):
            w = _swap_wallet(s)
            if w:
                wallets.add(w)
        cursor = d.get("cursor")
        if not cursor:
            break
    return wallets


def wallet_pnl(wallet: str, chain: str) -> dict | None:
    from src.onchain import moralis_client
    mchain = _MCHAIN.get(chain, "bsc")
    d = moralis_client.get(f"wallets/{wallet}/profitability/summary?chain={mchain}")
    if not d:
        return None
    return {
        "realized_usd": float(d.get("total_realized_profit_usd", 0) or 0),
        "realized_pct": float(d.get("total_realized_profit_percentage", 0) or 0),
        "sells": int(d.get("total_sells", 0) or 0),
        "trades": int(d.get("total_count_of_trades", 0) or 0),
    }


def is_smart(pnl: dict) -> bool:
    """Proven DIRECTIONAL profit-taker: real realized profit + win %, actually
    sells, but NOT a market-maker/HFT bot (huge trade count, tiny $/trade). We
    want whales who buy low and sell tops, not delta-neutral scalpers."""
    trades = max(pnl["trades"], 1)
    usd_per_trade = pnl["realized_usd"] / trades
    return (pnl["realized_usd"] >= MIN_REALIZED_USD
            and pnl["sells"] >= MIN_SELLS
            and pnl["realized_pct"] >= MIN_WIN_PCT
            and pnl["trades"] <= MAX_TRADES
            and usd_per_trade >= MIN_USD_PER_TRADE)


def find_smart_money(seeds=None, max_candidates: int = 60) -> list[dict]:
    seeds = seeds or _SEED_PUMPED
    candidates: set[tuple[str, str]] = set()
    for token, chain in seeds:
        for w in list(token_traders(token, chain))[:40]:
            candidates.add((w, chain))
    logger.info("smart_money_candidates", n=len(candidates))
    proven = []
    for w, chain in list(candidates)[:max_candidates]:
        pnl = wallet_pnl(w, chain)
        if pnl and is_smart(pnl):
            proven.append({"wallet": w, "chain": chain, **pnl})
    proven.sort(key=lambda x: -x["realized_usd"])
    return proven


def save_proven(proven: list[dict]) -> None:
    existing = {}
    if PROVEN_FILE.exists():
        try:
            existing = {w["wallet"]: w for w in json.loads(PROVEN_FILE.read_text())}
        except Exception:
            existing = {}
    for p in proven:
        existing[p["wallet"]] = p
    PROVEN_FILE.write_text(json.dumps(list(existing.values()), ensure_ascii=False, indent=2))


def forward_signal(top: int = 30) -> dict:
    """What are the proven smart wallets accumulating NOW? Tokens held by multiple
    of them = smart-money-backed candidates (forward-looking, not survivorship)."""
    from collections import Counter

    from src.onchain import moralis_client
    if not PROVEN_FILE.exists():
        return {}
    wallets = json.loads(PROVEN_FILE.read_text())
    tok = Counter()
    names = {}
    for w in wallets[:top]:
        mchain = _MCHAIN.get(w["chain"], "bsc")
        d = moralis_client.get(f"wallets/{w['wallet']}/tokens?chain={mchain}")
        for t in (d or {}).get("result", []):
            if t.get("possible_spam") or t.get("native_token"):
                continue
            ca = (t.get("token_address") or "").lower()
            if ca:
                tok[ca] += 1
                names[ca] = t.get("symbol", "?")
    return {ca: (names[ca], n) for ca, n in tok.most_common(20) if n >= 2}


def main():
    if "--forward" in sys.argv:
        print("聪明钱当前共同持仓(>=2个钱包):")
        for ca, (sym, n) in forward_signal().items():
            print(f"  {n}个 {sym:12} {ca}")
        return
    proven = find_smart_money()
    save_proven(proven)
    print(f"=== 筛出 {len(proven)} 个有战绩的聪明钱 ===")
    for p in proven[:20]:
        print(f"  {p['wallet'][:16]}… 已实现 ${p['realized_usd']:,.0f} "
              f"({p['realized_pct']:+.0f}%, {p['sells']}卖/{p['trades']}笔)")
    if proven:
        print(f"\n→ 已存入 {PROVEN_FILE}")
        print("  下一步: python -m src.pipeline.smart_money_finder --forward")


if __name__ == "__main__":
    from dotenv import load_dotenv

    from src.config import PROJECT_ROOT
    load_dotenv(PROJECT_ROOT / ".env")
    main()
