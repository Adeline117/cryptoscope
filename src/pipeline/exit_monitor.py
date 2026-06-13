"""Exit monitor — watch previously-accumulating tokens for distribution.

Whale distribution (wallet→CEX deposits) is the exit you most want to front-run.
This pipeline:
  1. Pulls tokens that recently fired an accumulation signal (from the scorecard).
  2. For each EVM token, fetches recent ERC-20 transfers and labels endpoints
     against the known-exchange set (whale_tracker.KNOWN_EXCHANGES).
  3. Runs DistributionExitSignal on the net flow.
  4. On an EXIT signal, pushes a critical alert.

EVM-only for the MVP: Solana CEX-deposit labeling needs a Solana exchange-address
set the repo doesn't have yet, so Solana tokens are skipped (logged).
"""

from __future__ import annotations

import json
import os
import sqlite3
import urllib.request
from datetime import datetime, timedelta, timezone

import structlog

from src.signals.distribution_exit import DistributionExitSignal, classify_flows

logger = structlog.get_logger()

MAX_TOKENS = 20             # bound per-run work
LOOKBACK_DAYS = 7           # only watch tokens that accumulated this recently
TRANSFER_WINDOW = 200       # recent transfers to scan per token

_EVM_CHAIN_IDS = {
    "ethereum": 1, "eth": 1, "base": 8453, "bsc": 56,
    "arbitrum": 42161, "optimism": 10, "polygon": 137,
}


def _recent_accumulation_tokens() -> list[dict]:
    """Read recent accumulation_divergence signals from the scorecard DB."""
    from src.trading.signal_scorecard import DB_PATH

    if not DB_PATH.exists():
        return []
    cutoff = (datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)).isoformat()
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    conn.execute("PRAGMA busy_timeout=10000")
    try:
        rows = conn.execute(
            """SELECT asset, chain, metadata FROM signals
               WHERE signal_type = 'accumulation_divergence' AND created_at >= ?
               ORDER BY created_at DESC""",
            (cutoff,),
        ).fetchall()
    finally:
        conn.close()

    out, seen = [], set()
    for asset, chain, meta_json in rows:
        try:
            meta = json.loads(meta_json) if meta_json else {}
        except (json.JSONDecodeError, TypeError):
            meta = {}
        addr = meta.get("token_address", "")
        key = (addr, chain)
        if addr and key not in seen:
            seen.add(key)
            out.append({"asset": asset, "chain": chain, "address": addr})
    return out


def _fetch_labeled_transfers(token: str, chain_id: int, timeout: int = 20) -> list[dict]:
    """Fetch recent ERC-20 transfers and label endpoints as CEX or unknown."""
    from src.onchain.cex_addresses import evm_exchanges

    exchanges = evm_exchanges()
    key = os.environ.get("ETHERSCAN_API_KEY", "")
    if not key:
        return []
    url = (
        f"https://api.etherscan.io/v2/api?chainid={chain_id}&module=account&action=tokentx"
        f"&contractaddress={token}&page=1&offset={TRANSFER_WINDOW}&sort=desc&apikey={key}"
    )
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "CryptoScope/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode())
        if data.get("status") != "1":
            return []
        txs = data.get("result", [])
    except Exception as e:
        logger.debug("exit_transfers_fetch_failed", token=token, error=str(e))
        return []

    return [
        {
            "from_label": exchanges.get((t.get("from") or "").lower(), "unknown"),
            "to_label": exchanges.get((t.get("to") or "").lower(), "unknown"),
        }
        for t in txs
    ]


def _solana_rpc(method: str, params: list, timeout: int = 15) -> dict:
    rpc = os.environ.get("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")
    req = urllib.request.Request(
        rpc,
        data=json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


def _fetch_solana_flows(mint: str, max_txs: int = 25, timeout: int = 15) -> dict:
    """Count CEX deposit/withdrawal flows for a Solana mint from recent txs.

    For each recent transaction touching the mint, compares pre/post token
    balances per owner: a CEX owner that GAINED tokens = wallet→CEX deposit
    (distribution); a CEX owner that LOST tokens = CEX→wallet (accumulation).
    Best-effort and bounded; returns {to_cex_count, from_cex_count}.
    """
    from src.onchain.cex_addresses import solana_exchanges

    cex = solana_exchanges()
    to_cex = from_cex = 0
    try:
        sigs = _solana_rpc("getSignaturesForAddress", [mint, {"limit": max_txs}], timeout).get("result", [])
    except Exception as e:
        logger.debug("solana_flows_sigs_failed", mint=mint, error=str(e))
        return {"to_cex_count": 0, "from_cex_count": 0}

    for s in sigs[:max_txs]:
        sig = s.get("signature")
        if not sig:
            continue
        try:
            tx = _solana_rpc(
                "getTransaction",
                [sig, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}],
                timeout,
            ).get("result", {})
        except Exception:
            continue
        meta = (tx or {}).get("meta", {}) or {}
        pre = {b.get("owner"): _ui(b) for b in meta.get("preTokenBalances", []) if b.get("mint") == mint}
        post = {b.get("owner"): _ui(b) for b in meta.get("postTokenBalances", []) if b.get("mint") == mint}
        tx_to = tx_from = False
        for owner in set(pre) | set(post):
            if owner not in cex:
                continue
            delta = post.get(owner, 0) - pre.get(owner, 0)
            if delta > 0:
                tx_to = True   # CEX received → someone deposited to sell
            elif delta < 0:
                tx_from = True  # CEX sent out → withdrawal
        to_cex += int(tx_to)
        from_cex += int(tx_from)
    return {"to_cex_count": to_cex, "from_cex_count": from_cex}


def _ui(balance: dict) -> float:
    try:
        return float(balance.get("uiTokenAmount", {}).get("uiAmount") or 0)
    except (ValueError, TypeError):
        return 0.0


async def run_exit_monitor(send: bool = True) -> dict:
    """One exit-monitor tick. Returns a summary dict."""
    tokens = _recent_accumulation_tokens()[:MAX_TOKENS]
    if not tokens:
        return {"status": "no_accumulation_tokens", "checked": 0, "exits": 0}

    sig_eval = DistributionExitSignal()
    exits = 0
    checked = 0
    for tok in tokens:
        chain = tok["chain"]
        if chain in ("solana", "sol"):
            flows = _fetch_solana_flows(tok["address"])
            if not flows.get("to_cex_count") and not flows.get("from_cex_count"):
                continue
        else:
            chain_id = _EVM_CHAIN_IDS.get(chain, 1)
            transfers = _fetch_labeled_transfers(tok["address"], chain_id)
            if not transfers:
                continue
            flows = classify_flows(transfers)
        checked += 1
        sig = await sig_eval.evaluate({
            **flows,
            "had_accumulation": True,
            "token_symbol": tok["asset"],
            "token_address": tok["address"],
            "chain": chain,
        })
        if not sig:
            continue
        exits += 1
        if send:
            await _emit_exit(tok, sig)

    summary = {"status": "complete", "checked": checked, "exits": exits}
    logger.info("exit_monitor_complete", **summary)
    return summary


async def _emit_exit(token: dict, sig) -> None:
    """Push a critical exit alert."""
    try:
        from src.distribution.telegram_sender import send_critical_alert

        c = sig.components
        msg = (
            f"🚨 <b>庄家开始派发 · 出场信号</b> · {token['asset']}\n"
            f"<i>{token['chain']}链 · 之前在吸筹，现在往交易所充币了</i>\n"
            f"━━━━━━━━━━━━━━\n"
            f"· 钱包→交易所 {c['to_cex_count']} 笔（出货）\n"
            f"· 交易所→钱包 {c['from_cex_count']} 笔（回流）\n"
            f"· 派发是回流的 <b>{c['distribution_ratio']:.1f} 倍</b>\n"
            f"信号强度 {sig.confidence}/100\n"
            f"📍 <code>{token['address']}</code>\n"
            f"<i>⚠️ 出场参考，非投资建议</i>"
        )
        await send_critical_alert(msg)
    except Exception as e:
        logger.warning("exit_alert_failed", error=str(e))
