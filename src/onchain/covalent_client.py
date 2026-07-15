"""Covalent / GoldRush client — a FREE multichain indexer to replace Moralis for the
data RPC eth_getLogs can't give: ranked token-holder lists (and, best-effort, an
address's first funder). Moralis' free tier exhausts daily and is the single point of
failure for all BSC/EVM holder+funder data; Covalent's free tier covers BSC/ETH/Base
and is the keyed fallback that keeps the hunt alive when Moralis is parked.

Needs a free key: sign up at https://goldrush.dev → drop COVALENT_API_KEY (cqt_...)
into .env. Without it, available() is False and callers fall through (no behavior
change). Auth is HTTP Basic with the key as username (Covalent's documented scheme).
"""

from __future__ import annotations

import base64
import json
import os
import urllib.request

import structlog

logger = structlog.get_logger()

_BASE = "https://api.covalenthq.com/v1/"
# Covalent accepts numeric chain ids; map our ints to the canonical chain id it expects.
_CHAINS = {1: "1", 56: "56", 8453: "8453", 42161: "42161", 10: "10", 137: "137"}
# An HONEST backend UA: Cloudflare 1010-blocks the bare urllib UA, but Covalent itself
# 452-rejects browser-stealth UAs ("un-stealth your User-Agent"). A plain identifier
# passes both. (Same Cloudflare-1010 hurdle as Moralis, opposite resolution.)
_UA = "CryptoScope/1.0"


def key() -> str:
    return os.environ.get("COVALENT_API_KEY", "").strip()


def available() -> bool:
    return bool(key())


def supports_chain(chain_id: object) -> bool:
    """Whether the concrete GoldRush endpoints used here support this chain."""
    try:
        return int(chain_id) in _CHAINS
    except (TypeError, ValueError):
        return False


def get(endpoint: str, timeout: int = 25):
    """GET a Covalent v1 endpoint (everything after /v1/). Returns parsed JSON or None.
    Basic auth: key as username, empty password."""
    k = key()
    if not k:
        return None
    auth = base64.b64encode(f"{k}:".encode()).decode()
    try:
        req = urllib.request.Request(
            _BASE + endpoint,
            headers={"Authorization": f"Basic {auth}", "accept": "application/json",
                     "User-Agent": _UA})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode())
    except Exception as e:
        logger.debug("covalent_request_failed", endpoint=endpoint[:70], error=str(e)[:80])
        return None


def fetch_holders(token: str, chain_id: int, page_size: int = 100,
                  max_pages: int = 3, timeout: int = 25) -> list[dict]:
    """Ranked (balance-desc) token holders with decimal-adjusted balances — the
    Moralis-owners replacement. token_holders_v2 returns balance (raw) +
    contract_decimals. page_size must be >=100 (Covalent rejects smaller)."""
    chain = _CHAINS.get(chain_id)
    if not chain or not available():
        return []
    zero = "0x0000000000000000000000000000000000000000"
    holders: list[dict] = []
    for page in range(max_pages):
        data = get(f"{chain}/tokens/{token}/token_holders_v2/"
                   f"?page-size={page_size}&page-number={page}", timeout)
        items = ((data or {}).get("data") or {}).get("items") or []
        if not items:
            break
        for it in items:
            addr = (it.get("address") or "").lower()
            try:
                dec = int(it.get("contract_decimals") or 18)
                bal = int(it.get("balance") or 0) / float(10 ** dec)
            except (ValueError, TypeError):
                bal = 0.0
            if addr and addr != zero and bal > 0:
                holders.append({"address": addr, "balance": round(bal, 8)})
        if len(items) < page_size:
            break
    return holders


def first_funder(address: str, chain_id: int, timeout: int = 25) -> str | None:
    """The sender of the address's FIRST incoming native-value transfer — the funder
    (Moralis-free). Walks transactions oldest-first; returns the first external `from`
    that sent value to `address`. Best-effort (None if undeterminable)."""
    chain = _CHAINS.get(chain_id)
    if not chain or not available():
        return None
    addr = address.lower()
    # transactions_v3 ascending → oldest first; one page usually reaches the funding tx
    # for a fresh accumulation/Sybil wallet.
    data = get(f"{chain}/address/{addr}/transactions_v3/page/0/"
               f"?block-signed-at-asc=true&no-logs=true", timeout)
    items = ((data or {}).get("data") or {}).get("items") or []
    for tx in items:
        to = (tx.get("to_address") or "").lower()
        frm = (tx.get("from_address") or "").lower()
        try:
            val = int(tx.get("value") or 0)
        except (ValueError, TypeError):
            val = 0
        if to == addr and frm and frm != addr and val > 0:
            return frm
    return None
