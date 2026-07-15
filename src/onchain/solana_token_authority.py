"""Finalized, read-only Solana mint-authority evidence for Launch candidates.

This module only calls ``getAccountInfo`` with ``jsonParsed`` encoding.  It does
not read keys, construct transactions, sign messages, or submit anything.  Legacy
SPL mints pass only after both mint and freeze authority are explicitly null.
Token-2022 remains cautionary until its extensions are parsed completely.
"""
from __future__ import annotations

import hashlib
import json
import os
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Callable

SPL_TOKEN_PROGRAM = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
TOKEN_2022_PROGRAM = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"
PUBLIC_SOLANA_RPC = "https://api.mainnet-beta.solana.com"

RpcCall = Callable[[str, list], object]


def _raw_hash(value: object) -> str:
    canonical = json.dumps(value, sort_keys=True, separators=(",", ":"),
                           ensure_ascii=False)
    return hashlib.sha256(canonical.encode()).hexdigest()


def _rpc(method: str, params: list, *, endpoint: str | None = None,
         timeout: float = 12) -> object:
    """Call Solana JSON-RPC while closing both normal and HTTP-error responses."""
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method,
                       "params": params}).encode()
    request = urllib.request.Request(
        endpoint or os.getenv("SOLANA_STREAM_RPC_URL")
        or os.getenv("SOLANA_RPC_URL", PUBLIC_SOLANA_RPC), data=body,
        headers={"Content-Type": "application/json", "User-Agent": "CryptoScope/1.0"},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            payload = json.load(response)
    except urllib.error.HTTPError as error:
        error.close()
        raise
    if not isinstance(payload, dict):
        raise RuntimeError("Solana RPC returned a non-object response")
    if payload.get("error"):
        raise RuntimeError(f"Solana RPC {method} failed: {payload['error']}")
    return payload.get("result")


def inspect_mint(mint: str, *, rpc_call: RpcCall | None = None) -> dict:
    """Return deterministic authority facts; every incomplete read fails closed."""
    checked_at = datetime.now(timezone.utc).isoformat()
    if not isinstance(mint, str) or not mint.strip():
        return {"state": "unknown", "source": "Solana finalized getAccountInfo",
                "reason": "missing mint", "checked_at": checked_at}
    call = rpc_call or _rpc
    try:
        result = call("getAccountInfo", [mint, {
            "encoding": "jsonParsed", "commitment": "finalized",
        }])
    except Exception as exc:
        return {"state": "unknown", "source": "Solana finalized getAccountInfo",
                "reason": f"RPC unavailable: {str(exc)[:80]}",
                "checked_at": checked_at}

    try:
        raw_hash = _raw_hash(result)
    except (TypeError, ValueError):
        return {"state": "unknown", "source": "Solana finalized getAccountInfo",
                "reason": "RPC result is not canonical JSON", "checked_at": checked_at}
    context = result.get("context") if isinstance(result, dict) else None
    value = result.get("value") if isinstance(result, dict) else None
    slot = context.get("slot") if isinstance(context, dict) else None
    if not isinstance(slot, int) or isinstance(slot, bool) or slot < 0:
        return {"state": "unknown", "source": "Solana finalized getAccountInfo",
                "reason": "malformed or missing finalized context slot",
                "slot": slot, "owner": None, "checked_at": checked_at,
                "raw_hash": raw_hash}
    if not isinstance(value, dict):
        return {"state": "unknown", "source": "Solana finalized getAccountInfo",
                "reason": "mint account not found or malformed", "slot": slot,
                "owner": None, "checked_at": checked_at, "raw_hash": raw_hash}

    owner = value.get("owner")
    base = {"source": "Solana finalized getAccountInfo", "slot": slot,
            "owner": owner, "checked_at": checked_at, "raw_hash": raw_hash,
            "commitment": "finalized"}
    if owner not in {SPL_TOKEN_PROGRAM, TOKEN_2022_PROGRAM}:
        return {**base, "state": "unknown",
                "reason": "mint account owner is not a supported SPL token program"}
    if value.get("executable") is not False:
        return {**base, "state": "unknown",
                "reason": "mint account executable flag is missing or invalid"}

    data = value.get("data")
    parsed = data.get("parsed") if isinstance(data, dict) else None
    info = parsed.get("info") if isinstance(parsed, dict) else None
    if (not isinstance(parsed, dict) or parsed.get("type") != "mint"
            or not isinstance(info, dict)):
        return {**base, "state": "unknown",
                "reason": "account is not a parsed mint"}
    required = ("isInitialized", "mintAuthority", "freezeAuthority")
    missing = [key for key in required if key not in info]
    if missing:
        return {**base, "state": "unknown", "reason": "parsed mint fields missing",
                "unknown_fields": missing}
    if info.get("isInitialized") is not True:
        return {**base, "state": "unknown", "reason": "mint is not initialized"}

    mint_authority = info.get("mintAuthority")
    freeze_authority = info.get("freezeAuthority")
    facts = {**base, "mint_authority": mint_authority,
             "freeze_authority": freeze_authority}
    hard = []
    if mint_authority is not None:
        hard.append("mint_authority")
    if freeze_authority is not None:
        hard.append("freeze_authority")
    if hard:
        return {**facts, "state": "avoid", "hard_flags": hard,
                "reason": "live mint or freeze authority"}
    if owner == TOKEN_2022_PROGRAM:
        return {**facts, "state": "caution",
                "cautions": ["token_2022_extensions_not_fully_parsed"],
                "reason": "Token-2022 extensions are not fully parsed"}
    return {**facts, "state": "pass", "hard_flags": [], "cautions": []}
