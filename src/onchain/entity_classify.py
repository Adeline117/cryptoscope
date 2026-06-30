"""On-chain entity classification — what KIND of holder is this address?

Structural concentration alone fooled us repeatedly: a believer EOA that only
holds (ESPORTS 0x99d4b3), a team multisig that fans tokens to dump wallets
(ESPORTS 0x1552160b, a Gnosis-Safe proxy), and a treasury/vesting contract
(ESPORTS 0x49a0c2, 24% locked) all looked the same to a pure %-of-supply view.

This labels an address so the operator logic can interpret a cluster correctly:
- `burn`      — null / dead address (not a holder at all)
- `cex`       — known exchange hot/deposit wallet (custody, not an operator)
- `multisig`  — small proxy / Gnosis-Safe (team / governance control)
- `contract`  — larger contract (treasury / vesting / staking / LP — custody)
- `eoa`       — externally-owned wallet (the only TRADING-operator candidate)

A label is CONTEXT, not a verdict — pair it with behaviour (distribution history /
live net flow). But it cheaply vetoes the obvious non-operators (burn/cex/LP) and
flags team-control (multisig) / locked-custody (contract) so they can never again
be mistaken for a disciplined trading operator.
"""

from __future__ import annotations

import structlog

logger = structlog.get_logger()

_BURN = {"0x0000000000000000000000000000000000000000",
         "0x000000000000000000000000000000000000dead"}
# Gnosis-Safe / minimal-proxy runtime code is tiny (~45-300 bytes); a full
# staking/vesting/treasury/token contract is multiple KB. The boundary cleanly
# separates the ESPORTS owner multisig (172 bytes) from its treasury (10KB).
_PROXY_MAX_BYTES = 300


def classify_address(address: str, chain: str, rpc=None) -> dict:
    """Return {address, type, is_operator_candidate, custody, detail}.

    is_operator_candidate=True ONLY for a non-CEX EOA — the only thing a human
    operator actually trades from. Everything else (burn/cex/multisig/contract) is
    custody or team/exchange control, never a "disciplined trading operator"."""
    a = (address or "").lower()
    if not a:
        return {"address": address, "type": "unknown", "is_operator_candidate": False,
                "custody": False, "detail": "empty"}
    if a in _BURN:
        return {"address": a, "type": "burn", "is_operator_candidate": False,
                "custody": False, "detail": "null/dead"}
    try:
        from src.onchain.cex_addresses import evm_exchanges, solana_exchanges
        cex = solana_exchanges() if chain in ("solana", "sol") else evm_exchanges()
        if a in cex or address in cex:
            return {"address": a, "type": "cex", "is_operator_candidate": False,
                    "custody": True, "detail": cex.get(a) or cex.get(address) or "exchange"}
    except Exception:
        pass
    # Solana: no eth_getCode; treat as EOA-ish (owner accounts). Program-owned
    # detection would need account-info parsing — out of scope here.
    if chain in ("solana", "sol"):
        return {"address": a, "type": "eoa", "is_operator_candidate": True,
                "custody": False, "detail": "solana owner (code-class unknown)"}
    # EVM: code distinguishes EOA vs contract; size distinguishes proxy vs full contract.
    try:
        from src.onchain.evm_archive import ArchiveRPC
        rpc = rpc or ArchiveRPC(chain)
        code = rpc._logs_call("eth_getCode", [address, "latest"]).get("result", "0x")
    except Exception as e:
        logger.debug("classify_getcode_failed", address=a, error=str(e)[:60])
        return {"address": a, "type": "unknown", "is_operator_candidate": False,
                "custody": False, "detail": "getCode failed"}
    if not code or code == "0x" or len(code) <= 4:
        return {"address": a, "type": "eoa", "is_operator_candidate": True,
                "custody": False, "detail": "no code"}
    nbytes = (len(code) - 2) // 2
    if nbytes <= _PROXY_MAX_BYTES:
        return {"address": a, "type": "multisig", "is_operator_candidate": False,
                "custody": True, "detail": f"proxy/Safe (~{nbytes}B)"}
    return {"address": a, "type": "contract", "is_operator_candidate": False,
            "custody": True, "detail": f"contract (~{nbytes}B: treasury/vesting/staking/LP)"}


def classify_cluster(addresses: list[str], chain: str, rpc=None) -> dict:
    """Classify a whole cluster → {counts, eoa_share_of_members, types, summary}.

    Lets the operator logic say e.g. 'this 38% is a multisig + treasury, not a
    trading operator' (ESPORTS) vs 'these are fresh EOAs trading' (a real play)."""
    from src.onchain.evm_archive import ArchiveRPC
    rpc = rpc or (None if chain in ("solana", "sol") else ArchiveRPC(chain))
    cats = [classify_address(a, chain, rpc=rpc) for a in addresses]
    counts: dict[str, int] = {}
    for c in cats:
        counts[c["type"]] = counts.get(c["type"], 0) + 1
    n = len(cats) or 1
    eoa = counts.get("eoa", 0)
    parts = [f"{k}×{v}" for k, v in sorted(counts.items(), key=lambda x: -x[1])]
    return {"counts": counts, "members": cats,
            "eoa_share_of_members": round(eoa / n, 2),
            "summary": " ".join(parts)}
