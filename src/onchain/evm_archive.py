"""Multi-chain archive reconstruction — our own, RPC-pluggable.

The key to "cover all chains ourselves" without Arkham: reconstruct any address
set's holding of any token AT ANY PAST BLOCK via `eth_call balanceOf` against an
archive RPC. This sidesteps Alchemy/Etherscan paid multichain tiers — it works
on ANY chain for which we have a working archive RPC.

Per-chain RPC is configured via env (RPC_<CHAIN>) with sensible defaults; ETH
uses the Alchemy key we already have (full archive). Chains whose free public
RPCs lack archive (e.g. BSC in practice) just need one archive RPC URL dropped
into the env — a ~$49/mo multichain plan covers everything, vs $390 for Arkham.

This is the foundation of the operator-curve and full-holder reconstruction.
"""

from __future__ import annotations

import json
import os
import time
import urllib.request

import structlog

logger = structlog.get_logger()

BALANCE_OF = "0x70a08231"  # balanceOf(address) selector
DECIMALS = "0x313ce567"    # decimals() selector
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
# A browser UA — several keyless public RPCs (publicnode) 403 the default urllib UA.
_BROWSER_UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
               "(KHTML, like Gecko) Chrome/120 Safari/537.36")

# Keyless RPCs that reliably serve eth_getLogs over wide ranges (separate from the
# archive pool, which is tuned for historical balanceOf). publicnode handles 10k-block
# getLogs cleanly; this is the Moralis-free path for transfer detection.
_LOGS_RPCS = {
    # NodeReal + bloXroute lead: live-tested to serve eth_getLogs over a 9000-block
    # range (publicnode now 403s and drpc 408s on BSC, which silently blinded the
    # transfer-based 庄买/庄卖 detection — flow_ts never advanced). Keep the old two
    # as last-resort fallbacks.
    "bsc": ["https://bsc-mainnet.nodereal.io/v1/64a9df0874fb4a93b9d0a3849de012d3",
            "https://bsc.rpc.blxrbdn.com",
            "https://bsc-rpc.publicnode.com", "https://bsc.drpc.org"],
    "ethereum": ["https://ethereum-rpc.publicnode.com", "https://eth.drpc.org"],
    "base": ["https://base-rpc.publicnode.com", "https://base.publicnode.com"],
}


def _default_rpcs(chain: str) -> list[str]:
    """RPC pool per chain. Env RPC_<CHAIN> (comma-separated) takes priority."""
    env = os.environ.get(f"RPC_{chain.upper()}", "")
    pool = [u.strip() for u in env.split(",") if u.strip()]
    if pool:
        return pool
    key = os.environ.get("ALCHEMY_API_KEY", "")
    defaults = {
        "ethereum": [f"https://eth-mainnet.g.alchemy.com/v2/{key}"] if key else [],
        "base": ["https://mainnet.base.org", "https://base.publicnode.com"],
        # Free keyless BSC archive endpoints (verified to serve historical state).
        "bsc": ["https://56.rpc.thirdweb.com", "https://bsc-mainnet.public.blastapi.io"],
        "arbitrum": ["https://arb1.arbitrum.io/rpc"],
        "optimism": ["https://mainnet.optimism.io"],
        "polygon": ["https://polygon-rpc.com"],
    }
    return defaults.get(chain, [])


class ArchiveRPC:
    """A small archive-RPC client with a fallback pool."""

    def __init__(self, chain: str):
        self.chain = chain
        self.rpcs = _default_rpcs(chain)
        self._idx = 0

    def available(self) -> bool:
        return bool(self.rpcs)

    def _call(self, method: str, params: list, timeout: int = 15) -> dict:
        last_err = None
        for _ in range(len(self.rpcs)):
            rpc = self.rpcs[self._idx % len(self.rpcs)]
            try:
                req = urllib.request.Request(
                    rpc, data=json.dumps({"jsonrpc": "2.0", "id": 1, "method": method,
                                          "params": params}).encode(),
                    headers={"Content-Type": "application/json", "User-Agent": _BROWSER_UA},
                )
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    data = json.loads(resp.read().decode())
                if "error" not in data:
                    return data
                last_err = data["error"]
            except Exception as e:
                last_err = e
            self._idx += 1  # rotate on failure
        raise RuntimeError(f"all RPCs failed for {self.chain}: {last_err}")

    def latest_block(self) -> int:
        return int(self._call("eth_blockNumber", [])["result"], 16)

    def _logs_call(self, method: str, params: list, timeout: int = 20) -> dict:
        """Call against the keyless LOGS pool (publicnode-first) — used for getLogs /
        block lookups where we don't need historical archive state."""
        pool = _LOGS_RPCS.get(self.chain) or self.rpcs
        last_err = None
        for rpc in pool:
            try:
                req = urllib.request.Request(
                    rpc, data=json.dumps({"jsonrpc": "2.0", "id": 1, "method": method,
                                          "params": params}).encode(),
                    headers={"Content-Type": "application/json", "User-Agent": _BROWSER_UA})
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    data = json.loads(resp.read().decode())
                if "error" not in data and data.get("result") is not None:
                    return data
                last_err = data.get("error")
            except Exception as e:
                last_err = e
        raise RuntimeError(f"all logs RPCs failed for {self.chain}: {last_err}")

    def logs_head(self) -> int:
        return int(self._logs_call("eth_blockNumber", [])["result"], 16)

    def get_transfer_logs(self, token: str, from_block: int, to_block: int | str = "latest",
                          chunk: int = 9000) -> list[dict]:
        """All Transfer logs for `token` in [from_block, to_block], chunked to respect
        keyless-RPC range limits. Keyless (publicnode) — no Moralis."""
        head = self.logs_head() if to_block == "latest" else int(to_block)
        out: list[dict] = []
        start = from_block
        while start <= head:
            end = min(start + chunk - 1, head)
            try:
                r = self._logs_call("eth_getLogs", [{
                    "address": token, "topics": [TRANSFER_TOPIC],
                    "fromBlock": hex(start), "toBlock": hex(end)}])
                out.extend(r.get("result") or [])
            except Exception as e:
                logger.debug("get_logs_chunk_failed", chain=self.chain, error=str(e)[:80])
            start += chunk
        return out

    def block_time(self, block: int) -> int | None:
        try:
            r = self._logs_call("eth_getBlockByNumber", [hex(block), False])
            ts = (r.get("result") or {}).get("timestamp")
            return int(ts, 16) if ts else None
        except Exception:
            return None

    def token_decimals(self, token: str) -> int:
        try:
            r = self._logs_call("eth_call", [{"to": token, "data": DECIMALS}, "latest"])
            res = r.get("result")
            return int(res, 16) if res and res != "0x" else 18
        except Exception:
            return 18

    def balance_of(self, token: str, holder: str, block: int | str = "latest") -> float | None:
        data = BALANCE_OF + holder[2:].lower().rjust(64, "0")
        blk = block if isinstance(block, str) else hex(block)
        try:
            r = self._call("eth_call", [{"to": token, "data": data}, blk])
            res = r.get("result")
            return int(res, 16) / 1e18 if res and res != "0x" else 0.0
        except Exception as e:
            logger.debug("balance_of_failed", token=token, holder=holder, error=str(e))
            return None


def combined_balance_at(token: str, addresses: list[str], chain: str,
                        block: int, rpc: ArchiveRPC | None = None) -> float:
    """Sum balanceOf for an address set at a given block (one entity's holding)."""
    rpc = rpc or ArchiveRPC(chain)
    total = 0.0
    for a in addresses:
        b = rpc.balance_of(token, a, block)
        if b:
            total += b
    return total


def operator_curve_evm(token: str, addresses: list[str], chain: str,
                       from_block: int, to_block: int | None = None,
                       n_points: int = 12, pause: float = 0.05) -> dict | None:
    """Reconstruct an operator cluster's combined holding over a block range.

    Samples the cluster's summed balance at N evenly-spaced blocks via archive
    eth_call. Works on any chain with a configured archive RPC. Returns
    {block_series, balance_series, n_addresses} or None.
    """
    rpc = ArchiveRPC(chain)
    if not rpc.available():
        logger.warning("no_archive_rpc", chain=chain)
        return None
    if to_block is None:
        to_block = rpc.latest_block()
    if to_block <= from_block:
        return None

    # Include the from_block endpoint so a from-zero baseline is captured.
    step = (to_block - from_block) / n_points
    blocks = [int(from_block + step * i) for i in range(0, n_points + 1)]
    balance_series = []
    for blk in blocks:
        total = combined_balance_at(token, addresses, chain, blk, rpc=rpc)
        balance_series.append(round(total, 4))
        if pause:
            time.sleep(pause)
    return {"block_series": blocks, "balance_series": balance_series,
            "n_addresses": len(addresses)}
