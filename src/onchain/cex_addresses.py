"""Known centralized-exchange addresses, by chain.

Used to disambiguate accumulation (CEX→wallet) from distribution (wallet→CEX).
EVM addresses are re-exported from whale_tracker; Solana addresses are the
publicly-known hot/deposit wallets of major exchanges.

This set is intentionally conservative and easy to extend — add verified deposit
addresses as they are identified. Missing addresses only cause under-detection
(a transfer to an unlabeled CEX wallet looks like a wallet→wallet move), never a
false exit signal.
"""

from __future__ import annotations


def evm_exchanges() -> dict[str, str]:
    """EVM exchange addresses (lowercased) → label, from whale_tracker."""
    try:
        from src.collectors.whale_tracker import WhaleTrackerCollector

        return {a.lower(): n for a, n in WhaleTrackerCollector.KNOWN_EXCHANGES.items()}
    except Exception:
        return {}


# Major Solana CEX hot/deposit wallets (publicly labeled on Solana explorers).
# Keep base58 case as-is — Solana addresses are case-sensitive.
SOLANA_EXCHANGES: dict[str, str] = {
    # Binance
    "2ojv9BAiHUrvsm9gxDe7fJSzbNZSJcxZvf8dqmWGHG8S": "Binance",
    "5tzFkiKscXHK5ZXCGbXZxdw7gTjjD1mBwuoFbhUvuAi9": "Binance",
    "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM": "Binance",
    # Coinbase
    "H8sMJSCQxfKiFTCfDR3DUMLPwcRbM61LGFJ8N4dK3WjS": "Coinbase",
    "GJRs4FwHtemZ5ZE9x3FNvJ8TMwitKTh21yxdRPqn7npE": "Coinbase",
    # OKX
    "5VCwKtCXgCJ6kit5FybXjvriW3xELsFDhYrPSqtJNmcD": "OKX",
    # Kraken
    "FWznbcNXWQuHTawe9RxvQ2LdCENssh12dsznf4RiouN5": "Kraken",
    # Bybit
    "AC5RDfQFmDS1deWZos921JfqscXdBy5DBmZdy7eDhKn5": "Bybit",
    # Bitget
    "A77HErqtfN1hLLpvZ9pCtu66FEtM8BveoaKbbMoZ4RiR": "Bitget",
    # Gate.io
    "u6PJ8DtQuPFnfmwHbGFULQ4u4EgjDiyYKjVEsynXq2w": "Gate.io",
}


def solana_exchanges() -> dict[str, str]:
    return dict(SOLANA_EXCHANGES)


def label_for(address: str, chain: str) -> str:
    """Return the exchange label for an address, or 'unknown'."""
    if not address:
        return "unknown"
    if chain in ("solana", "sol"):
        return SOLANA_EXCHANGES.get(address, "unknown")
    return evm_exchanges().get(address.lower(), "unknown")
