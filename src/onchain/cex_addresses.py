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


# BSC CEX hot/deposit wallets — sourced from Dune labels.owner_addresses (verified,
# not guessed) and FILTERED to real exchanges: DEX aggregators (bitget_dex_aggregator)
# and bridges (StarGate) were deliberately excluded — a transfer to a router/bridge is
# a swap/bridge, NOT a CEX deposit, so labeling them CEX would false-fire the dump
# signal. whale_tracker's set is ETH-centric; this lifts cex_flow's recall on BSC,
# our main chain (operator→CEX-deposit is the #1 leading dump signal).
BSC_CEX_SUPPLEMENT: dict[str, str] = {
    "0x4aefa39caeadd662ae31ab0ce7c8c2c9c0a013e8": "Binance",
    "0x25681ab599b4e2ceea31f8b498052c53fc2d74db": "Binance",
    "0xef7fb88f709ac6148c07d070bc71d252e8e13b92": "Binance",
    "0x07b664c8af37eddaa7e3b6030ed1f494975e9dfb": "Binance",
    "0xaba2d404c5c41da5964453a368aff2604ae80a14": "Binance",
    "0x43684d03d81d3a4c70da68febdd61029d426f042": "Binance",
    "0xe7804c37c13166ff0b37f5ae0bb07a3aebb6e245": "Binance",
    "0x34ea4138580435b5a521e460035edb19df1938c1": "Binance",
    "0xc9ebc59a7590e52b0904817f172ac82fc66a530b": "Coinbase",
    "0x829e3c7781a6ac8cd864cd8437a664ec07da75a8": "Coinbase",
    "0xe7178ad747f2c12ab1f8332e61cf6e756815d5c6": "Kraken",
    "0xb604f2d512eaa32e06f1ac40362bc9157ce5da96": "Kraken",
    "0xa861678bee80035114b47615142e9302139a8c32": "Kraken",
    "0xadae2f3b0db76cb3eafe76a8bf99b93f099c140a": "Kraken",
    "0xf72d20ff0972a36b01412cddda0bb1ba1a9d3d93": "Kraken",
    "0xc538f3351e0b8d3ed53402ea8f316898c160ca29": "Kraken",
    "0xfa820671257a3bf42379c7c4deeaf2f05500a3e4": "Kraken",
    "0xa40dfee99e1c85dc97fdc594b16a460717838703": "Kraken",
    "0x00d3c53b1ec47932c25595ba2e53e9db20fc7364": "Kraken",
    "0xe62e39a62672b54928ec3bce10fd0368a628afbc": "Kraken",
    "0xa39fed0345d617370b740d63e0a019a202b04f2e": "Kraken",
    "0xed9b8f05224b881a222ece2e20bd2f4bdb71d0f8": "Kraken",
    "0xb3be595ab898568567d08b3f179443a19e034d50": "Kraken",
    "0xf4dd9bc7ae7ae04502ec85fb9f4ee0463e905b20": "Kraken",
    "0x2c04af9362797bdc4b182e29e0c58440411a4481": "Kraken",
    "0x6a68d4acff1a1dacc80e4ae653543e0d2402803e": "Kraken",
    "0x8af3827a41c26c7f32c81e93bb66e837e0210d5c": "Kraken",
    "0x0162cd2ba40e23378bf0fd41f919e1be075f025f": "MEXC",
    "0xdf90c9b995a3b10a5b8570a47101e6c6a29eb945": "MEXC",
    "0xb86f1061e0d79e8319339d5fdbb187d4e7ad3300": "MEXC",
    "0xc2149f0d56e227e39077bf4d592f6314098f3b29": "MEXC",
    # Unmasked 2026-07-01 (Dune labels.owner_addresses) during the sentinel audit.
    # These four masqueraded as OPERATOR wallets/funders for weeks: 0x6596da8b was
    # "the SIREN/EVAA family root" (it's Gate.io), 0x631fc1ea "family root #2"
    # (Binance), 0x4982085c "the family's cross-token wallet" (MEXC inventory),
    # 0x0d0707 sat inside the SIREN cluster (Gate.io). Keeping them labeled kills
    # that whole class of false entity-links at the classification layer.
    "0x6596da8b65995d5feacff8c2936f0b7a2051b0d0": "Gate.io",
    "0x0d0707963952f2fba59dd06f2b425ace40b492fe": "Gate.io",
    "0x631fc1ea2270e98fbd9d92658ece0f5a269aa161": "Binance",
    "0x4982085c9e2f89f2ecb8131eca71afad896e89cb": "MEXC",
}


def _cached_dune_labels() -> dict[str, str]:
    """Dune-refreshed BSC exchange labels (data/cex_labels_bsc.json, written by the
    weekly scheduler job). Offline-first: one batched Dune pull → local file →
    every classifier gets the full label set with zero latency. Missing/corrupt
    file just means we fall back to the hardcoded supplement."""
    try:
        import json

        from src.config import DATA_DIR
        p = DATA_DIR / "cex_labels_bsc.json"
        if p.exists():
            d = json.loads(p.read_text())
            return {str(k).lower(): str(v) for k, v in d.items() if k and v}
    except Exception:
        pass
    return {}


def _eth_labels_cex() -> dict[str, str]:
    """ETH exchange hot wallets from dawsbot/eth-labels (data/eth_labels_cex.json,
    fetched from src/mainnet/exchange). Broadens the ETH CEX set beyond the BSC-centric
    supplement so a funder that is really a Binance/Coinbase hot wallet gets filtered
    before it manufactures a false operator cluster (the shared-funder falsification
    lesson). Empty dict on any read failure — never zeroes the rest of the set."""
    try:
        import json

        from src.config import DATA_DIR
        p = DATA_DIR / "eth_labels_cex.json"
        if p.exists():
            return {a.lower(): "eth-labels" for a in json.loads(p.read_text()).get("addresses", [])}
    except Exception:
        pass
    return {}


def evm_exchanges() -> dict[str, str]:
    """EVM exchange addresses (lowercased) → label: whale_tracker (ETH-centric) +
    the Dune-sourced BSC supplement + the weekly-refreshed Dune label cache.

    If whale_tracker can't import (it pulls in the async collector stack), we LOG
    rather than swallow: the ETH CEX set silently collapsing to BSC-only would
    quietly zero the #1 dump signal's ETH coverage — exactly the failure≠0 trap.
    We still return the BSC supplement so BSC detection is unaffected."""
    out = dict(BSC_CEX_SUPPLEMENT)
    out.update(_cached_dune_labels())
    out.update(_eth_labels_cex())      # 372 fresh ETH exchange hot wallets (dawsbot/eth-labels)
    try:
        from src.collectors.whale_tracker import WhaleTrackerCollector

        out.update({a.lower(): n for a, n in WhaleTrackerCollector.KNOWN_EXCHANGES.items()})
    except Exception as e:
        try:
            import structlog

            structlog.get_logger().warning(
                "evm_exchanges_eth_set_unavailable",
                error=str(e)[:100],
                note="ETH CEX labels degraded to BSC-only — check env deps")
        except Exception:
            pass
    return out


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
