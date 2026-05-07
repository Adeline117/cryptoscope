"""Whale transaction tracker using ClankApp API with Etherscan fallback."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

from src.collectors.base import BaseCollector, CollectedItem, CollectionResult


class WhaleTrackerCollector(BaseCollector):
    """Track large cryptocurrency transfers (whale transactions).

    Primary source: ClankApp API for cross-chain whale alerts.
    Fallback: Etherscan API for Ethereum-based whale activity.

    Detects accumulation signals (exchange -> unknown wallet) and
    selling pressure (unknown wallet -> exchange).
    """

    source_id = "whale_tracker"
    source_name = "Whale Transaction Tracker"
    source_type = "api"

    CLANKAPP_URL = "https://clankapp.com/api/v2/transfers"

    # Etherscan fallback for known whale wallets
    ETHERSCAN_BASE_URL = "https://api.etherscan.io/v2/api"

    # Known exchange addresses (lowercase) for labelling
    KNOWN_EXCHANGES: dict[str, str] = {
        "0x28c6c06298d514db089934071355e5743bf21d60": "Binance",
        "0x21a31ee1afc51d94c2efccaa2092ad1028285549": "Binance",
        "0xdfd5293d8e347dfe59e90efd55b2956a1343963d": "Binance",
        "0x56eddb7aa87536c09ccc2793473599fd21a8b17f": "Binance",
        "0xf977814e90da44bfa03b6295a0616a897441acec": "Binance",
        "0x503828976d22510aad0201ac7ec88293211d23da": "Coinbase",
        "0xddfabcdc4d8ffc6d5beaf154f18b778f892a0740": "Coinbase",
        "0x3cd751e6b0078be393132286c442345e68ff0aaa": "Coinbase",
        "0x71660c4005ba85c37ccec55d0c4493e66fe775d3": "Coinbase",
        "0xfbb1b73c4f0bda4f67dca266ce6ef42f520fbb98": "Bittrex",
        "0xe92356bab7625a58bec4bccee5873fa3bae4bb10": "Kraken",
        "0x267be1c1d684f78cb4f6a176c4911b741e4ffdc0": "Kraken",
        "0x2910543af39aba0cd09dbb2d50200b3e800a63d2": "Kraken",
        "0x1b3cb81e51011b549d78bf720b0d924ac763a7c2": "OKX",
        "0x6cc5f688a315f3dc28a7781717a9a798a59fda7b": "OKX",
        "0x40b38765696e3d5d8d9d834d8aad4bb6e418e489": "Robinhood",
    }

    # Minimum transfer value in USD for tracking
    MIN_VALUE_USD = 1_000_000

    def __init__(self, limit: int = 20, **kwargs):
        """Initialize with configurable parameters.

        Args:
            limit: Maximum number of transfers to fetch (default 20).
        """
        super().__init__(cache_ttl=300, **kwargs)  # 5 min cache
        self.limit = limit
        self._clankapp_api_key = os.environ.get("CLANKAPP_API_KEY", "")
        self._etherscan_api_key = os.environ.get("ETHERSCAN_API_KEY", "")

    def _label_address(self, address: str) -> str:
        """Return a human-readable label for known addresses."""
        if not address:
            return "unknown"
        return self.KNOWN_EXCHANGES.get(address.lower(), "unknown")

    def _classify_transfer(self, from_label: str, to_label: str) -> dict[str, Any]:
        """Classify a transfer based on source and destination labels.

        Returns classification info including signal type.
        """
        from_is_exchange = from_label != "unknown"
        to_is_exchange = to_label != "unknown"

        if from_is_exchange and not to_is_exchange:
            return {
                "signal": "accumulation",
                "description": f"Withdrawal from {from_label} to private wallet — possible accumulation",
                "priority": "high",
            }
        elif not from_is_exchange and to_is_exchange:
            return {
                "signal": "selling_pressure",
                "description": f"Deposit to {to_label} from private wallet — possible selling pressure",
                "priority": "high",
            }
        elif from_is_exchange and to_is_exchange:
            return {
                "signal": "exchange_transfer",
                "description": f"Transfer between exchanges: {from_label} -> {to_label}",
                "priority": "medium",
            }
        else:
            return {
                "signal": "wallet_transfer",
                "description": "Transfer between private wallets",
                "priority": "medium",
            }

    async def _collect_from_clankapp(self) -> list[dict[str, Any]]:
        """Try to fetch whale transfers from ClankApp API."""
        params: dict[str, str] = {
            "limit": str(self.limit),
            "min_value": str(self.MIN_VALUE_USD),
        }
        headers: dict[str, str] = {}
        if self._clankapp_api_key:
            headers["Authorization"] = f"Bearer {self._clankapp_api_key}"

        try:
            data = await self._fetch_json(
                self.CLANKAPP_URL,
                params=params,
                headers=headers if headers else None,
                use_cache=True,
            )
            transfers = data if isinstance(data, list) else data.get("data", data.get("transfers", []))
            return transfers
        except Exception as e:
            self.log.warning("clankapp_fetch_failed", error=str(e))
            return []

    async def _collect_from_etherscan(self) -> list[dict[str, Any]]:
        """Fallback: fetch large internal transactions from Etherscan."""
        if not self._etherscan_api_key:
            self.log.warning("etherscan_api_key_missing")
            return []

        transfers: list[dict[str, Any]] = []

        # Query recent large ETH transactions from known whale/exchange addresses
        sample_addresses = list(self.KNOWN_EXCHANGES.keys())[:3]

        for address in sample_addresses:
            try:
                data = await self._fetch_json(
                    self.ETHERSCAN_BASE_URL,
                    params={
                        "chainid": "1",
                        "module": "account",
                        "action": "txlist",
                        "address": address,
                        "startblock": "0",
                        "endblock": "99999999",
                        "page": "1",
                        "offset": "5",
                        "sort": "desc",
                        "apikey": self._etherscan_api_key,
                    },
                    use_cache=True,
                )

                txs = data.get("result", [])
                if not isinstance(txs, list):
                    continue

                for tx in txs:
                    value_wei = int(tx.get("value", "0") or "0")
                    value_eth = value_wei / 1e18

                    # Only include transactions with significant ETH value
                    # (rough filter; price-based filtering would need live price)
                    if value_eth >= 100:
                        transfers.append({
                            "hash": tx.get("hash", ""),
                            "from": tx.get("from", ""),
                            "to": tx.get("to", ""),
                            "value": value_eth,
                            "token": "ETH",
                            "chain": "ethereum",
                            "timestamp": int(tx.get("timeStamp", "0") or "0"),
                            "source": "etherscan",
                        })
            except Exception as e:
                self.log.warning(
                    "etherscan_fetch_failed",
                    address=address[:10],
                    error=str(e),
                )

        return transfers

    async def _collect(self) -> CollectionResult:
        """Fetch whale transactions from available sources."""
        # Try ClankApp first, fall back to Etherscan
        raw_transfers = await self._collect_from_clankapp()

        source_label = "clankapp"
        if not raw_transfers:
            self.log.info("falling_back_to_etherscan")
            raw_transfers = await self._collect_from_etherscan()
            source_label = "etherscan"

        if not raw_transfers:
            self.log.warning("no_whale_transfers_found")
            return CollectionResult(
                source_id=self.source_id,
                source_name=self.source_name,
                source_type=self.source_type,
            )

        items: list[CollectedItem] = []

        for transfer in raw_transfers:
            tx_hash = transfer.get("hash", transfer.get("id", ""))
            from_addr = transfer.get("from", transfer.get("from_address", ""))
            to_addr = transfer.get("to", transfer.get("to_address", ""))
            value = float(transfer.get("value", transfer.get("amount", 0)) or 0)
            usd_value = float(transfer.get("usd_value", transfer.get("value_usd", value)) or value)
            token = transfer.get("token", transfer.get("symbol", transfer.get("currency", "UNKNOWN")))
            chain = transfer.get("chain", transfer.get("blockchain", "ethereum"))

            timestamp = transfer.get("timestamp", transfer.get("time", 0))
            if isinstance(timestamp, (int, float)) and timestamp > 0:
                dt = datetime.fromtimestamp(int(timestamp), tz=timezone.utc)
            else:
                dt = datetime.now(timezone.utc)

            # Label addresses
            from_label = self._label_address(from_addr)
            to_label = self._label_address(to_addr)

            # Classify the transfer
            classification = self._classify_transfer(from_label, to_label)

            # Short address representation
            from_display = from_label if from_label != "unknown" else f"{from_addr[:8]}...{from_addr[-6:]}" if len(from_addr) > 14 else from_addr
            to_display = to_label if to_label != "unknown" else f"{to_addr[:8]}...{to_addr[-6:]}" if len(to_addr) > 14 else to_addr

            metadata: dict[str, Any] = {
                "data_type": "whale_transfer",
                "tx_hash": tx_hash,
                "amount": value,
                "token": token,
                "usd_value": usd_value,
                "from_address": from_addr,
                "to_address": to_addr,
                "from_label": from_label,
                "to_label": to_label,
                "chain": chain,
                "signal": classification["signal"],
                "signal_description": classification["description"],
                "data_source": source_label,
                "priority": classification["priority"],
            }

            # Build content
            content_parts = [
                f"{value:,.2f} {token} (${usd_value:,.0f})" if usd_value != value else f"{value:,.2f} {token}",
                f"From: {from_display} -> To: {to_display}",
                f"Chain: {chain}",
                classification["description"],
            ]

            title = f"Whale Transfer: {value:,.0f} {token} — {from_display} -> {to_display}"

            # Generate a stable item ID
            item_id = tx_hash if tx_hash else f"{from_addr}_{to_addr}_{value}_{timestamp}"

            items.append(
                CollectedItem(
                    id=f"whale_{item_id[:32]}",
                    title=title,
                    content=" | ".join(content_parts),
                    url=f"https://etherscan.io/tx/{tx_hash}" if tx_hash and chain == "ethereum" else "",
                    published_at=dt,
                    metadata=metadata,
                    raw=transfer,
                )
            )

        self.log.info(
            "whale_transfers_collected",
            total=len(items),
            source=source_label,
            accumulation=sum(1 for it in items if it.metadata.get("signal") == "accumulation"),
            selling_pressure=sum(1 for it in items if it.metadata.get("signal") == "selling_pressure"),
        )

        return CollectionResult(
            source_id=self.source_id,
            source_name=self.source_name,
            source_type=self.source_type,
            items=items,
        )
