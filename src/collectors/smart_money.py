"""Smart money wallet tracker — detect institutional and whale activity.

Monitors known fund, market maker, and whale wallets for:
- New token purchases
- Large transfers
- Exchange deposits / withdrawals
- "Smart money cluster" alerts when multiple tier-1 wallets buy the same token

Ethereum wallets are tracked via Etherscan V2 API.
Wallet list is loaded from config/smart_money_wallets.yaml.
"""

from __future__ import annotations

import os
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import yaml
import structlog

from src.collectors.base import BaseCollector, CollectedItem, CollectionResult
from src.config import CONFIG_DIR

logger = structlog.get_logger()

WALLETS_CONFIG = CONFIG_DIR / "smart_money_wallets.yaml"

# Etherscan V2 unified endpoint
ETHERSCAN_V2_BASE = "https://api.etherscan.io/v2/api"

# Helius Solana RPC (free tier: 50 req/sec)
HELIUS_BASE = "https://api.helius.xyz/v0"

CHAIN_IDS = {
    "ethereum": 1,
    "base": 8453,
    "arbitrum": 42161,
    "optimism": 10,
}

# Known exchange deposit addresses (simplified — extend as needed)
KNOWN_EXCHANGE_ADDRESSES: dict[str, str] = {
    # Placeholder — populate with real exchange hot wallet addresses
    # "0x...": "Binance Hot Wallet",
    # "0x...": "Coinbase Commerce",
}


def _load_wallets(config_path: Path = WALLETS_CONFIG) -> list[dict[str, Any]]:
    """Load wallet list from YAML config."""
    if not config_path.exists():
        logger.warning("smart_money_wallets_config_missing", path=str(config_path))
        return []
    with open(config_path) as f:
        data = yaml.safe_load(f) or {}
    return data.get("wallets", [])


def _load_cluster_settings(config_path: Path = WALLETS_CONFIG) -> dict[str, Any]:
    """Load cluster detection settings from YAML config."""
    if not config_path.exists():
        return {"min_tier1_wallets": 3, "window_hours": 24, "min_tx_value_usd": 1000}
    with open(config_path) as f:
        data = yaml.safe_load(f) or {}
    defaults = {"min_tier1_wallets": 3, "window_hours": 24, "min_tx_value_usd": 1000}
    settings = data.get("cluster_detection", {})
    return {**defaults, **settings}


class SmartMoneyCollector(BaseCollector):
    """Track smart money wallet activity across Ethereum and EVM chains.

    Uses Etherscan V2 API to fetch recent token transactions for each
    tracked wallet, then detects patterns:
    - New token buys
    - Large transfers
    - Exchange deposits / withdrawals
    - Smart money cluster (3+ tier-1 wallets buying same token in 24h)
    """

    source_id = "smart_money"
    source_name = "Smart Money Tracker"
    source_type = "api"

    def __init__(
        self,
        wallets_config: Path | None = None,
        chains: list[str] | None = None,
        lookback_hours: int = 24,
        **kwargs,
    ):
        super().__init__(cache_ttl=300, **kwargs)  # 5-min cache
        self._wallets_config = wallets_config or WALLETS_CONFIG
        self._wallets: list[dict[str, Any]] = []
        self._cluster_settings: dict[str, Any] = {}
        self.chains = chains or ["ethereum", "solana", "base"]
        self.lookback_hours = lookback_hours
        # Rotate through multiple Etherscan keys to avoid rate limits
        self._etherscan_keys = [
            os.environ.get("ETHERSCAN_API_KEY", ""),
            os.environ.get("ETHERSCAN_API_KEY_2", ""),
            os.environ.get("ETHERSCAN_API_KEY_3", ""),
            os.environ.get("ETHERSCAN_API_KEY_4", ""),
            os.environ.get("ETHERSCAN_API_KEY_5", ""),
            os.environ.get("ETHERSCAN_API_KEY_6", ""),
        ]
        self._etherscan_keys = [k for k in self._etherscan_keys if k]
        self._eth_key_idx = 0
        self.api_key = self._etherscan_keys[0] if self._etherscan_keys else ""
        self.helius_key = os.environ.get("HELIUS_API_KEY", "")
        self.alchemy_key = os.environ.get("ALCHEMY_API_KEY", "")
        # Use Alchemy Solana RPC if available (faster than public)
        self.solana_rpc = os.environ.get(
            "SOLANA_RPC_URL",
            f"https://solana-mainnet.g.alchemy.com/v2/{self.alchemy_key}" if self.alchemy_key
            else "https://api.mainnet-beta.solana.com"
        )

    def _get_next_etherscan_key(self) -> str:
        """Rotate through Etherscan API keys."""
        if not self._etherscan_keys:
            return ""
        key = self._etherscan_keys[self._eth_key_idx % len(self._etherscan_keys)]
        self._eth_key_idx += 1
        return key

    def _load_config(self) -> None:
        """Load wallets and cluster settings."""
        self._wallets = _load_wallets(self._wallets_config)
        self._cluster_settings = _load_cluster_settings(self._wallets_config)
        self.log.info(
            "wallets_loaded",
            count=len(self._wallets),
            chains=list({w.get("chain", "ethereum") for w in self._wallets}),
        )

    async def _fetch_token_txs(
        self, chain: str, address: str, offset: int = 50
    ) -> list[dict]:
        """Fetch recent token transactions for an address."""
        if chain == "solana":
            return await self._fetch_solana_txs(address)

        chain_id = CHAIN_IDS.get(chain)
        if not chain_id:
            return []
        try:
            data = await self._fetch_json(
                ETHERSCAN_V2_BASE,
                params={
                    "chainid": chain_id,
                    "module": "account",
                    "action": "tokentx",
                    "address": address,
                    "startblock": 0,
                    "endblock": 99999999,
                    "sort": "desc",
                    "offset": offset,
                    "apikey": self._get_next_etherscan_key(),
                },
                use_cache=True,
            )
            result = data.get("result", [])
            return result if isinstance(result, list) else []
        except Exception as e:
            self.log.warning(
                "etherscan_tokentx_failed", address=address, chain=chain, error=str(e)
            )
            return []

    async def _fetch_solana_txs(self, address: str) -> list[dict]:
        """Fetch recent Solana token transactions via Helius or Solana RPC.

        Returns transactions normalized to the same format as Etherscan for
        consistent downstream processing.
        """
        # Try Helius first (richer data)
        if self.helius_key:
            try:
                url = f"{HELIUS_BASE}/addresses/{address}/transactions"
                data = await self._fetch_json(
                    url,
                    params={"api-key": self.helius_key, "limit": 50},
                    use_cache=True,
                )
                if isinstance(data, list):
                    return self._normalize_helius_txs(data, address)
            except Exception as e:
                self.log.warning("helius_fetch_failed", address=address, error=str(e))

        # Fallback: Solana RPC getSignaturesForAddress + getTransaction
        try:
            import json
            import urllib.request

            # Get recent signatures
            payload = json.dumps({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getSignaturesForAddress",
                "params": [address, {"limit": 30}],
            })
            req = urllib.request.Request(
                self.solana_rpc,
                data=payload.encode(),
                headers={"Content-Type": "application/json"},
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                result = json.loads(resp.read().decode())
                signatures = result.get("result", [])

            normalized: list[dict] = []
            for sig_info in signatures[:20]:
                sig = sig_info.get("signature", "")
                block_time = sig_info.get("blockTime", 0)
                # Basic normalization — full tx parsing requires more work
                normalized.append({
                    "hash": sig,
                    "timeStamp": str(block_time),
                    "from": address,
                    "to": "",
                    "value": "0",
                    "tokenSymbol": "SOL",
                    "tokenName": "Solana",
                    "tokenDecimal": "9",
                    "contractAddress": "",
                    "_chain": "solana",
                    "_sig_info": sig_info,
                })
            return normalized
        except Exception as e:
            self.log.warning("solana_rpc_fetch_failed", address=address, error=str(e))
            return []

    def _normalize_helius_txs(self, txs: list[dict], wallet_address: str) -> list[dict]:
        """Convert Helius enhanced transaction format to Etherscan-compatible format."""
        normalized: list[dict] = []
        for tx in txs:
            timestamp = tx.get("timestamp", 0)
            sig = tx.get("signature", "")
            tx_type = tx.get("type", "")
            source = tx.get("source", "")

            # Extract token transfers
            token_transfers = tx.get("tokenTransfers", [])
            for transfer in token_transfers:
                from_addr = transfer.get("fromUserAccount", "")
                to_addr = transfer.get("toUserAccount", "")
                amount = transfer.get("tokenAmount", 0)
                mint = transfer.get("mint", "")
                # Helius sometimes has token metadata
                symbol = transfer.get("tokenStandard", "SPL")

                normalized.append({
                    "hash": sig,
                    "timeStamp": str(timestamp),
                    "from": from_addr,
                    "to": to_addr,
                    "value": str(int(amount * 1e9)) if amount else "0",
                    "tokenSymbol": symbol,
                    "tokenName": mint[:8] if mint else "Unknown",
                    "tokenDecimal": "9",
                    "contractAddress": mint,
                    "_chain": "solana",
                    "_source": source,
                    "_type": tx_type,
                })

            # Also capture native SOL transfers
            native_transfers = tx.get("nativeTransfers", [])
            for nt in native_transfers:
                from_addr = nt.get("fromUserAccount", "")
                to_addr = nt.get("toUserAccount", "")
                amount = nt.get("amount", 0)
                if amount > 1_000_000_000:  # > 1 SOL
                    normalized.append({
                        "hash": sig,
                        "timeStamp": str(timestamp),
                        "from": from_addr,
                        "to": to_addr,
                        "value": str(amount),
                        "tokenSymbol": "SOL",
                        "tokenName": "Solana",
                        "tokenDecimal": "9",
                        "contractAddress": "So11111111111111111111111111111111111111112",
                        "_chain": "solana",
                    })

        return normalized

    def _classify_transaction(
        self, tx: dict, wallet_address: str, wallet_label: str
    ) -> dict[str, Any]:
        """Classify a token transaction as buy, sell, transfer, or exchange interaction."""
        from_addr = (tx.get("from") or "").lower()
        to_addr = (tx.get("to") or "").lower()
        wallet_lower = wallet_address.lower()

        token_symbol = tx.get("tokenSymbol", "UNKNOWN")
        token_name = tx.get("tokenName", "Unknown Token")
        token_address = tx.get("contractAddress", "")
        decimals = int(tx.get("tokenDecimal", 18) or 18)
        raw_value = int(tx.get("value", 0) or 0)
        value = raw_value / (10**decimals) if decimals > 0 else raw_value

        timestamp = int(tx.get("timeStamp", 0) or 0)
        tx_time = datetime.fromtimestamp(timestamp, tz=timezone.utc) if timestamp else None

        # Classify direction
        is_incoming = to_addr == wallet_lower
        is_outgoing = from_addr == wallet_lower

        # Check exchange interaction
        counterparty = to_addr if is_outgoing else from_addr
        exchange_name = KNOWN_EXCHANGE_ADDRESSES.get(counterparty)

        if exchange_name and is_outgoing:
            action = "exchange_deposit"
        elif exchange_name and is_incoming:
            action = "exchange_withdrawal"
        elif is_incoming:
            action = "token_buy"
        elif is_outgoing:
            action = "token_sell_or_transfer"
        else:
            action = "unknown"

        return {
            "action": action,
            "wallet_address": wallet_address,
            "wallet_label": wallet_label,
            "token_symbol": token_symbol,
            "token_name": token_name,
            "token_address": token_address,
            "value": value,
            "tx_hash": tx.get("hash", ""),
            "tx_time": tx_time,
            "from": from_addr,
            "to": to_addr,
            "exchange_name": exchange_name,
            "block_number": tx.get("blockNumber"),
        }

    def _detect_clusters(
        self, classified_txs: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Detect smart money clusters: 3+ tier-1 wallets buying the same token in window."""
        min_wallets = self._cluster_settings.get("min_tier1_wallets", 3)
        window_hours = self._cluster_settings.get("window_hours", 24)
        cutoff = datetime.now(timezone.utc) - timedelta(hours=window_hours)

        # Group buys by token address
        token_buyers: dict[str, list[dict]] = defaultdict(list)
        tier1_addresses = {
            w["address"].lower()
            for w in self._wallets
            if w.get("tier") == 1
        }

        for tx in classified_txs:
            if tx["action"] != "token_buy":
                continue
            if tx["wallet_address"].lower() not in tier1_addresses:
                continue
            if tx["tx_time"] and tx["tx_time"] < cutoff:
                continue
            token_buyers[tx["token_address"].lower()].append(tx)

        clusters: list[dict[str, Any]] = []
        for token_addr, buys in token_buyers.items():
            unique_wallets = {b["wallet_address"].lower() for b in buys}
            if len(unique_wallets) >= min_wallets:
                labels = []
                for b in buys:
                    if b["wallet_address"].lower() in unique_wallets:
                        labels.append(b["wallet_label"])
                clusters.append({
                    "token_address": token_addr,
                    "token_symbol": buys[0]["token_symbol"],
                    "token_name": buys[0]["token_name"],
                    "wallet_count": len(unique_wallets),
                    "wallet_labels": list(set(labels)),
                    "total_buys": len(buys),
                    "severity": "critical",
                })

        return clusters

    async def _collect(self) -> CollectionResult:
        if not self.api_key:
            self.log.warning("no_etherscan_api_key")
            return CollectionResult(
                source_id=self.source_id,
                source_name=self.source_name,
                source_type=self.source_type,
            )

        self._load_config()
        items: list[CollectedItem] = []
        all_classified: list[dict[str, Any]] = []
        cutoff = datetime.now(timezone.utc) - timedelta(hours=self.lookback_hours)

        # Process each wallet
        for wallet in self._wallets:
            chain = wallet.get("chain", "ethereum")
            if chain not in self.chains:
                continue

            address = wallet["address"]
            label = wallet.get("label", address[:10])
            tier = wallet.get("tier", 3)

            txs = await self._fetch_token_txs(chain, address)

            for tx in txs:
                classified = self._classify_transaction(tx, address, label)

                # Filter by lookback window
                if classified["tx_time"] and classified["tx_time"] < cutoff:
                    continue

                classified["tier"] = tier
                classified["chain"] = chain
                all_classified.append(classified)

                # Create item for each transaction
                action_display = classified["action"].replace("_", " ").title()
                item_id = f"sm_{classified['tx_hash'][:16]}" if classified["tx_hash"] else f"sm_{address[:8]}_{classified['token_address'][:8]}"

                severity = "info"
                if tier == 1 and classified["action"] == "token_buy":
                    severity = "high"
                elif classified["action"] == "exchange_deposit":
                    severity = "medium"

                title = (
                    f"[Smart Money] {label} — {action_display}: "
                    f"{classified['value']:,.2f} {classified['token_symbol']}"
                )

                # Build explorer URL based on chain
                tx_hash = classified["tx_hash"]
                if tx_hash:
                    if chain == "solana":
                        tx_url = f"https://solscan.io/tx/{tx_hash}"
                    elif chain == "base":
                        tx_url = f"https://basescan.org/tx/{tx_hash}"
                    else:
                        tx_url = f"https://etherscan.io/tx/{tx_hash}"
                else:
                    tx_url = ""

                items.append(
                    CollectedItem(
                        id=item_id,
                        title=title,
                        content=f"Tier {tier} wallet {label} ({address[:10]}...) "
                                f"{action_display.lower()} "
                                f"{classified['value']:,.2f} {classified['token_symbol']} "
                                f"on {chain}.",
                        url=tx_url,
                        published_at=classified["tx_time"] or datetime.now(timezone.utc),
                        metadata={
                            "data_type": "smart_money_tx",
                            "severity": severity,
                            **classified,
                        },
                        raw=tx,
                    )
                )

        # Detect clusters
        clusters = self._detect_clusters(all_classified)
        for cluster in clusters:
            cluster_id = f"sm_cluster_{cluster['token_address'][:16]}"
            items.append(
                CollectedItem(
                    id=cluster_id,
                    title=(
                        f"[CLUSTER ALERT] {cluster['wallet_count']} tier-1 wallets "
                        f"buying {cluster['token_symbol']} ({cluster['token_name']})"
                    ),
                    content=(
                        f"Smart money cluster detected: {cluster['wallet_count']} "
                        f"tier-1 wallets bought {cluster['token_symbol']} in the last "
                        f"{self._cluster_settings.get('window_hours', 24)}h. "
                        f"Wallets: {', '.join(cluster['wallet_labels'])}."
                    ),
                    url="",
                    published_at=datetime.now(timezone.utc),
                    metadata={
                        "data_type": "smart_money_cluster",
                        "severity": "critical",
                        **cluster,
                    },
                    raw={},
                )
            )

        # Fire immediate alerts for clusters (don't wait for pipeline)
        if clusters:
            for cluster in clusters:
                try:
                    await self._send_cluster_alert(cluster)
                except Exception as e:
                    self.log.warning("cluster_alert_failed", error=str(e))

        self.log.info(
            "smart_money_scan_complete",
            transactions=len(all_classified),
            items=len(items),
            clusters=len(clusters),
        )
        return CollectionResult(
            source_id=self.source_id,
            source_name=self.source_name,
            source_type=self.source_type,
            items=items,
        )

    async def _send_cluster_alert(self, cluster: dict) -> None:
        """Immediately push a smart money cluster alert to Telegram."""
        wallets_str = ", ".join(cluster["wallet_labels"][:5])
        token = cluster["token_symbol"]
        count = cluster["wallet_count"]

        message = (
            f"🐋 <b>聪明钱聚集信号</b>\n\n"
            f"<b>{count} 个 T1 钱包</b>同时买入 <b>${token}</b>\n"
            f"钱包: {wallets_str}\n"
            f"总买入次数: {cluster['total_buys']}\n\n"
            f"⚠️ 历史数据: 3+ T1钱包15分钟内买同一币 → 52-61%概率5x\n\n"
            f"📍 合约: <code>{cluster['token_address'][:12]}...</code>"
        )

        try:
            from src.distribution.telegram_sender import send_meme_alert
            await send_meme_alert(message)
        except ImportError:
            # Fallback to basic alert
            try:
                from src.distribution.telegram_sender import send_critical_alert
                await send_critical_alert(message)
            except Exception:
                self.log.warning("telegram_not_available_for_cluster_alert")
