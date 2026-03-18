"""EN<->ZH crypto jargon glossary for consistent translations."""

# English term -> preferred Chinese translation
GLOSSARY: dict[str, str] = {
    # Core concepts
    "blockchain": "区块链",
    "smart contract": "智能合约",
    "decentralized": "去中心化",
    "consensus": "共识",
    "validator": "验证者",
    "staking": "质押",
    "unstaking": "解质押",
    "slashing": "罚没",
    "governance": "治理",
    "proposal": "提案",
    "treasury": "国库",

    # DeFi
    "Total Value Locked": "总锁仓量",
    "TVL": "TVL（总锁仓量）",
    "liquidity pool": "流动性池",
    "yield farming": "流动性挖矿",
    "impermanent loss": "无常损失",
    "flash loan": "闪电贷",
    "oracle": "预言机",
    "lending": "借贷",
    "borrowing": "借入",
    "collateral": "抵押品",
    "liquidation": "清算",
    "leverage": "杠杆",

    # Trading
    "funding rate": "资金费率",
    "open interest": "未平仓合约",
    "perpetual": "永续合约",
    "long": "做多",
    "short": "做空",
    "whale": "巨鲸",
    "market maker": "做市商",
    "order book": "订单簿",
    "slippage": "滑点",

    # Infrastructure
    "Layer 2": "二层网络",
    "L2": "L2（二层网络）",
    "rollup": "Rollup",
    "bridge": "跨链桥",
    "cross-chain": "跨链",
    "sharding": "分片",
    "sidechain": "侧链",
    "gas fee": "Gas 费",

    # MEV
    "MEV": "MEV（最大可提取价值）",
    "frontrunning": "抢跑",
    "sandwich attack": "三明治攻击",
    "backrunning": "尾随交易",

    # Security
    "exploit": "漏洞利用",
    "hack": "黑客攻击",
    "rug pull": "跑路",
    "audit": "审计",
    "vulnerability": "漏洞",

    # Stablecoins
    "stablecoin": "稳定币",
    "depeg": "脱锚",
    "peg": "锚定",

    # On-chain analysis
    "on-chain": "链上",
    "whale movement": "巨鲸动向",
    "token flow": "代币流向",
    "wallet": "钱包",
    "address": "地址",
    "transaction": "交易",
    "block": "区块",

    # Other
    "airdrop": "空投",
    "whitepaper": "白皮书",
    "testnet": "测试网",
    "mainnet": "主网",
    "hard fork": "硬分叉",
    "soft fork": "软分叉",
    "token burn": "代币销毁",
    "minting": "铸造",
    "restaking": "再质押",
    "liquid staking": "流动性质押",

    # 2026 additions
    "account abstraction": "账户抽象",
    "intent": "意图",
    "modular blockchain": "模块化区块链",
    "atomic swap": "原子交换",
    "inscription": "铭文",
    "runes": "符文",
    "data availability": "数据可用性",
    "agent framework": "Agent 框架",
    "fully homomorphic encryption": "全同态加密",
}
