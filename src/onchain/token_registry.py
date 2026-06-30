"""Shared non-operator token registry.

Stablecoins / wrapped majors / liquid-staking derivatives / blue-chip protocol
tokens whose holder concentration is CUSTODY or TREASURY — never a 妖币 operator.
Running the accumulation screener or operator-hunt over these produces the
LINK / WBTC / USDT / RLUSD / weETH false positives that flooded the watchlist
(every one fires "effective_concentration / hidden_cluster / smart_money" because
issuer/exchange/protocol wallets hold huge shares).

Single source of truth — replaces the scattered operator_hunt._SKIP_SYMBOLS and
run_historical.EXCLUDE_SYMBOLS. Symbol-based (cheap, pre-fetch); pair with the
on-chain entity classifier (multisig/treasury/CEX) for address-level exclusion.
"""

from __future__ import annotations

STABLECOINS = {
    "USDT", "USDC", "DAI", "TUSD", "FDUSD", "USDE", "USDS", "BUSD", "USDD", "FRAX",
    "LUSD", "GUSD", "USDP", "PYUSD", "RLUSD", "USD1", "USDTB", "AUSD", "CRVUSD",
    "GHO", "USDY", "EURC", "EURS", "EURT", "USDBC", "SUSD", "DOLA", "MIM", "USDX",
    "BUIDL", "USTB", "USD0", "DEUSD", "FDUSDT", "USDL", "USDR", "VUSD",
}
WRAPPED_MAJORS = {
    "WETH", "WBTC", "BTCB", "CBBTC", "UBTC", "WBNB", "WSOL", "WMATIC", "WAVAX",
    "WFTM", "TBTC", "RENBTC", "SBTC", "WBETH", "LBTC", "SOLVBTC", "WEETH",
}
LST = {
    "STETH", "WSTETH", "RETH", "CBETH", "EZETH", "WEETH", "EETH", "SFRXETH",
    "FRXETH", "RSETH", "METH", "ANKRETH", "OSETH", "LSETH", "SWETH", "STMATIC",
    "JITOSOL", "MSOL", "BSOL", "JUPSOL", "INF", "STBNB", "SLISBNB", "ANKRBNB",
}
MAJORS = {
    "ETH", "BTC", "SOL", "BNB", "XRP", "ADA", "DOGE", "LTC", "TRX", "DOT", "MATIC",
    "POL", "AVAX", "LINK", "UNI", "AAVE", "MKR", "LDO", "ARB", "OP", "ATOM", "NEAR",
    "FTM", "ETC", "XAUT", "PAXG", "ETHE", "PRO", "BCH", "XLM", "FIL", "ICP", "VET",
}
NON_OPERATOR_SYMBOLS = STABLECOINS | WRAPPED_MAJORS | LST | MAJORS


def is_non_operator(symbol: str | None) -> bool:
    """True if `symbol` is a stablecoin / wrapped major / LST / blue-chip whose
    concentration is custody/treasury — exclude from operator hunting and the
    accumulation screener. Explicit set only (no loose pattern) so a real meme
    operator named e.g. 'USDUCK' is never wrongly excluded."""
    if not symbol:
        return False
    return symbol.strip().upper() in NON_OPERATOR_SYMBOLS
