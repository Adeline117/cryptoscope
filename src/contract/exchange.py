"""Unified exchange interface — supports Binance, Bybit, OKX, Hyperliquid.

Uses ccxt for CEX integration. All methods return standardized dicts.
Bot sends signals + one-click buttons, user confirms execution.

IMPORTANT: This module NEVER auto-executes. All trades require user confirmation
via Telegram inline keyboard callback.
"""

from __future__ import annotations

import os
from typing import Any

import structlog

logger = structlog.get_logger()

# Supported exchanges
EXCHANGES = {
    "binance": {
        "class": "binance",
        "futures": True,
        "max_leverage": 125,
        "env_key": "BINANCE_API_KEY",
        "env_secret": "BINANCE_API_SECRET",
    },
    "bybit": {
        "class": "bybit",
        "futures": True,
        "max_leverage": 100,
        "env_key": "BYBIT_API_KEY",
        "env_secret": "BYBIT_API_SECRET",
    },
    "okx": {
        "class": "okx",
        "futures": True,
        "max_leverage": 100,
        "env_key": "OKX_API_KEY",
        "env_secret": "OKX_API_SECRET",
        "env_passphrase": "OKX_PASSPHRASE",
    },
}

_exchange_instances: dict[str, Any] = {}


def _get_exchange(name: str):
    """Get or create a ccxt exchange instance."""
    if name in _exchange_instances:
        return _exchange_instances[name]

    config = EXCHANGES.get(name)
    if not config:
        raise ValueError(f"Unsupported exchange: {name}")

    api_key = os.environ.get(config["env_key"], "")
    api_secret = os.environ.get(config["env_secret"], "")

    if not api_key or not api_secret:
        raise ValueError(f"{name} API keys not configured. Set {config['env_key']} and {config['env_secret']}")

    try:
        import ccxt
    except ImportError:
        raise ImportError("ccxt not installed. Run: pip install ccxt")

    params = {
        "apiKey": api_key,
        "secret": api_secret,
        "options": {"defaultType": "swap"},  # Perpetual futures
    }

    if name == "okx":
        params["password"] = os.environ.get(config.get("env_passphrase", ""), "")

    exchange_class = getattr(ccxt, config["class"])
    exchange = exchange_class(params)
    exchange.set_sandbox_mode(False)

    _exchange_instances[name] = exchange
    logger.info("exchange_connected", name=name)
    return exchange


def get_balance(exchange_name: str) -> dict:
    """Get account balance from exchange.

    Returns: {total_usd, available_usd, positions_margin, unrealized_pnl}
    """
    try:
        ex = _get_exchange(exchange_name)
        balance = ex.fetch_balance()

        # USDT balance for futures
        usdt = balance.get("USDT", balance.get("total", {}))
        total = float(usdt.get("total", 0) if isinstance(usdt, dict) else 0)
        free = float(usdt.get("free", 0) if isinstance(usdt, dict) else 0)
        used = float(usdt.get("used", 0) if isinstance(usdt, dict) else 0)

        return {
            "exchange": exchange_name,
            "total_usd": round(total, 2),
            "available_usd": round(free, 2),
            "in_positions": round(used, 2),
        }
    except Exception as e:
        logger.error("balance_failed", exchange=exchange_name, error=str(e))
        return {"exchange": exchange_name, "error": str(e)}


def get_positions(exchange_name: str) -> list[dict]:
    """Get all open futures positions.

    Returns list of: {symbol, side, size, entry_price, mark_price, pnl, pnl_pct, leverage, liq_price}
    """
    try:
        ex = _get_exchange(exchange_name)
        positions = ex.fetch_positions()

        result = []
        for p in positions:
            size = float(p.get("contracts", 0) or 0)
            if size == 0:
                continue

            entry = float(p.get("entryPrice", 0) or 0)
            mark = float(p.get("markPrice", 0) or 0)
            pnl = float(p.get("unrealizedPnl", 0) or 0)
            notional = float(p.get("notional", 0) or abs(size * mark))

            pnl_pct = (pnl / (notional / float(p.get("leverage", 1) or 1))) * 100 if notional else 0

            result.append({
                "symbol": p.get("symbol", "?"),
                "side": p.get("side", "?"),
                "size": size,
                "entry_price": entry,
                "mark_price": mark,
                "pnl": round(pnl, 2),
                "pnl_pct": round(pnl_pct, 2),
                "leverage": int(p.get("leverage", 1) or 1),
                "liq_price": float(p.get("liquidationPrice", 0) or 0),
                "margin": round(notional / float(p.get("leverage", 1) or 1), 2),
            })

        return result
    except Exception as e:
        logger.error("positions_failed", exchange=exchange_name, error=str(e))
        return []


def get_ticker(exchange_name: str, symbol: str) -> dict | None:
    """Get current ticker for a symbol."""
    try:
        ex = _get_exchange(exchange_name)
        ticker = ex.fetch_ticker(symbol)
        return {
            "symbol": symbol,
            "last": float(ticker.get("last", 0)),
            "bid": float(ticker.get("bid", 0)),
            "ask": float(ticker.get("ask", 0)),
            "change_24h": float(ticker.get("percentage", 0) or 0),
            "volume_24h": float(ticker.get("quoteVolume", 0) or 0),
        }
    except Exception as e:
        logger.error("ticker_failed", symbol=symbol, error=str(e))
        return None


def set_leverage(exchange_name: str, symbol: str, leverage: int) -> bool:
    """Set leverage for a symbol."""
    try:
        ex = _get_exchange(exchange_name)
        ex.set_leverage(leverage, symbol)
        logger.info("leverage_set", exchange=exchange_name, symbol=symbol, leverage=leverage)
        return True
    except Exception as e:
        logger.error("leverage_set_failed", error=str(e))
        return False


def place_order(
    exchange_name: str,
    symbol: str,
    side: str,  # "buy" or "sell"
    amount: float,  # in contracts/coins
    order_type: str = "market",
    price: float | None = None,
    leverage: int = 1,
    tp_price: float | None = None,
    sl_price: float | None = None,
) -> dict:
    """Place a futures order.

    Returns: {order_id, symbol, side, amount, price, status, tp_order_id, sl_order_id}
    """
    try:
        ex = _get_exchange(exchange_name)

        # Set leverage first
        try:
            ex.set_leverage(leverage, symbol)
        except Exception:
            pass  # Some exchanges don't support per-symbol leverage setting

        # Place main order
        params = {}
        if order_type == "market":
            order = ex.create_order(symbol, "market", side, amount, params=params)
        else:
            if price is None:
                raise ValueError("Limit order requires a price")
            order = ex.create_order(symbol, "limit", side, amount, price, params=params)

        result = {
            "order_id": order.get("id", ""),
            "symbol": symbol,
            "side": side,
            "amount": amount,
            "price": float(order.get("price", 0) or order.get("average", 0) or 0),
            "status": order.get("status", "unknown"),
            "leverage": leverage,
        }

        # Place TP order
        if tp_price:
            try:
                tp_side = "sell" if side == "buy" else "buy"
                tp_order = ex.create_order(
                    symbol, "limit", tp_side, amount, tp_price,
                    params={"reduceOnly": True}
                )
                result["tp_order_id"] = tp_order.get("id", "")
                result["tp_price"] = tp_price
            except Exception as e:
                logger.warning("tp_order_failed", error=str(e))

        # Place SL order
        if sl_price:
            try:
                sl_side = "sell" if side == "buy" else "buy"
                sl_order = ex.create_order(
                    symbol, "stop_market" if hasattr(ex, "create_order") else "market",
                    sl_side, amount, sl_price,
                    params={"reduceOnly": True, "stopPrice": sl_price}
                )
                result["sl_order_id"] = sl_order.get("id", "")
                result["sl_price"] = sl_price
            except Exception as e:
                logger.warning("sl_order_failed", error=str(e))

        logger.info("order_placed", **result)
        return result

    except Exception as e:
        logger.error("order_failed", exchange=exchange_name, symbol=symbol, error=str(e))
        return {"error": str(e)}


def close_position(exchange_name: str, symbol: str, side: str, amount: float) -> dict:
    """Close a position (reduce-only market order)."""
    try:
        ex = _get_exchange(exchange_name)
        close_side = "sell" if side == "long" else "buy"
        order = ex.create_order(
            symbol, "market", close_side, amount,
            params={"reduceOnly": True}
        )
        result = {
            "order_id": order.get("id", ""),
            "symbol": symbol,
            "side": close_side,
            "amount": amount,
            "status": order.get("status", "unknown"),
        }
        logger.info("position_closed", **result)
        return result
    except Exception as e:
        logger.error("close_failed", error=str(e))
        return {"error": str(e)}


def close_all_positions(exchange_name: str) -> list[dict]:
    """Emergency: close ALL open positions."""
    positions = get_positions(exchange_name)
    results = []
    for p in positions:
        r = close_position(exchange_name, p["symbol"], p["side"], p["size"])
        results.append(r)
    return results


def format_balance_message(balance: dict) -> str:
    """Format balance as Telegram HTML."""
    if "error" in balance:
        return f"❌ {balance['exchange']}: {balance['error']}"

    return (
        f"💰 <b>{balance['exchange'].upper()}</b>\n"
        f"  总资产: ${balance['total_usd']:,.2f}\n"
        f"  可用: ${balance['available_usd']:,.2f}\n"
        f"  持仓占用: ${balance['in_positions']:,.2f}"
    )


def format_positions_message(positions: list[dict], exchange_name: str) -> str:
    """Format positions as Telegram HTML."""
    if not positions:
        return f"📊 <b>{exchange_name.upper()}</b>\n\n无持仓"

    lines = [f"⚡ <b>实时持仓 | {exchange_name.upper()}</b>", "━━━━━━━━━━━━━━━━━━━━"]

    for p in positions:
        side_emoji = "🟢" if p["side"] == "long" else "🔴"
        pnl_emoji = "📈" if p["pnl"] >= 0 else "📉"

        lines.append(
            f"{p['symbol']} | {side_emoji} {p['side'].upper()} {p['leverage']}x\n"
            f"开仓: ${p['entry_price']:,.2f} → 现价: ${p['mark_price']:,.2f}\n"
            f"PnL: {pnl_emoji} ${p['pnl']:+,.2f} ({p['pnl_pct']:+.2f}%)\n"
            f"保证金: ${p['margin']:,.2f} | 强平: ${p['liq_price']:,.2f}"
        )
        lines.append("━━━━━━━━━━━━━━━━━━━━")

    return "\n".join(lines)
