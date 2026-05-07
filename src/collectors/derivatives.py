"""Derivatives data collectors: Coinglass, Deribit, Binance Futures."""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

from src.collectors.base import BaseCollector, CollectedItem, CollectionResult

# Symbols supported across all collectors
DEFAULT_SYMBOLS = ["BTC", "ETH", "SOL"]

# Anomaly thresholds
FUNDING_HIGH = 0.001  # 0.1%
FUNDING_LOW = -0.0005  # -0.05%
OI_CHANGE_THRESHOLD = 0.20  # 20%
LIQUIDATION_LARGE = 1_000_000  # $1M


def _flag_anomalies(metadata: dict[str, Any]) -> list[str]:
    """Return a list of anomaly flags based on metadata values."""
    flags: list[str] = []

    funding = metadata.get("funding_rate")
    if funding is not None:
        if funding > FUNDING_HIGH:
            flags.append(f"HIGH_FUNDING_RATE:{funding:.4%}")
        elif funding < FUNDING_LOW:
            flags.append(f"NEGATIVE_FUNDING_RATE:{funding:.4%}")

    oi_change = metadata.get("oi_change_24h_pct")
    if oi_change is not None and abs(oi_change) > OI_CHANGE_THRESHOLD:
        flags.append(f"OI_CHANGE_24H:{oi_change:.1%}")

    liq_usd = metadata.get("liquidation_usd")
    if liq_usd is not None and liq_usd > LIQUIDATION_LARGE:
        flags.append(f"LARGE_LIQUIDATION:${liq_usd:,.0f}")

    return flags


# ---------------------------------------------------------------------------
# 1. Coinglass
# ---------------------------------------------------------------------------


class CoinglassCollector(BaseCollector):
    """Collect derivatives data from Coinglass public API (no key required).

    Endpoints:
    - Open Interest
    - Funding rates
    - Liquidation history
    - Long/Short ratio
    """

    source_id = "coinglass"
    source_name = "Coinglass"
    source_type = "api"

    BASE_URL = "https://open-api.coinglass.com/public/v2"

    def __init__(self, symbols: list[str] | None = None, **kwargs):
        super().__init__(**kwargs)
        self.symbols = symbols or DEFAULT_SYMBOLS

    async def _collect(self) -> CollectionResult:
        items: list[CollectedItem] = []

        for symbol in self.symbols:
            # --- Open Interest ---
            try:
                oi_data = await self._fetch_json(
                    f"{self.BASE_URL}/open_interest",
                    params={"symbol": symbol},
                )
                oi_list = oi_data.get("data", []) if oi_data.get("success") else []
                if isinstance(oi_list, dict):
                    # API may return a single object
                    oi_list = [oi_list]
                for entry in (oi_list if isinstance(oi_list, list) else []):
                    oi_val = entry.get("openInterest") or entry.get("openInterestAmount") or 0
                    oi_change = entry.get("h24Change") or entry.get("openInterestChange24h")
                    oi_pct = None
                    if oi_change is not None and oi_val:
                        try:
                            oi_pct = float(oi_change) / 100.0  # API reports as percentage
                        except (ValueError, TypeError):
                            oi_pct = None
                    exchange = entry.get("exchangeName", "all")
                    meta = {
                        "data_type": "open_interest",
                        "symbol": symbol,
                        "exchange": exchange,
                        "open_interest_usd": oi_val,
                        "oi_change_24h_pct": oi_pct,
                    }
                    meta["anomalies"] = _flag_anomalies(meta)
                    items.append(
                        CollectedItem(
                            id=f"coinglass_oi_{symbol}_{exchange}",
                            title=f"[Coinglass] {symbol} OI ({exchange}): ${oi_val:,.0f}",
                            content="",
                            url=f"https://www.coinglass.com/tv/Bitfinex_{symbol}USD",
                            published_at=datetime.now(timezone.utc),
                            metadata=meta,
                            raw=entry,
                        )
                    )
            except Exception as e:
                self.log.warning("coinglass_oi_failed", symbol=symbol, error=str(e))

            # --- Funding Rates ---
            try:
                funding_data = await self._fetch_json(
                    f"{self.BASE_URL}/funding",
                    params={"symbol": symbol},
                )
                f_list = funding_data.get("data", []) if funding_data.get("success") else []
                if isinstance(f_list, dict):
                    f_list = [f_list]
                for entry in (f_list if isinstance(f_list, list) else []):
                    rate = entry.get("rate") or entry.get("fundingRate") or 0
                    try:
                        rate = float(rate)
                    except (ValueError, TypeError):
                        rate = 0.0
                    exchange = entry.get("exchangeName", "all")
                    meta = {
                        "data_type": "funding_rate",
                        "symbol": symbol,
                        "exchange": exchange,
                        "funding_rate": rate,
                    }
                    meta["anomalies"] = _flag_anomalies(meta)
                    items.append(
                        CollectedItem(
                            id=f"coinglass_funding_{symbol}_{exchange}",
                            title=f"[Coinglass] {symbol} Funding ({exchange}): {rate:.4%}",
                            content="",
                            url="https://www.coinglass.com/FundingRate",
                            published_at=datetime.now(timezone.utc),
                            metadata=meta,
                            raw=entry,
                        )
                    )
            except Exception as e:
                self.log.warning("coinglass_funding_failed", symbol=symbol, error=str(e))

            # --- Liquidation History ---
            try:
                liq_data = await self._fetch_json(
                    f"{self.BASE_URL}/liquidation_history",
                    params={"symbol": symbol, "timeType": "1"},
                )
                l_list = liq_data.get("data", []) if liq_data.get("success") else []
                if isinstance(l_list, dict):
                    l_list = [l_list]
                for entry in (l_list if isinstance(l_list, list) else []):
                    long_liq = entry.get("longLiquidationUsd") or entry.get("buyVolUsd") or 0
                    short_liq = entry.get("shortLiquidationUsd") or entry.get("sellVolUsd") or 0
                    total_liq = long_liq + short_liq
                    ts = entry.get("t") or entry.get("createTime")
                    meta = {
                        "data_type": "liquidation",
                        "symbol": symbol,
                        "long_liquidation_usd": long_liq,
                        "short_liquidation_usd": short_liq,
                        "total_liquidation_usd": total_liq,
                        "liquidation_usd": total_liq,
                        "timestamp": ts,
                    }
                    meta["anomalies"] = _flag_anomalies(meta)
                    items.append(
                        CollectedItem(
                            id=f"coinglass_liq_{symbol}_{ts}",
                            title=f"[Coinglass] {symbol} Liquidations: ${total_liq:,.0f} (L:${long_liq:,.0f} / S:${short_liq:,.0f})",
                            content="",
                            url="https://www.coinglass.com/LiquidationData",
                            published_at=datetime.now(timezone.utc),
                            metadata=meta,
                            raw=entry,
                        )
                    )
            except Exception as e:
                self.log.warning("coinglass_liq_failed", symbol=symbol, error=str(e))

            # --- Long/Short Ratio ---
            try:
                ls_data = await self._fetch_json(
                    f"{self.BASE_URL}/long_short_ratio",
                    params={"symbol": symbol, "timeType": "1"},
                )
                ls_list = ls_data.get("data", []) if ls_data.get("success") else []
                if isinstance(ls_list, dict):
                    ls_list = [ls_list]
                for entry in (ls_list if isinstance(ls_list, list) else []):
                    long_rate = entry.get("longRate") or entry.get("longRatio") or 0
                    short_rate = entry.get("shortRate") or entry.get("shortRatio") or 0
                    ls_ratio = entry.get("longShortRatio") or 0
                    exchange = entry.get("exchangeName", "all")
                    meta = {
                        "data_type": "long_short_ratio",
                        "symbol": symbol,
                        "exchange": exchange,
                        "long_rate": long_rate,
                        "short_rate": short_rate,
                        "long_short_ratio": ls_ratio,
                    }
                    items.append(
                        CollectedItem(
                            id=f"coinglass_ls_{symbol}_{exchange}",
                            title=f"[Coinglass] {symbol} L/S Ratio ({exchange}): {ls_ratio} (L:{long_rate}% / S:{short_rate}%)",
                            content="",
                            url="https://www.coinglass.com/LongShortRatio",
                            published_at=datetime.now(timezone.utc),
                            metadata=meta,
                            raw=entry,
                        )
                    )
            except Exception as e:
                self.log.warning("coinglass_ls_failed", symbol=symbol, error=str(e))

        return CollectionResult(
            source_id=self.source_id,
            source_name=self.source_name,
            source_type=self.source_type,
            items=items,
        )


# ---------------------------------------------------------------------------
# 2. Deribit
# ---------------------------------------------------------------------------


class DeribitCollector(BaseCollector):
    """Collect options and volatility data from Deribit public API (no key required).

    Endpoints:
    - Options book summary (by currency)
    - DVOL (volatility index)
    - Active instruments (for expiry analysis)

    Computed metrics:
    - Put/Call ratio (by OI and volume)
    - Max Pain price
    - ATM implied volatility
    - 25-delta risk reversal
    """

    source_id = "deribit"
    source_name = "Deribit"
    source_type = "api"

    BASE_URL = "https://www.deribit.com/api/v2/public"

    def __init__(self, currencies: list[str] | None = None, **kwargs):
        super().__init__(**kwargs)
        self.currencies = currencies or DEFAULT_SYMBOLS

    @staticmethod
    def _parse_instrument_name(name: str) -> dict[str, Any]:
        """Parse Deribit instrument name like 'BTC-28MAR25-90000-C'.

        Returns dict with currency, expiry_str, strike, option_type.
        """
        parts = name.split("-")
        if len(parts) < 4:
            return {}
        return {
            "currency": parts[0],
            "expiry_str": parts[1],
            "strike": float(parts[2]),
            "option_type": parts[3],  # "C" or "P"
        }

    @staticmethod
    def _compute_max_pain(options: list[dict[str, Any]]) -> float | None:
        """Compute max pain: strike where total value of expiring options is minimized.

        For each candidate strike price, sum up the intrinsic value losses
        for all option holders (calls and puts). The strike with the minimum
        total loss is max pain.
        """
        strikes: dict[float, dict[str, float]] = {}  # strike -> {call_oi, put_oi}
        for opt in options:
            parsed = DeribitCollector._parse_instrument_name(
                opt.get("instrument_name", "")
            )
            if not parsed:
                continue
            strike = parsed["strike"]
            oi = opt.get("open_interest", 0) or 0
            if strike not in strikes:
                strikes[strike] = {"call_oi": 0.0, "put_oi": 0.0}
            if parsed["option_type"] == "C":
                strikes[strike]["call_oi"] += oi
            else:
                strikes[strike]["put_oi"] += oi

        if not strikes:
            return None

        sorted_strikes = sorted(strikes.keys())
        min_pain = float("inf")
        max_pain_strike = sorted_strikes[0]

        for candidate in sorted_strikes:
            total_pain = 0.0
            for strike, oi_data in strikes.items():
                # Call holders lose when price < strike
                if candidate > strike:
                    total_pain += (candidate - strike) * oi_data["call_oi"]
                # Put holders lose when price > strike
                if candidate < strike:
                    total_pain += (strike - candidate) * oi_data["put_oi"]
            if total_pain < min_pain:
                min_pain = total_pain
                max_pain_strike = candidate

        return max_pain_strike

    @staticmethod
    def _compute_put_call_ratio(
        options: list[dict[str, Any]],
    ) -> dict[str, float | None]:
        """Compute put/call ratio by OI and volume."""
        call_oi = 0.0
        put_oi = 0.0
        call_vol = 0.0
        put_vol = 0.0

        for opt in options:
            parsed = DeribitCollector._parse_instrument_name(
                opt.get("instrument_name", "")
            )
            if not parsed:
                continue
            oi = opt.get("open_interest", 0) or 0
            vol = opt.get("volume", 0) or 0
            if parsed["option_type"] == "C":
                call_oi += oi
                call_vol += vol
            else:
                put_oi += oi
                put_vol += vol

        return {
            "pc_ratio_oi": put_oi / call_oi if call_oi > 0 else None,
            "pc_ratio_volume": put_vol / call_vol if call_vol > 0 else None,
            "total_call_oi": call_oi,
            "total_put_oi": put_oi,
            "total_call_volume": call_vol,
            "total_put_volume": put_vol,
        }

    @staticmethod
    def _compute_atm_iv(
        options: list[dict[str, Any]], underlying_price: float
    ) -> float | None:
        """Find the ATM implied volatility (closest strike to underlying)."""
        best_diff = float("inf")
        atm_iv = None
        for opt in options:
            parsed = DeribitCollector._parse_instrument_name(
                opt.get("instrument_name", "")
            )
            if not parsed:
                continue
            # Prefer calls for ATM IV
            if parsed["option_type"] != "C":
                continue
            diff = abs(parsed["strike"] - underlying_price)
            iv = opt.get("mark_iv") or opt.get("interest_rate")
            if diff < best_diff and iv is not None:
                best_diff = diff
                atm_iv = iv
        return atm_iv

    @staticmethod
    def _compute_risk_reversal_25d(options: list[dict[str, Any]]) -> float | None:
        """Compute 25-delta risk reversal (25d call IV - 25d put IV).

        Approximation: pick options with delta closest to 0.25 / -0.25
        from the book summary (which may not have greeks). Uses mark_iv
        of the nearest-to-25-delta call minus the nearest-to-25-delta put.

        If greeks are unavailable, we approximate by selecting OTM options at
        roughly the 25th percentile strike distance from ATM.
        """
        calls: list[tuple[float, float]] = []  # (strike, mark_iv)
        puts: list[tuple[float, float]] = []

        for opt in options:
            parsed = DeribitCollector._parse_instrument_name(
                opt.get("instrument_name", "")
            )
            if not parsed:
                continue
            iv = opt.get("mark_iv")
            if iv is None:
                continue
            strike = parsed["strike"]
            if parsed["option_type"] == "C":
                calls.append((strike, iv))
            else:
                puts.append((strike, iv))

        if not calls or not puts:
            return None

        calls.sort(key=lambda x: x[0])
        puts.sort(key=lambda x: x[0], reverse=True)

        # 25-delta call is roughly at the 75th percentile strike (OTM call)
        idx_call = int(len(calls) * 0.75)
        idx_call = min(idx_call, len(calls) - 1)
        # 25-delta put is roughly at the 25th percentile from top (OTM put)
        idx_put = int(len(puts) * 0.75)
        idx_put = min(idx_put, len(puts) - 1)

        call_iv = calls[idx_call][1]
        put_iv = puts[idx_put][1]
        return call_iv - put_iv

    async def _collect(self) -> CollectionResult:
        items: list[CollectedItem] = []
        now_ms = int(time.time() * 1000)
        day_ago_ms = now_ms - 24 * 3600 * 1000

        for currency in self.currencies:
            # --- Options Book Summary ---
            try:
                book_data = await self._fetch_json(
                    f"{self.BASE_URL}/get_book_summary_by_currency",
                    params={"currency": currency, "kind": "option"},
                )
                options = book_data.get("result", [])
                if not isinstance(options, list):
                    options = []

                # Underlying price from first option
                underlying_price = 0.0
                for opt in options:
                    up = opt.get("underlying_price")
                    if up:
                        underlying_price = float(up)
                        break

                # Compute analytics
                pc_ratios = self._compute_put_call_ratio(options)
                max_pain = self._compute_max_pain(options)
                atm_iv = self._compute_atm_iv(options, underlying_price) if underlying_price else None
                risk_reversal = self._compute_risk_reversal_25d(options)

                meta = {
                    "data_type": "options_summary",
                    "currency": currency,
                    "underlying_price": underlying_price,
                    "total_options": len(options),
                    "put_call_ratio_oi": pc_ratios["pc_ratio_oi"],
                    "put_call_ratio_volume": pc_ratios["pc_ratio_volume"],
                    "total_call_oi": pc_ratios["total_call_oi"],
                    "total_put_oi": pc_ratios["total_put_oi"],
                    "total_call_volume": pc_ratios["total_call_volume"],
                    "total_put_volume": pc_ratios["total_put_volume"],
                    "max_pain": max_pain,
                    "atm_iv": atm_iv,
                    "risk_reversal_25d": risk_reversal,
                }
                pc_str = f"{pc_ratios['pc_ratio_oi']:.2f}" if pc_ratios["pc_ratio_oi"] else "N/A"
                mp_str = f"${max_pain:,.0f}" if max_pain else "N/A"
                iv_str = f"{atm_iv:.1f}%" if atm_iv else "N/A"

                items.append(
                    CollectedItem(
                        id=f"deribit_options_{currency}",
                        title=(
                            f"[Deribit] {currency} Options — P/C: {pc_str}, "
                            f"MaxPain: {mp_str}, ATM IV: {iv_str}"
                        ),
                        content="",
                        url=f"https://www.deribit.com/options/{currency}",
                        published_at=datetime.now(timezone.utc),
                        metadata=meta,
                        raw={"option_count": len(options), "sample": options[:5]},
                    )
                )
            except Exception as e:
                self.log.warning("deribit_options_failed", currency=currency, error=str(e))

            # --- DVOL (Volatility Index) ---
            try:
                dvol_data = await self._fetch_json(
                    f"{self.BASE_URL}/get_volatility_index_data",
                    params={
                        "currency": currency,
                        "resolution": 3600,
                        "start_timestamp": day_ago_ms,
                        "end_timestamp": now_ms,
                    },
                )
                dvol_result = dvol_data.get("result", {})
                dvol_points = dvol_result.get("data", [])
                if isinstance(dvol_points, list) and dvol_points:
                    # Each point: [timestamp, open, high, low, close]
                    latest = dvol_points[-1]
                    if isinstance(latest, list) and len(latest) >= 5:
                        dvol_close = latest[4]
                        dvol_high = max(p[2] for p in dvol_points if isinstance(p, list) and len(p) >= 5)
                        dvol_low = min(p[3] for p in dvol_points if isinstance(p, list) and len(p) >= 5)
                        dvol_open = dvol_points[0][1] if isinstance(dvol_points[0], list) and len(dvol_points[0]) >= 5 else None

                        meta = {
                            "data_type": "dvol",
                            "currency": currency,
                            "dvol_current": dvol_close,
                            "dvol_24h_high": dvol_high,
                            "dvol_24h_low": dvol_low,
                            "dvol_24h_open": dvol_open,
                            "dvol_24h_change": (dvol_close - dvol_open) if dvol_open else None,
                            "data_points": len(dvol_points),
                        }
                        items.append(
                            CollectedItem(
                                id=f"deribit_dvol_{currency}",
                                title=f"[Deribit] {currency} DVOL: {dvol_close:.1f} (24h: {dvol_low:.1f}-{dvol_high:.1f})",
                                content="",
                                url=f"https://www.deribit.com/statistics/{currency}/volatility-index",
                                published_at=datetime.now(timezone.utc),
                                metadata=meta,
                                raw={"latest_point": latest, "total_points": len(dvol_points)},
                            )
                        )
            except Exception as e:
                self.log.warning("deribit_dvol_failed", currency=currency, error=str(e))

            # --- Active Instruments (expiry analysis) ---
            try:
                instr_data = await self._fetch_json(
                    f"{self.BASE_URL}/get_instruments",
                    params={"currency": currency, "kind": "option", "expired": "false"},
                )
                instruments = instr_data.get("result", [])
                if not isinstance(instruments, list):
                    instruments = []

                # Group by expiry
                expiry_groups: dict[str, dict[str, int]] = {}  # expiry -> {calls, puts}
                for instr in instruments:
                    parsed = self._parse_instrument_name(instr.get("instrument_name", ""))
                    if not parsed:
                        continue
                    exp = parsed["expiry_str"]
                    if exp not in expiry_groups:
                        expiry_groups[exp] = {"calls": 0, "puts": 0}
                    if parsed["option_type"] == "C":
                        expiry_groups[exp]["calls"] += 1
                    else:
                        expiry_groups[exp]["puts"] += 1

                # Sort by expiry date string (Deribit format: DDMMMYY)
                sorted_expiries = sorted(expiry_groups.keys())

                meta = {
                    "data_type": "option_expiries",
                    "currency": currency,
                    "total_instruments": len(instruments),
                    "expiry_count": len(expiry_groups),
                    "expiries": {
                        exp: expiry_groups[exp] for exp in sorted_expiries
                    },
                    "nearest_expiry": sorted_expiries[0] if sorted_expiries else None,
                }
                items.append(
                    CollectedItem(
                        id=f"deribit_expiries_{currency}",
                        title=f"[Deribit] {currency} Option Expiries: {len(expiry_groups)} dates, {len(instruments)} instruments",
                        content="",
                        url=f"https://www.deribit.com/options/{currency}",
                        published_at=datetime.now(timezone.utc),
                        metadata=meta,
                        raw={"expiry_groups": expiry_groups},
                    )
                )
            except Exception as e:
                self.log.warning("deribit_instruments_failed", currency=currency, error=str(e))

        return CollectionResult(
            source_id=self.source_id,
            source_name=self.source_name,
            source_type=self.source_type,
            items=items,
        )


# ---------------------------------------------------------------------------
# 3. Binance Futures
# ---------------------------------------------------------------------------


class BinanceFuturesCollector(BaseCollector):
    """Collect futures data from Binance public API (no key required).

    Endpoints:
    - Open Interest
    - Funding Rate
    - Top Trader Long/Short Ratio
    - Force Orders (Liquidations)
    """

    source_id = "binance_futures"
    source_name = "Binance Futures"
    source_type = "api"

    BASE_URL = "https://fapi.binance.com"

    # Map generic symbols to Binance USDT futures pairs
    SYMBOL_MAP = {
        "BTC": "BTCUSDT",
        "ETH": "ETHUSDT",
        "SOL": "SOLUSDT",
    }

    def __init__(self, symbols: list[str] | None = None, **kwargs):
        super().__init__(**kwargs)
        self.symbols = symbols or DEFAULT_SYMBOLS

    def _to_pair(self, symbol: str) -> str:
        return self.SYMBOL_MAP.get(symbol, f"{symbol}USDT")

    async def _collect(self) -> CollectionResult:
        items: list[CollectedItem] = []

        for symbol in self.symbols:
            pair = self._to_pair(symbol)

            # --- Open Interest ---
            try:
                oi_data = await self._fetch_json(
                    f"{self.BASE_URL}/fapi/v1/openInterest",
                    params={"symbol": pair},
                    use_cache=False,
                )
                oi_val = float(oi_data.get("openInterest", 0))
                meta = {
                    "data_type": "open_interest",
                    "symbol": symbol,
                    "pair": pair,
                    "exchange": "binance",
                    "open_interest": oi_val,
                    "open_interest_unit": "contracts",
                }
                items.append(
                    CollectedItem(
                        id=f"binance_oi_{pair}",
                        title=f"[Binance] {symbol} Open Interest: {oi_val:,.2f}",
                        content="",
                        url=f"https://www.binance.com/en/futures/{pair}",
                        published_at=datetime.now(timezone.utc),
                        metadata=meta,
                        raw=oi_data,
                    )
                )
            except Exception as e:
                self.log.warning("binance_oi_failed", symbol=symbol, error=str(e))

            # --- Funding Rate ---
            try:
                funding_data = await self._fetch_json(
                    f"{self.BASE_URL}/fapi/v1/fundingRate",
                    params={"symbol": pair, "limit": 3},
                    use_cache=False,
                )
                if isinstance(funding_data, list):
                    for entry in funding_data:
                        rate = float(entry.get("fundingRate", 0))
                        fund_time = entry.get("fundingTime", 0)
                        ts_str = (
                            datetime.fromtimestamp(fund_time / 1000, tz=timezone.utc).isoformat()
                            if fund_time
                            else ""
                        )
                        meta = {
                            "data_type": "funding_rate",
                            "symbol": symbol,
                            "pair": pair,
                            "exchange": "binance",
                            "funding_rate": rate,
                            "funding_time": ts_str,
                        }
                        meta["anomalies"] = _flag_anomalies(meta)
                        items.append(
                            CollectedItem(
                                id=f"binance_funding_{pair}_{fund_time}",
                                title=f"[Binance] {symbol} Funding: {rate:.4%} ({ts_str})",
                                content="",
                                url=f"https://www.binance.com/en/futures/funding-history/perpetual/funding-fee-history",
                                published_at=datetime.now(timezone.utc),
                                metadata=meta,
                                raw=entry,
                            )
                        )
            except Exception as e:
                self.log.warning("binance_funding_failed", symbol=symbol, error=str(e))

            # --- Top Trader Long/Short Ratio ---
            try:
                ls_data = await self._fetch_json(
                    f"{self.BASE_URL}/futures/data/topLongShortAccountRatio",
                    params={"symbol": pair, "period": "1h", "limit": 5},
                    use_cache=False,
                )
                if isinstance(ls_data, list):
                    for entry in ls_data:
                        long_pct = float(entry.get("longAccount", 0))
                        short_pct = float(entry.get("shortAccount", 0))
                        ratio = float(entry.get("longShortRatio", 0))
                        ts = entry.get("timestamp", 0)
                        ts_str = (
                            datetime.fromtimestamp(ts / 1000, tz=timezone.utc).isoformat()
                            if ts
                            else ""
                        )
                        meta = {
                            "data_type": "top_trader_long_short",
                            "symbol": symbol,
                            "pair": pair,
                            "exchange": "binance",
                            "long_account_pct": long_pct,
                            "short_account_pct": short_pct,
                            "long_short_ratio": ratio,
                            "timestamp": ts_str,
                        }
                        items.append(
                            CollectedItem(
                                id=f"binance_ls_{pair}_{ts}",
                                title=f"[Binance] {symbol} Top Trader L/S: {ratio:.4f} (L:{long_pct:.2%} S:{short_pct:.2%})",
                                content="",
                                url=f"https://www.binance.com/en/futures/{pair}",
                                published_at=datetime.now(timezone.utc),
                                metadata=meta,
                                raw=entry,
                            )
                        )
            except Exception as e:
                self.log.warning("binance_ls_failed", symbol=symbol, error=str(e))

            # --- Force Orders (Liquidations) ---
            try:
                liq_data = await self._fetch_json(
                    f"{self.BASE_URL}/fapi/v1/allForceOrders",
                    params={"symbol": pair, "limit": 50},
                    use_cache=False,
                )
                if isinstance(liq_data, list):
                    for entry in liq_data:
                        price = float(entry.get("price", 0))
                        qty = float(entry.get("origQty", 0))
                        usd_value = price * qty
                        side = entry.get("side", "")
                        order_time = entry.get("time", 0)
                        ts_str = (
                            datetime.fromtimestamp(order_time / 1000, tz=timezone.utc).isoformat()
                            if order_time
                            else ""
                        )
                        meta = {
                            "data_type": "liquidation",
                            "symbol": symbol,
                            "pair": pair,
                            "exchange": "binance",
                            "side": side,
                            "price": price,
                            "quantity": qty,
                            "liquidation_usd": usd_value,
                            "status": entry.get("status"),
                            "time_in_force": entry.get("timeInForce"),
                            "timestamp": ts_str,
                        }
                        meta["anomalies"] = _flag_anomalies(meta)
                        direction = "LONG" if side == "SELL" else "SHORT"
                        items.append(
                            CollectedItem(
                                id=f"binance_liq_{pair}_{order_time}",
                                title=f"[Binance] {symbol} Liquidation: {direction} ${usd_value:,.0f} @ ${price:,.2f}",
                                content="",
                                url=f"https://www.binance.com/en/futures/{pair}",
                                published_at=datetime.now(timezone.utc),
                                metadata=meta,
                                raw=entry,
                            )
                        )
            except Exception as e:
                self.log.warning("binance_liq_failed", symbol=symbol, error=str(e))

        return CollectionResult(
            source_id=self.source_id,
            source_name=self.source_name,
            source_type=self.source_type,
            items=items,
        )
