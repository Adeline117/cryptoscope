"""Real-time liquidity collector: TGA (Treasury) and RRP (NY Fed).

Provides faster updates than FRED for the net liquidity calculation:
    Net Liquidity = Fed Balance Sheet (WALCL from FRED) - TGA - RRP

Data sources:
- TGA: US Treasury Fiscal Data API (daily, same-day updates)
- RRP: NY Fed Markets API (daily, same-day updates)
- Fed BS: FRED WALCL (weekly, used as baseline)
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any

from src.collectors.base import BaseCollector, CollectedItem, CollectionResult

# FRED WALCL is used as the Fed Balance Sheet baseline
FRED_BASE_URL = "https://api.stlouisfed.org/fred"


class RealtimeLiquidityCollector(BaseCollector):
    """Collect real-time TGA and RRP data for net liquidity calculation.

    This collector fetches fresher data than FRED for the two most volatile
    components of the net liquidity equation (TGA and RRP), then combines
    with the latest FRED WALCL value for the Fed Balance Sheet.
    """

    source_id = "realtime_liquidity"
    source_name = "Real-Time Net Liquidity"
    source_type = "api"

    TREASURY_TGA_URL = (
        "https://api.fiscaldata.treasury.gov/services/api/fiscal_service"
        "/v1/accounting/dts/deposits_withdrawals_operating_cash"
    )
    NYFED_RRP_URL = (
        "https://markets.newyorkfed.org/api/rp/reverserepo"
        "/propositions/search.json"
    )

    def __init__(self, **kwargs):
        super().__init__(cache_ttl=1800, **kwargs)  # 30 min cache
        self.fred_api_key = os.environ.get("FRED_API_KEY", "")

    async def _fetch_tga(self) -> dict[str, Any] | None:
        """Fetch latest Treasury General Account balance from Treasury Fiscal Data API."""
        try:
            data = await self._fetch_json(
                self.TREASURY_TGA_URL,
                params={
                    "fields": "record_date,open_today_bal",
                    "sort": "-record_date",
                    "page[size]": "5",
                },
            )

            records = data.get("data", [])
            if not records:
                return None

            latest = records[0]
            balance = float(latest["open_today_bal"])
            record_date = latest["record_date"]

            return {
                "value": balance,
                "date": record_date,
                "source": "treasury_fiscal_data",
            }
        except Exception as e:
            self.log.warning("tga_fetch_error", error=str(e))
            return None

    async def _fetch_rrp(self) -> dict[str, Any] | None:
        """Fetch latest Reverse Repo (ON RRP) data from NY Fed Markets API."""
        try:
            today = datetime.now(timezone.utc)
            start_date = (today - timedelta(days=7)).strftime("%Y-%m-%d")
            end_date = today.strftime("%Y-%m-%d")

            data = await self._fetch_json(
                self.NYFED_RRP_URL,
                params={
                    "startDate": start_date,
                    "endDate": end_date,
                },
            )

            # NY Fed API returns propositions; sum totalAmtAccepted for the latest date
            props = data.get("repo", {}).get("operations", [])
            if not props:
                # Try alternate response structure
                props = data.get("operations", [])

            if not props:
                return None

            # Sort by date descending and take the latest
            props.sort(key=lambda x: x.get("operationDate", ""), reverse=True)
            latest = props[0]

            total_amt = float(latest.get("totalAmtAccepted", 0))
            op_date = latest.get("operationDate", "")

            return {
                "value": total_amt,
                "date": op_date,
                "source": "ny_fed_markets",
            }
        except Exception as e:
            self.log.warning("rrp_fetch_error", error=str(e))
            return None

    async def _fetch_fed_bs(self) -> dict[str, Any] | None:
        """Fetch latest Fed Balance Sheet (WALCL) from FRED as baseline."""
        if not self.fred_api_key:
            return None

        try:
            data = await self._fetch_json(
                f"{FRED_BASE_URL}/series/observations",
                params={
                    "series_id": "WALCL",
                    "api_key": self.fred_api_key,
                    "file_type": "json",
                    "sort_order": "desc",
                    "limit": 2,
                },
            )

            observations = data.get("observations", [])
            if not observations:
                return None

            latest = observations[0]
            value = latest.get("value", ".")
            if value == ".":
                return None

            return {
                "value": float(value),
                "date": latest["date"],
                "source": "fred_walcl",
            }
        except Exception as e:
            self.log.warning("fed_bs_fetch_error", error=str(e))
            return None

    async def _collect(self) -> CollectionResult:
        import asyncio

        items: list[CollectedItem] = []

        # Fetch all three components in parallel
        tga_task = self._fetch_tga()
        rrp_task = self._fetch_rrp()
        fed_bs_task = self._fetch_fed_bs()

        tga_result, rrp_result, fed_bs_result = await asyncio.gather(
            tga_task, rrp_task, fed_bs_task, return_exceptions=True
        )

        # Handle exceptions from gather
        if isinstance(tga_result, Exception):
            self.log.warning("tga_exception", error=str(tga_result))
            tga_result = None
        if isinstance(rrp_result, Exception):
            self.log.warning("rrp_exception", error=str(rrp_result))
            rrp_result = None
        if isinstance(fed_bs_result, Exception):
            self.log.warning("fed_bs_exception", error=str(fed_bs_result))
            fed_bs_result = None

        now = datetime.now(timezone.utc)

        # Emit individual component items
        if tga_result:
            # TGA values from Treasury API are in millions
            tga_val = tga_result["value"]
            items.append(CollectedItem(
                id=f"rt_tga_{tga_result['date']}",
                title=f"TGA (Real-Time): ${tga_val / 1e3:,.1f}B",
                content=(
                    f"Treasury General Account balance: ${tga_val / 1e3:,.1f}B "
                    f"as of {tga_result['date']} (source: Treasury Fiscal Data API)"
                ),
                url="https://fiscaldata.treasury.gov/datasets/daily-treasury-statement/",
                published_at=now,
                metadata={
                    "data_type": "realtime_tga",
                    "category": "liquidity",
                    "value": tga_val,
                    "date": tga_result["date"],
                    "source": tga_result["source"],
                    "priority": "high",
                },
                raw=tga_result,
            ))

        if rrp_result:
            # RRP values from NY Fed are in billions
            rrp_val = rrp_result["value"]
            items.append(CollectedItem(
                id=f"rt_rrp_{rrp_result['date']}",
                title=f"ON RRP (Real-Time): ${rrp_val / 1e9:,.1f}B",
                content=(
                    f"Overnight Reverse Repo: ${rrp_val / 1e9:,.1f}B "
                    f"as of {rrp_result['date']} (source: NY Fed Markets API)"
                ),
                url="https://www.newyorkfed.org/markets/desk-operations/reverse-repo",
                published_at=now,
                metadata={
                    "data_type": "realtime_rrp",
                    "category": "liquidity",
                    "value": rrp_val,
                    "date": rrp_result["date"],
                    "source": rrp_result["source"],
                    "priority": "high",
                },
                raw=rrp_result,
            ))

        # Calculate real-time net liquidity if all three components available
        if tga_result and rrp_result and fed_bs_result:
            # WALCL is in millions, TGA from Treasury is in millions,
            # RRP from NY Fed is in billions -> convert to millions
            fed_bs_val = fed_bs_result["value"]     # millions
            tga_val = tga_result["value"]            # millions
            rrp_val = rrp_result["value"] / 1e3      # billions -> millions

            net_liq = fed_bs_val - tga_val - rrp_val

            items.append(CollectedItem(
                id=f"rt_net_liquidity_{now.strftime('%Y%m%d_%H%M')}",
                title=f"Net Liquidity (Real-Time): ${net_liq / 1e6:,.2f}T",
                content=(
                    f"Real-Time Net Liquidity: ${net_liq / 1e6:,.2f}T | "
                    f"Fed BS: ${fed_bs_val / 1e6:,.2f}T ({fed_bs_result['date']}) | "
                    f"TGA: ${tga_val / 1e3:,.0f}B ({tga_result['date']}) | "
                    f"RRP: ${rrp_result['value'] / 1e9:,.1f}B ({rrp_result['date']})"
                ),
                url="https://fred.stlouisfed.org/series/WALCL",
                published_at=now,
                metadata={
                    "data_type": "realtime_net_liquidity",
                    "category": "liquidity",
                    "value": net_liq,
                    "components": {
                        "fed_bs": fed_bs_val,
                        "fed_bs_date": fed_bs_result["date"],
                        "tga": tga_val,
                        "tga_date": tga_result["date"],
                        "rrp": rrp_val,
                        "rrp_date": rrp_result["date"],
                    },
                    "priority": "critical",
                    "note": "Real-time estimate; Fed BS updates weekly, TGA/RRP update daily",
                },
                raw={
                    "fed_bs": fed_bs_result,
                    "tga": tga_result,
                    "rrp": rrp_result,
                },
            ))

        return CollectionResult(
            source_id=self.source_id,
            source_name=self.source_name,
            source_type=self.source_type,
            items=items,
        )
