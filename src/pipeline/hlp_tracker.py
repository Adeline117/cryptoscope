"""HLP lane — track the Hyperliquidity Provider vault as a passive positive-EV
source (deposit as the market's counterparty), never as a return promise.

This lane exists because the repo's only individually-reproducible positive-EV
sources are funding carry (falsified as a spike-chaser), defense, and HLP — and
HLP was the one with zero code. It reads the public Hyperliquid vaultDetails API
(free) and projects the honest money question: historical annualized return and,
more importantly, max drawdown — the number that decides whether to deposit.

Fail-closed: any network/parse defect yields available=False with a reason, never
a fabricated metric. Drawdown is computed on the API's own coarse pnlHistory and
UNDERSTATES intraday events (e.g. the JELLY incident); this is disclosed, not hidden.
"""
from __future__ import annotations

import json
import urllib.request
from datetime import datetime, timezone

from src.config import DATA_DIR

# The canonical Hyperliquidity Provider vault. A leader mismatch or a different
# address must never be silently accepted as "HLP".
HLP_VAULT_ADDRESS = "0xdfc24b077bc1425ad1dea75bcb6f8158e10df303"
HLP_INFO_URL = "https://api.hyperliquid.xyz/info"
STATE_FILE = DATA_DIR / "hlp_state.json"
# Only windows we can annualize honestly. "day" is too short to annualize and
# the perp* windows double-count the spot book, so they are deliberately excluded.
REPORTED_WINDOWS = ("week", "month", "allTime")
DRAWDOWN_BASIS = "coarse_pnl_history_understates_intraday"
DISCLAIMER = (
    "被动做市对手盘的历史表现,不是收益承诺;最大回撤在 API 粗粒度 pnl 上计算,"
    "低估 JELLY 类日内事件;非投资建议。"
)


class HlpUnavailable(Exception):
    """The vault projection cannot be trusted this cycle; fail closed."""


def _finite(value: object, *, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        raise HlpUnavailable(f"{field} is not numeric")
    try:
        number = float(value)
    except (TypeError, ValueError):
        raise HlpUnavailable(f"{field} is not numeric")
    if number != number or number in (float("inf"), float("-inf")):
        raise HlpUnavailable(f"{field} is not finite")
    return number


def _series(rows: object, *, field: str) -> list[tuple[int, float]]:
    if not isinstance(rows, list) or len(rows) < 2:
        raise HlpUnavailable(f"{field} needs at least two points")
    out: list[tuple[int, float]] = []
    previous_ms = None
    for row in rows:
        if not isinstance(row, (list, tuple)) or len(row) != 2:
            raise HlpUnavailable(f"{field} point is malformed")
        ms = _finite(row[0], field=f"{field} timestamp")
        if ms < 0 or (previous_ms is not None and ms < previous_ms):
            raise HlpUnavailable(f"{field} timestamps are not ascending")
        previous_ms = ms
        out.append((int(ms), _finite(row[1], field=f"{field} value")))
    return out


def _window_metrics(window: dict) -> dict:
    """Annualized return + peak-to-trough drawdown for one portfolio window."""
    if not isinstance(window, dict):
        raise HlpUnavailable("portfolio window is not an object")
    account = _series(window.get("accountValueHistory"), field="accountValueHistory")
    pnl = _series(window.get("pnlHistory"), field="pnlHistory")
    span_days = (pnl[-1][0] - pnl[0][0]) / 86_400_000
    if span_days <= 0:
        raise HlpUnavailable("window spans no time")
    total_pnl = pnl[-1][1] - pnl[0][1]
    avg_tvl = sum(value for _, value in account) / len(account)
    if avg_tvl <= 0:
        raise HlpUnavailable("window average TVL is not positive")
    # Peak-to-trough of cumulative PnL: the depositor's worst observed decline
    # from a prior high, on the coarse series (disclosed as understating intraday).
    peak = pnl[0][1]
    max_drawdown = 0.0
    for _, value in pnl:
        peak = max(peak, value)
        max_drawdown = min(max_drawdown, value - peak)
    return {
        "span_days": round(span_days, 2),
        "pnl_usd": round(total_pnl, 2),
        "avg_tvl_usd": round(avg_tvl, 2),
        "annualized_pct": round((total_pnl / avg_tvl) * (365 / span_days) * 100, 2),
        "max_drawdown_usd": round(max_drawdown, 2),
        "max_drawdown_pct_of_avg_tvl": round(max_drawdown / avg_tvl * 100, 2),
    }


def compute_hlp_state(details: object, *, now: datetime | None = None) -> dict:
    """Pure projection: turn a vaultDetails payload into the fail-closed money view."""
    generated_at = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    stamp = generated_at.isoformat()
    try:
        if not isinstance(details, dict):
            raise HlpUnavailable("vaultDetails is not an object")
        address = str(details.get("vaultAddress", "")).lower()
        if address != HLP_VAULT_ADDRESS:
            raise HlpUnavailable("vaultAddress is not the canonical HLP vault")
        raw_windows = details.get("portfolio")
        if not isinstance(raw_windows, list):
            raise HlpUnavailable("portfolio is missing")
        by_name: dict[str, dict] = {}
        for entry in raw_windows:
            if (isinstance(entry, (list, tuple)) and len(entry) == 2
                    and isinstance(entry[0], str)):
                by_name[entry[0]] = entry[1]
        windows: dict[str, dict] = {}
        for name in REPORTED_WINDOWS:
            if name not in by_name:
                raise HlpUnavailable(f"portfolio window {name} is missing")
            windows[name] = _window_metrics(by_name[name])
        current_tvl = _series(
            by_name["week"].get("accountValueHistory"), field="accountValueHistory",
        )[-1][1]
        allow_deposits = details.get("allowDeposits")
        if not isinstance(allow_deposits, bool):
            raise HlpUnavailable("allowDeposits is not boolean")
        return {
            "available": True,
            "generated_at": stamp,
            "vault": str(details.get("name") or "HLP"),
            "vault_address": HLP_VAULT_ADDRESS,
            "current_tvl_usd": round(current_tvl, 2),
            "allow_deposits": allow_deposits,
            # The API's rolling apr is noisy/instantaneous, disclosed as such and
            # never used as the historical return.
            "instant_apr_pct": round(_finite(
                details.get("apr", 0.0), field="apr") * 100, 4),
            "windows": windows,
            "drawdown_basis": DRAWDOWN_BASIS,
            "disclaimer": DISCLAIMER,
        }
    except HlpUnavailable as exc:
        return {"available": False, "generated_at": stamp, "reason": str(exc)}


def fetch_details(*, timeout: float = 20.0) -> dict:
    request = urllib.request.Request(
        HLP_INFO_URL,
        data=json.dumps({
            "type": "vaultDetails", "vaultAddress": HLP_VAULT_ADDRESS,
        }).encode(),
        headers={"Content-Type": "application/json", "User-Agent": "CryptoScope/1.0"},
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read())


def _save_state(state: dict) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, separators=(",", ":")))
    tmp.replace(STATE_FILE)


def run(*, now: datetime | None = None) -> dict:
    """Fetch, project, and persist the HLP state; a fetch failure fails closed."""
    generated_at = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    try:
        details = fetch_details()
    except Exception as exc:
        state = {
            "available": False,
            "generated_at": generated_at.isoformat(),
            "reason": f"fetch failed: {type(exc).__name__}",
        }
        _save_state(state)
        return state
    state = compute_hlp_state(details, now=generated_at)
    _save_state(state)
    return state


if __name__ == "__main__":
    print(json.dumps(run(), ensure_ascii=False, indent=2))
