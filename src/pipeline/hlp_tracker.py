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
import sqlite3
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
DRAWDOWN_BASIS = "return_compounded_lower_bound_at_series_resolution"
DISCLAIMER = (
    "被动做市对手盘的历史表现,不是收益承诺;最大回撤按每步收益复利计算,"
    "但受该窗口采样分辨率限制——全周期是 ~14 天粗桶,只是真实日内回撤的下界"
    "(JELLY 类分钟级事件被平滑);非投资建议。"
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
    if len(account) != len(pnl):
        raise HlpUnavailable("account and pnl series lengths disagree")
    total_pnl = pnl[-1][1] - pnl[0][1]
    avg_tvl = sum(value for _, value in account) / len(account)
    if avg_tvl <= 0:
        raise HlpUnavailable("window average TVL is not positive")
    # Return-based max drawdown: each step's strategy return is its PnL change
    # over the account value AT THAT TIME, then compounded — so an early loss on
    # a small book counts at its real weight. Dividing the dollar drawdown by the
    # window's AVERAGE TVL (the old method) understated early-period drawdowns
    # ~3x. The dollar peak-to-trough is kept as the worst absolute loss. Both are
    # still bounded by the series resolution (see resolution_hours): a coarse
    # window is a LOWER BOUND on the true intraday drawdown, disclosed as such.
    equity = 1.0
    equity_peak = 1.0
    return_drawdown = 0.0
    dollar_peak = pnl[0][1]
    dollar_drawdown = 0.0
    for index in range(len(pnl) - 1):
        base = account[index][1]
        if base > 0:
            equity *= 1 + (pnl[index + 1][1] - pnl[index][1]) / base
            equity_peak = max(equity_peak, equity)
            return_drawdown = min(return_drawdown, equity / equity_peak - 1)
        value = pnl[index + 1][1]
        dollar_peak = max(dollar_peak, value)
        dollar_drawdown = min(dollar_drawdown, value - dollar_peak)
    gaps = sorted(
        (pnl[i + 1][0] - pnl[i][0]) / 3_600_000 for i in range(len(pnl) - 1)
    )
    resolution_hours = gaps[len(gaps) // 2] if gaps else 0.0
    return {
        "span_days": round(span_days, 2),
        "pnl_usd": round(total_pnl, 2),
        "avg_tvl_usd": round(avg_tvl, 2),
        "annualized_pct": round((total_pnl / avg_tvl) * (365 / span_days) * 100, 2),
        "max_drawdown_usd": round(dollar_drawdown, 2),
        "max_drawdown_pct": round(return_drawdown * 100, 3),
        "resolution_hours": round(resolution_hours, 2),
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


HISTORY_DB = DATA_DIR / "hlp_history.db"


def _history_conn() -> sqlite3.Connection:
    HISTORY_DB.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(HISTORY_DB), timeout=10)
    conn.execute("""CREATE TABLE IF NOT EXISTS steps(
        start_ms INTEGER NOT NULL, end_ms INTEGER NOT NULL,
        pnl_usd REAL NOT NULL, account_value_usd REAL NOT NULL,
        captured_at TEXT NOT NULL,
        PRIMARY KEY(start_ms, end_ms))""")
    return conn


def record_fine_history(details: object, *, now: datetime | None = None) -> dict:
    """Accumulate fine-resolution PnL steps from the 'day' window, forever.

    The API keeps only ~0.4h resolution for the last day; older windows are
    ~14-day buckets, so the TRUE long-run intraday drawdown can only be built
    forward from now. The day window's absolute pnl is REBASED to 0 at every
    window start, so absolute levels cannot be stored across fetches — but each
    step (pnl[i+1]-pnl[i] over account_value[i]) is base-invariant (verified
    live: 43/43 overlapping steps identical across a window slide). Steps are
    keyed by (start_ms, end_ms); the first capture of an interval is frozen.
    """
    stamp = ((now or datetime.now(timezone.utc)).astimezone(timezone.utc)
             .isoformat())
    try:
        if not isinstance(details, dict):
            raise HlpUnavailable("vaultDetails is not an object")
        if str(details.get("vaultAddress", "")).lower() != HLP_VAULT_ADDRESS:
            raise HlpUnavailable("vaultAddress is not the canonical HLP vault")
        windows = {
            entry[0]: entry[1]
            for entry in details.get("portfolio") or []
            if isinstance(entry, (list, tuple)) and len(entry) == 2
        }
        if "day" not in windows or not isinstance(windows["day"], dict):
            raise HlpUnavailable("day window is missing")
        account = _series(
            windows["day"].get("accountValueHistory"),
            field="accountValueHistory",
        )
        pnl = _series(windows["day"].get("pnlHistory"), field="pnlHistory")
        if len(account) != len(pnl):
            raise HlpUnavailable("account and pnl series lengths disagree")
    except HlpUnavailable as exc:
        return {"recorded": False, "reason": str(exc)}
    rows = [
        (pnl[i][0], pnl[i + 1][0],
         pnl[i + 1][1] - pnl[i][1], account[i][1], stamp)
        for i in range(len(pnl) - 1)
        if pnl[i + 1][0] > pnl[i][0] and account[i][1] > 0
    ]
    conn = _history_conn()
    try:
        before = conn.execute("SELECT COUNT(*) FROM steps").fetchone()[0]
        conn.executemany(
            "INSERT OR IGNORE INTO steps VALUES (?,?,?,?,?)", rows,
        )
        conn.commit()
        total = conn.execute("SELECT COUNT(*) FROM steps").fetchone()[0]
    finally:
        conn.close()
    return {"recorded": True, "inserted": total - before, "total": total}


def fine_history_summary() -> dict:
    """True intraday drawdown over the ACCUMULATED fine steps, gap-honest.

    Compounding across a recording gap would silently assume zero return while
    the recorder was down, so contiguous runs are compounded separately and the
    worst per-segment drawdown is reported alongside coverage (a reader can see
    exactly how much wall-clock the evidence spans versus covers).
    """
    conn = _history_conn()
    try:
        rows = conn.execute(
            "SELECT start_ms,end_ms,pnl_usd,account_value_usd FROM steps "
            "ORDER BY start_ms",
        ).fetchall()
    finally:
        conn.close()
    if len(rows) < 2:
        return {"available": False, "reason": "insufficient_history",
                "n_steps": len(rows)}
    segments = 1
    covered_ms = 0
    equity = 1.0
    peak = 1.0
    max_drawdown = 0.0
    previous_end = None
    for start_ms, end_ms, step_pnl, base in rows:
        if previous_end is not None and start_ms != previous_end:
            segments += 1
            equity = peak = 1.0  # never compound across a gap
        covered_ms += end_ms - start_ms
        equity *= 1 + step_pnl / base
        peak = max(peak, equity)
        max_drawdown = min(max_drawdown, equity / peak - 1)
        previous_end = end_ms
    span_ms = rows[-1][1] - rows[0][0]
    return {
        "available": True,
        "n_steps": len(rows),
        "segments": segments,
        "first_at": datetime.fromtimestamp(
            rows[0][0] / 1000, timezone.utc).isoformat(),
        "last_at": datetime.fromtimestamp(
            rows[-1][1] / 1000, timezone.utc).isoformat(),
        "span_days": round(span_ms / 86_400_000, 2),
        "covered_days": round(covered_ms / 86_400_000, 2),
        "coverage_pct": round(covered_ms / span_ms * 100, 1) if span_ms else 0.0,
        "max_drawdown_pct": round(max_drawdown * 100, 4),
        "basis": "forward_accumulated_fine_steps_gap_segmented",
    }


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
    # Accumulate fine-resolution history from the same fetch (zero extra API
    # calls). Recorder trouble must never break the published state.
    try:
        record_fine_history(details, now=generated_at)
    except Exception:
        pass
    return state


if __name__ == "__main__":
    print(json.dumps(run(), ensure_ascii=False, indent=2))
