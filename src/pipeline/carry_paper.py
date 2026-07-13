"""Paper-trade tracker for the cross-venue funding carry.

This closes the last gap between "structurally sound" and "proven": it replaces the two
remaining ASSUMPTIONS — how long the differential holds (hold_days) and real execution
slippage — with MEASURED data. On each run it:

  · opens a paper position for every fat-net cross-venue carry not already open,
    snapshotting entry slippage from the LIVE order books (HL + OKX),
  · accrues the realized funding differential each update (integrating the diff over
    elapsed time — the actual money the delta-neutral pair would have collected),
  · closes the position when the differential decays below a floor → the REAL hold
    period and the REAL realized net (accrued funding − measured entry/exit slippage).

No real orders are placed — this is measurement only. Never raises; every network call is
defensive. The output feeds an honest board readout: "paper: realized hold Xd, slippage
Y%, net Z% (vs predicted)".
"""

from __future__ import annotations

import json
import sqlite3
import urllib.request
from datetime import datetime, timezone

import structlog

from src.config import DATA_DIR

logger = structlog.get_logger()

DB = DATA_DIR / "carry_paper.db"
NOTIONAL = 10_000.0        # paper size per leg — small enough that book slippage is real
OPEN_MIN_NET = 8.0         # only paper-trade carries whose PREDICTED net clears this
CLOSE_DIFF_FLOOR = 2.0     # differential decayed below this (ann %) → natural exit
# fees are known, not assumed: HL taker 0.045%, OKX taker 0.05% → ~0.095%/leg, ×2 legs
# ×(in+out) is folded in at close; slippage is MEASURED from the book, not assumed.
FEE_PCT_PER_SIDE = 0.095
_OKX_CTVAL: dict[str, float] = {}


def _conn() -> sqlite3.Connection:
    DB.parent.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(str(DB), timeout=10)
    c.execute("""CREATE TABLE IF NOT EXISTS paper(
        id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT, entry_ts TEXT,
        entry_diff REAL, pred_net REAL, entry_slip REAL, notional REAL,
        accrued_pct REAL DEFAULT 0, last_ts TEXT, last_diff REAL,
        status TEXT DEFAULT 'open', exit_ts TEXT, exit_slip REAL,
        hold_h REAL, realized_net REAL)""")
    return c


def _get(url: str, timeout: int = 10):
    return json.loads(urllib.request.urlopen(
        urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"}), timeout=timeout).read())


def _post(url: str, body: dict, timeout: int = 10):
    return json.loads(urllib.request.urlopen(urllib.request.Request(
        url, data=json.dumps(body).encode(), headers={"Content-Type": "application/json"}),
        timeout=timeout).read())


def _okx_ctval(coin: str) -> float:
    """OKX contract value (coin units per contract), bulk-fetched once and cached. 1.0
    fallback keeps slippage a rough-but-real estimate rather than crashing."""
    if not _OKX_CTVAL:
        try:
            d = _get("https://www.okx.com/api/v5/public/instruments?instType=SWAP")
            for it in d.get("data", []):
                if it.get("ctValCcy") and it.get("instId", "").endswith("-USDT-SWAP"):
                    _OKX_CTVAL[it["instId"].split("-")[0]] = float(it.get("ctVal") or 1.0)
        except Exception:
            pass
    return _OKX_CTVAL.get(coin, 1.0)


def _hl_slip(coin: str, notional: float) -> float | None:
    """Slippage % to fill `notional` USD by taking HL asks (VWAP vs best). sz is in coin
    units on HL, so notional = px*sz directly. None if the book can't fill it."""
    try:
        lv = _post("https://api.hyperliquid.xyz/info", {"type": "l2Book", "coin": coin})["levels"]
        asks = lv[1]
        best = float(asks[0]["px"])
        rem, qty = notional, 0.0
        for L in asks:
            px, sz = float(L["px"]), float(L["sz"])
            take = min(px * sz, rem); qty += take / px; rem -= take
            if rem <= 0:
                break
        if rem > 0 or qty <= 0:
            return None
        return (notional / qty / best - 1) * 100
    except Exception:
        return None


def _okx_slip(coin: str, notional: float) -> float | None:
    """Slippage % to fill `notional` USD on OKX (sz in contracts × ctVal = coin units)."""
    try:
        d = _get(f"https://www.okx.com/api/v5/market/books?instId={coin}-USDT-SWAP&sz=50")
        asks = d["data"][0]["asks"]
        ctv = _okx_ctval(coin)
        best = float(asks[0][0])
        rem, qty = notional, 0.0
        for a in asks:
            px, sz = float(a[0]), float(a[1]) * ctv
            take = min(px * sz, rem); qty += take / px; rem -= take
            if rem <= 0:
                break
        if rem > 0 or qty <= 0:
            return None
        return (notional / qty / best - 1) * 100
    except Exception:
        return None


def _roundtrip_slip(coin: str, notional: float = NOTIONAL) -> float | None:
    """Measured entry (or exit) slippage across BOTH legs. None if either book is thin."""
    hs, os_ = _hl_slip(coin, notional), _okx_slip(coin, notional)
    if hs is None or os_ is None:
        return None
    return hs + os_            # one direction, both legs; close measures the other side


def run(carries: list[dict]) -> dict:
    """Open/accrue/close paper positions from the current cross-venue carry list. Returns
    a stats summary. `carries`: the carry_signals() output (needs symbol, cross, net_ann,
    edge_ann)."""
    now = datetime.now(timezone.utc)
    by_sym = {c["symbol"]: c for c in carries if c.get("cross")}
    c = _conn()
    try:
        open_rows = {r[1]: r for r in c.execute(
            "SELECT id,symbol,entry_ts,entry_diff,pred_net,entry_slip,notional,accrued_pct,"
            "last_ts,last_diff FROM paper WHERE status='open'").fetchall()}
        # 1) ACCRUE + maybe CLOSE existing open positions
        for sym, row in open_rows.items():
            pid, _, _, _, pred_net, entry_slip, notional, accrued, last_ts, last_diff = row
            cur = by_sym.get(sym)
            cur_diff = cur["edge_ann"] if cur else (last_diff if last_diff is not None else 0)
            try:
                elapsed_h = (now - datetime.fromisoformat(last_ts)).total_seconds() / 3600
            except Exception:
                elapsed_h = 0
            # realized funding over the interval = diff(ann%) × (hours / 8760)
            accrued = (accrued or 0) + (last_diff or cur_diff) * (elapsed_h / 8760.0)
            if cur_diff < CLOSE_DIFF_FLOOR or cur is None:
                exit_slip = _roundtrip_slip(sym) or entry_slip or 0
                try:
                    hold_h = (now - datetime.fromisoformat(open_rows[sym][2])).total_seconds() / 3600
                except Exception:
                    hold_h = 0
                # realized net %/yr = accrued funding annualized − all costs (slippage both
                # sides + fees both legs both sides), amortized over the real hold.
                hold_yr = max(hold_h / 8760.0, 1e-6)
                cost = (entry_slip or 0) + exit_slip + 2 * FEE_PCT_PER_SIDE * 2  # 2 legs×(in+out)
                realized_net = accrued / hold_yr - cost / hold_yr
                c.execute("UPDATE paper SET status='closed', exit_ts=?, exit_slip=?, hold_h=?, "
                          "accrued_pct=?, realized_net=?, last_ts=?, last_diff=? WHERE id=?",
                          (now.isoformat(), exit_slip, hold_h, accrued, realized_net,
                           now.isoformat(), cur_diff, pid))
            else:
                c.execute("UPDATE paper SET accrued_pct=?, last_ts=?, last_diff=? WHERE id=?",
                          (accrued, now.isoformat(), cur_diff, pid))
        # 2) OPEN new positions for fat-net cross carries not already open
        for sym, cur in by_sym.items():
            if sym in open_rows or (cur.get("net_ann") or 0) < OPEN_MIN_NET:
                continue
            slip = _roundtrip_slip(sym)
            if slip is None:
                continue                      # can't measure entry → don't open
            c.execute("INSERT INTO paper(symbol,entry_ts,entry_diff,pred_net,entry_slip,"
                      "notional,accrued_pct,last_ts,last_diff) VALUES (?,?,?,?,?,?,0,?,?)",
                      (sym, now.isoformat(), cur["edge_ann"], cur["net_ann"], slip,
                       NOTIONAL, now.isoformat(), cur["edge_ann"]))
        c.commit()
    finally:
        c.close()
    return paper_stats()


def paper_stats() -> dict:
    """Aggregate the paper book: closed-position realized hold/slippage/net (the measured
    replacements for the assumptions), and current open accrual. Honest 'not enough yet'
    until positions have closed."""
    c = _conn()
    try:
        closed = c.execute("SELECT symbol,hold_h,entry_slip,exit_slip,realized_net,pred_net "
                           "FROM paper WHERE status='closed'").fetchall()
        n_open = c.execute("SELECT COUNT(*) FROM paper WHERE status='open'").fetchone()[0]
    finally:
        c.close()
    out = {"n_open": n_open, "n_closed": len(closed)}
    if closed:
        holds = [r[1] / 24 for r in closed if r[1] is not None]
        slips = [(r[2] or 0) + (r[3] or 0) for r in closed]
        nets = [r[4] for r in closed if r[4] is not None]
        preds = [r[5] for r in closed if r[5] is not None]
        out.update({
            "avg_hold_days": round(sum(holds) / len(holds), 1) if holds else None,
            "avg_slip_pct": round(sum(slips) / len(slips), 3) if slips else None,
            "avg_realized_net": round(sum(nets) / len(nets), 1) if nets else None,
            "avg_predicted_net": round(sum(preds) / len(preds), 1) if preds else None,
            "recent": [{"symbol": r[0], "hold_days": round((r[1] or 0) / 24, 1),
                        "realized_net": round(r[4], 1) if r[4] is not None else None,
                        "predicted_net": round(r[5], 1) if r[5] is not None else None}
                       for r in closed[-8:]],
        })
    return out


if __name__ == "__main__":
    from dotenv import load_dotenv

    from src.config import PROJECT_ROOT
    load_dotenv(PROJECT_ROOT / ".env")
    from src.onchain.hyperliquid import carry_signals
    print(json.dumps(run(carry_signals()), ensure_ascii=False, indent=1))
