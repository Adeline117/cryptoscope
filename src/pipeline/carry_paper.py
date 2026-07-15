"""Paper-trade tracker for the cross-venue funding carry.

This closes the last gap between "structurally sound" and "proven": it replaces the two
remaining ASSUMPTIONS — how long the differential holds (hold_days) and real execution
slippage — with MEASURED data. On each run it:

  · opens a paper position for every fat-net cross-venue carry not already open,
    snapshotting entry slippage from the LIVE order books (HL + OKX),
  · accrues the observed funding differential only between consecutive valid updates;
    source gaps are measured explicitly and never backfilled as profit,
  · closes the position when the differential decays below a floor → the REAL hold
    period and the REAL realized net (accrued funding − measured entry/exit slippage).

No real orders are placed — this is measurement only. Never raises; every network call is
defensive. The output feeds an honest board readout: "paper: realized hold Xd, slippage
Y%, net Z% (vs predicted)".
"""

from __future__ import annotations

import json
import math
import sqlite3
import urllib.request
from datetime import datetime, timezone

import structlog

from src.config import DATA_DIR
from src.onchain.okx_symbols import candidates as okx_symbol_candidates

logger = structlog.get_logger()

DB = DATA_DIR / "carry_paper.db"
NOTIONAL = 10_000.0        # paper size per leg — small enough that book slippage is real
OPEN_MIN_NET = 8.0         # only paper-trade carries whose PREDICTED net clears this
CLOSE_DIFF_FLOOR = 2.0     # differential decayed below this (ann %) → natural exit
# fees are known, not assumed. ONE direction across BOTH legs = HL taker 0.045% + OKX
# taker 0.05% = 0.095%. A round trip (enter + exit) is 2× that = 0.19%. Slippage is
# MEASURED from the book separately (entry_slip + exit_slip), never folded in here.
FEE_PCT_ONEWAY_BOTHLEGS = 0.095
CARRY_EXIT_QUOTE_SLA_S = 60
MIN_ANNUALIZED_HOLD_H = 30 * 24
MIN_ANNUALIZED_SAMPLES = 5
_OKX_CTVAL: dict[str, float] = {}
_OKX_META_LOADED = False


def edge_exclusion_reasons(sample: dict) -> list[str]:
    """Return every reason a closed paper episode cannot enter the edge cohort."""
    reasons: list[str] = []
    if sample.get("episode_version") != 2:
        reasons.append("legacy_episode")
    if sample.get("observation_version") != 1:
        reasons.append("unverified_observation_method")
    close_reason = sample.get("close_reason")
    if close_reason == "market_missing":
        reasons.append("market_missing_close")
    elif close_reason != "diff_below_floor":
        reasons.append("invalid_close_reason")
    if (sample.get("cost_complete") is not True
            or sample.get("entry_slip_pct") is None
            or sample.get("exit_slip_pct") is None):
        reasons.append("incomplete_cost")
    try:
        quote_delay_s = float(sample["exit_quote_delay_s"])
        if quote_delay_s < 0 or quote_delay_s > CARRY_EXIT_QUOTE_SLA_S:
            reasons.append("exit_quote_outside_sla")
    except (KeyError, TypeError, ValueError):
        reasons.append("exit_quote_outside_sla")
    try:
        if float(sample.get("unmeasured_h") or 0) > 1e-9:
            reasons.append("incomplete_funding_path")
    except (TypeError, ValueError):
        reasons.append("incomplete_funding_path")
    if (sample.get("hold_h") is None or sample.get("funding_accrued_pct") is None
            or sample.get("net_return_pct") is None):
        reasons.append("missing_result")
    return reasons


def _conn() -> sqlite3.Connection:
    DB.parent.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(str(DB), timeout=10)
    c.execute("""CREATE TABLE IF NOT EXISTS paper(
        id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT, entry_ts TEXT,
        entry_diff REAL, pred_net REAL, entry_slip REAL, notional REAL,
        accrued_pct REAL DEFAULT 0, last_ts TEXT, last_diff REAL,
        status TEXT DEFAULT 'open', exit_ts TEXT, exit_slip REAL,
        hold_h REAL, realized_net REAL, close_reason TEXT,
        last_attempt_ts TEXT, last_valid_ts TEXT, unmeasured_h REAL DEFAULT 0,
        measurement_state TEXT DEFAULT 'observed',
        episode_version INTEGER DEFAULT 2, cost_complete INTEGER DEFAULT 0,
        observation_version INTEGER, exit_signal_ts TEXT, exit_signal_diff REAL,
        exit_quote_ts TEXT, exit_quote_delay_s REAL)""")
    cols = {r[1] for r in c.execute("PRAGMA table_info(paper)").fetchall()}
    if "close_reason" not in cols:
        c.execute("ALTER TABLE paper ADD COLUMN close_reason TEXT")
    additions = {
        "last_attempt_ts": "TEXT",
        "last_valid_ts": "TEXT",
        "unmeasured_h": "REAL DEFAULT 0",
        "measurement_state": "TEXT DEFAULT 'observed'",
        # Deliberately no migration default: pre-v2 rows stay NULL and quarantined.
        "episode_version": "INTEGER",
        "cost_complete": "INTEGER",
        "observation_version": "INTEGER",
        "exit_signal_ts": "TEXT",
        "exit_signal_diff": "REAL",
        "exit_quote_ts": "TEXT",
        "exit_quote_delay_s": "REAL",
    }
    added: set[str] = set()
    for name, declaration in additions.items():
        if name not in cols:
            c.execute(f"ALTER TABLE paper ADD COLUMN {name} {declaration}")
            added.add(name)
    if added:
        now = datetime.now(timezone.utc)
        rows = c.execute(
            "SELECT id,last_ts,status,last_attempt_ts,last_valid_ts,unmeasured_h,"
            "measurement_state FROM paper"
        ).fetchall()
        for pid, last_ts, status, attempt, valid, unmeasured, state in rows:
            if status == "open" and "measurement_state" in added:
                try:
                    migration_gap_h = max(
                        (now - datetime.fromisoformat(last_ts)).total_seconds() / 3600, 0
                    )
                except Exception:
                    migration_gap_h = 0
                c.execute(
                    "UPDATE paper SET last_ts=?,last_attempt_ts=?,last_valid_ts=?,"
                    "unmeasured_h=?,measurement_state='migration_gap' WHERE id=?",
                    (now.isoformat(), now.isoformat(), valid or last_ts,
                     (unmeasured or 0) + migration_gap_h, pid),
                )
            else:
                c.execute(
                    "UPDATE paper SET last_attempt_ts=?,last_valid_ts=?,unmeasured_h=?,"
                    "measurement_state=? WHERE id=?",
                    (attempt or last_ts, valid or last_ts, unmeasured or 0,
                     state or "observed", pid),
                )
        c.commit()
    return c


def _sync_opportunity_ledger() -> dict:
    """Mirror every paper episode into the shared five-lane evidence ledger."""
    from src.pipeline import opportunity_ledger

    c = _conn()
    try:
        rows = c.execute(
            "SELECT id,symbol,entry_ts,entry_diff,pred_net,entry_slip,notional,"
            "accrued_pct,last_ts,last_diff,status,exit_ts,exit_slip,hold_h,"
            "realized_net,close_reason,last_attempt_ts,last_valid_ts,unmeasured_h,"
            "measurement_state,episode_version,cost_complete,observation_version,"
            "exit_signal_ts,exit_signal_diff,exit_quote_ts,exit_quote_delay_s "
            "FROM paper ORDER BY id"
        ).fetchall()
    finally:
        c.close()
    synced = resolved = 0
    for row in rows:
        (pid, symbol, entry_ts, entry_diff, pred_net, entry_slip, notional,
         accrued_pct, last_ts, last_diff, status, exit_ts, exit_slip, hold_h,
         realized_net, close_reason, last_attempt_ts, last_valid_ts, unmeasured_h,
         measurement_state, episode_version, cost_complete, observation_version,
         exit_signal_ts, exit_signal_diff, exit_quote_ts, exit_quote_delay_s) = row
        estimated_roundtrip_cost = ((entry_slip or 0) * 2
                                    + 2 * FEE_PCT_ONEWAY_BOTHLEGS)
        candidate = {
            "lane": "carry", "chain": "hyperliquid+okx", "token": symbol,
            "event_key": f"paper:{pid}", "symbol": symbol,
            "source": "Hyperliquid + OKX live order books",
            "event_at": entry_ts, "detected_at": entry_ts, "decision_at": entry_ts,
            "quote_at": entry_ts, "state": f"paper_{status}",
            "decision": "PAPER_OPEN", "max_notional_usd": notional,
            "gross_notional_usd": (notional or 0) * 2,
            "entry_diff_ann_pct": entry_diff, "predicted_net_ann_pct": pred_net,
            "entry_slip_pct": entry_slip,
            "roundtrip_cost_pct_est": estimated_roundtrip_cost,
            "cost_model": "paper_books_symmetric_exit_estimate_plus_known_taker_fees",
            "execution_mode": "paper_orderbook_measurement",
            "exit_diff_floor_ann_pct": CLOSE_DIFF_FLOOR,
            "paper_position_id": pid,
            "episode_version": episode_version,
            "observation_version": observation_version,
        }
        ident, _ = opportunity_ledger.record(candidate)
        outcome = {
            "version": 2 if episode_version == 2 else 1,
            "episode_version": episode_version,
            "observation_version": observation_version,
            "kind": "delta_neutral_carry_paper",
            "execution_mode": "paper_orderbook_measurement",
            "cost_is_real_fill": False, "status": status,
            "funding_accrued_pct": accrued_pct or 0,
            "entry_slip_pct": entry_slip,
            "last_diff_ann_pct": last_diff, "last_measured_at": last_valid_ts,
            "last_attempt_at": last_attempt_ts, "last_valid_at": last_valid_ts,
            "unmeasured_h": unmeasured_h or 0,
            "measurement_state": measurement_state or "observed",
            "cost_complete": bool(cost_complete),
            "exit_signal_at": exit_signal_ts, "exit_signal_diff_ann_pct": exit_signal_diff,
            "exit_quote_at": exit_quote_ts, "exit_quote_delay_s": exit_quote_delay_s,
        }
        state = "open"
        if status == "closed":
            outcome.update({
                "closed_at": exit_ts, "hold_h": hold_h,
                "close_reason": close_reason or "legacy_unknown",
            })
            if cost_complete:
                fees_pct = 2 * FEE_PCT_ONEWAY_BOTHLEGS
                realized_cost_pct = entry_slip + exit_slip + fees_pct
                outcome.update({
                    "exit_slip_pct": exit_slip, "fees_pct": fees_pct,
                    "realized_cost_pct": realized_cost_pct,
                    "net_return_pct": (accrued_pct or 0) - realized_cost_pct,
                    "realized_net_ann_pct": realized_net,
                })
            else:
                outcome["evidence_exclusion_reason"] = "incomplete_book_cost"
            reasons = edge_exclusion_reasons(outcome)
            outcome["cost_completeness"] = "complete" if cost_complete else "incomplete"
            outcome["edge_exclusion_reasons"] = reasons
            outcome["edge_sample_eligible"] = not reasons
            state = "resolved"
            resolved += 1
        opportunity_ledger.save_outcome(ident, outcome, state)
        synced += 1
    return {"status": "ok", "synced": synced, "resolved": resolved}


def _get(url: str, timeout: int = 10):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=timeout) as response:
        return json.loads(response.read())


def _post(url: str, body: dict, timeout: int = 10):
    req = urllib.request.Request(
        url, data=json.dumps(body).encode(), headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as response:
        return json.loads(response.read())


def _load_okx_contracts() -> None:
    """Cache only live linear USDT contracts with a verified positive contract value."""
    global _OKX_META_LOADED
    if _OKX_META_LOADED:
        return
    try:
        data = _get("https://www.okx.com/api/v5/public/instruments?instType=SWAP")
    except Exception:
        return
    items = (data.get("data") if isinstance(data, dict)
             and str(data.get("code")) == "0" else None)
    if not isinstance(items, list) or not items:
        return
    _OKX_META_LOADED = True
    for item in items:
        inst_id = str(item.get("instId") or "")
        parts = inst_id.split("-")
        if (len(parts) != 3 or parts[1:] != ["USDT", "SWAP"]
                or item.get("state") != "live" or item.get("ctType") != "linear"
                or item.get("settleCcy") != "USDT"):
            continue
        base = parts[0].upper()
        if str(item.get("ctValCcy") or "").upper() != base:
            continue
        try:
            value = float(item["ctVal"])
        except (KeyError, TypeError, ValueError):
            continue
        if math.isfinite(value) and value > 0:
            _OKX_CTVAL[base] = value


def _okx_contract(coin: str) -> tuple[str, float] | None:
    """Return the exact live OKX base and ctVal; metadata uncertainty fails closed."""
    _load_okx_contracts()
    for base in okx_symbol_candidates(coin):
        value = _OKX_CTVAL.get(base)
        if value is not None:
            return base, value
    return None


def _okx_ctval(coin: str) -> float | None:
    contract = _okx_contract(coin)
    return contract[1] if contract else None


def _hl_slip(coin: str, notional: float, side: str) -> float | None:
    """Directional HL book slippage for a marketable buy or sell."""
    try:
        notional = float(notional)
    except (TypeError, ValueError):
        return None
    if side not in {"buy", "sell"} or not math.isfinite(notional) or notional <= 0:
        return None
    try:
        lv = _post("https://api.hyperliquid.xyz/info", {"type": "l2Book", "coin": coin})["levels"]
        levels = lv[1] if side == "buy" else lv[0]
        best = float(levels[0]["px"])
        if not math.isfinite(best) or best <= 0:
            return None
        rem, qty = notional, 0.0
        for L in levels:
            px, sz = float(L["px"]), float(L["sz"])
            if not all(math.isfinite(x) and x > 0 for x in (px, sz)):
                return None
            take = min(px * sz, rem); qty += take / px; rem -= take
            if rem <= 0:
                break
        if rem > 0 or qty <= 0:
            return None
        average = notional / qty
        slip = ((average / best - 1) if side == "buy" else (1 - average / best)) * 100
        return max(0.0, slip) if math.isfinite(slip) and slip >= -1e-9 else None
    except Exception:
        return None


def _okx_slip(coin: str, notional: float, side: str) -> float | None:
    """Directional OKX book slippage (contract size converted to coin units)."""
    try:
        notional = float(notional)
    except (TypeError, ValueError):
        return None
    if side not in {"buy", "sell"} or not math.isfinite(notional) or notional <= 0:
        return None
    try:
        contract = _okx_contract(coin)
        if contract is None:
            return None
        base, ctv = contract
        d = _get(f"https://www.okx.com/api/v5/market/books?instId={base}-USDT-SWAP&sz=50")
        if not isinstance(d, dict) or str(d.get("code")) != "0":
            return None
        levels = d["data"][0]["asks" if side == "buy" else "bids"]
        best = float(levels[0][0])
        if not math.isfinite(best) or best <= 0:
            return None
        rem, qty = notional, 0.0
        for a in levels:
            px, sz = float(a[0]), float(a[1]) * ctv
            if not all(math.isfinite(x) and x > 0 for x in (px, sz)):
                return None
            take = min(px * sz, rem); qty += take / px; rem -= take
            if rem <= 0:
                break
        if rem > 0 or qty <= 0:
            return None
        average = notional / qty
        slip = ((average / best - 1) if side == "buy" else (1 - average / best)) * 100
        return max(0.0, slip) if math.isfinite(slip) and slip >= -1e-9 else None
    except Exception:
        return None


def _roundtrip_slip(coin: str, notional: float = NOTIONAL,
                    phase: str = "entry") -> float | None:
    """Measure the two legs in their actual direction for entry or exit."""
    if phase == "entry":
        hl_side, okx_side = "sell", "buy"   # short HL, long OKX
    elif phase == "exit":
        hl_side, okx_side = "buy", "sell"   # cover HL, close OKX long
    else:
        return None
    hs = _hl_slip(coin, notional, hl_side)
    os_ = _okx_slip(coin, notional, okx_side)
    if (hs is None or os_ is None or not math.isfinite(hs) or not math.isfinite(os_)
            or hs < 0 or os_ < 0):
        return None
    return hs + os_            # one direction, both legs; close measures the other side


def open_symbols() -> list[str]:
    """Symbols with an open paper episode, in stable creation order."""
    c = _conn()
    try:
        return [row[0] for row in c.execute(
            "SELECT symbol FROM paper WHERE status IN ('open','exit_pending') ORDER BY id"
        ).fetchall()]
    finally:
        c.close()


def run(carries: list[dict], *, observations: list[dict] | None = None) -> dict:
    """Open from ranked candidates; accrue/close only from paired current observations.

    ``observations=[]`` is an explicit source gap. ``None`` keeps legacy callers running,
    but those candidate-proxy episodes receive observation_version=0 and are quarantined.
    """
    now = datetime.now(timezone.utc)
    entry_by_sym = {item["symbol"]: item for item in carries if item.get("cross")}
    raw_observations = observations
    if raw_observations is None:
        raw_observations = [{
            "symbol": item["symbol"], "status": "observed", "cross": True,
            "observation_version": 0,
            "observed_edge_ann": item.get("observed_edge_ann", item.get("edge_ann")),
        } for item in carries if item.get("cross")]
    observed_by_sym: dict[str, dict] = {}
    for observation in raw_observations:
        if observation.get("status") != "observed" or not observation.get("cross"):
            continue
        try:
            edge = float(observation["observed_edge_ann"])
        except (KeyError, TypeError, ValueError):
            continue
        if not math.isfinite(edge):
            continue
        observed_by_sym[observation["symbol"]] = {**observation,
                                                   "observed_edge_ann": edge}
    c = _conn()
    try:
        open_rows = {r[1]: r for r in c.execute(
            "SELECT id,symbol,entry_ts,entry_diff,pred_net,entry_slip,notional,accrued_pct,"
            "last_ts,last_diff,last_attempt_ts,last_valid_ts,unmeasured_h,measurement_state,"
            "status,exit_signal_ts,exit_signal_diff FROM paper "
            "WHERE status IN ('open','exit_pending')").fetchall()}
        # 1) ACCRUE + maybe CLOSE existing open positions
        for sym, row in open_rows.items():
            (pid, _, _, _, pred_net, entry_slip, notional, accrued, last_ts, last_diff,
             _last_attempt_ts, _last_valid_ts, unmeasured_h, measurement_state, status,
             exit_signal_ts, exit_signal_diff) = row
            if status == "exit_pending":
                measured_exit_slip = _roundtrip_slip(sym, phase="exit")
                if measured_exit_slip is None:
                    c.execute(
                        "UPDATE paper SET last_attempt_ts=?,measurement_state='exit_quote_gap' "
                        "WHERE id=?",
                        (now.isoformat(), pid),
                    )
                    continue
                try:
                    signal_dt = datetime.fromisoformat(exit_signal_ts)
                    hold_h = max(
                        (signal_dt - datetime.fromisoformat(row[2])).total_seconds() / 3600, 0
                    )
                    quote_delay_s = max((now - signal_dt).total_seconds(), 0)
                except Exception:
                    signal_dt = now
                    hold_h = 0
                    quote_delay_s = 0
                cost_complete = entry_slip is not None
                hold_yr = max(hold_h / 8760.0, 1e-6)
                cost = ((entry_slip + measured_exit_slip
                         + 2 * FEE_PCT_ONEWAY_BOTHLEGS) if cost_complete else None)
                realized_net = ((accrued or 0) / hold_yr - cost / hold_yr
                                if cost is not None else None)
                c.execute(
                    "UPDATE paper SET status='closed',exit_ts=?,exit_slip=?,hold_h=?,"
                    "realized_net=?,close_reason='diff_below_floor',last_attempt_ts=?,"
                    "measurement_state='observed',cost_complete=?,exit_quote_ts=?,"
                    "exit_quote_delay_s=? WHERE id=?",
                    (signal_dt.isoformat(), measured_exit_slip, hold_h, realized_net,
                     now.isoformat(), int(cost_complete), now.isoformat(), quote_delay_s, pid),
                )
                continue
            cur = observed_by_sym.get(sym)
            try:
                elapsed_h = max(
                    (now - datetime.fromisoformat(last_ts)).total_seconds() / 3600, 0
                )
            except Exception:
                elapsed_h = 0
            if cur is None:
                c.execute(
                    "UPDATE paper SET last_ts=?,last_attempt_ts=?,unmeasured_h=?,"
                    "measurement_state='source_gap' WHERE id=?",
                    (now.isoformat(), now.isoformat(), (unmeasured_h or 0) + elapsed_h, pid),
                )
                continue

            cur_diff = cur["observed_edge_ann"]
            # realized funding over the interval = diff(ann%) × (hours / 8760)
            # A recovery observation only re-establishes the measurement clock. The
            # preceding interval remains unknown and must not be filled with last_diff.
            if measurement_state != "observed":
                unmeasured_h = (unmeasured_h or 0) + elapsed_h
                elapsed_h = 0
            interval_diff = last_diff if last_diff is not None else cur_diff
            accrued = (accrued or 0) + interval_diff * (elapsed_h / 8760.0)
            if cur_diff < CLOSE_DIFF_FLOOR:
                measured_exit_slip = _roundtrip_slip(sym, phase="exit")
                try:
                    hold_h = (now - datetime.fromisoformat(open_rows[sym][2])).total_seconds() / 3600
                except Exception:
                    hold_h = 0
                if measured_exit_slip is None:
                    c.execute(
                        "UPDATE paper SET status='exit_pending',accrued_pct=?,last_ts=?,"
                        "last_diff=?,last_attempt_ts=?,last_valid_ts=?,unmeasured_h=?,"
                        "measurement_state='exit_quote_gap',cost_complete=0,"
                        "exit_signal_ts=?,exit_signal_diff=? WHERE id=?",
                        (accrued, now.isoformat(), cur_diff, now.isoformat(), now.isoformat(),
                         unmeasured_h or 0, now.isoformat(), cur_diff, pid),
                    )
                    continue
                cost_complete = entry_slip is not None
                # realized net %/yr = accrued funding annualized − all costs (slippage both
                # sides + fees both legs both sides), amortized over the real hold.
                hold_yr = max(hold_h / 8760.0, 1e-6)
                # one-time round-trip cost = slippage in + slippage out + fees(2× one-way)
                cost = ((entry_slip + measured_exit_slip + 2 * FEE_PCT_ONEWAY_BOTHLEGS)
                        if cost_complete else None)
                realized_net = accrued / hold_yr - cost / hold_yr if cost is not None else None
                c.execute("UPDATE paper SET status='closed', exit_ts=?, exit_slip=?, hold_h=?, "
                          "accrued_pct=?, realized_net=?, last_ts=?, last_diff=?,"
                          "close_reason='diff_below_floor',last_attempt_ts=?,last_valid_ts=?,"
                          "unmeasured_h=?,measurement_state='observed',cost_complete=?,"
                          "exit_signal_ts=?,exit_signal_diff=?,exit_quote_ts=?,"
                          "exit_quote_delay_s=0 WHERE id=?",
                          (now.isoformat(), measured_exit_slip, hold_h, accrued, realized_net,
                           now.isoformat(), cur_diff, now.isoformat(), now.isoformat(),
                           unmeasured_h or 0, int(cost_complete), now.isoformat(), cur_diff,
                           now.isoformat(), pid))
            else:
                c.execute(
                    "UPDATE paper SET accrued_pct=?,last_ts=?,last_diff=?,last_attempt_ts=?,"
                    "last_valid_ts=?,unmeasured_h=?,measurement_state='observed' WHERE id=?",
                    (accrued, now.isoformat(), cur_diff, now.isoformat(), now.isoformat(),
                     unmeasured_h or 0, pid),
                )
        # 2) OPEN new positions for fat-net cross carries not already open
        for sym, cur in entry_by_sym.items():
            if sym in open_rows or (cur.get("net_ann") or 0) < OPEN_MIN_NET:
                continue
            observation = observed_by_sym.get(sym)
            if observation is None:
                continue
            slip = _roundtrip_slip(sym, phase="entry")
            if slip is None:
                continue                      # can't measure entry → don't open
            c.execute("INSERT INTO paper(symbol,entry_ts,entry_diff,pred_net,entry_slip,"
                      "notional,accrued_pct,last_ts,last_diff,last_attempt_ts,last_valid_ts,"
                      "unmeasured_h,measurement_state,episode_version,cost_complete,"
                      "observation_version) VALUES (?,?,?,?,?,?,0,?,?,?,?,0,'observed',2,0,?)",
                      (sym, now.isoformat(), observation["observed_edge_ann"], cur["net_ann"],
                       slip, NOTIONAL, now.isoformat(), observation["observed_edge_ann"],
                       now.isoformat(), now.isoformat(),
                       int(observation.get("observation_version") or 0)))
        c.commit()
    finally:
        c.close()
    stats = paper_stats()
    try:
        stats["ledger_sync"] = _sync_opportunity_ledger()
    except Exception as exc:
        logger.warning("carry_ledger_sync_failed", error=str(exc)[:120])
        stats["ledger_sync"] = {"status": "error", "error": str(exc)[:120]}
    return stats


def paper_stats() -> dict:
    """Report absolute paper PnL; annualize only a stable-enough closed cohort."""
    c = _conn()
    try:
        closed_all = c.execute("SELECT symbol,hold_h,entry_slip,exit_slip,realized_net,pred_net,"
                           "accrued_pct,entry_ts,exit_ts,close_reason,episode_version,cost_complete,"
                           "unmeasured_h,observation_version,exit_signal_ts,exit_quote_ts,"
                           "exit_quote_delay_s "
                           "FROM paper WHERE status='closed'").fetchall()
        opened = c.execute(
            "SELECT symbol,entry_ts,last_ts,entry_diff,last_diff,pred_net,entry_slip,notional,"
            "last_attempt_ts,last_valid_ts,unmeasured_h,measurement_state,episode_version,"
            "observation_version,status,exit_signal_ts,exit_signal_diff "
            "FROM paper WHERE status IN ('open','exit_pending') ORDER BY entry_ts DESC"
        ).fetchall()
    finally:
        c.close()
    def as_sample(row: tuple) -> dict:
        cost_complete = bool(row[11])
        net_return = ((row[6] or 0) - row[2] - row[3] - 2 * FEE_PCT_ONEWAY_BOTHLEGS
                      if cost_complete and row[2] is not None and row[3] is not None else None)
        return {
            "episode_version": row[10], "close_reason": row[9],
            "entry_slip_pct": row[2], "exit_slip_pct": row[3],
            "cost_complete": cost_complete, "unmeasured_h": row[12],
            "observation_version": row[13],
            "exit_quote_delay_s": row[16],
            "hold_h": row[1], "funding_accrued_pct": row[6],
            "net_return_pct": net_return,
        }

    reasons_by_row = [edge_exclusion_reasons(as_sample(row)) for row in closed_all]
    valid_closed = [row for row, reasons in zip(closed_all, reasons_by_row) if not reasons]
    excluded: dict[str, int] = {}
    for reasons in reasons_by_row:
        for reason in reasons:
            excluded[reason] = excluded.get(reason, 0) + 1
    out = {
        "n_open": len(opened), "n_closed": len(valid_closed),
        "n_exit_pending": sum(row[14] == "exit_pending" for row in opened),
        "n_closed_total": len(closed_all),
        "n_closed_excluded": len(closed_all) - len(valid_closed),
        "excluded_by_reason": excluded,
        "exit_rule": f"valid paired observation: differential < {CLOSE_DIFF_FLOOR}% ann",
        "exit_quote_sla_s": CARRY_EXIT_QUOTE_SLA_S,
        "open_positions": [
            {
                "symbol": row[0], "entry_at": row[1], "last_measured_at": row[9],
                "entry_diff_ann_pct": row[3], "last_diff_ann_pct": row[4],
                "predicted_net_ann_pct": row[5], "entry_slip_pct": row[6],
                "notional_usd_per_leg": row[7],
                "last_attempt_at": row[8], "last_valid_at": row[9],
                "integration_cursor_at": row[2],
                "unmeasured_h": round(row[10] or 0, 2),
                "measurement_state": row[11] or "observed",
                "episode_version": row[12],
                "observation_version": row[13],
                "status": row[14], "exit_signal_at": row[15],
                "exit_signal_diff_ann_pct": row[16],
                "exit_diff_floor_ann_pct": CLOSE_DIFF_FLOOR,
                "execution_mode": "paper_orderbook_measurement",
            }
            for row in opened
        ],
    }
    if valid_closed:
        holds = [r[1] / 24 for r in valid_closed if r[1] is not None]
        costs = [(r[2] or 0) + (r[3] or 0) + 2 * FEE_PCT_ONEWAY_BOTHLEGS
                 for r in valid_closed]
        accrued = [r[6] or 0 for r in valid_closed]
        nets = [funding - cost for funding, cost in zip(accrued, costs)]
        preds = [r[5] for r in valid_closed if r[5] is not None]
        annualized = [r[4] for r in valid_closed
                      if (r[1] or 0) >= MIN_ANNUALIZED_HOLD_H and r[4] is not None]
        out.update({
            "avg_hold_days": round(sum(holds) / len(holds), 1) if holds else None,
            "avg_funding_accrued_pct": round(sum(accrued) / len(accrued), 4),
            "avg_cost_pct": round(sum(costs) / len(costs), 4),
            "avg_net_return_pct": round(sum(nets) / len(nets), 4),
            "avg_predicted_ann_pct": round(sum(preds) / len(preds), 1) if preds else None,
            "annualized_n": len(annualized),
            "annualized_min_hold_days": MIN_ANNUALIZED_HOLD_H // 24,
            "recent": [{"symbol": r[0], "hold_days": round((r[1] or 0) / 24, 1),
                        "entry_at": r[7], "closed_at": r[8],
                        "exit_signal_at": r[14], "exit_quote_at": r[15],
                        "exit_quote_delay_s": r[16],
                        "close_reason": r[9] or "legacy_unknown",
                        "funding_accrued_pct": round(r[6] or 0, 4),
                        "cost_pct": round((r[2] or 0) + (r[3] or 0)
                                          + 2 * FEE_PCT_ONEWAY_BOTHLEGS, 4),
                        "net_return_pct": round((r[6] or 0) - (r[2] or 0) - (r[3] or 0)
                                                - 2 * FEE_PCT_ONEWAY_BOTHLEGS, 4)}
                       for r in valid_closed[-8:]],
        })
        if len(annualized) >= MIN_ANNUALIZED_SAMPLES:
            out["avg_annualized_net_pct"] = round(sum(annualized) / len(annualized), 1)
        else:
            out["annualized_note"] = (
                f"年化隐藏:需至少{MIN_ANNUALIZED_SAMPLES}个持有≥"
                f"{MIN_ANNUALIZED_HOLD_H // 24}天的已平仓样本"
            )
    return out


if __name__ == "__main__":
    from dotenv import load_dotenv

    from src.config import PROJECT_ROOT
    load_dotenv(PROJECT_ROOT / ".env")
    from src.onchain.hyperliquid import scan_carry
    scan = scan_carry(priority_symbols=open_symbols())
    print(json.dumps(run(scan["signals"], observations=scan["open_observations"]),
                     ensure_ascii=False, indent=1))
