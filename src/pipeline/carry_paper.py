"""Read-only quote-proxy tracker for the cross-venue funding Carry hypothesis.

This measures differential persistence and order-book impact without claiming fills,
funding settlements, basis PnL, account fees, collateral cost or real profitability:

  · opens a paper position for every fat-net cross-venue carry not already open,
    snapshotting entry slippage from the LIVE order books (HL + OKX),
  · accrues the observed funding differential only between consecutive valid updates;
    source gaps are measured explicitly and never backfilled as profit,
  · closes the paper episode when the differential decays below a floor, producing an
    auditable quote-rate integral minus measured book impact and a modeled fee proxy.

No real orders are placed. Every output remains ineligible for a real-edge verdict until
actual settlements, equal-base-quantity basis accounting and complete costs exist.
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
OPEN_MIN_PARTIAL_MODEL_PROXY_ANN = 8.0  # entry screen only; not a real-edge threshold
CLOSE_DIFF_FLOOR = 2.0     # differential decayed below this (ann %) → natural exit
# Fee tiers depend on the actual account and maker/taker path. This is only a disclosed
# proxy assumption: one direction across both legs = HL 0.045% + OKX 0.05%.
MODELED_FEE_PCT_ONEWAY_BOTH_LEGS = 0.095
MODELED_ROUNDTRIP_FEE_PCT = 2 * MODELED_FEE_PCT_ONEWAY_BOTH_LEGS
CURRENT_EPISODE_VERSION = 3
CARRY_EXIT_QUOTE_SLA_S = 60
CARRY_OBSERVATION_MAX_AGE_S = 60
MIN_ANNUALIZED_HOLD_H = 30 * 24
MIN_ANNUALIZED_SAMPLES = 5
_OKX_CTVAL: dict[str, float] = {}
_OKX_META_LOADED = False


def proxy_exclusion_reasons(sample: dict) -> list[str]:
    """Return reasons a close cannot enter the descriptive quote-proxy cohort."""
    from src.pipeline.execution_cost import validate as validate_cost_contract

    reasons: list[str] = []
    if sample.get("episode_version") != CURRENT_EPISODE_VERSION:
        reasons.append("legacy_episode")
    if sample.get("observation_version") != 1:
        reasons.append("unverified_observation_method")
    close_reason = sample.get("close_reason")
    if close_reason == "market_missing":
        reasons.append("market_missing_close")
    elif close_reason != "diff_below_floor":
        reasons.append("invalid_close_reason")
    if (sample.get("book_quote_cost_complete") is not True
            or sample.get("entry_book_impact_pct") is None
            or sample.get("exit_book_impact_pct") is None):
        reasons.append("incomplete_book_quote_cost")
    try:
        contract = validate_cost_contract(sample.get("cost_contract"))
    except (TypeError, ValueError):
        contract = None
    if (contract is None or contract.get("purpose") != "paper_measurement"
            or contract.get("completeness") != "partial"
            or contract.get("all_in_total_pct") is not None
            or contract.get("is_real_fill") is not False
            or contract.get("book_quote_cost_complete")
            is not sample.get("book_quote_cost_complete")):
        reasons.append("invalid_partial_cost_contract")
    try:
        quote_delay_s = float(sample["exit_quote_delay_s"])
        if (not math.isfinite(quote_delay_s) or quote_delay_s < 0
                or quote_delay_s > CARRY_EXIT_QUOTE_SLA_S):
            reasons.append("exit_quote_outside_sla")
    except (KeyError, TypeError, ValueError):
        reasons.append("exit_quote_outside_sla")
    try:
        unmeasured_h = float(sample["unmeasured_h"])
        if not math.isfinite(unmeasured_h) or unmeasured_h < 0 or unmeasured_h > 1e-9:
            reasons.append("incomplete_quote_rate_path")
    except (KeyError, TypeError, ValueError):
        reasons.append("incomplete_quote_rate_path")
    if (sample.get("hold_h") is None or sample.get("quoted_rate_integral_pct") is None
            or sample.get("net_proxy_after_book_quotes_and_modeled_fee_pct") is None):
        reasons.append("missing_result")
    else:
        try:
            hold_h = float(sample["hold_h"])
            if not math.isfinite(hold_h) or hold_h < 0:
                reasons.append("invalid_hold_period")
        except (KeyError, TypeError, ValueError):
            reasons.append("invalid_hold_period")
    if ("missing_result" not in reasons and "invalid_hold_period" not in reasons
            and contract is not None and contract.get("book_quote_cost_complete") is True):
        try:
            quote_integral = float(sample["quoted_rate_integral_pct"])
            net_proxy = float(sample["net_proxy_after_book_quotes_and_modeled_fee_pct"])
            proxy_cost = float(contract["modeled_proxy_total_pct"])
            valid_numbers = all(math.isfinite(value) for value in (
                quote_integral, net_proxy, proxy_cost))
        except (KeyError, TypeError, ValueError):
            valid_numbers = False
        if (not valid_numbers
                or not math.isclose(net_proxy, quote_integral - proxy_cost,
                                    rel_tol=1e-9, abs_tol=1e-6)):
            reasons.append("inconsistent_proxy_math")
    return reasons


def edge_exclusion_reasons(sample: dict) -> list[str]:
    """Compatibility alias; this cohort is descriptive and never proves real edge."""
    return proxy_exclusion_reasons(sample)


def _conn() -> sqlite3.Connection:
    DB.parent.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(str(DB), timeout=10)
    # ``accrued_pct``, ``realized_net`` and ``cost_complete`` are legacy SQLite
    # column names kept to avoid rewriting an operational database.  Public v3
    # output maps them to quote-rate integral, annualized proxy and book-quote
    # completeness; none of the three represents a settlement, fill or all-in PnL.
    c.execute("""CREATE TABLE IF NOT EXISTS paper(
        id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT, entry_ts TEXT,
        entry_diff REAL, pred_net REAL, entry_slip REAL, notional REAL,
        accrued_pct REAL DEFAULT 0, last_ts TEXT, last_diff REAL,
        status TEXT DEFAULT 'open', exit_ts TEXT, exit_slip REAL,
        hold_h REAL, realized_net REAL, close_reason TEXT,
        last_attempt_ts TEXT, last_valid_ts TEXT, unmeasured_h REAL DEFAULT 0,
        measurement_state TEXT DEFAULT 'observed',
        episode_version INTEGER DEFAULT 3, cost_complete INTEGER DEFAULT 0,
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
    from src.pipeline.execution_cost import carry_paper_contract

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
        legacy_active = (
            status in {"open", "exit_pending"}
            and (episode_version != CURRENT_EPISODE_VERSION or observation_version != 1)
        )
        sync_status = "quarantined" if legacy_active else status
        measurement_notional = float(notional or NOTIONAL)
        entry_contract = carry_paper_contract(
            notional_usd_per_leg=measurement_notional,
            entry_book_impact_pct=entry_slip,
            exit_book_impact_pct=None,
            modeled_fee_pct=MODELED_ROUNDTRIP_FEE_PCT,
        )
        outcome_contract = carry_paper_contract(
            notional_usd_per_leg=measurement_notional,
            entry_book_impact_pct=entry_slip,
            exit_book_impact_pct=exit_slip if status == "closed" else None,
            modeled_fee_pct=MODELED_ROUNDTRIP_FEE_PCT,
        )
        candidate = {
            "lane": "carry", "chain": "hyperliquid+okx", "token": symbol,
            "event_key": f"paper:{pid}", "symbol": symbol,
            "source": "Hyperliquid + OKX live order books",
            "event_at": entry_ts, "detected_at": entry_ts, "decision_at": entry_ts,
            "quote_at": entry_ts, "state": f"paper_{sync_status}",
            "decision": "WATCH", "max_notional_usd": None,
            "measurement_notional_usd_per_leg": measurement_notional,
            "measurement_gross_notional_usd": measurement_notional * 2,
            "position_limit_status": "unknown",
            "action_level": "A1_WATCH", "actionable_now": False,
            "auto_execution_allowed": False,
            "action_reason_codes": ["paper_measurement_not_position_limit"],
            "entry_diff_ann_pct": entry_diff,
            "predicted_partial_model_net_ann_pct": pred_net,
            "entry_book_impact_pct": entry_slip,
            "cost_contract": entry_contract,
            "cost_model": "cross_perp_paper_quote_proxy_v1",
            "prediction_model": "entry_snapshot_partial_cost_scenario_unversioned",
            "prediction_cost_completeness": "partial",
            "execution_mode": "paper_orderbook_measurement",
            "exit_diff_floor_ann_pct": CLOSE_DIFF_FLOOR,
            "paper_position_id": pid,
            "episode_version": episode_version,
            "observation_version": observation_version,
        }
        ident, _ = opportunity_ledger.record(candidate)
        outcome = {
            "version": CURRENT_EPISODE_VERSION if episode_version == CURRENT_EPISODE_VERSION else 1,
            "episode_version": episode_version,
            "observation_version": observation_version,
            "kind": "delta_neutral_carry_paper",
            "execution_mode": "paper_orderbook_measurement",
            "cost_is_real_fill": False, "status": sync_status,
            "cost_contract": outcome_contract,
            "quoted_rate_integral_pct": accrued_pct or 0,
            "settled_funding_pct": None, "basis_pnl_pct": None,
            "realized_net_return_pct": None,
            "entry_book_impact_pct": entry_slip,
            "last_diff_ann_pct": last_diff, "last_measured_at": last_valid_ts,
            "last_attempt_at": last_attempt_ts, "last_valid_at": last_valid_ts,
            "unmeasured_h": unmeasured_h or 0,
            "measurement_state": measurement_state or "observed",
            "book_quote_cost_complete": outcome_contract["book_quote_cost_complete"],
            "all_in_cost_complete": False,
            "real_edge_eligible": False,
            "exit_signal_at": exit_signal_ts, "exit_signal_diff_ann_pct": exit_signal_diff,
            "exit_quote_at": exit_quote_ts, "exit_quote_delay_s": exit_quote_delay_s,
        }
        state = "open"
        if sync_status == "quarantined":
            outcome.update({
                "quarantine_reason": close_reason or "legacy_observation_protocol",
                "proxy_sample_eligible": False,
                "edge_sample_eligible": False,
                "real_edge_eligible": False,
            })
            state = "unresolvable"
        elif sync_status == "closed":
            outcome.update({
                "closed_at": exit_ts, "hold_h": hold_h,
                "close_reason": close_reason or "legacy_unknown",
            })
            if outcome_contract["book_quote_cost_complete"]:
                book_quote_cost_pct = entry_slip + exit_slip
                proxy_cost_pct = book_quote_cost_pct + MODELED_ROUNDTRIP_FEE_PCT
                outcome.update({
                    "exit_book_impact_pct": exit_slip,
                    "book_quote_cost_pct": book_quote_cost_pct,
                    "modeled_fee_proxy_pct": MODELED_ROUNDTRIP_FEE_PCT,
                    "book_and_modeled_fee_proxy_pct": proxy_cost_pct,
                    "net_proxy_after_book_quotes_and_modeled_fee_pct":
                        (accrued_pct or 0) - proxy_cost_pct,
                    "annualized_net_proxy_pct": realized_net,
                })
            else:
                outcome["proxy_exclusion_reason"] = "incomplete_book_quote_cost"
            reasons = proxy_exclusion_reasons(outcome)
            outcome["cost_completeness"] = outcome_contract["completeness"]
            outcome["proxy_exclusion_reasons"] = reasons
            outcome["proxy_sample_eligible"] = not reasons
            outcome["edge_sample_eligible"] = False
            annualized_mature = (
                isinstance(hold_h, (int, float)) and math.isfinite(hold_h)
                and hold_h >= MIN_ANNUALIZED_HOLD_H
            )
            outcome["annualized_proxy_eligible"] = annualized_mature
            outcome["annualized_proxy_min_hold_h"] = MIN_ANNUALIZED_HOLD_H
            if not annualized_mature:
                outcome["annualized_net_proxy_pct"] = None
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
    """Symbols with a current-protocol open episode, in stable creation order."""
    c = _conn()
    try:
        return [row[0] for row in c.execute(
            "SELECT symbol FROM paper WHERE status IN ('open','exit_pending') "
            "AND episode_version=? AND observation_version=1 ORDER BY id",
            (CURRENT_EPISODE_VERSION,),
        ).fetchall()]
    finally:
        c.close()


def run(carries: list[dict], *, observations: list[dict] | None = None) -> dict:
    """Open from ranked candidates; accrue/close only from paired current observations.

    ``observations=[]`` or ``None`` is an explicit source gap. Only observation protocol
    v1 may open, accrue or close a v3 episode; older caller shapes fail closed.
    """
    now = datetime.now(timezone.utc)
    entry_by_sym = {item["symbol"]: item for item in carries if item.get("cross")}
    raw_observations = observations or []
    observed_by_sym: dict[str, dict] = {}
    for observation in raw_observations:
        if (observation.get("status") != "observed" or not observation.get("cross")
                or observation.get("observation_version") != 1):
            continue
        try:
            edge = float(observation["observed_edge_ann"])
            observed_at = datetime.fromisoformat(str(observation["observed_at"]))
        except (KeyError, TypeError, ValueError):
            continue
        if observed_at.tzinfo is None or observed_at.utcoffset() is None:
            continue
        observation_age_s = (now - observed_at).total_seconds()
        if (not math.isfinite(edge) or not math.isfinite(observation_age_s)
                or observation_age_s < 0
                or observation_age_s > CARRY_OBSERVATION_MAX_AGE_S):
            continue
        observed_by_sym[observation["symbol"]] = {**observation,
                                                   "observed_edge_ann": edge,
                                                   "observation_age_s": observation_age_s}
    c = _conn()
    try:
        # A historical open row cannot be upgraded in place: its entry snapshot and
        # already-integrated path were produced under an unknown protocol. Preserve it
        # as an auditable, unresolvable row, but never let it accrue, close, request
        # quotes, or block a fresh v3 episode for the same symbol.
        c.execute(
            "UPDATE paper SET status='quarantined',"
            "measurement_state='legacy_quarantined',"
            "close_reason='legacy_observation_protocol' "
            "WHERE status IN ('open','exit_pending') AND "
            "(episode_version IS NULL OR episode_version<>? OR "
            "observation_version IS NULL OR observation_version<>1)",
            (CURRENT_EPISODE_VERSION,),
        )
        open_rows = {r[1]: r for r in c.execute(
            "SELECT id,symbol,entry_ts,entry_diff,pred_net,entry_slip,notional,accrued_pct,"
            "last_ts,last_diff,last_attempt_ts,last_valid_ts,unmeasured_h,measurement_state,"
            "status,exit_signal_ts,exit_signal_diff FROM paper "
            "WHERE status IN ('open','exit_pending') AND episode_version=? "
            "AND observation_version=1",
            (CURRENT_EPISODE_VERSION,),
        ).fetchall()}
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
                book_quotes_complete = entry_slip is not None
                hold_yr = max(hold_h / 8760.0, 1e-6)
                proxy_cost = ((entry_slip + measured_exit_slip
                               + MODELED_ROUNDTRIP_FEE_PCT)
                              if book_quotes_complete else None)
                net_proxy_ann = ((accrued or 0) / hold_yr - proxy_cost / hold_yr
                                 if proxy_cost is not None else None)
                c.execute(
                    "UPDATE paper SET status='closed',exit_ts=?,exit_slip=?,hold_h=?,"
                    "realized_net=?,close_reason='diff_below_floor',last_attempt_ts=?,"
                    "measurement_state='observed',cost_complete=?,exit_quote_ts=?,"
                    "exit_quote_delay_s=? WHERE id=?",
                    (signal_dt.isoformat(), measured_exit_slip, hold_h, net_proxy_ann,
                     now.isoformat(), int(book_quotes_complete), now.isoformat(),
                     quote_delay_s, pid),
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
            # Left-rectangle quote-rate integral, not exchange funding settlements.
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
                book_quotes_complete = entry_slip is not None
                # Annualized proxy = quote-rate integral minus read-only book impact and
                # a modeled account-fee assumption. Basis/collateral/rebalance are absent.
                hold_yr = max(hold_h / 8760.0, 1e-6)
                proxy_cost = ((entry_slip + measured_exit_slip
                               + MODELED_ROUNDTRIP_FEE_PCT)
                              if book_quotes_complete else None)
                net_proxy_ann = (accrued / hold_yr - proxy_cost / hold_yr
                                 if proxy_cost is not None else None)
                c.execute("UPDATE paper SET status='closed', exit_ts=?, exit_slip=?, hold_h=?, "
                          "accrued_pct=?, realized_net=?, last_ts=?, last_diff=?,"
                          "close_reason='diff_below_floor',last_attempt_ts=?,last_valid_ts=?,"
                          "unmeasured_h=?,measurement_state='observed',cost_complete=?,"
                          "exit_signal_ts=?,exit_signal_diff=?,exit_quote_ts=?,"
                          "exit_quote_delay_s=0 WHERE id=?",
                          (now.isoformat(), measured_exit_slip, hold_h, accrued, net_proxy_ann,
                           now.isoformat(), cur_diff, now.isoformat(), now.isoformat(),
                           unmeasured_h or 0, int(book_quotes_complete), now.isoformat(), cur_diff,
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
            try:
                partial_proxy = float(cur["partial_model_proxy_ann_pct"])
            except (KeyError, TypeError, ValueError):
                continue
            if (sym in open_rows or not math.isfinite(partial_proxy)
                    or partial_proxy < OPEN_MIN_PARTIAL_MODEL_PROXY_ANN):
                continue
            observation = observed_by_sym.get(sym)
            if observation is None:
                continue
            # Persistence can rank a candidate, but a new episode must also clear the
            # exact same partial-cost screen on the fresh paired quote. Otherwise a
            # collapsed differential could open below its own natural exit threshold.
            from src.onchain.hyperliquid import _carry_partial_model_proxy_ann
            current_partial_proxy = _carry_partial_model_proxy_ann(
                observation["observed_edge_ann"])
            if (not math.isfinite(current_partial_proxy)
                    or current_partial_proxy < OPEN_MIN_PARTIAL_MODEL_PROXY_ANN):
                continue
            slip = _roundtrip_slip(sym, phase="entry")
            if slip is None:
                continue                      # can't measure entry → don't open
            c.execute("INSERT INTO paper(symbol,entry_ts,entry_diff,pred_net,entry_slip,"
                      "notional,accrued_pct,last_ts,last_diff,last_attempt_ts,last_valid_ts,"
                      "unmeasured_h,measurement_state,episode_version,cost_complete,"
                      "observation_version) VALUES (?,?,?,?,?,?,0,?,?,?,?,0,'observed',?,0,?)",
                      (sym, now.isoformat(), observation["observed_edge_ann"], partial_proxy,
                       slip, NOTIONAL, now.isoformat(), observation["observed_edge_ann"],
                       now.isoformat(), now.isoformat(), CURRENT_EPISODE_VERSION,
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
    """Report a descriptive quote-proxy cohort, never realized or all-in PnL."""
    from src.pipeline.execution_cost import carry_paper_contract

    c = _conn()
    try:
        closed_all = c.execute("SELECT symbol,hold_h,entry_slip,exit_slip,realized_net,pred_net,"
                           "accrued_pct,entry_ts,exit_ts,close_reason,episode_version,cost_complete,"
                           "unmeasured_h,observation_version,exit_signal_ts,exit_quote_ts,"
                           "exit_quote_delay_s,notional "
                           "FROM paper WHERE status='closed'").fetchall()
        opened = c.execute(
            "SELECT symbol,entry_ts,last_ts,entry_diff,last_diff,pred_net,entry_slip,notional,"
            "last_attempt_ts,last_valid_ts,unmeasured_h,measurement_state,episode_version,"
            "observation_version,status,exit_signal_ts,exit_signal_diff "
            "FROM paper WHERE status IN ('open','exit_pending') AND episode_version=? "
            "AND observation_version=1 ORDER BY entry_ts DESC",
            (CURRENT_EPISODE_VERSION,),
        ).fetchall()
        quarantined_total = c.execute(
            "SELECT COUNT(*) FROM paper WHERE status='quarantined' OR "
            "(status IN ('open','exit_pending') AND "
            "(episode_version IS NULL OR episode_version<>? OR "
            "observation_version IS NULL OR observation_version<>1))",
            (CURRENT_EPISODE_VERSION,),
        ).fetchone()[0]
    finally:
        c.close()

    def as_sample(row: tuple) -> dict:
        book_complete = bool(row[11])
        contract = carry_paper_contract(
            notional_usd_per_leg=float(row[17] or NOTIONAL),
            entry_book_impact_pct=row[2], exit_book_impact_pct=row[3],
            modeled_fee_pct=MODELED_ROUNDTRIP_FEE_PCT,
        )
        net_proxy = ((row[6] or 0) - row[2] - row[3] - MODELED_ROUNDTRIP_FEE_PCT
                     if book_complete and row[2] is not None and row[3] is not None else None)
        return {
            "episode_version": row[10], "close_reason": row[9],
            "entry_book_impact_pct": row[2], "exit_book_impact_pct": row[3],
            "book_quote_cost_complete": book_complete,
            "cost_contract": contract, "unmeasured_h": row[12],
            "observation_version": row[13],
            "exit_quote_delay_s": row[16],
            "hold_h": row[1], "quoted_rate_integral_pct": row[6],
            "net_proxy_after_book_quotes_and_modeled_fee_pct": net_proxy,
        }

    reasons_by_row = [proxy_exclusion_reasons(as_sample(row)) for row in closed_all]
    proxy_closed = [row for row, reasons in zip(closed_all, reasons_by_row) if not reasons]
    excluded: dict[str, int] = {}
    for reasons in reasons_by_row:
        for reason in reasons:
            excluded[reason] = excluded.get(reason, 0) + 1
    out = {
        "cohort_kind": "descriptive_quote_proxy",
        "n_open": len(opened), "n_closed": len(proxy_closed),
        "n_proxy_closed": len(proxy_closed), "real_edge_n": 0,
        "n_exit_pending": sum(row[14] == "exit_pending" for row in opened),
        "n_quarantined_total": quarantined_total,
        "n_closed_total": len(closed_all),
        "n_closed_excluded": len(closed_all) - len(proxy_closed),
        "excluded_by_reason": excluded,
        "exit_rule": f"valid paired observation: differential < {CLOSE_DIFF_FLOOR}% ann",
        "exit_quote_sla_s": CARRY_EXIT_QUOTE_SLA_S,
        "cost_completeness": "partial", "all_in_total_pct": None,
        "is_real_fill": False, "real_edge_eligible": False,
        "open_positions": [
            {
                "symbol": row[0], "entry_at": row[1], "last_measured_at": row[9],
                "entry_diff_ann_pct": row[3], "last_diff_ann_pct": row[4],
                "predicted_partial_model_net_ann_pct": row[5],
                "entry_book_impact_pct": row[6],
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
                "cost_contract": carry_paper_contract(
                    notional_usd_per_leg=float(row[7] or NOTIONAL),
                    entry_book_impact_pct=row[6], exit_book_impact_pct=None,
                    modeled_fee_pct=MODELED_ROUNDTRIP_FEE_PCT,
                ),
                "settled_funding_pct": None, "basis_pnl_pct": None,
                "realized_net_return_pct": None, "real_edge_eligible": False,
            }
            for row in opened
        ],
    }
    if proxy_closed:
        holds = [r[1] / 24 for r in proxy_closed if r[1] is not None]
        costs = [(r[2] or 0) + (r[3] or 0) + MODELED_ROUNDTRIP_FEE_PCT
                 for r in proxy_closed]
        quote_integrals = [r[6] or 0 for r in proxy_closed]
        net_proxies = [integral - cost for integral, cost in zip(quote_integrals, costs)]
        preds = [r[5] for r in proxy_closed if r[5] is not None]
        annualized = [r[4] for r in proxy_closed
                      if (r[1] or 0) >= MIN_ANNUALIZED_HOLD_H and r[4] is not None]
        out.update({
            "avg_hold_days": round(sum(holds) / len(holds), 1) if holds else None,
            "avg_quoted_rate_integral_pct": round(
                sum(quote_integrals) / len(quote_integrals), 4),
            "avg_book_and_modeled_fee_proxy_pct": round(sum(costs) / len(costs), 4),
            "avg_net_proxy_pct": round(sum(net_proxies) / len(net_proxies), 4),
            "avg_predicted_partial_model_ann_pct": (
                round(sum(preds) / len(preds), 1) if preds else None),
            "annualized_proxy_n": len(annualized),
            "annualized_proxy_min_hold_days": MIN_ANNUALIZED_HOLD_H // 24,
            "recent": [{"symbol": r[0], "hold_days": round((r[1] or 0) / 24, 1),
                        "entry_at": r[7], "closed_at": r[8],
                        "exit_signal_at": r[14], "exit_quote_at": r[15],
                        "exit_quote_delay_s": r[16],
                        "close_reason": r[9] or "legacy_unknown",
                        "quoted_rate_integral_pct": round(r[6] or 0, 4),
                        "book_and_modeled_fee_proxy_pct": round(
                            (r[2] or 0) + (r[3] or 0) + MODELED_ROUNDTRIP_FEE_PCT, 4),
                        "net_proxy_pct": round(
                            (r[6] or 0) - (r[2] or 0) - (r[3] or 0)
                            - MODELED_ROUNDTRIP_FEE_PCT, 4),
                        "cost_contract": carry_paper_contract(
                            notional_usd_per_leg=float(r[17] or NOTIONAL),
                            entry_book_impact_pct=r[2], exit_book_impact_pct=r[3],
                            modeled_fee_pct=MODELED_ROUNDTRIP_FEE_PCT,
                        ),
                        "settled_funding_pct": None, "basis_pnl_pct": None,
                        "realized_net_return_pct": None, "real_edge_eligible": False}
                       for r in proxy_closed[-8:]],
        })
        if len(annualized) >= MIN_ANNUALIZED_SAMPLES:
            out["avg_annualized_net_proxy_pct"] = round(
                sum(annualized) / len(annualized), 1)
        else:
            out["annualized_proxy_note"] = (
                f"年化代理隐藏:需至少{MIN_ANNUALIZED_SAMPLES}个持有≥"
                f"{MIN_ANNUALIZED_HOLD_H // 24}天的描述性关闭样本"
            )
    return out


if __name__ == "__main__":
    from dotenv import load_dotenv

    from src.config import PROJECT_ROOT
    load_dotenv(PROJECT_ROOT / ".env")
    from src.onchain.hyperliquid import scan_carry
    scan = scan_carry(priority_symbols=open_symbols())
    print(json.dumps(run(
        scan["signals"],
        observations=scan.get("paper_observations", scan["open_observations"]),
    ),
                     ensure_ascii=False, indent=1))
