"""Watch a known multi-token operator's FUNDER at the source — the earliest signal.

The SIREN/EVAA/SKYAI family all trace to one funder (0x6596da8b…). When that funder
sends gas to a NEW address, that address is a likely fresh operator wallet seeding
the next shell token — earlier than any on-chain concentration signal. This tracks
the funder's outbound fundees and reports ones not seen before.

Discipline: a failed fetch returns [] and must NOT wipe the baseline (failure ≠
"no fundees") — else every transient error would re-flag the whole set as "new".
"""

from __future__ import annotations

import json
from pathlib import Path

import structlog

from src.config import DATA_DIR
from src.onchain import moralis_client
from src.onchain.funder_graph import _MORALIS_CHAINS, MIN_FUNDER_VALUE_WEI

logger = structlog.get_logger()

# Multi-token operator roots. HARD RULE (2026-07-01 audit): verify against Dune
# labels.owner_addresses BEFORE adding — the two previous "SIREN family roots"
# turned out to be Gate.io and Binance hot wallets (a CEX hot wallet funds
# thousands of withdrawal wallets; watching it = watching exchange traffic).
WATCHED_FUNDERS = {
    # BASED's cluster funder: a multisig that seeded all 9 uniform-band wallets and
    # itself holds 5.17M BASED (#3 holder). No exchange label on Dune — a genuine
    # operator root. If it seeds fresh wallets that converge on a new token, that
    # is the next shell.
    "0xfd09a9cc989cd9d7ff0a1cab6af28c677267a2b9": "BASED operator funder (multisig)",
}

_STATE = DATA_DIR / "funder_watch.json"


def funded_addresses(funder: str, chain: str = "bsc", limit: int = 100) -> list[dict]:
    """Recent addresses the funder sent native gas to (value ≥ funding threshold).
    Returns [{to, ts}] newest-first; [] on no key / no data / failure."""
    mchain = _MORALIS_CHAINS.get(chain)
    if not moralis_client.available() or not mchain:
        return []
    data = moralis_client.get(f"{funder}?chain={mchain}&order=DESC&limit={limit}")
    if not data:
        return []
    fl = funder.lower()
    out: list[dict] = []
    for tx in data.get("result", []):
        frm = (tx.get("from_address") or "").lower()
        to = (tx.get("to_address") or "").lower()
        try:
            value = int(tx.get("value", "0") or 0)
        except (ValueError, TypeError):
            value = 0
        if frm == fl and to and value >= MIN_FUNDER_VALUE_WEI:
            out.append({"to": to, "ts": tx.get("block_timestamp", "")})
    return out


def shell_convergence(wallets: list[str], chain: str = "bsc",
                      min_wallets: int = 2) -> list[dict]:
    """Do >= min_wallets of these (freshly-funded) wallets hold the SAME non-major
    token? That convergence — not the bare funding event — is the real new-shell
    signal. A high-fan-out funder (0x6596da8b funds 89+ addrs, looks disperser-ish)
    makes lone 'new fundee' events noisy; convergence separates a real coordinated
    shell from gas top-ups / unrelated withdrawals. Returns [{token, symbol, holders}]."""
    from src.onchain.token_registry import is_non_operator
    holders: dict[str, list[str]] = {}
    meta: dict[str, str] = {}
    for w in wallets:
        d = moralis_client.get(f"{w}/erc20?chain={_MORALIS_CHAINS.get(chain, chain)}")
        if not d:
            continue
        rows = d if isinstance(d, list) else d.get("result", d)
        for t in (rows or []):
            ca = (t.get("token_address") or "").lower()
            sym = t.get("symbol") or "?"
            try:
                bal = float(t.get("balance", 0)) / (10 ** int(t.get("decimals", 18)))
            except (ValueError, TypeError):
                bal = 0
            if ca and bal > 0 and not is_non_operator(sym):
                holders.setdefault(ca, []).append(w)
                meta[ca] = sym
    return [{"token": ca, "symbol": meta[ca], "holders": ws}
            for ca, ws in holders.items() if len(set(ws)) >= min_wallets]


def _load_state() -> dict:
    if _STATE.exists():
        try:
            return json.loads(_STATE.read_text())
        except Exception:
            return {}
    return {}


def _save_state(state: dict) -> None:
    _STATE.parent.mkdir(parents=True, exist_ok=True)
    _STATE.write_text(json.dumps(state, ensure_ascii=False))


def check_new_fundees(chain: str = "bsc") -> dict:
    """For each watched funder: detect addresses funded since baseline, and — only
    when there ARE new ones — check whether the recent fundees CONVERGE on a common
    new token (the real new-shell signal; bare funding is noisy at high fan-out).

    Returns {"new_fundees": [...], "shell_candidates": [...]}. First run just records
    the baseline (both empty). Never wipes baseline on a failed fetch."""
    state = _load_state()
    new_fundees: list[dict] = []
    shell_candidates: list[dict] = []
    for funder, label in WATCHED_FUNDERS.items():
        rec = state.get(funder, {})
        seen = set(rec.get("fundees", []))
        current = funded_addresses(funder, chain)
        if not current:                       # fetch failed → keep baseline, skip
            continue
        first_run = not seen
        fresh = [f for f in current if f["to"] not in seen]
        state[funder] = {
            "fundees": sorted(seen.union(f["to"] for f in current)),
            "label": label,
            "last_check_ts": current[0]["ts"],
        }
        if fresh and not first_run:
            new_fundees.extend({"funder": funder, "label": label, **f} for f in fresh)
            # Convergence over the recent fundee window (fresh + a few prior) = shell.
            window = [f["to"] for f in current[:18]]
            for c in shell_convergence(window, chain):
                shell_candidates.append({"funder": funder, "label": label, **c})
    _save_state(state)
    return {"new_fundees": new_fundees, "shell_candidates": shell_candidates}
