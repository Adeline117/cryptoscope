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

# Multi-token operator family roots. Add more as they're identified.
WATCHED_FUNDERS = {
    "0x6596da8b65995d5feacff8c2936f0b7a2051b0d0": "SIREN/EVAA/SKYAI family",
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


def check_new_fundees(chain: str = "bsc") -> list[dict]:
    """For each watched funder, report addresses funded since the baseline (not seen
    before). The FIRST run just records the baseline and returns [] (all are "new"
    only because we hadn't looked yet — not a real launch signal). New fundees on
    later runs = candidate fresh-shell operator wallets. Never wipes baseline on a
    failed fetch."""
    state = _load_state()
    hits: list[dict] = []
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
            hits.extend({"funder": funder, "label": label, **f} for f in fresh)
    _save_state(state)
    return hits
