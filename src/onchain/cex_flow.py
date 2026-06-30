"""CEX deposit-flow detection — the leading dump signal.

WHY: an operator/team wallet sending its tokens to an exchange DEPOSIT address
PRECEDES the actual dump by minutes-to-hours. By the time the sell prints on the
chart (token → LP/router) the crash is already underway; the deposit transfer is
the *earliest* on-chain warning. This is exactly what CryptoQuant's "Exchange
Whale Ratio" and Nansen's "CEX Token Flow" key on — and what would have flagged
the ESPORTS team-multisig dump BEFORE the price collapse.

The hard part is that exchanges don't receive into one labeled hot wallet; each
user (and each operator) deposits into a unique, freshly-created DEPOSIT address
that later sweeps into the exchange's hot wallet. So we detect a CEX deposit two
ways:
  1. DIRECT — the destination is a known CEX hot/deposit wallet
     (`cex_addresses.evm_exchanges()` / `solana_exchanges()`).
  2. DISCOVERY (1-hop) — the destination is an unlabeled intermediary that itself
     only forwards the same token onward to a KNOWN CEX hot wallet. A fresh
     address whose sole job is "receive from operator, sweep to Binance" IS a
     deposit address even though no list contains it yet. We do this cheaply by
     reusing the token Transfer logs we already fetched (no extra RPC fan-out).

Defensive contract (the project's root-cure principle): a failed/partial log scan
is UNKNOWN, never a confident "no CEX flow". We set `complete=False` and never
fire `has_signal` off incomplete data — an RPC outage must not read as "clean".
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import structlog

logger = structlog.get_logger()

# Default look-back window: a deposit signal is only actionable while fresh, but
# operators sometimes pre-position the stack hours ahead of the dump, so ~24h.
_DEFAULT_LOOKBACK_H = 24
# Never scan more than this far back (matches the sentinel's ~2-day getLogs cap).
_MAX_LOOKBACK_H = 48
# Fire when the cluster has moved at least this % of its (pre-outflow) stack to a
# CEX in the window. Low, because a deposit is intent-revealing even at small size.
_SIGNAL_PCT = 1.0
# Bound the 1-hop discovery: only probe the largest few non-CEX destinations.
_MAX_DISCOVERY = 4
# A discovered intermediary counts as a deposit address only if this share of its
# onward token flow lands on a known CEX hot wallet (a pure pass-through).
_FORWARD_SHARE = 0.80

_MORALIS_EVM = {"bsc": "bsc", "ethereum": "eth", "base": "base",
                "arbitrum": "arbitrum", "optimism": "optimism", "polygon": "polygon"}

# Label substrings that mark a Moralis-labeled destination as an exchange — used as
# a no-cost enrichment when Moralis labels are available (mirrors the sentinel's
# _classify_outflow venue check). Missing labels only cause under-detection.
_CEX_LABEL_HINTS = ("binance", "okx", "okex", "gate", "mexc", "kucoin", "bybit",
                    "bitget", "coinbase", "kraken", "huobi", "htx", "bitfinex",
                    "crypto.com", "exchange", "hotwallet", "hot wallet", "deposit")


def _exchange_set(chain: str) -> dict[str, str]:
    """Known CEX hot/deposit addresses for the chain → label. EVM keys are
    lowercased; Solana keys keep base58 case (case-sensitive)."""
    try:
        from src.onchain.cex_addresses import evm_exchanges, solana_exchanges
        if chain in ("solana", "sol"):
            return solana_exchanges()
        return evm_exchanges()
    except Exception as e:  # never raise — under-detect, don't crash
        logger.debug("cex_set_load_failed", chain=chain, error=str(e)[:80])
        return {}


def is_cex_address(addr: str, chain: str) -> bool:
    """True if `addr` is a KNOWN exchange hot/deposit wallet on `chain`."""
    if not addr:
        return False
    s = _exchange_set(chain)
    if chain in ("solana", "sol"):
        return addr in s
    return addr.lower() in s


def _cex_label(addr: str, chain: str) -> str | None:
    if not addr:
        return None
    s = _exchange_set(chain)
    return s.get(addr if chain in ("solana", "sol") else addr.lower())


def looks_like_deposit_address(addr: str, chain: str,
                               forward_map: dict[str, list[tuple[str, float]]] | None = None) -> bool:
    """Heuristic: is `addr` a CEX DEPOSIT address (a fresh pass-through that only
    sweeps onward to a known exchange hot wallet)?

    `forward_map` is an address → [(to, amount), ...] index of the same token's
    Transfer logs we already pulled. If supplied we answer for free: `addr` is a
    deposit address when the dominant share of its onward token flow lands on a
    known CEX. Without a forward_map we can only confirm addresses already on the
    known list (we never do an unbounded extra fetch here — callers bound cost)."""
    if is_cex_address(addr, chain):
        return True
    if not forward_map:
        return False
    onward = forward_map.get(addr if chain in ("solana", "sol") else addr.lower())
    if not onward:
        return False
    total = sum(a for _, a in onward)
    if total <= 0:
        return False
    to_cex = sum(a for to, a in onward if is_cex_address(to, chain))
    return (to_cex / total) >= _FORWARD_SHARE


def _since(since_iso: str | None) -> tuple[str, float]:
    """Resolve the look-back window → (since_iso, since_ts), clamped to _MAX."""
    now = datetime.now(timezone.utc)
    floor = now - timedelta(hours=_MAX_LOOKBACK_H)
    if since_iso:
        try:
            dt = datetime.fromisoformat(since_iso.replace("Z", "+00:00"))
            if dt < floor:
                dt = floor
        except Exception:
            dt = now - timedelta(hours=_DEFAULT_LOOKBACK_H)
    else:
        dt = now - timedelta(hours=_DEFAULT_LOOKBACK_H)
    return dt.isoformat(), dt.timestamp()


def _safe(has_signal: bool, outflow: float, pct: float | None, dests: list,
          detail: str, complete: bool) -> dict:
    return {"has_signal": bool(has_signal),
            "cex_outflow": round(float(outflow), 6),
            "pct_of_cluster": (round(float(pct), 2) if pct is not None else None),
            "destinations": dests,
            "detail": detail,
            "complete": bool(complete)}


def _evm_cluster_balance(token: str, chain: str, wallets: list[str], rpc) -> float | None:
    """Current combined cluster balance (strict: None on any RPC error, never an
    understated total that would inflate pct_of_cluster)."""
    try:
        from src.onchain.evm_archive import combined_balance_at
        return combined_balance_at(token, wallets, chain, rpc.latest_block(),
                                   rpc=rpc, strict=True)
    except Exception as e:
        logger.debug("cex_flow_balance_failed", token=token, error=str(e)[:80])
        return None


def _evm_signal(token: str, chain: str, wallets: list[str], since_iso: str,
                since_ts: float, cluster_balance: float | None) -> dict:
    """EVM path — keyless eth_getLogs spine (Moralis-free, completeness-tracked)."""
    try:
        from src.onchain.evm_archive import ArchiveRPC
    except Exception as e:
        return _safe(False, 0.0, None, [], f"archive import failed: {e}", False)

    rpc = ArchiveRPC(chain)
    if not rpc.available():
        return _safe(False, 0.0, None, [], "no archive RPC for chain", False)

    try:
        head = rpc.logs_head()
        secs = rpc.seconds_per_block()
        blocks_ago = int((datetime.now(timezone.utc).timestamp() - since_ts) / secs) + 50
        cap = int(_MAX_LOOKBACK_H * 3600 / secs)
        blocks_ago = max(50, min(blocks_ago, cap))
        from_block = max(0, head - blocks_ago)
        scale = float(10 ** rpc.token_decimals(token))
        logs = rpc.get_transfer_logs(token, from_block, head)
    except Exception as e:
        return _safe(False, 0.0, None, [], f"log scan error: {str(e)[:80]}", False)

    if not rpc.logs_complete:
        # Partial scan → UNKNOWN. Never assert "no CEX flow" off a failed read.
        return _safe(False, 0.0, None, [], "log scan incomplete — UNKNOWN", False)

    wl = {w.lower() for w in wallets if w}
    # Outbound cluster→external per destination, plus a from→onward index for 1-hop
    # deposit discovery (built from the SAME logs, no extra fetch).
    out_by_dest: dict[str, float] = {}
    forward_map: dict[str, list[tuple[str, float]]] = {}
    for lg in logs:
        t = lg.get("topics", [])
        if len(t) < 3:
            continue
        frm = "0x" + t[1][-40:].lower()
        to = "0x" + t[2][-40:].lower()
        try:
            amt = int(lg.get("data", "0x0"), 16) / scale
        except (ValueError, TypeError):
            continue
        forward_map.setdefault(frm, []).append((to, amt))
        if frm in wl and to not in wl:           # cluster → external = candidate outflow
            out_by_dest[to] = out_by_dest.get(to, 0.0) + amt

    # Classify each destination: direct CEX, or 1-hop discovered deposit address.
    destinations: list[dict] = []
    cex_outflow = 0.0
    # Probe the biggest unlabeled destinations only (bounded discovery).
    ranked = sorted(out_by_dest.items(), key=lambda kv: kv[1], reverse=True)
    discovery_budget = _MAX_DISCOVERY
    for dest, amt in ranked:
        label = _cex_label(dest, chain)
        via = None
        if label:
            via = "direct"
        elif discovery_budget > 0:
            discovery_budget -= 1
            if looks_like_deposit_address(dest, chain, forward_map):
                # find which hot wallet it sweeps to, for the label
                onward = forward_map.get(dest, [])
                hot = next((to for to, _ in onward if is_cex_address(to, chain)), None)
                label = f"deposit→{_cex_label(hot, chain) or 'CEX'}"
                via = "deposit-1hop"
        if via:
            cex_outflow += amt
            destinations.append({"address": dest, "label": label,
                                 "amount": round(amt, 6), "via": via})

    # Optional Moralis label enrichment for any still-unclassified large dest — only
    # if a key is live (never forces a fetch when quota is parked).
    if not destinations:
        cex_outflow, destinations = _moralis_label_pass(
            token, chain, ranked, cex_outflow, destinations)

    # pct of the cluster's PRE-outflow stack (current balance + what already left).
    if cluster_balance is None:
        cluster_balance = _evm_cluster_balance(token, chain, list(wl), rpc)
    pct: float | None
    if cluster_balance is None:
        pct = None
    else:
        base = cluster_balance + cex_outflow
        pct = (cex_outflow / base * 100.0) if base > 0 else 0.0

    if cex_outflow <= 0:
        return _safe(False, 0.0, pct, [], "no cluster→CEX flow in window", True)

    if pct is None:
        # Balance unknown but a real CEX deposit was detected — that is itself the
        # leading signal; fire, but flag that we could not size it.
        return _safe(True, cex_outflow, None, destinations,
                     f"cluster→CEX deposit detected ({len(destinations)} dest); "
                     f"cluster balance unknown — size not gated", True)

    has = pct >= _SIGNAL_PCT
    detail = (f"{'SIGNAL ' if has else ''}cluster→CEX outflow {cex_outflow:.4f} "
              f"= {pct:.2f}% of stack via {len(destinations)} destination(s)")
    return _safe(has, cex_outflow, pct, destinations, detail, True)


def _moralis_label_pass(token: str, chain: str, ranked: list[tuple[str, float]],
                        cex_outflow: float, destinations: list[dict]) -> tuple[float, list[dict]]:
    """Last-resort enrichment: if a Moralis key is live, label the top destinations
    via Moralis' address labels (catches exchanges not on our static list). Cheap:
    one bounded call, skipped entirely when quota is parked."""
    try:
        from src.onchain import moralis_client
    except Exception:
        return cex_outflow, destinations
    mchain = _MORALIS_EVM.get(chain)
    if not mchain or not moralis_client.usable() or not ranked:
        return cex_outflow, destinations
    for dest, amt in ranked[:_MAX_DISCOVERY]:
        try:
            d = moralis_client.get(f"{dest}?chain={mchain}")
            lbl = ((d or {}).get("name") or "").lower() if isinstance(d, dict) else ""
        except Exception:
            lbl = ""
        if lbl and any(h in lbl for h in _CEX_LABEL_HINTS):
            cex_outflow += amt
            destinations.append({"address": dest, "label": lbl,
                                 "amount": round(amt, 6), "via": "moralis-label"})
    return cex_outflow, destinations


def _solana_signal(token: str, wallets: list[str], since_iso: str, since_ts: float,
                   cluster_balance: float | None, max_wallets: int = 8,
                   timeout: int = 15) -> dict:
    """Solana path — per-tx token-balance deltas: a tx where a cluster owner's mint
    balance DROPS and a known CEX owner's balance RISES is a CEX deposit. Known-set
    only (no 1-hop discovery on Solana yet). Defensive: any RPC failure → UNKNOWN."""
    import json
    import urllib.request

    rpc = os.environ.get("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")
    cex = _exchange_set("solana")

    def _call(method, params):
        req = urllib.request.Request(
            rpc, data=json.dumps({"jsonrpc": "2.0", "id": 1, "method": method,
                                  "params": params}).encode(),
            headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode()).get("result")

    wl = set(wallets)
    sigs: dict[str, int] = {}
    try:
        for w in list(wl)[:max_wallets]:
            accs = (_call("getTokenAccountsByOwner",
                          [w, {"mint": token}, {"encoding": "jsonParsed"}]) or {}).get("value", [])
            for acc in accs:
                ata = acc.get("pubkey")
                if not ata:
                    continue
                for s in (_call("getSignaturesForAddress", [ata, {"limit": 40}]) or []):
                    bt = s.get("blockTime")
                    if bt is None or bt <= since_ts:
                        continue
                    sigs[s["signature"]] = bt
    except Exception as e:
        return _safe(False, 0.0, None, [], f"solana sig scan failed: {str(e)[:60]}", False)

    cex_outflow = 0.0
    destinations: dict[str, float] = {}
    complete = True
    for sig in sorted(sigs, key=lambda k: sigs[k])[-60:]:
        try:
            tx = _call("getTransaction",
                       [sig, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}])
        except Exception:
            complete = False
            continue
        if not tx:
            continue
        meta = tx.get("meta") or {}

        def _by_owner(bals):
            m: dict[str, float] = {}
            for b in bals or []:
                if b.get("mint") != token:
                    continue
                o = b.get("owner")
                m[o] = m.get(o, 0.0) + float((b.get("uiTokenAmount") or {}).get("uiAmount") or 0)
            return m
        pre, post = _by_owner(meta.get("preTokenBalances")), _by_owner(meta.get("postTokenBalances"))
        cluster_delta = sum(post.get(o, 0.0) - pre.get(o, 0.0) for o in wl)
        cex_gain = sum(max(0.0, post.get(o, 0.0) - pre.get(o, 0.0)) for o in cex)
        if cluster_delta < -1e-9 and cex_gain > 1e-9:
            moved = min(-cluster_delta, cex_gain)
            cex_outflow += moved
            hot = max(cex, key=lambda o: post.get(o, 0.0) - pre.get(o, 0.0), default="")
            destinations[hot] = destinations.get(hot, 0.0) + moved

    dests = [{"address": a, "label": cex.get(a, "CEX"), "amount": round(v, 6),
              "via": "direct"} for a, v in destinations.items()]
    pct: float | None
    if cluster_balance is None:
        pct = None
    else:
        base = cluster_balance + cex_outflow
        pct = (cex_outflow / base * 100.0) if base > 0 else 0.0

    if cex_outflow <= 0:
        return _safe(False, 0.0, pct, [],
                     "no cluster→CEX flow in window" if complete else "partial scan — UNKNOWN",
                     complete)
    if pct is None:
        return _safe(True, cex_outflow, None, dests,
                     "cluster→CEX deposit detected; balance unknown — size not gated", complete)
    has = (pct >= _SIGNAL_PCT) and complete
    return _safe(has, cex_outflow, pct, dests,
                 f"{'SIGNAL ' if has else ''}cluster→CEX {cex_outflow:.4f} = {pct:.2f}% of stack",
                 complete)


def cex_outflow_signal(token: str, chain: str, wallets: list[str],
                       since_iso: str | None = None,
                       cluster_balance: float | None = None) -> dict:
    """Leading dump signal: did the operator cluster move tokens to a CEX deposit?

    Scans the cluster wallets' OUTBOUND token transfers since `since_iso` (default
    ~24h), classifies each destination as an exchange (known hot/deposit wallet, or
    a 1-hop-discovered deposit address that only sweeps to a known hot wallet), and
    sizes the cluster→CEX outflow against the cluster's stack.

    Pass `cluster_balance` to skip the balance RPC calls (cheap path). Returns a
    safe dict — NEVER raises:
      {has_signal, cex_outflow, pct_of_cluster, destinations, detail, complete}
    `complete=False` means the underlying scan was partial/failed → treat as
    UNKNOWN, not as a clean "no CEX flow". `has_signal` is never True off
    incomplete data."""
    wallets = [w for w in (wallets or []) if w]
    if not token or not wallets:
        return _safe(False, 0.0, None, [], "missing token or wallets", False)
    since_resolved, since_ts = _since(since_iso)
    try:
        if chain in ("solana", "sol"):
            return _solana_signal(token, wallets, since_resolved, since_ts, cluster_balance)
        return _evm_signal(token, chain, wallets, since_resolved, since_ts, cluster_balance)
    except Exception as e:  # final guard — a detector must never take the loop down
        logger.debug("cex_outflow_signal_failed", token=token, chain=chain, error=str(e)[:120])
        return _safe(False, 0.0, None, [], f"unexpected error: {str(e)[:100]}", False)
