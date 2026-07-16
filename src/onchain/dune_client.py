"""Reusable Dune Analytics client — verified on-chain data via SQL.

Dune's labeled datasets are a RELIABLE source for things we must not guess:
exchange/router/bridge address labels, holder/transfer data at scale, etc. The
key auto-loads via src.config (DUNE_API_KEY). Pattern: create-or-reuse a query →
execute → poll results.

Critical discipline baked in (learned building the BSC CEX list): Dune's exchange
labels INCLUDE DEX aggregators and bridges — a transfer to a router/bridge is a
swap/bridge, NOT a CEX deposit. ALWAYS filter those out before using a label as a
"CEX" signal, or the #1 dump signal false-fires. See bsc_cex_addresses().

    from src.onchain.dune_client import run_sql
    rows = run_sql("select address, custody_owner from labels.owner_addresses "
                   "where blockchain = 'bnb' limit 10")
"""

from __future__ import annotations

import json
import math
import os
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Literal, TypedDict

import structlog

logger = structlog.get_logger()

_BASE = "https://api.dune.com/api/v1"
_ROUTER_BRIDGE_HINTS = ("dex", "aggregator", "bridge", "stargate", "router", "swap")
_DEFAULT_402_COOLDOWN_SECONDS = 3600
_MAX_COOLDOWN_SECONDS = 86400


def available() -> bool:
    return bool(os.environ.get("DUNE_API_KEY"))


# Backwards-compatible diagnostic flag. It now expires with a finite cooldown;
# a single 402 must not permanently disable Dune for the lifetime of the process.
CREDITS_EXHAUSTED = False
_CREDITS_EXHAUSTED_UNTIL = 0.0


class DuneRequestResult(TypedDict):
    ok: bool
    payload: dict | None
    error_kind: str | None
    http_status: int | None
    retry_after_seconds: int | None
    detail: str | None


class DuneSqlResult(TypedDict):
    """Status-bearing SQL result: valid zero rows is distinct from no answer."""

    state: Literal["ok", "failed", "deferred"]
    rows: list[dict]
    error_kind: str | None
    http_status: int | None
    retry_after_seconds: int | None
    retry_at: str | None
    query_id: int | None
    execution_id: str | None
    detail: str | None


def _configured_402_cooldown() -> int:
    try:
        seconds = float(os.environ.get(
            "DUNE_402_COOLDOWN_SECONDS", _DEFAULT_402_COOLDOWN_SECONDS))
    except (TypeError, ValueError):
        seconds = _DEFAULT_402_COOLDOWN_SECONDS
    if not math.isfinite(seconds):
        seconds = _DEFAULT_402_COOLDOWN_SECONDS
    return max(60, min(_MAX_COOLDOWN_SECONDS, int(seconds)))


def _retry_after_seconds(headers) -> int | None:
    raw = headers.get("Retry-After") if headers else None
    if raw is None:
        return None
    try:
        seconds = float(raw)
        if math.isfinite(seconds) and seconds >= 0:
            return min(_MAX_COOLDOWN_SECONDS, int(math.ceil(seconds)))
    except (TypeError, ValueError):
        pass
    try:
        target = parsedate_to_datetime(str(raw))
        if target.tzinfo is None:
            target = target.replace(tzinfo=timezone.utc)
        seconds = (target - datetime.now(timezone.utc)).total_seconds()
        if math.isfinite(seconds) and seconds >= 0:
            return min(_MAX_COOLDOWN_SECONDS, int(math.ceil(seconds)))
    except (TypeError, ValueError, OverflowError):
        pass
    return None


def _cooldown_remaining() -> int:
    global CREDITS_EXHAUSTED, _CREDITS_EXHAUSTED_UNTIL
    # Monotonic time keeps an NTP/manual wall-clock jump from extending or
    # prematurely ending the process-local billing backoff.
    remaining = _CREDITS_EXHAUSTED_UNTIL - time.monotonic()
    if remaining > 0:
        CREDITS_EXHAUSTED = True
        return int(math.ceil(remaining))
    CREDITS_EXHAUSTED = False
    _CREDITS_EXHAUSTED_UNTIL = 0.0
    return 0


def _request_failure(kind: str, *, status: int | None = None,
                     retry_after: int | None = None,
                     detail: str | None = None) -> DuneRequestResult:
    return {"ok": False, "payload": None, "error_kind": kind,
            "http_status": status, "retry_after_seconds": retry_after,
            "detail": detail}


def _request(method: str, path: str, body: dict | None = None,
             timeout: int = 25) -> DuneRequestResult:
    """Issue one request without collapsing transport/API failures into emptiness."""
    global CREDITS_EXHAUSTED, _CREDITS_EXHAUSTED_UNTIL
    key = os.environ.get("DUNE_API_KEY")
    if not key:
        return _request_failure("not_configured")
    try:
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(
            f"{_BASE}{path}", data=data, method=method,
            headers={"X-Dune-Api-Key": key, "Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=timeout) as response:
            raw = response.read()
        try:
            payload = json.loads(raw.decode())
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            return _request_failure("invalid_response", detail=str(exc)[:240])
        if not isinstance(payload, dict):
            return _request_failure(
                "invalid_response", detail="Dune response is not a JSON object")
        return {"ok": True, "payload": payload, "error_kind": None,
                "http_status": None, "retry_after_seconds": None, "detail": None}
    except urllib.error.HTTPError as exc:
        retry_after = _retry_after_seconds(exc.headers)
        try:
            detail = exc.read(2048).decode(errors="replace")[:240]
        except Exception:
            detail = None
        finally:
            try:
                exc.close()
            except Exception:
                pass

        if exc.code == 402:
            cooldown = max(_configured_402_cooldown(), retry_after or 0)
            cooldown = min(_MAX_COOLDOWN_SECONDS, cooldown)
            execution_or_export = (
                path.endswith("/execute") or path.endswith("/results"))
            kind = (
                "credits_exhausted" if execution_or_export
                else "billing_or_plan_required")
            if execution_or_export:
                _CREDITS_EXHAUSTED_UNTIL = time.monotonic() + cooldown
                CREDITS_EXHAUSTED = True
            logger.warning(
                "dune_billing_limit", path=path, error_kind=kind,
                retry_after_seconds=cooldown,
                note="HTTP 402 is a billing/plan limit, not an empty query result")
            return _request_failure(
                kind, status=402, retry_after=cooldown, detail=detail)
        kinds = {
            401: "auth_failed",
            403: "auth_failed",
            404: "not_found",
            429: "rate_limited",
        }
        kind = kinds.get(exc.code, "upstream_error" if exc.code >= 500 else "http_error")
        logger.warning("dune_http_error", path=path, code=exc.code, error_kind=kind)
        return _request_failure(
            kind, status=exc.code, retry_after=retry_after, detail=detail)
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        logger.debug("dune_req_failed", path=path, error=str(exc)[:80])
        return _request_failure("transport_error", detail=str(exc)[:240])
    except Exception as exc:
        logger.debug("dune_req_failed", path=path, error=str(exc)[:80])
        return _request_failure("transport_error", detail=str(exc)[:240])


def _query_cache_path():
    from src.config import DATA_DIR
    return DATA_DIR / "dune_query_cache.json"


def _cached_query_id(sql: str) -> int | None:
    import hashlib
    h = hashlib.sha256(sql.encode()).hexdigest()[:24]
    try:
        cache = json.loads(_query_cache_path().read_text())
        value = cache.get(h)
        if isinstance(value, bool):
            return None
        value = int(value)
        return value if value > 0 else None
    except Exception:
        return None


def _store_query_id(sql: str, qid: int) -> None:
    import hashlib
    h = hashlib.sha256(sql.encode()).hexdigest()[:24]
    p = _query_cache_path()
    try:
        cache = json.loads(p.read_text()) if p.exists() else {}
    except Exception:
        cache = {}
    if not isinstance(cache, dict):
        cache = {}
    try:
        cache[h] = qid
        p.write_text(json.dumps(cache))
    except Exception:
        pass


def _iso_retry_at(seconds: int | None) -> str | None:
    if seconds is None:
        return None
    return datetime.fromtimestamp(
        time.time() + max(0, seconds), tz=timezone.utc,
    ).isoformat().replace("+00:00", "Z")


def _sql_result(state: Literal["ok", "failed", "deferred"], *,
                rows: list[dict] | None = None, error_kind: str | None = None,
                http_status: int | None = None,
                retry_after_seconds: int | None = None,
                query_id: int | None = None,
                execution_id: str | None = None,
                detail: str | None = None) -> DuneSqlResult:
    return {
        "state": state,
        "rows": rows or [],
        "error_kind": error_kind,
        "http_status": http_status,
        "retry_after_seconds": retry_after_seconds,
        "retry_at": _iso_retry_at(retry_after_seconds),
        "query_id": query_id,
        "execution_id": execution_id,
        "detail": detail,
    }


_DEFERRED_REQUEST_ERRORS = {
    "credits_exhausted", "billing_or_plan_required", "rate_limited",
    "transport_error", "upstream_error",
}


def _sql_request_failure(request: DuneRequestResult, *, query_id: int | None = None,
                         execution_id: str | None = None) -> DuneSqlResult:
    kind = request["error_kind"] or "unknown_error"
    state: Literal["failed", "deferred"] = (
        "deferred" if kind in _DEFERRED_REQUEST_ERRORS else "failed")
    return _sql_result(
        state, error_kind=kind, http_status=request["http_status"],
        retry_after_seconds=request["retry_after_seconds"], query_id=query_id,
        execution_id=execution_id, detail=request["detail"])


def _query_id(payload: dict | None) -> int | None:
    value = (payload or {}).get("query_id")
    if isinstance(value, bool):
        return None
    try:
        value = int(value)
    except (TypeError, ValueError):
        return None
    return value if value > 0 else None


def _execution_id(payload: dict | None) -> str | None:
    value = (payload or {}).get("execution_id")
    return str(value).strip() if value is not None and str(value).strip() else None


def _create_query(sql: str) -> tuple[int | None, DuneSqlResult | None]:
    created = _request(
        "POST", "/query",
        {"name": "cryptoscope_adhoc", "query_sql": sql, "is_private": False})
    if not created["ok"]:
        return None, _sql_request_failure(created)
    qid = _query_id(created["payload"])
    if qid is None:
        return None, _sql_result(
            "failed", error_kind="invalid_response",
            detail="query creation response omitted query_id")
    _store_query_id(sql, qid)
    return qid, None


def run_sql_result(sql: str, *, query_id: int | None = None, poll_s: int = 5,
                   max_polls: int = 18) -> DuneSqlResult:
    """Execute SQL while preserving whether Dune answered, failed, or deferred.

    A cached query is recreated exactly once only when its execute endpoint returns
    an explicit HTTP 404. Billing, auth, rate, transport, malformed-response, and
    query-state failures never create a replacement query or issue a second execute.
    A successful query with zero rows returns ``state='ok', rows=[]``.
    """
    if not available():
        return _sql_result("failed", error_kind="not_configured")
    remaining = _cooldown_remaining()
    if remaining:
        return _sql_result(
            "deferred", error_kind="credits_cooldown",
            http_status=402, retry_after_seconds=remaining)

    explicit_query = query_id is not None
    qid = query_id
    cached_query = False
    if qid is None:
        qid = _cached_query_id(sql)
        cached_query = qid is not None

    if qid is None:
        qid, failure = _create_query(sql)
        if failure is not None:
            return failure
    elif explicit_query:
        updated = _request("PATCH", f"/query/{qid}", {"query_sql": sql})
        if not updated["ok"]:
            return _sql_request_failure(updated, query_id=qid)

    executed = _request("POST", f"/query/{qid}/execute")
    if (not executed["ok"] and cached_query
            and executed["error_kind"] == "not_found"):
        # A 404 is the only evidence that the cached remote query disappeared.
        qid, failure = _create_query(sql)
        if failure is not None:
            return failure
        executed = _request("POST", f"/query/{qid}/execute")
    if not executed["ok"]:
        return _sql_request_failure(executed, query_id=qid)

    eid = _execution_id(executed["payload"])
    if eid is None:
        return _sql_result(
            "failed", error_kind="invalid_response", query_id=qid,
            detail="query execution response omitted execution_id")

    terminal_errors = {
        "QUERY_STATE_FAILED": "query_failed",
        "QUERY_STATE_CANCELED": "query_canceled",
        "QUERY_STATE_EXPIRED": "query_expired",
        "QUERY_STATE_COMPLETED_PARTIAL": "partial_result",
    }
    for _ in range(max(0, max_polls)):
        if poll_s > 0:
            time.sleep(poll_s)
        # Dune's status endpoint is not billed. Poll it instead of repeatedly
        # exporting /results, then export exactly once after full completion.
        response = _request("GET", f"/execution/{eid}/status")
        if not response["ok"]:
            return _sql_request_failure(response, query_id=qid, execution_id=eid)
        payload = response["payload"] or {}
        execution_state = payload.get("state")
        if execution_state == "QUERY_STATE_COMPLETED":
            break
        if execution_state in terminal_errors:
            detail = str(payload.get("error") or "")[:240] or None
            logger.debug(
                "dune_query_terminal", state=execution_state, error=detail)
            return _sql_result(
                "failed", error_kind=terminal_errors[execution_state], query_id=qid,
                execution_id=eid, detail=detail)
        if execution_state not in ("QUERY_STATE_PENDING", "QUERY_STATE_EXECUTING"):
            return _sql_result(
                "failed", error_kind="invalid_response", query_id=qid,
                execution_id=eid, detail=f"unknown query state: {execution_state!r}")
    else:
        return _sql_result(
            "deferred", error_kind="poll_timeout", query_id=qid, execution_id=eid)

    response = _request("GET", f"/execution/{eid}/results")
    if not response["ok"]:
        return _sql_request_failure(response, query_id=qid, execution_id=eid)
    payload = response["payload"] or {}
    result_state = payload.get("state")
    if result_state in terminal_errors:
        return _sql_result(
            "failed", error_kind=terminal_errors[result_state], query_id=qid,
            execution_id=eid, detail=str(payload.get("error") or "")[:240] or None)
    if result_state in ("QUERY_STATE_PENDING", "QUERY_STATE_EXECUTING"):
        return _sql_result(
            "deferred", error_kind="result_not_ready", query_id=qid,
            execution_id=eid)
    if result_state != "QUERY_STATE_COMPLETED":
        return _sql_result(
            "failed", error_kind="invalid_response", query_id=qid,
            execution_id=eid, detail=f"unknown result state: {result_state!r}")
    result = payload.get("result")
    rows = result.get("rows") if isinstance(result, dict) else None
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        return _sql_result(
            "failed", error_kind="invalid_response", query_id=qid,
            execution_id=eid, detail="completed result omitted a rows list")
    return _sql_result("ok", rows=rows, query_id=qid, execution_id=eid)


def run_sql(sql: str, *, query_id: int | None = None, poll_s: int = 5,
            max_polls: int = 18) -> list[dict]:
    """Compatibility wrapper. Use run_sql_result when failure vs zero matters."""
    result = run_sql_result(
        sql, query_id=query_id, poll_s=poll_s, max_polls=max_polls)
    return result["rows"] if result["state"] == "ok" else []


# Main quote/major tokens to exclude from volume-ranked discovery — they dominate
# DEX volume but are never the operator target. Downstream filters would drop them
# anyway, but excluding here stops them from eating the top-N candidate slots.
_QUOTE_MAJORS = (
    # BNB chain
    "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c",  # WBNB
    "0x55d398326f99059ff775485246999027b3197955",  # USDT
    "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d",  # USDC
    "0xe9e7cea3dedca5984780bafc599bd69add087d56",  # BUSD
    "0x2170ed0880ac9a755fd29b2688956bd959f933f8",  # ETH
    "0x7130d2a12b9bcbfae4f2634d864a1ee1ce3ead9c",  # BTCB
    # Base
    "0x4200000000000000000000000000000000000006",  # WETH
    "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",  # USDC
    "0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca",  # USDbC
)


def top_dex_tokens(chain_map: dict[str, str] | None = None, hours: int = 24,
                   per_chain: int = 120, query_id: int | None = None) -> dict[str, list[str]]:
    """Volume-ranked candidate tokens per chain from dex.trades — a paid, rate-limit-
    free replacement for the GeckoTerminal free-tier discovery that 429s and starves
    coverage. EVM-only (dex.trades has no Solana). Returns {hunt_chain: [address,...]}.

    chain_map maps Dune blockchain -> our hunt chain id, e.g. {'bnb':'bsc','base':'base'}.
    Majors/quote tokens are excluded so they don't eat the top-N slots; everything
    else (age, liquidity band, non-operator veto) is left to the hunt's own filters."""
    chain_map = chain_map or {"bnb": "bsc", "base": "base"}
    quotes = ", ".join(_QUOTE_MAJORS)
    duned = "', '".join(chain_map.keys())
    rows = run_sql(
        f"select blockchain, token_bought_address as token, sum(amount_usd) as vol "
        f"from dex.trades "
        f"where block_time > now() - interval '{int(hours)}' hour "
        f"and blockchain in ('{duned}') and amount_usd between 100 and 5000000 "
        f"and token_bought_address not in ({quotes}) "
        f"group by 1, 2 having sum(amount_usd) > 50000 "
        f"order by blockchain, vol desc",
        query_id=query_id)
    _skip = {"0x0000000000000000000000000000000000000000",
             "0x000000000000000000000000000000000000dead"}
    out: dict[str, list[str]] = {v: [] for v in chain_map.values()}
    for r in rows:
        hunt_chain = chain_map.get(r.get("blockchain"))
        tok = (r.get("token") or "").lower()
        if hunt_chain and tok and tok not in _skip and len(out[hunt_chain]) < per_chain:
            out[hunt_chain].append(tok)
    return out


_ERC20_TABLES = {56: "erc20_bnb", 8453: "erc20_base", 1: "erc20_ethereum"}


def reconstruct_holders(token: str, chain_id: int, decimals: int = 18,
                        limit: int = 100) -> list[dict]:
    """Holder list reconstructed from full transfer-history net flows on Dune —
    the provider-independent LAST fallback (during the 2026-07 sentinel audit,
    Alchemy 403'd and Moralis+Covalent returned empty simultaneously; this path
    was the only one standing, and its sums matched live balance_of exactly).
    Returns [{address, balance}] like the other fetchers; [] on failure/unknown chain.
    Slow (~1-3 min full-history aggregation) — for audits/fallback, not hot paths."""
    table = _ERC20_TABLES.get(chain_id)
    if not table or not available():
        return []
    tok = token.lower()
    rows = run_sql(
        "with flows as ("
        "select \"to\" as addr, cast(value as double)/1e%d as amt "
        "from %s.evt_Transfer where contract_address = %s "
        "union all "
        "select \"from\" as addr, -cast(value as double)/1e%d as amt "
        "from %s.evt_Transfer where contract_address = %s) "
        "select addr, sum(amt) as bal from flows "
        "where addr != 0x0000000000000000000000000000000000000000 "
        "group by 1 having sum(amt) > 0 order by bal desc limit %d"
        % (decimals, table, tok, decimals, table, tok, limit),
        poll_s=6, max_polls=40)
    return [{"address": str(r["addr"]).lower(), "balance": float(r["bal"])}
            for r in rows if r.get("addr")]


def bsc_cex_addresses(query_id: int | None = 7852861) -> dict[str, str]:
    """Verified BSC exchange hot/deposit wallets from Dune labels, with routers and
    bridges FILTERED OUT (they are not CEX deposits). Returns {address_lower: label}.
    Used to refresh cex_addresses.BSC_CEX_SUPPLEMENT; cheap to re-run periodically."""
    rows = run_sql(
        "select address, custody_owner, owner_key from labels.owner_addresses "
        "where blockchain = 'bnb' and ("
        "lower(owner_key) like '%binance%' or lower(owner_key) like '%okx%' or "
        "lower(owner_key) like '%gate%' or lower(owner_key) like '%mexc%' or "
        "lower(owner_key) like '%bitget%' or lower(owner_key) like '%kucoin%' or "
        "lower(owner_key) like '%bybit%' or lower(owner_key) like '%htx%' or "
        "lower(owner_key) like '%huobi%' or lower(owner_key) like '%crypto_com%' or "
        "lower(owner_key) like '%bitfinex%' or lower(owner_key) like '%upbit%' or "
        "lower(owner_key) like '%bithumb%' or lower(owner_key) like '%bitmart%' or "
        "lower(owner_key) like '%kraken%' or lower(owner_key) like '%coinbase%') limit 1500",
        query_id=query_id)
    out: dict[str, str] = {}
    for r in rows:
        key = (r.get("owner_key") or "").lower()
        owner = r.get("custody_owner") or r.get("owner_key") or ""
        addr = (r.get("address") or "").lower()
        if not addr or any(h in key for h in _ROUTER_BRIDGE_HINTS) \
                or any(h in owner.lower() for h in _ROUTER_BRIDGE_HINTS):
            continue
        out[addr] = owner if owner and owner[:1].isupper() else key
    return out
