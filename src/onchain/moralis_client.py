"""Shared Moralis client with multi-key rotation.

DISABLED BY DEFAULT. Moralis is off unless MORALIS_ENABLED is explicitly truthy,
regardless of whether keys are configured. This is the kill switch: a paid plan
bills per-request on monthly overage and does NOT return 401 (so the free-tier
401-parking below never trips), and there was previously no way to stop the
scheduler from calling Moralis short of deleting the keys. Every caller already
fails closed when no key is usable (falls back to Covalent/Alchemy/Etherscan or
marks the result "不可判"), so returning no keys cleanly halts all traffic.
Re-enable deliberately with MORALIS_ENABLED=1.

Moralis free tier has a DAILY quota per key. With several keys (MORALIS_API_KEY,
MORALIS_API_KEY_2, ... or a MORALIS_API_KEYS csv) we rotate: a key that returns
401 (quota consumed) or 429 (rate limited) is parked for the rest of the process
and the next key is tried. This multiplies the daily free quota across keys.

Cloudflare blocks the default urllib UA (error 1010) — a browser UA is mandatory.
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request

import structlog

logger = structlog.get_logger()

_UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
       "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")
_BASE = "https://deep-index.moralis.io/api/v2.2/"

# Keys parked this process (quota consumed / rate limited) so we stop retrying them.
_dead: set[str] = set()


def enabled() -> bool:
    """Moralis is opt-in. Off unless MORALIS_ENABLED is set to a truthy value."""
    return os.environ.get("MORALIS_ENABLED", "").strip().lower() in {
        "1", "true", "yes", "on",
    }


def keys() -> list[str]:
    """Configured Moralis keys — but ONLY when Moralis is explicitly enabled.

    Disabled (the default) returns [] even with keys present, so available()/
    usable() are False and get() never makes a request.
    """
    if not enabled():
        return []
    csv = os.environ.get("MORALIS_API_KEYS", "")
    if csv.strip():
        return [k.strip() for k in csv.split(",") if k.strip()]
    out = []
    for name in ("MORALIS_API_KEY", "MORALIS_API_KEY_2", "MORALIS_API_KEY_3",
                 "MORALIS_API_KEY_4"):
        v = os.environ.get(name)
        if v:
            out.append(v.strip())
    return out


def available() -> bool:
    return bool(keys())


def usable() -> bool:
    """A key exists that is NOT parked (quota consumed / rate limited this process).
    available() only says keys are configured; usable() says a request can succeed —
    gate keyless fallbacks on `not usable()`."""
    ks = keys()
    return bool(ks) and any(k not in _dead for k in ks)


def get(path: str, timeout: int = 25):
    """GET a Moralis v2.2 path, rotating keys on quota/rate-limit. Returns parsed
    JSON or None. `path` is everything after /api/v2.2/.

    429 (RATE limit) is transient — back off and retry the SAME key a few times
    (deep pagination trips it; permanently parking the key there was aborting whole
    full-history pulls mid-way). 401 (quota/plan) is terminal — park the key."""
    import time
    all_keys = keys()
    live = [k for k in all_keys if k not in _dead] or all_keys
    last_err = None
    for k in live:
        for attempt in range(4):
            try:
                req = urllib.request.Request(
                    _BASE + path,
                    headers={"X-API-Key": k, "accept": "application/json", "User-Agent": _UA})
                with urllib.request.urlopen(req, timeout=timeout) as r:
                    return json.loads(r.read().decode())
            except urllib.error.HTTPError as e:
                # HTTPError is also the live response object; close it before retrying
                # or rotating keys so repeated 400/429 replies cannot leak sockets.
                code = e.code
                e.close()
                if code == 429:              # rate limited → back off, retry same key
                    last_err = "429 (rate limited, backing off)"
                    time.sleep(0.6 * (attempt + 1))
                    continue
                if code == 401:              # quota/plan → terminal, park key
                    _dead.add(k)
                    last_err = "401 (key parked)"
                    if all(kk in _dead for kk in all_keys):
                        logger.warning("moralis_quota_exhausted",
                                       note="HTTP 401 — all Moralis keys parked (quota/plan)")
                    break
                last_err = f"HTTP Error {code}"
                break
            except Exception as e:
                last_err = str(e)
                break
        # exhausted this key's attempts (429 retries or a break) → try next key
    if last_err:
        logger.debug("moralis_request_failed", path=path[:60], error=str(last_err)[:80])
    return None
