"""Shared Moralis client with multi-key rotation.

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


def keys() -> list[str]:
    """All configured Moralis keys: MORALIS_API_KEYS (csv) or MORALIS_API_KEY[_N]."""
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
    JSON or None. `path` is everything after /api/v2.2/."""
    all_keys = keys()
    live = [k for k in all_keys if k not in _dead] or all_keys
    last_err = None
    for k in live:
        try:
            req = urllib.request.Request(
                _BASE + path,
                headers={"X-API-Key": k, "accept": "application/json", "User-Agent": _UA})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            if e.code in (401, 429):       # quota consumed / rate limited → park key
                _dead.add(k)
                last_err = f"{e.code} (key parked)"
                continue
            last_err = str(e)
            break
        except Exception as e:
            last_err = str(e)
            break
    if last_err:
        logger.debug("moralis_request_failed", path=path[:60], error=str(last_err)[:80])
    return None
