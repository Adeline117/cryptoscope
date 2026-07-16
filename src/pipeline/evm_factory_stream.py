"""Read-only EVM factory log stream, starting with PancakeSwap V2 on BSC."""
from __future__ import annotations

import hashlib
import ipaddress
import json
import os
import re
import socket
import sqlite3
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable

import structlog
from websocket import WebSocketTimeoutException

from src.config import DATA_DIR
from src.ops import stream_disk_guard
from src.pipeline import stream_health
from src.pipeline.stream_runner import StreamEvent, StreamRunner

logger = structlog.get_logger()
_audit_monotonic = time.monotonic

PAIR_CREATED_TOPIC = "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9"
POOL_CREATED_TOPIC = "0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118"
AERODROME_POOL_TOPIC = "0x2128d88d14c80cb081c1252a5acff7a264671bf199ce226b53788fb26065005e"
PANCAKE_V2_FACTORY = "0xca143ce32fe78f1f7019d7d551a6402fc5350c73"
PANCAKE_V2_BASE_FACTORY = "0x02a84c1b3bbd7401a5f7fa98a384ebc70bb5749e"
PANCAKE_V2_ETH_FACTORY = "0x1097053fd2ea711dad45caccc45eff7548fcb362"
PANCAKE_V3_FACTORY = "0x0bfbcf9fa4f9c56b0f40a671ad40e0805a091865"
AERODROME_FACTORY = "0x420dd381b31aef6683db6b902084cb0ffece40da"
PUBLIC_BSC_WS = ("wss://bsc-rpc.publicnode.com", "wss://bsc.drpc.org")
PUBLIC_BSC_RPC = ("https://bsc.rpc.blxrbdn.com", "https://bsc.drpc.org",
                  "https://56.rpc.thirdweb.com")
PUBLIC_BASE_WS = ("wss://base-rpc.publicnode.com", "wss://base.drpc.org")
PUBLIC_BASE_RPC = ("https://mainnet.base.org", "https://base.drpc.org")
PUBLIC_ETH_WS = ("wss://ethereum-rpc.publicnode.com", "wss://eth.drpc.org")
PUBLIC_ETH_RPC = ("https://rpc.flashbots.net", "https://eth.drpc.org",
                  "https://ethereum-rpc.publicnode.com")
MAX_BACKFILL_BLOCKS = 2_000
MAX_COVERAGE_BLOCKS = 2_000
MAX_COVERAGE_AUDIT_DURATION_MS = 60_000
INITIAL_COVERAGE_LOOKBACK_BLOCKS = 2_000
COVERAGE_EPOCH_BLOCKS = 2_000
COVERAGE_EPOCH_RETENTION = 256
COVERAGE_RETRY_BASE_SECONDS = 60
COVERAGE_RETRY_MAX_SECONDS = 60 * 60
CHAIN_IDS = {"ethereum": 1, "bsc": 56, "base": 8453}
# Conservative operational bounds, not protocol finality guarantees.  They keep
# a responsive websocket from masking a stalled finalized HTTP source.  Values
# reflect ETH's ~12s slots/~15m finality, Base's ~2s L2 blocks plus L1 batch
# finality, and BSC Fermi's sub-second blocks/fast finality with ample headroom.
FINALIZED_HEAD_MAX_AGE_SECONDS = {"bsc": 120, "ethereum": 30 * 60, "base": 45 * 60}
FINALIZED_HEAD_MAX_LAG_BLOCKS = {"bsc": 256, "ethereum": 160, "base": 1_500}
DB = DATA_DIR / "evm_factory_events.db"
_BROWSER_UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
               "AppleWebKit/537.36 Chrome/120 Safari/537.36 CryptoScope/1.0")
QUALIFICATION_STATES = {
    "raw_unqualified", "market_pending", "market_error", "below_threshold",
    "qualified_recorded", "ledger_orphan", "duplicate_token_existing",
    "unsupported_quote_pair", "ambiguous_target", "expired_unqualified",
    "reorg_removed", "historical_raw_only",
}
RETRYABLE_QUALIFICATION_STATES = {
    "raw_unqualified", "market_pending", "market_error", "below_threshold",
}


class ProviderIndependenceError(RuntimeError):
    """No credential-free HTTP source is independent of the live WS source."""


class FinalizedHeadStale(RuntimeError):
    """The bound HTTP provider's finalized header is operationally stale."""


class FinalizedHeadFuture(RuntimeError):
    """The bound HTTP provider returned a future-dated finalized header."""


class CoverageAuditTooSlow(RuntimeError):
    """A coverage cycle outlived the bounded maintenance interval."""


class CoverageEvidenceConflict(RuntimeError):
    """Finalized HTTP logs conflict with already persisted websocket evidence."""


class CoverageCheckpointMismatch(RuntimeError):
    """The selected HTTP provider does not extend the persisted checkpoint."""


class RawEvidenceConflict(RuntimeError):
    """An immutable transaction/log identity changed its decoded payload."""


_ACTIVE_WS_PROVIDERS: dict[tuple[str, str], tuple[str, str]] = {}
_ACTIVE_WS_PROVIDERS_LOCK = threading.Lock()
_PROVIDER_FINGERPRINT_RE = re.compile(r"provider:[0-9a-f]{64}\Z")


def _bounded_error_kind(exc: BaseException) -> str:
    kind = type(exc).__name__
    return (kind if re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,79}", kind)
            else "Exception")


_COVERAGE_EPOCH_COLUMNS = {
    "id", "chain", "venue", "factory", "topic", "epoch_start_block",
    "from_block", "to_block", "checked_at", "ws_provider_id",
    "http_provider_id", "provider_independent", "log_count",
    "evidence_digest", "segment_count", "status",
}
_COVERAGE_EPOCH_DDL = """CREATE TABLE IF NOT EXISTS coverage_epochs(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    chain TEXT NOT NULL, venue TEXT NOT NULL, factory TEXT NOT NULL,
    topic TEXT NOT NULL, epoch_start_block INTEGER NOT NULL,
    from_block INTEGER NOT NULL, to_block INTEGER NOT NULL,
    checked_at TEXT NOT NULL, ws_provider_id TEXT NOT NULL,
    http_provider_id TEXT NOT NULL, provider_independent INTEGER NOT NULL,
    log_count INTEGER NOT NULL, evidence_digest TEXT NOT NULL,
    segment_count INTEGER NOT NULL, status TEXT NOT NULL,
    UNIQUE(chain,venue,factory,topic,epoch_start_block))"""
_COVERAGE_STATE_MIGRATIONS = (
    ("verified_through_hash", "TEXT"),
    ("safe_head_hash", "TEXT"),
    ("safe_head_at", "TEXT"),
    ("audit_duration_ms", "INTEGER"),
)


def provider_id(endpoint: str) -> str:
    """Return an opaque hostname-level identity without leaking provider URLs.

    Provider independence is deliberately stricter than socket independence: two
    URLs on the same host are one provider even when their schemes or ports differ.
    The canonical hostname is immediately hashed so tenant-bearing subdomains are
    never persisted in health, proof, log, or exception surfaces.
    """
    try:
        if (not isinstance(endpoint, str) or endpoint != endpoint.strip()
                or any(ch.isspace() for ch in endpoint)):
            raise ValueError("provider endpoint is invalid")
        parsed = urllib.parse.urlsplit(endpoint)
        host = (parsed.hostname or "").lower().rstrip(".")
        # Accessing ``port`` rejects malformed values even though ports are not
        # part of the provider identity.
        _ = parsed.port
    except (TypeError, ValueError) as exc:
        raise ValueError("provider endpoint is invalid") from exc
    if parsed.scheme not in {"http", "https", "ws", "wss"} or not host:
        raise ValueError("provider endpoint is invalid")
    try:
        host.encode("ascii")
    except UnicodeEncodeError as exc:
        raise ValueError("provider endpoint is invalid") from exc
    try:
        host = ipaddress.ip_address(host).compressed
    except ValueError:
        labels = host.split(".")
        if (len(host) > 253 or any(
            not label or len(label) > 63
            or re.fullmatch(r"[a-z0-9](?:[a-z0-9-]*[a-z0-9])?", label) is None
            for label in labels
        )):
            raise ValueError("provider endpoint is invalid")
    return "provider:" + hashlib.sha256(host.encode("utf-8")).hexdigest()


def _canonical_provider_identity(value: object) -> str:
    """Accept an endpoint or an already canonical opaque fingerprint only."""
    if not isinstance(value, str):
        raise ValueError("provider identity is invalid")
    if "://" in value:
        return provider_id(value)
    if not _PROVIDER_FINGERPRINT_RE.fullmatch(value):
        raise ValueError("provider identity is invalid")
    return value


def coverage_stream(spec: "FactorySpec") -> str:
    return f"{spec.stream}_finalized_coverage"


def _set_active_ws_provider(
    spec: "FactorySpec", identity: str | None, generation: str | None = None,
) -> None:
    key = (spec.chain, spec.stream)
    with _ACTIVE_WS_PROVIDERS_LOCK:
        if identity is None:
            _ACTIVE_WS_PROVIDERS.pop(key, None)
        else:
            if not generation:
                raise ValueError("websocket connection generation is required")
            _ACTIVE_WS_PROVIDERS[key] = (identity, generation)


def active_ws_connection(spec: "FactorySpec") -> tuple[str, str] | None:
    with _ACTIVE_WS_PROVIDERS_LOCK:
        return _ACTIVE_WS_PROVIDERS.get((spec.chain, spec.stream))


def active_ws_provider(spec: "FactorySpec") -> str | None:
    connection = active_ws_connection(spec)
    return connection[0] if connection else None


def _clear_active_ws_provider(
    spec: "FactorySpec", expected_identity: str, expected_generation: str,
) -> None:
    key = (spec.chain, spec.stream)
    with _ACTIVE_WS_PROVIDERS_LOCK:
        if _ACTIVE_WS_PROVIDERS.get(key) == (
            expected_identity, expected_generation,
        ):
            _ACTIVE_WS_PROVIDERS.pop(key, None)


def _invalidate_connect_health(spec: "FactorySpec") -> None:
    """Revoke stale live claims before a new socket has proved both subscriptions.

    The persisted coverage proof remains intact.  Only its runtime health claim is
    downgraded until maintenance audits it against the currently connected provider.
    Rows already non-live are left untouched, bounding writes during retry storms.
    """
    observed = {(row["source"], row["stream"]): row
                for row in stream_health.snapshot()}
    transport = observed.get((spec.chain, spec.stream))
    if transport is None or transport.get("status") == "live":
        stream_health.mark_disconnected(
            spec.chain, spec.stream, "websocket subscription revalidation required",
        )


def _mark_coverage_reaudit_required(
    spec: "FactorySpec", *, identity: str, generation: str,
) -> None:
    """Persist the successful socket generation before it can emit a live event."""
    stream_health.report_worker(
        spec.chain, coverage_stream(spec), status="degraded",
        error="ws_provider_changed_reaudit_required",
            details={
            "schema_version": 2,
            "state": "ws_provider_changed_reaudit_required",
            "ws_provider_id": identity,
            "connection_generation": generation,
            "provider_independent": False,
            "last_error_kind": "ws_provider_changed_reaudit_required",
        },
    )


@dataclass(frozen=True)
class FactorySpec:
    chain: str
    venue: str
    address: str
    event_kind: str
    topic: str
    ws_urls: tuple[str, ...]
    rpc_urls: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.event_kind not in {"pair_v2", "pool_v3", "aerodrome_pool"}:
            raise ValueError("unsupported factory event kind")
        for value, size, name in ((self.address, 40, "address"),
                                  (self.topic, 64, "topic")):
            raw = value.removeprefix("0x")
            if (len(raw) != size
                    or any(ch not in "0123456789abcdef" for ch in raw)):
                raise ValueError(f"factory {name} must be lowercase canonical hex")

    @property
    def stream(self) -> str:
        suffix = "pairs" if self.event_kind == "pair_v2" else "pools"
        return f"{self.venue}_{suffix}"


def _urls(env_name: str, defaults: tuple[str, ...]) -> tuple[str, ...]:
    configured = tuple(value.strip() for value in os.getenv(env_name, "").split(",")
                       if value.strip())
    return configured or defaults


def bsc_pancake_v2_spec() -> FactorySpec:
    return FactorySpec(
        chain="bsc", venue="pancakeswap_v2", address=PANCAKE_V2_FACTORY,
        event_kind="pair_v2", topic=PAIR_CREATED_TOPIC,
        ws_urls=_urls("BSC_FACTORY_WS_URLS", PUBLIC_BSC_WS),
        rpc_urls=_urls("BSC_FACTORY_RPC_URLS", PUBLIC_BSC_RPC),
    )


def base_pancake_v2_spec() -> FactorySpec:
    return FactorySpec(
        chain="base", venue="pancakeswap_v2", address=PANCAKE_V2_BASE_FACTORY,
        event_kind="pair_v2", topic=PAIR_CREATED_TOPIC,
        ws_urls=_urls("BASE_FACTORY_WS_URLS", PUBLIC_BASE_WS),
        rpc_urls=_urls("BASE_FACTORY_RPC_URLS", PUBLIC_BASE_RPC),
    )


def ethereum_pancake_v2_spec() -> FactorySpec:
    return FactorySpec(
        chain="ethereum", venue="pancakeswap_v2", address=PANCAKE_V2_ETH_FACTORY,
        event_kind="pair_v2", topic=PAIR_CREATED_TOPIC,
        ws_urls=_urls("ETH_FACTORY_WS_URLS", PUBLIC_ETH_WS),
        rpc_urls=_urls("ETH_FACTORY_RPC_URLS", PUBLIC_ETH_RPC),
    )


def bsc_pancake_v3_spec() -> FactorySpec:
    return FactorySpec(
        chain="bsc", venue="pancakeswap_v3", address=PANCAKE_V3_FACTORY,
        event_kind="pool_v3", topic=POOL_CREATED_TOPIC,
        ws_urls=_urls("BSC_FACTORY_WS_URLS", PUBLIC_BSC_WS),
        rpc_urls=_urls("BSC_FACTORY_RPC_URLS", PUBLIC_BSC_RPC),
    )


def base_aerodrome_spec() -> FactorySpec:
    return FactorySpec(
        chain="base", venue="aerodrome", address=AERODROME_FACTORY,
        event_kind="aerodrome_pool", topic=AERODROME_POOL_TOPIC,
        ws_urls=_urls("BASE_FACTORY_WS_URLS", PUBLIC_BASE_WS),
        rpc_urls=_urls("BASE_FACTORY_RPC_URLS", PUBLIC_BASE_RPC),
    )


def configured_specs() -> tuple[FactorySpec, ...]:
    return (bsc_pancake_v2_spec(), bsc_pancake_v3_spec(), base_pancake_v2_spec(),
            base_aerodrome_spec(), ethereum_pancake_v2_spec())


def _coverage_epoch_columns(c: sqlite3.Connection) -> set[str]:
    return {str(row[1]) for row in c.execute("PRAGMA table_info(coverage_epochs)")}


def _ensure_coverage_epoch_schema(c: sqlite3.Connection) -> None:
    """Atomically replace the pre-bucket epoch schema under startup races."""
    columns = _coverage_epoch_columns(c)
    if not columns:
        c.execute(_COVERAGE_EPOCH_DDL)
        return
    if _COVERAGE_EPOCH_COLUMNS.issubset(columns):
        return

    # Multiple factory processes can import the new code together.  Serialize the
    # migration, then re-read while holding the write lock so only the first one
    # rebuilds the legacy table.
    c.commit()
    try:
        c.execute("BEGIN IMMEDIATE")
        columns = _coverage_epoch_columns(c)
        if _COVERAGE_EPOCH_COLUMNS.issubset(columns):
            c.commit()
            return
        legacy_rows = int(c.execute(
            "SELECT COUNT(*) FROM coverage_epochs"
        ).fetchone()[0])
        c.execute("DROP INDEX IF EXISTS idx_coverage_epochs_spec")
        if legacy_rows:
            suffix = 1
            while c.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                (f"coverage_epochs_legacy_v{suffix}",),
            ).fetchone():
                suffix += 1
            legacy_name = f"coverage_epochs_legacy_v{suffix}"
            # ``legacy_name`` is generated solely from an integer above.
            c.execute(f"ALTER TABLE coverage_epochs RENAME TO {legacy_name}")
        else:
            c.execute("DROP TABLE coverage_epochs")
        c.execute(_COVERAGE_EPOCH_DDL)
        now = datetime.now(timezone.utc).isoformat()
        # A legacy per-head epoch layout cannot prove the new fixed-window
        # contract. Preserve the original start boundary, but force a complete
        # re-audit from start-1 instead of carrying a false verified watermark.
        c.execute(
            """UPDATE coverage_state
                  SET verified_through_block=CASE
                        WHEN coverage_started_block IS NULL THEN NULL
                        ELSE coverage_started_block-1 END,
                      verified_at=NULL,provider_independent=0,status='blocked',
                      consecutive_failures=0,next_retry_at=NULL,
                      last_error_kind='legacy_coverage_schema_quarantined',
                      updated_at=?""",
            (now,),
        )
        c.commit()
    except Exception:
        c.rollback()
        raise


def _ensure_coverage_state_schema(c: sqlite3.Connection) -> None:
    """Add finalized-head freshness evidence safely under process races."""
    columns = {str(row[1]) for row in c.execute("PRAGMA table_info(coverage_state)")}
    missing = [(name, kind) for name, kind in _COVERAGE_STATE_MIGRATIONS
               if name not in columns]
    if not missing:
        return
    c.commit()
    try:
        c.execute("BEGIN IMMEDIATE")
        columns = {
            str(row[1]) for row in c.execute("PRAGMA table_info(coverage_state)")
        }
        if all(name in columns for name, _kind in _COVERAGE_STATE_MIGRATIONS):
            c.commit()
            return
        for name, kind in _COVERAGE_STATE_MIGRATIONS:
            if name not in columns:
                c.execute(f"ALTER TABLE coverage_state ADD COLUMN {name} {kind}")
                columns.add(name)
        # Pre-migration rows lack the canonical checkpoint required to extend a
        # finalized range.  Preserve the public start boundary, but replay from
        # start-1 and discard epochs that cannot be linked to a block hash.
        c.execute(
            """UPDATE coverage_state
                  SET verified_through_block=CASE
                        WHEN coverage_started_block IS NULL THEN NULL
                        ELSE coverage_started_block-1 END,
                      verified_through_hash=NULL,provider_independent=0,
                      safe_head_block=NULL,safe_head_hash=NULL,safe_head_at=NULL,
                      audit_duration_ms=NULL,
                      status='blocked',verified_at=NULL,consecutive_failures=0,
                      next_retry_at=NULL,
                      last_error_kind='coverage_freshness_reaudit_required',
                      updated_at=?
                WHERE verified_through_hash IS NULL OR safe_head_hash IS NULL
                   OR safe_head_at IS NULL OR audit_duration_ms IS NULL""",
            (datetime.now(timezone.utc).isoformat(),),
        )
        c.execute("DELETE FROM coverage_epochs")
        c.commit()
    except Exception:
        c.rollback()
        raise


def _opaque_legacy_provider(value: object) -> str:
    raw = str(value or "")
    if _PROVIDER_FINGERPRINT_RE.fullmatch(raw):
        return raw
    return "provider:" + hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _quarantine_plaintext_provider_ids(c: sqlite3.Connection) -> None:
    """Remove legacy hostnames from proof tables and force a clean re-audit."""
    marker = "provider_fingerprint_migration_v1"
    if c.execute(
        "SELECT 1 FROM bridge_meta WHERE key=?", (marker,),
    ).fetchone():
        return
    state_rows = c.execute(
        """SELECT chain,venue,factory,topic,ws_provider_id,http_provider_id
             FROM coverage_state"""
    ).fetchall()
    epoch_rows = c.execute(
        """SELECT DISTINCT chain,venue,factory,topic,ws_provider_id,http_provider_id
             FROM coverage_epochs"""
    ).fetchall()
    legacy_tables = [name for row in c.execute(
        """SELECT name FROM sqlite_master
             WHERE type='table' AND name LIKE 'coverage_epochs_legacy_v%'"""
    ) if re.fullmatch(r"coverage_epochs_legacy_v[0-9]+", name := str(row[0]))]
    bad_state = [row for row in state_rows if not (
        _PROVIDER_FINGERPRINT_RE.fullmatch(str(row[4] or ""))
        and _PROVIDER_FINGERPRINT_RE.fullmatch(str(row[5] or ""))
    )]
    bad_epoch_keys = {tuple(row[:4]) for row in epoch_rows if not (
        _PROVIDER_FINGERPRINT_RE.fullmatch(str(row[4] or ""))
        and _PROVIDER_FINGERPRINT_RE.fullmatch(str(row[5] or ""))
    )}
    legacy_needs_redaction = any(
        not (_PROVIDER_FINGERPRINT_RE.fullmatch(str(ws_id or ""))
             and _PROVIDER_FINGERPRINT_RE.fullmatch(str(http_id or "")))
        for table in legacy_tables
        for ws_id, http_id in c.execute(
            f"SELECT ws_provider_id,http_provider_id FROM {table}"
        ).fetchall()
    )
    c.commit()
    try:
        c.execute("BEGIN IMMEDIATE")
        if c.execute(
            "SELECT 1 FROM bridge_meta WHERE key=?", (marker,),
        ).fetchone():
            c.commit()
            return
        now = datetime.now(timezone.utc).isoformat()
        current_bad_epoch_keys = {tuple(row[:4]) for row in c.execute(
            """SELECT DISTINCT chain,venue,factory,topic,
                              ws_provider_id,http_provider_id
                 FROM coverage_epochs"""
        ).fetchall() if not (
            _PROVIDER_FINGERPRINT_RE.fullmatch(str(row[4] or ""))
            and _PROVIDER_FINGERPRINT_RE.fullmatch(str(row[5] or ""))
        )}
        for row in c.execute(
            """SELECT chain,venue,factory,topic,ws_provider_id,http_provider_id
                 FROM coverage_state"""
        ).fetchall():
            key = tuple(row[:4])
            ws_id, http_id = str(row[4] or ""), str(row[5] or "")
            if (_PROVIDER_FINGERPRINT_RE.fullmatch(ws_id)
                    and _PROVIDER_FINGERPRINT_RE.fullmatch(http_id)
                    and key not in current_bad_epoch_keys):
                continue
            c.execute(
                """UPDATE coverage_state
                      SET verified_through_block=CASE
                            WHEN coverage_started_block IS NULL THEN NULL
                            ELSE coverage_started_block-1 END,
                          verified_through_hash=NULL,safe_head_block=NULL,
                          safe_head_hash=NULL,safe_head_at=NULL,
                          audit_duration_ms=NULL,verified_at=NULL,
                          ws_provider_id=?,http_provider_id=?,provider_independent=0,
                          status='blocked',consecutive_failures=0,next_retry_at=NULL,
                          last_error_kind='provider_identity_reaudit_required',
                          updated_at=?
                    WHERE chain=? AND venue=? AND factory=? AND topic=?""",
                (_opaque_legacy_provider(ws_id), _opaque_legacy_provider(http_id),
                 now, *key),
            )
            c.execute(
                """DELETE FROM coverage_epochs
                     WHERE chain=? AND venue=? AND factory=? AND topic=?""", key,
            )
        for table in legacy_tables:
            for row_id, ws_id, http_id in c.execute(
                f"SELECT id,ws_provider_id,http_provider_id FROM {table}"
            ).fetchall():
                c.execute(
                    f"UPDATE {table} SET ws_provider_id=?,http_provider_id=? WHERE id=?",
                    (_opaque_legacy_provider(ws_id),
                     _opaque_legacy_provider(http_id), row_id),
                )
        for key in current_bad_epoch_keys:
            c.execute(
                """DELETE FROM coverage_epochs
                     WHERE chain=? AND venue=? AND factory=? AND topic=?""", key,
            )
        c.execute(
            """INSERT INTO bridge_meta(key,value,updated_at)
                 VALUES (?,?,?)
                 ON CONFLICT(key) DO UPDATE SET
                   value=excluded.value,updated_at=excluded.updated_at""",
            (marker, "complete", now),
        )
        c.commit()
    except Exception:
        c.rollback()
        raise


def _ensure_provider_identity_triggers(c: sqlite3.Connection) -> None:
    """Reject mixed-version writes that would reintroduce provider hostnames."""
    valid_ws = (
        "NEW.ws_provider_id IS NULL OR (length(NEW.ws_provider_id)=73 "
        "AND substr(NEW.ws_provider_id,1,9)='provider:' "
        "AND substr(NEW.ws_provider_id,10) NOT GLOB '*[^0-9a-f]*')"
    )
    valid_http = (
        "NEW.http_provider_id IS NULL OR (length(NEW.http_provider_id)=73 "
        "AND substr(NEW.http_provider_id,1,9)='provider:' "
        "AND substr(NEW.http_provider_id,10) NOT GLOB '*[^0-9a-f]*')"
    )
    for table in ("coverage_state", "coverage_epochs"):
        for operation in ("INSERT", "UPDATE"):
            name = f"trg_{table}_opaque_provider_{operation.lower()}"
            c.execute(
                f"""CREATE TRIGGER IF NOT EXISTS {name}
                    BEFORE {operation} ON {table}
                    WHEN NOT ({valid_ws}) OR NOT ({valid_http})
                    BEGIN
                      SELECT RAISE(ABORT,'provider identity must be opaque');
                    END"""
            )


def _conn() -> sqlite3.Connection:
    DB.parent.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(str(DB), timeout=10)
    try:
        return _initialize_connection(c)
    except Exception:
        c.rollback()
        c.close()
        raise


def _initialize_connection(c: sqlite3.Connection) -> sqlite3.Connection:
    c.execute("PRAGMA busy_timeout=8000")
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA synchronous=NORMAL")
    c.execute("""CREATE TABLE IF NOT EXISTS raw_pools(
        chain TEXT NOT NULL, venue TEXT NOT NULL, factory TEXT NOT NULL,
        transaction_hash TEXT NOT NULL, log_index INTEGER NOT NULL,
        block_number INTEGER NOT NULL, block_hash TEXT, transaction_index INTEGER,
        token0 TEXT NOT NULL, token1 TEXT NOT NULL, pool TEXT NOT NULL,
        pair_index INTEGER NOT NULL, stable INTEGER, block_at TEXT,
        detected_at TEXT NOT NULL,
        updated_at TEXT NOT NULL, raw_payload_hash TEXT NOT NULL,
        removed INTEGER NOT NULL DEFAULT 0, evidence_state TEXT NOT NULL,
        qualification_state TEXT NOT NULL DEFAULT 'raw_unqualified',
        PRIMARY KEY(chain,transaction_hash,log_index))""")
    columns = {row[1] for row in c.execute("PRAGMA table_info(raw_pools)")}
    if "stable" not in columns:
        c.execute("ALTER TABLE raw_pools ADD COLUMN stable INTEGER")
    c.execute("CREATE INDEX IF NOT EXISTS idx_raw_pools_block "
              "ON raw_pools(chain,venue,block_number,log_index)")
    c.execute("""CREATE TABLE IF NOT EXISTS raw_v3_pools(
        chain TEXT NOT NULL, venue TEXT NOT NULL, factory TEXT NOT NULL,
        transaction_hash TEXT NOT NULL, log_index INTEGER NOT NULL,
        block_number INTEGER NOT NULL, block_hash TEXT, transaction_index INTEGER,
        token0 TEXT NOT NULL, token1 TEXT NOT NULL, pool TEXT NOT NULL,
        fee INTEGER NOT NULL, tick_spacing INTEGER NOT NULL, block_at TEXT,
        detected_at TEXT NOT NULL, updated_at TEXT NOT NULL,
        raw_payload_hash TEXT NOT NULL, removed INTEGER NOT NULL DEFAULT 0,
        evidence_state TEXT NOT NULL,
        qualification_state TEXT NOT NULL DEFAULT 'raw_unqualified',
        PRIMARY KEY(chain,transaction_hash,log_index))""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_raw_v3_pools_block "
              "ON raw_v3_pools(chain,venue,block_number,log_index)")
    for table in ("raw_pools", "raw_v3_pools"):
        table_columns = {row[1] for row in c.execute(f"PRAGMA table_info({table})")}
        for name, kind, default in (
            ("qualification_attempted_at", "TEXT", ""),
            ("qualification_retry_at", "TEXT", ""),
            ("qualification_reason", "TEXT", ""),
            ("qualification_attempts", "INTEGER", " NOT NULL DEFAULT 0"),
            ("qualified_at", "TEXT", ""),
            ("ledger_event_id", "TEXT", ""),
            ("target_token", "TEXT", ""),
        ):
            if name not in table_columns:
                c.execute(f"ALTER TABLE {table} ADD COLUMN {name} {kind}{default}")
        c.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_qualification "
                  f"ON {table}(evidence_state,qualification_state,detected_at)")
    c.execute("""CREATE TABLE IF NOT EXISTS bridge_meta(
        key TEXT PRIMARY KEY,value TEXT NOT NULL,updated_at TEXT NOT NULL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS coverage_state(
        chain TEXT NOT NULL, venue TEXT NOT NULL, factory TEXT NOT NULL,
        topic TEXT NOT NULL, coverage_started_block INTEGER,
        verified_through_block INTEGER, safe_head_block INTEGER,
        verified_through_hash TEXT, safe_head_hash TEXT,
        safe_head_at TEXT, audit_duration_ms INTEGER,
        verified_at TEXT, ws_provider_id TEXT, http_provider_id TEXT,
        provider_independent INTEGER NOT NULL DEFAULT 0,
        status TEXT NOT NULL, consecutive_failures INTEGER NOT NULL DEFAULT 0,
        next_retry_at TEXT, last_error_kind TEXT, updated_at TEXT NOT NULL,
        PRIMARY KEY(chain,venue,factory,topic))""")
    _ensure_coverage_epoch_schema(c)
    _ensure_coverage_state_schema(c)
    _quarantine_plaintext_provider_ids(c)
    _ensure_provider_identity_triggers(c)
    c.execute("CREATE INDEX IF NOT EXISTS idx_coverage_epochs_spec "
              "ON coverage_epochs(chain,venue,factory,topic,to_block)")
    return c


def ensure_bridge_started_at(*, at: datetime | None = None) -> str:
    """Freeze the forward-test boundary and quarantine pre-deployment inventory."""
    now = (at or datetime.now(timezone.utc)).astimezone(timezone.utc).isoformat()
    c = _conn()
    try:
        c.execute("BEGIN IMMEDIATE")
        c.execute("INSERT OR IGNORE INTO bridge_meta(key,value,updated_at) "
                  "VALUES ('launch_bridge_started_at',?,?)", (now, now))
        started = c.execute(
            "SELECT value FROM bridge_meta WHERE key='launch_bridge_started_at'"
        ).fetchone()[0]
        for table in ("raw_pools", "raw_v3_pools"):
            c.execute(f"UPDATE {table} SET qualification_state='historical_raw_only', "
                      "qualification_reason='observed before forward bridge boundary' "
                      "WHERE detected_at<? AND qualification_state='raw_unqualified'",
                      (started,))
        c.commit()
        return started
    finally:
        c.close()


def qualification_batch(*, now: datetime | None = None, limit: int = 10,
                        max_age_hours: float = 24) -> list[dict]:
    """Return forward, non-reorg factory rows due for an exact-pool market check."""
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    started = ensure_bridge_started_at(at=now)
    cutoff = (now - timedelta(hours=max(0, float(max_age_hours)))).isoformat()
    c = _conn()
    try:
        for table in ("raw_pools", "raw_v3_pools"):
            c.execute(f"""UPDATE {table} SET qualification_state='expired_unqualified',
                              qualification_reason='older than 24h launch window',
                              qualification_attempted_at=?
                           WHERE detected_at>=? AND COALESCE(block_at,detected_at)<?
                             AND removed=0 AND evidence_state='complete'
                             AND qualification_state IN
                               ('raw_unqualified','market_pending','market_error','below_threshold')""",
                      (now.isoformat(), started, cutoff))
        c.commit()
        common = """chain,venue,factory,transaction_hash,log_index,block_number,
                    block_hash,transaction_index,token0,token1,pool,block_at,detected_at,
                    qualification_state,qualification_attempts,qualification_retry_at"""
        rows = c.execute(f"""SELECT 'v2' AS table_kind,{common} FROM raw_pools
            WHERE detected_at>=? AND removed=0 AND evidence_state='complete'
              AND qualification_state IN
                ('raw_unqualified','market_pending','market_error','below_threshold')
              AND (qualification_retry_at IS NULL OR qualification_retry_at<=?)
            UNION ALL SELECT 'v3' AS table_kind,{common} FROM raw_v3_pools
            WHERE detected_at>=? AND removed=0 AND evidence_state='complete'
              AND qualification_state IN
                ('raw_unqualified','market_pending','market_error','below_threshold')
              AND (qualification_retry_at IS NULL OR qualification_retry_at<=?)
            ORDER BY block_at,detected_at LIMIT ?""",
            (started, now.isoformat(), started, now.isoformat(), max(0, int(limit))),
        ).fetchall()
    finally:
        c.close()
    keys = ("table_kind", "chain", "venue", "factory", "transaction_hash",
            "log_index", "block_number", "block_hash", "transaction_index",
            "token0", "token1", "pool", "block_at", "detected_at",
            "qualification_state", "qualification_attempts", "qualification_retry_at")
    return [dict(zip(keys, row)) for row in rows]


def set_qualification(row: dict, state: str, *, reason: str | None = None,
                      target_token: str | None = None,
                      ledger_event_id: str | None = None,
                      retry_after_seconds: float | None = None,
                      at: datetime | None = None) -> bool:
    """Conditionally transition a raw row while preserving reorg terminal state."""
    if state not in QUALIFICATION_STATES:
        raise ValueError(f"unknown qualification state: {state}")
    table = {"v2": "raw_pools", "v3": "raw_v3_pools"}.get(row.get("table_kind"))
    if not table:
        raise ValueError("qualification row has unknown table_kind")
    now = (at or datetime.now(timezone.utc)).astimezone(timezone.utc)
    retry_at = ((now + timedelta(seconds=max(0, float(retry_after_seconds)))).isoformat()
                if retry_after_seconds is not None else None)
    qualified_at = now.isoformat() if state == "qualified_recorded" else None
    c = _conn()
    try:
        changed = c.execute(f"""UPDATE {table} SET qualification_state=?,
                    qualification_attempted_at=?,qualification_retry_at=?,
                    qualification_reason=?,qualification_attempts=qualification_attempts+1,
                    qualified_at=COALESCE(?,qualified_at),
                    ledger_event_id=COALESCE(?,ledger_event_id),
                    target_token=COALESCE(?,target_token)
                 WHERE chain=? AND transaction_hash=? AND log_index=? AND removed=0
                   AND qualification_state IN
                     ('raw_unqualified','market_pending','market_error','below_threshold')""",
            (state, now.isoformat(), retry_at, str(reason)[:240] if reason else None,
             qualified_at, ledger_event_id, target_token.lower() if target_token else None,
             row["chain"], row["transaction_hash"].lower(), int(row["log_index"])),
        ).rowcount
        c.commit()
        return bool(changed)
    finally:
        c.close()


def qualification_summary(
    *, ledger_readback: Callable[[str, str, str], bool] | None = None,
) -> dict:
    """Report coverage without trusting cross-database IDs by presence alone.

    The stream database cannot enforce a foreign key into the opportunity
    ledger.  Historical rows marked ``qualified_recorded`` therefore count as
    recorded only when an injected reader confirms the exact ledger ID, chain,
    and target token.  Failed read-backs are exposed as orphans; an unavailable
    reader is reported separately instead of silently accepting the claim.
    """
    c = _conn()
    try:
        rows = c.execute("""SELECT evidence_state,qualification_state,COUNT(*) FROM raw_pools
                            GROUP BY evidence_state,qualification_state
                            UNION ALL
                            SELECT evidence_state,qualification_state,COUNT(*) FROM raw_v3_pools
                            GROUP BY evidence_state,qualification_state""").fetchall()
        started = c.execute(
            "SELECT value FROM bridge_meta WHERE key='launch_bridge_started_at'"
        ).fetchone()
        marked_recorded = c.execute("""SELECT ledger_event_id,chain,target_token
            FROM raw_pools WHERE qualification_state='qualified_recorded'
            UNION ALL SELECT ledger_event_id,chain,target_token
            FROM raw_v3_pools WHERE qualification_state='qualified_recorded'""").fetchall()
        quarantined_ids = c.execute("""SELECT ledger_event_id FROM raw_pools
            WHERE qualification_state='ledger_orphan'
            UNION ALL SELECT ledger_event_id FROM raw_v3_pools
            WHERE qualification_state='ledger_orphan'""").fetchall()
    finally:
        c.close()
    evidence, raw_qualification = {}, {}
    for evidence_state, qualification_state, count in rows:
        evidence[evidence_state] = evidence.get(evidence_state, 0) + count
        raw_qualification[qualification_state] = (
            raw_qualification.get(qualification_state, 0) + count)

    traceable_ids: set[str] = set()
    orphan_ids = {str(row[0]) for row in quarantined_ids if row[0]}
    traceable_rows = 0
    orphan_rows = 0
    missing_id_rows = 0
    missing_identity_rows = 0
    readback_unavailable_rows = 0
    readback_error_rows = 0
    for ledger_id, chain, target_token in marked_recorded:
        if not ledger_id:
            missing_id_rows += 1
            orphan_rows += 1
            continue
        if not chain or not target_token:
            missing_identity_rows += 1
            orphan_rows += 1
            orphan_ids.add(str(ledger_id))
            continue
        if ledger_readback is None:
            readback_unavailable_rows += 1
            continue
        try:
            valid = bool(ledger_readback(
                str(ledger_id), str(chain), str(target_token)))
        except Exception:
            readback_error_rows += 1
            continue
        if valid:
            traceable_rows += 1
            traceable_ids.add(str(ledger_id))
        else:
            orphan_rows += 1
            orphan_ids.add(str(ledger_id))

    qualification = dict(raw_qualification)
    if marked_recorded or "qualified_recorded" in qualification:
        qualification["qualified_recorded"] = len(traceable_ids)
    quarantined_state_rows = int(raw_qualification.get("ledger_orphan") or 0)
    if orphan_rows or quarantined_state_rows:
        qualification["ledger_orphan"] = orphan_rows + quarantined_state_rows
    unavailable_rows = readback_unavailable_rows + readback_error_rows
    if unavailable_rows:
        qualification["ledger_readback_unavailable"] = unavailable_rows
    return {"raw_total": sum(evidence.values()), "evidence": evidence,
            "qualification": qualification,
            "raw_qualification_states": raw_qualification,
            "traceability": {
                "state": ("unavailable" if unavailable_rows else
                          "ok" if not orphan_rows and not quarantined_state_rows else
                          "partial"),
                "raw_marked_recorded_rows": len(marked_recorded),
                "traceable_rows": traceable_rows,
                "traceable_unique_ledger_events": len(traceable_ids),
                "orphan_rows": orphan_rows + quarantined_state_rows,
                "orphan_unique_ledger_ids": len(orphan_ids),
                "missing_ledger_id_rows": missing_id_rows,
                "missing_identity_rows": missing_identity_rows,
                "quarantined_state_rows": quarantined_state_rows,
                "readback_unavailable_rows": readback_unavailable_rows,
                "readback_error_rows": readback_error_rows,
            },
            "bridge_started_at": started[0] if started else None}


def subscribe_requests(spec: FactorySpec) -> list[dict]:
    return [
        {"jsonrpc": "2.0", "id": 1, "method": "eth_subscribe", "params": [
            "logs", {"address": spec.address, "topics": [spec.topic]},
        ]},
        {"jsonrpc": "2.0", "id": 2, "method": "eth_subscribe",
         "params": ["newHeads"]},
    ]


def _hex_int(value: object, field: str) -> int:
    if (not isinstance(value, str)
            or re.fullmatch(r"0x(?:0|[1-9a-fA-F][0-9a-fA-F]*)", value) is None):
        raise ValueError(f"EVM factory log has invalid {field}")
    number = int(value, 16)
    if number < 0:
        raise ValueError(f"EVM factory log has invalid {field}")
    return number


def _raw_word_address(raw: str, field: str) -> str:
    if (len(raw) != 64 or any(ch not in "0123456789abcdef" for ch in raw)
            or raw[:24] != "0" * 24):
        raise ValueError(f"EVM factory log has invalid {field}")
    return "0x" + raw[-40:]


def _word_address(value: str, field: str) -> str:
    if not isinstance(value, str) or not value.startswith("0x"):
        raise ValueError(f"EVM factory log has invalid {field}")
    return _raw_word_address(value[2:].lower(), field)


def _hash32(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.startswith("0x"):
        raise ValueError(f"EVM factory log has invalid {field}")
    raw = value[2:].lower()
    if len(raw) != 64 or any(ch not in "0123456789abcdef" for ch in raw):
        raise ValueError(f"EVM factory log has invalid {field}")
    return "0x" + raw


def _uint_word(value: object, field: str) -> int:
    raw = str(value).lower()
    if len(raw) != 64 or any(ch not in "0123456789abcdef" for ch in raw):
        raise ValueError(f"EVM factory log has invalid {field}")
    return int(raw, 16)


def _topic_uint_word(value: object, field: str) -> int:
    if not isinstance(value, str) or not value.startswith("0x"):
        raise ValueError(f"EVM factory log has invalid {field}")
    return _uint_word(value[2:], field)


def _int24_word(value: str, field: str) -> int:
    number = _uint_word(value, field)
    if number >= 1 << 255:
        number -= 1 << 256
    if not -(1 << 23) <= number < (1 << 23):
        raise ValueError(f"EVM factory log has invalid {field}")
    return number


def parse_message(raw: object, *, spec: FactorySpec | None = None) -> StreamEvent | None:
    spec = spec or bsc_pancake_v2_spec()
    msg = json.loads(raw) if isinstance(raw, (str, bytes, bytearray)) else raw
    if not isinstance(msg, dict):
        raise ValueError("EVM websocket message must be an object")
    if msg.get("jsonrpc") != "2.0":
        raise ValueError("EVM websocket JSON-RPC version is invalid")
    request_id = msg.get("id")
    if "id" in msg:
        if type(request_id) is not int or request_id not in {1, 2}:
            raise PermissionError("EVM websocket response id is invalid")
        has_result = "result" in msg
        has_error = "error" in msg
        if has_result == has_error:
            raise PermissionError("EVM websocket response envelope is invalid")
        if has_error:
            # Provider errors may echo credential-bearing request URLs.
            raise PermissionError(
                "EVM subscription rejected; error_kind=remote_error",
            )
        subscription_id = msg.get("result")
        if (not isinstance(subscription_id, str)
                or not subscription_id.strip()):
            raise PermissionError("EVM subscription acknowledgement is invalid")
        return None
    if "error" in msg or "result" in msg:
        raise PermissionError("EVM websocket notification envelope is invalid")
    if msg.get("method") != "eth_subscription":
        return None
    params = msg.get("params")
    if not isinstance(params, dict) or not isinstance(params.get("result"), dict):
        raise ValueError("EVM websocket notification payload is invalid")
    result = params["result"]
    if "number" in result and "topics" not in result:
        block = _hex_int(result.get("number"), "block number")
        block_hash = _hash32(result.get("hash"), "block hash")
        timestamp = _hex_int(result.get("timestamp"), "timestamp")
        event_at = datetime.fromtimestamp(timestamp, timezone.utc)
        return StreamEvent({"kind": "head", "block_number": block,
                            "block_hash": block_hash},
                           cursor=block, event_at=event_at)
    topics = result.get("topics") or []
    expected_topics = 3 if spec.event_kind == "pair_v2" else 4
    if (str(result.get("address", "")).lower() != spec.address
            or len(topics) != expected_topics
            or str(topics[0]).lower() != spec.topic):
        return None
    encoded_data = result.get("data")
    if not isinstance(encoded_data, str) or not encoded_data.startswith("0x"):
        raise ValueError("factory creation data must be 0x-prefixed")
    data = encoded_data[2:].lower()
    if len(data) != 128:
        raise ValueError("factory creation data must contain two ABI words")
    if "removed" not in result or not isinstance(result.get("removed"), bool):
        raise ValueError("EVM factory log removed flag must be boolean")
    removed = result["removed"]
    token0 = _word_address(topics[1], "token0")
    token1 = _word_address(topics[2], "token1")
    pool_word = data[64:] if spec.event_kind == "pool_v3" else data[:64]
    pool = _raw_word_address(pool_word, "pool")
    zero = "0x" + "0" * 40
    if token0 == zero or token1 == zero or pool == zero:
        raise ValueError("PairCreated addresses must be non-zero")
    if int(token0, 16) >= int(token1, 16):
        raise ValueError("PairCreated tokens must be distinct and sorted")
    payload = {
        "kind": spec.event_kind, "chain": spec.chain, "venue": spec.venue,
        "factory": spec.address,
        "transaction_hash": _hash32(result.get("transactionHash"), "transaction hash"),
        "log_index": _hex_int(result.get("logIndex"), "log index"),
        "block_number": _hex_int(result.get("blockNumber"), "block number"),
        "block_hash": _hash32(result.get("blockHash"), "block hash"),
        "transaction_index": _hex_int(result.get("transactionIndex"), "transaction index"),
        "token0": token0, "token1": token1, "pool": pool,
        "removed": removed,
    }
    if spec.event_kind == "pair_v2":
        pair_index = _uint_word(data[64:], "pair index")
        if pair_index <= 0:
            raise ValueError("PairCreated index must be positive")
        payload["pair_index"] = pair_index
    elif spec.event_kind == "pool_v3":
        fee = _topic_uint_word(topics[3], "fee")
        tick_spacing = _int24_word(data[:64], "tick spacing")
        if not 0 < fee <= 1_000_000 or tick_spacing <= 0:
            raise ValueError("PoolCreated fee and tick spacing must be positive")
        payload["fee"] = fee
        payload["tick_spacing"] = tick_spacing
    elif spec.event_kind == "aerodrome_pool":
        stable = _topic_uint_word(topics[3], "stable")
        pool_index = _uint_word(data[64:], "pool index")
        if stable not in {0, 1} or pool_index <= 0:
            raise ValueError("Aerodrome stable flag or pool index is invalid")
        payload["stable"] = bool(stable)
        payload["pair_index"] = pool_index
    else:
        raise ValueError(f"unsupported factory event kind: {spec.event_kind}")
    return StreamEvent(payload)


class EvmSubscriptionParser:
    """Bind notifications to both acknowledged subscriptions per connection."""
    def __init__(self, spec: FactorySpec):
        self.spec = spec
        self.reset()

    def reset(self) -> None:
        self.subscription_ids: dict[int, str] = {}

    def __call__(self, raw: object) -> StreamEvent | None:
        msg = json.loads(raw) if isinstance(raw, (str, bytes, bytearray)) else raw
        if not isinstance(msg, dict):
            raise ValueError("EVM websocket message must be an object")
        request_id = msg.get("id")
        if type(request_id) is int and request_id in {1, 2}:
            parse_message(msg, spec=self.spec)
            subscription_id = str(msg["result"])
            previous = self.subscription_ids.get(int(request_id))
            if previous is not None and previous != subscription_id:
                raise PermissionError("EVM subscription acknowledgement changed")
            if (subscription_id in self.subscription_ids.values()
                    and previous != subscription_id):
                raise PermissionError("EVM subscriptions share one identifier")
            self.subscription_ids[int(request_id)] = subscription_id
            return None
        if msg.get("method") != "eth_subscription":
            return parse_message(msg, spec=self.spec)
        if set(self.subscription_ids) != {1, 2}:
            raise PermissionError("EVM notification arrived before both subscriptions")
        params = msg.get("params")
        subscription_id = (
            params.get("subscription") if isinstance(params, dict) else None
        )
        if not isinstance(subscription_id, str) or not subscription_id:
            raise PermissionError("EVM notification subscription id is invalid")
        if subscription_id not in self.subscription_ids.values():
            raise PermissionError("EVM notification has an unacknowledged subscription")
        event = parse_message(msg, spec=self.spec)
        expected_logs = subscription_id == self.subscription_ids[1]
        is_head = bool(event is not None and isinstance(event.payload, dict)
                       and event.payload.get("kind") == "head")
        if expected_logs and event is None:
            raise ValueError("EVM log subscription returned an invalid factory event")
        if expected_logs == is_head:
            raise ValueError("EVM notification does not match its subscription")
        return event


class JsonRpc:
    def __init__(self, endpoints: tuple[str, ...], *, timeout: float = 15):
        if not endpoints:
            raise ValueError("at least one EVM RPC endpoint is required")
        self.endpoints = endpoints
        self.timeout = timeout
        self._index = 0

    def _call_endpoint(self, endpoint: str, method: str, params: list) -> object:
        request_id = uuid.uuid4().hex
        body = json.dumps({"jsonrpc": "2.0", "id": request_id, "method": method,
                           "params": params}).encode()
        request = urllib.request.Request(
            endpoint, data=body,
            headers={"Content-Type": "application/json",
                     "User-Agent": _BROWSER_UA})
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                geturl = getattr(response, "geturl", None)
                if not callable(geturl):
                    raise RuntimeError("EVM RPC response URL is unavailable")
                try:
                    final_url = geturl()
                    final_identity = provider_id(final_url)
                    original_scheme = urllib.parse.urlsplit(endpoint).scheme
                    final_scheme = urllib.parse.urlsplit(final_url).scheme
                except (TypeError, ValueError):
                    raise RuntimeError("EVM RPC response URL is invalid") from None
                if final_identity != provider_id(endpoint):
                    raise ProviderIndependenceError(
                        "EVM RPC crossed provider identity during redirect",
                    )
                if original_scheme == "https" and final_scheme != "https":
                    raise ProviderIndependenceError(
                        "EVM RPC redirect downgraded transport security",
                    )
                result = json.load(response)
        except urllib.error.HTTPError as exc:
            # The rejected response owns a socket too.  Always close it before
            # another provider is attempted.
            exc.close()
            raise
        if not isinstance(result, dict):
            raise RuntimeError(f"EVM RPC returned malformed {method} response")
        response_id = result.get("id")
        if result.get("jsonrpc") != "2.0" or response_id != request_id:
            raise RuntimeError(f"EVM RPC returned mismatched {method} envelope")
        has_result = "result" in result
        has_error = "error" in result
        if has_result == has_error:
            raise RuntimeError(f"EVM RPC returned ambiguous {method} envelope")
        if has_error:
            remote_error = result.get("error")
            if (not isinstance(remote_error, dict)
                    or isinstance(remote_error.get("code"), bool)
                    or not isinstance(remote_error.get("code"), int)
                    or not isinstance(remote_error.get("message"), str)):
                raise RuntimeError(f"EVM RPC returned malformed {method} error")
            # Never interpolate the remote payload; it can echo a keyed URL.
            raise RuntimeError(f"EVM RPC returned remote {method} error")
        return result.get("result")

    def call_with_provider(self, method: str, params: list, *,
                           exclude_provider_ids: set[str] | frozenset[str] = frozenset()
                           ) -> tuple[object, str]:
        """Call one endpoint and return its sanitized identity with the result."""
        last_error: object = "no endpoint attempted"
        attempted = 0
        for offset in range(len(self.endpoints)):
            index = (self._index + offset) % len(self.endpoints)
            endpoint = self.endpoints[index]
            identity = provider_id(endpoint)
            if identity in exclude_provider_ids:
                continue
            attempted += 1
            try:
                result = self._call_endpoint(endpoint, method, params)
                self._index = index
                return result, identity
            except Exception as exc:
                last_error = exc
        if not attempted:
            raise ProviderIndependenceError(
                f"no independent EVM HTTP provider is configured for {method}")
        raise RuntimeError(
            f"all EVM RPC endpoints failed for {method}; "
            f"error_kind={type(last_error).__name__}"
        )

    def call_from_provider(self, identity: str, method: str, params: list) -> object:
        """Keep an audit epoch on the provider that supplied its finalized head."""
        matches = [(index, endpoint) for index, endpoint in enumerate(self.endpoints)
                   if provider_id(endpoint) == identity]
        if not matches:
            raise ProviderIndependenceError("selected EVM HTTP provider is unavailable")
        last_error: object = "no endpoint attempted"
        for index, endpoint in matches:
            try:
                return self._call_endpoint(endpoint, method, params)
            except Exception as exc:
                last_error = exc
                # The next coverage cycle should probe a different configured
                # provider instead of repeatedly selecting a node whose head works
                # but whose getLogs method is broken or rate-limited.
                self._index = (index + 1) % len(self.endpoints)
        raise RuntimeError(
            f"selected EVM RPC provider failed for {method}; "
            f"error_kind={type(last_error).__name__}"
        )

    def call(self, method: str, params: list) -> object:
        result, _identity = self.call_with_provider(method, params)
        return result


def _block_at(
    rpc: JsonRpc, block_number: int, expected_block_hash: object,
) -> str:
    block = rpc.call("eth_getBlockByNumber", [hex(block_number), False])
    if (not isinstance(block, dict) or block.get("number") is None
            or block.get("timestamp") is None or block.get("hash") is None):
        raise RuntimeError("EVM canonical block evidence is unavailable")
    if _hex_int(block["number"], "canonical block number") != block_number:
        raise RuntimeError("EVM RPC returned the wrong block")
    observed_hash = _hash32(block.get("hash"), "canonical block hash")
    expected_hash = _hash32(expected_block_hash, "factory log block hash")
    if observed_hash != expected_hash:
        raise RuntimeError("EVM factory log does not match the canonical block")
    timestamp = _hex_int(block["timestamp"], "canonical block timestamp")
    return datetime.fromtimestamp(timestamp, timezone.utc).isoformat()


def _hash(payload: object) -> str:
    return hashlib.sha256(json.dumps(
        payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def persisted_evidence_state(payload: object) -> str | None:
    """Read back the exact committed raw row instead of trusting writer intent."""
    if (not isinstance(payload, dict)
            or payload.get("kind") not in {"pair_v2", "pool_v3", "aerodrome_pool"}):
        return None
    table = "raw_v3_pools" if payload["kind"] == "pool_v3" else "raw_pools"
    c = _conn()
    try:
        row = c.execute(
            f"""SELECT evidence_state,raw_payload_hash,block_at,removed
                  FROM {table}
                 WHERE chain=? AND transaction_hash=? AND log_index=?""",
            (payload["chain"], str(payload["transaction_hash"]).lower(),
             int(payload["log_index"])),
        ).fetchone()
    finally:
        c.close()
    if row is None or str(row[1]) != _hash(payload):
        return None
    state, _payload_hash, block_at, removed = row
    if state == "complete" and (not block_at or int(removed) != 0):
        return None
    if state == "removed_reorg" and int(removed) != 1:
        return None
    return str(state)


def _existing_raw_payload(
    c: sqlite3.Connection, payload: dict,
) -> tuple[dict, str] | None:
    table = "raw_v3_pools" if payload["kind"] == "pool_v3" else "raw_pools"
    if table == "raw_v3_pools":
        columns = (
            "venue,factory,block_number,block_hash,transaction_index,token0,token1,"
            "pool,fee,tick_spacing,removed,raw_payload_hash"
        )
    else:
        columns = (
            "venue,factory,block_number,block_hash,transaction_index,token0,token1,"
            "pool,pair_index,stable,removed,raw_payload_hash"
        )
    row = c.execute(
        f"SELECT {columns} FROM {table} "
        "WHERE chain=? AND transaction_hash=? AND log_index=?",
        (payload["chain"], str(payload["transaction_hash"]).lower(),
         int(payload["log_index"])),
    ).fetchone()
    if row is None:
        return None
    existing = {
        "kind": payload["kind"], "chain": payload["chain"],
        "venue": row[0], "factory": row[1],
        "transaction_hash": str(payload["transaction_hash"]).lower(),
        "log_index": int(payload["log_index"]), "block_number": int(row[2]),
        "block_hash": row[3], "transaction_index": int(row[4]),
        "token0": row[5], "token1": row[6], "pool": row[7],
        "removed": bool(row[10]),
    }
    if table == "raw_v3_pools":
        existing.update({"fee": int(row[8]), "tick_spacing": int(row[9])})
    else:
        existing["pair_index"] = int(row[8])
        if payload["kind"] == "aerodrome_pool":
            existing["stable"] = bool(row[9])
    return existing, str(row[11])


def persist(payload: object, *, rpc: JsonRpc | None = None) -> str | None:
    if (not isinstance(payload, dict)
            or payload.get("kind") not in {"pair_v2", "pool_v3", "aerodrome_pool"}):
        return None
    payload_hash = _hash(payload)
    now = datetime.now(timezone.utc).isoformat()
    block_at = None
    state = "removed_reorg" if payload["removed"] else "timestamp_unavailable"
    if not payload["removed"] and rpc is not None:
        try:
            block_at = _block_at(
                rpc, payload["block_number"], payload.get("block_hash"),
            )
            state = "complete"
        except Exception as exc:
            logger.warning("evm_pool_timestamp_failed", chain=payload["chain"],
                           block=payload["block_number"],
                           error_kind=type(exc).__name__)
    c = _conn()
    ledger_event_id = None
    try:
        c.execute("BEGIN IMMEDIATE")
        existing = _existing_raw_payload(c, payload)
        if existing is not None:
            existing_payload, stored_hash = existing
            if _hash(existing_payload) != stored_hash:
                raise RawEvidenceConflict("persisted raw evidence hash is corrupt")
            if payload["removed"]:
                comparable = dict(payload)
                comparable["removed"] = existing_payload["removed"]
                if _hash(comparable) != stored_hash:
                    raise RawEvidenceConflict(
                        "removed factory evidence changed immutable fields",
                    )
            elif existing_payload["removed"] or payload_hash != stored_hash:
                raise RawEvidenceConflict(
                    "factory evidence identity changed immutable payload",
                )
        evidence_changed = existing is None or payload_hash != existing[1]
        common = (payload["chain"], payload["venue"], payload["factory"],
                  payload["transaction_hash"].lower(), payload["log_index"],
                  payload["block_number"], payload.get("block_hash"),
                  payload["transaction_index"], payload["token0"], payload["token1"],
                  payload["pool"])
        if payload["kind"] in {"pair_v2", "aerodrome_pool"}:
            c.execute("""INSERT INTO raw_pools(
                chain,venue,factory,transaction_hash,log_index,block_number,block_hash,
                transaction_index,token0,token1,pool,pair_index,stable,block_at,
                detected_at,updated_at,raw_payload_hash,removed,evidence_state,
                qualification_state
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'raw_unqualified')
            ON CONFLICT(chain,transaction_hash,log_index) DO UPDATE SET
              block_number=excluded.block_number,block_hash=excluded.block_hash,
              block_at=COALESCE(raw_pools.block_at,excluded.block_at),
              stable=excluded.stable,
              updated_at=excluded.updated_at,raw_payload_hash=excluded.raw_payload_hash,
              removed=excluded.removed,evidence_state=CASE
                WHEN raw_pools.evidence_state='complete'
                     AND excluded.evidence_state='timestamp_unavailable'
                     AND raw_pools.raw_payload_hash=excluded.raw_payload_hash
                  THEN raw_pools.evidence_state
                ELSE excluded.evidence_state END,
              qualification_state=CASE WHEN excluded.removed=1 THEN 'reorg_removed'
                                       ELSE raw_pools.qualification_state END,
              qualification_reason=CASE WHEN excluded.removed=1 THEN 'factory log removed by reorg'
                                        ELSE raw_pools.qualification_reason END""",
                      (*common, payload["pair_index"],
                       int(payload["stable"]) if "stable" in payload else None,
                       block_at, now, now,
                       payload_hash, int(payload["removed"]), state))
            if payload["removed"]:
                c.execute("""UPDATE raw_pools SET qualification_state='reorg_removed',
                              qualification_reason='factory log removed by reorg'
                           WHERE chain=? AND transaction_hash=? AND log_index=?""",
                          (payload["chain"], payload["transaction_hash"].lower(),
                           payload["log_index"]))
                row = c.execute(
                    "SELECT ledger_event_id FROM raw_pools WHERE chain=? "
                    "AND transaction_hash=? AND log_index=?",
                    (payload["chain"], payload["transaction_hash"].lower(),
                     payload["log_index"]),
                ).fetchone()
                ledger_event_id = row[0] if row else None
        else:
            c.execute("""INSERT INTO raw_v3_pools(
                chain,venue,factory,transaction_hash,log_index,block_number,block_hash,
                transaction_index,token0,token1,pool,fee,tick_spacing,block_at,
                detected_at,updated_at,raw_payload_hash,removed,evidence_state,
                qualification_state
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'raw_unqualified')
            ON CONFLICT(chain,transaction_hash,log_index) DO UPDATE SET
              block_number=excluded.block_number,block_hash=excluded.block_hash,
              block_at=COALESCE(raw_v3_pools.block_at,excluded.block_at),
              updated_at=excluded.updated_at,raw_payload_hash=excluded.raw_payload_hash,
              removed=excluded.removed,evidence_state=CASE
                WHEN raw_v3_pools.evidence_state='complete'
                     AND excluded.evidence_state='timestamp_unavailable'
                     AND raw_v3_pools.raw_payload_hash=excluded.raw_payload_hash
                  THEN raw_v3_pools.evidence_state
                ELSE excluded.evidence_state END,
              qualification_state=CASE WHEN excluded.removed=1 THEN 'reorg_removed'
                                       ELSE raw_v3_pools.qualification_state END,
              qualification_reason=CASE WHEN excluded.removed=1 THEN 'factory log removed by reorg'
                                        ELSE raw_v3_pools.qualification_reason END""",
                      (*common, payload["fee"], payload["tick_spacing"], block_at,
                       now, now, payload_hash, int(payload["removed"]), state))
            if payload["removed"]:
                c.execute("""UPDATE raw_v3_pools SET qualification_state='reorg_removed',
                              qualification_reason='factory log removed by reorg'
                           WHERE chain=? AND transaction_hash=? AND log_index=?""",
                          (payload["chain"], payload["transaction_hash"].lower(),
                           payload["log_index"]))
                row = c.execute(
                    "SELECT ledger_event_id FROM raw_v3_pools WHERE chain=? "
                    "AND transaction_hash=? AND log_index=?",
                    (payload["chain"], payload["transaction_hash"].lower(),
                     payload["log_index"]),
                ).fetchone()
                ledger_event_id = row[0] if row else None
        if evidence_changed:
            affected = c.execute(
                """SELECT topic FROM coverage_state
                     WHERE chain=? AND venue=? AND factory=?
                       AND coverage_started_block IS NOT NULL
                       AND coverage_started_block<=?
                       AND verified_through_block>=?""",
                (payload["chain"], payload["venue"], payload["factory"],
                 int(payload["block_number"]), int(payload["block_number"])),
            ).fetchall()
            if affected:
                c.execute(
                    """UPDATE coverage_state
                          SET verified_through_block=coverage_started_block-1,
                              verified_through_hash=NULL,verified_at=NULL,
                              provider_independent=0,status='blocked',
                              consecutive_failures=0,next_retry_at=NULL,
                              last_error_kind='late_evidence_after_coverage',
                              updated_at=?
                        WHERE chain=? AND venue=? AND factory=?
                          AND coverage_started_block IS NOT NULL
                          AND coverage_started_block<=?
                          AND verified_through_block>=?""",
                    (now, payload["chain"], payload["venue"], payload["factory"],
                     int(payload["block_number"]),
                     int(payload["block_number"])),
                )
                for (topic,) in affected:
                    c.execute(
                        """DELETE FROM coverage_epochs
                             WHERE chain=? AND venue=? AND factory=? AND topic=?""",
                        (payload["chain"], payload["venue"],
                         payload["factory"], topic),
                    )
        c.commit()
    except Exception:
        c.rollback()
        raise
    finally:
        c.close()
    if payload["removed"] and ledger_event_id:
        from src.pipeline.opportunity_ledger import invalidate
        invalidate(ledger_event_id, state="reorg_removed",
                   reason=(f"{payload['chain']} factory log removed by reorg: "
                           f"{payload['transaction_hash']}:{payload['log_index']}"))
    return persisted_evidence_state(payload)


def _persist_stream_event(payload: object, *, spec: FactorySpec,
                          rpc: JsonRpc) -> str | None:
    """Block before the writer so StreamRunner cannot advance its health cursor."""
    stream_disk_guard.GUARD.require_evidence_write(spec.chain)
    state = persist(payload, rpc=rpc)
    if (isinstance(payload, dict)
            and payload.get("kind") in {"pair_v2", "pool_v3", "aerodrome_pool"}
            and state not in {"complete", "removed_reorg"}):
        raise RawEvidenceConflict("factory evidence did not reach a durable terminal state")
    return state


def _backfill_blocks(start: int, end: int, *, spec: FactorySpec, rpc: JsonRpc) -> None:
    if end < start:
        return
    if end - start + 1 > MAX_BACKFILL_BLOCKS:
        raise ValueError("EVM backfill range exceeds bounded block budget")
    # Gap repair is evidence persistence too.  Refuse before the first RPC so a
    # full volume cannot turn recovery into more in-memory data or DB writes.
    stream_disk_guard.GUARD.require_evidence_write(spec.chain)
    logs = rpc.call("eth_getLogs", [{
        "address": spec.address, "topics": [spec.topic],
        "fromBlock": hex(start), "toBlock": hex(end),
    }])
    if not isinstance(logs, list):
        raise RuntimeError("eth_getLogs returned a non-list result")
    for item in logs:
        event = parse_message({"jsonrpc": "2.0", "method": "eth_subscription",
                               "params": {"result": item}}, spec=spec)
        if event:
            if not start <= event.payload["block_number"] <= end:
                raise RuntimeError("eth_getLogs returned an event outside requested range")
            evidence_state = _persist_stream_event(event.payload, spec=spec, rpc=rpc)
            if (evidence_state != "complete"
                    or persisted_evidence_state(event.payload) != "complete"):
                raise RuntimeError("EVM backfill evidence is incomplete")


def _coverage_key(spec: FactorySpec) -> tuple[str, str, str, str]:
    return spec.chain, spec.venue, spec.address, spec.topic


def coverage_snapshot(spec: FactorySpec) -> dict:
    """Return the persisted finalized-range proof for one exact factory filter."""
    c = _conn()
    try:
        row = c.execute(
            """SELECT s.coverage_started_block,s.verified_through_block,
                      s.safe_head_block,s.verified_through_hash,s.safe_head_hash,
                      s.safe_head_at,s.audit_duration_ms,s.verified_at,
                      s.ws_provider_id,s.http_provider_id,s.provider_independent,
                      s.status,s.consecutive_failures,s.next_retry_at,
                      s.last_error_kind,s.updated_at,
                      (SELECT MIN(e.from_block) FROM coverage_epochs AS e
                        WHERE e.chain=s.chain AND e.venue=s.venue
                          AND e.factory=s.factory AND e.topic=s.topic)
                 FROM coverage_state AS s
                WHERE s.chain=? AND s.venue=? AND s.factory=? AND s.topic=?""",
            _coverage_key(spec),
        ).fetchone()
    finally:
        c.close()
    keys = (
        "coverage_started_block", "verified_through_block", "safe_head_block",
        "verified_through_hash", "safe_head_hash", "safe_head_at",
        "audit_duration_ms", "verified_at",
        "ws_provider_id", "http_provider_id",
        "provider_independent", "state", "consecutive_failures",
        "next_retry_at", "last_error_kind", "updated_at",
    )
    if row is None:
        return {
            "coverage_started_block": None, "verified_through_block": None,
            "safe_head_block": None, "verified_through_hash": None,
            "safe_head_hash": None, "safe_head_at": None,
            "audit_duration_ms": None, "verified_at": None,
            "ws_provider_id": None, "http_provider_id": None,
            "provider_independent": False, "state": "missing",
            "lag_blocks": None,
            "consecutive_failures": 0, "next_retry_at": None,
            "last_error_kind": "coverage_not_initialized", "updated_at": None,
        }
    item = dict(zip(keys, row[:-1]))
    retained_epoch_start = row[-1] if type(row[-1]) is int else None
    error_kind = item.get("last_error_kind")
    if (not isinstance(error_kind, str)
            or re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,79}", error_kind) is None):
        item["last_error_kind"] = (
            None if error_kind is None else "invalid_coverage_error"
        )
    claimed_independent = item["provider_independent"] == 1
    ws_raw = item.get("ws_provider_id")
    http_raw = item.get("http_provider_id")
    ws_identity = (ws_raw if isinstance(ws_raw, str)
                   and _PROVIDER_FINGERPRINT_RE.fullmatch(ws_raw) else None)
    http_identity = (http_raw if isinstance(http_raw, str)
                     and _PROVIDER_FINGERPRINT_RE.fullmatch(http_raw) else None)
    item["ws_provider_id"] = ws_identity
    item["http_provider_id"] = http_identity
    item["provider_independent"] = bool(
        claimed_independent and ws_identity and http_identity
        and ws_identity != http_identity
    )
    verified, head = item.get("verified_through_block"), item.get("safe_head_block")
    started_number = (item.get("coverage_started_block")
                      if type(item.get("coverage_started_block")) is int else None)
    verified_number = verified if type(verified) is int else None
    head_number = head if type(head) is int else None
    item["lag_blocks"] = (
        head_number - verified_number
        if verified_number is not None and head_number is not None else None
    )
    try:
        verified_hash = _hash32(
            item.get("verified_through_hash"), "coverage checkpoint hash",
        )
        verified_hash_valid = True
    except (TypeError, ValueError):
        verified_hash = None
        verified_hash_valid = False
    try:
        safe_head_hash = _hash32(
            item.get("safe_head_hash"), "finalized block hash",
        )
        safe_head_hash_valid = True
    except (TypeError, ValueError):
        safe_head_hash = None
        safe_head_hash_valid = False
    try:
        safe_head_at = datetime.fromisoformat(
            str(item.get("safe_head_at")).replace("Z", "+00:00")
        )
        safe_head_time_valid = safe_head_at.tzinfo is not None
    except (TypeError, ValueError, OverflowError):
        safe_head_time_valid = False
        safe_head_at = None
    try:
        verified_at = datetime.fromisoformat(
            str(item.get("verified_at")).replace("Z", "+00:00")
        )
        verified_time_valid = bool(
            verified_at.tzinfo is not None and safe_head_at is not None
            and verified_at.astimezone(timezone.utc)
                >= safe_head_at.astimezone(timezone.utc)
        )
    except (TypeError, ValueError, OverflowError):
        verified_time_valid = False
    audit_duration_valid = bool(
        type(item.get("audit_duration_ms")) is int
        and item["audit_duration_ms"] >= 0
    )
    valid_states = {"verified", "catching_up", "blocked"}
    proof_shape_valid = bool(
        item.get("state") in valid_states
        and started_number is not None and started_number >= 0
        and verified_number is not None
        and verified_number >= started_number - 1
        and head_number is not None
        and head_number >= started_number - 1
        and verified_number <= head_number
    )
    retained_shape_valid = bool(
        proof_shape_valid
        and (verified_number < started_number
             or (retained_epoch_start is not None
                 and retained_epoch_start <= started_number))
    )
    verified_shape_without_retention = bool(
        proof_shape_valid and item.get("state") == "verified"
        and verified_number == head_number
        and verified_number >= started_number
        and verified_hash_valid and safe_head_hash_valid
        and item.get("verified_through_hash") == item.get("safe_head_hash")
        and safe_head_time_valid and verified_time_valid and audit_duration_valid
    )
    verified_shape_valid = bool(
        verified_shape_without_retention and retained_shape_valid
    )
    if item.get("state") not in valid_states:
        item["state"] = "blocked"
        item["last_error_kind"] = "invalid_coverage_proof"
    elif ((item.get("state") == "verified"
           and item["provider_independent"]
           and verified_shape_without_retention
           and not retained_shape_valid)
          or (item.get("state") == "catching_up"
              and proof_shape_valid and not retained_shape_valid)):
        item["state"] = "blocked"
        item["last_error_kind"] = "retention_boundary_mismatch"
    elif item.get("state") == "catching_up" and not proof_shape_valid:
        item["state"] = "blocked"
        item["last_error_kind"] = "invalid_coverage_proof"
    elif item.get("state") == "verified" and (
            not item["provider_independent"] or not verified_shape_valid):
        item["state"] = "blocked"
        item["last_error_kind"] = "invalid_coverage_proof"
    item["coverage_started_block"] = started_number
    item["verified_through_block"] = verified_number
    item["safe_head_block"] = head_number
    item["verified_through_hash"] = verified_hash
    item["safe_head_hash"] = safe_head_hash
    item["safe_head_at"] = (
        safe_head_at.astimezone(timezone.utc).isoformat()
        if safe_head_time_valid and safe_head_at is not None else None
    )
    item["verified_at"] = (
        verified_at.astimezone(timezone.utc).isoformat()
        if verified_time_valid else None
    )
    item["audit_duration_ms"] = (
        item.get("audit_duration_ms") if audit_duration_valid else None
    )
    failures = item.get("consecutive_failures")
    item["consecutive_failures"] = (
        failures if type(failures) is int and failures >= 0 else 0
    )
    for clock_field in ("next_retry_at", "updated_at"):
        value = item.get(clock_field)
        if value is None:
            continue
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            item[clock_field] = (
                parsed.astimezone(timezone.utc).isoformat()
                if parsed.tzinfo is not None else None
            )
        except (TypeError, ValueError, OverflowError):
            item[clock_field] = None
    return item


def _coverage_details(spec: FactorySpec, state: dict) -> dict:
    return {
        "schema_version": 2,
        "state": state.get("state", "blocked"),
        "chain": spec.chain,
        "venue": spec.venue,
        "factory": spec.address,
        "coverage_started_block": state.get("coverage_started_block"),
        "verified_through_block": state.get("verified_through_block"),
        "verified_through_hash": state.get("verified_through_hash"),
        "safe_head_block": state.get("safe_head_block"),
        "safe_head_hash": state.get("safe_head_hash"),
        "safe_head_at": state.get("safe_head_at"),
        "audit_duration_ms": state.get("audit_duration_ms"),
        "lag_blocks": state.get("lag_blocks"),
        "verified_at": state.get("verified_at"),
        "ws_provider_id": state.get("ws_provider_id"),
        "http_provider_id": state.get("http_provider_id"),
        "provider_independent": state.get("provider_independent") is True,
        "consecutive_failures": int(state.get("consecutive_failures") or 0),
        "next_retry_at": state.get("next_retry_at"),
        "last_error_kind": state.get("last_error_kind"),
    }


def _report_coverage(
    spec: FactorySpec, state: dict, *, connection_generation: str | None = None,
) -> bool:
    verified = (
        state.get("state") == "verified"
        and state.get("provider_independent") is True
        and state.get("verified_through_block") is not None
        and state.get("safe_head_block") is not None
        and int(state["verified_through_block"]) == int(state["safe_head_block"])
    )
    error = None if verified else str(
        state.get("last_error_kind") or state.get("state") or "unverified"
    )[:240]
    details = _coverage_details(spec, state)
    if connection_generation is not None:
        details["connection_generation"] = connection_generation
    try:
        if connection_generation is not None:
            return stream_health.report_worker_if_connection_generation(
                spec.chain, coverage_stream(spec),
                expected_generation=connection_generation,
                status="live" if verified else "degraded",
                error=error, details=details,
            )
        stream_health.report_worker(
            spec.chain, coverage_stream(spec),
            status="live" if verified else "degraded",
            error=error, details=details,
        )
        return True
    except Exception as exc:
        logger.warning(
            "evm_coverage_health_write_failed", chain=spec.chain,
            venue=spec.venue, error_kind=type(exc).__name__,
        )
        return False


def _disk_blocked_coverage(
    spec: FactorySpec, previous: dict | None = None, *,
    connection_generation: str | None = None,
) -> dict:
    """Report disk shedding without touching the coverage evidence database."""
    state = dict(previous or {
        "coverage_started_block": None, "verified_through_block": None,
        "safe_head_block": None, "safe_head_hash": None, "safe_head_at": None,
        "audit_duration_ms": None, "verified_at": None,
        "ws_provider_id": None, "http_provider_id": None,
        "lag_blocks": None, "consecutive_failures": 0,
        "next_retry_at": None, "updated_at": None,
    })
    state.update({
        "state": "blocked", "provider_independent": False,
        "last_error_kind": "disk_critical",
    })
    _report_coverage(
        spec, state, connection_generation=connection_generation,
    )
    return state


def _coverage_failure(spec: FactorySpec, *, error_kind: str,
                      ws_provider_id: str | None,
                      http_provider_id: str | None = None,
                      safe_head_block: int | None = None,
                      safe_head_hash: str | None = None,
                      safe_head_at: datetime | None = None,
                      audit_duration_ms: int | None = None,
                      at: datetime | None = None,
                      connection_generation: str | None = None) -> dict:
    now = (at or datetime.now(timezone.utc)).astimezone(timezone.utc)
    previous = coverage_snapshot(spec)
    failures = int(previous.get("consecutive_failures") or 0) + 1
    delay = min(
        COVERAGE_RETRY_MAX_SECONDS,
        COVERAGE_RETRY_BASE_SECONDS * (2 ** min(failures - 1, 10)),
    )
    next_retry = (now + timedelta(seconds=delay)).isoformat()
    c = _conn()
    try:
        c.execute(
            """INSERT INTO coverage_state(
                   chain,venue,factory,topic,coverage_started_block,
                   verified_through_block,verified_through_hash,
                   safe_head_block,safe_head_hash,safe_head_at,
                   audit_duration_ms,verified_at,
                   ws_provider_id,http_provider_id,provider_independent,status,
                   consecutive_failures,next_retry_at,last_error_kind,updated_at
               ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,'blocked',?,?,?,?)
               ON CONFLICT(chain,venue,factory,topic) DO UPDATE SET
                   safe_head_block=COALESCE(coverage_state.safe_head_block,
                                            excluded.safe_head_block),
                   safe_head_hash=COALESCE(coverage_state.safe_head_hash,
                                           excluded.safe_head_hash),
                   safe_head_at=COALESCE(coverage_state.safe_head_at,
                                         excluded.safe_head_at),
                   audit_duration_ms=COALESCE(coverage_state.audit_duration_ms,
                                              excluded.audit_duration_ms),
                   ws_provider_id=excluded.ws_provider_id,
                   http_provider_id=excluded.http_provider_id,
                   provider_independent=0,status='blocked',
                   consecutive_failures=excluded.consecutive_failures,
                   next_retry_at=excluded.next_retry_at,
                   last_error_kind=excluded.last_error_kind,
                   updated_at=excluded.updated_at""",
            (*_coverage_key(spec), previous.get("coverage_started_block"),
             previous.get("verified_through_block"),
             previous.get("verified_through_hash"), safe_head_block,
             safe_head_hash,
             safe_head_at.astimezone(timezone.utc).isoformat()
             if safe_head_at is not None else None,
             audit_duration_ms, previous.get("verified_at"),
             ws_provider_id, http_provider_id,
             failures, next_retry, str(error_kind)[:80], now.isoformat()),
        )
        c.commit()
    finally:
        c.close()
    state = coverage_snapshot(spec)
    _report_coverage(
        spec, state, connection_generation=connection_generation,
    )
    return state


def _freeze_coverage_boundary(
    spec: FactorySpec, *, safe_head: int, lookback: int,
    safe_head_hash: str, safe_head_at: datetime,
    ws_provider_id: str, http_provider_id: str,
    at: datetime,
) -> dict:
    """Persist the first forward boundary before any fallible getLogs work."""
    stream_disk_guard.GUARD.require_evidence_write(spec.chain)
    candidate_start = max(0, int(safe_head) - int(lookback) + 1)
    now = at.astimezone(timezone.utc).isoformat()
    c = _conn()
    try:
        c.execute("BEGIN IMMEDIATE")
        row = c.execute(
            """SELECT coverage_started_block FROM coverage_state
                 WHERE chain=? AND venue=? AND factory=? AND topic=?""",
            _coverage_key(spec),
        ).fetchone()
        if row is None:
            c.execute(
                """INSERT INTO coverage_state(
                       chain,venue,factory,topic,coverage_started_block,
                       verified_through_block,verified_through_hash,
                       safe_head_block,safe_head_hash,safe_head_at,
                       audit_duration_ms,verified_at,
                       ws_provider_id,http_provider_id,provider_independent,status,
                       consecutive_failures,next_retry_at,last_error_kind,updated_at
                   ) VALUES (?,?,?,?,?,?,NULL,?,?,?,NULL,NULL,?,?,1,'catching_up',0,NULL,NULL,?)""",
                (*_coverage_key(spec), candidate_start, candidate_start - 1,
                 safe_head, safe_head_hash,
                 safe_head_at.astimezone(timezone.utc).isoformat(),
                 ws_provider_id, http_provider_id, now),
            )
        elif row[0] is None:
            c.execute(
                """UPDATE coverage_state
                      SET coverage_started_block=?,verified_through_block=?,
                          verified_through_hash=NULL,safe_head_block=?,
                          safe_head_hash=?,safe_head_at=?,
                          audit_duration_ms=NULL,
                          verified_at=NULL,ws_provider_id=?,
                          http_provider_id=?,provider_independent=1,
                          status='catching_up',consecutive_failures=0,
                          next_retry_at=NULL,last_error_kind=NULL,updated_at=?
                    WHERE chain=? AND venue=? AND factory=? AND topic=?
                      AND coverage_started_block IS NULL""",
                (candidate_start, candidate_start - 1, safe_head,
                 safe_head_hash,
                 safe_head_at.astimezone(timezone.utc).isoformat(),
                 ws_provider_id, http_provider_id, now, *_coverage_key(spec)),
            )
        c.commit()
    except Exception:
        c.rollback()
        raise
    finally:
        c.close()
    return coverage_snapshot(spec)


def _validated_coverage_events(logs: object, *, spec: FactorySpec,
                               start: int, end: int) -> list[StreamEvent]:
    if not isinstance(logs, list):
        raise RuntimeError("eth_getLogs returned a non-list result")
    events: list[StreamEvent] = []
    seen: set[tuple[str, int]] = set()
    for item in logs:
        event = parse_message(
            {"jsonrpc": "2.0", "method": "eth_subscription",
             "params": {"result": item}},
            spec=spec,
        )
        if event is None or not isinstance(event.payload, dict):
            raise RuntimeError("eth_getLogs returned an event outside the exact filter")
        if event.payload.get("removed") is not False:
            raise RuntimeError("finalized eth_getLogs returned a removed event")
        block = int(event.payload["block_number"])
        if not start <= block <= end:
            raise RuntimeError("eth_getLogs returned an event outside requested range")
        identity = (
            str(event.payload["transaction_hash"]), int(event.payload["log_index"]),
        )
        if identity in seen:
            raise RuntimeError("eth_getLogs returned a duplicate log identity")
        seen.add(identity)
        events.append(event)
    events.sort(key=lambda event: (
        int(event.payload["block_number"]),
        int(event.payload["transaction_index"]),
        int(event.payload["log_index"]),
        str(event.payload["transaction_hash"]),
    ))
    return events


def _reconcile_persisted_range(
    spec: FactorySpec, *, start: int, end: int,
    events: list[StreamEvent], connection: sqlite3.Connection | None = None,
) -> None:
    """Require finalized HTTP logs to contain every persisted live WS identity.

    HTTP-only events are allowed and will be written below.  A websocket event
    omitted by the independent finalized provider, a payload conflict at the same
    transaction/log identity, or resurrection of an already removed identity is
    fail-closed and cannot stamp the range as covered.
    """
    table = "raw_v3_pools" if spec.event_kind == "pool_v3" else "raw_pools"
    canonical = {
        (str(event.payload["transaction_hash"]).lower(),
         int(event.payload["log_index"])): _hash(event.payload)
        for event in events
    }
    c = connection or _conn()
    try:
        rows = c.execute(
            f"""SELECT transaction_hash,log_index,raw_payload_hash,removed
                   FROM {table}
                  WHERE chain=? AND venue=? AND factory=?
                    AND block_number BETWEEN ? AND ?""",
            (spec.chain, spec.venue, spec.address, int(start), int(end)),
        ).fetchall()
        for identity, canonical_hash in canonical.items():
            existing = _existing_raw_payload(c, {
                "kind": spec.event_kind, "chain": spec.chain,
                "transaction_hash": identity[0], "log_index": identity[1],
            })
            if existing is not None and (
                _hash(existing[0]) != existing[1]
                or existing[0]["removed"]
                or existing[1] != canonical_hash
            ):
                raise CoverageEvidenceConflict(
                    "finalized HTTP identity conflicts with persisted evidence",
                )
        for transaction_hash, log_index, payload_hash, removed in rows:
            identity = (str(transaction_hash).lower(), int(log_index))
            reconstructed = _existing_raw_payload(c, {
                "kind": spec.event_kind, "chain": spec.chain,
                "transaction_hash": identity[0], "log_index": identity[1],
            })
            if reconstructed is None or _hash(reconstructed[0]) != str(payload_hash):
                raise CoverageEvidenceConflict("persisted raw evidence hash is corrupt")
            canonical_hash = canonical.get(identity)
            if int(removed):
                if canonical_hash is not None:
                    raise CoverageEvidenceConflict(
                        "finalized HTTP resurrected removed websocket evidence",
                    )
                continue
            if canonical_hash is None or str(payload_hash) != canonical_hash:
                raise CoverageEvidenceConflict(
                    "finalized HTTP omitted or conflicted with websocket evidence",
                )
    finally:
        if connection is None:
            c.close()


class _ProviderBoundRpc:
    def __init__(self, rpc: JsonRpc, identity: str):
        self.rpc = rpc
        self.identity = identity

    def call(self, method: str, params: list) -> object:
        return self.rpc.call_from_provider(self.identity, method, params)


def _checkpoint_hash(rpc: JsonRpc, identity: str, block_number: int) -> str:
    header = rpc.call_from_provider(
        identity, "eth_getBlockByNumber", [hex(block_number), False],
    )
    if not isinstance(header, dict):
        raise CoverageCheckpointMismatch("coverage checkpoint header is unavailable")
    observed_number = _hex_int(
        header.get("number"), "coverage checkpoint block number",
    )
    if observed_number != block_number:
        raise CoverageCheckpointMismatch("coverage checkpoint number changed")
    return _hash32(header.get("hash"), "coverage checkpoint block hash")


def audit_finalized_coverage(
    spec: FactorySpec, rpc: JsonRpc, *, ws_provider_id: str | None,
    now: datetime | None = None, max_blocks: int = MAX_COVERAGE_BLOCKS,
    initial_lookback_blocks: int = INITIAL_COVERAGE_LOOKBACK_BLOCKS,
    connection_generation: str | None = None,
) -> dict:
    """Prove every finalized block range with an independent HTTP ``getLogs``.

    A websocket head is only transport liveness.  This durable watermark advances
    after every returned factory log has validated and passed the evidence writer.
    Replays after a crash are idempotent; a partial write never claims coverage.
    """
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        raise ValueError("coverage clock must include a timezone")
    current = current.astimezone(timezone.utc)
    if connection_generation is not None:
        connection_generation = str(connection_generation).strip()
        if not connection_generation:
            raise ValueError("connection generation must be non-empty")
    try:
        # This is deliberately the first storage/provider operation.  Under
        # CRITICAL pressure the audit may emit one bounded health heartbeat, but
        # must not even open the coverage DB or spend an RPC request.
        stream_disk_guard.GUARD.require_evidence_write(spec.chain)
    except stream_disk_guard.StreamDiskCritical:
        return _disk_blocked_coverage(
            spec, connection_generation=connection_generation,
        )
    if not ws_provider_id:
        return _coverage_failure(
            spec, error_kind="missing_ws_provider", ws_provider_id=None, at=current,
            connection_generation=connection_generation,
        )
    try:
        normalized_ws = _canonical_provider_identity(ws_provider_id)
    except (TypeError, ValueError):
        normalized_ws = ""
    if not normalized_ws:
        return _coverage_failure(
            spec, error_kind="invalid_ws_provider", ws_provider_id=None, at=current,
            connection_generation=connection_generation,
        )

    previous = coverage_snapshot(spec)
    next_retry_at = previous.get("next_retry_at")
    if next_retry_at:
        try:
            retry = datetime.fromisoformat(str(next_retry_at).replace("Z", "+00:00"))
            if (retry.tzinfo is not None
                    and retry.astimezone(timezone.utc) > current):
                _report_coverage(
                    spec, previous,
                    connection_generation=connection_generation,
                )
                return previous
        except (TypeError, ValueError, OverflowError):
            # A corrupt persisted clock must not enable a proof.  Retry the audit
            # and overwrite it only after fresh provider evidence.
            pass

    requested = int(max_blocks)
    lookback = int(initial_lookback_blocks)
    if requested <= 0 or requested > MAX_COVERAGE_BLOCKS:
        raise ValueError("coverage block budget is out of bounds")
    if lookback <= 0:
        raise ValueError("initial coverage lookback must be positive")

    audit_started = _audit_monotonic()
    try:
        header, raw_http_provider = rpc.call_with_provider(
            "eth_getBlockByNumber", ["finalized", False],
            exclude_provider_ids={normalized_ws},
        )
        http_provider = _canonical_provider_identity(raw_http_provider)
        if http_provider == normalized_ws:
            raise ProviderIndependenceError("EVM HTTP and WS providers are identical")
        if not isinstance(header, dict):
            raise RuntimeError("finalized EVM head is unavailable")
        safe_head = _hex_int(header.get("number"), "finalized block number")
        safe_head_hash = _hash32(header.get("hash"), "finalized block hash")
        safe_head_timestamp = _hex_int(
            header.get("timestamp"), "finalized block timestamp",
        )
        safe_head_at = datetime.fromtimestamp(
            safe_head_timestamp, timezone.utc,
        )
        head_age_seconds = (current - safe_head_at).total_seconds()
        max_head_age = FINALIZED_HEAD_MAX_AGE_SECONDS.get(spec.chain)
        if max_head_age is None:
            raise RuntimeError("finalized-head age bound is unavailable")
        if head_age_seconds < 0:
            raise FinalizedHeadFuture("finalized EVM head timestamp is in the future")
        if head_age_seconds > max_head_age:
            raise FinalizedHeadStale("finalized EVM head timestamp is stale")
        raw_chain_id = rpc.call_from_provider(http_provider, "eth_chainId", [])
        chain_id = _hex_int(raw_chain_id, "chain id")
        if chain_id != CHAIN_IDS.get(spec.chain):
            raise RuntimeError("EVM HTTP provider returned the wrong chain")

        previous = coverage_snapshot(spec)
        if previous.get("coverage_started_block") is None:
            previous = _freeze_coverage_boundary(
                spec, safe_head=safe_head, lookback=lookback,
                safe_head_hash=safe_head_hash, safe_head_at=safe_head_at,
                ws_provider_id=normalized_ws,
                http_provider_id=http_provider, at=current,
            )
        started = previous.get("coverage_started_block")
        verified = previous.get("verified_through_block")
        previous_updated_at = previous.get("updated_at")
        if started is None or verified is None:
            raise RuntimeError("EVM coverage boundary is unavailable")
        started = int(started)
        verified = int(verified)
        if safe_head < verified:
            raise RuntimeError("finalized EVM head regressed behind coverage watermark")
        prior_safe = previous.get("safe_head_block")
        if prior_safe is not None and started >= 0:
            prior_safe = int(prior_safe)
            if safe_head < prior_safe:
                raise CoverageCheckpointMismatch(
                    "finalized EVM head regressed behind the prior safe head",
                )
            prior_safe_hash = _hash32(
                previous.get("safe_head_hash"), "persisted safe-head hash",
            )
            observed_prior_safe = (
                safe_head_hash if safe_head == prior_safe
                else _checkpoint_hash(rpc, http_provider, prior_safe)
            )
            if observed_prior_safe != prior_safe_hash:
                raise CoverageCheckpointMismatch(
                    "selected provider changed the persisted safe head",
                )
        if verified >= started:
            prior_hash = _hash32(
                previous.get("verified_through_hash"),
                "persisted coverage checkpoint hash",
            )
            observed_checkpoint = (
                safe_head_hash if safe_head == verified
                else _checkpoint_hash(rpc, http_provider, verified)
            )
            if observed_checkpoint != prior_hash:
                raise CoverageCheckpointMismatch(
                    "selected provider changed the persisted coverage checkpoint",
                )

        if safe_head == verified:
            start = end = None
            events: list[StreamEvent] = []
            digest = hashlib.sha256(b"[]").hexdigest()
        else:
            start = verified + 1
            epoch_start = (start // COVERAGE_EPOCH_BLOCKS) * COVERAGE_EPOCH_BLOCKS
            epoch_end = epoch_start + COVERAGE_EPOCH_BLOCKS - 1
            end = min(safe_head, start + requested - 1, epoch_end)
            logs = rpc.call_from_provider(http_provider, "eth_getLogs", [{
                "address": spec.address, "topics": [spec.topic],
                "fromBlock": hex(start), "toBlock": hex(end),
            }])
            events = _validated_coverage_events(
                logs, spec=spec, start=start, end=end,
            )
            # The provider call may have outlived a disk-state transition.  Do
            # not even open the reconciliation DB after CRITICAL was declared.
            stream_disk_guard.GUARD.require_evidence_write(spec.chain)
            _reconcile_persisted_range(
                spec, start=start, end=end, events=events,
            )
            digest = _hash([event.payload for event in events])
            bound_rpc = _ProviderBoundRpc(rpc, http_provider)
            for event in events:
                evidence_state = _persist_stream_event(
                    event.payload, spec=spec, rpc=bound_rpc,
                )
                if (evidence_state != "complete"
                        or persisted_evidence_state(event.payload) != "complete"):
                    raise RuntimeError("finalized EVM log evidence is incomplete")

        next_verified = verified if end is None else end
        next_verified_hash = (
            safe_head_hash if next_verified == safe_head
            else _checkpoint_hash(rpc, http_provider, next_verified)
        )

        audit_duration_ms = max(
            0, round((_audit_monotonic() - audit_started) * 1_000),
        )
        if head_age_seconds + audit_duration_ms / 1_000 > max_head_age:
            raise FinalizedHeadStale(
                "finalized EVM head expired during coverage audit",
            )
        if audit_duration_ms > MAX_COVERAGE_AUDIT_DURATION_MS:
            raise CoverageAuditTooSlow("finalized coverage audit exceeded its budget")
        checked_at = (
            current + timedelta(milliseconds=audit_duration_ms)
        ).isoformat()
        state_name = "verified" if next_verified == safe_head else "catching_up"
        # A long zero-log request must not bypass disk shedding merely because
        # no per-event evidence writer ran.  Recheck immediately before opening
        # the final proof transaction.
        stream_disk_guard.GUARD.require_evidence_write(spec.chain)
        c = _conn()
        try:
            c.execute("BEGIN IMMEDIATE")
            if start is not None and end is not None:
                _reconcile_persisted_range(
                    spec, start=start, end=end, events=events, connection=c,
                )
            latest = c.execute(
                """SELECT coverage_started_block,verified_through_block,
                          verified_through_hash,updated_at
                     FROM coverage_state
                    WHERE chain=? AND venue=? AND factory=? AND topic=?""",
                _coverage_key(spec),
            ).fetchone()
            if latest is None:
                raise RuntimeError("EVM coverage state disappeared concurrently")
            (latest_started, latest_verified, latest_verified_hash,
             latest_updated_at) = latest
            if (latest_started is None or int(latest_started) != int(started)
                    or latest_verified is None or int(latest_verified) != verified
                    or latest_verified_hash != previous.get("verified_through_hash")
                    or latest_updated_at != previous_updated_at):
                raise RuntimeError("EVM coverage proof changed concurrently")
            if start is not None and end is not None:
                existing_epoch = c.execute(
                    """SELECT from_block,to_block,log_count,evidence_digest,
                              segment_count
                         FROM coverage_epochs
                        WHERE chain=? AND venue=? AND factory=? AND topic=?
                          AND epoch_start_block=?""",
                    (*_coverage_key(spec), epoch_start),
                ).fetchone()
                epoch_status = (
                    "sealed" if end >= epoch_start + COVERAGE_EPOCH_BLOCKS - 1
                    else "open"
                )
                if existing_epoch is None:
                    c.execute(
                        """INSERT INTO coverage_epochs(
                               chain,venue,factory,topic,epoch_start_block,
                               from_block,to_block,checked_at,ws_provider_id,
                               http_provider_id,provider_independent,log_count,
                               evidence_digest,segment_count,status
                           ) VALUES (?,?,?,?,?,?,?,?,?,?,1,?,?,1,?)""",
                        (*_coverage_key(spec), epoch_start, start, end, checked_at,
                         normalized_ws, http_provider, len(events), digest,
                         epoch_status),
                    )
                else:
                    (epoch_from, epoch_through, epoch_logs, prior_digest,
                     segment_count) = existing_epoch
                    if int(epoch_through) != start - 1:
                        raise RuntimeError("EVM coverage epoch is not contiguous")
                    chained_digest = _hash({
                        "previous": prior_digest,
                        "from_block": start,
                        "to_block": end,
                        "segment_digest": digest,
                    })
                    c.execute(
                        """UPDATE coverage_epochs SET to_block=?,checked_at=?,
                                  ws_provider_id=?,http_provider_id=?,
                                  provider_independent=1,log_count=?,
                                  evidence_digest=?,segment_count=?,status=?
                             WHERE chain=? AND venue=? AND factory=? AND topic=?
                               AND epoch_start_block=? AND from_block=?""",
                        (end, checked_at, normalized_ws, http_provider,
                         int(epoch_logs) + len(events), chained_digest,
                         int(segment_count) + 1, epoch_status,
                         *_coverage_key(spec), epoch_start, int(epoch_from)),
                    )
            # Pruning and the public start boundary are one proof mutation.  This
            # also repairs databases written by the older implementation on an
            # audit that has no new blocks to append.
            c.execute(
                """DELETE FROM coverage_epochs WHERE id IN (
                       SELECT id FROM coverage_epochs
                        WHERE chain=? AND venue=? AND factory=? AND topic=?
                        ORDER BY epoch_start_block DESC
                        LIMIT -1 OFFSET ?
                   )""",
                (*_coverage_key(spec), COVERAGE_EPOCH_RETENTION),
            )
            retained = c.execute(
                """SELECT MIN(from_block) FROM coverage_epochs
                    WHERE chain=? AND venue=? AND factory=? AND topic=?""",
                _coverage_key(spec),
            ).fetchone()[0]
            if retained is None:
                if next_verified >= started:
                    raise RuntimeError(
                        "EVM verified coverage has no retained epoch evidence"
                    )
                effective_started = started
            else:
                retained = int(retained)
                if retained > next_verified:
                    raise RuntimeError(
                        "EVM retained epoch begins after the coverage watermark"
                    )
                effective_started = max(started, retained)
            c.execute(
                """INSERT INTO coverage_state(
                       chain,venue,factory,topic,coverage_started_block,
                       verified_through_block,verified_through_hash,
                       safe_head_block,safe_head_hash,safe_head_at,
                       audit_duration_ms,verified_at,
                       ws_provider_id,http_provider_id,provider_independent,status,
                       consecutive_failures,next_retry_at,last_error_kind,updated_at
                   ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,?,0,NULL,NULL,?)
                   ON CONFLICT(chain,venue,factory,topic) DO UPDATE SET
                       coverage_started_block=excluded.coverage_started_block,
                       verified_through_block=excluded.verified_through_block,
                       verified_through_hash=excluded.verified_through_hash,
                       safe_head_block=excluded.safe_head_block,
                       safe_head_hash=excluded.safe_head_hash,
                       safe_head_at=excluded.safe_head_at,
                       audit_duration_ms=excluded.audit_duration_ms,
                       verified_at=excluded.verified_at,
                       ws_provider_id=excluded.ws_provider_id,
                       http_provider_id=excluded.http_provider_id,
                       provider_independent=1,status=excluded.status,
                       consecutive_failures=0,next_retry_at=NULL,
                       last_error_kind=NULL,updated_at=excluded.updated_at""",
                (*_coverage_key(spec), int(effective_started), next_verified,
                 next_verified_hash, safe_head,
                 safe_head_hash, safe_head_at.isoformat(), audit_duration_ms, checked_at,
                 normalized_ws, http_provider, state_name, checked_at),
            )
            c.commit()
        except Exception:
            c.rollback()
            raise
        finally:
            c.close()
    except stream_disk_guard.StreamDiskCritical:
        return _disk_blocked_coverage(
            spec, locals().get("previous"),
            connection_generation=connection_generation,
        )
    except Exception as exc:
        return _coverage_failure(
            spec, error_kind=type(exc).__name__, ws_provider_id=normalized_ws,
            http_provider_id=locals().get("http_provider"),
            safe_head_block=locals().get("safe_head"), at=current,
            safe_head_hash=locals().get("safe_head_hash"),
            safe_head_at=locals().get("safe_head_at"),
            audit_duration_ms=max(
                0, round((_audit_monotonic() - locals().get(
                    "audit_started", _audit_monotonic()
                )) * 1_000),
            ),
            connection_generation=connection_generation,
        )

    state = coverage_snapshot(spec)
    _report_coverage(
        spec, state, connection_generation=connection_generation,
    )
    return state


def backfill_blocks(start: int, end: int, *, spec: FactorySpec, rpc: JsonRpc) -> bool:
    """Compatibility wrapper for immediate recovery; maintenance keeps the error."""
    try:
        _backfill_blocks(start, end, spec=spec, rpc=rpc)
        return True
    except Exception as exc:
        logger.warning(
            "evm_factory_backfill_failed", chain=spec.chain,
            start=start, end=end, error_kind=_bounded_error_kind(exc),
        )
        return False


def retry_open_gaps(spec: FactorySpec, rpc: JsonRpc, *, limit: int = 10) -> dict:
    gaps = stream_health.open_gaps(spec.chain, spec.stream, limit=limit)
    advanced = recovered = failed = 0
    for gap in gaps:
        start = int(gap["from_cursor"])
        end = min(int(gap["to_cursor"]), start + MAX_BACKFILL_BLOCKS - 1)
        try:
            _backfill_blocks(start, end, spec=spec, rpc=rpc)
            state = stream_health.advance_gap(gap["id"], end, details={
                "backfilled": True, "retry": True,
                "from": start, "to": end,
            })
            if state == "resolved":
                recovered += 1
            elif state == "advanced":
                advanced += 1
        except Exception as exc:
            failed += 1
            error_kind = _bounded_error_kind(exc)
            deferred = stream_health.defer_gap(
                gap["id"], f"EVM gap retry failed; error_kind={error_kind}",
            )
            logger.warning(
                "evm_factory_gap_retry_deferred",
                chain=spec.chain, stream=spec.stream,
                start=start, end=end, error_kind=error_kind,
                next_retry_at=(deferred or {}).get("next_retry_at"),
            )
    return {"attempted": len(gaps), "advanced": advanced,
            "recovered": recovered, "failed": failed}


class _EvmSocket:
    def __init__(
        self, socket, *, spec: FactorySpec, identity: str, generation: str,
    ):
        self.socket = socket
        self.spec = spec
        self.identity = identity
        self.generation = generation
        self._provider_cleared = False

    def _clear_provider(self):
        if not self._provider_cleared:
            self._provider_cleared = True
            _clear_active_ws_provider(
                self.spec, self.identity, self.generation,
            )

    @staticmethod
    def _transport_error(operation: str, exc: Exception) -> ConnectionError:
        error_kind = type(exc).__name__
        if re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,79}", error_kind) is None:
            error_kind = "TransportError"
        return ConnectionError(
            f"EVM websocket {operation} failed; error_kind={error_kind}"
        )

    def recv(self):
        try:
            return self.socket.recv()
        except (TimeoutError, socket.timeout, WebSocketTimeoutException):
            # StreamRunner consumes receive timeouts as heartbeat ticks; they are
            # never persisted or logged and therefore must retain their type.
            raise
        except Exception as exc:
            raise self._transport_error("receive", exc) from None

    def ping(self):
        try:
            self.socket.ping()
        except Exception as exc:
            raise self._transport_error("heartbeat", exc) from None

    def send_json(self, payload: dict):
        try:
            self.socket.send(json.dumps(payload, separators=(",", ":")))
        except Exception as exc:
            raise self._transport_error("send", exc) from None

    def close(self):
        try:
            try:
                self.socket.close()
            except Exception as exc:
                raise self._transport_error("close", exc) from None
        finally:
            self._clear_provider()

    def shutdown(self):
        try:
            shutdown = getattr(self.socket, "shutdown", None)
            if callable(shutdown):
                try:
                    shutdown()
                except Exception as exc:
                    raise self._transport_error("shutdown", exc) from None
        finally:
            self._clear_provider()


def build_runner(*, spec: FactorySpec | None = None, rpc: JsonRpc | None = None,
                 socket_factory: Callable[[str], object] | None = None) -> StreamRunner:
    spec = spec or bsc_pancake_v2_spec()
    rpc = rpc or JsonRpc(spec.rpc_urls)
    if socket_factory is None:
        from websocket import create_connection

        socket_factory = lambda endpoint: create_connection(
            endpoint, timeout=10, redirect_limit=0,
        )

    parser = EvmSubscriptionParser(spec)

    def dispose_raw_socket(raw_socket: object) -> None:
        for action in ("close", "shutdown"):
            method = getattr(raw_socket, action, None)
            if callable(method):
                try:
                    method()
                except Exception:
                    pass

    def connect():
        _set_active_ws_provider(spec, None)
        try:
            _invalidate_connect_health(spec)
        except Exception as exc:
            raise ConnectionError(
                "websocket health gate failed; "
                f"error_kind={type(exc).__name__}"
            ) from None
        last_error: object = "no endpoint attempted"
        for endpoint in spec.ws_urls:
            try:
                identity = provider_id(endpoint)
                raw_socket = socket_factory(endpoint)
            except Exception as exc:
                last_error = exc
                continue
            handshake = getattr(raw_socket, "handshake_response", None)
            handshake_status = getattr(handshake, "status", None)
            if (handshake_status in {301, 302, 303, 307, 308}
                    or (handshake is not None and handshake_status != 101)):
                dispose_raw_socket(raw_socket)
                last_error = ConnectionError("websocket handshake was not upgraded")
                continue
            generation = uuid.uuid4().hex
            try:
                # This is the one coverage-health write for the new connection.
                # It must commit before the active provider can be published or
                # the parser can acknowledge a subscription.
                _mark_coverage_reaudit_required(
                    spec, identity=identity, generation=generation,
                )
            except Exception as exc:
                dispose_raw_socket(raw_socket)
                raise ConnectionError(
                    "websocket coverage gate failed; "
                    f"error_kind={type(exc).__name__}"
                ) from None
            socket = _EvmSocket(
                raw_socket, spec=spec, identity=identity,
                generation=generation,
            )
            _set_active_ws_provider(spec, identity, generation)
            return socket
        raise ConnectionError(
            f"all {spec.chain} factory websockets failed; "
            f"error_kind={type(last_error).__name__}"
        )

    def subscribe(ws):
        parser.reset()
        for request in subscribe_requests(spec):
            ws.send_json(request)

    return StreamRunner(
        source=spec.chain, stream=spec.stream, connect=connect, subscribe=subscribe,
        parse=parser,
        on_event=lambda payload: _persist_stream_event(payload, spec=spec, rpc=rpc),
        heartbeat_seconds=30, health_interval_seconds=1, expect_contiguous=True,
        backfill=lambda start, end: backfill_blocks(start, end, spec=spec, rpc=rpc),
    )


def _maintenance(stop: threading.Event,
                 bindings: tuple[tuple[FactorySpec, JsonRpc], ...]) -> None:
    while not stop.wait(60):
        for spec, rpc in bindings:
            try:
                disk = stream_disk_guard.GUARD.snapshot()
                if disk.get("state") == "critical":
                    connection = active_ws_connection(spec)
                    generation = connection[1] if connection is not None else None
                    _disk_blocked_coverage(
                        spec, connection_generation=generation,
                    )
                    continue
                # ``unknown`` remains fail-open: the guard owns measurement
                # validation, while source liveness stays visible here.
                result = retry_open_gaps(spec, rpc)
                if result["attempted"]:
                    logger.info("evm_factory_gap_retry", chain=spec.chain, **result)
                connection = active_ws_connection(spec)
                if connection is None:
                    continue
                ws_identity, generation = connection
                coverage = audit_finalized_coverage(
                    spec, rpc, ws_provider_id=ws_identity,
                    connection_generation=generation,
                )
                if coverage.get("state") != "verified":
                    logger.warning(
                        "evm_factory_coverage_unverified", chain=spec.chain,
                        venue=spec.venue, state=coverage.get("state"),
                        error_kind=coverage.get("last_error_kind"),
                        lag_blocks=coverage.get("lag_blocks"),
                    )
            except Exception as exc:
                connection = active_ws_connection(spec)
                generation = connection[1] if connection is not None else None
                _report_coverage(
                    spec, {
                        "state": "blocked", "provider_independent": False,
                        "last_error_kind": "maintenance_failed",
                    },
                    connection_generation=generation,
                )
                logger.warning(
                    "evm_factory_maintenance_binding_failed",
                    chain=spec.chain, venue=spec.venue,
                    error_kind=_bounded_error_kind(exc),
                )


def main() -> None:
    from dotenv import load_dotenv
    from src.config import PROJECT_ROOT

    load_dotenv(PROJECT_ROOT / ".env")
    _conn().close()
    bindings = tuple((spec, JsonRpc(spec.rpc_urls)) for spec in configured_specs())
    for spec, rpc in bindings:
        initial = retry_open_gaps(spec, rpc)
        if initial["attempted"]:
            logger.info("evm_factory_initial_gap_retry", chain=spec.chain, **initial)
    stop = threading.Event()
    worker = threading.Thread(target=_maintenance, args=(stop, bindings), daemon=True)
    worker.start()
    runners = [build_runner(spec=spec, rpc=rpc) for spec, rpc in bindings]
    children = [threading.Thread(target=runner.run_forever, args=(stop,), daemon=True,
                                 name=f"factory-{runner.source}")
                for runner in runners[1:]]
    for child in children:
        child.start()
    try:
        runners[0].run_forever(stop)
    finally:
        stop.set()
        worker.join(timeout=2)
        for child in children:
            child.join(timeout=2)


if __name__ == "__main__":
    main()
