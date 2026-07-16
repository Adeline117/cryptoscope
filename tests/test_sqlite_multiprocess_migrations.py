"""Exercise startup migrations across independent Python processes."""

from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
import time
from pathlib import Path


_WORKER = r'''
import importlib
import json
import sys
import time
from pathlib import Path

module_name, database, case, worker_id, ready_file, start_file = sys.argv[1:]
module = importlib.import_module(module_name)
module.DB = Path(database)

ready = Path(ready_file)
start = Path(start_file)
ready.touch()
deadline = time.monotonic() + 15
while not start.exists():
    if time.monotonic() >= deadline:
        raise TimeoutError("startup barrier was never released")
    time.sleep(0.01)

connection = module._conn()
try:
    if case == "stream_health":
        connection.execute(
            """INSERT INTO streams(
                source,stream,cursor,last_event_at,last_received_at,latency_ms,
                status,last_error,updated_at
            ) VALUES (?,?,?,?,?,?,?,?,?)""",
            (
                "multiprocess", f"worker-{worker_id}", int(worker_id),
                "2026-07-15T00:00:00+00:00", "2026-07-15T00:00:00+00:00",
                0, "live", None, "2026-07-15T00:00:00+00:00",
            ),
        )
        table = "gaps"
        written = connection.execute(
            "SELECT COUNT(*) FROM streams WHERE source='multiprocess'"
        ).fetchone()[0]
    elif case == "solana_launch":
        connection.execute(
            """INSERT INTO raw_launches(
                signature,slot,transaction_index,program,event_type,creator,mint,
                detected_at,hydrated_at,raw_payload_hash,hydration_payload_hash,logs,
                evidence_state,hydration_error,qualification_state
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                f"signature-{worker_id}", int(worker_id), 0, "pump_fun", "create",
                f"creator-{worker_id}", f"mint-{worker_id}",
                "2026-07-15T00:00:00+00:00", None, "r" * 64, None, "[]",
                "raw_only", None, "raw_unqualified",
            ),
        )
        table = "raw_launches"
        written = connection.execute(
            "SELECT COUNT(*) FROM raw_launches WHERE signature LIKE 'signature-%'"
        ).fetchone()[0]
    else:
        raise ValueError(f"unsupported case: {case}")
    connection.commit()
    columns = [row[1] for row in connection.execute(f"PRAGMA table_info({table})")]
    journal = connection.execute("PRAGMA journal_mode").fetchone()[0]
    print(json.dumps({"columns": columns, "journal": journal, "written": written}))
finally:
    connection.close()
'''


def _legacy_stream_health(path: Path) -> None:
    with sqlite3.connect(path) as connection:
        connection.execute("""CREATE TABLE gaps(
            id INTEGER PRIMARY KEY AUTOINCREMENT, source TEXT NOT NULL,
            stream TEXT NOT NULL, from_cursor INTEGER NOT NULL,
            to_cursor INTEGER NOT NULL, detected_at TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'open', resolved_at TEXT, details TEXT,
            UNIQUE(source,stream,from_cursor,to_cursor))""")


def _legacy_solana_launch(path: Path) -> None:
    with sqlite3.connect(path) as connection:
        connection.execute("""CREATE TABLE raw_launches(
            signature TEXT PRIMARY KEY, slot INTEGER NOT NULL,
            transaction_index INTEGER, program TEXT NOT NULL,
            event_type TEXT NOT NULL, creator TEXT, mint TEXT,
            detected_at TEXT NOT NULL, hydrated_at TEXT,
            raw_payload_hash TEXT NOT NULL, hydration_payload_hash TEXT,
            logs TEXT NOT NULL, evidence_state TEXT NOT NULL DEFAULT 'raw_only',
            hydration_error TEXT,
            qualification_state TEXT NOT NULL DEFAULT 'raw_unqualified')""")


def _launch_workers(
    tmp_path: Path,
    *,
    module: str,
    database: Path,
    case: str,
    worker_ids: range,
) -> list[dict]:
    barrier = tmp_path / f"{case}-{worker_ids.start}.start"
    processes: list[tuple[subprocess.Popen[str], Path]] = []
    try:
        for worker_id in worker_ids:
            ready = tmp_path / f"{case}-{worker_id}.ready"
            process = subprocess.Popen(
                [
                    sys.executable, "-c", _WORKER, module, str(database), case,
                    str(worker_id), str(ready), str(barrier),
                ],
                cwd=Path(__file__).resolve().parents[1],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            processes.append((process, ready))

        deadline = time.monotonic() + 15
        while not all(ready.exists() for _process, ready in processes):
            exited = [
                process for process, _ready in processes
                if process.poll() is not None
            ]
            if exited:
                details = [
                    process.communicate(timeout=1) for process in exited
                ]
                raise AssertionError(f"worker exited before barrier: {details}")
            if time.monotonic() >= deadline:
                raise AssertionError("workers did not reach the startup barrier")
            time.sleep(0.01)

        barrier.touch()
        results = []
        for process, _ready in processes:
            stdout, stderr = process.communicate(timeout=30)
            assert process.returncode == 0, stderr
            results.append(json.loads(stdout))
        return results
    finally:
        for process, _ready in processes:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)


def test_legacy_sqlite_migrations_survive_concurrent_process_startup_and_restart(
    tmp_path: Path,
) -> None:
    cases = (
        (
            "src.pipeline.stream_health", "stream_health", _legacy_stream_health,
            ("retry_count", "next_retry_at", "last_error"), "streams",
        ),
        (
            "src.pipeline.solana_launch_stream", "solana_launch", _legacy_solana_launch,
            (
                "qualification_attempted_at", "qualification_error", "qualified_at",
                "ledger_event_id", "hydration_retry_count", "hydration_next_retry_at",
                "hydration_attempted_at", "hydration_last_rpc_error",
            ),
            "raw_launches",
        ),
    )

    for module, case, create_legacy, migrated_columns, written_table in cases:
        database = tmp_path / f"{case}.db"
        create_legacy(database)

        concurrent = _launch_workers(
            tmp_path, module=module, database=database, case=case,
            worker_ids=range(6),
        )
        assert all(tuple(result["columns"][-len(migrated_columns):]) == migrated_columns
                   for result in concurrent)
        assert all(str(result["journal"]).lower() == "wal" for result in concurrent)

        sequential = _launch_workers(
            tmp_path, module=module, database=database, case=case,
            worker_ids=range(6, 7),
        )
        assert tuple(sequential[0]["columns"][-len(migrated_columns):]) == migrated_columns

        with sqlite3.connect(database) as connection:
            assert connection.execute(
                f"SELECT COUNT(*) FROM {written_table}"
            ).fetchone()[0] == 7
            assert connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
