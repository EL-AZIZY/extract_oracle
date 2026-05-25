"""
audit_store.py
--------------
Audit local des executions dans SQLite (logs/audit.db).
"""

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4


@dataclass
class RunContext:
    run_id: str
    job_name: str
    mode: str
    started_at: str


def init_audit_db(log_dir: Path) -> Path:
    db_path = log_dir / "audit.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS stg_logs (
                run_id TEXT PRIMARY KEY,
                job_name TEXT NOT NULL,
                mode TEXT NOT NULL,
                statut TEXT NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                rows_count INTEGER NOT NULL DEFAULT 0,
                error_message TEXT
            )
            """
        )
    return db_path


def create_run(job_name: str, mode: str) -> RunContext:
    return RunContext(
        run_id=str(uuid4()),
        job_name=job_name,
        mode=mode,
        started_at=datetime.now(tz=timezone.utc).isoformat(),
    )


def log_run_start(db_path: Path, ctx: RunContext) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO stg_logs(run_id, job_name, mode, statut, started_at)
            VALUES (?, ?, ?, 'EN_COURS', ?)
            """,
            (ctx.run_id, ctx.job_name, ctx.mode, ctx.started_at),
        )


def log_run_end(db_path: Path, ctx: RunContext, ok: bool, rows: int, error: str = "") -> None:
    statut = "OK" if ok else "ERREUR"
    finished_at = datetime.now(tz=timezone.utc).isoformat()
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            UPDATE stg_logs
            SET statut = ?, finished_at = ?, rows_count = ?, error_message = ?
            WHERE run_id = ?
            """,
            (statut, finished_at, rows, error or None, ctx.run_id),
        )
