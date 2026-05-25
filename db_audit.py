"""
db_audit.py
-----------
Audit en base PostgreSQL dans le schema `log`.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable
from uuid import uuid4

try:
    from psycopg2 import sql as pg_sql
except ImportError:  # pragma: no cover
    pg_sql = None

log = logging.getLogger(__name__)

LOG_SCHEMA = "log"
_VALID_IDENT = re.compile(r"[^a-zA-Z0-9_]")


@dataclass
class RunContext:
    run_id: str
    job_name: str
    mode: str
    started_at: str
    job_table: str


def create_run(job_name: str, mode: str) -> RunContext:
    return RunContext(
        run_id=str(uuid4()),
        job_name=job_name,
        mode=mode,
        started_at=datetime.now(tz=timezone.utc).isoformat(),
        job_table=_normalize_job_table_name(job_name),
    )


def init_db_audit(pool, conn_cfg, jobs: Iterable[str]) -> bool:
    db_type = conn_cfg.get("database", "type", fallback="").lower()
    if db_type != "postgresql":
        log.warning("Audit SQL schema 'log' ignore: type de base '%s' non postgresql.", db_type)
        return False
    if pg_sql is None:
        log.error("psycopg2 non disponible: audit SQL desactive.")
        return False

    with pool.acquire() as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS log")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS log.stg_logs (
                    run_id TEXT PRIMARY KEY,
                    job_name TEXT NOT NULL,
                    mode TEXT NOT NULL,
                    statut TEXT NOT NULL,
                    started_at TIMESTAMPTZ NOT NULL,
                    finished_at TIMESTAMPTZ,
                    rows_count BIGINT NOT NULL DEFAULT 0,
                    error_message TEXT
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS log.logs_csv (
                    id BIGSERIAL PRIMARY KEY,
                    run_id TEXT NOT NULL,
                    job_name TEXT NOT NULL,
                    event_level TEXT NOT NULL,
                    event_message TEXT NOT NULL,
                    event_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS log.rejets_ingestion (
                    id BIGSERIAL PRIMARY KEY,
                    run_id TEXT NOT NULL,
                    job_name TEXT NOT NULL,
                    reject_reason TEXT NOT NULL,
                    raw_payload TEXT,
                    event_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            for job in jobs:
                job_table = _normalize_job_table_name(job)
                cur.execute(
                    pg_sql.SQL(
                        """
                        CREATE TABLE IF NOT EXISTS {}.{} (
                            id BIGSERIAL PRIMARY KEY,
                            run_id TEXT NOT NULL,
                            job_name TEXT NOT NULL,
                            mode TEXT NOT NULL,
                            statut TEXT NOT NULL,
                            rows_count BIGINT NOT NULL DEFAULT 0,
                            error_message TEXT,
                            event_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
                        )
                        """
                    ).format(
                        pg_sql.Identifier(LOG_SCHEMA),
                        pg_sql.Identifier(job_table),
                    )
                )
        conn.commit()
    log.info("Audit SQL initialise dans le schema '%s'.", LOG_SCHEMA)
    return True


def log_run_start(pool, ctx: RunContext) -> None:
    with pool.acquire() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO log.stg_logs(run_id, job_name, mode, statut, started_at)
                VALUES (%s, %s, %s, 'EN_COURS', %s::timestamptz)
                """,
                (ctx.run_id, ctx.job_name, ctx.mode, ctx.started_at),
            )
            cur.execute(
                """
                INSERT INTO log.logs_csv(run_id, job_name, event_level, event_message)
                VALUES (%s, %s, 'INFO', %s)
                """,
                (ctx.run_id, ctx.job_name, "Debut du job"),
            )
            cur.execute(
                pg_sql.SQL(
                    """
                    INSERT INTO {}.{}(run_id, job_name, mode, statut, rows_count, error_message)
                    VALUES (%s, %s, %s, 'EN_COURS', 0, NULL)
                    """
                ).format(pg_sql.Identifier(LOG_SCHEMA), pg_sql.Identifier(ctx.job_table)),
                (ctx.run_id, ctx.job_name, ctx.mode),
            )
        conn.commit()


def log_run_end(pool, ctx: RunContext, ok: bool, rows: int, error: str = "") -> None:
    statut = "OK" if ok else "ERREUR"
    level = "INFO" if ok else "ERROR"
    msg = "Fin du job avec succes" if ok else f"Echec du job: {error}"
    with pool.acquire() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE log.stg_logs
                SET statut=%s, finished_at=NOW(), rows_count=%s, error_message=%s
                WHERE run_id=%s
                """,
                (statut, rows, error or None, ctx.run_id),
            )
            cur.execute(
                """
                INSERT INTO log.logs_csv(run_id, job_name, event_level, event_message)
                VALUES (%s, %s, %s, %s)
                """,
                (ctx.run_id, ctx.job_name, level, msg),
            )
            cur.execute(
                pg_sql.SQL(
                    """
                    INSERT INTO {}.{}(run_id, job_name, mode, statut, rows_count, error_message)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """
                ).format(pg_sql.Identifier(LOG_SCHEMA), pg_sql.Identifier(ctx.job_table)),
                (ctx.run_id, ctx.job_name, ctx.mode, statut, rows, error or None),
            )
        conn.commit()


def _normalize_job_table_name(job_name: str) -> str:
    name = _VALID_IDENT.sub("_", job_name.strip().lower())
    name = name.strip("_")
    if not name:
        name = "job"
    if name[0].isdigit():
        name = f"job_{name}"
    return name[:63]
