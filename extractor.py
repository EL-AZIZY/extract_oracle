"""
extractor.py
------------
Extraction d'une requete vers CSV, Parquet, JSON ou transfert DB-to-DB.
"""

import configparser
import csv
import decimal
import gzip
import json
import logging
import time
from pathlib import Path
from typing import Generator

from settings import get_job_param
from timeout import QueryTimeoutError, get_db_level_timeout_sql, query_timeout

log = logging.getLogger(__name__)

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    _ARROW_AVAILABLE = True
except ImportError:
    _ARROW_AVAILABLE = False


def run(pool,
        query: str,
        job_cfg: configparser.ConfigParser,
        job: str,
        job_log: logging.Logger,
        dry_run: bool = False) -> int:
    fmt = get_job_param(job_cfg, job, "format", "csv").lower()
    chunk_size = int(get_job_param(job_cfg, job, "chunk_size", "10000"))
    progress_step = int(get_job_param(job_cfg, job, "progress_step", str(chunk_size)))
    query_timeout_seconds = int(get_job_param(job_cfg, job, "query_timeout_sec", "0"))

    output_path = _resolve_output_path(job_cfg, job, fmt)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    checkpoint_path = output_path.with_suffix(".checkpoint")
    resume_from = _read_checkpoint(checkpoint_path)
    keyset_column = (get_job_param(job_cfg, job, "keyset_column", "") or "").strip()
    keyset_last_value = get_job_param(job_cfg, job, "keyset_last_value", None)

    if fmt == "csv.gz" and resume_from:
        job_log.warning("Checkpoint detecte pour csv.gz: redemarrage depuis 0.")
        resume_from = 0
        _clear_checkpoint(checkpoint_path)

    job_log.info("-" * 60)
    job_log.info("Format      : %s", fmt.upper())
    job_log.info("Sortie      : %s", output_path)
    job_log.info("Batch       : %d lignes", chunk_size)
    job_log.info("Dry run     : %s", dry_run)
    if resume_from:
        job_log.info("Reprise     : depuis la ligne %d (checkpoint)", resume_from)

    t0 = time.time()
    total_rows = resume_from

    try:
        with pool.acquire() as conn:
            with conn.cursor() as cursor:
                cursor.arraysize = chunk_size
                _apply_db_statement_timeout(cursor, pool, query_timeout_seconds, job_log)
                with query_timeout(query_timeout_seconds, query_preview=query):
                    _execute_with_offset(
                        cursor,
                        query,
                        resume_from,
                        job_log,
                        keyset_column=keyset_column or None,
                        last_value=keyset_last_value,
                    )
                col_names = [d[0] for d in cursor.description]
                job_log.info("Colonnes (%d) : %s", len(col_names), col_names)

                row_stream = _stream_rows(cursor, chunk_size)

                if dry_run:
                    for _ in row_stream:
                        total_rows += 1
                        if progress_step > 0 and total_rows % progress_step == 0:
                            job_log.info("%d lignes extraites", total_rows)
                elif fmt == "csv":
                    total_rows = _write_csv(
                        row_stream, col_names, output_path, job_cfg, job,
                        resume_from, checkpoint_path, total_rows, job_log,
                    )
                elif fmt == "csv.gz":
                    total_rows = _write_csv_gz(
                        row_stream, col_names, output_path,
                        checkpoint_path, total_rows, job_log, progress_step,
                    )
                elif fmt == "parquet":
                    total_rows = _write_parquet(
                        row_stream, col_names, output_path, chunk_size,
                        checkpoint_path, total_rows, job_log, progress_step,
                    )
                elif fmt == "jsonl":
                    total_rows = _write_jsonl(
                        row_stream, col_names, output_path,
                        resume_from, checkpoint_path, total_rows, job_log, progress_step,
                    )
                elif fmt == "db":
                    total_rows = _write_to_db(
                        row_stream, col_names, job_cfg, job,
                        checkpoint_path, total_rows, job_log,
                    )
                else:
                    raise ValueError(
                        f"Format '{fmt}' non supporte. Valeurs acceptees : csv, csv.gz, parquet, jsonl, db"
                    )

        if not dry_run:
            _clear_checkpoint(checkpoint_path)

        elapsed = time.time() - t0
        job_log.info("SUCCESS: %d lignes -> %s (%.1f s)", total_rows, output_path, elapsed)

    except QueryTimeoutError as exc:
        elapsed = time.time() - t0
        job_log.error("TIMEOUT after %.1f s (checkpoint ligne %d): %s", elapsed, total_rows, exc, exc_info=True)
        raise
    except Exception as exc:
        elapsed = time.time() - t0
        job_log.error("ERROR after %.1f s (checkpoint ligne %d): %s", elapsed, total_rows, exc, exc_info=True)
        raise

    job_log.info("-" * 60)
    return total_rows


def _stream_rows(cursor, chunk_size: int) -> Generator:
    while True:
        batch = cursor.fetchmany(chunk_size)
        if not batch:
            break
        for row in batch:
            yield _sanitize_row(row)


def _execute_with_offset(cursor,
                         query: str,
                         offset: int,
                         job_log: logging.Logger,
                         keyset_column: str = None,
                         last_value: str = None) -> None:
    """
    Executes query using keyset pagination when configured, otherwise OFFSET.
    """
    if keyset_column and last_value is not None:
        wrapped = (
            f"SELECT * FROM ({query}) __subq__\n"
            f"WHERE {keyset_column} > :last_val\n"
            f"ORDER BY {keyset_column}"
        )
        job_log.info("Keyset pagination: %s > %s", keyset_column, last_value)
        cursor.execute(wrapped, {"last_val": last_value})
    elif offset > 0:
        job_log.warning(
            "Using OFFSET %d - this is slow for large values. "
            "Consider 'keyset_column' in config.",
            offset,
        )
        wrapped = (
            f"SELECT * FROM ({query}) __subq__\n"
            f"OFFSET {offset} ROWS"
        )
        cursor.execute(wrapped)
    else:
        cursor.execute(query)


def _apply_db_statement_timeout(cursor, pool, timeout_seconds: int, job_log: logging.Logger) -> None:
    if timeout_seconds <= 0:
        return
    db_type = getattr(pool, "db_type", "")
    timeout_sql = get_db_level_timeout_sql(db_type, timeout_seconds)
    if not timeout_sql:
        return
    try:
        cursor.execute(timeout_sql)
        job_log.info("DB statement timeout active: %ss (%s)", timeout_seconds, db_type)
    except Exception as exc:
        job_log.warning("Unable to apply DB timeout (%s): %s", db_type, exc)


def _write_csv(row_stream, col_names, output_path, job_cfg, job,
               resume_from, checkpoint_path, total_rows, job_log) -> int:
    separator = get_job_param(job_cfg, job, "separator", ",")
    encoding = get_job_param(job_cfg, job, "encoding", "utf-8-sig")
    no_header = _is_true(get_job_param(job_cfg, job, "no_header", "false"))
    progress_step = int(get_job_param(job_cfg, job, "progress_step", get_job_param(job_cfg, job, "chunk_size", "10000")))

    mode = "a" if resume_from else "w"
    with open(output_path, mode, newline="", encoding=encoding) as f:
        writer = csv.writer(f, delimiter=separator, quoting=csv.QUOTE_MINIMAL, lineterminator="\n")
        if not no_header and not resume_from:
            writer.writerow(col_names)
        for row in row_stream:
            writer.writerow(row)
            total_rows += 1
            if progress_step > 0 and total_rows % progress_step == 0:
                f.flush()
                _write_checkpoint(checkpoint_path, total_rows)
                job_log.info("%d lignes extraites", total_rows)
    return total_rows


def _write_csv_gz(row_stream, col_names, output_path,
                  checkpoint_path, total_rows, job_log,
                  progress_step: int = 10000) -> int:
    with gzip.open(output_path, "wt", encoding="utf-8", newline="") as gz:
        writer = csv.writer(gz, lineterminator="\n")
        writer.writerow(col_names)
        for row in row_stream:
            writer.writerow(row)
            total_rows += 1
            if progress_step > 0 and total_rows % progress_step == 0:
                _write_checkpoint(checkpoint_path, total_rows)
                job_log.info("%d lignes extraites", total_rows)
    return total_rows


def _write_parquet(row_stream, col_names, output_path,
                   chunk_size, checkpoint_path, total_rows, job_log,
                   progress_step: int = 10000) -> int:
    if not _ARROW_AVAILABLE:
        raise ImportError("pyarrow est requis pour le format Parquet. Installez-le avec : pip install pyarrow")

    writer = None
    buffer = []
    try:
        for row in row_stream:
            buffer.append([_parquet_safe_value(v) for v in row])
            total_rows += 1
            if len(buffer) >= chunk_size:
                table = pa.table({c: [r[i] for r in buffer] for i, c in enumerate(col_names)})
                if writer is None:
                    writer = pq.ParquetWriter(output_path, table.schema, compression="snappy")
                writer.write_table(table)
                buffer.clear()
                _write_checkpoint(checkpoint_path, total_rows)
                if progress_step > 0 and total_rows % progress_step == 0:
                    job_log.info("%d lignes extraites", total_rows)

        if buffer:
            table = pa.table({c: [r[i] for r in buffer] for i, c in enumerate(col_names)})
            if writer is None:
                writer = pq.ParquetWriter(output_path, table.schema, compression="snappy")
            writer.write_table(table)
    finally:
        if writer:
            writer.close()

    return total_rows


def _parquet_safe_value(value):
    if isinstance(value, decimal.Decimal):
        return float(value)
    return value


def _write_jsonl(row_stream, col_names, output_path,
                 resume_from, checkpoint_path, total_rows, job_log,
                 progress_step: int = 10000) -> int:
    mode = "a" if resume_from else "w"
    with open(output_path, mode, encoding="utf-8") as f:
        for row in row_stream:
            record = dict(zip(col_names, row))
            f.write(json.dumps(record, default=str, ensure_ascii=False) + "\n")
            total_rows += 1
            if progress_step > 0 and total_rows % progress_step == 0:
                f.flush()
                _write_checkpoint(checkpoint_path, total_rows)
                job_log.info("%d lignes extraites", total_rows)
    return total_rows


def _write_to_db(row_stream, col_names, job_cfg, job,
                 checkpoint_path, total_rows, job_log) -> int:
    target_dsn = get_job_param(job_cfg, job, "target_dsn")
    target_table = get_job_param(job_cfg, job, "target_table")
    batch_size = int(get_job_param(job_cfg, job, "chunk_size", "5000"))
    progress_step = int(get_job_param(job_cfg, job, "progress_step", str(batch_size)))

    if not target_dsn or not target_table:
        raise ValueError("'target_dsn' et 'target_table' sont requis pour le format 'db'.")

    try:
        import psycopg2
        target_conn = psycopg2.connect(target_dsn)
    except ImportError:
        raise ImportError("psycopg2 requis pour le transfert DB-to-DB. pip install psycopg2-binary")

    placeholders = ", ".join(["%s"] * len(col_names))
    cols_str = ", ".join(col_names)
    insert_sql = f"INSERT INTO {target_table} ({cols_str}) VALUES ({placeholders})"

    buffer = []
    with target_conn:
        with target_conn.cursor() as cur:
            for row in row_stream:
                buffer.append(row)
                total_rows += 1
                if len(buffer) >= batch_size:
                    cur.executemany(insert_sql, buffer)
                    target_conn.commit()
                    buffer.clear()
                    _write_checkpoint(checkpoint_path, total_rows)
                    if progress_step > 0 and total_rows % progress_step == 0:
                        job_log.info("%d lignes extraites", total_rows)
            if buffer:
                cur.executemany(insert_sql, buffer)
                target_conn.commit()

    target_conn.close()
    return total_rows


def _write_checkpoint(path: Path, rows: int) -> None:
    path.write_text(str(rows), encoding="utf-8")


def _read_checkpoint(path: Path) -> int:
    if path.exists():
        try:
            return int(path.read_text(encoding="utf-8").strip())
        except (ValueError, OSError):
            pass
    return 0


def _clear_checkpoint(path: Path) -> None:
    try:
        path.unlink(missing_ok=True)
    except OSError:
        pass


def _resolve_output_path(job_cfg: configparser.ConfigParser, job: str, fmt: str) -> Path:
    ext_map = {"csv": ".csv", "csv.gz": ".csv.gz", "parquet": ".parquet", "jsonl": ".jsonl", "db": ".done"}
    ext = ext_map.get(fmt, ".csv")
    output_dir = (get_job_param(job_cfg, job, "output_dir") or ".").strip()
    output_file = (get_job_param(job_cfg, job, "output_file") or f"{job}{ext}").strip()
    return Path(output_dir) / output_file


def _sanitize_row(row: tuple) -> list:
    result = []
    for val in row:
        if hasattr(val, "read"):
            try:
                result.append(val.read())
            except Exception:
                result.append("<LOB>")
        else:
            result.append(val)
    return result


def _is_true(value: str) -> bool:
    return (value or "").lower().strip() in ("1", "true", "yes", "oui")
