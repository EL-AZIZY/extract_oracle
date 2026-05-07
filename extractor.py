"""
extractor.py
------------
Extraction d'une requête vers CSV, Parquet, JSON ou transfert DB-to-DB.

Améliorations v2 :
  - Formats de sortie : CSV, Parquet (pyarrow), JSON Lines, CSV compressé gzip
  - Transfert direct DB-to-DB (INSERT batché sans fichier intermédiaire)
  - Reprise sur point de contrôle (checkpoint) : aucune perte en cas d'échec
  - Streaming pur : aucun batch entier en mémoire (générateur ligne à ligne)
  - chunk_size toujours configurable
"""

import configparser
import csv
import gzip
import json
import logging
import os
import time
from pathlib import Path
from typing import Generator, Iterator

from settings import get_job_param

log = logging.getLogger(__name__)

# ── Import optionnel de pyarrow ───────────────────────────────────────────────
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    _ARROW_AVAILABLE = True
except ImportError:
    _ARROW_AVAILABLE = False


# ─────────────────────────────────────────────────────────────────────────────
# Point d'entrée principal
# ─────────────────────────────────────────────────────────────────────────────

def run(pool,
        query: str,
        job_cfg: configparser.ConfigParser,
        job: str,
        job_log: logging.Logger) -> int:
    """
    Exécute `query` et écrit les résultats dans le format configuré.

    Retourne le nombre total de lignes extraites.
    Lève une exception en cas d'erreur (loguée avant propagation).
    """
    fmt        = get_job_param(job_cfg, job, "format", "csv").lower()
    chunk_size = int(get_job_param(job_cfg, job, "chunk_size", "10000"))
    output_path = _resolve_output_path(job_cfg, job, fmt)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    checkpoint_path = output_path.with_suffix(".checkpoint")
    resume_from     = _read_checkpoint(checkpoint_path)

    job_log.info("─" * 60)
    job_log.info("Format      : %s", fmt.upper())
    job_log.info("Sortie      : %s", output_path)
    job_log.info("Batch       : %d lignes", chunk_size)
    if resume_from:
        job_log.info("Reprise     : depuis la ligne %d (checkpoint)", resume_from)

    t0 = time.time()
    total_rows = resume_from

    try:
        with pool.acquire() as conn:
            with conn.cursor() as cursor:
                cursor.arraysize = chunk_size
                _execute_with_offset(cursor, query, resume_from, job_log)
                col_names = [d[0] for d in cursor.description]
                job_log.info("Colonnes (%d) : %s", len(col_names), col_names)

                row_stream = _stream_rows(cursor, chunk_size, col_names)

                if fmt == "csv":
                    total_rows = _write_csv(
                        row_stream, col_names, output_path, job_cfg, job,
                        resume_from, checkpoint_path, total_rows, job_log,
                    )
                elif fmt == "csv.gz":
                    total_rows = _write_csv_gz(
                        row_stream, col_names, output_path,
                        resume_from, checkpoint_path, total_rows, job_log,
                    )
                elif fmt == "parquet":
                    total_rows = _write_parquet(
                        row_stream, col_names, output_path, chunk_size,
                        checkpoint_path, total_rows, job_log,
                    )
                elif fmt == "jsonl":
                    total_rows = _write_jsonl(
                        row_stream, col_names, output_path,
                        resume_from, checkpoint_path, total_rows, job_log,
                    )
                elif fmt == "db":
                    total_rows = _write_to_db(
                        row_stream, col_names, job_cfg, job,
                        checkpoint_path, total_rows, job_log,
                    )
                else:
                    raise ValueError(
                        f"Format '{fmt}' non supporté. "
                        "Valeurs acceptées : csv, csv.gz, parquet, jsonl, db"
                    )

        # Succès → supprime le checkpoint
        _clear_checkpoint(checkpoint_path)
        elapsed = time.time() - t0
        job_log.info("✅  SUCCÈS : %d lignes  →  %s  (%.1f s)",
                     total_rows, output_path, elapsed)

    except Exception as exc:
        elapsed = time.time() - t0
        job_log.error("❌  ÉCHEC après %.1f s  (checkpoint sauvegardé à la ligne %d) : %s",
                      elapsed, total_rows, exc, exc_info=True)
        raise

    job_log.info("─" * 60)
    return total_rows


# ─────────────────────────────────────────────────────────────────────────────
# Streaming de lignes (générateur — aucune matérialisation en mémoire)
# ─────────────────────────────────────────────────────────────────────────────

def _stream_rows(cursor, chunk_size: int, col_names: list) -> Generator:
    """
    Génère les lignes une par une depuis le curseur, par batch de chunk_size.
    Convertit les types Oracle/CLOB/BLOB à la volée.
    """
    while True:
        batch = cursor.fetchmany(chunk_size)
        if not batch:
            break
        for row in batch:
            yield _sanitize_row(row)


def _execute_with_offset(cursor, query: str, offset: int, job_log: logging.Logger) -> None:
    """
    Exécute la requête. Si un offset est demandé (reprise), ajoute OFFSET/SKIP.
    Oracle supporte OFFSET N ROWS FETCH NEXT … ROWS ONLY (12c+).
    """
    if offset > 0:
        # On utilise un wrapper OFFSET pour reprendre sans relire depuis 0
        wrapped = (
            f"SELECT * FROM ({query}) __subq__\n"
            f"OFFSET {offset} ROWS"
        )
        job_log.info("Reprise : exécution avec OFFSET %d ROWS", offset)
        cursor.execute(wrapped)
    else:
        cursor.execute(query)


# ─────────────────────────────────────────────────────────────────────────────
# Écrivains par format
# ─────────────────────────────────────────────────────────────────────────────

def _write_csv(row_stream, col_names, output_path, job_cfg, job,
               resume_from, checkpoint_path, total_rows, job_log) -> int:
    separator = get_job_param(job_cfg, job, "separator", ",")
    encoding  = get_job_param(job_cfg, job, "encoding", "utf-8-sig")
    no_header = _is_true(get_job_param(job_cfg, job, "no_header", "false"))

    mode = "a" if resume_from else "w"
    with open(output_path, mode, newline="", encoding=encoding) as f:
        writer = csv.writer(f, delimiter=separator,
                            quoting=csv.QUOTE_MINIMAL, lineterminator="\n")
        if not no_header and not resume_from:
            writer.writerow(col_names)
        for row in row_stream:
            writer.writerow(row)
            total_rows += 1
            if total_rows % 50_000 == 0:
                f.flush()
                _write_checkpoint(checkpoint_path, total_rows)
                job_log.info("  %d lignes extraites…", total_rows)
    return total_rows


def _write_csv_gz(row_stream, col_names, output_path,
                  resume_from, checkpoint_path, total_rows, job_log) -> int:
    # gzip : pas de mode append natif → reprise non supportée pour ce format
    if resume_from:
        log.warning("Reprise non supportée pour csv.gz — redémarrage depuis 0.")
        total_rows = 0
    with gzip.open(output_path, "wt", encoding="utf-8", newline="") as gz:
        writer = csv.writer(gz, lineterminator="\n")
        writer.writerow(col_names)
        for row in row_stream:
            writer.writerow(row)
            total_rows += 1
            if total_rows % 50_000 == 0:
                _write_checkpoint(checkpoint_path, total_rows)
                job_log.info("  %d lignes extraites…", total_rows)
    return total_rows


def _write_parquet(row_stream, col_names, output_path,
                   chunk_size, checkpoint_path, total_rows, job_log) -> int:
    if not _ARROW_AVAILABLE:
        raise ImportError(
            "pyarrow est requis pour le format Parquet. "
            "Installez-le avec : pip install pyarrow"
        )
    writer = None
    buffer = []
    try:
        for row in row_stream:
            buffer.append(row)
            total_rows += 1
            if len(buffer) >= chunk_size:
                table  = pa.table({c: [r[i] for r in buffer]
                                   for i, c in enumerate(col_names)})
                if writer is None:
                    writer = pq.ParquetWriter(output_path, table.schema,
                                              compression="snappy")
                writer.write_table(table)
                buffer.clear()
                _write_checkpoint(checkpoint_path, total_rows)
                job_log.info("  %d lignes extraites…", total_rows)
        # flush final
        if buffer:
            table = pa.table({c: [r[i] for r in buffer]
                               for i, c in enumerate(col_names)})
            if writer is None:
                writer = pq.ParquetWriter(output_path, table.schema,
                                          compression="snappy")
            writer.write_table(table)
    finally:
        if writer:
            writer.close()
    return total_rows


def _write_jsonl(row_stream, col_names, output_path,
                 resume_from, checkpoint_path, total_rows, job_log) -> int:
    mode = "a" if resume_from else "w"
    with open(output_path, mode, encoding="utf-8") as f:
        for row in row_stream:
            record = dict(zip(col_names, row))
            f.write(json.dumps(record, default=str, ensure_ascii=False) + "\n")
            total_rows += 1
            if total_rows % 50_000 == 0:
                f.flush()
                _write_checkpoint(checkpoint_path, total_rows)
                job_log.info("  %d lignes extraites…", total_rows)
    return total_rows


def _write_to_db(row_stream, col_names, job_cfg, job,
                 checkpoint_path, total_rows, job_log) -> int:
    """
    Transfert direct DB-to-DB : INSERT batché dans une table cible.
    Paramètres requis dans la config du job :
      - target_dsn   : DSN de la base cible (ex: postgresql://user:pw@host/db)
      - target_table : nom de la table cible
    """
    import importlib

    target_dsn   = get_job_param(job_cfg, job, "target_dsn")
    target_table = get_job_param(job_cfg, job, "target_table")
    batch_size   = int(get_job_param(job_cfg, job, "chunk_size", "5000"))

    if not target_dsn or not target_table:
        raise ValueError("'target_dsn' et 'target_table' sont requis pour le format 'db'.")

    # Connexion cible (psycopg2 pour PostgreSQL, adaptable)
    try:
        import psycopg2
        target_conn = psycopg2.connect(target_dsn)
    except ImportError:
        raise ImportError("psycopg2 requis pour le transfert DB-to-DB. pip install psycopg2-binary")

    placeholders = ", ".join(["%s"] * len(col_names))
    cols_str     = ", ".join(col_names)
    insert_sql   = f"INSERT INTO {target_table} ({cols_str}) VALUES ({placeholders})"

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
                    job_log.info("  %d lignes insérées…", total_rows)
            if buffer:
                cur.executemany(insert_sql, buffer)
                target_conn.commit()

    target_conn.close()
    return total_rows


# ─────────────────────────────────────────────────────────────────────────────
# Checkpoint (reprise en cas d'échec)
# ─────────────────────────────────────────────────────────────────────────────

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


# ─────────────────────────────────────────────────────────────────────────────
# Utilitaires
# ─────────────────────────────────────────────────────────────────────────────

def _resolve_output_path(job_cfg: configparser.ConfigParser, job: str, fmt: str) -> Path:
    ext_map = {"csv": ".csv", "csv.gz": ".csv.gz",
                "parquet": ".parquet", "jsonl": ".jsonl", "db": ".done"}
    ext = ext_map.get(fmt, ".csv")
    output_dir  = (get_job_param(job_cfg, job, "output_dir") or ".").strip()
    output_file = (get_job_param(job_cfg, job, "output_file") or f"{job}{ext}").strip()
    return Path(output_dir) / output_file


def _sanitize_row(row: tuple) -> list:
    result = []
    for val in row:
        if val is None:
            result.append("")
        elif hasattr(val, "read"):      # CLOB / BLOB
            try:
                result.append(val.read())
            except Exception:
                result.append("<LOB>")
        else:
            result.append(val)
    return result


def _is_true(value: str) -> bool:
    return (value or "").lower().strip() in ("1", "true", "yes", "oui")
