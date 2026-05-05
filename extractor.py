"""
extractor.py
------------
Extraction d'une requête Oracle vers un fichier CSV.

Responsabilités :
  - Exécuter la requête en mode batch (fetchmany)
  - Écrire les données dans le fichier CSV
  - Convertir les types Oracle non-standards (None, LOB…)
  - Logger la progression et le résultat
"""

import configparser
import csv
import logging
import time
from pathlib import Path
from settings import get_job_param

log = logging.getLogger(__name__)


def run(conn, query: str,
        job_cfg: configparser.ConfigParser,
        job: str,
        job_log: logging.Logger) -> int:
    """
    Exécute `query` et écrit les résultats dans le fichier CSV configuré.

    Retourne le nombre total de lignes extraites.
    Lève une exception en cas d'erreur (logguée avant propagation).
    """
    output_path = _resolve_output_path(job_cfg, job)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    separator  = get_job_param(job_cfg, job, "separator", ",")
    encoding   = get_job_param(job_cfg, job, "encoding", "utf-8-sig")
    no_header  = _is_true(get_job_param(job_cfg, job, "no_header", "false"))
    chunk_size = int(get_job_param(job_cfg, job, "chunk_size", "10000"))

    job_log.info("─" * 60)
    job_log.info("Sortie      : %s", output_path)
    job_log.info("Séparateur  : '%s'  |  Encodage : %s  |  Batch : %d lignes",
                 separator, encoding, chunk_size)

    total_rows = 0
    t0 = time.time()

    try:
        with conn.cursor() as cursor:
            cursor.arraysize = chunk_size
            cursor.execute(query)

            col_names = [d[0] for d in cursor.description]
            job_log.info("Colonnes (%d) : %s", len(col_names), col_names)

            with open(output_path, "w", newline="", encoding=encoding) as f:
                writer = csv.writer(
                    f,
                    delimiter=separator,
                    quoting=csv.QUOTE_MINIMAL,
                    lineterminator="\n",
                )
                if not no_header:
                    writer.writerow(col_names)

                while True:
                    rows = cursor.fetchmany(chunk_size)
                    if not rows:
                        break
                    writer.writerows(_sanitize_batch(rows))
                    total_rows += len(rows)
                    job_log.info("  %d lignes extraites…", total_rows)

        elapsed = time.time() - t0
        job_log.info("✅  SUCCÈS : %d lignes  →  %s  (%.1f s)",
                     total_rows, output_path, elapsed)

    except Exception as exc:
        elapsed = time.time() - t0
        job_log.error("❌  ÉCHEC après %.1f s : %s", elapsed, exc, exc_info=True)
        raise

    job_log.info("─" * 60)
    return total_rows


# ─────────────────────────────────────────────────────────────────────────────
# Méthodes privées
# ─────────────────────────────────────────────────────────────────────────────

def _resolve_output_path(job_cfg: configparser.ConfigParser, job: str) -> Path:
    """Calcule le chemin complet du fichier CSV de sortie."""
    output_dir  = (get_job_param(job_cfg, job, "output_dir") or ".").strip()
    output_file = (get_job_param(job_cfg, job, "output_file") or f"{job}.csv").strip()
    return Path(output_dir) / output_file


def _sanitize_batch(rows: list[tuple]) -> list[list]:
    """Convertit chaque ligne en liste de valeurs sérialisables en CSV."""
    return [_sanitize_row(row) for row in rows]


def _sanitize_row(row: tuple) -> list:
    """Convertit les types Oracle non-standards en valeurs Python natives."""
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
    """Interprète une chaîne de config comme un booléen."""
    return (value or "").lower().strip() in ("1", "true", "yes", "oui")
