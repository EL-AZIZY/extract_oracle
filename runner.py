"""
runner.py
---------
Orchestration des jobs d'extraction.

Responsabilités :
  - Itérer sur la liste des jobs
  - Coordonner query_loader, extractor et logger pour chaque job
  - Collecter les résultats et produire un récapitulatif final
  - Isoler les erreurs (un job en échec ne bloque pas les suivants)
"""

import configparser
import logging
from dataclasses import dataclass, field
from pathlib import Path

from logger import get_job_logger
from extractor import run as run_extraction
from query_loader import load as load_query



log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Modèle de résultat
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class JobResult:
    job:    str
    rows:   int   = 0
    ok:     bool  = False
    error:  str   = ""

    @property
    def status_label(self) -> str:
        return "✅ OK" if self.ok else f"❌ ERREUR : {self.error}"


# ─────────────────────────────────────────────────────────────────────────────
# Exécution
# ─────────────────────────────────────────────────────────────────────────────

def run_all(conn,
            job_names: list[str],
            job_cfg: configparser.ConfigParser,
            queries_dir: Path,
            log_dir: Path) -> list[JobResult]:
    """
    Exécute tous les jobs dans l'ordre.
    Retourne la liste des résultats (succès ou échec par job).
    """
    results = []

    for job in job_names:
        result = _run_one(conn, job, job_cfg, queries_dir, log_dir)
        results.append(result)

    _print_summary(results)
    return results


# ─────────────────────────────────────────────────────────────────────────────
# Méthodes privées
# ─────────────────────────────────────────────────────────────────────────────

def _run_one(conn, job: str,
             job_cfg: configparser.ConfigParser,
             queries_dir: Path,
             log_dir: Path) -> JobResult:
    """Exécute un seul job et retourne son résultat."""
    job_log = get_job_logger(job, log_dir)
    job_log.info("╔══ DÉBUT DU JOB : %s ══╗", job)
    result = JobResult(job=job)

    try:
        query       =load_query(job_cfg, job, queries_dir)
        result.rows = run_extraction(conn, query, job_cfg, job, job_log)
        result.ok   = True
        job_log.info("╚══ FIN DU JOB : %s ══╝  %d lignes extraites", job, result.rows)

    except Exception as exc:
        result.error = str(exc)
        job_log.error("╚══ FIN DU JOB : %s ══╝  ÉCHEC", job)

    return result


def _print_summary(results: list[JobResult]) -> None:
    """Affiche le récapitulatif final dans le log global."""
    log.info("═" * 72)
    log.info("RÉCAPITULATIF  (%d job(s))", len(results))
    log.info("═" * 72)
    for r in results:
        log.info("  %-30s  %6d lignes  %s", r.job, r.rows, r.status_label)
    log.info("═" * 72)

    nb_errors = sum(1 for r in results if not r.ok)
    if nb_errors:
        log.warning("%d job(s) en erreur sur %d.", nb_errors, len(results))
    else:
        log.info("Tous les jobs ont réussi.")
