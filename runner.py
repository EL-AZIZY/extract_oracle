"""
runner.py
---------
Orchestration des jobs d'extraction avec parallélisme configurable.

Améliorations v2 :
  - Exécution parallèle via concurrent.futures.ThreadPoolExecutor
  - Chaque job acquiert sa propre connexion depuis le pool
  - Nombre de workers configurable (max_workers dans [settings])
  - Résumé final identique, mais avec temps par job
"""

import configparser
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
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
    job:     str
    rows:    int   = 0
    ok:      bool  = False
    error:   str   = ""
    elapsed: float = 0.0

    @property
    def status_label(self) -> str:
        return f"✅ OK  ({self.elapsed:.1f}s)" if self.ok \
               else f"❌ ERREUR : {self.error}"


# ─────────────────────────────────────────────────────────────────────────────
# Exécution
# ─────────────────────────────────────────────────────────────────────────────

def run_all(pool,
            job_names: list[str],
            job_cfg: configparser.ConfigParser,
            queries_dir: Path,
            log_dir: Path,
            max_workers: int = 1) -> list[JobResult]:
    """
    Exécute tous les jobs, en parallèle si max_workers > 1.
    Chaque job récupère sa connexion indépendamment depuis le pool.

    Retourne la liste des résultats (succès ou échec par job).
    """
    log.info("Démarrage de %d job(s) avec %d worker(s).",
             len(job_names), max_workers)

    results: list[JobResult] = []

    if max_workers <= 1:
        # Mode séquentiel (comportement v1)
        for job in job_names:
            results.append(_run_one(pool, job, job_cfg, queries_dir, log_dir))
    else:
        # Mode parallèle
        futures = {}
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for job in job_names:
                f = executor.submit(
                    _run_one, pool, job, job_cfg, queries_dir, log_dir
                )
                futures[f] = job

            for future in as_completed(futures):
                job = futures[future]
                try:
                    results.append(future.result())
                except Exception as exc:
                    # Ne devrait pas arriver car _run_one intercepte, mais sécurité
                    results.append(JobResult(job=job, error=str(exc)))

    _print_summary(results)
    return results


# ─────────────────────────────────────────────────────────────────────────────
# Méthodes privées
# ─────────────────────────────────────────────────────────────────────────────

def _run_one(pool, job: str,
             job_cfg: configparser.ConfigParser,
             queries_dir: Path,
             log_dir: Path) -> JobResult:
    """Exécute un seul job et retourne son résultat."""
    job_log = get_job_logger(job, log_dir)
    job_log.info("╔══ DÉBUT DU JOB : %s ══╗", job)
    result = JobResult(job=job)
    t0 = time.time()

    try:
        query       = load_query(job_cfg, job, queries_dir)
        result.rows = run_extraction(pool, query, job_cfg, job, job_log)
        result.ok   = True
        result.elapsed = time.time() - t0
        job_log.info("╚══ FIN DU JOB : %s ══╝  %d lignes  (%.1f s)",
                     job, result.rows, result.elapsed)

    except Exception as exc:
        result.error   = str(exc)
        result.elapsed = time.time() - t0
        job_log.error("╚══ FIN DU JOB : %s ══╝  ÉCHEC  (%.1f s)",
                      job, result.elapsed)

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
