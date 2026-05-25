"""
runner.py
---------
Orchestration des jobs d'extraction avec parallÃ©lisme configurable.

AmÃ©liorations v2 :
  - ExÃ©cution parallÃ¨le via concurrent.futures.ThreadPoolExecutor
  - Chaque job acquiert sa propre connexion depuis le pool
  - Nombre de workers configurable (max_workers dans [settings])
  - RÃ©sumÃ© final identique, mais avec temps par job
"""

import configparser
import logging
import signal
import sys
import time
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from threading import Event
from typing import Dict, List

from logger import get_job_logger
from logger import clear_run_id, set_run_id
from extractor import run as run_extraction
from query_loader import load as load_query
from db_audit import create_run, log_run_end, log_run_start
from retry import RetryableOperation, RETRYABLE_EXCEPTIONS
from settings import get_job_param

log = logging.getLogger(__name__)
_shutdown_event = Event()


def _signal_handler(signum, frame):
    del frame
    signal_name = signal.Signals(signum).name
    log.warning(
        "\nReceived %s - initiating graceful shutdown.\n"
        "Current jobs will complete, new jobs will not start.\n"
        "Press Ctrl+C again to force exit.",
        signal_name,
    )
    if _shutdown_event.is_set():
        log.warning("Second signal received: force exit.")
        sys.exit(1)
    _shutdown_event.set()


def setup_signal_handlers() -> None:
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)


def is_shutdown_requested() -> bool:
    return _shutdown_event.is_set()


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# ModÃ¨le de rÃ©sultat
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

@dataclass
class JobResult:
    job:     str
    rows:    int   = 0
    ok:      bool  = False
    error:   str   = ""
    elapsed: float = 0.0
    skipped: bool  = False

    @property
    def status_label(self) -> str:
        if self.skipped:
            return "SKIPPED (shutdown)"
        return f"OK ({self.elapsed:.1f}s)" if self.ok else f"ERROR: {self.error}"


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# ExÃ©cution
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def run_all(pool,
            job_names: List[str],
            job_cfg: configparser.ConfigParser,
            queries_dir: Path,
            log_dir: Path,
            max_workers: int = 1,
            dry_run: bool = False,
            audit_enabled: bool = False) -> List[JobResult]:
    """
    ExÃ©cute tous les jobs, en parallÃ¨le si max_workers > 1.
    Chaque job rÃ©cupÃ¨re sa connexion indÃ©pendamment depuis le pool.

    Retourne la liste des rÃ©sultats (succÃ¨s ou Ã©chec par job).
    """
    setup_signal_handlers()
    log.info("Starting %d job(s) with %d worker(s).",
             len(job_names), max_workers)

    results: List[JobResult] = []
    pending_jobs = list(job_names)

    if max_workers <= 1:
        # Mode sÃ©quentiel (comportement v1)
        for job in pending_jobs:
            if is_shutdown_requested():
                results.append(JobResult(job=job, skipped=True))
                continue
            results.append(_run_one(pool, job, job_cfg, queries_dir, log_dir, dry_run, audit_enabled))
    else:
        # Mode parallÃ¨le
        futures: Dict[Future, str] = {}
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for job in pending_jobs:
                if is_shutdown_requested():
                    break
                f = executor.submit(
                    _run_one, pool, job, job_cfg, queries_dir, log_dir, dry_run, audit_enabled
                )
                futures[f] = job

            completed_count = 0
            for future in as_completed(futures):
                job = futures[future]
                try:
                    results.append(future.result())
                except Exception as exc:
                    # Ne devrait pas arriver car _run_one intercepte, mais sÃ©curitÃ©
                    results.append(JobResult(job=job, error=str(exc)))
                completed_count += 1
                remaining = len(pending_jobs) - completed_count
                if remaining > 0:
                    log.info(
                        "Progress: %d/%d jobs complete, %d remaining",
                        completed_count, len(pending_jobs), remaining
                    )

            started_jobs = set(futures.values())
            for job in pending_jobs:
                if job not in started_jobs:
                    results.append(JobResult(job=job, skipped=True))

    _print_summary(results)
    return results


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# MÃ©thodes privÃ©es
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def _run_one(pool, job: str,
             job_cfg: configparser.ConfigParser,
             queries_dir: Path,
             log_dir: Path,
             dry_run: bool = False,
             audit_enabled: bool = False) -> JobResult:
    """ExÃ©cute un seul job et retourne son rÃ©sultat."""
    job_log = get_job_logger(job, log_dir)
    job_log.info("=" * 84)
    job_log.info("JOB START | %s", job)
    job_log.info("=" * 84)
    result = JobResult(job=job)
    t0 = time.time()
    ctx = create_run(job_name=job, mode="DRY_RUN" if dry_run else "RUN")
    set_run_id(ctx.run_id)
    if audit_enabled:
        log_run_start(pool, ctx)

    try:
        query       = load_query(job_cfg, job, queries_dir)
        retry_count = int(get_job_param(job_cfg, job, "retry_count", "0"))
        retry_base_delay = float(get_job_param(job_cfg, job, "retry_base_delay_sec", "1.0"))
        rows = 0
        with RetryableOperation(max_retries=retry_count, base_delay=retry_base_delay) as op:
            for attempt in op:
                try:
                    if attempt > 0:
                        job_log.warning("Retry attempt %d/%d for job %s", attempt, retry_count, job)
                    rows = run_extraction(pool, query, job_cfg, job, job_log, dry_run=dry_run)
                    break
                except RETRYABLE_EXCEPTIONS as exc:
                    if attempt < retry_count:
                        job_log.warning("Transient failure on job %s: %s", job, exc)
                        op.retry()
                        continue
                    raise
        result.rows = rows
        result.ok   = True
        result.elapsed = time.time() - t0
        if audit_enabled:
            log_run_end(pool, ctx, ok=True, rows=result.rows)
        job_log.info("-" * 84)
        job_log.info("JOB END   | %s | STATUS=OK | rows=%d | elapsed=%.1fs",
                     job, result.rows, result.elapsed)
        job_log.info("-" * 84)

    except Exception as exc:
        result.error   = str(exc)
        result.elapsed = time.time() - t0
        if audit_enabled:
            log_run_end(pool, ctx, ok=False, rows=result.rows, error=result.error)
        job_log.error("-" * 84)
        job_log.error("JOB END   | %s | STATUS=ERROR | elapsed=%.1fs | error=%s",
                      job, result.elapsed, result.error)
        job_log.error("-" * 84)
    finally:
        clear_run_id()

    return result


def _print_summary(results: list[JobResult]) -> None:
    """Affiche le rÃ©capitulatif final dans le log global."""
    log.info("-" * 60)
    log.info("RECAPITULATIF (%d job(s))", len(results))
    log.info("-" * 60)
    for r in results:
        log.info("  %-30s  %6d lignes  %s", r.job, r.rows, r.status_label)
    log.info("-" * 60)

    nb_errors = sum(1 for r in results if not r.ok)
    if nb_errors:
        log.warning("%d job(s) en erreur sur %d.", nb_errors, len(results))
    else:
        log.info("Tous les jobs ont rÃ©ussi.")


