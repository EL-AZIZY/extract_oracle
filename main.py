#!/usr/bin/env python3
"""
main.py
-------
Point d'entrée du programme d'extraction DB → CSV / Parquet / JSON / DB.

Utilisation :
    python main.py                            # tous les jobs (séquentiel)
    python main.py --config config.ini        # config explicite
    python main.py --job EMPLOYES             # un seul job
    python main.py --workers 4                # parallélisme (4 workers)
    python main.py --secure                   # chmod 600 sur la connexion
    python main.py --list                     # liste les jobs disponibles
"""

import argparse
import logging
import signal
import sys

import runner
from cleanup import cleanup_orphan_checkpoints
from config_validator import validate_config
from connection import ConnectionPool
from db_audit import init_db_audit
from logger import setup_global_logger
from preflight_validator import run_validation
from security import apply_secure_permissions, check_permissions
from settings import (
    get_job_names,
    get_job_param,
    get_log_dir,
    get_max_workers,
    get_queries_dir,
    load_connection_config,
    load_job_config,
)

CONFIG_DEFAULT = "config.ini"
_shutdown_requested = False


def _handle_signal(signum, frame):
    del frame
    global _shutdown_requested
    name = "SIGTERM" if signum == signal.SIGTERM else "SIGINT"
    logging.getLogger(__name__).warning("%s recu - arret propre demande.", name)
    _shutdown_requested = True


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Extraction DB → CSV / Parquet / JSON / DB.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Exemples :\n"
            "  python main.py                          # tous les jobs\n"
            "  python main.py --job EMPLOYES           # un seul job\n"
            "  python main.py --workers 4              # 4 jobs en parallèle\n"
            "  python main.py --list                   # liste les jobs\n"
            "  python main.py --secure                 # sécurise la connexion\n"
        ),
    )
    p.add_argument("--config", "-c", default=CONFIG_DEFAULT,
                   help=f"Fichier de config des jobs (défaut : {CONFIG_DEFAULT})")
    p.add_argument("--job", "-j", default=None,
                   help="Nom du job à exécuter. Défaut : tous.")
    p.add_argument("--workers", "-w", type=int, default=None,
                   help="Nombre de jobs en parallèle (écrase max_workers du config).")
    p.add_argument("--dry-run", action="store_true",
                   help="Simulation complete sans ecriture de sortie.")
    p.add_argument("--validate", action="store_true",
                   help="Pre-flight checks sans execution des jobs.")
    p.add_argument("--cleanup", action="store_true",
                   help="Nettoie les checkpoints orphelins et quitte.")
    p.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                   help="Niveau de logs.")
    p.add_argument("--secure", action="store_true",
                   help="Applique chmod 600 sur le fichier de connexion puis quitte.")
    p.add_argument("--list", action="store_true",
                   help="Liste les jobs disponibles et quitte.")
    return p.parse_args()


def main() -> None:
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)
    args    = parse_args()
    job_cfg = load_job_config(args.config)

    # ── Logs ─────────────────────────────────────────────────────────────────
    log_dir    = get_log_dir(job_cfg, args.config)
    global_log = setup_global_logger(log_dir, level=args.log_level)

    # ── Connexion ─────────────────────────────────────────────────────────────
    conn_cfg, conn_path = load_connection_config(job_cfg, args.config)
    check_permissions(conn_path)

    # ── Mode --secure ─────────────────────────────────────────────────────────
    if args.secure:
        apply_secure_permissions(conn_path)
        global_log.info("Sécurisation terminée.")
        return

    # ── Résolution des chemins ────────────────────────────────────────────────
    queries_dir = get_queries_dir(job_cfg, args.config)
    jobs        = get_job_names(job_cfg, only=args.job)

    # ── Mode --list ───────────────────────────────────────────────────────────
    if args.list:
        _print_job_list(jobs, job_cfg)
        return

    if args.cleanup:
        removed = cleanup_orphan_checkpoints(log_dir, threshold_hours=24)
        global_log.info("Cleanup termine: %d checkpoint(s) supprimes.", removed)
        return

    if args.validate:
        cfg_ok = validate_config(job_cfg)
        pool = ConnectionPool(conn_cfg)
        try:
            ok, checks = run_validation(job_cfg, queries_dir, args.config, pool=pool)
        finally:
            pool.close()
        for line in checks:
            global_log.info(line)

        if not ok or not cfg_ok:
            global_log.error("VALIDATION FAILED.")
            for line in checks:
                if " KO" in line or " erreur " in line or "vide" in line:
                    global_log.error("Reason: %s", line)
            sys.exit(1)
        global_log.info("VALIDATION PASSED - READY FOR TESTS.")
        return

    # ── Parallélisme ─────────────────────────────────────────────────────────
    max_workers = args.workers if args.workers is not None else get_max_workers(job_cfg)

    global_log.info("Jobs à traiter : %s  (workers=%d)", jobs, max_workers)
    global_log.info("Mode dry-run : %s", args.dry_run)

    if _shutdown_requested:
        global_log.warning("Arret demande avant execution des jobs.")
        return

    # ── Pool + extraction ─────────────────────────────────────────────────────
    pool = ConnectionPool(conn_cfg)
    try:
        audit_enabled = init_db_audit(pool, conn_cfg, jobs)
        global_log.info("Audit SQL en base active: %s", audit_enabled)
        results = runner.run_all(
            pool, jobs, job_cfg, queries_dir, log_dir,
            max_workers=max_workers,
            dry_run=args.dry_run,
            audit_enabled=audit_enabled,
        )
    finally:
        pool.close()

    if any(not r.ok for r in results):
        sys.exit(1)


def _print_job_list(jobs: list, job_cfg) -> None:
    print(f"\n{'JOB':<30}  {'SOURCE SQL':<40}  {'FORMAT':<10}  SORTIE")
    print("─" * 100)
    for job in jobs:
        qf  = job_cfg.get(job, "query_file", fallback=None)
        src = f"queries/{qf}" if qf else f"table:{job_cfg.get(job, 'table', fallback='?')}"
        fmt = get_job_param(job_cfg, job, "format") or "csv"
        out = get_job_param(job_cfg, job, "output_file") or f"{job}.{fmt}"
        print(f"{job:<30}  {src:<40}  {fmt:<10}  {out}")
    print()


if __name__ == "__main__":
    main()
