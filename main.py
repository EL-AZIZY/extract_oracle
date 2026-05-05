# !/usr/bin/env python3
"""
main.py
-------
Point d'entrée du programme d'extraction Oracle → CSV.

Ce fichier ne contient que le parsing des arguments CLI
et l'appel aux modules du package oracle_extract.

Utilisation :
    python main.py                            # tous les jobs
    python main.py --config config.ini        # config explicite
    python main.py --job EMPLOYES             # un seul job
    python main.py --secure                   # chmod 600 sur la connexion
    python main.py --list                     # liste les jobs disponibles
"""

import argparse
import sys

import runner
from connection import managed_connection
from logger import setup_global_logger
from security import apply_secure_permissions, check_permissions
from settings import (
    get_job_names,
    get_job_param,
    get_log_dir,
    get_queries_dir,
    load_connection_config,
    load_job_config,
)

CONFIG_DEFAULT = "config.ini"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Extraction Oracle → CSV (connexion / jobs / SQL séparés).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Exemples :\n"
            "  python main.py                          # tous les jobs\n"
            "  python main.py --job EMPLOYES           # un seul job\n"
            "  python main.py --list                   # liste les jobs\n"
            "  python main.py --secure                 # sécurise la connexion\n"
        ),
    )
    p.add_argument("--config", "-c", default=CONFIG_DEFAULT,
                   help=f"Fichier de config des jobs (défaut : {CONFIG_DEFAULT})")
    p.add_argument("--job", "-j", default=None,
                   help="Nom du job à exécuter. Défaut : tous.")
    p.add_argument("--secure", action="store_true",
                   help="Applique chmod 600 sur le fichier de connexion puis quitte.")
    p.add_argument("--list", action="store_true",
                   help="Liste les jobs disponibles et quitte.")
    return p.parse_args()


def main() -> None:
    args    = parse_args()
    job_cfg = load_job_config(args.config)

    # ── Initialisation des logs (dès que possible) ────────────────────────────
    log_dir    = get_log_dir(job_cfg, args.config)
    global_log = setup_global_logger(log_dir)

    # ── Chargement de la connexion ────────────────────────────────────────────
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

    # ── Extraction ────────────────────────────────────────────────────────────
    global_log.info("Jobs à traiter : %s", jobs)

    with managed_connection(conn_cfg) as conn:
        results = runner.run_all(conn, jobs, job_cfg, queries_dir, log_dir)

    # Code de retour non-zéro si au moins un job a échoué
    if any(not r.ok for r in results):
        sys.exit(1)


def _print_job_list(jobs: list, job_cfg) -> None:
    print(f"\n{'JOB':<30}  {'SOURCE SQL':<40}  SORTIE")
    print("─" * 85)
    for job in jobs:
        qf  = job_cfg.get(job, "query_file", fallback=None)
        src = f"queries/{qf}" if qf else f"table:{job_cfg.get(job, 'table', fallback='?')}"
        out = get_job_param(job_cfg, job, "output_file") or f"{job}.csv"
        print(f"{job:<30}  {src:<40}  {out}")
    print()


if __name__ == "__main__":
    main()
