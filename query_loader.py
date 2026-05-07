"""
query_loader.py
---------------
Chargement des requêtes SQL.

Responsabilités :
  - Lire un fichier .sql depuis le répertoire des requêtes
  - Générer un SELECT * simple si aucun fichier n'est fourni (mode table brute)
  - Nettoyer la requête (suppression du point-virgule final, strip)
"""

import configparser
import logging
import sys
from pathlib import Path

log = logging.getLogger(__name__)


def load(job_cfg: configparser.ConfigParser, job: str, queries_dir: Path) -> str:
    """
    Retourne la requête SQL pour le job donné.

    Priorité :
      1. query_file  →  lit le fichier .sql dans queries_dir
      2. table       →  génère SELECT * FROM [schema.]table
    """
    if job_cfg.has_option(job, "query_file"):
        return _load_from_file(job_cfg, job, queries_dir)

    if job_cfg.has_option(job, "table"):
        return _build_select_star(job_cfg, job)

    sys.exit(
        f"  [{job}] Ni 'query_file' ni 'table' définis dans la configuration.\n"
        f"    Ajoutez l'une de ces clés dans la section [{job}]."
    )


# ─────────────────────────────────────────────────────────────────────────────
# Méthodes privées
# ─────────────────────────────────────────────────────────────────────────────

def _load_from_file(job_cfg: configparser.ConfigParser,
                    job: str, queries_dir: Path) -> str:
    """Lit et retourne le contenu du fichier .sql référencé par le job."""
    filename = job_cfg.get(job, "query_file").strip()
    sql_path = queries_dir / filename

    if not sql_path.exists():
        sys.exit(
            f"  [{job}] Fichier SQL introuvable : {sql_path}\n"
            f"    Vérifiez 'query_file = {filename}' dans la section [{job}]."
        )

    query = sql_path.read_text(encoding="utf-8").strip().rstrip(";")
    log.info("Requête chargée depuis : %s  (%d caractères)", sql_path.name, len(query))
    return query


def _build_select_star(job_cfg: configparser.ConfigParser, job: str) -> str:
    """Génère un SELECT * FROM [schema.]table."""
    table  = job_cfg.get(job, "table").upper()
    schema = (job_cfg.get(job, "schema", fallback="") or "").strip().upper()
    full   = f"{schema}.{table}" if schema else table
    query  = f"SELECT * FROM {full}"
    log.info("Requête générée automatiquement : %s", query)
    return query
