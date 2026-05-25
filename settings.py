"""
settings.py
-----------
Chargement et validation des fichiers de configuration.

Modifications v2 :
  - Section [oracle] → [database] pour généricité multi-bases
  - Lecture de max_workers (parallélisme)
  - Lecture de pool_min / pool_max (connection pooling)
  - password_env : référence à une variable d'environnement
"""

import configparser
import sys
from pathlib import Path

# Sections réservées dans config.ini (ne sont pas des jobs)
RESERVED_SECTIONS = {"settings", "csv"}


# ─────────────────────────────────────────────────────────────────────────────
# Chargement des fichiers INI
# ─────────────────────────────────────────────────────────────────────────────

def load_job_config(config_path: str) -> configparser.ConfigParser:
    """Charge et valide le fichier de config des jobs (config.ini)."""
    cfg = configparser.ConfigParser(interpolation=None)
    if not cfg.read(config_path):
        sys.exit(f" Fichier de configuration introuvable : {config_path}")
    if not cfg.has_option("settings", "connection_file"):
        sys.exit(" [settings] doit contenir la clé 'connection_file'.")
    return cfg


def load_connection_config(
    job_cfg: configparser.ConfigParser,
    config_path: str,
) -> tuple[configparser.ConfigParser, Path]:
    """
    Charge et valide le fichier de connexion (connection.ini).
    Supporte les sections [database] (v2) et [oracle] (v1, rétrocompat).
    """
    raw       = job_cfg.get("settings", "connection_file").strip()
    conn_path = _resolve_path(raw, config_path)

    if not conn_path.exists():
        sys.exit(
            f" Fichier de connexion introuvable : {conn_path}\n"
            f"    Vérifiez 'connection_file' dans [settings]."
        )

    conn_cfg = configparser.ConfigParser(interpolation=None)
    conn_cfg.read(conn_path)

    # Rétrocompatibilité : [oracle] → alias [database]
    if conn_cfg.has_section("oracle") and not conn_cfg.has_section("database"):
        conn_cfg.add_section("database")
        conn_cfg.set("database", "type", "oracle")
        for key, val in conn_cfg.items("oracle"):
            conn_cfg.set("database", key, val)

    if not conn_cfg.has_section("database"):
        sys.exit(
            f" Section [database] (ou [oracle]) absente dans {conn_path.name}"
        )

    for key in ("host", "port", "service", "user"):
        if not conn_cfg.has_option("database", key):
            sys.exit(f" Clé manquante dans [database] ({conn_path.name}) : {key}")

    # Vérifier qu'au moins un moyen d'authentification est fourni
    has_pw     = conn_cfg.has_option("database", "password")
    has_pw_env = conn_cfg.has_option("database", "password_env")
    if not has_pw and not has_pw_env:
        sys.exit(
            f" [database] dans {conn_path.name} doit contenir "
            "'password_env' (recommandé) ou 'password'."
        )

    return conn_cfg, conn_path


# ─────────────────────────────────────────────────────────────────────────────
# Résolution des répertoires
# ─────────────────────────────────────────────────────────────────────────────

def get_queries_dir(job_cfg: configparser.ConfigParser, config_path: str) -> Path:
    raw  = job_cfg.get("settings", "queries_dir", fallback="queries").strip()
    path = _resolve_path(raw, config_path)
    if not path.exists():
        sys.exit(f" Répertoire des requêtes introuvable : {path}")
    return path


def get_log_dir(job_cfg: configparser.ConfigParser, config_path: str) -> Path:
    raw  = job_cfg.get("settings", "log_dir", fallback="logs").strip()
    path = _resolve_path(raw, config_path)
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_job_output_dir(job_cfg: configparser.ConfigParser, job: str, config_path: str) -> Path:
    """
    Retourne le répertoire de sortie d'un job avec la même résolution
    de chemin que le reste du programme (relatif au fichier config).
    """
    raw = (get_job_param(job_cfg, job, "output_dir", ".") or ".").strip()
    path = _resolve_path(raw, config_path)
    return path


def get_max_workers(job_cfg: configparser.ConfigParser) -> int:
    """Retourne le nombre de workers parallèles (défaut : 1 = séquentiel)."""
    try:
        return int(job_cfg.get("settings", "max_workers", fallback="1"))
    except ValueError:
        return 1


# ─────────────────────────────────────────────────────────────────────────────
# Gestion des jobs
# ─────────────────────────────────────────────────────────────────────────────

def get_job_names(job_cfg: configparser.ConfigParser, only: str = None) -> list[str]:
    jobs = [s for s in job_cfg.sections() if s.lower() not in RESERVED_SECTIONS]
    if not jobs:
        sys.exit(" Aucun job défini dans le fichier de configuration.")
    if only:
        if only not in jobs:
            sys.exit(f"  Job '{only}' introuvable. Jobs disponibles : {jobs}")
        return [only]
    return jobs


def get_job_param(job_cfg: configparser.ConfigParser,
                  job: str, key: str, fallback=None) -> str:
    """
    Lit une clé de configuration avec héritage :
      1. Section du job  [JOB_NAME]
      2. Section des défauts  [csv]
      3. Valeur de fallback
    """
    if job_cfg.has_option(job, key):
        return job_cfg.get(job, key)
    if job_cfg.has_option("csv", key):
        return job_cfg.get("csv", key)
    return fallback


# ─────────────────────────────────────────────────────────────────────────────
# Utilitaire interne
# ─────────────────────────────────────────────────────────────────────────────

def _resolve_path(raw: str, reference_file: str) -> Path:
    p = Path(raw)
    if not p.is_absolute():
        p = (Path(reference_file).parent / p).resolve()
    return p
