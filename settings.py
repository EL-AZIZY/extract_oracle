"""
settings.py
-----------
Chargement et validation des fichiers de configuration.

Responsabilités :
  - Lire config.ini  (jobs + paramètres CSV)
  - Lire oracle_connection.ini  (credentials Oracle)
  - Résoudre les chemins : queries_dir, log_dir, output_dir
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
        sys.exit(f"❌  Fichier de configuration introuvable : {config_path}")
    if not cfg.has_option("settings", "connection_file"):
        sys.exit("❌  [settings] doit contenir la clé 'connection_file'.")
    return cfg


def load_connection_config(job_cfg: configparser.ConfigParser,
                           config_path: str) -> configparser.ConfigParser:
    """
    Charge et valide le fichier de connexion Oracle (oracle_connection.ini).
    Le chemin est résolu relativement à config.ini si non absolu.
    """
    raw       = job_cfg.get("settings", "connection_file").strip()
    conn_path = _resolve_path(raw, config_path)

    if not conn_path.exists():
        sys.exit(
            f"  Fichier de connexion introuvable : {conn_path}\n"
            f"    Vérifiez 'connection_file' dans [settings]."
        )

    conn_cfg = configparser.ConfigParser(interpolation=None)
    conn_cfg.read(conn_path)

    for key in ("host", "port", "service", "user", "password"):
        if not conn_cfg.has_option("oracle", key):
            sys.exit(f"  Clé manquante dans [oracle] ({conn_path.name}) : {key}")

    return conn_cfg, conn_path


# ─────────────────────────────────────────────────────────────────────────────
# Résolution des répertoires
# ─────────────────────────────────────────────────────────────────────────────

def get_queries_dir(job_cfg: configparser.ConfigParser, config_path: str) -> Path:
    """Retourne le chemin absolu du répertoire des fichiers SQL."""
    raw  = job_cfg.get("settings", "queries_dir", fallback="queries").strip()
    path = _resolve_path(raw, config_path)
    if not path.exists():
        sys.exit(f"❌  Répertoire des requêtes introuvable : {path}")
    return path


def get_log_dir(job_cfg: configparser.ConfigParser, config_path: str) -> Path:
    """Retourne le chemin absolu du répertoire des logs (créé si absent)."""
    raw  = job_cfg.get("settings", "log_dir", fallback="logs").strip()
    path = _resolve_path(raw, config_path)
    path.mkdir(parents=True, exist_ok=True)
    return path


# ─────────────────────────────────────────────────────────────────────────────
# Gestion des jobs
# ─────────────────────────────────────────────────────────────────────────────

def get_job_names(job_cfg: configparser.ConfigParser, only: str = None) -> list[str]:
    """
    Retourne la liste des noms de jobs définis dans config.ini.
    Si `only` est fourni, retourne uniquement ce job après vérification.
    """
    jobs = [s for s in job_cfg.sections() if s.lower() not in RESERVED_SECTIONS]
    if not jobs:
        sys.exit("  Aucun job défini dans le fichier de configuration.")
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
    """Résout un chemin relatif par rapport au répertoire du fichier de référence."""
    p = Path(raw)
    if not p.is_absolute():
        p = (Path(reference_file).parent / p).resolve()
    return p
