"""
logger.py
---------
Initialisation du système de logs.

Responsabilités :
  - Logger racine  →  console + logs/extraction_YYYY-MM-DD.log  (global)
  - Logger par job →  logs/<JOB>_YYYY-MM-DD.log
  - Rotation par date (un nouveau fichier chaque jour, append dans la journée)
"""

import logging
import sys
from datetime import date
from pathlib import Path

LOG_FORMAT   = "%(asctime)s  %(levelname)-8s  %(name)-24s  %(message)s"
LOG_DATE_FMT = "%Y-%m-%d %H:%M:%S"
TODAY        = date.today().strftime("%Y-%m-%d")


def setup_global_logger(log_dir: Path) -> logging.Logger:
    """
    Configure le logger racine avec deux handlers :
      - StreamHandler   → sortie console
      - FileHandler     → logs/extraction_YYYY-MM-DD.log  (append)

    Retourne le logger racine.
    """
    log_file = log_dir / f"extraction_{TODAY}.log"

    root = logging.getLogger()
    root.setLevel(logging.INFO)

    # Évite les doublons si la fonction est appelée plusieurs fois
    if root.handlers:
        return root

    fmt = logging.Formatter(LOG_FORMAT, datefmt=LOG_DATE_FMT)

    # Console
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(fmt)
    root.addHandler(console_handler)

    # Fichier global
    file_handler = logging.FileHandler(log_file, mode="a", encoding="utf-8")
    file_handler.setFormatter(fmt)
    root.addHandler(file_handler)

    root.info("═" * 72)
    root.info("Démarrage extraction  |  log global : %s", log_file)
    root.info("═" * 72)

    return root


def get_job_logger(job: str, log_dir: Path) -> logging.Logger:
    """
    Retourne un logger dédié au job `job`.

    Ce logger :
      - Propage les messages vers le logger racine (console + log global)
      - Écrit aussi dans logs/<JOB>_YYYY-MM-DD.log  (append)
    """
    log_file = log_dir / f"{job}_{TODAY}.log"
    logger   = logging.getLogger(job)
    logger.setLevel(logging.INFO)

    # Ajoute le handler fichier une seule fois par fichier
    already_attached = any(
        isinstance(h, logging.FileHandler)
        and getattr(h, "baseFilename", "") == str(log_file.resolve())
        for h in logger.handlers
    )
    if not already_attached:
        fmt = logging.Formatter(LOG_FORMAT, datefmt=LOG_DATE_FMT)
        fh  = logging.FileHandler(log_file, mode="a", encoding="utf-8")
        fh.setFormatter(fmt)
        logger.addHandler(fh)

    return logger
