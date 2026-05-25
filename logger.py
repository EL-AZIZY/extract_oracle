"""
logger.py
---------
Initialisation du systÃ¨me de logs.

ResponsabilitÃ©s :
  - Logger racine  â†’  console + logs/extraction_YYYY-MM-DD.log  (global)
  - Logger par job â†’  logs/<JOB>_YYYY-MM-DD.log
  - Rotation par date (un nouveau fichier chaque jour, append dans la journÃ©e)
"""

import logging
import sys
import threading
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

LOG_FORMAT   = "%(asctime)s  %(levelname)-8s  [%(run_id)s]  %(name)-24s  %(message)s"
LOG_DATE_FMT = "%Y-%m-%d %H:%M:%S"
DEFAULT_RUN_ID = "--------"


class RunIdFilter(logging.Filter):
    _local = threading.local()

    def filter(self, record: logging.LogRecord) -> bool:
        record.run_id = getattr(self._local, "run_id", DEFAULT_RUN_ID)
        return True

    def set(self, run_id: str) -> None:
        self._local.run_id = run_id[:8] if run_id else DEFAULT_RUN_ID

    def clear(self) -> None:
        self._local.run_id = DEFAULT_RUN_ID


_run_id_filter = RunIdFilter()


def set_run_id(run_id: str) -> None:
    _run_id_filter.set(run_id)


def clear_run_id() -> None:
    _run_id_filter.clear()


def setup_global_logger(log_dir: Path, level: str = "INFO", retention_days: int = 30) -> logging.Logger:
    """
    Configure le logger racine avec deux handlers :
      - StreamHandler   â†’ sortie console
      - FileHandler     â†’ logs/extraction_YYYY-MM-DD.log  (append)

    Retourne le logger racine.
    """
    log_file = log_dir / "extraction.log"

    root = logging.getLogger()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))

    # Ã‰vite les doublons si la fonction est appelÃ©e plusieurs fois
    if root.handlers:
        return root

    fmt = logging.Formatter(LOG_FORMAT, datefmt=LOG_DATE_FMT)

    # Console
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(fmt)
    console_handler.addFilter(_run_id_filter)
    root.addHandler(console_handler)

    # Fichier global avec rotation journaliere
    file_handler = TimedRotatingFileHandler(
        filename=str(log_file),
        when="midnight",
        interval=1,
        backupCount=retention_days,
        encoding="utf-8",
    )
    file_handler.setFormatter(fmt)
    file_handler.addFilter(_run_id_filter)
    root.addHandler(file_handler)

    root.info("-" * 60)
    root.info("Demarrage extraction  |  log global : %s  |  niveau : %s", log_file, level)
    root.info("-" * 60)

    return root


def get_job_logger(job: str, log_dir: Path) -> logging.Logger:
    """
    Retourne un logger dÃ©diÃ© au job `job`.

    Ce logger :
      - Propage les messages vers le logger racine (console + log global)
      - Ã‰crit aussi dans logs/<JOB>_YYYY-MM-DD.log  (append)
    """
    log_file = log_dir / f"{job}.log"
    logger   = logging.getLogger(job)
    logger.setLevel(logging.NOTSET)

    # Ajoute le handler fichier une seule fois par fichier
    already_attached = any(
        isinstance(h, TimedRotatingFileHandler)
        and getattr(h, "baseFilename", "") == str(log_file.resolve())
        for h in logger.handlers
    )
    if not already_attached:
        fmt = logging.Formatter(LOG_FORMAT, datefmt=LOG_DATE_FMT)
        fh = TimedRotatingFileHandler(
            filename=str(log_file),
            when="midnight",
            interval=1,
            backupCount=30,
            encoding="utf-8",
        )
        fh.setFormatter(fmt)
        fh.addFilter(_run_id_filter)
        logger.addHandler(fh)

    return logger

