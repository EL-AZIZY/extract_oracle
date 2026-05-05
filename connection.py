"""
connection.py
-------------
Gestion de la connexion Oracle.

Responsabilités :
  - Importer le driver Oracle (oracledb ou cx_Oracle)
  - Ouvrir la connexion depuis la configuration
  - Exposer un context manager pour garantir la fermeture
"""

import configparser
import logging
import sys
from contextlib import contextmanager

log = logging.getLogger(__name__)

# ── Import du driver Oracle (oracledb moderne ou cx_Oracle legacy) ────────────
try:
    import oracledb
    DRIVER_NAME = "oracledb"
except ImportError:
    try:
        import cx_Oracle as oracledb
        DRIVER_NAME = "cx_Oracle"
    except ImportError:
        sys.exit(
            " Aucun driver Oracle installé.\n"
            "    Installez-en un avec : pip install oracledb"
        )


def open_connection(conn_cfg: configparser.ConfigParser):
    """
    Ouvre et retourne une connexion Oracle depuis la section [oracle]
    du fichier de configuration de connexion.
    """
    host     = conn_cfg.get("oracle", "host")
    port     = conn_cfg.getint("oracle", "port", fallback=1522)
    service  = conn_cfg.get("oracle", "service")
    user     = conn_cfg.get("oracle", "user")
    password = conn_cfg.get("oracle", "password")
    dsn      = f"{host}:{port}/{service}"

    log.info("Driver utilisé : %s", DRIVER_NAME)
    log.info("Connexion à %s en tant que %s …", dsn, user)

    conn = oracledb.connect(user=user, password=password, dsn=dsn)
    log.info("Connecté ✓")
    return conn


@contextmanager
def managed_connection(conn_cfg: configparser.ConfigParser):
    """
    Context manager : ouvre la connexion et garantit sa fermeture,
    même en cas d'exception.

    Usage :
        with managed_connection(conn_cfg) as conn:
            ...
    """
    conn = open_connection(conn_cfg)
    try:
        yield conn
    finally:
        conn.close()
        log.info("Connexion Oracle fermée.")
