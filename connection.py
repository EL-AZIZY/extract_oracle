"""
connection.py
-------------
Gestion des connexions avec connection pooling et support multi-bases.

AmÃ©liorations v2 :
  - Connection pooling via oracledb.create_pool()
  - Support multi-drivers : Oracle, PostgreSQL, MySQL
  - Context manager retournant une connexion depuis le pool
  - ParamÃ¨tres du pool configurables (min, max, increment)
"""

import configparser
import logging
import sys
from contextlib import contextmanager
from typing import Any

log = logging.getLogger(__name__)

# â”€â”€ Import des drivers disponibles â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
_DRIVERS: dict[str, Any] = {}

try:
    import oracledb
    _DRIVERS["oracle"] = oracledb
except ImportError:
    pass

try:
    import psycopg2
    from psycopg2 import pool as pg_pool
    _DRIVERS["postgresql"] = psycopg2
except ImportError:
    pass

try:
    import mysql.connector
    from mysql.connector.pooling import MySQLConnectionPool
    _DRIVERS["mysql"] = mysql.connector
except ImportError:
    pass

if not _DRIVERS:
    sys.exit(
        "  Aucun driver de base de donnÃ©es installÃ©.\n"
        "    Oracle    : pip install oracledb\n"
        "    PostgreSQL: pip install psycopg2-binary\n"
        "    MySQL     : pip install mysql-connector-python"
    )


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Connection Pool
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

class ConnectionPool:
    """
    Pool de connexions abstrait supportant Oracle, PostgreSQL et MySQL.
    Une seule instance est crÃ©Ã©e et rÃ©utilisÃ©e sur toute la durÃ©e du programme.
    """

    def __init__(self, conn_cfg: configparser.ConfigParser):
        self._cfg     = conn_cfg
        self._db_type = conn_cfg.get("database", "type", fallback="oracle").lower()
        self._pool    = None
        self._init_pool()

    def _init_pool(self) -> None:
        """Initialise le pool selon le type de base de donnÃ©es."""
        if self._db_type not in _DRIVERS:
            available = list(_DRIVERS.keys())
            sys.exit(
                f" Driver '{self._db_type}' non installÃ© ou non supportÃ©.\n"
                f"    Drivers disponibles : {available}"
            )

        cfg = self._cfg
        section = "database"

        host     = cfg.get(section, "host")
        port     = cfg.getint(section, "port")
        service  = cfg.get(section, "service")
        user     = cfg.get(section, "user")
        password = _resolve_password(cfg, section)
        pool_min = cfg.getint(section, "pool_min", fallback=1)
        pool_max = cfg.getint(section, "pool_max", fallback=4)

        log.info("Initialisation pool %s â†’ %s@%s:%s/%s (min=%d max=%d)",
                 self._db_type, user, host, port, service, pool_min, pool_max)

        if self._db_type == "oracle":
            self._pool = _DRIVERS["oracle"].create_pool(
                user=user, password=password,
                dsn=f"{host}:{port}/{service}",
                min=pool_min, max=pool_max, increment=1,
            )

        elif self._db_type == "postgresql":
            self._pool = pg_pool.ThreadedConnectionPool(
                minconn=pool_min, maxconn=pool_max,
                host=host, port=port, dbname=service,
                user=user, password=password,
            )

        elif self._db_type == "mysql":
            self._pool = MySQLConnectionPool(
                pool_name="extract_pool",
                pool_size=pool_max,
                host=host, port=port, database=service,
                user=user, password=password,
            )

        log.info("Pool initialisÃ© âœ“")

    @contextmanager
    def acquire(self):
        """
        Context manager : acquiert une connexion du pool et la libÃ¨re Ã  la fin.

        Usage :
            with pool.acquire() as conn:
                ...
        """
        conn = None
        try:
            if self._db_type == "oracle":
                conn = self._pool.acquire()
            elif self._db_type == "postgresql":
                conn = self._pool.getconn()
            elif self._db_type == "mysql":
                conn = self._pool.get_connection()
            yield conn
        finally:
            if conn is not None:
                if self._db_type == "oracle":
                    self._pool.release(conn)
                elif self._db_type == "postgresql":
                    self._pool.putconn(conn)
                elif self._db_type == "mysql":
                    conn.close()  # retour automatique au pool MySQL

    def close(self) -> None:
        """Ferme toutes les connexions du pool."""
        if self._pool is None:
            return
        try:
            if self._db_type == "oracle":
                self._pool.close()
            elif self._db_type == "postgresql":
                self._pool.closeall()
            # MySQL : pas de mÃ©thode closeall, les connexions expirent naturellement
            log.info("Pool de connexions fermÃ©.")
        except Exception as e:
            log.warning("Erreur Ã  la fermeture du pool : %s", e)

    @property
    def db_type(self) -> str:
        """Returns current database type (oracle/postgresql/mysql)."""
        return self._db_type


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# RÃ©solution du mot de passe
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
def _resolve_password(cfg: configparser.ConfigParser, section: str) -> str:
    """
    RÃ©sout le mot de passe avec prioritÃ© :
      1. Variable d'environnement rÃ©fÃ©rencÃ©e par 'password_env'
      2. Valeur directe 'password' dans le fichier .ini (dÃ©conseillÃ©)
    """
    import os

    env_var = cfg.get(section, "password_env", fallback="").strip()
    if env_var:
        pw = os.environ.get(env_var)
        if pw:
            log.info("Mot de passe lu depuis la variable d'environnement '%s'.", env_var)
            return pw
        log.warning(
            "Variable d'environnement '%s' absente â€” tentative avec 'password' du fichier ini.",
            env_var,
        )

    pw = cfg.get(section, "password", fallback="").strip()
    if pw:
        log.warning(
            " Mot de passe lu en clair depuis le fichier ini. "
            "PrÃ©fÃ©rez 'password_env = NOM_VAR_ENV' pointant sur une variable d'environnement."
        )
        return pw

    sys.exit(
        "Mot de passe introuvable. DÃ©finissez 'password_env = VOTRE_VAR' "
        "dans [database] du fichier de connexion, ou exportez la variable en shell."
    )

