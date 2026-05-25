"""
preflight_validator.py
----------------------
Validation pre-execution pour extract_oracle_v2.
"""

import configparser
from pathlib import Path

from query_loader import load as load_query
from settings import get_job_names, get_job_output_dir
from validators import (
    ValidationError,
    check_database_connectivity,
    check_output_writable,
    check_table_exists,
)


def run_validation(job_cfg: configparser.ConfigParser,
                   queries_dir: Path,
                   config_path: str,
                   pool=None) -> tuple[bool, list[str]]:
    checks: list[str] = []
    ok = True
    jobs = get_job_names(job_cfg)

    if pool is not None:
        try:
            check_database_connectivity(pool)
            checks.append("[database] connectivite OK")
        except ValidationError as exc:
            checks.append(f"[database] connectivite KO: {exc}")
            ok = False

    for job in jobs:
        try:
            query = load_query(job_cfg, job, queries_dir)
            if not query.strip():
                checks.append(f"[{job}] requete vide")
                ok = False
            else:
                checks.append(f"[{job}] requete OK")
        except Exception as exc:
            checks.append(f"[{job}] erreur requete: {exc}")
            ok = False

        path = get_job_output_dir(job_cfg, job, config_path)
        try:
            check_output_writable(path / ".validate.tmp")
            checks.append(f"[{job}] output_dir OK: {path}")
        except Exception as exc:
            checks.append(f"[{job}] output_dir KO: {path} ({exc})")
            ok = False

        if pool is not None and job_cfg.has_option(job, "table"):
            table_name = job_cfg.get(job, "table").strip()
            schema = job_cfg.get(job, "schema", fallback="").strip() or None
            try:
                check_table_exists(pool, table_name=table_name, schema=schema)
                full_name = f"{schema}.{table_name}" if schema else table_name
                checks.append(f"[{job}] table OK: {full_name}")
            except ValidationError as exc:
                checks.append(f"[{job}] table KO: {exc}")
                ok = False

    return ok, checks
