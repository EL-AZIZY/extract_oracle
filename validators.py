"""
validators.py
-------------
Pre-flight validation checks.
"""

import logging
import shutil
from pathlib import Path

log = logging.getLogger(__name__)


class ValidationError(Exception):
    """Raised when pre-flight validation fails."""


def _compact_error_message(exc: Exception) -> str:
    """
    Returns first line only to avoid noisy multi-line SQL errors.
    """
    msg = str(exc).strip()
    if not msg:
        return exc.__class__.__name__
    return msg.splitlines()[0].strip()


def check_disk_space(
    output_path: Path,
    estimated_size_bytes: int,
    safety_factor: float = 2.0,
    min_free_gb: float = 1.0,
) -> None:
    """
    Validates sufficient disk space before extraction.
    """
    target_dir = output_path.parent
    target_dir.mkdir(parents=True, exist_ok=True)

    try:
        usage = shutil.disk_usage(target_dir)
    except OSError as e:
        log.warning("Could not check disk space: %s", e)
        return

    free_bytes = usage.free
    free_gb = free_bytes / (1024 ** 3)
    required_bytes = int(estimated_size_bytes * safety_factor)
    required_gb = required_bytes / (1024 ** 3)
    min_bytes = int(min_free_gb * (1024 ** 3))

    log.info(
        "Disk space check: %.2f GB free, ~%.2f GB required",
        free_gb,
        required_gb,
    )

    if free_bytes < max(required_bytes, min_bytes):
        raise ValidationError(
            f"Insufficient disk space. "
            f"Free: {free_gb:.2f} GB, Required: {max(required_gb, min_free_gb):.2f} GB. "
            f"Path: {target_dir}"
        )


def estimate_output_size(
    row_count: int,
    column_count: int,
    avg_field_size: int = 50,
    format: str = "csv",
) -> int:
    """
    Estimates output file size in bytes.
    """
    row_size = column_count * avg_field_size
    raw_size = row_count * row_size

    compression_ratios = {
        "csv": 1.0,
        "csv.gz": 0.15,
        "parquet": 0.25,
        "jsonl": 1.2,
    }

    ratio = compression_ratios.get(format, 1.0)
    return int(raw_size * ratio)


def check_database_connectivity(pool, timeout_seconds: int = 10) -> None:
    """
    Validates database connection before starting extraction.
    """
    del timeout_seconds
    db_type = getattr(pool, "db_type", "").lower()

    ping_sql = "SELECT 1"
    if db_type == "oracle":
        ping_sql = "SELECT 1 FROM DUAL"

    try:
        with pool.acquire() as conn:
            with conn.cursor() as cur:
                cur.execute(ping_sql)
                cur.fetchone()
        log.info("Database connectivity check: OK")
    except Exception as e:
        raise ValidationError(f"Database connectivity check failed: {_compact_error_message(e)}")


def check_table_exists(pool, table_name: str, schema: str = None) -> None:
    """
    Validates that the target table exists and is accessible.
    """
    full_name = f"{schema}.{table_name}" if schema else table_name

    try:
        with pool.acquire() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT * FROM {full_name} WHERE 1=0")
        log.info("Table access check for %s: OK", full_name)
    except Exception as e:
        raise ValidationError(f"Cannot access table {full_name}: {_compact_error_message(e)}")


def check_output_writable(output_path: Path) -> None:
    """
    Validates that we can write to the output location.
    """
    target_dir = output_path.parent

    try:
        target_dir.mkdir(parents=True, exist_ok=True)
        test_file = target_dir / ".write_test"
        test_file.write_text("test", encoding="utf-8")
        test_file.unlink()
        log.info("Output path writable: %s", target_dir)
    except OSError as e:
        raise ValidationError(f"Cannot write to output directory {target_dir}: {e}")
