"""
config_validator.py
-------------------
Comprehensive configuration validation.
"""

import configparser
import logging
from dataclasses import dataclass
from typing import List

log = logging.getLogger(__name__)


@dataclass
class ConfigError:
    """Configuration validation error."""

    section: str
    key: str
    message: str
    severity: str = "error"  # "error" or "warning"

    def __str__(self):
        icon = "ERROR" if self.severity == "error" else "WARN"
        return f"{icon} [{self.section}].{self.key}: {self.message}"


class ConfigValidator:
    """
    Validates configuration files before extraction.
    """

    VALID_FORMATS = {"csv", "csv.gz", "parquet", "jsonl", "db"}
    VALID_ENCODINGS = {"utf-8", "utf-8-sig", "latin-1", "cp1252", "ascii"}

    def __init__(self, job_cfg: configparser.ConfigParser):
        self.cfg = job_cfg
        self.errors: List[ConfigError] = []

    def validate_all(self) -> bool:
        """
        Run all validations.
        Returns True if no errors (warnings are OK).
        """
        self.errors.clear()

        self._validate_settings()
        self._validate_jobs()

        for error in self.errors:
            if error.severity == "error":
                log.error(str(error))
            else:
                log.warning(str(error))

        error_count = sum(1 for e in self.errors if e.severity == "error")
        if error_count > 0:
            log.error("Configuration validation failed: %d error(s)", error_count)
            return False

        log.info("Configuration validation passed")
        return True

    def _validate_settings(self) -> None:
        section = "settings"

        if not self.cfg.has_section(section):
            self.errors.append(ConfigError(section, "*", "Section [settings] is required"))
            return

        if self.cfg.has_option(section, "max_workers"):
            try:
                workers = int(self.cfg.get(section, "max_workers"))
                if workers < 1:
                    self.errors.append(ConfigError(section, "max_workers", f"Must be >= 1, got {workers}"))
                elif workers > 16:
                    self.errors.append(
                        ConfigError(
                            section,
                            "max_workers",
                            f"Value {workers} is very high - may overload database",
                            severity="warning",
                        )
                    )
            except ValueError:
                self.errors.append(ConfigError(section, "max_workers", "Must be an integer"))

    def _validate_jobs(self) -> None:
        reserved = {"settings", "csv", "database", "oracle"}
        jobs = [s for s in self.cfg.sections() if s.lower() not in reserved]

        if not jobs:
            self.errors.append(ConfigError("*", "*", "No job sections defined"))
            return

        for job in jobs:
            self._validate_job(job)

    def _validate_job(self, job: str) -> None:
        has_query = self.cfg.has_option(job, "query_file")
        has_table = self.cfg.has_option(job, "table")

        if not has_query and not has_table:
            self.errors.append(
                ConfigError(job, "query_file/table", "Either 'query_file' or 'table' must be specified")
            )

        if has_query and has_table:
            self.errors.append(
                ConfigError(
                    job,
                    "query_file/table",
                    "Both 'query_file' and 'table' specified - 'query_file' will be used",
                    severity="warning",
                )
            )

        if self.cfg.has_option(job, "format"):
            fmt = self.cfg.get(job, "format").lower().strip()
            if fmt not in self.VALID_FORMATS:
                self.errors.append(
                    ConfigError(job, "format", f"Invalid format '{fmt}'. Valid: {self.VALID_FORMATS}")
                )

        if self.cfg.has_option(job, "chunk_size"):
            try:
                chunk = int(self.cfg.get(job, "chunk_size"))
                if chunk < 100:
                    self.errors.append(
                        ConfigError(
                            job, "chunk_size", f"Value {chunk} is very small - may be slow", severity="warning"
                        )
                    )
                elif chunk > 100000:
                    self.errors.append(
                        ConfigError(
                            job,
                            "chunk_size",
                            f"Value {chunk} is large - may cause memory issues",
                            severity="warning",
                        )
                    )
            except ValueError:
                self.errors.append(ConfigError(job, "chunk_size", "Must be an integer"))

        if self.cfg.has_option(job, "encoding"):
            enc = self.cfg.get(job, "encoding").lower().strip()
            if enc not in self.VALID_ENCODINGS:
                self.errors.append(
                    ConfigError(
                        job,
                        "encoding",
                        f"Unknown encoding '{enc}'. Common: {self.VALID_ENCODINGS}",
                        severity="warning",
                    )
                )

        if self.cfg.has_option(job, "output_file"):
            out_file = self.cfg.get(job, "output_file")
            if "/" in out_file or "\\" in out_file:
                self.errors.append(
                    ConfigError(
                        job,
                        "output_file",
                        "Should be a filename only. Use 'output_dir' for directory",
                        severity="warning",
                    )
                )

        if self.cfg.has_option(job, "format"):
            fmt = self.cfg.get(job, "format").lower().strip()
            if fmt == "db":
                for required in ("target_dsn", "target_table"):
                    if not self.cfg.has_option(job, required):
                        self.errors.append(ConfigError(job, required, "Required for format='db'"))


def validate_config(job_cfg: configparser.ConfigParser) -> bool:
    """Convenience function to validate configuration."""
    validator = ConfigValidator(job_cfg)
    return validator.validate_all()

