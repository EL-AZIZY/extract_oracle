"""
cleanup.py
----------
Utilities to clean orphan checkpoint files.
"""

from __future__ import annotations

import time
from pathlib import Path


def cleanup_orphan_checkpoints(log_dir: Path, threshold_hours: int = 24) -> int:
    """
    Remove stale *.checkpoint files under the project output tree.

    Args:
        log_dir: kept for API compatibility with existing caller.
        threshold_hours: delete only files older than this age.
    """
    del log_dir  # not used, but kept to preserve call signature

    root = Path(".")
    now = time.time()
    cutoff = now - (threshold_hours * 3600)
    removed = 0

    for checkpoint in root.rglob("*.checkpoint"):
        try:
            if checkpoint.stat().st_mtime <= cutoff:
                checkpoint.unlink(missing_ok=True)
                removed += 1
        except OSError:
            continue

    return removed

