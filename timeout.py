"""
timeout.py
----------
Query timeout management for database operations.
"""

import logging
import signal
import threading
from contextlib import contextmanager

log = logging.getLogger(__name__)


class QueryTimeoutError(Exception):
    """Raised when a database query exceeds the configured timeout."""


@contextmanager
def query_timeout(seconds: int, query_preview: str = ""):
    """
    Context manager that raises QueryTimeoutError after `seconds`.
    Works on Unix-like systems only (signal-based).
    """
    if seconds <= 0:
        yield
        return

    if not hasattr(signal, "SIGALRM"):
        log.warning("Query timeout not supported on this platform")
        yield
        return

    if threading.current_thread() is not threading.main_thread():
        # signal.alarm works only in main thread
        log.debug("Signal timeout skipped outside main thread")
        yield
        return

    def _timeout_handler(signum, frame):
        del signum, frame
        raise QueryTimeoutError(
            f"Query timed out after {seconds}s. Query: {query_preview[:100]}..."
        )

    old_handler = signal.signal(signal.SIGALRM, _timeout_handler)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old_handler)


def get_db_level_timeout_sql(db_type: str, seconds: int):
    """
    Returns database-specific timeout SQL to execute before queries.
    """
    if seconds <= 0:
        return None
    if db_type == "oracle":
        return None
    if db_type == "postgresql":
        return f"SET statement_timeout = '{seconds}s';"
    if db_type == "mysql":
        return f"SET SESSION max_execution_time = {seconds * 1000};"
    return None

