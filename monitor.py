"""
monitor.py
----------
Resource monitoring during extraction.
"""

import logging
import threading
import time
from dataclasses import dataclass
from typing import Optional

log = logging.getLogger(__name__)

try:
    import psutil

    _PSUTIL_AVAILABLE = True
except ImportError:
    _PSUTIL_AVAILABLE = False
    log.warning("psutil not installed - resource monitoring disabled")


@dataclass
class ResourceSnapshot:
    """Point-in-time resource usage."""

    timestamp: float
    memory_percent: float = 0.0
    memory_mb: float = 0.0
    cpu_percent: float = 0.0
    open_files: int = 0
    disk_write_mb: float = 0.0


@dataclass
class ResourceThresholds:
    """Thresholds that trigger warnings or stops."""

    memory_warning_percent: float = 75.0
    memory_critical_percent: float = 90.0
    cpu_warning_percent: float = 85.0
    disk_write_rate_warning_mb_s: float = 100.0


class ResourceMonitor:
    """
    Background thread that monitors system resources.
    """

    def __init__(
        self,
        thresholds: ResourceThresholds = None,
        interval_seconds: float = 5.0,
        on_critical: callable = None,
    ):
        self.thresholds = thresholds or ResourceThresholds()
        self.interval = interval_seconds
        self.on_critical = on_critical

        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._snapshots: list[ResourceSnapshot] = []
        self._lock = threading.Lock()
        self._process = psutil.Process() if _PSUTIL_AVAILABLE else None

    def start(self) -> None:
        """Start background monitoring."""
        if not _PSUTIL_AVAILABLE:
            log.warning("Resource monitoring disabled (psutil not installed)")
            return

        self._running = True
        self._thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._thread.start()
        log.info("Resource monitoring started (interval: %.1fs)", self.interval)

    def stop(self) -> None:
        """Stop background monitoring."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=2.0)
            self._thread = None
        log.info("Resource monitoring stopped")

    def _monitor_loop(self) -> None:
        """Background monitoring loop."""
        while self._running:
            try:
                snapshot = self._take_snapshot()

                with self._lock:
                    self._snapshots.append(snapshot)
                    if len(self._snapshots) > 1000:
                        self._snapshots = self._snapshots[-500:]

                self._check_thresholds(snapshot)

            except Exception as e:
                log.debug("Monitoring error: %s", e)

            time.sleep(self.interval)

    def _take_snapshot(self) -> ResourceSnapshot:
        """Capture current resource usage."""
        mem = self._process.memory_info()

        return ResourceSnapshot(
            timestamp=time.time(),
            memory_percent=self._process.memory_percent(),
            memory_mb=mem.rss / (1024 * 1024),
            cpu_percent=self._process.cpu_percent(),
            open_files=len(self._process.open_files()),
        )

    def _check_thresholds(self, snapshot: ResourceSnapshot) -> None:
        """Check if any thresholds are exceeded."""
        t = self.thresholds

        if snapshot.memory_percent >= t.memory_critical_percent:
            log.critical(
                "CRITICAL: Memory usage at %.1f%% (%.0f MB)",
                snapshot.memory_percent,
                snapshot.memory_mb,
            )
            if self.on_critical:
                self.on_critical("memory", snapshot)

        elif snapshot.memory_percent >= t.memory_warning_percent:
            log.warning(
                "High memory usage: %.1f%% (%.0f MB)",
                snapshot.memory_percent,
                snapshot.memory_mb,
            )

        if snapshot.cpu_percent >= t.cpu_warning_percent:
            log.warning("High CPU usage: %.1f%%", snapshot.cpu_percent)

    def get_stats(self) -> dict:
        """Get summary statistics from monitoring session."""
        with self._lock:
            if not self._snapshots:
                return {}

            mem_values = [s.memory_mb for s in self._snapshots]
            cpu_values = [s.cpu_percent for s in self._snapshots]

            return {
                "duration_seconds": (self._snapshots[-1].timestamp - self._snapshots[0].timestamp),
                "memory_mb_avg": sum(mem_values) / len(mem_values),
                "memory_mb_max": max(mem_values),
                "cpu_percent_avg": sum(cpu_values) / len(cpu_values),
                "cpu_percent_max": max(cpu_values),
                "samples": len(self._snapshots),
            }

    def get_current(self) -> Optional[ResourceSnapshot]:
        """Get most recent snapshot."""
        with self._lock:
            return self._snapshots[-1] if self._snapshots else None

