import psutil
import gc
import time
from lead_cleaner.config import MEMORY_CAP_PERCENT
from lead_cleaner.exceptions import MemoryLimitError
from lead_cleaner.logging.logger import PipelineLogger

# Memory thresholds
WARNING_THRESHOLD = 0.90  # 90% - trigger GC and pause
CRITICAL_THRESHOLD = 0.95  # 95% - terminate to protect SSD


class MemoryGuard:
    def __init__(self, logger: PipelineLogger):
        self.logger = logger
        self.cap_percent = MEMORY_CAP_PERCENT

    def check_memory(self) -> float:
        """
        Checks current system memory usage.
        - At 90%: Trigger GC, clear MLX cache, pause 5 seconds
        - At 95%: Raise MemoryLimitError to terminate (Zero Swap Policy)

        Returns current memory usage as a float (0.0 - 1.0).
        """
        mem = psutil.virtual_memory()
        usage_percent = mem.percent / 100.0

        # Level 1: Warning threshold (90%)
        if usage_percent > WARNING_THRESHOLD:
            self.logger.log_event(
                phase="MEMORY",
                action="WARNING_THRESHOLD",
                reason=f"Memory at {mem.percent:.1f}% - triggering GC and pause",
                before=f"{mem.used / (1024**3):.2f} GB used",
            )

            # Force garbage collection
            gc.collect()

            # Attempt to clear MLX cache if available
            self._clear_mlx_cache()

            # Pause to allow memory to settle
            time.sleep(5)

            # Re-check after pause
            mem = psutil.virtual_memory()
            usage_percent = mem.percent / 100.0

            self.logger.log_event(
                phase="MEMORY",
                action="POST_GC_CHECK",
                reason=f"After GC: {mem.percent:.1f}%",
                after=f"{mem.used / (1024**3):.2f} GB used",
            )

        # Level 2: Critical threshold (95%) - TERMINATE
        if usage_percent > CRITICAL_THRESHOLD:
            msg = f"CRITICAL: Memory at {mem.percent:.1f}% exceeds {CRITICAL_THRESHOLD * 100}%. Terminating to protect SSD (Zero Swap Policy)."
            self.logger.log_event(
                phase="MEMORY",
                action="CRITICAL_LIMIT_EXCEEDED",
                reason=msg,
                before=f"{mem.used / (1024**3):.2f} GB used",
            )
            raise MemoryLimitError(msg)

        return usage_percent

    def _clear_mlx_cache(self):
        """Attempt to clear MLX memory cache if available."""
        try:
            import mlx.core as mx

            mx.clear_cache()  # Updated from deprecated mx.metal.clear_cache()
            self.logger.log_event("MEMORY", "MLX_CACHE_CLEARED")
        except (ImportError, AttributeError):
            pass  # MLX not available or no cache method

    def force_gc(self):
        gc.collect()
        self._clear_mlx_cache()
