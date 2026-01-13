import psutil
import os
import gc
from lead_cleaner.config import MEMORY_CAP_PERCENT
from lead_cleaner.exceptions import MemoryLimitError
from lead_cleaner.logging.logger import PipelineLogger

class MemoryGuard:
    def __init__(self, logger: PipelineLogger):
        self.logger = logger
        self.cap_percent = MEMORY_CAP_PERCENT

    def check_memory(self):
        """
        Checks current system memory usage.
        Raises MemoryLimitError if usage > MEMORY_CAP_PERCENT.
        """
        mem = psutil.virtual_memory()
        usage_percent = mem.percent / 100.0
        
        if usage_percent > self.cap_percent:
            msg = f"CRITICAL MEMORY WARNING: Usage at {mem.percent}% (Limit: {self.cap_percent*100}%)"
            self.logger.log_event(
                phase="MEMORY", 
                action="LIMIT_EXCEEDED", 
                reason=msg,
                before=f"{mem.used / (1024**3):.2f} GB used"
            )
            
            # Force GC attempt before crashing
            gc.collect()
            
            # Re-check
            mem = psutil.virtual_memory()
            if (mem.percent / 100.0) > self.cap_percent:
                 raise MemoryLimitError(msg)
            else:
                 self.logger.log_event("MEMORY", "GC_RECLAIMED", reason="GC reduced memory below threshold")
                 return mem.percent / 100.0
                 
        return usage_percent

    def force_gc(self):
        gc.collect()
