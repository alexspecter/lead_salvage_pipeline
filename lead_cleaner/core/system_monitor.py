import psutil
import platform
from lead_cleaner.logging.logger import PipelineLogger

class SystemMonitor:
    def __init__(self, logger: PipelineLogger):
        self.logger = logger
        
    def log_baseline(self):
        vm = psutil.virtual_memory()
        
        info = {
            "system": platform.system(),
            "machine": platform.machine(),
            "ram_total_gb": round(vm.total / (1024**3), 2),
            "ram_available_gb": round(vm.available / (1024**3), 2),
            "ram_percent": vm.percent,
            "cpu_cores": psutil.cpu_count(logical=False),
            "logical_cpus": psutil.cpu_count(logical=True)
        }
        
        self.logger.log_event("SYSTEM", "BASELINE_CHECK", reason="Startup diagnostics", after=str(info))
        
        # Check hard constraint (M4 Max has 64GB+, usually)
        # We don't fail here, just warn if low spec for 70B model
        if info["ram_total_gb"] < 32:
            self.logger.log_event("SYSTEM", "WARNING", reason="Low RAM detected. 70B model may crash.")
