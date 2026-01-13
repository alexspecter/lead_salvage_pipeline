import sys
import os
import subprocess
import traceback
from typing import Optional

from lead_cleaner.logging.logger import PipelineLogger
from lead_cleaner.core.system_monitor import SystemMonitor
from lead_cleaner.core.validator import DataValidator
from lead_cleaner.utils.uuid import generate_run_id
from lead_cleaner.config import DEFAULT_OUTPUT_DIR

# Phase Runners
from lead_cleaner.phase1_deterministic.runner import Phase1Runner
from lead_cleaner.phase2_semantic.runner import Phase2Runner
from lead_cleaner.phase3_merge.runner import Phase3Runner
from lead_cleaner.exceptions import LeadCleanerError, VerificationError
from lead_cleaner.core.security import scan_and_secure

class Orchestrator:
    def __init__(self):
        self.run_id = generate_run_id()
        self.logger = PipelineLogger(self.run_id)
        self.monitor = SystemMonitor(self.logger)
        
    def run_pipeline(self, input_csv: str, output_dir: str = DEFAULT_OUTPUT_DIR):
        print(f"--- Starting Pipeline Run {self.run_id} ---")
        try:
            # 1. System Check
            self.monitor.log_baseline()
            
            # 2. Phase 0: Security Scan
            self.logger.log_event("ORCHESTRATOR", "PHASE_Start", reason="Phase 0: Security Scan")
            sanitized_csv = scan_and_secure(input_csv, self.logger)
            
            # 3. Phase 0: Validate Input (using sanitized file)
            self.logger.log_event("ORCHESTRATOR", "PHASE_Start", reason="Phase 0: Validation")
            validator = DataValidator(self.logger)
            df = validator.validate_csv(sanitized_csv)
            
            # 4. Phase 1: Deterministic
            self.logger.log_event("ORCHESTRATOR", "PHASE_Start", reason="Phase 1: Deterministic")
            p1_runner = Phase1Runner(self.logger, self.run_id)
            rows = p1_runner.process(df)
            
            # 5. CRITICAL GATE: Unit Tests
            self._run_critical_gate_tests()
            
            # 6. Phase 2: Semantic (AI)
            self.logger.log_event("ORCHESTRATOR", "PHASE_Start", reason="Phase 2: Semantic")
            p2_runner = Phase2Runner(self.logger, self.run_id)
            rows = p2_runner.process(rows)
            
            # 7. Phase 3: Merge & Verify
            self.logger.log_event("ORCHESTRATOR", "PHASE_Start", reason="Phase 3: Merge/Verify")
            p3_runner = Phase3Runner(self.logger, self.run_id)
            p3_runner.process(rows, output_dir)
            
            print(f"--- SUCCESS: Run {self.run_id} Completed ---")
            print(f"Logs: {self.logger.log_file}")
            
        except LeadCleanerError as e:
            msg = f"Pipeline Failed: {str(e)}"
            self.logger.log_error("ORCHESTRATOR", "FATAL_ERROR", e)
            print(f"\n[FATAL] {msg}")
            sys.exit(1)
            
        except Exception as e:
            msg = f"Unexpected Error: {str(e)}"
            self.logger.log_error("ORCHESTRATOR", "CRASH", e)
            traceback.print_exc()
            sys.exit(1)

    def _run_critical_gate_tests(self):
        """Runs Phase 1 unit tests. Aborts if failed."""
        self.logger.log_event("ORCHESTRATOR", "GATE_CHECK", reason="Running Phase 1 Tests")
        print(">>> Executing Critical Gate: Phase 1 Tests...")
        
        # We run via subprocess to ensure clean state
        cmd = [sys.executable, "tests/test_phase1.py"]
        
        # Ensure PYTHONPATH includes CWD
        env = dict(sys.modules['os'].environ)
        cwd = os.getcwd()
        if "PYTHONPATH" in env:
            env["PYTHONPATH"] = f"{cwd}{os.pathsep}{env['PYTHONPATH']}"
        else:
            env["PYTHONPATH"] = cwd
        
        result = subprocess.run(cmd, env=env, capture_output=True, text=True)
        
        if result.returncode != 0:
            error_msg = f"Unit Test Gate Failed:\n{result.stderr}"
            self.logger.log_event("ORCHESTRATOR", "GATE_FAILED", reason=error_msg)
            raise VerificationError("Critical Gate Failed: Phase 1 logic is broken. Aborting Phase 2.")
            
        self.logger.log_event("ORCHESTRATOR", "GATE_PASSED")
        print(">>> Gate Passed.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python -m lead_cleaner.core.orchestrator <input_csv>")
        sys.exit(1)
        
    orch = Orchestrator()
    orch.run_pipeline(sys.argv[1])
