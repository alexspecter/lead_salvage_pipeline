import sys
import os
import subprocess
import traceback

from lead_cleaner.logging.logger import PipelineLogger
from lead_cleaner.core.system_monitor import SystemMonitor
from lead_cleaner.core.validator import DataValidator
from lead_cleaner.utils.uuid import generate_run_id
from lead_cleaner.config import DEFAULT_OUTPUT_DIR

# Phase Runners
from lead_cleaner.phase1_deterministic.runner import Phase1Runner
from lead_cleaner.phase2_semantic.runner import Phase2Runner
from lead_cleaner.phase3_merge.runner import Phase3Runner
from lead_cleaner.exceptions import (
    LeadCleanerError,
    VerificationError,
    SecurityViolationError,
    MalwareDetectedError,
)
from lead_cleaner.core.security import run_security_checks
from lead_cleaner.utils.rejection_cache import RejectionCache


class Orchestrator:
    def __init__(self):
        self.run_id = generate_run_id()
        self.logger = PipelineLogger(self.run_id)
        self.monitor = SystemMonitor(self.logger)
        self.rejection_cache = RejectionCache(
            DEFAULT_OUTPUT_DIR, self.run_id, self.logger
        )

    def run_pipeline(
        self,
        input_file: str,
        output_dir: str = DEFAULT_OUTPUT_DIR,
        health_check: bool = False,
    ):
        print(f"--- Starting Pipeline Run {self.run_id} ---")
        if health_check:
            print("[MODE] HEALTH CHECK / DRY RUN - No files will be written.")

        try:
            # Update rejection cache if output_dir is different
            if output_dir != DEFAULT_OUTPUT_DIR:
                self.rejection_cache = RejectionCache(
                    output_dir, self.run_id, self.logger
                )

            # 1. System Check
            self.monitor.log_baseline()

            # 2. Phase 0: Security Scan (multi-layer)
            self.logger.log_event(
                "ORCHESTRATOR", "PHASE_Start", reason="Phase 0: Security Scan"
            )
            # Wrap security checks to move rejected files to cache
            try:
                sanitized_file = run_security_checks(input_file, self.logger)
            except (SecurityViolationError, MalwareDetectedError) as e:
                # Security module deletes the file. We just log it to rejection cache.
                self.rejection_cache.log_security_rejection(
                    os.path.basename(input_file), str(e)
                )
                if health_check:
                    print(
                        f"\n[HEALTH REPORT] CRITICAL: Security Check Failed - {str(e)}"
                    )
                    return
                raise

            # 3. Phase 0: Validate Input (using sanitized file)
            self.logger.log_event(
                "ORCHESTRATOR", "PHASE_Start", reason="Phase 0: Validation"
            )
            validator = DataValidator(self.logger)
            df = validator.validate_input(sanitized_file)

            # 4. Phase 1: Deterministic
            self.logger.log_event(
                "ORCHESTRATOR", "PHASE_Start", reason="Phase 1: Deterministic"
            )
            p1_runner = Phase1Runner(self.logger, self.run_id, self.rejection_cache)
            rows = p1_runner.process(df)

            # 5. CRITICAL GATE: Unit Tests
            self._run_critical_gate_tests()

            # HEALTH CHECK INTERCEPT
            if health_check:
                from lead_cleaner.types import RowStatus

                total = len(rows)
                clean = sum(1 for r in rows if r["status"] == RowStatus.CLEAN)
                ai = sum(1 for r in rows if r["status"] == RowStatus.AI_REQUIRED)
                rejected = sum(1 for r in rows if r["status"] == RowStatus.REJECTED)

                print("\n" + "=" * 40)
                print("       DATA HEALTH REPORT       ")
                print("=" * 40)
                print(f"Total Rows:       {total}")
                print(f"Phase 1 Clean:    {clean} ({clean / total:.1%} - Ready)")
                print(f"AI Required:      {ai} ({ai / total:.1%} - Needs Phase 2)")
                print(f"Rejected/Dupes:   {rejected} ({rejected / total:.1%})")
                print("-" * 40)

                if ai > (total * 0.5):
                    print("⚠️  High AI Usage Predicted (>50% of rows need AI)")
                else:
                    print("✅ Data Quality Looks Good")

                print("\nReasoning for AI Routing (Sample):")
                ai_rows = [r for r in rows if r["status"] == RowStatus.AI_REQUIRED][:5]
                for r in ai_rows:
                    print(
                        f" - Row {r['raw_data'].get('_line_number', '?')}: {r.get('failure_reason', 'Semantic Processing Required')}"
                    )

                print("=" * 40 + "\n")
                return

            # 6. Phase 2: Semantic (AI)
            self.logger.log_event(
                "ORCHESTRATOR", "PHASE_Start", reason="Phase 2: Semantic"
            )
            p2_runner = Phase2Runner(self.logger, self.run_id, self.rejection_cache)
            rows = p2_runner.process(rows)

            # 7. Phase 3: Merge & Verify
            self.logger.log_event(
                "ORCHESTRATOR", "PHASE_Start", reason="Phase 3: Merge/Verify"
            )
            p3_runner = Phase3Runner(self.logger, self.run_id, self.rejection_cache)
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
        self.logger.log_event(
            "ORCHESTRATOR", "GATE_CHECK", reason="Running Phase 1 Tests"
        )
        print(">>> Executing Critical Gate: Phase 1 Tests...")

        # We run via subprocess to ensure clean state
        cmd = [sys.executable, "tests/test_phase1.py"]

        # Ensure PYTHONPATH includes CWD
        env = dict(sys.modules["os"].environ)
        cwd = os.getcwd()
        if "PYTHONPATH" in env:
            env["PYTHONPATH"] = f"{cwd}{os.pathsep}{env['PYTHONPATH']}"
        else:
            env["PYTHONPATH"] = cwd

        result = subprocess.run(cmd, env=env, capture_output=True, text=True)

        if result.returncode != 0:
            error_msg = f"Unit Test Gate Failed:\n{result.stderr}"
            self.logger.log_event("ORCHESTRATOR", "GATE_FAILED", reason=error_msg)
            raise VerificationError(
                "Critical Gate Failed: Phase 1 logic is broken. Aborting Phase 2."
            )

        self.logger.log_event("ORCHESTRATOR", "GATE_PASSED")
        print(">>> Gate Passed.")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python -m lead_cleaner.core.orchestrator <input_file>")
        sys.exit(1)

    orch = Orchestrator()
    orch.run_pipeline(sys.argv[1])
