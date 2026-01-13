import os
import sys
import shutil

# Ensure we can import
sys.path.append(os.getcwd())

from lead_cleaner.phase0_setup.generator import GarbageGenerator
from lead_cleaner.core.orchestrator import Orchestrator

def verify_e2e():
    print(">>> E2E VERIFICATION START >>>")
    
    # 1. Setup
    input_file = "input/e2e_test.csv"
    output_dir = "output/e2e_test_run"
    
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
        
    print(f"Generating 50 rows to {input_file}")
    gen = GarbageGenerator(seed=999)
    gen.generate_csv(input_file, count=50)
    
    # 2. Run Orchestrator
    # We will invoke the class directly to avoid subprocess complexity for this test,
    # but the orchestrator internals invoke subprocess for the unit test gate.
    
    print("\n>>> SCRIPT: Running Orchestrator...")
    orch = Orchestrator()
    try:
        orch.run_pipeline(input_file, output_dir=output_dir)
    except SystemExit as e:
        if e.code != 0:
            print("[FAIL] Orchestrator exited with error code")
            sys.exit(1)
        # 0 is fine (though orchestrator typically doesn't exit(0) unless success print)
        
    # 3. Validation
    # Check outputs exist
    run_id = orch.run_id
    final_csv = os.path.join(output_dir, f"final_output_{run_id}.csv")
    reject_csv = os.path.join(output_dir, f"reject_store_{run_id}.csv")
    
    if not os.path.exists(final_csv):
        print(f"[FAIL] Final CSV missing: {final_csv}")
        sys.exit(1)
        
    if not os.path.exists(reject_csv):
        print(f"[FAIL] Reject CSV missing: {reject_csv}")
        sys.exit(1)
        
    print(f"[PASS] Outputs found: {final_csv}, {reject_csv}")
    print(">>> E2E VERIFICATION COMPLETE >>>")

if __name__ == "__main__":
    verify_e2e()
