import sys
import os
import pandas as pd
from typing import List

# Add path
sys.path.append(os.getcwd())

from lead_cleaner.phase1_deterministic.runner import Phase1Runner
from lead_cleaner.logging.logger import PipelineLogger
from lead_cleaner.types import RowStatus

def verify_phase1():
    input_file = "input/test_phase0.csv"
    if not os.path.exists(input_file):
        print(f"Error: {input_file} not found. Run Phase 0 verification first.")
        sys.exit(1)
        
    print(f"--- Loading {input_file} ---")
    df = pd.read_csv(input_file)
    print(f"Loaded {len(df)} rows")
    
    logger = PipelineLogger(run_id="test_run_p1")
    runner = Phase1Runner(logger, run_id="test_run_p1")
    
    print("--- Running Phase 1 ---")
    rows = runner.process(df)
    
    print(f"--- Processed {len(rows)} rows ---")
    
    clean_count = 0
    ai_count = 0
    rejected_count = 0
    
    for row in rows:
        status = row["status"]
        if status == RowStatus.CLEAN:
            clean_count += 1
        elif status == RowStatus.AI_REQUIRED:
            ai_count += 1
        elif status == RowStatus.REJECTED:
            rejected_count += 1
            
        # Verify deterministic ID
        assert row["row_id"] is not None
        
        # Verify clean data population attempt
        assert isinstance(row["clean_data"], dict)
        
    print(f"Stats:\nCLEAN: {clean_count}\nAI_REQUIRED: {ai_count}\nREJECTED: {rejected_count}")
    
    # We expect some rejections due to duplicates in the garbage generator
    # We expect some CLEAN if data was lucky, but mostly AI_REQUIRED/CLEAN mix
    
    assert len(rows) == len(df)
    print("Row count preserved. Success.")

if __name__ == "__main__":
    verify_phase1()
