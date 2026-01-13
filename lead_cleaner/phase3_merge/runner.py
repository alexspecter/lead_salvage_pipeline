import pandas as pd
import os
from typing import List

from lead_cleaner.types import LeadRow, RowStatus
from lead_cleaner.logging.logger import PipelineLogger
from lead_cleaner.phase3_merge.verifier import Verifier
from lead_cleaner.config import DEFAULT_OUTPUT_DIR
from lead_cleaner.constants import PHASE_3_MERGE

class Phase3Runner:
    def __init__(self, logger: PipelineLogger, run_id: str):
        self.logger = logger
        self.run_id = run_id
        self.verifier = Verifier(logger)
        
    def process(self, all_rows: List[LeadRow], output_dir: str = DEFAULT_OUTPUT_DIR):
        self.logger.log_event(PHASE_3_MERGE, "START")
        
        # Split
        final_rows = [r for r in all_rows if r["status"] == RowStatus.CLEAN]
        rejected_rows = [r for r in all_rows if r["status"] == RowStatus.REJECTED]
        
        # Verify
        # Note: If any are still AI_REQUIRED, that's an error state for Phase 3 entry, 
        # but technically they count as "not clean".
        # We should treat any non-CLEAN/non-REJECTED as REJECTED (UNKNOWN) or fail?
        # Directive says "Merge P1_CLEAN and AI_CLEAN -> final".
        # If status is still AI_REQUIRED, it means Phase 2 didn't process it?
        # Let's check for stragglers.
        
        stragglers = [r for r in all_rows if r["status"] not in (RowStatus.CLEAN, RowStatus.REJECTED)]
        if stragglers:
             self.logger.log_event(PHASE_3_MERGE, "WARNING", reason=f"{len(stragglers)} rows have unfinished status. Marking REJECTED.")
             for r in stragglers:
                 r["status"] = RowStatus.REJECTED
                 r["failure_reason"] = "UNFINISHED_PROCESSING"
                 rejected_rows.append(r)

        self.verifier.verify_outputs(all_rows, final_rows, rejected_rows)
        
        # Write Outputs
        os.makedirs(output_dir, exist_ok=True)
        
        # Flatten for CSV
        # We need to decide what schema to write.
        # "final_output.csv" -> clean_data + metadata?
        
        final_df = self._flatten_rows(final_rows, include_raw=False)
        rejected_df = self._flatten_rows(rejected_rows, include_raw=True)
        
        final_path = os.path.join(output_dir, f"final_output_{self.run_id}.csv")
        rejected_path = os.path.join(output_dir, f"reject_store_{self.run_id}.csv")
        
        final_df.to_csv(final_path, index=False)
        rejected_df.to_csv(rejected_path, index=False)
        
        self.logger.log_event(PHASE_3_MERGE, "COMPLETE", reason=f"Written to {output_dir}")
        
    def _flatten_rows(self, rows: List[LeadRow], include_raw: bool = False) -> pd.DataFrame:
        flat_data = []
        for r in rows:
            item = {
                "row_id": r["row_id"],
                "run_id": r["run_id"],
                "status": r["status"],
                "confidence_score": r["confidence_score"],
                "failure_reason": r["failure_reason"]
            }
            # Add clean data
            item.update(r["clean_data"])
            
            if include_raw:
                 # Prefix raw for clarity
                 for k, v in r["raw_data"].items():
                     item[f"raw_{k}"] = v
                     
            flat_data.append(item)
            
        return pd.DataFrame(flat_data)
