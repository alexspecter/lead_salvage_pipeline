"""
Phase 3 Runner - Merge & Verify

Handles:
1. Splitting rows into final (CLEAN) and rejected
2. Verification of outputs
3. Writing CSV files with dynamic field preservation
"""

import pandas as pd
import os
from typing import List, Dict, Any, Set

from lead_cleaner.types import LeadRow, RowStatus
from lead_cleaner.logging.logger import PipelineLogger
from lead_cleaner.phase3_merge.verifier import Verifier
from lead_cleaner.config import DEFAULT_OUTPUT_DIR, PRESERVE_ALL_FIELDS


PHASE_3_MERGE = "PHASE_3"


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
        
        # Handle stragglers (rows still in AI_REQUIRED state)
        stragglers = [r for r in all_rows if r["status"] not in (RowStatus.CLEAN, RowStatus.REJECTED)]
        if stragglers:
            self.logger.log_event(
                PHASE_3_MERGE, 
                "WARNING", 
                reason=f"{len(stragglers)} rows have unfinished status. Marking REJECTED."
            )
            for r in stragglers:
                r["status"] = RowStatus.REJECTED
                r["failure_reason"] = "UNFINISHED_PROCESSING"
                rejected_rows.append(r)

        self.verifier.verify_outputs(all_rows, final_rows, rejected_rows)
        
        # Write Outputs
        os.makedirs(output_dir, exist_ok=True)
        
        # Flatten for CSV with dynamic field support
        final_df = self._flatten_rows(final_rows, include_raw=False)
        rejected_df = self._flatten_rows(rejected_rows, include_raw=True)
        
        final_path = os.path.join(output_dir, f"final_output_{self.run_id}.csv")
        rejected_path = os.path.join(output_dir, f"reject_store_{self.run_id}.csv")
        
        final_df.to_csv(final_path, index=False)
        rejected_df.to_csv(rejected_path, index=False)
        
        self.logger.log_event(PHASE_3_MERGE, "COMPLETE", reason=f"Written to {output_dir}")
        
    def _flatten_rows(self, rows: List[LeadRow], include_raw: bool = False) -> pd.DataFrame:
        """
        Flattens LeadRow objects to a DataFrame.
        
        With PRESERVE_ALL_FIELDS enabled, all clean_data fields are included
        (not just the hardcoded ones).
        
        Args:
            rows: List of LeadRow dictionaries
            include_raw: Whether to include raw_data with "raw_" prefix
            
        Returns:
            Flattened DataFrame
        """
        if not rows:
            return pd.DataFrame()
        
        flat_data = []
        
        # Collect all possible clean_data keys for consistent columns
        all_clean_keys: Set[str] = set()
        if PRESERVE_ALL_FIELDS:
            for r in rows:
                all_clean_keys.update(r.get("clean_data", {}).keys())
        
        for r in rows:
            # Base metadata columns
            item: Dict[str, Any] = {
                "row_id": r["row_id"],
                "run_id": r["run_id"],
                "status": r["status"].value if hasattr(r["status"], 'value') else r["status"],
                "confidence_score": r["confidence_score"],
                "failure_reason": r["failure_reason"].value if hasattr(r.get("failure_reason"), 'value') else r.get("failure_reason")
            }
            
            # Add ALL clean data fields
            clean_data = r.get("clean_data", {})
            for key in all_clean_keys:
                item[key] = clean_data.get(key)
            
            # Also add any keys that might be in this row but not in all_clean_keys
            for key, value in clean_data.items():
                if key not in item:
                    item[key] = value
            
            # Include raw data for rejected rows (for debugging/review)
            if include_raw:
                for k, v in r.get("raw_data", {}).items():
                    item[f"raw_{k}"] = v
                    
            flat_data.append(item)
        
        df = pd.DataFrame(flat_data)
        
        # Reorder columns: metadata first, then clean fields, then raw
        metadata_cols = ["row_id", "run_id", "status", "confidence_score", "failure_reason"]
        clean_cols = sorted([c for c in df.columns if c not in metadata_cols and not c.startswith("raw_")])
        raw_cols = sorted([c for c in df.columns if c.startswith("raw_")])
        
        ordered_cols = metadata_cols + clean_cols + raw_cols
        # Filter to only existing columns
        ordered_cols = [c for c in ordered_cols if c in df.columns]
        
        return df[ordered_cols]
