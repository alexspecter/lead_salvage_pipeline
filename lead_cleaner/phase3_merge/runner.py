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
        
        # CONDITIONAL EMPTY NAME REJECTION
        # Only applies if dataset has first_name AND last_name columns
        from lead_cleaner.config import MISSING_VALUE_PLACEHOLDER
        
        # Check schema from first row's clean_data
        if all_rows:
            sample_keys = set(all_rows[0].get("clean_data", {}).keys())
            has_name_cols = "first_name" in sample_keys and "last_name" in sample_keys
            
            if has_name_cols:
                empty_name_count = 0
                for r in all_rows:
                    if r["status"] == RowStatus.CLEAN:
                        cd = r.get("clean_data", {})
                        fn = cd.get("first_name", "")
                        ln = cd.get("last_name", "")
                        
                        # Check if both are effectively empty
                        fn_empty = not fn or fn == MISSING_VALUE_PLACEHOLDER
                        ln_empty = not ln or ln == MISSING_VALUE_PLACEHOLDER
                        
                        if fn_empty and ln_empty:
                            r["status"] = RowStatus.REJECTED
                            r["failure_reason"] = "EMPTY_NAME"
                            empty_name_count += 1
                
                if empty_name_count > 0:
                    self.logger.log_event(PHASE_3_MERGE, "EMPTY_NAME_REJECTION", 
                                          reason=f"Rejected {empty_name_count} rows with empty first_name AND last_name")
        
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
        # CLEAN output gets minimal metadata (only row_id) and reordered columns
        final_df = self._flatten_rows(final_rows, include_raw=False, minimal_metadata=True)
        # REJECT store keeps everything for debugging
        rejected_df = self._flatten_rows(rejected_rows, include_raw=True, minimal_metadata=False)
        
        final_path = os.path.join(output_dir, f"final_output_{self.run_id}.csv")
        rejected_path = os.path.join(output_dir, f"reject_store_{self.run_id}.csv")
        
        final_df.to_csv(final_path, index=False)
        if not rejected_df.empty:
            rejected_df.to_csv(rejected_path, index=False)
        
        # EXPORT LOG TO USER_OUTPUT as requested
        import shutil
        log_dest = os.path.join(output_dir, f"pipeline_log_{self.run_id}.csv")
        try:
            shutil.copy2(self.logger.log_file, log_dest)
            self.logger.log_event(PHASE_3_MERGE, "LOG_EXPORTED", reason=f"Log copied to {log_dest}")
        except Exception as e:
            self.logger.log_error(PHASE_3_MERGE, "LOG_EXPORT_FAILED", e)
            
        self.logger.log_event(PHASE_3_MERGE, "COMPLETE", reason=f"Written to {output_dir}")
        
    def _flatten_rows(self, rows: List[LeadRow], include_raw: bool = False, minimal_metadata: bool = False) -> pd.DataFrame:
        """
        Flattens LeadRow objects to a DataFrame.
        
        Args:
            rows: List of LeadRow dictionaries
            include_raw: Whether to include raw_data with "raw_" prefix
            minimal_metadata: If True, only row_id is kept from metadata.
        """
        if not rows:
            return pd.DataFrame()
        
        flat_data = []
        
        # Collect all possible clean_data keys for consistent columns
        all_clean_keys: Set[str] = set()
        if PRESERVE_ALL_FIELDS:
            for r in rows:
                all_clean_keys.update(r.get("clean_data", {}).keys())
        
        # Internal metadata columns that are ALWAYS added by the pipeline (not from user data).
        # These are controlled by 'minimal_metadata' flag when building the 'item' dict.
        internal_keys = ["run_id", "confidence_score", "failure_reason", "row_id", "status"]
        
        # Keys that are reserved by the pipeline's internal logic.
        # If user data has these keys, they must be renamed to avoid collision/overwrite.
        RESERVED_KEYS = set(internal_keys + ["status"]) 

        for r in rows:
            # Metadata columns
            if minimal_metadata:
                item: Dict[str, Any] = {"row_id": r["row_id"]}
            else:
                item: Dict[str, Any] = {
                    "row_id": r["row_id"],
                    "run_id": r["run_id"],
                    "status": r["status"].value if hasattr(r["status"], 'value') else r["status"],
                    "confidence_score": r["confidence_score"],
                    "failure_reason": r["failure_reason"].value if hasattr(r.get("failure_reason"), 'value') else r.get("failure_reason")
                }
            
            # Add ALL clean data fields with COLLISION PROTECTION
            clean_data = r.get("clean_data", {})
            for key in all_clean_keys:
                final_key = key
                if key in RESERVED_KEYS:
                    final_key = f"{key}_original"
                item[final_key] = clean_data.get(key)
            
            # Also add any keys that might be in this row but not in all_clean_keys
            for key, value in clean_data.items():
                final_key = key
                if key in RESERVED_KEYS:
                    final_key = f"{key}_original"
                
                if final_key not in item:
                    item[final_key] = value
            
            # Include raw data for rejected rows
            if include_raw:
                for k, v in r.get("raw_data", {}).items():
                    item[f"raw_{k}"] = v
                    
            flat_data.append(item)
        
        df = pd.DataFrame(flat_data)
        
        # Priority Ordering Logic:
        # 1. row_id
        # 2. name, longname, first_name, last_name
        # 3. Everything else
        # 4. Renamed original columns (e.g. status_original)
        
        metadata_cols = ["row_id"] if minimal_metadata else ["row_id", "run_id", "status", "confidence_score", "failure_reason"]
        
        # Identify priority columns (case-insensitive search)
        priority_keys = ["name", "longname", "long_name", "first_name", "last_name", "surname"]
        priority_cols = []
        
        # We use a loop to maintain some order among priority columns if they exist
        for pk in priority_keys:
            for col in df.columns:
                if col.lower() == pk and col not in metadata_cols and col not in priority_cols:
                    priority_cols.append(col)
        
        # Dynamic Priority: Find the first column with "_name" or "_id" that isn't already Prioritized
        # This handles cases like "campaign_name", "user_id", etc.
        for col in df.columns:
            if col not in metadata_cols and col not in priority_cols:
                col_lower = col.lower()
                if "_name" in col_lower or "_id" in col_lower:
                    priority_cols.append(col)
                    break  # Only take the FIRST one found to avoid disrupting too much order
        
        # Rest of the columns (excluding raw columns, internal metadata, and priority)
        # To preserve natural order (e.g. keep 'status' where it was), we simply iterate df.columns
        # and take anything that isn't already handled.
        remaining_cols = [
            c for c in df.columns 
            if c not in metadata_cols 
            and c not in priority_cols 
            and not c.startswith("raw_")
        ]

        raw_cols = sorted([c for c in df.columns if c.startswith("raw_")])
        
        ordered_cols = metadata_cols + priority_cols + remaining_cols + raw_cols
        ordered_cols = [c for c in ordered_cols if c in df.columns]
        
        df_ordered = df[ordered_cols]
        
        # RESTORATION LOGIC:
        # User requested: "remove the _original from the name" at the end if possible.
        # We try to rename "status_original" -> "status" IF "status" doesn't exist in the final df.
        # This typically happens in Clean Output where internal "status" is excluded.
        
        # We need to identify renamed columns again for this step
        renamed_cols = sorted([c for c in df_ordered.columns if c.endswith("_original")])
        
        final_rename_map = {}
        top_columns = set(df_ordered.columns)
        
        for col in renamed_cols:
            if col.endswith("_original"):
                base_name = col[:-9] # Remove "_original"
                if base_name not in top_columns:
                    final_rename_map[col] = base_name
                    # Update set to prevent double renaming complications (though unlikely)
                    top_columns.add(base_name)
                    
        if final_rename_map:
            df_ordered = df_ordered.rename(columns=final_rename_map)
        
        # FINAL CLEANUP: Drop columns that are entirely "Not Provided", Empty, or None
        from lead_cleaner.config import MISSING_VALUE_PLACEHOLDER
        
        cols_to_drop = []
        for col in df_ordered.columns:
            # Get unique values (converted to string for easier check)
            unique_vals = set(df_ordered[col].astype(str).unique())
            
            # Check if all unique values are "useless"
            # Useless = "nan", "None", "", MISSING_VALUE_PLACEHOLDER
            is_empty = True
            for val in unique_vals:
                v = val.strip()
                if v and v.lower() != 'nan' and v.lower() != 'none' and v != MISSING_VALUE_PLACEHOLDER:
                    is_empty = False
                    break
            
            if is_empty:
                cols_to_drop.append(col)
        
        if cols_to_drop:
            df_ordered = df_ordered.drop(columns=cols_to_drop)
            
        return df_ordered

