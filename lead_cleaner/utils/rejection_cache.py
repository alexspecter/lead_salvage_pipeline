import pandas as pd
import os
import json
from typing import List, Dict, Any, Optional

class RejectionCache:
    """
    Centralized cache for all "deleted" items in the pipeline.
    Produces a SINGLE consolidated CSV file instead of a folder.
    """
    def __init__(self, output_dir: str, run_id: str, logger: Optional[Any] = None):
        self.output_dir = output_dir
        self.run_id = run_id
        self.logger = logger
        self.cache_file = os.path.join(output_dir, f"rejection_cache_{run_id}.csv")
        
        self.cache_entries: List[Dict[str, Any]] = []

    def add_column_rejection(self, column_name: str, reason: str):
        """Track a column that was dropped."""
        self.cache_entries.append({
            "run_id": self.run_id,
            "type": "COLUMN_REJECTED",
            "item_id": column_name,
            "reason": reason,
            "details": json.dumps({"column": column_name})
        })
        if self.logger:
            self.logger.log_event("REJECTION_CACHE", "COLUMN_REJECTED", reason=f"Column '{column_name}' dropped: {reason}")

    def add_value_rejection(self, row_id: str, field: str, original_value: Any, new_value: Any, reason: str):
        """Track a specific field value that was deleted/sanitized."""
        self.cache_entries.append({
            "run_id": self.run_id,
            "type": "VALUE_REJECTED",
            "item_id": f"{row_id}:{field}",
            "reason": reason,
            "details": json.dumps({
                "row_id": row_id,
                "field": field,
                "original": str(original_value),
                "new": str(new_value)
            })
        })

    def cache_rejected_rows(self, rejected_df: pd.DataFrame):
        """Save rejected rows to the cache."""
        if rejected_df.empty:
            return

        for _, row in rejected_df.iterrows():
            row_dict = row.to_dict()
            row_id = str(row_dict.get("row_id", "unknown"))
            reason = str(row_dict.get("failure_reason", "Unknown"))
            
            # Serialize full row data to details
            # We filter out metadata for cleaner JSON if needed, but keeping all is safer for audit
            self.cache_entries.append({
                "run_id": self.run_id,
                "type": "ROW_REJECTED",
                "item_id": row_id,
                "reason": reason,
                "details": json.dumps(row_dict, default=str)
            })
        
        if self.logger:
            self.logger.log_event("REJECTION_CACHE", "ROWS_CACHED", reason=f"Cached {len(rejected_df)} rejected rows")

    def log_security_rejection(self, filename: str, reason: str):
        """Log a file rejected by security."""
        self.cache_entries.append({
            "run_id": self.run_id,
            "type": "FILE_REJECTED_SECURITY",
            "item_id": filename,
            "reason": reason,
            "details": json.dumps({"filename": filename, "action": "DELETED"})
        })
        if self.logger:
             self.logger.log_event("REJECTION_CACHE", "FILE_REJECTED", reason=f"File '{filename}' rejected: {reason}")

    def save(self):
        """Commit all rejections to a single CSV file."""
        if not self.cache_entries:
            # Create an empty file with headers just to exist (as implied by requirements "should be 3 files")
            # Or assume no rejections means no file? 
            # User said "there should be 3 files", implying existence even if empty is better structure for consumers
            df = pd.DataFrame(columns=["run_id", "type", "item_id", "reason", "details"])
        else:
            df = pd.DataFrame(self.cache_entries)
            # Ensure column order
            cols = ["run_id", "type", "item_id", "reason", "details"]
            # Add any extra cols if they somehow got in, but prioritize these
            df = df.reindex(columns=cols)
            
        df.to_csv(self.cache_file, index=False)
            
        if self.logger:
            self.logger.log_event("REJECTION_CACHE", "SAVED", reason=f"Rejection cache saved to {self.cache_file}")
