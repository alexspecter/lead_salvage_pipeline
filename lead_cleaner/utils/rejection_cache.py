import pandas as pd
import os
import shutil
import logging
from typing import List, Dict, Any, Optional

class RejectionCache:
    """
    Centralized cache for all "deleted" items in the pipeline:
    1. Rows (marked as REJECTED)
    2. Columns (dropped because they are empty/placeholders)
    3. Values (invalid values replaced by placeholders or corrected)
    4. Files (rejected by security)
    """
    def __init__(self, output_dir: str, run_id: str, logger: Optional[Any] = None):
        self.output_dir = output_dir
        self.run_id = run_id
        self.logger = logger
        
        # Path: output/rejection_cache_<run_id>/
        self.cache_dir = os.path.join(output_dir, f"rejection_cache_{run_id}")
        os.makedirs(self.cache_dir, exist_ok=True)
        
        self.rejected_rows_file = os.path.join(self.cache_dir, "rejected_rows.csv")
        self.rejected_columns_file = os.path.join(self.cache_dir, "rejected_columns.csv")
        self.rejected_values_file = os.path.join(self.cache_dir, "rejected_values.csv")
        self.rejected_files_log = os.path.join(self.cache_dir, "rejected_files_log.csv")
        
        self.column_rejections = []
        self.value_rejections = []
        self.file_rejections = []

    def add_column_rejection(self, column_name: str, reason: str):
        """Track a column that was dropped from the final output."""
        self.column_rejections.append({
            "run_id": self.run_id,
            "column_name": column_name,
            "reason": reason
        })
        if self.logger:
            self.logger.log_event("REJECTION_CACHE", "COLUMN_REJECTED", reason=f"Column '{column_name}' dropped: {reason}")

    def add_value_rejection(self, row_id: str, field: str, original_value: Any, new_value: Any, reason: str):
        """Track a specific field value that was deleted, marked invalid, or replaced."""
        self.value_rejections.append({
            "run_id": self.run_id,
            "row_id": row_id,
            "field": field,
            "original_value": original_value,
            "new_value": new_value,
            "reason": reason
        })

    def cache_rejected_rows(self, rejected_df: pd.DataFrame):
        """Save rejected rows to the cache."""
        if not rejected_df.empty:
            rejected_df.to_csv(self.rejected_rows_file, index=False)
            if self.logger:
                self.logger.log_event("REJECTION_CACHE", "ROWS_CACHED", reason=f"Cached {len(rejected_df)} rejected rows")

    def log_security_rejection(self, filename: str, reason: str):
        """
        Log a file rejected by security. 
        CRITICAL: Do NOT copy the file. It should be deleted by the security module.
        Only metadata is kept.
        """
        self.file_rejections.append({
            "run_id": self.run_id,
            "original_file": filename,
            "cached_file": "DELETED_BY_SECURITY", # Explicit marker
            "reason": reason
        })
        
        if self.logger:
             self.logger.log_event("REJECTION_CACHE", "FILE_REJECTED_SECURITY", reason=f"File '{filename}' rejected and deleted: {reason}")

    def save(self):
        """Commit all metadata rejections to CSV files."""
        if self.column_rejections:
            pd.DataFrame(self.column_rejections).to_csv(self.rejected_columns_file, index=False)
            
        if self.value_rejections:
            pd.DataFrame(self.value_rejections).to_csv(self.rejected_values_file, index=False)
            
        if self.file_rejections:
            pd.DataFrame(self.file_rejections).to_csv(self.rejected_files_log, index=False)
            
        if self.logger:
            self.logger.log_event("REJECTION_CACHE", "SAVED", reason=f"Rejection cache saved to {self.cache_dir}")
