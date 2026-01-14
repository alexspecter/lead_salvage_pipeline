import pandas as pd
import os
from typing import Tuple, List

from lead_cleaner.exceptions import ValidationError
from lead_cleaner.constants import REQUIRED_FIELDS
from lead_cleaner.logging.logger import PipelineLogger
from lead_cleaner.io.db_reader import load_from_sqlite

class DataValidator:
    def __init__(self, logger: PipelineLogger):
        self.logger = logger

    def validate_input(self, file_path: str) -> pd.DataFrame:
        """
        Validates input file and loads it into a DataFrame.
        Dispatches to appropriate loader based on file extension.
        
        Supported formats:
        - .csv: Standard CSV files
        - .db, .sqlite: SQLite database files
        
        Returns the loaded DataFrame with normalized headers.
        """
        if not os.path.exists(file_path):
            error_msg = f"Input file not found: {file_path}"
            self.logger.log_event(
                phase="SETUP",
                action="VALIDATION_FAILED",
                reason=error_msg
            )
            raise ValidationError(error_msg)
        
        ext = os.path.splitext(file_path)[1].lower()
        
        # Dispatch based on file extension
        if ext == '.csv':
            df = self._load_csv(file_path)
        elif ext in ('.db', '.sqlite'):
            df = load_from_sqlite(file_path, self.logger)
        else:
            error_msg = f"Unsupported file format: {ext}. Use .csv, .db, or .sqlite"
            self.logger.log_event(
                phase="SETUP",
                action="VALIDATION_FAILED",
                reason=error_msg
            )
            raise ValidationError(error_msg)
        
        # Normalize headers (common for all formats)
        df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]
        
        # Check required columns
        headers = set(df.columns)
        intersection = headers.intersection(REQUIRED_FIELDS)
        
        if not intersection:
            self.logger.log_event(
                phase="SETUP",
                action="VALIDATION_WARNING",
                reason=f"No standard contact fields (email/phone) found. Processing in generic mode."
            )
            if df.empty:
                raise ValidationError("Input file is empty")

        # Log success
        self.logger.log_event(
            phase="SETUP",
            action="VALIDATION_SUCCESS",
            reason=f"File {os.path.basename(file_path)} passed validation. Columns: {list(df.columns)}"
        )
        
        return df
    
    def _load_csv(self, file_path: str) -> pd.DataFrame:
        """Load a CSV file into a DataFrame."""
        try:
            return pd.read_csv(file_path)
        except Exception as e:
            error_msg = f"Failed to read CSV: {str(e)}"
            self.logger.log_error("SETUP", "CSV Read Failed", e)
            raise ValidationError(error_msg)

    def validate_csv(self, file_path: str) -> pd.DataFrame:
        """
        Validates CSV exists, is readable, and has required columns.
        Returns the loaded DataFrame with normalized headers.
        
        DEPRECATED: Use validate_input() for new code.
        """
        return self.validate_input(file_path)

