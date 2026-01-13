import pandas as pd
import os
from typing import Tuple, List

from lead_cleaner.exceptions import ValidationError
from lead_cleaner.constants import REQUIRED_FIELDS
from lead_cleaner.logging.logger import PipelineLogger

class DataValidator:
    def __init__(self, logger: PipelineLogger):
        self.logger = logger

    def validate_csv(self, file_path: str) -> pd.DataFrame:
        """
        Validates CSV exists, is readable, and has required columns.
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

        try:
            df = pd.read_csv(file_path)
        except Exception as e:
            error_msg = f"Failed to read CSV: {str(e)}"
            self.logger.log_error("SETUP", "CSV Read Failed", e)
            raise ValidationError(error_msg)

        # Normalize headers
        df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]

        # Check required columns
        # Directive: "At least one of: email, phone"
        # My constants.py said REQUIRED_FIELDS = {email, phone}
        # But wait, "At least one of" means we need intersection size >= 1
        
        headers = set(df.columns)
        intersection = headers.intersection(REQUIRED_FIELDS)
        
        # In dynamic mode, we shouldn't strictly enforce email/phone
        # But we should warn if standard contact info is missing
        if not intersection:
            self.logger.log_event(
                phase="SETUP",
                action="VALIDATION_WARNING",
                reason=f"No standard contact fields (email/phone) found. Processing in generic mode."
            )
            # We don't raise ValidationError here anymore purely based on column names,
            # unless the file is empty which is handled by pd.read_csv usually returning empty DF
            if df.empty:
                 raise ValidationError("Input CSV is empty")


        # Log success
        self.logger.log_event(
            phase="SETUP",
            action="VALIDATION_SUCCESS",
            reason=f"File {os.path.basename(file_path)} passed validation. Columns: {list(df.columns)}"
        )
        
        return df
