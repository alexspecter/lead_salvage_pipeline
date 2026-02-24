import pandas as pd
import os

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
                phase="SETUP", action="VALIDATION_FAILED", reason=error_msg
            )
            raise ValidationError(error_msg)

        ext = os.path.splitext(file_path)[1].lower()

        # Dispatch based on file extension
        if ext == ".csv":
            df = self._load_csv(file_path)
        elif ext in (".db", ".sqlite"):
            df = load_from_sqlite(file_path, self.logger)
        else:
            error_msg = f"Unsupported file format: {ext}. Use .csv, .db, or .sqlite"
            self.logger.log_event(
                phase="SETUP", action="VALIDATION_FAILED", reason=error_msg
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
                reason="No standard contact fields (email/phone) found. Processing in generic mode.",
            )
            if df.empty:
                raise ValidationError("Input file is empty")

        # Log success
        self.logger.log_event(
            phase="SETUP",
            action="VALIDATION_SUCCESS",
            reason=f"File {os.path.basename(file_path)} passed validation. Columns: {list(df.columns)}",
        )

        # Add line number metadata for order preservation
        df["_line_number"] = range(1, len(df) + 1)

        return df

    def _load_csv(self, file_path: str) -> pd.DataFrame:
        """Load a CSV file into a DataFrame with smart header detection."""
        try:
            # First attempt: standard load
            df = pd.read_csv(file_path)

            # Check if headers look invalid (mostly "Unnamed" or empty)
            # A header is considered "bad" if >50% of cols are Unnamed or empty,
            # OR if it's completely empty

            def is_bad_header(columns):
                unnamed_count = sum(
                    1
                    for c in columns
                    if str(c).startswith("Unnamed:") or str(c).strip() == ""
                )
                return unnamed_count > (len(columns) / 2)

            if is_bad_header(df.columns) and not df.empty:
                self.logger.log_event(
                    "SETUP",
                    "HEADER_DETECTION",
                    "Initial headers look invalid. Scanning for real header...",
                )

                # Scan first few rows for a candidate
                best_header_row = -1

                # Look at first 10 rows max
                rows_to_check = min(10, len(df))

                for i in range(rows_to_check):
                    row_values = df.iloc[i].astype(str).tolist()
                    # simplistic check: do we have at least 2 non-empty, non-nan-looking strings?
                    # and preferably no "nan" strings if possible, though pandas casts them so it's tricky.

                    valid_cols = sum(
                        1
                        for v in row_values
                        if v.strip() and v.lower() != "nan" and v.lower() != "none"
                    )

                    # If this row has significantly more valid columns than the current bad header...
                    # or just looks "good enough" (e.g. > 50% filled)
                    if valid_cols > (len(df.columns) / 2):
                        best_header_row = i
                        break

                if best_header_row != -1:
                    # Reload with the new header offset
                    # Note: 'header' in read_csv is 0-indexed.
                    # If df.iloc[0] is the header, that is actually line 1 (0-indexed line 1, since line 0 was the bad header)
                    # wait, read_csv uses line numbers of the *file*.
                    # The bad header was line 0. df.iloc[0] is line 1.
                    # So if we found a header at df.iloc[i], the header argument should be i + 1

                    new_header_idx = best_header_row + 1
                    self.logger.log_event(
                        "SETUP",
                        "HEADER_CORRECTION",
                        f"Found better header at line {new_header_idx}. Reloading...",
                    )
                    return pd.read_csv(file_path, header=new_header_idx)

            return df
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
