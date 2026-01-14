"""
Phase 1 Runner - Deterministic Processing

Handles:
1. Dynamic field detection (Header-based + Content-based Inference)
2. Normalization of known field types
3. Missing value handling
4. Deduplication
5. Routing (CLEAN vs AI_REQUIRED)
"""

import pandas as pd
from typing import List, Dict, Any, Optional

from lead_cleaner.types import LeadRow, RowStatus, ProcessingStatus
from lead_cleaner.logging.logger import PipelineLogger
from lead_cleaner.utils.uuid import generate_row_id
from lead_cleaner.constants import (
    PHASE_1_DETERMINISTIC,
    FIELD_Email, FIELD_Phone, FIELD_FirstName, FIELD_LastName,
    FIELD_Date, FIELD_JobTitle, FIELD_Company,
    FIELD_TYPE_PATTERNS,
)
from lead_cleaner.config import PRESERVE_ALL_FIELDS, NORMALIZE_KNOWN_FIELDS_ONLY
from lead_cleaner.phase1_deterministic.normalizers import emails, phones, names, dates, job_titles
from lead_cleaner.phase1_deterministic.deduplication import detect_duplicates
from lead_cleaner.phase1_deterministic.routing import route_row
from lead_cleaner.phase1_deterministic.missing_values import sanitize_value, is_missing
from lead_cleaner.phase1_deterministic.type_inference import infer_column_type, parse_currency


from lead_cleaner.utils.rejection_cache import RejectionCache


class Phase1Runner:
    def __init__(self, logger: PipelineLogger, run_id: str, rejection_cache: RejectionCache):
        self.logger = logger
        self.run_id = run_id
        self.rejection_cache = rejection_cache
        # Map: lowercase_col_name -> detected_type
        self._field_type_cache: Dict[str, Optional[str]] = {}

    def process(self, df: pd.DataFrame) -> List[LeadRow]:
        self.logger.log_event(PHASE_1_DETERMINISTIC, "START", reason=f"Processing {len(df)} rows")
        
        # 0. Deduplicate column names to prevent DataFrame vs Series access issues
        df = self._deduplicate_columns(df)
        
        # 1. Detect field types (Header + Content Inference)
        self._field_type_cache = self._detect_field_types(df)
        self.logger.log_event(
            PHASE_1_DETERMINISTIC, 
            "FIELD_MAPPING", 
            reason=f"Detected mappings: {self._field_type_cache}"
        )
        
        rows: List[LeadRow] = []
        
        # 2. Initialize Rows
        for _, raw_row in df.iterrows():
            row_id = generate_row_id()
            row_data = raw_row.to_dict()
            
            lead_row: LeadRow = {
                "row_id": row_id,
                "run_id": self.run_id,
                "raw_data": row_data,
                "clean_data": {},
                "status": RowStatus.AI_REQUIRED,  # Default to AI until proven Clean
                "failure_reason": None,
                "confidence_score": 0.0,
                "is_duplicate": False,
                "duplicate_of": None,
                "validation_details": {}
            }
            rows.append(lead_row)
            
        # 3. Normalize (with dynamic field handling)
        for row in rows:
            self._normalize_row(row)
            
        # 4. Deduplicate (uses configurable strategy)
        rows = detect_duplicates(rows, self.logger)
        
        # 5. Score & Route
        # Determine expected fields from schema (field_type_cache)
        # If we detected an 'email' field in the schema, we expect rows to have it.
        expected_fields = {
            FIELD_Email: any(t == FIELD_Email for t in self._field_type_cache.values()),
            FIELD_Phone: any(t == FIELD_Phone for t in self._field_type_cache.values())
        }
        
        self.logger.log_event(
            PHASE_1_DETERMINISTIC, 
            "SCHEMA_EXPECTATIONS", 
            reason=f"Expected Fields: {expected_fields}"
        )
        
        for row in rows:
            route_row(row, expected_fields)
            self.logger.log_event(
                PHASE_1_DETERMINISTIC, 
                "ROW_PROCESSED", 
                row_id=row["row_id"], 
                after=row["status"],
                confidence=row["confidence_score"]
            )
            
        self.logger.log_event(PHASE_1_DETERMINISTIC, "COMPLETE")
        return rows

    def _detect_field_types(self, df: pd.DataFrame) -> Dict[str, Optional[str]]:
        """
        Maps input column names to known field types using:
        1. Header name matching
        2. Content-based inference (if header match fails)
        
        Returns a dict where:
        - key = original column name (lowercase)
        - value = detected type (email, phone, date, currency, etc.) or None
        """
        mapping = {}
        
        for col in df.columns:
            col_lower = str(col).lower().strip()
            detected_type = None
            
            # A. Header Pattern Matching
            for field_type, patterns in FIELD_TYPE_PATTERNS.items():
                if col_lower in patterns:
                    detected_type = field_type
                    break
            
            # B. Content-Based Inference (if no header match)
            if not detected_type:
                inferred = infer_column_type(df[col])
                
                # SAFETY: Prevent IDs and financial columns from being inferred as Phone numbers
                # IDs often look like phone numbers (digits) but shouldn't be formatted
                # Salary/wage columns are numeric but aren't phones
                financial_keywords = ["id", "code", "salary", "wage", "amount", "price", "cost", "score", "age"]
                if inferred == FIELD_Phone and any(x in col_lower for x in financial_keywords):
                    inferred = None
                    
                if inferred:
                    detected_type = inferred
            
            mapping[col_lower] = detected_type
        
        return mapping

    def _normalize_row(self, row: LeadRow):
        """
        Normalizes a row with dynamic field handling.
        
        1. Preserves ALL fields from raw_data (with sanitization)
        2. Applies specialized normalizers to detected field types
        3. Handles missing values appropriately
        4. Writes output:
           - Critical fields (Email, Phone) -> Canonical keys (for dedupe)
           - Generic types (Date, Currency) -> Original keys (normalized)
        """
        raw = row["raw_data"]
        clean = {}
        details = {}
        row_id = row["row_id"]
        
        # STEP 1: Preserve all fields with basic sanitization
        # We start by initializing clean_data with sanitized raw values
        if PRESERVE_ALL_FIELDS:
            for field_name, value in raw.items():
                field_lower = field_name.lower().strip()
                # Apply basic sanitization (handles missing values logic)
                sanitized = sanitize_value(value, field_lower)
                clean[field_lower] = sanitized
                
                # REJECTION CACHE: If value was changed by sanitization (and不是None to None), track it
                if is_missing(value) and str(value).strip() != str(sanitized).strip():
                     self.rejection_cache.add_value_rejection(
                         row_id=row_id,
                         field=field_name,
                         original_value=value,
                         new_value=sanitized,
                         reason="Missing/Placeholder Sanitization"
                     )
        
        # STEP 2: Apply specialized normalizers to detected field types
        for field_name, value in raw.items():
            field_lower = field_name.lower().strip()
            detected_type = self._field_type_cache.get(field_lower)
            
            if detected_type and (not NORMALIZE_KNOWN_FIELDS_ONLY or detected_type):
                result = self._apply_normalizer(detected_type, value, field_lower)
                
                if result:
                    details[field_name] = result  # Store under original name in details
                    
                    norm_val = result.get("normalized_value")
                    field_status = result.get("field_status")
                    
                    # REJECTION CACHE: If validation failed, log it as a value rejection
                    if field_status in ["INVALID", "ERROR"]:
                        self.rejection_cache.add_value_rejection(
                            row_id=row_id,
                            field=field_name,
                            original_value=value,
                            new_value=norm_val,
                            reason=f"Normalization {field_status}: {result.get('reason')}"
                        )

                    if norm_val is not None:
                        # LOGIC FOR WRITING TO CLEAN DATA:
                        
                        # 1. Always update the original field (in-place improvement)
                        #    This ensures "Joined" becomes "2004-07-01", "Wage" becomes 100000.0
                        clean[field_lower] = norm_val
                        
                        # 2. If it's a Critical Canonical Field (Email, Phone)
                        #    Also map to the canonical key for deduplication & routing support
                        if detected_type in {FIELD_Email, FIELD_Phone}:
                            clean[detected_type] = norm_val
                            
                        # 3. For First/Last names and Job Titles
                        #    But be careful not to overwrite if we have multiple name fields
                        #    Assuming standard dataset logic for now.
                        if detected_type in {FIELD_FirstName, FIELD_LastName, FIELD_JobTitle}:
                            clean[detected_type] = norm_val

        row["clean_data"] = clean
        row["validation_details"] = details

    def _apply_normalizer(
        self, 
        field_type: str, 
        value: Any, 
        original_field: str
    ) -> Optional[Dict[str, Any]]:
        """
        Applies the appropriate normalizer based on field type.
        """
        # Skip if value is missing (already handled)
        if is_missing(value):
            return {
                "normalized_value": None,
                "field_status": "MISSING",
                "reason": "Value was missing or empty"
            }
        
        try:
            if field_type == FIELD_Email:
                return emails.normalize_email(value)
            elif field_type == FIELD_Phone:
                return phones.normalize_phone(value)
            elif field_type == FIELD_FirstName or field_type == FIELD_LastName:
                # Pass field name for context-aware validation (e.g., single-letter last names)
                fn = "first_name" if field_type == FIELD_FirstName else "last_name"
                return names.normalize_name(value, field_name=fn)
            elif field_type == FIELD_Date:
                return dates.normalize_date(value)
            elif field_type == FIELD_JobTitle:
                return job_titles.normalize_job_title(value)
            elif field_type == "currency":
                # New currency normalizer logic
                amount = parse_currency(str(value))
                return {
                    "normalized_value": amount,
                    "field_status": "VALID" if amount is not None else "ERROR",
                    "reason": None
                }
            elif field_type == FIELD_Company:
                clean_value = str(value).strip() if value else None
                return {
                    "normalized_value": clean_value,
                    "field_status": "VALID",
                    "reason": None
                }
            else:
                return None
        except Exception as e:
            return {
                "normalized_value": None,
                "field_status": "ERROR",
                "reason": str(e)
            }

    def _deduplicate_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Ensures all columns have unique names by appending suffixes to duplicates.
        Example: clicks, clicks -> clicks, clicks_1
        """
        cols = pd.Series(df.columns)
        for dup in cols[cols.duplicated()].unique(): 
            cols[cols[cols == dup].index.values.tolist()] = [dup if i == 0 else f"{dup}_{i}" for i in range(sum(cols == dup))]
        df.columns = cols
        return df
