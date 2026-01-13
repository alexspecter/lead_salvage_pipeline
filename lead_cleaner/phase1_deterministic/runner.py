import pandas as pd
from typing import List, Dict, Any

from lead_cleaner.types import LeadRow, RowStatus, ProcessingStatus
from lead_cleaner.logging.logger import PipelineLogger
from lead_cleaner.utils.uuid import generate_row_id
from lead_cleaner.constants import *
from lead_cleaner.phase1_deterministic.normalizers import emails, phones, names, dates, job_titles
from lead_cleaner.phase1_deterministic.deduplication import detect_duplicates
from lead_cleaner.phase1_deterministic.routing import route_row

class Phase1Runner:
    def __init__(self, logger: PipelineLogger, run_id: str):
        self.logger = logger
        self.run_id = run_id

    def process(self, df: pd.DataFrame) -> List[LeadRow]:
        self.logger.log_event(PHASE_1_DETERMINISTIC, "START", reason=f"Processing {len(df)} rows")
        
        rows: List[LeadRow] = []
        
        # 1. Initialize Rows
        for _, raw_row in df.iterrows():
            row_id = generate_row_id()
            row_data = raw_row.to_dict()
            
            lead_row: LeadRow = {
                "row_id": row_id,
                "run_id": self.run_id,
                "raw_data": row_data,
                "clean_data": {},
                "status": RowStatus.AI_REQUIRED, # Default to AI until proven Clean
                "failure_reason": None,
                "confidence_score": 0.0,
                "is_duplicate": False,
                "duplicate_of": None,
                "validation_details": {}
            }
            rows.append(lead_row)
            
        # 2. Normalize
        for row in rows:
            self._normalize_row(row)
            
        # 3. Deduplicate
        rows = detect_duplicates(rows, self.logger)
        
        # 4. Score & Route
        for row in rows:
            route_row(row)
            self.logger.log_event(
                PHASE_1_DETERMINISTIC, 
                "ROW_PROCESSED", 
                row_id=row["row_id"], 
                after=row["status"],
                confidence=row["confidence_score"]
            )
            
        self.logger.log_event(PHASE_1_DETERMINISTIC, "COMPLETE")
        return rows

    def _normalize_row(self, row: LeadRow):
        raw = row["raw_data"]
        clean = {}
        details = {}
        
        # Apply normalizers
        # Email
        res_email = emails.normalize_email(raw.get(FIELD_Email))
        details[FIELD_Email] = res_email
        if res_email["normalized_value"]:
            clean[FIELD_Email] = res_email["normalized_value"]
            
        # Phone
        res_phone = phones.normalize_phone(raw.get(FIELD_Phone))
        details[FIELD_Phone] = res_phone
        if res_phone["normalized_value"]:
            clean[FIELD_Phone] = res_phone["normalized_value"]
            
        # Names
        res_fname = names.normalize_name(raw.get(FIELD_FirstName))
        details[FIELD_FirstName] = res_fname
        if res_fname["normalized_value"]:
            clean[FIELD_FirstName] = res_fname["normalized_value"]
            
        res_lname = names.normalize_name(raw.get(FIELD_LastName))
        details[FIELD_LastName] = res_lname
        if res_lname["normalized_value"]:
            clean[FIELD_LastName] = res_lname["normalized_value"]
            
        # Date
        res_date = dates.normalize_date(raw.get(FIELD_Date))
        details[FIELD_Date] = res_date
        if res_date["normalized_value"]:
            clean[FIELD_Date] = res_date["normalized_value"]
            
        # Job
        res_job = job_titles.normalize_job_title(raw.get(FIELD_JobTitle))
        details[FIELD_JobTitle] = res_job
        if res_job["normalized_value"]:
            clean[FIELD_JobTitle] = res_job["normalized_value"]
            
        # Company (Pass through + basic clean)
        if raw.get(FIELD_Company):
             clean[FIELD_Company] = str(raw.get(FIELD_Company)).strip()
             
        row["clean_data"] = clean
        row["validation_details"] = details
