import pandas as pd
from typing import List, Set

from lead_cleaner.types import LeadRow, RowStatus, FailureReason
from lead_cleaner.logging.logger import PipelineLogger
from lead_cleaner.utils.uuid import generate_fingerprint
from lead_cleaner.constants import FIELD_Email, FIELD_Phone

def detect_duplicates(rows: List[LeadRow], logger: PipelineLogger) -> List[LeadRow]:
    """
    Detects duplicates based on fingerprint (email|phone).
    Marks subsequent duplicates as REJECTED.
    """
    seen_fingerprints: Set[str] = set()
    
    for row in rows:
        if row["status"] == RowStatus.REJECTED:
            continue
            
        # Generate fingerprint from CLEAN data if available, else RAW
        # Priority: Email, then Phone
        # We need to look at what's in the row.
        # But wait, we haven't populated 'clean_data' fully yet when we run this?
        # The runner will call this. Let's assume 'clean_data' has the normalized values.
        
        email = row["clean_data"].get(FIELD_Email) or row["raw_data"].get(FIELD_Email)
        phone = row["clean_data"].get(FIELD_Phone) or row["raw_data"].get(FIELD_Phone)
        
        # If both missing, we can't really fingerprint, maybe use row_id?
        # But directive says "Generate row fingerprint (email|phone or row_id)"
        
        content = ""
        if email:
            content = f"email:{str(email).strip().lower()}"
        elif phone:
            content = f"phone:{str(phone).strip()}"
        else:
            content = f"row_id:{row['row_id']}"
            
        fp = generate_fingerprint(content)
        
        if fp in seen_fingerprints:
            row["is_duplicate"] = True
            row["status"] = RowStatus.REJECTED
            row["failure_reason"] = FailureReason.DUPLICATE
            
            logger.log_event(
                phase="PHASE_1",
                action="DUPLICATE_DETECTED",
                row_id=row["row_id"],
                reason=f"Fingerprint collision: {content}"
            )
        else:
            seen_fingerprints.add(fp)
            row["is_duplicate"] = False
            
    return rows
