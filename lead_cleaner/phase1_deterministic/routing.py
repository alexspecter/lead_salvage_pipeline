from typing import List
from lead_cleaner.types import LeadRow, RowStatus
from lead_cleaner.config import CONFIDENCE_THRESHOLD, ENABLE_GENERIC_MODE
from lead_cleaner.constants import FIELD_Email, FIELD_Phone

def calculate_confidence(row: LeadRow, expected_fields: dict) -> float:
    """
    Calculates deterministic confidence score.
    Logic:
    - Base 1.0
    - Missing Expected Email: -0.3
    - Missing Expected Phone: -0.1
    - Any INVALID field: -0.5
    """
    score = 1.0
    data = row["clean_data"]
    
    # Penalize missing contact info ONLY if it was EXPECTED in the schema
    # (i.e. the input CSV actually had an Email/Phone column)
    if expected_fields.get(FIELD_Email) and not data.get(FIELD_Email):
        score -= 0.3
    if expected_fields.get(FIELD_Phone) and not data.get(FIELD_Phone):
        score -= 0.1
        
    # Check validation details for explicit invalidity
    details = row.get("validation_details", {})
    for field, result in details.items():
        if result.get("field_status") == "INVALID":
            score -= 0.5
    
    return max(0.0, score)

def route_row(row: LeadRow, expected_fields: dict = None) -> LeadRow:
    """
    Routes row to CLEAN or AI_REQUIRED based on completeness and confidence.
    """
    if row["status"] == RowStatus.REJECTED:
        return row
    
    if expected_fields is None:
        expected_fields = {FIELD_Email: True, FIELD_Phone: True} # Default to expecting all for safety
        
    score = calculate_confidence(row, expected_fields)
    row["confidence_score"] = score
    
    # Routing Logic:
    # 1. Do we have contact info?
    has_contact = bool(row["clean_data"].get(FIELD_Email) or row["clean_data"].get(FIELD_Phone))
    
    # 2. Are we missing CRITICAL expected fields?
    # If schema expects Email, and we don't have it -> Critical Missing
    critical_missing = False
    if expected_fields.get(FIELD_Email) and not row["clean_data"].get(FIELD_Email):
        critical_missing = True
    if expected_fields.get(FIELD_Phone) and not row["clean_data"].get(FIELD_Phone):
        critical_missing = True

    # CLEAN Criteria:
    # A. High Confidence
    # B. NOT missing any critical expected fields (if expected)
    # C. OR Generic Mode (relaxed)
    
    # If schema has NO contact fields, then critical_missing remains False
    # and we rely on score.
    
    if score >= CONFIDENCE_THRESHOLD and not critical_missing:
        row["status"] = RowStatus.CLEAN
        
    # Fallback to Generic Mode logic (if enabled AND scoring failed)
    elif ENABLE_GENERIC_MODE:
        # If Generic Mode is ON, we allow missing critical fields if data is valid
        # Recalculate 'validity' based on errors
        has_errors = any(
            r.get("field_status") == "INVALID" 
            for r in row.get("validation_details", {}).values()
        )
        # Require minimal content (3 fields)
        field_count = len([v for v in row["clean_data"].values() if v is not None and v != ""])
        
        if field_count >= 3 and not has_errors:
            row["status"] = RowStatus.CLEAN
            if score < CONFIDENCE_THRESHOLD:
                row["confidence_score"] = CONFIDENCE_THRESHOLD # Boost score
    else:
        row["status"] = RowStatus.AI_REQUIRED
        
    return row
