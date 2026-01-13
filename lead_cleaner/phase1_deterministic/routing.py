from typing import List
from lead_cleaner.types import LeadRow, RowStatus
from lead_cleaner.config import CONFIDENCE_THRESHOLD
from lead_cleaner.constants import FIELD_Email, FIELD_Phone

def calculate_confidence(row: LeadRow) -> float:
    """
    Calculates deterministic confidence score.
    Logic:
    - Base 1.0
    - Missing Email: -0.3
    - Missing Phone: -0.1
    - Any INVALID field: -0.5
    """
    score = 1.0
    
    data = row["clean_data"]
    
    if not data.get(FIELD_Email):
        score -= 0.3
    if not data.get(FIELD_Phone):
        score -= 0.1
        
    # Check validation details for explicit invalidity
    details = row.get("validation_details", {})
    for field, result in details.items():
        if result.get("field_status") == "INVALID":
            score -= 0.5
    
    return max(0.0, score)

def route_row(row: LeadRow) -> LeadRow:
    """
    Routes row to CLEAN or AI_REQUIRED based on completeness and confidence.
    """
    if row["status"] == RowStatus.REJECTED:
        return row
        
    score = calculate_confidence(row)
    row["confidence_score"] = score
    
    # Routing Logic
    # If we have (Email OR Phone) AND score >= Threshold -> CLEAN
    # Else -> AI_REQUIRED
    
    has_contact = bool(row["clean_data"].get(FIELD_Email) or row["clean_data"].get(FIELD_Phone))
    
    if has_contact and score >= CONFIDENCE_THRESHOLD:
        row["status"] = RowStatus.CLEAN
    else:
        row["status"] = RowStatus.AI_REQUIRED
        
    return row
