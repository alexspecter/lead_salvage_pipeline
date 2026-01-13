from typing import Tuple, Optional
import re
from lead_cleaner.types import NormalizerResult
from lead_cleaner.utils.text import clean_whitespace

def normalize_name(value: Optional[str]) -> NormalizerResult:
    if not value:
         return {
            "normalized_value": None,
            "field_status": "MISSING",
            "reason": "Empty value"
        }
    
    s = str(value)
    # Remove emojis and bad chars (basic set)
    # Using a simple block list for now or regex for alpha-ish
    
    # Strip whitespace
    cleaned = clean_whitespace(s)
    
    # Title Case
    cleaned = cleaned.title()
    
    if len(cleaned) < 2:
         return {
            "normalized_value": value,
            "field_status": "INVALID",
            "reason": "Too short"
        }
        
    return {
        "normalized_value": cleaned,
        "field_status": "VALID",
        "reason": None
    }
