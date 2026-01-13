from typing import Optional
from lead_cleaner.types import NormalizerResult
from lead_cleaner.utils.text import clean_whitespace

def normalize_job_title(value: Optional[str]) -> NormalizerResult:
    if not value:
         return {
            "normalized_value": None,
            "field_status": "MISSING",
            "reason": "Empty value"
        }
    
    cleaned = clean_whitespace(str(value))
    cleaned = cleaned.title()
    
    # Map common variations
    mappings = {
        "Ceo": "CEO",
        "Cto": "CTO",
        "Vp": "VP",
    }
    
    words = cleaned.split()
    fixed_words = [mappings.get(w, w) for w in words]
    cleaned = " ".join(fixed_words)
    
    return {
        "normalized_value": cleaned,
        "field_status": "VALID",
        "reason": None
    }
