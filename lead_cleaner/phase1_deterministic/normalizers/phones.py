from typing import Tuple, Optional
import re
from lead_cleaner.types import NormalizerResult

def normalize_phone(value: Optional[str]) -> NormalizerResult:
    if not value:
        return {
            "normalized_value": None,
            "field_status": "MISSING",
            "reason": "Empty value"
        }
        
    s = str(value).strip()
    
    # Strip leading quotes/apostrophes (common Excel artifact)
    s = s.lstrip("'\"")
    
    # Remove non-digits
    digits = re.sub(r'\D', '', s)
    
    # Simple logic: if 10 digits, good. If 11 and starts with 1, strip 1.
    if len(digits) == 11 and digits.startswith('1'):
        digits = digits[1:]
        
    if len(digits) != 10:
         return {
            "normalized_value": value,
            "field_status": "INVALID",
            "reason": f"Expected 10 digits, found {len(digits)}"
        }
        
    # Format as XXX-XXX-XXXX
    formatted = f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"
    
    return {
        "normalized_value": formatted,
        "field_status": "VALID",
        "reason": None
    }
