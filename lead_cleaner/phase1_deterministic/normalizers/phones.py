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
    
    # Detect Extension
    # Look for 'ext', 'x', 'extension' followed by digits
    ext_match = re.search(r'(?:ext\.?|extension|x)\s*(\d+)', s, re.IGNORECASE)
    extension = ext_match.group(1) if ext_match else None
    
    # Remove extension part from main string for digit counting
    if ext_match:
        s = s[:ext_match.start()]

    # Clean non-digits from the main part
    digits = re.sub(r'\D', '', s)
    
    # Logic:
    # 1. 11 digits starting with 1 -> Strip 1, keep 10
    # 2. 10 digits -> XXX-XXX-XXXX
    # 3. 7 digits -> XXX-XXXX (Local US)
    
    if len(digits) == 11 and digits.startswith('1'):
        digits = digits[1:]
        
    formatted = None
    
    if len(digits) == 10:
        formatted = f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"
    elif len(digits) == 7:
        formatted = f"{digits[:3]}-{digits[4:]}" # Treat as local 555-0199 -> 555-0199
        # Actually 7 digit is usually 3-4 split
        formatted = f"{digits[:3]}-{digits[3:]}"
    else:
        # Invalid length
        return {
            "normalized_value": value,
            "field_status": "INVALID",
            "reason": f"Expected 7 or 10 digits, found {len(digits)}"
        }
        
    if extension:
        formatted = f"{formatted} x{extension}"
    
    return {
        "normalized_value": formatted,
        "field_status": "VALID",
        "reason": None
    }
