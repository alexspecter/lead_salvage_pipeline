from typing import Tuple, Optional
import re
from lead_cleaner.types import NormalizerResult
from lead_cleaner.utils.text import clean_whitespace

# Common honorifics to strip from names
HONORIFICS = [
    r'\bDr\.?\s*',
    r'\bMr\.?\s*',
    r'\bMrs\.?\s*',
    r'\bMs\.?\s*',
    r'\bProf\.?\s*',
    r'\bSir\s+',
    r'\bLady\s+',
]

def normalize_name(value: Optional[str], field_name: str = "name") -> NormalizerResult:
    if not value:
         return {
            "normalized_value": None,
            "field_status": "MISSING",
            "reason": "Empty value"
        }
    
    s = str(value)
    
    # Strip whitespace
    cleaned = clean_whitespace(s)
    
    # Strip honorifics (Dr., Mr., etc.)
    for pattern in HONORIFICS:
        cleaned = re.sub(pattern, '', cleaned, flags=re.IGNORECASE)
    
    # Clean up any double spaces left over
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    
    # Title Case
    cleaned = cleaned.title()
    
    # Strict Check: If name contains digits or parentheses, flag for AI repair
    if re.search(r'[\d\(\)]', cleaned):
         return {
            "normalized_value": value,
            "field_status": "INVALID",
            "reason": "Contains digits or parentheses"
        }
    
    # Single-letter detection (for last_name specifically)
    # E.g., "M." or "M" is suspicious - likely truncated
    if field_name == "last_name" and len(cleaned.rstrip('.')) <= 2:
         return {
            "normalized_value": value,
            "field_status": "INVALID",
            "reason": "Suspiciously short (possible truncation)"
        }
    
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
