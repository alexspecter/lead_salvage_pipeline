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
    
    # Job Title Detection in Name (e.g. "Sarah (CEO)")
    # If we find these, we flag as INVALID so AI can interpret/extract the title properly.
    JOB_TITLE_INDICATORS = ["CEO", "CFO", "CTO", "VP", "PRESIDENT", "FOUNDER", "OWNER", "MANAGER", "DIRECTOR", "HEAD OF", "CHIEF"]
    
    paren_match = re.search(r'\(([^\)]+)\)', cleaned, re.IGNORECASE)
    if paren_match:
        content = paren_match.group(1).strip().upper()
        # Check if content contains any title indicator
        if any(indicator in content for indicator in JOB_TITLE_INDICATORS):
            return {
                "normalized_value": value,
                "field_status": "INVALID",
                "reason": "Potential Job Title in Name"
            }
    
    # Strip nicknames in quotes or parentheses (e.g. Robert "Bobby", Kevin (Kev))
    # Matches "..." or (...)
    cleaned = re.sub(r'\"[^\"]*\"', '', cleaned)
    cleaned = re.sub(r'\([^\)]*\)', '', cleaned)

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
