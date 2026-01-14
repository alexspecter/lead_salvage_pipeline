"""
Missing Value Handler

Implements industry-standard practices for handling missing/null values:
1. Detects various representations of missing data (empty, N/A, null, etc.)
2. Applies context-appropriate handling per field type
3. Optionally creates indicator columns for ML use cases

Based on best practices from data science community:
- Use standardized placeholder "Not Provided" for text fields
- Preserve numeric nulls for proper statistical handling
- Create indicator columns when missingness may be informative
"""

from typing import Any, Dict, Optional
import pandas as pd
import math

from lead_cleaner.config import (
    MISSING_VALUE_PLACEHOLDER,
    MISSING_VALUE_INDICATORS,
)
from lead_cleaner.constants import (
    PLACEHOLDER_ELIGIBLE_FIELDS,
    NUMERIC_FIELDS,
)


def is_missing(value: Any) -> bool:
    """
    Detects if a value represents missing data.
    
    Handles:
    - None/NaN
    - Empty strings
    - Common placeholder strings (N/A, null, -, ?, etc.)
    - Whitespace-only strings
    - Leading security quotes (from scan_and_secure)
    
    Args:
        value: The value to check
        
    Returns:
        True if the value represents missing data
    """
    if value is None:
        return True
    
    # Handle pandas/numpy NaN
    if isinstance(value, float) and math.isnan(value):
        return True
    
    # Handle string representations
    if isinstance(value, str):
        # Strip potential security quote for missingness check
        check_val = value.strip()
        if check_val.startswith("'") and len(check_val) > 1:
            check_val = check_val[1:].strip()
            
        if check_val == "" or check_val in MISSING_VALUE_INDICATORS:
            return True
        # Case-insensitive check
        if check_val.lower() in {v.lower() for v in MISSING_VALUE_INDICATORS if v}:
            return True
    
    return False


def get_field_category(field_name: str) -> str:
    """
    Determines the category of a field for appropriate missing value handling.
    """
    field_lower = field_name.lower()
    
    # Check if it's a known placeholder-eligible field
    if field_lower in PLACEHOLDER_ELIGIBLE_FIELDS:
        return "placeholder_eligible"
    
    # Check if it matches numeric field patterns
    for numeric_pattern in NUMERIC_FIELDS:
        if numeric_pattern in field_lower:
            return "numeric"
    
    # Default to other (basic sanitization)
    return "other"


def handle_missing(
    value: Any, 
    field_name: str, 
    field_type: Optional[str] = None,
    use_placeholder: bool = True
) -> Any:
    """
    Applies appropriate missing value handling based on field type.
    """
    # 1. Check if missing
    missing = is_missing(value)
    
    # 2. If NOT missing, return original (but strip security quotes)
    if not missing:
        if isinstance(value, str):
            val = value.strip()
            # Strip security quote if present
            if val.startswith("'") and len(val) > 1:
                val = val[1:].strip()
            return val
        return value
    
    # 3. If missing, apply strategy
    category = get_field_category(field_name)
    
    if category == "numeric":
        return None
    
    if use_placeholder:
        return MISSING_VALUE_PLACEHOLDER
    
    return None


def sanitize_value(value: Any, field_name: str) -> Any:
    """
    Sanitizes a value - handles missing values and applies basic cleaning.
    """
    return handle_missing(value, field_name, use_placeholder=True)


def create_missing_indicators(raw_data: Dict[str, Any]) -> Dict[str, bool]:
    """
    Creates binary indicators for fields that were originally missing.
    """
    indicators = {}
    for field_name, value in raw_data.items():
        indicators[f"{field_name}_was_missing"] = is_missing(value)
    return indicators
