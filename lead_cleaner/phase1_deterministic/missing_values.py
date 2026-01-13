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
        stripped = value.strip()
        # Check if empty or in known indicators
        if stripped == "" or stripped in MISSING_VALUE_INDICATORS:
            return True
        # Case-insensitive check
        if stripped.lower() in {v.lower() for v in MISSING_VALUE_INDICATORS if v}:
            return True
    
    return False


def get_field_category(field_name: str) -> str:
    """
    Determines the category of a field for appropriate missing value handling.
    
    Categories:
    - "placeholder_eligible": Text fields that should get "Not Provided"
    - "numeric": Numeric fields that should preserve null/NaN
    - "other": Other fields that get basic sanitization
    
    Args:
        field_name: The normalized field name
        
    Returns:
        Field category string
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
    
    Handling strategies:
    - Text fields (email, name, etc.): Replace with "Not Provided"
    - Numeric fields: Preserve as None/NaN for statistical accuracy
    - Date fields: Preserve as None
    - Other fields: Basic sanitization, optionally use placeholder
    
    Args:
        value: The value to process
        field_name: The field/column name
        field_type: Optional explicit field type override
        use_placeholder: Whether to use placeholder for eligible fields
        
    Returns:
        The processed value
    """
    # If not missing, return as-is (with basic sanitization)
    if not is_missing(value):
        # Basic sanitization for strings
        if isinstance(value, str):
            return value.strip()
        return value
    
    # Determine field category
    category = get_field_category(field_name)
    
    if category == "numeric":
        # Preserve null for numeric fields (important for stats/ML)
        return None
    
    if category == "placeholder_eligible" and use_placeholder:
        return MISSING_VALUE_PLACEHOLDER
    
    # For other fields, use placeholder if requested
    if use_placeholder:
        return MISSING_VALUE_PLACEHOLDER
    
    return None


def sanitize_value(value: Any, field_name: str) -> Any:
    """
    Sanitizes a value - handles missing values and applies basic cleaning.
    
    This is the main entry point for value sanitization in the pipeline.
    
    Args:
        value: The value to sanitize
        field_name: The field/column name
        
    Returns:
        The sanitized value
    """
    return handle_missing(value, field_name, use_placeholder=True)


def create_missing_indicators(raw_data: Dict[str, Any]) -> Dict[str, bool]:
    """
    Creates binary indicators for fields that were originally missing.
    
    This is useful for ML models where the fact that data was missing
    may itself be informative (Missing Not at Random - MNAR).
    
    Args:
        raw_data: The raw data dictionary
        
    Returns:
        Dictionary mapping field names to boolean (True = was missing)
    """
    indicators = {}
    for field_name, value in raw_data.items():
        indicators[f"{field_name}_was_missing"] = is_missing(value)
    return indicators
