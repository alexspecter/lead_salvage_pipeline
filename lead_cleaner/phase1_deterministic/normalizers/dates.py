from typing import Tuple, Optional
import pandas as pd
from lead_cleaner.types import NormalizerResult

def normalize_date(value: Optional[str]) -> NormalizerResult:
    if not value:
         return {
            "normalized_value": None,
            "field_status": "MISSING",
            "reason": "Empty value"
        }
        
    try:
        # Use pandas for robust parsing
        dt = pd.to_datetime(value, errors='raise')
        formatted = dt.strftime("%Y-%m-%d")
        return {
            "normalized_value": formatted,
            "field_status": "VALID",
            "reason": None
        }
    except Exception:
        return {
            "normalized_value": value,
            "field_status": "INVALID",
            "reason": "Date parse failed"
        }
