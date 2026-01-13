"""
Type Inference Module

Implements content-based inference to detect field types when header mapping fails.
Strategies:
1. Regex matching for standardized formats (Email, Phone)
2. parsing attempts for dates
3. Symbol/Pattern detection for Currency/Numeric
"""

import pandas as pd
import re
from typing import Optional
from dateutil.parser import parse
from lead_cleaner.constants import FIELD_Email, FIELD_Phone, FIELD_Date

# Regex Patterns
EMAIL_REGEX = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
PHONE_REGEX = re.compile(r'^[\+]?[(]?[0-9]{3}[)]?[-\s\.]?[0-9]{3}[-\s\.]?[0-9]{4,6}$')
CURRENCY_REGEX = re.compile(r'^[\$€£¥₹] ?\d+(?:[\.,]\d+)?[KMBkmb]?$')

def infer_column_type(series: pd.Series, sample_size: int = 10) -> Optional[str]:
    """
    Infers the data type of a pandas Series by sampling non-null values.
    
    Returns:
        Canonical field type string (e.g., 'email', 'phone', 'date', 'currency')
        or None if no specific type is detected.
    """
    # Get non-null, non-empty samples
    samples = series.dropna().astype(str)
    samples = samples[samples.str.strip() != ""].head(sample_size)
    
    if samples.empty:
        return None
        
    # Check types in order of specificity
    
    # 1. Email (High confidence regex)
    if _check_all(samples, _is_email):
        return FIELD_Email
        
    # 2. Currency (Has symbols or K/M suffixes)
    if _check_all(samples, _is_currency):
        return "currency"
        
    # 3. Date (Parsable)
    # We use a threshold here because date parsing can be aggressive
    if _check_all(samples, _is_date):
        return FIELD_Date
        
    # 4. Phone (Regex)
    if _check_any_threshold(samples, _is_phone, threshold=0.8):
        return FIELD_Phone
        
    return None

def _check_all(samples: pd.Series, check_func) -> bool:
    """Returns True if ALL samples pass the check function."""
    return all(check_func(x) for x in samples)

def _check_any_threshold(samples: pd.Series, check_func, threshold: float) -> bool:
    """Returns True if proportion of passing samples >= threshold."""
    count = sum(1 for x in samples if check_func(x))
    return (count / len(samples)) >= threshold

def _is_email(value: str) -> bool:
    return bool(EMAIL_REGEX.match(value.strip()))

def _is_phone(value: str) -> bool:
    # Cleanup strict characters for loose matching
    clean = re.sub(r'[\s\-\(\)\.]', '', value)
    if not clean.isdigit():
        return False
    # Stricter: phone numbers usually have 7-15 digits
    # And preferably usually have some separators if they are standardized
    # But clean data might not. 
    # Let's enforce: If it's pure digits, must be at least 10 chars to be safe (avoid IDs like 1234567)
    if value.isdigit() and len(value) < 10:
        return False
        
    return 7 <= len(clean) <= 15

def _is_date(value: str) -> bool:
    # Skip simple numbers/floats that dateutil might misinterpret
    s = value.strip()
    if s.replace('.','',1).isdigit():
        return False
        
    # Must have some date-like characteristics (separators or month names)
    # prevent "Start" or "End" being parsed as dates
    if not re.search(r'[/\-,\s]', s) and not any(m in s.lower() for m in ['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']):
        return False
        
    try:
        # fuzzy=False ensures we don't parse "My cat is 10" as a date
        parse(s, fuzzy=False)
        return True
    except:
        return False

def _is_currency(value: str) -> bool:
    return bool(CURRENCY_REGEX.match(value.strip()))

def parse_currency(value: str) -> Optional[float]:
    """
    Helper to convert currency string (e.g. €1.5M) to float.
    """
    if not value or pd.isna(value):
        return None
        
    s = str(value).strip().upper()
    
    # Extract numeric part and multiplier
    numeric_match = re.search(r'(\d+(?:\.\d+)?)', s)
    if not numeric_match:
        return None
        
    amount = float(numeric_match.group(1))
    
    if 'K' in s:
        amount *= 1000
    elif 'M' in s:
        amount *= 1000000
    elif 'B' in s:
        amount *= 1000000000
        
    return amount
