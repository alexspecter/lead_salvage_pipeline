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
EMAIL_REGEX = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
PHONE_REGEX = re.compile(r"^[\+]?[(]?[0-9]{3}[)]?[-\s\.]?[0-9]{3}[-\s\.]?[0-9]{4,6}$")
# Updated: Support optional leading quote, negative sign, and whitespace
CURRENCY_REGEX = re.compile(r"^\s*'?\s*[\$€£¥₹]?\s*-?\d+(?:[\.,]\d+)?[KMBkmb]?\s*$")


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

    # Pre-process samples to strip potential security quotes for testing
    def _clean_sample(val: str) -> str:
        s = val.strip()
        if s.startswith("'") and len(s) > 1:
            return s[1:]
        return s

    clean_samples = [target for target in (_clean_sample(s) for s in samples) if target]

    if not clean_samples:
        return None

    # Check types in order of specificity

    # 1. Email (High confidence regex)
    if _check_all(clean_samples, _is_email):
        return FIELD_Email

    # 2. Currency (Has symbols or K/M suffixes)
    if _check_all(clean_samples, _is_currency):
        return "currency"

    # 3. Date (Parsable)
    if _check_any_threshold(clean_samples, _is_date, threshold=0.85):
        return FIELD_Date

    # 4. Phone (Regex)
    if _check_any_threshold(clean_samples, _is_phone, threshold=0.8):
        return FIELD_Phone

    return None


def _check_all(samples, check_func) -> bool:
    """Returns True if ALL samples pass the check function."""
    return all(check_func(x) for x in samples)


def _check_any_threshold(samples, check_func, threshold: float) -> bool:
    """Returns True if proportion of passing samples >= threshold."""
    count = sum(1 for x in samples if check_func(x))
    return (count / len(samples)) >= threshold


def _is_email(value: str) -> bool:
    return bool(EMAIL_REGEX.match(value.strip()))


def _is_phone(value: str) -> bool:
    # Cleanup strict characters for loose matching
    clean = re.sub(r"[\s\-\(\)\.]", "", value)
    if not clean.isdigit():
        return False
    # Stricter: phone numbers usually have 7-15 digits
    if value.isdigit() and len(value) < 10:
        return False

    return 7 <= len(clean) <= 15


def _is_date(value: str) -> bool:
    # Skip simple numbers/floats that dateutil might misinterpret
    s = value.strip()
    if s.replace(".", "", 1).isdigit():
        return False

    if not re.search(r"[/\-,\s]", s) and not any(
        m in s.lower()
        for m in [
            "jan",
            "feb",
            "mar",
            "apr",
            "may",
            "jun",
            "jul",
            "aug",
            "sep",
            "oct",
            "nov",
            "dec",
        ]
    ):
        return False

    try:
        parse(s, fuzzy=False)
        return True
    except Exception:
        return False


def _is_currency(value: str) -> bool:
    # Handle the quote inside the check if needed, but clean_samples handled it
    return bool(CURRENCY_REGEX.match(value.strip())) or bool(
        CURRENCY_REGEX.match("'" + value.strip())
    )


def parse_currency(value: str) -> Optional[float]:
    """
    Helper to convert currency string (e.g. €1.5M) to float.
    Handles negative numbers and security quotes.
    """
    if not value or pd.isna(value):
        return None

    s = str(value).strip().upper()

    # Strip security quote if present
    if s.startswith("'") and len(s) > 1:
        s = s[1:]

    # Extract numeric part including negative sign and optional decimal
    numeric_match = re.search(r"(-?\d+(?:\.\d+)?)", s)
    if not numeric_match:
        return None

    amount = float(numeric_match.group(1))

    if "K" in s:
        amount *= 1000
    elif "M" in s:
        amount *= 1000000
    elif "B" in s:
        amount *= 1000000000

    return amount
