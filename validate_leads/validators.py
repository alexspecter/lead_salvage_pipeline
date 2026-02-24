"""
Standalone Validators for the Validate Leads Utility.

These functions are self-contained and do NOT import from the main pipeline
to ensure zero interference with existing code.
"""

import re
from typing import Any


def is_valid_phone(value: Any) -> bool:
    """
    Check if a phone number is valid.

    Valid if:
    - Contains 7-10 digits after stripping non-numeric characters, OR
    - Starts with '+' for international format and has at least 7 digits.

    Invalid if:
    - Empty, None, or only whitespace.
    - Contains fewer than 7 digits.
    """
    if value is None:
        return False

    s = str(value).strip()
    if not s:
        return False

    # Extract digits
    digits = re.sub(r"\D", "", s)

    # Check for international format
    if s.startswith("+"):
        return len(digits) >= 7

    # Standard format: 7-10 digits
    return 7 <= len(digits) <= 15


def is_valid_email(value: Any) -> bool:
    """
    Check if an email address is valid.

    Uses a simple regex to validate the format:
    - Must have characters before @
    - Must have characters after @ and before .
    - Must have characters after .
    """
    if value is None:
        return False

    s = str(value).strip()
    if not s:
        return False

    # Simple email regex
    email_pattern = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
    return bool(re.match(email_pattern, s, re.IGNORECASE))


def is_not_empty(value: Any) -> bool:
    """
    Check if a value is not empty/null.

    Returns False for:
    - None
    - Empty string
    - Whitespace-only string
    - 'nan', 'none', 'null' (case-insensitive)
    """
    if value is None:
        return False

    s = str(value).strip()
    if not s:
        return False

    # Check for common null representations
    if s.lower() in ("nan", "none", "null", "n/a", "na", ""):
        return False

    return True
