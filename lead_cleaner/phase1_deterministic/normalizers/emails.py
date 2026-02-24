from typing import Optional
import re
from lead_cleaner.types import NormalizerResult


def normalize_email(value: Optional[str]) -> NormalizerResult:
    if not value or not isinstance(value, str):
        return {
            "normalized_value": None,
            "field_status": "MISSING",
            "reason": "Empty value",
        }

    # Basic cleaning
    cleaned = value.strip().lower()

    # Remove common garbage chars
    cleaned = re.sub(r"[\s\t\n]+", "", cleaned)

    # Basic regex validation
    # This is a permissive regex
    email_regex = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"

    if not re.match(email_regex, cleaned):
        return {
            "normalized_value": value,  # Return original if invalid
            "field_status": "INVALID",
            "reason": "Invalid format",
        }

    return {"normalized_value": cleaned, "field_status": "VALID", "reason": None}
