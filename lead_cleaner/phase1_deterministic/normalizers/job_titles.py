import re
from typing import Optional
from lead_cleaner.types import NormalizerResult
from lead_cleaner.utils.text import clean_whitespace, strip_emojis
from lead_cleaner.constants import PROFESSIONAL_ACRONYMS

# Words that should remain lowercase in titles (unless first word)
LOWERCASE_WORDS = {
    "of",
    "and",
    "the",
    "a",
    "an",
    "in",
    "on",
    "at",
    "for",
    "to",
    "with",
    "by",
    "from",
}


def normalize_job_title(value: Optional[str]) -> NormalizerResult:
    if not value:
        return {
            "normalized_value": None,
            "field_status": "MISSING",
            "reason": "Empty value",
        }

    cleaned = strip_emojis(str(value))
    cleaned = clean_whitespace(cleaned)

    # Proper title casing: capitalize first letter of each word, but keep certain words lowercase
    words = cleaned.split()
    title_cased = []
    for i, word in enumerate(words):
        if i == 0:
            # Always capitalize first word
            title_cased.append(word.capitalize())
        elif word.lower() in LOWERCASE_WORDS:
            # Keep articles/prepositions lowercase
            title_cased.append(word.lower())
        else:
            title_cased.append(word.capitalize())

    cleaned = " ".join(title_cased)

    # Apply acronym protection
    # Fix any acronyms that were capitalized incorrectly (e.g., "Hr" -> "HR")
    for acronym in PROFESSIONAL_ACRONYMS:
        # Use word boundaries to avoid matching inside other words
        pattern = rf"\b{re.escape(acronym)}\b"
        cleaned = re.sub(pattern, acronym, cleaned, flags=re.IGNORECASE)

    return {"normalized_value": cleaned, "field_status": "VALID", "reason": None}
