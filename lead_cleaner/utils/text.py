import re

def clean_whitespace(s: str) -> str:
    """Removes extra whitespace and newlines."""
    if not s:
        return ""
    return re.sub(r'\s+', ' ', s).strip()
