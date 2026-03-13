import re

# Compiled regex covering major Unicode emoji ranges
_EMOJI_PATTERN = re.compile(
    "["
    "\U0001f600-\U0001f64f"  # Emoticons
    "\U0001f300-\U0001f5ff"  # Misc Symbols & Pictographs
    "\U0001f680-\U0001f6ff"  # Transport & Map Symbols
    "\U0001f1e0-\U0001f1ff"  # Flags (iOS)
    "\U00002702-\U000027b0"  # Dingbats
    "\U000024c2-\U0001f251"  # Enclosed chars & CJK symbols
    "\U0001f900-\U0001f9ff"  # Supplemental Symbols & Pictographs
    "\U0001fa00-\U0001fa6f"  # Chess Symbols
    "\U0001fa70-\U0001faff"  # Symbols & Pictographs Extended-A
    "\U00002600-\U000026ff"  # Misc Symbols
    "\U0000fe00-\U0000fe0f"  # Variation Selectors
    "\U0000200d"  # Zero Width Joiner
    "]+",
    flags=re.UNICODE,
)


def strip_emojis(s: str) -> str:
    """Removes emoji characters from a string."""
    if not s:
        return s
    return _EMOJI_PATTERN.sub("", s)


def clean_whitespace(s: str) -> str:
    """Removes extra whitespace and newlines."""
    if not s:
        return ""
    return re.sub(r"\s+", " ", s).strip()
