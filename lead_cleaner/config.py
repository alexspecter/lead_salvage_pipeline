import os

# System Constraints
MEMORY_CAP_PERCENT = 0.95
FORCE_GC_COLLECT = True

# Processing
CHUNK_SIZE = 10
MAX_ROWS_PER_RUN = 10000
CONFIDENCE_THRESHOLD = 0.7

# Options
ENABLE_LLM = True
DRY_RUN = False
ENABLE_GENERIC_MODE = False  # Allows datasets without Email/Phone to be marked CLEAN if valid

# Paths (Relative to execution root)
DEFAULT_INPUT_DIR = "input"
DEFAULT_OUTPUT_DIR = "output"
DEFAULT_LOG_DIR = "logs"

# ============================================
# Missing Value Handling
# ============================================
# Industry-standard placeholder for missing/null values
MISSING_VALUE_PLACEHOLDER = "Not Provided"

# Values that indicate missing data (case-insensitive matching applied)
MISSING_VALUE_INDICATORS = {
    "", "N/A", "NA", "n/a", "na", "null", "NULL", "None", "none", 
    "-", "?", "missing", "unknown", "MISSING", "UNKNOWN", "NaN", "nan"
}

# Values that indicate missing data (case-insensitive matching applied)
# If a value matches any of these, it will be treated as None/Empty
# and processed according to the field category (placeholder vs null)

# ============================================
# Deduplication
# ============================================
DEDUP_ENABLED = True

# Strategies: "email_only", "phone_only", "composite", "all_fields", "disabled"
DEDUP_STRATEGY = "composite"

# Fields used for composite deduplication
DEDUP_COMPOSITE_FIELDS = ["email", "first_name", "last_name"]

# ============================================
# Dynamic Field Handling
# ============================================
# If True, output CSV will include ALL columns from input, not just standard fields
PRESERVE_ALL_FIELDS = True

# If True, normalizers are only applied to fields detected as Email/Phone/Name/Date/etc.
# Other fields are passed through sanitized but raw.
NORMALIZE_KNOWN_FIELDS_ONLY = True

# ============================================
# Security
# ============================================
STRICT_SECURITY_MODE = True
