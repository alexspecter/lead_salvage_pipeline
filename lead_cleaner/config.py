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

# Supported file extensions (whitelist)
ALLOWED_FILE_EXTENSIONS = {".csv", ".docx", ".db"}

# Hazardous file extensions (blacklist) - these will be deleted on sight
HAZARDOUS_FILE_EXTENSIONS = {
    ".exe", ".bat", ".sh", ".cmd", ".com", ".msi", ".scr",
    ".js", ".vbs", ".wsf", ".ps1", ".jar", ".py", ".rb",
    ".dll", ".so", ".dylib"
}

# Known malicious file hashes (SHA256) - local blocklist
# Add known bad hashes here; VirusTotal integration can be added later
KNOWN_MALICIOUS_HASHES = {
    # Example placeholder hashes (not real threats)
    # "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",  # empty file hash
}

# ============================================
# Defense-in-Depth Safeguards
# ============================================

# Maximum file size in bytes (50 MB default)
MAX_FILE_SIZE_BYTES = 50 * 1024 * 1024

# Maximum decompressed size for archive-based files like DOCX (200 MB)
MAX_DECOMPRESSED_SIZE_BYTES = 200 * 1024 * 1024

# Processing timeout in seconds (prevents infinite loops)
PROCESSING_TIMEOUT_SECONDS = 300  # 5 minutes

# Magic bytes (file signatures) for supported file types
# Format: extension -> (magic_bytes, description)
FILE_SIGNATURES = {
    ".docx": (b"PK\x03\x04", "ZIP archive (DOCX)"),
    ".db": (b"SQLite format 3\x00", "SQLite database"),
    # CSV is text-based, validated separately
}

# Patterns that indicate embedded malware or dangerous content
# These are checked against raw file bytes/content
MALWARE_PATTERNS = {
    # Executable signatures
    b"MZ": "Windows executable (PE)",
    b"\x7fELF": "Linux executable (ELF)",
    b"#!": "Shebang script",
    
    # Dangerous script patterns (as bytes for binary scanning)
    b"powershell": "PowerShell command",
    b"Invoke-Expression": "PowerShell code execution",
    b"WScript.Shell": "Windows Script Host",
    b"cmd.exe": "Windows command shell",
    
    # Web-based threats
    b"<script": "Embedded JavaScript",
    b"javascript:": "JavaScript URL",
    b"data:text/html": "Data URL with HTML",
    b"vbscript:": "VBScript URL",
    
    # SQL injection indicators
    b"DROP TABLE": "SQL DROP statement",
    b"DELETE FROM": "SQL DELETE statement",
    b"TRUNCATE TABLE": "SQL TRUNCATE statement",
    b"'; --": "SQL injection pattern",
    b"OR 1=1": "SQL injection pattern",
}

# Dangerous content patterns for text-based files (checked as strings)
DANGEROUS_TEXT_PATTERNS = [
    "=cmd|",  # Excel DDE attack
    "=HYPERLINK(",  # Excel hyperlink injection
    "@SUM(",  # Malicious formula
    "IMPORTXML(",  # Google Sheets data exfiltration
]

# Files that indicate macros in DOCX (should not exist in safe documents)
DOCX_DANGEROUS_COMPONENTS = {
    "vbaProject.bin",  # VBA macro storage
    "vbaData.xml",     # VBA data
    ".vbs",            # VBScript files
    ".js",             # JavaScript files
    "oleObject",       # Embedded OLE objects
}
