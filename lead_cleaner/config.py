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

# Paths (Relative to execution root)
DEFAULT_INPUT_DIR = "input"
DEFAULT_OUTPUT_DIR = "output"
DEFAULT_LOG_DIR = "logs"
