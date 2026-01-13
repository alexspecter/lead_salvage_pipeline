import uuid
import hashlib
from typing import Any

def generate_row_id() -> str:
    """Generates a random UUID4 for a row."""
    return str(uuid.uuid4())

def generate_run_id() -> str:
    """Generates a random UUID4 for the run."""
    return str(uuid.uuid4())

def deterministic_uuid(content: str) -> str:
    """Generates a deterministic UUID based on content string."""
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, content))

def generate_fingerprint(data: Any) -> str:
    """Generates a SHA-256 hash of the input data."""
    # Ensure consistent string representation
    s = str(data).strip().lower()
    return hashlib.sha256(s.encode('utf-8')).hexdigest()
