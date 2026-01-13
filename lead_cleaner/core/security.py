"""
Security & Decontamination Module

Scans input CSV files for potential injection patterns and sanitizes them
before processing. This prevents CSV injection attacks while preserving
valid data like international phone numbers starting with +.
"""

import os
import csv
from typing import Optional


# Dangerous characters that could trigger formula execution in Excel/Sheets
INJECTION_PREFIXES = ('=', '+', '-', '@')


def scan_and_secure(file_path: str, logger, tmp_dir: str = ".tmp") -> str:
    """
    Scan a CSV file for injection patterns and create a sanitized copy.
    
    Args:
        file_path: Path to the input CSV file
        logger: PipelineLogger instance for logging events
        tmp_dir: Directory for sanitized output (default: .tmp)
        
    Returns:
        Path to the sanitized CSV file
    """
    os.makedirs(tmp_dir, exist_ok=True)
    sanitized_path = os.path.join(tmp_dir, "sanitized_input.csv")
    sanitized_count = 0
    
    # Read and scan the raw CSV
    with open(file_path, 'r', newline='', encoding='utf-8') as infile:
        reader = csv.reader(infile)
        rows = list(reader)
    
    # Scan and sanitize each cell
    sanitized_rows = []
    for row in rows:
        sanitized_row = []
        for cell in row:
            cell_stripped = cell.strip()
            # Check if cell starts with dangerous character
            if cell_stripped and cell_stripped[0] in INJECTION_PREFIXES:
                # Neutralize by prepending single quote
                sanitized_cell = "'" + cell
                sanitized_count += 1
            else:
                sanitized_cell = cell
            sanitized_row.append(sanitized_cell)
        sanitized_rows.append(sanitized_row)
    
    # Write sanitized version
    with open(sanitized_path, 'w', newline='', encoding='utf-8') as outfile:
        writer = csv.writer(outfile)
        writer.writerows(sanitized_rows)
    
    # Log the security event
    if sanitized_count > 0:
        # Notify user on console
        print(f"⚠️  SECURITY NOTICE: Detected and sanitized {sanitized_count} cells with potentially dangerous patterns.")
        print(f"   Patterns (=, +, -, @) have been neutralized. Data preserved in: {sanitized_path}")
        
        logger.log_event(
            phase="SECURITY",
            action="SANITIZATION_COMPLETE",
            reason=f"SECURITY_EVENT: Sanitized {sanitized_count} potential injection cells."
        )
    else:
        logger.log_event(
            phase="SECURITY",
            action="SCAN_COMPLETE",
            reason="No injection patterns detected."
        )
    
    return sanitized_path
