"""
Deduplication Module

Implements configurable deduplication strategies:
- email_only: Original behavior (dedupe by email)
- phone_only: Dedupe by phone number
- composite: Dedupe by combination of fields (recommended)
- all_fields: Dedupe by all clean fields (strictest)
- disabled: No deduplication

The strategy can be configured in config.py via DEDUP_STRATEGY.
"""

import pandas as pd
from typing import List, Set, Optional

from lead_cleaner.types import LeadRow, RowStatus, FailureReason
from lead_cleaner.logging.logger import PipelineLogger
from lead_cleaner.utils.uuid import generate_fingerprint
from lead_cleaner.constants import FIELD_Email, FIELD_Phone, FIELD_FirstName, FIELD_LastName
from lead_cleaner.config import DEDUP_ENABLED, DEDUP_STRATEGY, DEDUP_COMPOSITE_FIELDS


def detect_duplicates(rows: List[LeadRow], logger: PipelineLogger) -> List[LeadRow]:
    """
    Detects duplicates based on configurable strategy.
    Marks subsequent duplicates as REJECTED.
    
    Args:
        rows: List of LeadRow dictionaries
        logger: Pipeline logger instance
        
    Returns:
        The same list with duplicates marked as REJECTED
    """
    if not DEDUP_ENABLED:
        logger.log_event("PHASE_1", "DEDUP_DISABLED", reason="Deduplication is disabled")
        return rows
    
    strategy = DEDUP_STRATEGY.lower()
    
    if strategy == "disabled":
        logger.log_event("PHASE_1", "DEDUP_DISABLED", reason="Strategy set to disabled")
        return rows
    elif strategy == "email_only":
        return _dedup_by_single_field(rows, logger, FIELD_Email, "email")
    elif strategy == "phone_only":
        return _dedup_by_single_field(rows, logger, FIELD_Phone, "phone")
    elif strategy == "composite":
        return _dedup_by_composite(rows, logger, DEDUP_COMPOSITE_FIELDS)
    elif strategy == "all_fields":
        return _dedup_by_all_fields(rows, logger)
    else:
        logger.log_event("PHASE_1", "DEDUP_WARNING", 
                        reason=f"Unknown strategy '{strategy}', defaulting to composite")
        return _dedup_by_composite(rows, logger, DEDUP_COMPOSITE_FIELDS)


def _get_field_value(row: LeadRow, field_name: str) -> Optional[str]:
    """
    Gets a field value from clean_data or raw_data, normalized for comparison.
    """
    value = row["clean_data"].get(field_name) or row["raw_data"].get(field_name)
    if value is None:
        return None
    return str(value).strip().lower()


def _dedup_by_single_field(
    rows: List[LeadRow], 
    logger: PipelineLogger, 
    field_name: str,
    field_label: str
) -> List[LeadRow]:
    """
    Deduplicates by a single field (email or phone).
    """
    seen_fingerprints: Set[str] = set()
    
    for row in rows:
        if row["status"] == RowStatus.REJECTED:
            continue
        
        value = _get_field_value(row, field_name)
        
        # If field is missing, can't fingerprint - use row_id
        if not value:
            content = f"row_id:{row['row_id']}"
        else:
            content = f"{field_label}:{value}"
        
        fp = generate_fingerprint(content)
        
        if fp in seen_fingerprints:
            _mark_as_duplicate(row, content, logger)
        else:
            seen_fingerprints.add(fp)
            row["is_duplicate"] = False
    
    return rows


def _dedup_by_composite(
    rows: List[LeadRow], 
    logger: PipelineLogger,
    fields: List[str]
) -> List[LeadRow]:
    """
    Deduplicates by a composite key made from multiple fields.
    
    This is the recommended strategy as it allows different records
    with same email but different names (e.g., name corrections,
    different people sharing email in some contexts).
    """
    seen_fingerprints: Set[str] = set()
    
    for row in rows:
        if row["status"] == RowStatus.REJECTED:
            continue
        
        # Build composite key from all specified fields
        key_parts = []
        for field in fields:
            value = _get_field_value(row, field)
            if value:
                key_parts.append(f"{field}:{value}")
        
        # If no fields have values, use row_id
        if not key_parts:
            content = f"row_id:{row['row_id']}"
        else:
            content = "|".join(sorted(key_parts))
        
        fp = generate_fingerprint(content)
        
        if fp in seen_fingerprints:
            _mark_as_duplicate(row, content, logger)
        else:
            seen_fingerprints.add(fp)
            row["is_duplicate"] = False
    
    return rows


def _dedup_by_all_fields(
    rows: List[LeadRow], 
    logger: PipelineLogger
) -> List[LeadRow]:
    """
    Strictest deduplication - considers ALL clean fields.
    Only marks as duplicate if entire record matches.
    """
    seen_fingerprints: Set[str] = set()
    
    for row in rows:
        if row["status"] == RowStatus.REJECTED:
            continue
        
        # Build composite key from ALL clean data fields
        key_parts = []
        for field, value in sorted(row["clean_data"].items()):
            if value is not None:
                key_parts.append(f"{field}:{str(value).strip().lower()}")
        
        # If no clean data, use row_id
        if not key_parts:
            content = f"row_id:{row['row_id']}"
        else:
            content = "|".join(key_parts)
        
        fp = generate_fingerprint(content)
        
        if fp in seen_fingerprints:
            _mark_as_duplicate(row, "all_fields_match", logger)
        else:
            seen_fingerprints.add(fp)
            row["is_duplicate"] = False
    
    return rows


def _mark_as_duplicate(row: LeadRow, content: str, logger: PipelineLogger) -> None:
    """
    Marks a row as duplicate and logs the event.
    """
    row["is_duplicate"] = True
    row["status"] = RowStatus.REJECTED
    row["failure_reason"] = FailureReason.DUPLICATE
    
    logger.log_event(
        phase="PHASE_1",
        action="DUPLICATE_DETECTED",
        row_id=row["row_id"],
        reason=f"Fingerprint collision: {content}"
    )
