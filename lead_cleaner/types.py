from typing import Dict, Optional, Any, TypedDict
from enum import Enum


class RowStatus(str, Enum):
    CLEAN = "CLEAN"
    AI_REQUIRED = "AI_REQUIRED"
    REJECTED = "REJECTED"


class FailureReason(str, Enum):
    DUPLICATE = "DUPLICATE"
    INVALID_FORMAT = "INVALID_FORMAT"
    MISSING_REQUIRED_FIELD = "MISSING_REQUIRED_FIELD"
    MODEL_CRASH = "MODEL_CRASH"
    VERIFICATION_FAILED = "VERIFICATION_FAILED"
    UNKNOWN = "UNKNOWN"


class ProcessingStatus(str, Enum):
    PENDING = "PENDING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class LeadRow(TypedDict):
    row_id: str  # UUID
    run_id: str  # UUID
    raw_data: Dict[str, Any]
    clean_data: Dict[str, Any]
    status: RowStatus
    failure_reason: Optional[FailureReason]
    confidence_score: float
    is_duplicate: bool
    duplicate_of: Optional[str]  # UUID
    validation_details: Dict[str, Any]  # Map of field -> NormalizerResult


class NormalizerResult(TypedDict):
    normalized_value: Any
    field_status: str  # Valid/Invalid/Fixed
    reason: Optional[str]
