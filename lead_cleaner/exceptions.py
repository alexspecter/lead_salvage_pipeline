class LeadCleanerError(Exception):
    """Base exception for the lead cleaner pipeline."""

    pass


class ValidationError(LeadCleanerError):
    """Raised when data validation fails."""

    pass


class VerificationError(LeadCleanerError):
    """Raised when pipeline result verification fails."""

    pass


class MemoryLimitError(LeadCleanerError):
    """Raised when memory usage exceeds the safe threshold."""

    pass


class ConfigurationError(LeadCleanerError):
    """Raised when configuration is invalid."""

    pass


class SecurityViolationError(LeadCleanerError):
    """Raised when a hazardous file is detected and deleted."""

    pass


class FileTypeError(LeadCleanerError):
    """Raised when an unsupported file type is provided."""

    pass


class MalwareDetectedError(LeadCleanerError):
    """Raised when malware patterns or embedded threats are detected in a file."""

    pass


class FileSizeError(LeadCleanerError):
    """Raised when a file exceeds the maximum allowed size."""

    pass


class FileSignatureError(LeadCleanerError):
    """Raised when file magic bytes don't match the expected file type."""

    pass
