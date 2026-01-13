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
