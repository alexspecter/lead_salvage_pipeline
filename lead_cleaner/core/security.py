"""
Security & Decontamination Module

Provides multi-layer defense-in-depth security checks for input files:
1. File size validation (prevent resource exhaustion)
2. File signature validation (magic bytes)
3. File extension validation (whitelist/blacklist)
4. Hash-based threat detection (local blocklist)
5. Malware pattern scanning (executable signatures, scripts)
6. DOCX macro detection (VBA, embedded objects)
7. CSV injection sanitization

CRITICAL: Files are NEVER executed - only read in binary/text mode.
All parsing is done safely with proper exception handling.
"""

import os
import csv
import hashlib
import zipfile

from lead_cleaner.config import (
    ALLOWED_FILE_EXTENSIONS,
    HAZARDOUS_FILE_EXTENSIONS,
    KNOWN_MALICIOUS_HASHES,
    MAX_FILE_SIZE_BYTES,
    FILE_SIGNATURES,
    MALWARE_PATTERNS,
    DANGEROUS_TEXT_PATTERNS,
    DOCX_DANGEROUS_COMPONENTS,
)
from lead_cleaner.exceptions import (
    SecurityViolationError,
    FileTypeError,
    MalwareDetectedError,
    FileSizeError,
    FileSignatureError,
)


# Dangerous characters that could trigger formula execution in Excel/Sheets
INJECTION_PREFIXES = ("=", "+", "-", "@")


def check_file_size(file_path: str, logger) -> None:
    """
    Verify file size is within acceptable limits.

    Args:
        file_path: Path to the file
        logger: PipelineLogger instance

    Raises:
        FileSizeError: If file exceeds MAX_FILE_SIZE_BYTES
    """
    file_size = os.path.getsize(file_path)
    max_size_mb = MAX_FILE_SIZE_BYTES / (1024 * 1024)

    if file_size > MAX_FILE_SIZE_BYTES:
        logger.log_event(
            phase="SECURITY",
            action="FILE_SIZE_EXCEEDED",
            reason=f"File size {file_size / (1024 * 1024):.2f} MB exceeds limit of {max_size_mb:.0f} MB",
        )
        raise FileSizeError(
            f"File size ({file_size / (1024 * 1024):.2f} MB) exceeds maximum allowed "
            f"size of {max_size_mb:.0f} MB"
        )

    logger.log_event(
        phase="SECURITY",
        action="FILE_SIZE_OK",
        reason=f"File size {file_size / (1024 * 1024):.2f} MB within limits",
    )


def validate_magic_bytes(file_path: str, logger) -> None:
    """
    Verify file magic bytes match the expected file type.

    This prevents files from masquerading as different types.
    CSV files are text-based and validated by checking for valid UTF-8.

    Args:
        file_path: Path to the file
        logger: PipelineLogger instance

    Raises:
        FileSignatureError: If magic bytes don't match expected type
    """
    ext = os.path.splitext(file_path)[1].lower()

    if ext == ".csv":
        # CSV should be valid text - check for binary content
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                # Read first 8KB to verify it's text
                sample = f.read(8192)
                # Check for null bytes (binary content indicator)
                if "\x00" in sample:
                    logger.log_event(
                        phase="SECURITY",
                        action="INVALID_FILE_SIGNATURE",
                        reason="CSV file contains binary content (null bytes detected)",
                    )
                    raise FileSignatureError(
                        "File claims to be CSV but contains binary content. "
                        "This may indicate a disguised malicious file."
                    )
        except UnicodeDecodeError:
            logger.log_event(
                phase="SECURITY",
                action="INVALID_FILE_SIGNATURE",
                reason="CSV file is not valid UTF-8 text",
            )
            raise FileSignatureError(
                "File claims to be CSV but is not valid UTF-8 text. "
                "This may indicate a disguised malicious file."
            )

    elif ext in (".db", ".sqlite"):
        # SQLite files should start with "SQLite format 3\0"
        expected_magic = b"SQLite format 3\x00"
        with open(file_path, "rb") as f:
            actual_magic = f.read(len(expected_magic))

        if actual_magic != expected_magic:
            logger.log_event(
                phase="SECURITY",
                action="INVALID_FILE_SIGNATURE",
                reason=f"Expected SQLite signature, got: {actual_magic[:16]!r}",
            )
            raise FileSignatureError(
                "File claims to be SQLite database but magic bytes don't match. "
                "This may indicate a corrupted or malicious file."
            )

    elif ext in FILE_SIGNATURES:
        expected_magic, description = FILE_SIGNATURES[ext]

        with open(file_path, "rb") as f:
            actual_magic = f.read(len(expected_magic))

        if actual_magic != expected_magic:
            logger.log_event(
                phase="SECURITY",
                action="INVALID_FILE_SIGNATURE",
                reason=f"Expected {description} signature, got: {actual_magic[:10]!r}",
            )
            raise FileSignatureError(
                f"File claims to be {ext} but magic bytes don't match. "
                f"Expected: {description}. This may indicate a disguised malicious file."
            )

    logger.log_event(
        phase="SECURITY",
        action="MAGIC_BYTES_VALIDATED",
        reason=f"File signature validated for {ext}",
    )


def scan_for_malware_patterns(file_path: str, logger) -> None:
    """
    Scan file content for known malware patterns and dangerous content.

    This reads the file in BINARY mode only - never executes anything.

    Args:
        file_path: Path to the file
        logger: PipelineLogger instance

    Raises:
        MalwareDetectedError: If dangerous patterns are found
    """
    # Read raw bytes for pattern matching
    with open(file_path, "rb") as f:
        content = f.read()

    # Check for binary malware signatures
    for pattern, description in MALWARE_PATTERNS.items():
        if pattern in content:
            logger.log_event(
                phase="SECURITY",
                action="MALWARE_PATTERN_DETECTED",
                reason=f"CRITICAL: {description} detected in file",
            )
            print(f"🚨 MALWARE DETECTED: {description}")
            print(f"   File contains dangerous pattern: {pattern[:20]!r}")

            # Delete the malicious file
            try:
                os.remove(file_path)
            except OSError:
                pass

            raise MalwareDetectedError(
                f"Dangerous content detected: {description}. File has been deleted."
            )

    # For text-based files, also check string patterns
    ext = os.path.splitext(file_path)[1].lower()
    if ext in {".csv"}:
        try:
            text_content = content.decode("utf-8", errors="ignore")
            for pattern in DANGEROUS_TEXT_PATTERNS:
                if pattern.lower() in text_content.lower():
                    logger.log_event(
                        phase="SECURITY",
                        action="DANGEROUS_PATTERN_DETECTED",
                        reason=f"CRITICAL: Dangerous text pattern '{pattern}' detected",
                    )
                    print(f"🚨 DANGEROUS CONTENT: Pattern '{pattern}' detected")

                    try:
                        os.remove(file_path)
                    except OSError:
                        pass

                    raise MalwareDetectedError(
                        f"Dangerous content pattern detected: {pattern}. File has been deleted."
                    )
        except Exception:
            pass  # If decode fails, binary scan already completed

    logger.log_event(
        phase="SECURITY",
        action="MALWARE_SCAN_COMPLETE",
        reason="No malware patterns detected",
    )


def check_docx_for_macros(file_path: str, logger) -> None:
    """
    Check DOCX file for VBA macros, embedded scripts, or OLE objects.

    DOCX files are ZIP archives. We inspect the contents WITHOUT
    executing anything - just checking for dangerous component names.

    Args:
        file_path: Path to the DOCX file
        logger: PipelineLogger instance

    Raises:
        MalwareDetectedError: If macros or dangerous components are found
    """
    ext = os.path.splitext(file_path)[1].lower()
    if ext != ".docx":
        return  # Only check DOCX files

    try:
        with zipfile.ZipFile(file_path, "r") as zf:
            # Get list of all files in the archive
            file_list = zf.namelist()

            for zip_entry in file_list:
                entry_lower = zip_entry.lower()

                # Check for dangerous components
                for dangerous in DOCX_DANGEROUS_COMPONENTS:
                    if dangerous.lower() in entry_lower:
                        logger.log_event(
                            phase="SECURITY",
                            action="MACRO_DETECTED",
                            reason=f"CRITICAL: Dangerous component '{zip_entry}' found in DOCX",
                        )
                        print(f"🚨 MACRO DETECTED: Document contains '{zip_entry}'")
                        print("   This file may contain malicious code!")

                        # Delete the file
                        try:
                            os.remove(file_path)
                        except OSError:
                            pass

                        raise MalwareDetectedError(
                            f"DOCX contains dangerous component: {zip_entry}. "
                            f"VBA macros and embedded scripts are not allowed. "
                            f"File has been deleted."
                        )

            logger.log_event(
                phase="SECURITY",
                action="DOCX_MACRO_CHECK_PASSED",
                reason="No macros or dangerous components found in DOCX",
            )

    except zipfile.BadZipFile:
        logger.log_event(
            phase="SECURITY",
            action="INVALID_DOCX",
            reason="File is not a valid ZIP/DOCX archive",
        )
        raise FileSignatureError(
            "File claims to be DOCX but is not a valid ZIP archive. "
            "This may indicate a corrupted or malicious file."
        )


def validate_file_extension(file_path: str, logger) -> None:
    """
    Validate that the file has a supported extension.

    Args:
        file_path: Path to the input file
        logger: PipelineLogger instance for logging events

    Raises:
        SecurityViolationError: If file has a hazardous extension (file is deleted)
        FileTypeError: If file has an unsupported extension
    """
    ext = os.path.splitext(file_path)[1].lower()

    # Check for hazardous extensions first
    if ext in HAZARDOUS_FILE_EXTENSIONS:
        logger.log_event(
            phase="SECURITY",
            action="HAZARDOUS_FILE_DETECTED",
            reason=f"CRITICAL: Hazardous file extension '{ext}' detected. Deleting file.",
        )
        print(f"🚨 SECURITY VIOLATION: Hazardous file type '{ext}' detected!")
        print(f"   File has been quarantined and deleted: {file_path}")

        # Delete the file immediately
        try:
            os.remove(file_path)
        except OSError:
            pass  # File may already be gone or inaccessible

        raise SecurityViolationError(
            f"Hazardous file extension '{ext}' is not allowed. File has been deleted."
        )

    # Check if extension is in the supported whitelist
    if ext not in ALLOWED_FILE_EXTENSIONS:
        logger.log_event(
            phase="SECURITY",
            action="UNSUPPORTED_FILE_TYPE",
            reason=f"File extension '{ext}' is not supported.",
        )
        supported = ", ".join(sorted(ALLOWED_FILE_EXTENSIONS))
        raise FileTypeError(
            f"File extension '{ext}' is not supported. "
            f"Supported file types: {supported}"
        )

    logger.log_event(
        phase="SECURITY",
        action="EXTENSION_VALIDATED",
        reason=f"File extension '{ext}' is supported.",
    )


def compute_file_hash(file_path: str, algorithm: str = "sha256") -> str:
    """
    Compute the hash of a file.

    Args:
        file_path: Path to the file
        algorithm: Hash algorithm to use ('md5', 'sha1', 'sha256')

    Returns:
        Hexadecimal hash string
    """
    hash_func = hashlib.new(algorithm)

    with open(file_path, "rb") as f:
        # Read in chunks to handle large files
        for chunk in iter(lambda: f.read(8192), b""):
            hash_func.update(chunk)

    return hash_func.hexdigest()


def check_hash_against_threats(file_hash: str, logger) -> bool:
    """
    Check if a file hash matches known malicious hashes.

    Args:
        file_hash: SHA256 hash of the file
        logger: PipelineLogger instance

    Returns:
        True if threat detected, False otherwise
    """
    if file_hash in KNOWN_MALICIOUS_HASHES:
        logger.log_event(
            phase="SECURITY",
            action="KNOWN_THREAT_DETECTED",
            reason=f"CRITICAL: File hash matches known malicious file: {file_hash[:16]}...",
        )
        print("🚨 THREAT DETECTED: File matches known malicious hash!")
        return True

    logger.log_event(
        phase="SECURITY",
        action="HASH_CHECK_PASSED",
        reason="File hash not found in threat database.",
    )
    return False


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

    # Read and scan the raw CSV (read-only mode)
    with open(file_path, "r", newline="", encoding="utf-8") as infile:
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
    with open(sanitized_path, "w", newline="", encoding="utf-8") as outfile:
        writer = csv.writer(outfile)
        writer.writerows(sanitized_rows)

    # Log the security event
    if sanitized_count > 0:
        # Notify user on console
        print(
            f"⚠️  SECURITY NOTICE: Detected and sanitized {sanitized_count} cells with potentially dangerous patterns."
        )
        print(
            f"   Patterns (=, +, -, @) have been neutralized. Data preserved in: {sanitized_path}"
        )

        logger.log_event(
            phase="SECURITY",
            action="SANITIZATION_COMPLETE",
            reason=f"SECURITY_EVENT: Sanitized {sanitized_count} potential injection cells.",
        )
    else:
        logger.log_event(
            phase="SECURITY",
            action="SCAN_COMPLETE",
            reason="No injection patterns detected.",
        )

    return sanitized_path


def run_security_checks(file_path: str, logger, tmp_dir: str = ".tmp") -> str:
    """
    Run ALL security checks on an input file.

    This is the main entry point for security validation. It performs
    defense-in-depth checks in order of severity:

    1. File size validation (prevent resource exhaustion)
    2. File extension validation (whitelist/blacklist)
    3. Magic bytes validation (verify file is what it claims)
    4. Malware pattern scanning (detect embedded threats)
    5. DOCX macro detection (for DOCX files)
    6. Hash-based threat detection (local blocklist)
    7. CSV injection sanitization (for CSV files)

    CRITICAL: Files are NEVER executed - only read in binary/text mode.

    Args:
        file_path: Path to the input file
        logger: PipelineLogger instance
        tmp_dir: Directory for sanitized output

    Returns:
        Path to the sanitized/validated file

    Raises:
        SecurityViolationError: If file is hazardous
        FileTypeError: If file type is not supported
        FileSizeError: If file exceeds size limits
        FileSignatureError: If magic bytes don't match
        MalwareDetectedError: If malware patterns detected
    """
    logger.log_event(
        phase="SECURITY",
        action="SECURITY_CHECK_START",
        reason=f"Running defense-in-depth security checks on: {os.path.basename(file_path)}",
    )

    print(f"🔒 Running security checks on: {os.path.basename(file_path)}")

    # 1. Check file size first (cheap operation)
    check_file_size(file_path, logger)

    # 2. Validate file extension
    validate_file_extension(file_path, logger)

    # 3. Validate magic bytes (verify file type)
    validate_magic_bytes(file_path, logger)

    # 4. Scan for malware patterns
    scan_for_malware_patterns(file_path, logger)

    # 5. For DOCX files, check for macros
    ext = os.path.splitext(file_path)[1].lower()
    if ext == ".docx":
        check_docx_for_macros(file_path, logger)

    # 6. Compute file hash and check against known threats
    file_hash = compute_file_hash(file_path)
    if check_hash_against_threats(file_hash, logger):
        # Delete the malicious file
        try:
            os.remove(file_path)
        except OSError:
            pass
        raise SecurityViolationError(
            "File matches known malicious hash. File has been deleted."
        )

    # 7. For CSV files, run injection sanitization
    if ext == ".csv":
        result_path = scan_and_secure(file_path, logger, tmp_dir)
        print("✅ All security checks passed. File sanitized.")
        return result_path

    # For other supported types, return original path
    logger.log_event(
        phase="SECURITY",
        action="SECURITY_CHECK_COMPLETE",
        reason="All defense-in-depth security checks passed.",
    )
    print("✅ All security checks passed.")
    return file_path
