"""
Tests for the Security & Decontamination Module
"""

import os
import sys
import csv
import tempfile
import unittest

# Add parent directory for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lead_cleaner.core.security import (
    scan_and_secure, 
    INJECTION_PREFIXES,
    validate_file_extension,
    compute_file_hash,
    check_hash_against_threats,
    run_security_checks
)
from lead_cleaner.exceptions import SecurityViolationError, FileTypeError


class MockLogger:
    """Mock logger for testing"""
    def __init__(self):
        self.events = []
    
    def log_event(self, phase, action, reason=None, **kwargs):
        self.events.append({
            'phase': phase,
            'action': action,
            'reason': reason
        })


class TestSecurityModule(unittest.TestCase):
    
    def setUp(self):
        self.logger = MockLogger()
        self.temp_dir = tempfile.mkdtemp()
        self.tmp_output = os.path.join(self.temp_dir, "tmp")
        
    def tearDown(self):
        # Clean up temp files
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def _create_test_csv(self, rows):
        """Helper to create test CSV files"""
        path = os.path.join(self.temp_dir, "test_input.csv")
        with open(path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerows(rows)
        return path
    
    def test_detects_equals_pattern(self):
        """Test detection of = at cell start (benign formula-like pattern)"""
        csv_path = self._create_test_csv([
            ["Name", "Note"],
            ["John", "=TEST_PATTERN"]  # Benign test pattern
        ])
        
        result_path = scan_and_secure(csv_path, self.logger, self.tmp_output)
        
        with open(result_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            rows = list(reader)
        
        # Should be sanitized with leading quote
        self.assertEqual(rows[1][1], "'=TEST_PATTERN")
    
    def test_detects_plus_pattern(self):
        """Test detection of + at cell start (international phone format)"""
        csv_path = self._create_test_csv([
            ["Name", "Phone"],
            ["Jane", "+1-555-0199"]  # Standard phone number format
        ])
        
        result_path = scan_and_secure(csv_path, self.logger, self.tmp_output)
        
        with open(result_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            rows = list(reader)
        
        # Phone number sanitized but data preserved
        self.assertEqual(rows[1][1], "'+1-555-0199")
    
    def test_detects_minus_pattern(self):
        """Test detection of - at cell start (negative number)"""
        csv_path = self._create_test_csv([
            ["Name", "Value"],
            ["Test", "-100"]  # Simple negative number
        ])
        
        result_path = scan_and_secure(csv_path, self.logger, self.tmp_output)
        
        with open(result_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            rows = list(reader)
        
        self.assertEqual(rows[1][1], "'-100")
    
    def test_detects_at_pattern(self):
        """Test detection of @ at cell start (benign pattern)"""
        csv_path = self._create_test_csv([
            ["Name", "Handle"],
            ["User", "@username"]  # Social media handle format
        ])
        
        result_path = scan_and_secure(csv_path, self.logger, self.tmp_output)
        
        with open(result_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            rows = list(reader)
        
        self.assertEqual(rows[1][1], "'@username")
    
    def test_clean_file_unchanged(self):
        """Test that clean files pass through without modification"""
        csv_path = self._create_test_csv([
            ["Name", "City", "Country"],
            ["Alice", "New York", "USA"],
            ["Bob", "London", "UK"]
        ])
        
        result_path = scan_and_secure(csv_path, self.logger, self.tmp_output)
        
        with open(result_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            rows = list(reader)
        
        # Data should be unchanged
        self.assertEqual(rows[1], ["Alice", "New York", "USA"])
        self.assertEqual(rows[2], ["Bob", "London", "UK"])
        
        # Check log says no patterns detected
        self.assertEqual(self.logger.events[-1]['action'], "SCAN_COMPLETE")
    
    def test_logs_sanitization_count(self):
        """Test that sanitization count is logged correctly"""
        csv_path = self._create_test_csv([
            ["A", "B", "C"],
            ["=pattern1", "+pattern2", "-pattern3"],
            ["safe", "@pattern4", "safe"]
        ])
        
        scan_and_secure(csv_path, self.logger, self.tmp_output)
        
        # Should log 4 sanitized cells
        last_event = self.logger.events[-1]
        self.assertEqual(last_event['action'], "SANITIZATION_COMPLETE")
        self.assertIn("4", last_event['reason'])
    
    def test_returns_sanitized_path(self):
        """Test that function returns path to sanitized file"""
        csv_path = self._create_test_csv([["Name"], ["Test"]])
        
        result_path = scan_and_secure(csv_path, self.logger, self.tmp_output)
        
        self.assertTrue(os.path.exists(result_path))
        self.assertIn("sanitized_input.csv", result_path)


class TestFileExtensionValidation(unittest.TestCase):
    """Tests for file extension validation"""
    
    def setUp(self):
        self.logger = MockLogger()
        self.temp_dir = tempfile.mkdtemp()
    
    def tearDown(self):
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def _create_file(self, filename):
        """Create an empty file with the given name"""
        path = os.path.join(self.temp_dir, filename)
        with open(path, 'w') as f:
            f.write("test content")
        return path
    
    def test_allows_csv_extension(self):
        """Test that .csv files are allowed"""
        path = self._create_file("test.csv")
        # Should not raise
        validate_file_extension(path, self.logger)
        self.assertEqual(self.logger.events[-1]['action'], "EXTENSION_VALIDATED")
    
    def test_allows_docx_extension(self):
        """Test that .docx files are allowed"""
        path = self._create_file("test.docx")
        validate_file_extension(path, self.logger)
        self.assertEqual(self.logger.events[-1]['action'], "EXTENSION_VALIDATED")
    
    def test_allows_db_extension(self):
        """Test that .db files are allowed"""
        path = self._create_file("test.db")
        validate_file_extension(path, self.logger)
        self.assertEqual(self.logger.events[-1]['action'], "EXTENSION_VALIDATED")
    
    def test_blocks_unsupported_extension(self):
        """Test that unsupported extensions raise FileTypeError"""
        path = self._create_file("video.mkv")
        with self.assertRaises(FileTypeError) as context:
            validate_file_extension(path, self.logger)
        self.assertIn(".mkv", str(context.exception))
        self.assertIn("not supported", str(context.exception))
    
    def test_blocks_txt_extension(self):
        """Test that .txt files raise FileTypeError"""
        path = self._create_file("notes.txt")
        with self.assertRaises(FileTypeError):
            validate_file_extension(path, self.logger)
    
    def test_blocks_and_deletes_exe(self):
        """Test that .exe files are blocked and deleted"""
        path = self._create_file("malware.exe")
        self.assertTrue(os.path.exists(path))
        
        with self.assertRaises(SecurityViolationError):
            validate_file_extension(path, self.logger)
        
        # File should be deleted
        self.assertFalse(os.path.exists(path))
    
    def test_blocks_and_deletes_bat(self):
        """Test that .bat files are blocked and deleted"""
        path = self._create_file("script.bat")
        with self.assertRaises(SecurityViolationError):
            validate_file_extension(path, self.logger)
        self.assertFalse(os.path.exists(path))


class TestHashComputation(unittest.TestCase):
    """Tests for file hash computation"""
    
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
    
    def tearDown(self):
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_computes_sha256_hash(self):
        """Test SHA256 hash computation"""
        path = os.path.join(self.temp_dir, "test.txt")
        with open(path, 'w') as f:
            f.write("hello world")
        
        hash_result = compute_file_hash(path, 'sha256')
        
        # Known SHA256 hash of "hello world"
        expected = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        self.assertEqual(hash_result, expected)
    
    def test_computes_md5_hash(self):
        """Test MD5 hash computation"""
        path = os.path.join(self.temp_dir, "test.txt")
        with open(path, 'w') as f:
            f.write("hello world")
        
        hash_result = compute_file_hash(path, 'md5')
        
        # Known MD5 hash of "hello world"
        expected = "5eb63bbbe01eeed093cb22bb8f5acdc3"
        self.assertEqual(hash_result, expected)


class TestHashThreatCheck(unittest.TestCase):
    """Tests for hash-based threat detection"""
    
    def setUp(self):
        self.logger = MockLogger()
    
    def test_clean_hash_passes(self):
        """Test that unknown hashes pass the check"""
        clean_hash = "abc123def456"
        result = check_hash_against_threats(clean_hash, self.logger)
        self.assertFalse(result)
        self.assertEqual(self.logger.events[-1]['action'], "HASH_CHECK_PASSED")


class TestFileSizeValidation(unittest.TestCase):
    """Tests for file size validation"""
    
    def setUp(self):
        self.logger = MockLogger()
        self.temp_dir = tempfile.mkdtemp()
    
    def tearDown(self):
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_small_file_passes(self):
        """Test that small files pass size check"""
        from lead_cleaner.core.security import check_file_size
        
        path = os.path.join(self.temp_dir, "small.csv")
        with open(path, 'w') as f:
            f.write("name,email\ntest,test@test.com")
        
        # Should not raise
        check_file_size(path, self.logger)
        self.assertEqual(self.logger.events[-1]['action'], "FILE_SIZE_OK")


class TestMagicBytesValidation(unittest.TestCase):
    """Tests for file magic bytes validation"""
    
    def setUp(self):
        self.logger = MockLogger()
        self.temp_dir = tempfile.mkdtemp()
    
    def tearDown(self):
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_valid_csv_passes(self):
        """Test that valid UTF-8 CSV passes magic bytes check"""
        from lead_cleaner.core.security import validate_magic_bytes
        
        path = os.path.join(self.temp_dir, "valid.csv")
        with open(path, 'w', encoding='utf-8') as f:
            f.write("name,email\ntest,test@test.com")
        
        validate_magic_bytes(path, self.logger)
        self.assertEqual(self.logger.events[-1]['action'], "MAGIC_BYTES_VALIDATED")
    
    def test_binary_csv_rejected(self):
        """Test that CSV with binary content is rejected"""
        from lead_cleaner.core.security import validate_magic_bytes
        from lead_cleaner.exceptions import FileSignatureError
        
        path = os.path.join(self.temp_dir, "binary.csv")
        with open(path, 'wb') as f:
            f.write(b"name,email\x00\x00binary content")
        
        with self.assertRaises(FileSignatureError):
            validate_magic_bytes(path, self.logger)


class TestMalwarePatternScanning(unittest.TestCase):
    """Tests for malware pattern scanning"""
    
    def setUp(self):
        self.logger = MockLogger()
        self.temp_dir = tempfile.mkdtemp()
    
    def tearDown(self):
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_clean_file_passes(self):
        """Test that clean files pass malware scan"""
        from lead_cleaner.core.security import scan_for_malware_patterns
        
        path = os.path.join(self.temp_dir, "clean.csv")
        with open(path, 'w') as f:
            f.write("name,email\nJohn,john@example.com")
        
        scan_for_malware_patterns(path, self.logger)
        self.assertEqual(self.logger.events[-1]['action'], "MALWARE_SCAN_COMPLETE")
    
    def test_detects_executable_signature(self):
        """Test that Windows executable signature is detected"""
        from lead_cleaner.core.security import scan_for_malware_patterns
        from lead_cleaner.exceptions import MalwareDetectedError
        
        path = os.path.join(self.temp_dir, "malware.csv")
        # MZ is Windows PE signature
        with open(path, 'wb') as f:
            f.write(b"MZ\x00\x00fake executable data")
        
        with self.assertRaises(MalwareDetectedError):
            scan_for_malware_patterns(path, self.logger)
        
        # File should be deleted
        self.assertFalse(os.path.exists(path))
    
    def test_detects_script_pattern(self):
        """Test that script patterns are detected"""
        from lead_cleaner.core.security import scan_for_malware_patterns
        from lead_cleaner.exceptions import MalwareDetectedError
        
        path = os.path.join(self.temp_dir, "script.csv")
        with open(path, 'wb') as f:
            f.write(b"name,data\ntest,<script>alert('xss')</script>")
        
        with self.assertRaises(MalwareDetectedError):
            scan_for_malware_patterns(path, self.logger)


class TestDocxMacroDetection(unittest.TestCase):
    """Tests for DOCX macro detection"""
    
    def setUp(self):
        self.logger = MockLogger()
        self.temp_dir = tempfile.mkdtemp()
    
    def tearDown(self):
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_clean_docx_passes(self):
        """Test that clean DOCX without macros passes"""
        from lead_cleaner.core.security import check_docx_for_macros
        import zipfile
        
        path = os.path.join(self.temp_dir, "clean.docx")
        # Create minimal valid DOCX (ZIP with required structure)
        with zipfile.ZipFile(path, 'w') as zf:
            zf.writestr('[Content_Types].xml', '<?xml version="1.0"?><Types></Types>')
            zf.writestr('word/document.xml', '<?xml version="1.0"?><document></document>')
        
        check_docx_for_macros(path, self.logger)
        self.assertEqual(self.logger.events[-1]['action'], "DOCX_MACRO_CHECK_PASSED")
    
    def test_detects_vba_project(self):
        """Test that VBA macro files are detected and blocked"""
        from lead_cleaner.core.security import check_docx_for_macros
        from lead_cleaner.exceptions import MalwareDetectedError
        import zipfile
        
        path = os.path.join(self.temp_dir, "macro.docx")
        with zipfile.ZipFile(path, 'w') as zf:
            zf.writestr('[Content_Types].xml', '<?xml version="1.0"?><Types></Types>')
            zf.writestr('word/document.xml', '<?xml version="1.0"?><document></document>')
            zf.writestr('word/vbaProject.bin', 'fake VBA macro content')
        
        with self.assertRaises(MalwareDetectedError) as context:
            check_docx_for_macros(path, self.logger)
        
        self.assertIn("vbaProject.bin", str(context.exception))
        # File should be deleted
        self.assertFalse(os.path.exists(path))


if __name__ == "__main__":
    unittest.main()
