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

from lead_cleaner.core.security import scan_and_secure, INJECTION_PREFIXES


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


if __name__ == "__main__":
    unittest.main()
