import unittest
from lead_cleaner.phase1_deterministic.normalizers import (
    emails,
    phones,
    names,
    job_titles,
)
from lead_cleaner.phase1_deterministic.deduplication import detect_duplicates
from lead_cleaner.logging.logger import PipelineLogger
from lead_cleaner.types import RowStatus, FailureReason


class TestNormalizers(unittest.TestCase):
    def test_email_normalizer(self):
        # Valid
        self.assertEqual(
            emails.normalize_email(" Test@Example.com ")["normalized_value"],
            "test@example.com",
        )
        # Invalid
        self.assertEqual(
            emails.normalize_email("not_an_email")["field_status"], "INVALID"
        )
        # Empty
        self.assertEqual(emails.normalize_email(None)["field_status"], "MISSING")

    def test_email_emoji_strip(self):
        result = emails.normalize_email("Bob.Doe@yahoo.com 🔥")
        self.assertEqual(result["normalized_value"], "bob.doe@yahoo.com")
        self.assertEqual(result["field_status"], "VALID")

    def test_phone_normalizer(self):
        # Clean 10 digit
        self.assertEqual(
            phones.normalize_phone("1234567890")["normalized_value"], "123-456-7890"
        )
        # Clean 11 digit with 1
        self.assertEqual(
            phones.normalize_phone("11234567890")["normalized_value"], "123-456-7890"
        )
        # Invalid
        self.assertEqual(phones.normalize_phone("123")["field_status"], "INVALID")

    def test_phone_emoji_strip(self):
        result = phones.normalize_phone("🔥5551234567")
        self.assertEqual(result["normalized_value"], "555-123-4567")
        self.assertEqual(result["field_status"], "VALID")

    def test_name_normalizer(self):
        self.assertEqual(
            names.normalize_name("  john doe  ")["normalized_value"], "John Doe"
        )
        self.assertIsNone(names.normalize_name("")["normalized_value"])

    def test_name_emoji_strip(self):
        result = names.normalize_name("Alice 🔥")
        self.assertEqual(result["normalized_value"], "Alice")
        self.assertEqual(result["field_status"], "VALID")

    def test_job_title_emoji_strip(self):
        result = job_titles.normalize_job_title("Director 🔥")
        self.assertEqual(result["normalized_value"], "Director")
        self.assertEqual(result["field_status"], "VALID")

        result2 = job_titles.normalize_job_title("CEO🔥")
        self.assertEqual(result2["normalized_value"], "CEO")
        self.assertEqual(result2["field_status"], "VALID")


class TestDeduplication(unittest.TestCase):
    def setUp(self):
        self.logger = PipelineLogger("test_run")

    def test_deduplication(self):
        rows = [
            {
                "row_id": "1",
                "status": RowStatus.AI_REQUIRED,
                "clean_data": {"email": "a@b.com"},
                "raw_data": {},
            },
            {
                "row_id": "2",
                "status": RowStatus.AI_REQUIRED,
                "clean_data": {"email": "a@b.com"},
                "raw_data": {},
            },  # Duplicate
            {
                "row_id": "3",
                "status": RowStatus.AI_REQUIRED,
                "clean_data": {"email": "c@d.com"},
                "raw_data": {},
            },
        ]

        results = detect_duplicates(rows, self.logger)

        self.assertFalse(results[0]["is_duplicate"])
        self.assertTrue(results[1]["is_duplicate"])
        self.assertEqual(results[1]["status"], RowStatus.REJECTED)
        self.assertEqual(results[1]["failure_reason"], FailureReason.DUPLICATE)
        self.assertFalse(results[2]["is_duplicate"])


if __name__ == "__main__":
    unittest.main()
