import unittest
from lead_cleaner.phase3_merge.verifier import Verifier
from lead_cleaner.logging.logger import PipelineLogger
from lead_cleaner.types import RowStatus
from lead_cleaner.exceptions import VerificationError


class TestPhase3(unittest.TestCase):
    def setUp(self):
        self.logger = PipelineLogger("test_run_p3")
        self.verifier = Verifier(self.logger)

    def test_verification_success(self):
        inputs = [{"row_id": "1"}, {"row_id": "2"}]
        final = [{"row_id": "1", "status": RowStatus.CLEAN}]
        rejected = [{"row_id": "2", "status": RowStatus.REJECTED}]

        # Should not raise
        self.verifier.verify_outputs(inputs, final, rejected)

    def test_verification_count_fail(self):
        inputs = [{"row_id": "1"}]
        final = []
        rejected = []

        with self.assertRaises(VerificationError):
            self.verifier.verify_outputs(inputs, final, rejected)

    def test_verification_overlap_fail(self):
        inputs = [{"row_id": "1"}]
        final = [{"row_id": "1"}]
        rejected = [{"row_id": "1"}]

        with self.assertRaises(VerificationError):
            self.verifier.verify_outputs(inputs, final, rejected)


if __name__ == "__main__":
    unittest.main()
