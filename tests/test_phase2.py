import unittest
from unittest.mock import MagicMock, patch
from lead_cleaner.phase2_semantic.runner import Phase2Runner
from lead_cleaner.types import RowStatus, FailureReason
from lead_cleaner.logging.logger import PipelineLogger

class TestPhase2(unittest.TestCase):
    def setUp(self):
        self.logger = PipelineLogger("test_run_p2")
        self.runner = Phase2Runner(self.logger, "test_run_p2")
        
    @patch("lead_cleaner.phase2_semantic.model.LocalLLM.load_model")
    @patch("lead_cleaner.phase2_semantic.model.LocalLLM.generate_response")
    @patch("lead_cleaner.phase2_semantic.memory_guard.MemoryGuard.check_memory")
    @patch("lead_cleaner.phase2_semantic.memory_guard.MemoryGuard.force_gc")
    def test_runner_flow_success(self, mock_gc, mock_mem, mock_gen, mock_load):
        # Setup Mocks - check_memory now returns usage percentage
        mock_mem.return_value = 0.50  # 50% usage
        mock_gen.return_value = '{"email": "clean@example.com", "phone": "123-456-7890"}'
        
        rows = [
            {"row_id": "1", "status": RowStatus.AI_REQUIRED, "raw_data": {"email": "bad"}, "clean_data": {}, "confidence_score": 0.0},
            {"row_id": "2", "status": RowStatus.CLEAN, "raw_data": {}, "clean_data": {}, "confidence_score": 1.0}
        ]
        
        processed = self.runner.process(rows)
        
        # Verify Row 1
        r1 = next(r for r in processed if r["row_id"] == "1")
        self.assertEqual(r1["status"], RowStatus.CLEAN)
        self.assertEqual(r1["clean_data"]["email"], "clean@example.com")
        
        # Verify Row 2 untouched
        r2 = next(r for r in processed if r["row_id"] == "2")
        self.assertEqual(r2["status"], RowStatus.CLEAN)

    @patch("lead_cleaner.phase2_semantic.model.LocalLLM.load_model")
    @patch("lead_cleaner.phase2_semantic.model.LocalLLM.generate_response")
    @patch("lead_cleaner.phase2_semantic.memory_guard.MemoryGuard.check_memory")
    @patch("lead_cleaner.phase2_semantic.memory_guard.MemoryGuard.force_gc")
    def test_runner_json_fail(self, mock_gc, mock_mem, mock_gen, mock_load):
        mock_mem.return_value = 0.50  # 50% usage
        mock_gen.return_value = "NOT JSON"
        
        rows = [
             {"row_id": "1", "status": RowStatus.AI_REQUIRED, "raw_data": {"email": "bad"}, "clean_data": {}, "confidence_score": 0.0}
        ]
        
        processed = self.runner.process(rows)
        r1 = processed[0]
        self.assertEqual(r1["status"], RowStatus.REJECTED)
        self.assertEqual(r1["failure_reason"], FailureReason.INVALID_FORMAT)

if __name__ == '__main__':
    unittest.main()
