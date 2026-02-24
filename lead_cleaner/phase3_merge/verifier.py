from typing import List
from lead_cleaner.types import LeadRow
from lead_cleaner.exceptions import VerificationError
from lead_cleaner.logging.logger import PipelineLogger


class Verifier:
    def __init__(self, logger: PipelineLogger):
        self.logger = logger

    def verify_outputs(
        self,
        input_rows: List[LeadRow],
        final_output: List[LeadRow],
        rejected_output: List[LeadRow],
    ):
        self.logger.log_event("PHASE_3", "VERIFICATION_START")

        input_count = len(input_rows)
        final_count = len(final_output)
        rejected_count = len(rejected_output)

        # 1. Count Check
        if input_count != (final_count + rejected_count):
            msg = f"Count Mismatch: Input({input_count}) != Final({final_count}) + Rejected({rejected_count})"
            self.logger.log_error("PHASE_3", "VERIFICATION_FAILED", Exception(msg))
            raise VerificationError(msg)

        # 2. ID Uniqueness & Overlap Check
        final_ids = {r["row_id"] for r in final_output}
        rejected_ids = {r["row_id"] for r in rejected_output}

        overlap = final_ids.intersection(rejected_ids)
        if overlap:
            msg = f"Row IDs found in both Final and Rejected: {overlap}"
            self.logger.log_error("PHASE_3", "VERIFICATION_FAILED", Exception(msg))
            raise VerificationError(msg)

        # 3. Completeness Check
        all_output_ids = final_ids.union(rejected_ids)
        input_ids = {r["row_id"] for r in input_rows}

        missing = input_ids - all_output_ids
        if missing:
            msg = f"Missing Row IDs in output: {missing}"
            self.logger.log_error("PHASE_3", "VERIFICATION_FAILED", Exception(msg))
            raise VerificationError(msg)

        extra = all_output_ids - input_ids
        if extra:
            msg = f"Extra Row IDs in output (Ghost rows): {extra}"
            self.logger.log_error("PHASE_3", "VERIFICATION_FAILED", Exception(msg))
            raise VerificationError(msg)

        self.logger.log_event(
            "PHASE_3", "VERIFICATION_SUCCESS", reason="All checks passed"
        )
