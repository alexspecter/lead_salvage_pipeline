import sys
import os

# Ensure we can import lead_cleaner
sys.path.append(os.getcwd())

from lead_cleaner.phase0_setup.generator import GarbageGenerator
from lead_cleaner.core.validator import DataValidator
from lead_cleaner.logging.logger import PipelineLogger


def test_phase0():
    print("--- 1. Generating Garbage Data ---")
    gen = GarbageGenerator(seed=123)
    output_file = "input/test_phase0.csv"
    gen.generate_csv(output_file, count=20)

    assert os.path.exists(output_file)
    print(f"Generated file at {output_file}")

    print("\n--- 2. Validating Data ---")
    logger = PipelineLogger(run_id="test_run_001")
    validator = DataValidator(logger)

    df = validator.validate_csv(output_file)
    print("Validation successful!")
    print(f"Columns: {list(df.columns)}")
    print(f"Row count: {len(df)}")

    assert len(df) >= 20  # Might be more due to duplicates

    print("\n--- 3. Checking for Failure Case ---")
    # Create bad file
    bad_file = "input/bad.csv"
    with open(bad_file, "w") as f:
        f.write("col1,col2\n1,2")  # Missing required cols

    try:
        validator.validate_csv(bad_file)
        print("ERROR: Validator should have failed but didn't!")
        sys.exit(1)
    except Exception as e:
        print(f"Caught expected error: {e}")


if __name__ == "__main__":
    test_phase0()
