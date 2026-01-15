#!/usr/bin/env python3
"""
Batch process all test datasets from user_input_tests through the pipeline.
Outputs results to user_output directory.
"""
import sys
import os
from pathlib import Path

# Add local directory to path
sys.path.append(os.getcwd())

from lead_cleaner.core.orchestrator import Orchestrator


def main():
    # Define paths
    input_dir = Path("user_input_tests")
    output_dir = Path("user_output")
    
    # Find all CSV files in input directory
    csv_files = sorted(input_dir.glob("*.csv"))
    
    if not csv_files:
        print(f"No CSV files found in {input_dir}")
        sys.exit(1)
    
    print(f"Found {len(csv_files)} datasets to process:")
    for f in csv_files:
        print(f"  - {f.name}")
    print()
    
    # Process each file
    results = []
    for csv_file in csv_files:
        print("=" * 80)
        print(f"Processing: {csv_file.name}")
        print("=" * 80)
        
        try:
            orch = Orchestrator()
            orch.run_pipeline(str(csv_file), str(output_dir))
            results.append((csv_file.name, "SUCCESS", orch.run_id))
            print(f"✅ {csv_file.name} - SUCCESS\n")
        except Exception as e:
            results.append((csv_file.name, f"FAILED: {str(e)}", None))
            print(f"❌ {csv_file.name} - FAILED: {str(e)}\n")
    
    # Summary
    print("\n" + "=" * 80)
    print("BATCH PROCESSING SUMMARY")
    print("=" * 80)
    for filename, status, run_id in results:
        status_icon = "✅" if status == "SUCCESS" else "❌"
        print(f"{status_icon} {filename}: {status}")
        if run_id:
            print(f"   Run ID: {run_id}")
    print("=" * 80)
    
    # Exit with error if any failed
    failed_count = sum(1 for _, status, _ in results if status != "SUCCESS")
    if failed_count > 0:
        print(f"\n⚠️  {failed_count} dataset(s) failed processing")
        sys.exit(1)
    else:
        print(f"\n✅ All {len(results)} datasets processed successfully!")


if __name__ == "__main__":
    main()
