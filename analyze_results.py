#!/usr/bin/env python3
"""
Analyze Pipeline Results
------------------------
Usage:
  python analyze_results.py --output_dir user_output --run_id <UUID>
  python analyze_results.py --output_dir testcache --latest  # Auto-detect latest run
"""

import pandas as pd
import os
import argparse
import glob


def find_latest_run(output_dir: str) -> str:
    """Find the latest run_id in a directory based on file modification time."""
    pattern = os.path.join(output_dir, "final_output_*.csv")
    files = glob.glob(pattern)
    if not files:
        return None
    latest = max(files, key=os.path.getmtime)
    # Extract run_id from filename
    basename = os.path.basename(latest)
    return basename.replace("final_output_", "").replace(".csv", "")


def analyze(output_dir: str, run_id: str):
    """Run analysis on pipeline output."""
    clean_file = os.path.join(output_dir, f"final_output_{run_id}.csv")
    log_file = os.path.join(output_dir, f"pipeline_log_{run_id}.csv")
    cache_dir = os.path.join(output_dir, f"rejection_cache_{run_id}")

    print(f"\n--- Analysis for Run: {run_id} ---")
    print(f"Output Dir: {output_dir}")

    if os.path.exists(clean_file):
        df = pd.read_csv(clean_file)
        print(f"\n[OUTPUT] Shape: {df.shape}")
        print(f"[OUTPUT] Columns: {df.columns.tolist()}")
        print(df.head(10).to_string())
    else:
        print(f"\n[ERROR] Clean file not found: {clean_file}")
        return

    if os.path.exists(log_file):
        log = pd.read_csv(log_file)
        enrichment = (
            log[log["action"] == "ROW_ENRICHED"] if "action" in log.columns else []
        )
        p2_resolved = (
            log[log["action"] == "ROW_RESOLVED"] if "action" in log.columns else []
        )
        print(f"\n[LOG] Smart Enrichments: {len(enrichment)}")
        print(f"[LOG] AI Resolved Rows: {len(p2_resolved)}")

    # Check rejection cache
    if os.path.exists(cache_dir):
        for fname in os.listdir(cache_dir):
            fpath = os.path.join(cache_dir, fname)
            if fname.endswith(".csv"):
                rdf = pd.read_csv(fpath)
                print(f"\n[REJECTION_CACHE] {fname}: {len(rdf)} entries")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analyze pipeline results")
    parser.add_argument(
        "--output_dir", "-o", default="user_output", help="Output directory to analyze"
    )
    parser.add_argument("--run_id", "-r", help="Specific run ID to analyze")
    parser.add_argument(
        "--latest",
        "-l",
        action="store_true",
        help="Auto-detect and analyze the latest run",
    )

    args = parser.parse_args()

    if args.latest:
        run_id = find_latest_run(args.output_dir)
        if not run_id:
            print(f"No runs found in {args.output_dir}")
            exit(1)
    elif args.run_id:
        run_id = args.run_id
    else:
        print("Error: Provide --run_id or --latest")
        exit(1)

    analyze(args.output_dir, run_id)
