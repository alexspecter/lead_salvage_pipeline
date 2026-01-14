#!/usr/bin/env python3
import sys
import os

# Add local directory to path so imports work
sys.path.append(os.getcwd())

from lead_cleaner.core.orchestrator import Orchestrator

def main():
    health_check = False
    if "--health-check" in sys.argv:
        health_check = True
        sys.argv.remove("--health-check")

    if len(sys.argv) < 2:
        print("Usage: python run_pipeline.py <path_to_input_csv> [output_dir] [--health-check]")
        sys.exit(1)
        
    input_csv = sys.argv[1]
    output_dir = sys.argv[2] if len(sys.argv) > 2 else "output"
    
    if not os.path.exists(input_csv):
        print(f"Error: Input file '{input_csv}' not found.")
        sys.exit(1)
        
    orch = Orchestrator()
    orch.run_pipeline(input_csv, output_dir, health_check=health_check)

if __name__ == "__main__":
    main()
