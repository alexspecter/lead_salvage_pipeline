#!/usr/bin/env python3
"""
Validate Leads - Standalone Post-Pipeline Filter Utility

This tool filters a cleaned CSV file against a custom set of rules
defined in a simple text file using "Easy Syntax".

Usage:
    python validate_leads.py --input <csv_file> --rules <rules_file> [--output <output_file>]

Example:
    python validate_leads.py --input final_output.csv --rules my_rules.txt

The output will be written to 'valid_leads.csv' in the same directory as the input,
unless a custom output path is specified.
"""

import argparse
import os
import sys
import pandas as pd

# Add parent directory to path so imports work when run as a script
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from validate_leads.rules_parser import RulesParser


def main():
    parser = argparse.ArgumentParser(
        description='Filter a CSV file against custom validation rules.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Rules File Syntax:
  # This is a comment
  Column[HeaderName] == True              # Column must not be empty
  Column[Phone] == Valid                  # Phone must be valid format
  Column[Email] == Valid                  # Email must be valid format
  Column[Status] == "Active"              # Exact match
  Column[Response] == "Yes" OR "Y"        # Match any of these values

Example rules.txt:
  # Require all leads to have valid contact info
  Column[email] == Valid
  Column[phone] == Valid
  
  # Only keep leads who responded positively
  Column[interested] == "Yes" OR "Y" OR "Sure"
"""
    )
    
    parser.add_argument(
        '--input', '-i',
        required=True,
        help='Path to the input CSV file (typically the pipeline output)'
    )
    
    parser.add_argument(
        '--rules', '-r',
        required=True,
        help='Path to the rules file containing validation criteria'
    )
    
    parser.add_argument(
        '--output', '-o',
        default=None,
        help='Path for the output CSV (default: valid_leads.csv in same directory as input)'
    )
    
    args = parser.parse_args()
    
    # Validate input file
    if not os.path.exists(args.input):
        print(f"Error: Input file not found: {args.input}")
        sys.exit(1)
    
    # Validate rules file
    if not os.path.exists(args.rules):
        print(f"Error: Rules file not found: {args.rules}")
        sys.exit(1)
    
    # Determine output path
    if args.output:
        output_path = args.output
    else:
        input_dir = os.path.dirname(os.path.abspath(args.input))
        output_path = os.path.join(input_dir, 'valid_leads.csv')
    
    print(f"=== Validate Leads Utility ===")
    print(f"Input:  {args.input}")
    print(f"Rules:  {args.rules}")
    print(f"Output: {output_path}")
    print()
    
    # Load CSV
    try:
        df = pd.read_csv(args.input)
        print(f"Loaded {len(df)} rows from input CSV.")
        print(f"Columns: {list(df.columns)}")
        print()
    except Exception as e:
        print(f"Error reading CSV: {e}")
        sys.exit(1)
    
    # Parse and apply rules
    try:
        print("Parsing rules file...")
        rules_parser = RulesParser(args.rules)
        print(f"Found {len(rules_parser.rules)} rule(s).")
        print()
        
        print("Applying rules...")
        filtered_df, messages = rules_parser.apply(df)
        
        for msg in messages:
            print(msg)
        print()
        
    except ValueError as e:
        print(f"Error parsing rules: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error applying rules: {e}")
        sys.exit(1)
    
    # Write output
    try:
        filtered_df.to_csv(output_path, index=False)
        print(f"Successfully wrote {len(filtered_df)} valid leads to: {output_path}")
    except Exception as e:
        print(f"Error writing output: {e}")
        sys.exit(1)
    
    print()
    print("=== Done ===")


if __name__ == '__main__':
    main()
