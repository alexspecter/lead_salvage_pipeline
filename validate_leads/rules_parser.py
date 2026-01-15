"""
Easy Syntax Rules Parser for the Validate Leads Utility.

Parses a simple text file containing validation rules and applies them
to filter a Pandas DataFrame.

Supported Syntax:
    Column[HeaderName] == True           # Existence check (not empty)
    Column[Phone] == Valid               # Phone format validation
    Column[Email] == Valid               # Email format validation
    Column[Header] == "Exact Value"      # Exact string match
    Column[Header] == "A" OR "B" OR "C"  # Match any of multiple values
"""

import re
import pandas as pd
from typing import List, Tuple, Callable, Any
from dataclasses import dataclass

from validate_leads.validators import is_valid_phone, is_valid_email, is_not_empty


@dataclass
class Rule:
    """Represents a parsed validation rule."""
    column: str
    check_type: str  # 'existence', 'valid_phone', 'valid_email', 'exact', 'multi'
    values: List[str]  # For exact/multi match, the allowed values
    original_line: str  # For error reporting


class RulesParser:
    """
    Parses and applies Easy Syntax rules to a DataFrame.
    """
    
    # Regex patterns for parsing
    COLUMN_PATTERN = re.compile(r'Column\[([^\]]+)\]\s*==\s*(.+)', re.IGNORECASE)
    QUOTED_STRING_PATTERN = re.compile(r'"([^"]*)"')
    
    def __init__(self, rules_file_path: str):
        self.rules_file_path = rules_file_path
        self.rules: List[Rule] = []
        self._parse_file()
    
    def _parse_file(self):
        """Parse the rules file and populate self.rules."""
        with open(self.rules_file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        for line_num, line in enumerate(lines, start=1):
            line = line.strip()
            
            # Skip empty lines and comments
            if not line or line.startswith('#'):
                continue
            
            rule = self._parse_line(line, line_num)
            if rule:
                self.rules.append(rule)
    
    def _parse_line(self, line: str, line_num: int) -> Rule:
        """Parse a single rule line."""
        match = self.COLUMN_PATTERN.match(line)
        if not match:
            raise ValueError(f"Line {line_num}: Invalid syntax. Expected 'Column[HeaderName] == ...'\n  Got: {line}")
        
        column_name = match.group(1).strip()
        value_part = match.group(2).strip()
        
        # Determine the check type
        value_lower = value_part.lower()
        
        if value_lower == 'true':
            return Rule(
                column=column_name,
                check_type='existence',
                values=[],
                original_line=line
            )
        
        if value_lower == 'valid':
            # Determine type based on column name
            col_lower = column_name.lower()
            if 'phone' in col_lower:
                check_type = 'valid_phone'
            elif 'email' in col_lower or 'mail' in col_lower:
                check_type = 'valid_email'
            else:
                raise ValueError(
                    f"Line {line_num}: 'Valid' check requires column name to contain 'phone' or 'email'.\n"
                    f"  Column: {column_name}"
                )
            return Rule(
                column=column_name,
                check_type=check_type,
                values=[],
                original_line=line
            )
        
        # Check for quoted strings (exact or multi-match)
        quoted_values = self.QUOTED_STRING_PATTERN.findall(value_part)
        if quoted_values:
            check_type = 'multi' if len(quoted_values) > 1 or ' OR ' in value_part.upper() else 'exact'
            return Rule(
                column=column_name,
                check_type=check_type,
                values=quoted_values,
                original_line=line
            )
        
        raise ValueError(f"Line {line_num}: Could not parse value. Expected True, Valid, or quoted string(s).\n  Got: {value_part}")
    
    def apply(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
        """
        Apply all rules to the DataFrame and return the filtered result.
        
        Returns:
            Tuple of (filtered_df, list of rule application messages)
        """
        messages = []
        result_df = df.copy()
        initial_count = len(result_df)
        
        for rule in self.rules:
            # Check if column exists
            if rule.column not in result_df.columns:
                raise ValueError(f"Column '{rule.column}' not found in the input CSV. Available columns: {list(result_df.columns)}")
            
            before_count = len(result_df)
            
            if rule.check_type == 'existence':
                mask = result_df[rule.column].apply(is_not_empty)
                result_df = result_df[mask]
                
            elif rule.check_type == 'valid_phone':
                mask = result_df[rule.column].apply(is_valid_phone)
                result_df = result_df[mask]
                
            elif rule.check_type == 'valid_email':
                mask = result_df[rule.column].apply(is_valid_email)
                result_df = result_df[mask]
                
            elif rule.check_type in ('exact', 'multi'):
                # Case-insensitive string matching
                mask = result_df[rule.column].astype(str).str.strip().str.lower().isin(
                    [v.lower() for v in rule.values]
                )
                result_df = result_df[mask]
            
            after_count = len(result_df)
            filtered_count = before_count - after_count
            
            if filtered_count > 0:
                messages.append(f"  Rule '{rule.original_line}' filtered out {filtered_count} row(s).")
            else:
                messages.append(f"  Rule '{rule.original_line}' - all rows passed.")
        
        final_count = len(result_df)
        total_filtered = initial_count - final_count
        
        messages.insert(0, f"Starting with {initial_count} rows.")
        messages.append(f"Final result: {final_count} valid rows ({total_filtered} filtered out).")
        
        return result_df, messages
