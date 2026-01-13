import pandas as pd
import os
from lead_cleaner.constants import *

def generate_semantic_test_data(filename: str):
    """
    Generates data specifically designed to fail Phase 1 (Deterministic)
    but pass Phase 0 (Validation) and require Phase 2 (LLM).
    
    Triggers:
    - Obfuscated emails (john [at] gmail)
    - Natural language phones (call me at 555...)
    - Typos in names
    - Complex job titles
    """
    rows = [
        # Case 1: Natural Language Phone
        {
            FIELD_FirstName: "Alice",
            FIELD_LastName: "Semantic",
            FIELD_Email: "alice@example.com", # Valid email to pass Ph0
            FIELD_Phone: "Call 555 123 4567 after 5pm", # P1 phone normalizer checks for exactly 10 digits usually
            FIELD_JobTitle: "Manager",
            FIELD_Company: "TestCorp",
            FIELD_Date: "2023-01-01"
        },
        # Case 2: Obfuscated Email
        {
            FIELD_FirstName: "Bob",
            FIELD_LastName: "Obfuscated",
            FIELD_Email: "bob [at] example dot com", # P1 regex will likely fail
            FIELD_Phone: "555-999-8888",
            FIELD_JobTitle: "Dev",
            FIELD_Company: "TestCorp",
            FIELD_Date: "2023-01-01"
        },
        # Case 3: Messy Name & Job
        {
            FIELD_FirstName: "charlie (the boss)", # P1 name might keep parens
            FIELD_LastName: "rogers",
            FIELD_Email: "charlie@example.com",
            FIELD_Phone: "555-111-2222",
            FIELD_JobTitle: "Chief Executive Officer & Founder", # P1 might not map strictly
            FIELD_Company: "Startup",
            FIELD_Date: "Jan 1st 23"
        },
        # Case 4: Wordy Date
        {
            FIELD_FirstName: "Dave",
            FIELD_LastName: "Date",
            FIELD_Email: "dave@example.com",
            FIELD_Phone: "555-333-4444",
            FIELD_JobTitle: "Admin",
            FIELD_Company: "Corp",
            FIELD_Date: "Second of January, 2024" # P1 date (pandas) specific parsing might fail or pass depending on strictness
        },
        # Case 5: Missing key fields but valid structure (Low Confidence Trigger)
        {
            FIELD_FirstName: "Eve",
            FIELD_LastName: "Empty",
            FIELD_Email: "", # Missing
            FIELD_Phone: "555-000-0000",
            FIELD_JobTitle: "",
            FIELD_Company: "",
            FIELD_Date: ""
        }
    ]
    
    df = pd.DataFrame(rows)
    os.makedirs(os.path.dirname(filename) or ".", exist_ok=True)
    df.to_csv(filename, index=False)
    print(f"Generated {len(df)} semantic test rows to {filename}")

if __name__ == "__main__":
    generate_semantic_test_data("input/semantic_challenge.csv")
