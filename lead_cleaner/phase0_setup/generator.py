import pandas as pd
import random
import os
from typing import List, Dict

from lead_cleaner.constants import FIELD_Email, FIELD_Phone, FIELD_FirstName, FIELD_LastName, FIELD_JobTitle, FIELD_Company, FIELD_Date

# Deterministic "Messy" Data constants
BAD_CHARS = ["🔥", " ", "\t", "\n", "...", "N/A", "null"]
DOMAINS = ["gmail.com", "yahoo.com", "hotmail.com", "corp.net"]

class GarbageGenerator:
    def __init__(self, seed: int = 42):
        random.seed(seed)

    def _mess_up_string(self, s: str, prob: float = 0.3) -> str:
        if random.random() > prob:
            return s
        
        action = random.choice(["prepend", "append", "insert", "case", "emoji"])
        if action == "prepend":
            return random.choice(BAD_CHARS) + s
        elif action == "append":
            return s + random.choice(BAD_CHARS)
        elif action == "emoji":
            return s + " 🔥"
        elif action == "case":
            return s.upper() if random.random() > 0.5 else s.lower()
        return s

    def generate_row(self) -> Dict[str, str]:
        first = random.choice(["John", "Jane", "Alice", "Bob", "Charlie", "David", "Eve"])
        last = random.choice(["Doe", "Smith", "Johnson", "Brown", "Williams"])
        
        # 80% chance of having email
        email = ""
        if random.random() < 0.8:
            clean_email = f"{first}.{last}@{random.choice(DOMAINS)}"
            email = self._mess_up_string(clean_email, prob=0.4)
            
        # 80% chance of phone
        phone = ""
        if random.random() < 0.8:
            # Generate random 10 digit
            p = "".join([str(random.randint(0,9)) for _ in range(10)])
            # Format nicely sometimes
            if random.random() > 0.5:
                p = f"({p[:3]}) {p[3:6]}-{p[6:]}"
            phone = self._mess_up_string(p, prob=0.4)

        if not email and not phone:
             # Force one
             email = f"{first}.{last}@forced.com"
        
        job = random.choice(["Engineer", "Manager", "CEO", "Consultant", "Director"])
        job = self._mess_up_string(job)
        
        date_val = "2023-01-01"
        if random.random() < 0.2:
            date_val = "01/01/23" # Ambiguous
        elif random.random() < 0.1:
            date_val = "Jan 1st, 2023"
            
        return {
            FIELD_FirstName: first,
            FIELD_LastName: last,
            FIELD_Email: email,
            FIELD_Phone: phone,
            FIELD_JobTitle: job,
            FIELD_Company: "Acme Corp",
            FIELD_Date: date_val
        }

    def generate_csv(self, filename: str, count: int = 100):
        rows = [self.generate_row() for _ in range(count)]
        
        # Inject duplicates
        if count > 10:
             # Take random 5 rows and duplicate them
             dupes = random.sample(rows, 5)
             rows.extend(dupes)
        
        df = pd.DataFrame(rows)
        # Shuffle
        df = df.sample(frac=1).reset_index(drop=True)
        
        os.makedirs(os.path.dirname(filename) or ".", exist_ok=True)
        df.to_csv(filename, index=False)
        print(f"Generated {len(df)} rows to {filename}")

if __name__ == "__main__":
    # When run as script, generate default set
    gen = GarbageGenerator()
    gen.generate_csv("input/messy_leads.csv", count=50)
