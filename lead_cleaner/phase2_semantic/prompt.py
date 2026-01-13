import json
from typing import Dict, Any

class PromptGenerator:
    @staticmethod
    def get_system_prompt() -> str:
        return """You are a precise data cleaning assistant. 
Your goal is to extract clean, normalized data from a raw record.
Rules:
1. Return ONLY valid JSON.
2. No markdown, no comments.
3. Fix typos in names, emails, and cities.
4. Normalize dates to YYYY-MM-DD.
5. Do not invent missing data. Use null if missing.
6. Schema: {"first_name": str, "last_name": str, "email": str, "phone": str, "job_title": str, "company": str, "date": str}
"""

    @staticmethod
    def format_row(row_data: Dict[str, Any]) -> str:
        return f"""
Record to clean:
{json.dumps(row_data, indent=2)}

JSON Output:
"""
