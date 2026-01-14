import json
from typing import Dict, Any

class PromptGenerator:
    @staticmethod
    def get_system_prompt() -> str:
        return """You are a data cleaner. Output ONLY valid JSON. No code. No explanations. No markdown.

RULES:
- Fix typos in names (J0hn → John)
- Extract job titles from names (e.g. "Sarah (CEO)" → Name: "Sarah", Job: "Chief Executive Officer")
- Expand job abbreviations to full titles (CEO → Chief Executive Officer, VP → Vice President, CTO → Chief Technology Officer)
- If last name appears truncated (e.g. "M." or "M"), try to infer or flag as incomplete
- Standardize dates to YYYY-MM-DD
- Clean job titles (remove emojis, keep primary role only)
- Return the cleaned record as JSON

OUTPUT FORMAT: {"field1": "value1", "field2": "value2"}
"""

    @staticmethod
    def format_row(row_data: Dict[str, Any]) -> str:
        return f"""Input: {json.dumps(row_data)}
Output:"""


