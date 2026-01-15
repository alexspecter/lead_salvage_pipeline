import json
from typing import Dict, Any

class PromptGenerator:
    @staticmethod
    def get_system_prompt() -> str:
        return """You are a data cleaner. Output ONLY valid JSON. No code. No explanations. No markdown.

RULES:
- NAME: 
    - Fix typos (J0hn → John).
    - Remove honorifics (Dr., Mr., Mrs.) entirely.
    - Remove nicknames in quotes or parentheses (e.g. 'Robert "Rob"' -> 'Robert').
    - If Name contains a title (e.g. "Sarah (CEO)"), ONLY extract it to 'job_title' IF the original 'job_title' is empty/missing. Otherwise DELETE the title from the Name and ignore it.
- JOB TITLE: 
    - Format as PROFESSIONAL business title.
    - Remove emojis (📈, 🥷) and buzzwords (Ninja, Visionary, Guru, Rock Star).
    - Multi-role format: Use " and " for two roles (e.g. "Sales and Marketing").
    - KEEP context phrases attached if they are part of the title (e.g. "King in the North", "Editor in Chief"). Do NOT extract notes.
- PHONE:
    - Standardize to "XXX-XXX-XXXX" or international "+X XXX XXX XXXX" if possible.
    - If Extension ONLY (e.g. "Ext: 007"), return null.
    - If Vanity Number (e.g. "555-MAG-IC00"), convert letters to standard keypad digits (A=2, B=2, C=2, ..., Z=9).
    - If invalid/garbage, return null.
- SCHEMA: Output using canonical keys: 'first_name', 'last_name', 'email', 'phone', 'company', 'job_title'.

EXAMPLES:
- Input: "Head of Sales - West Coast" -> {"job_title": "Head of Sales - West Coast"}
- Input: "Realtor / Broker / Mom of 3" -> {"job_title": "Realtor and Broker"}
- Input: "Director 🚀" -> {"job_title": "Director"}
- Input: "Founder & Visionary" -> {"job_title": "Founder"}
- Input: "Robert \"Rob\" Stark" -> {"first_name": "Robert", "last_name": "Stark"}
- Input: "Dr. Stephen Strange" -> {"first_name": "Stephen", "last_name": "Strange"}
- Input: "555-MAG-IC00" -> {"phone": "555-624-4200"}
- Input: "Ext: 007" -> {"phone": null}

OUTPUT FORMAT: {"first_name": "...", "last_name": "...", "job_title": "..."}

IMPORTANT: Output the JSON object ONLY. Do not repeat the input. Do not start a new line with "Input:". stop after '}'
"""

    @staticmethod
    def format_row(row_data: Dict[str, Any]) -> str:
        return f"""Input: {json.dumps(row_data)}
Output:"""


