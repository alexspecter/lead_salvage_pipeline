"""
DOCX Cleaning Prompt Generator
Generates prompts for the LLM to clean document content.
"""

import json
from typing import List, Dict


class DocxPromptGenerator:
    """Generates prompts for cleaning DOCX content."""

    @staticmethod
    def get_system_prompt() -> str:
        return """You are a document cleaning assistant. Your task is to clean and normalize messy text content from business documents.

RULES:
1. Fix obvious typos and grammatical errors
2. Normalize inconsistent formatting (capitalization, spacing, punctuation)
3. Clean up malformed data (dates, phone numbers, emails, names)
4. Remove extra whitespace, emojis, or non-standard characters if inappropriate
5. Preserve the original meaning - do NOT invent or add information
6. Keep technical terms, proper nouns, and intentional formatting
7. If a piece of text is already clean, return it unchanged

OUTPUT FORMAT:
Return a JSON array with the cleaned text for each block.
Each object should have "id" and "cleaned" fields.

Example:
[{"id": 0, "cleaned": "John Smith"}, {"id": 1, "cleaned": "john.smith@email.com"}]

Return ONLY valid JSON. No explanations, no markdown."""

    @staticmethod
    def format_blocks(blocks: List[Dict]) -> str:
        """Format content blocks for LLM processing."""
        prompt = "Clean the following text blocks and return the cleaned versions:\n\n"
        for block in blocks:
            prompt += f"[ID: {block['id']}] {block['text']}\n"
        return prompt

    @staticmethod
    def parse_response(response: str, original_blocks: List[Dict]) -> List[Dict]:
        """Parse LLM response and merge with original blocks."""
        try:
            # Try to extract JSON from response
            # Handle markdown code blocks
            if "```" in response:
                import re

                json_match = re.search(
                    r"```(?:json)?\s*(\[.*?\])\s*```", response, re.DOTALL
                )
                if json_match:
                    response = json_match.group(1)

            # Find array bounds
            start = response.find("[")
            end = response.rfind("]")
            if start != -1 and end != -1:
                response = response[start : end + 1]

            cleaned_data = json.loads(response)

            # Build lookup
            cleaned_lookup = {item["id"]: item["cleaned"] for item in cleaned_data}

            # Merge with original
            result = []
            for block in original_blocks:
                result.append(
                    {
                        "id": block["id"],
                        "original": block["text"],
                        "cleaned": cleaned_lookup.get(block["id"], block["text"]),
                    }
                )

            return result

        except json.JSONDecodeError:
            # If parsing fails, return originals unchanged
            return [
                {"id": b["id"], "original": b["text"], "cleaned": b["text"]}
                for b in original_blocks
            ]
