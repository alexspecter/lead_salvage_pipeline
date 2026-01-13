"""
DOCX Cleaner Runner
Orchestrates the full DOCX cleaning pipeline.
"""
import os
import sys
from typing import Optional

from lead_cleaner.docx_cleaner.extractor import DocxExtractor
from lead_cleaner.docx_cleaner.reconstructor import DocxReconstructor
from lead_cleaner.docx_cleaner.prompt import DocxPromptGenerator
from lead_cleaner.phase2_semantic.model import LocalLLM
from lead_cleaner.phase2_semantic.memory_guard import MemoryGuard
from lead_cleaner.logging.logger import PipelineLogger
from lead_cleaner.utils.uuid import generate_run_id
from lead_cleaner.config import CHUNK_SIZE


class DocxCleanerRunner:
    """Runs the DOCX cleaning pipeline."""
    
    def __init__(self, input_path: str, output_path: Optional[str] = None):
        self.input_path = input_path
        self.run_id = generate_run_id()
        self.logger = PipelineLogger(self.run_id)
        
        # Default output path
        if output_path is None:
            base, ext = os.path.splitext(input_path)
            self.output_path = f"{base}_cleaned{ext}"
        else:
            self.output_path = output_path
        
        self.memory_guard = MemoryGuard(self.logger)
        self.llm = LocalLLM(self.logger)
        
    def run(self) -> str:
        """Run the full cleaning pipeline."""
        print(f"--- DOCX Cleaner Run {self.run_id} ---")
        self.logger.log_event("DOCX_CLEANER", "START", reason=f"Processing {self.input_path}")
        
        try:
            # 1. Extract content
            self.logger.log_event("DOCX_CLEANER", "EXTRACT_START")
            extractor = DocxExtractor(self.input_path)
            blocks = extractor.get_blocks_for_cleaning()
            self.logger.log_event("DOCX_CLEANER", "EXTRACT_COMPLETE", reason=f"Found {len(blocks)} content blocks")
            print(f"Extracted {len(blocks)} content blocks")
            
            if not blocks:
                print("No content to clean.")
                return self.input_path
            
            # 2. Check memory and load model
            self.memory_guard.check_memory()
            self.llm.load_model()
            
            # 3. Clean content in chunks
            cleaned_blocks = []
            chunk_size = min(CHUNK_SIZE, 20)  # Limit for DOCX (more context per block)
            
            for i in range(0, len(blocks), chunk_size):
                self.memory_guard.check_memory()
                
                chunk = blocks[i:i + chunk_size]
                cleaned_chunk = self._clean_chunk(chunk)
                cleaned_blocks.extend(cleaned_chunk)
                
                print(f"Cleaned blocks {i+1}-{min(i+chunk_size, len(blocks))} of {len(blocks)}")
            
            # 4. Reconstruct document
            self.logger.log_event("DOCX_CLEANER", "RECONSTRUCT_START")
            reconstructor = DocxReconstructor(self.input_path, cleaned_blocks)
            output_path = reconstructor.reconstruct(self.output_path)
            self.logger.log_event("DOCX_CLEANER", "RECONSTRUCT_COMPLETE", reason=f"Saved to {output_path}")
            
            print(f"\n--- SUCCESS ---")
            print(f"Cleaned document saved to: {output_path}")
            print(f"Log file: {self.logger.log_file}")
            
            return output_path
            
        except Exception as e:
            self.logger.log_error("DOCX_CLEANER", "FATAL_ERROR", e)
            print(f"\n[ERROR] {str(e)}")
            raise e
    
    def _clean_chunk(self, chunk) -> list:
        """Clean a chunk of content blocks using LLM."""
        system_prompt = DocxPromptGenerator.get_system_prompt()
        user_prompt = DocxPromptGenerator.format_blocks(chunk)
        full_prompt = f"{system_prompt}\n\n{user_prompt}"
        
        response = self.llm.generate_response(full_prompt)
        
        cleaned = DocxPromptGenerator.parse_response(response, chunk)
        
        # Log changes
        for item in cleaned:
            if item["original"] != item["cleaned"]:
                self.logger.log_event(
                    "DOCX_CLEANER", 
                    "TEXT_CLEANED",
                    before=item["original"][:50],
                    after=item["cleaned"][:50]
                )
        
        return cleaned


def main():
    """CLI entry point."""
    if len(sys.argv) < 2:
        print("Usage: python -m lead_cleaner.docx_cleaner.runner <input.docx> [output.docx]")
        sys.exit(1)
    
    input_path = sys.argv[1]
    output_path = sys.argv[2] if len(sys.argv) > 2 else None
    
    if not os.path.exists(input_path):
        print(f"Error: File not found: {input_path}")
        sys.exit(1)
    
    runner = DocxCleanerRunner(input_path, output_path)
    runner.run()


if __name__ == "__main__":
    main()
