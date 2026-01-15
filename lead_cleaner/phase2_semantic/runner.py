import json
import logging
from typing import List

from lead_cleaner.types import LeadRow, RowStatus, FailureReason
from lead_cleaner.logging.logger import PipelineLogger
from lead_cleaner.config import CHUNK_SIZE, FORCE_GC_COLLECT
from lead_cleaner.phase2_semantic.model import LocalLLM
from lead_cleaner.phase2_semantic.memory_guard import MemoryGuard
from lead_cleaner.phase2_semantic.chunker import chunk_data
from lead_cleaner.phase2_semantic.prompt import PromptGenerator
from lead_cleaner.constants import PHASE_2_SEMANTIC

from lead_cleaner.utils.rejection_cache import RejectionCache

class Phase2Runner:
    def __init__(self, logger: PipelineLogger, run_id: str, rejection_cache: RejectionCache):
        self.logger = logger
        self.run_id = run_id
        self.rejection_cache = rejection_cache
        self.memory_guard = MemoryGuard(logger)
        self.llm = LocalLLM(logger)
        
    def process(self, rows: List[LeadRow]) -> List[LeadRow]:
        self.logger.log_event(PHASE_2_SEMANTIC, "START", reason=f"Processing {len(rows)} rows")
        
        # Filter for AI_REQUIRED
        ai_rows = [r for r in rows if r["status"] == RowStatus.AI_REQUIRED]
        skipped_rows = [r for r in rows if r["status"] != RowStatus.AI_REQUIRED]
        
        if not ai_rows:
            self.logger.log_event(PHASE_2_SEMANTIC, "SKIP", reason="No rows require AI processing")
            return rows

        # Dynamic Model Selection
        # If batch is small (<= 200), use the high-quality 70B model
        # Otherwise use the fast 8B model (default)
        model_id = "mlx-community/Meta-Llama-3.1-8B-Instruct-4bit"
        if len(ai_rows) <= 200:
            model_id = "mlx-community/Meta-Llama-3.1-70B-Instruct-4bit"
            self.logger.log_event(PHASE_2_SEMANTIC, "MODEL_SELECT", reason=f"Small batch ({len(ai_rows)} rows). Using 70B model for quality.")
        else:
            self.logger.log_event(PHASE_2_SEMANTIC, "MODEL_SELECT", reason=f"Large batch ({len(ai_rows)} rows). Using 8B model for speed.")
            
        # Re-initialize LLM with chosen model
        self.llm = LocalLLM(self.logger, model_path=model_id)

        # Load Model (Guarded)
        try:
            self.memory_guard.check_memory()
            self.llm.load_model()
        except Exception as e:
            self.logger.log_error(PHASE_2_SEMANTIC, "MODEL_LOAD_ABORT", e)
            # Mark all as REJECTED due to model failure
            for r in ai_rows:
                r["status"] = RowStatus.REJECTED
                r["failure_reason"] = FailureReason.MODEL_CRASH
            return rows

        # Adaptive Chunking
        current_chunk_size = CHUNK_SIZE
        
        # We need to manage the index manually since we might resize chunks
        i = 0
        total_ai = len(ai_rows)
        
        while i < total_ai:
            # Check memory pressure before batch
            try:
                usage = self.memory_guard.check_memory()
                
                # Proactive resizing if > 85%
                if usage > 0.85:
                    new_size = max(1, current_chunk_size // 2)
                    if new_size < current_chunk_size:
                        self.logger.log_event(PHASE_2_SEMANTIC, "MEMORY_PRESSURE", reason=f"Usage {usage*100:.1f}%. Reducing chunk size to {new_size}")
                        current_chunk_size = new_size
                        
            except Exception:
                 # If we are already over limit (95%), check_memory raises MemoryLimitError
                 # We let it bubble up to terminate as per directive.
                 raise

            # Helper to get batch
            batch = ai_rows[i : i + current_chunk_size]
            
            try:
                self._process_batch(batch)
                
                # Success: If memory is okay (< 70%), maybe recover chunk size?
                # Optimization for speed.
                if usage < 0.70 and current_chunk_size < CHUNK_SIZE:
                     current_chunk_size = min(current_chunk_size * 2, CHUNK_SIZE)
                
                i += len(batch)
                
            except MemoryError: # Python actual OOM
                 self.logger.log_event(PHASE_2_SEMANTIC, "OOM_WARNING", reason="Python MemoryError caught. Halving chunk size.")
                 
                 if current_chunk_size == 1:
                     # We cannot reduce further. Fail this specific row and move on.
                     self.logger.log_error(PHASE_2_SEMANTIC, "ROW_OOM", Exception("Row too large for memory."))
                     row = ai_rows[i]
                     row["status"] = RowStatus.REJECTED
                     row["failure_reason"] = FailureReason.MODEL_CRASH
                     i += 1
                 else:
                     current_chunk_size = max(1, current_chunk_size // 2)
                     self.memory_guard.force_gc()
                     # Retry same `i`
                 
            except Exception as e:
                # If it's a model crash or other error not memory, we fail the batch
                self.logger.log_error(PHASE_2_SEMANTIC, "BATCH_FAILED", e)
                for r in batch:
                    r["status"] = RowStatus.REJECTED
                    r["failure_reason"] = FailureReason.MODEL_CRASH
                i += len(batch)

            if FORCE_GC_COLLECT:
                self.memory_guard.force_gc()

        self.logger.log_event(PHASE_2_SEMANTIC, "COMPLETE")
        
        # Re-assemble
        return skipped_rows + ai_rows

    def _process_batch(self, batch: List[LeadRow]):
        system_prompt = PromptGenerator.get_system_prompt()
        
        for row in batch:
            try:
                user_prompt = PromptGenerator.format_row(row["raw_data"])
                full_prompt = f"{system_prompt}\n{user_prompt}"
                
                response_str = self.llm.generate_response(full_prompt)
                
                # Parse JSON (Robust)
                cleaned_data = None
                parse_error = None
                
                try:
                    # 1. Try direct parse
                    cleaned_data = json.loads(response_str)
                except json.JSONDecodeError:
                    # 2. Try stripping markdown code blocks
                    import re
                    json_match = re.search(r"```json\s*(\{.*?\})\s*```", response_str, re.DOTALL)
                    if not json_match:
                         json_match = re.search(r"```\s*(\{.*?\})\s*```", response_str, re.DOTALL)
                    
                    if json_match:
                        try:
                            cleaned_data = json.loads(json_match.group(1))
                        except json.JSONDecodeError as e:
                            parse_error = e
                    else:
                        # 3. Try finding first { and last }
                        # Handle Hallucinated Suffixes (e.g. "Input: ...")
                        # Truncate at "Input:" if present
                        check_str = response_str
                        input_idx = check_str.find("Input:")
                        if input_idx != -1:
                            check_str = check_str[:input_idx]
                            
                        start = check_str.find("{")
                        end = check_str.rfind("}")
                        
                        if start != -1 and end != -1:
                            try:
                                candidate = check_str[start:end+1]
                                cleaned_data = json.loads(candidate)
                            except json.JSONDecodeError as e:
                                parse_error = e
                        else:
                             # Fallback: try original string just in case 'Input:' wasn't the delimiter
                             start = response_str.find("{")
                             end = response_str.rfind("}")
                             if start != -1 and end != -1:
                                 try:
                                     cleaned_data = json.loads(response_str[start:end+1])
                                 except json.JSONDecodeError as e:
                                     parse_error = e
                             else:
                                 parse_error = json.JSONDecodeError("No JSON found", response_str, 0)
                
                # Handle parse failure
                if parse_error or cleaned_data is None:
                    row["status"] = RowStatus.REJECTED
                    row["failure_reason"] = FailureReason.INVALID_FORMAT
                    self.logger.log_event(PHASE_2_SEMANTIC, "JSON_PARSE_ERROR", row_id=row["row_id"], reason=response_str)
                    continue
                    
                # Validate schema roughly
                if not isinstance(cleaned_data, dict):
                    row["status"] = RowStatus.REJECTED
                    row["failure_reason"] = FailureReason.INVALID_FORMAT
                    self.logger.log_event(PHASE_2_SEMANTIC, "SCHEMA_ERROR", row_id=row["row_id"], reason="LLM returned non-dict")
                    continue
                
                # NORMALIZE AI KEYS: Map aliases back to canonical names
                # e.g. AI returns "job" -> map to "job_title"
                from lead_cleaner.constants import FIELD_TYPE_PATTERNS
                
                normalized_ai_data = {}
                for key, value in cleaned_data.items():
                    key_lower = key.lower().strip()
                    mapped_key = key
                    
                    # Check against all patterns
                    for canonical, aliases in FIELD_TYPE_PATTERNS.items():
                        if key_lower == canonical or key_lower in aliases:
                            mapped_key = canonical
                            break
                    
                    normalized_ai_data[mapped_key] = value
                
                # Update row
                cleaned_data = normalized_ai_data
                    
                # Merge LLM output but preserve Phase 1 normalized values for critical fields
                # LLM may return raw_data phone format, but Phase 1 already normalized it
                preserved_fields = {'phone', 'email'}  # Fields Phase 1 handles well
                
                for key, value in cleaned_data.items():
                    key_lower = key.lower()
                    
                    # Safety Check: Don't let AI overwrite existing data with "Not Provided" or None
                    if key_lower in row["clean_data"] and row["clean_data"][key_lower]:
                        # Definition of "useless" new value
                        is_useless = (
                            value is None 
                            or (isinstance(value, str) and (not value.strip() or value.strip().lower() == "not provided"))
                        )
                        if is_useless and key_lower != 'phone':
                            continue # Keep Phase 1 value

                    # If this is a preserved field and Phase 1 already has a valid value, keep it
                    if key_lower in preserved_fields and key_lower in row["clean_data"]:
                        existing = row["clean_data"][key_lower]
                        # Phone: AI is authority. Do not preserve Phase 1 phone.
                        
                        if key_lower == 'email' and existing and '@' in str(existing):
                            continue  # Keep Phase 1's email
                    
                    row["clean_data"][key_lower] = value
                    
                row["status"] = RowStatus.CLEAN
                row["confidence_score"] = 0.8 # Arbitrary for now, could ask LLM for confidence
                
                self.logger.log_event(
                    PHASE_2_SEMANTIC, 
                    "ROW_RESOLVED", 
                    row_id=row["row_id"],
                    confidence=row["confidence_score"]
                )
                    
            except Exception as e:
                row["status"] = RowStatus.REJECTED
                row["failure_reason"] = FailureReason.UNKNOWN
                self.logger.log_error(PHASE_2_SEMANTIC, "ROW_ERROR", e)

