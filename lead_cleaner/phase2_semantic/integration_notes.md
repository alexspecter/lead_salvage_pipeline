# Phase 2 Integration Notes

The `runner.py` for Phase 2 needs to:
1. Initialize `MemoryGuard` and `LocalLLM`.
2. Load "AI_REQUIRED" rows from the input DataFrame (passed by Orchestrator).
3. Batch them using `chunker`.
4. Loop through batches:
   - Check MemoryGuard.
   - Construct prompt using `PromptGenerator`.
   - Call `LocalLLM`.
   - Parse JSON.
   - Update row status to CLEAN or REJECTED (if model fails).
   - FORCE GC.
5. Return processed rows.

Note: `model.py` import will fail without `mlx-lm` installed and on Apple Silicon.
For the tests, we should mock `LocalLLM`.
