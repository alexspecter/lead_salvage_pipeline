# Developer Testing Workflow

## Directory Structure
- `testcache/` - All intermediate test runs go here. Clean up after verification.
- `user_output/` - Final verified output only.
- `user_input_tests/` - Input test files.

## Running Tests

### 1. Run Pipeline to Testcache
```bash
./venv/bin/python run_pipeline.py "user_input_tests/your_file.csv" "testcache"
```

### 2. Analyze Results
```bash
# Analyze the latest run in testcache
./venv/bin/python analyze_results.py -o testcache --latest

# Or specify a run ID
./venv/bin/python analyze_results.py -o testcache -r <run_id>
```

### 3. If Successful, Move to user_output
```bash
# Move the specific successful files
mv testcache/final_output_<run_id>.csv user_output/
mv testcache/pipeline_log_<run_id>.csv user_output/
mv testcache/rejection_cache_<run_id> user_output/
```

### 4. Cleanup Testcache
```bash
rm -rf testcache/*
```

## Key Files
- `analyze_results.py` - Permanent analysis tool (never delete)
- `run_pipeline.py` - Main pipeline entry point

## Anti-Patterns
❌ Do NOT create `analyze_v6.py`, `analyze_v7.py`, etc.
❌ Do NOT create `user_output_v2`, `user_output_v3`, etc.
✅ Use `testcache/` for all intermediate runs.
✅ Use flags with `analyze_results.py`.
