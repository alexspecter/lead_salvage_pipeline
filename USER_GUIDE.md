# Lead Cleaner Pipeline - User Guide

## Quick Start

### 1. Install Dependencies
```bash
cd data_cleaning_pipeline
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Run the Pipeline

**CSV Cleaning:**
```bash
python run_pipeline.py <input_csv> <output_directory>
```

**Example:**
```bash
python run_pipeline.py input/leads.csv output/cleaned_run
```

**DOCX Cleaning:**
```bash
python -m lead_cleaner.docx_cleaner.runner <input.docx> [output.docx]
```

---

## Input Requirements

### CSV Format
Your CSV must contain at least one of these columns:
- `email`
- `phone`

Optional columns: `first_name`, `last_name`, `job_title`, `company`, `date`

### DOCX Format
Any valid `.docx` file with text content.

---

## Output Locations

After running, find your outputs in the specified output directory:

```
output/<your_output_dir>/
├── final_output_<run_id>.csv    # Successfully cleaned rows
├── reject_store_<run_id>.csv    # Rows that could not be cleaned
```

**Logs:**
```
logs/run_<run_id>.csv            # Full audit trail of all operations
```

---

## Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           LEAD CLEANER PIPELINE                             │
└─────────────────────────────────────────────────────────────────────────────┘

                              ┌──────────────┐
                              │  INPUT CSV   │
                              └──────┬───────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  PHASE 0: VALIDATION                                                        │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  • Validate file exists and is readable                              │   │
│  │  • Check required columns (email OR phone)                           │   │
│  │  • Normalize headers (lowercase, strip spaces)                       │   │
│  │  • Generate unique run_id                                            │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
└────────────────────────────────────┬────────────────────────────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  PHASE 1: DETERMINISTIC CLEANING                                            │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  For each row:                                                        │   │
│  │  1. Assign unique row_id (UUID)                                       │   │
│  │  2. Normalize fields using deterministic rules:                       │   │
│  │     • Emails → lowercase, validate format                            │   │
│  │     • Phones → extract digits, format consistently                   │   │
│  │     • Names → proper case, trim whitespace                           │   │
│  │     • Dates → standardize to YYYY-MM-DD                              │   │
│  │     • Job Titles → expand abbreviations, proper case                 │   │
│  │  3. Generate fingerprint (email|phone)                                │   │
│  │  4. Detect duplicates by fingerprint                                  │   │
│  │  5. Calculate confidence score (0.0 - 1.0)                            │   │
│  │  6. Route row based on score and validation                           │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│                          ┌──────────────────┐                               │
│                          │  ROUTE DECISION  │                               │
│                          └────────┬─────────┘                               │
│                    ┌──────────────┼──────────────┐                          │
│                    ▼              ▼              ▼                          │
│              ┌─────────┐   ┌───────────┐  ┌──────────┐                      │
│              │  CLEAN  │   │ AI_REQUIRED│  │ REJECTED │                      │
│              │ (≥0.7)  │   │  (<0.7)   │  │(Duplicate)│                      │
│              └────┬────┘   └─────┬─────┘  └─────┬────┘                      │
│                   │              │              │                            │
└───────────────────┼──────────────┼──────────────┼───────────────────────────┘
                    │              │              │
                    │              ▼              │
                    │   ┌─────────────────────┐   │
                    │   │  *** CRITICAL GATE ***│   │
                    │   │  Run Phase 1 Tests  │   │
                    │   │  If FAIL → ABORT    │   │
                    │   └──────────┬──────────┘   │
                    │              │              │
                    │              ▼              │
┌───────────────────┼─────────────────────────────┼───────────────────────────┐
│  PHASE 2: SEMANTIC CLEANING (LLM)              │                            │
│                    │              │              │                            │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  • Check memory (abort if >95%)                                      │   │
│  │  • Load Llama 3.1 70B model via MLX                                  │   │
│  │  • Process AI_REQUIRED rows in chunks                                │   │
│  │  • For each row:                                                      │   │
│  │    - Generate cleaning prompt                                        │   │
│  │    - Send to LLM (temperature=0)                                     │   │
│  │    - Parse JSON response                                             │   │
│  │    - Update clean_data                                               │   │
│  │  • Force garbage collection after each chunk                         │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                    │              │              │                            │
│                    │              ▼              │                            │
│                    │      ┌───────────────┐      │                            │
│                    │      │ CLEAN (fixed) │      │                            │
│                    │      └───────┬───────┘      │                            │
│                    │              │              │                            │
└────────────────────┼──────────────┼──────────────┼──────────────────────────┘
                     │              │              │
                     ▼              ▼              ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  PHASE 3: MERGE & VERIFY                                                     │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  1. Merge all CLEAN rows → final_output.csv                          │   │
│  │  2. Merge all REJECTED rows → reject_store.csv                       │   │
│  │  3. Verify:                                                           │   │
│  │     • input_count == final_count + reject_count                      │   │
│  │     • No row_id in both outputs                                       │   │
│  │     • Every original row accounted for exactly once                  │   │
│  │  4. If verification fails → DO NOT write outputs                     │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
                                     │
                     ┌───────────────┴───────────────┐
                     ▼                               ▼
           ┌─────────────────┐             ┌─────────────────┐
           │ final_output.csv│             │reject_store.csv │
           │  (Clean leads)  │             │ (Failed leads)  │
           └─────────────────┘             └─────────────────┘
```

---

## Row Status Meanings

| Status | Meaning |
|:---|:---|
| `CLEAN` | Row passed all cleaning steps |
| `AI_REQUIRED` | Row needs LLM processing (intermediate) |
| `REJECTED` | Row failed cleaning (duplicate, invalid, or LLM failure) |

## Failure Reasons

| Reason | Meaning |
|:---|:---|
| `DUPLICATE` | Row is a duplicate of another |
| `INVALID_FORMAT` | LLM returned invalid JSON |
| `MODEL_CRASH` | LLM failed to process |
| `UNKNOWN` | Unexpected error |

---

## Memory Safety

The pipeline enforces a **95% RAM limit**. If memory usage exceeds this threshold:
1. The process will **abort safely**
2. Current progress is logged
3. You can resume by re-running with the same input

### Adjusting Chunk Size
Edit `lead_cleaner/config.py`:
```python
CHUNK_SIZE = 10  # Reduce if memory issues occur
```
